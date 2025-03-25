import os
import gzip
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin  # Airflow-native logging
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Initialize Airflow logger
log = LoggingMixin().log

# -----------------------------
# Configuration Section
# -----------------------------
DOWNLOAD_DIR = '/opt/airflow/dags/files'     # Download destination
SCHEMA_DIR = '/opt/airflow/schemas'          # SQL schema scripts directory

# IMDb dataset filenames
IMDB_FILES = {
    'title_basics': 'title.basics.tsv.gz',
    'title_akas': 'title.akas.tsv.gz',
    'title_ratings': 'title.ratings.tsv.gz',
    'name_basics': 'name.basics.tsv.gz',
    'title_crew': 'title.crew.tsv.gz',
    'title_episode': 'title.episode.tsv.gz',
    'title_principals': 'title.principals.tsv.gz'
}

BASE_URL = 'https://datasets.imdbws.com/'    # IMDb download base URL

# Schema definition for validation and insertion
TABLE_CONFIGS = {
    'title_basics': {
        'columns': ['tconst', 'titleType', 'primaryTitle', 'originalTitle',
                    'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres'],
        'column_count': 9
    },
    'title_akas': {
        'columns': ['titleId', 'ordering', 'title', 'region', 'language',
                    'types', 'attributes', 'isOriginalTitle'],
        'column_count': 8
    },
    'title_ratings': {
        'columns': ['tconst', 'averageRating', 'numVotes'],
        'column_count': 3
    },
    'name_basics': {
        'columns': ['nconst', 'primaryName', 'birthYear', 'deathYear',
                    'primaryProfession', 'knownForTitles'],
        'column_count': 6
    },
    'title_crew': {
        'columns': ['tconst', 'directors', 'writers'],
        'column_count': 3
    },
    'title_episode': {
        'columns': ['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber'],
        'column_count': 4
    },
    'title_principals': {
        'columns': ['tconst', 'ordering', 'nconst', 'category', 'job', 'characters'],
        'column_count': 6
    }
}

# -----------------------------
# Download Task Function
# -----------------------------
def download_imdb_data(file_key):
    """
    Download IMDb dataset file if not already downloaded.
    Skips download if the file already exists and is >1KB.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = IMDB_FILES[file_key]
    url = BASE_URL + filename
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(filepath):
        size_kb = os.path.getsize(filepath) / 1024
        if size_kb > 1:
            log.info(f"[SKIP] {filename} already exists. Size: {size_kb:.2f} KB")
            return
        else:
            log.warning(f"[WARN] {filename} exists but too small ({size_kb:.2f} KB). Re-downloading.")

    log.info(f"Downloading {filename} from {url}...")
    response = requests.get(url)
    response.raise_for_status()  # Raise HTTP error if download failed

    with open(filepath, 'wb') as f:
        f.write(response.content)
    size_kb = os.path.getsize(filepath) / 1024
    log.info(f"[DONE] Saved {filename} to {filepath} ({size_kb:.2f} KB)")


def sanitize_value(val):
    return None if val == r'\N' else val


# -----------------------------
# Load Task Function
# -----------------------------
def load_table_to_postgres(file_key):
    """
    Load IMDb data into PostgreSQL:
    1. Create table if not exists using SQL file.
    2. Truncate table (idempotent).
    3. Load data from decompressed TSV.
    """
    hook = PostgresHook(postgres_conn_id='my_postgres')
    conn = hook.get_conn()
    cur = conn.cursor()

    config = TABLE_CONFIGS[file_key]
    table_name = f"imdb_{file_key}"
    columns = config['columns']
    expected_col_count = config['column_count']

    # Step 1: Create table from schema
    schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
    log.info(f"[SCHEMA] Executing SQL for table: {table_name}")
    with open(schema_file_path, 'r') as schema_file:
        create_sql = schema_file.read()
        cur.execute(create_sql)
        conn.commit()
        log.info(f"[TABLE CREATED] {table_name}")

    # Step 2: File check and truncate
    gz_path = os.path.join(DOWNLOAD_DIR, IMDB_FILES[file_key])
    if not os.path.exists(gz_path) or os.path.getsize(gz_path) < 1000:
        log.warning(f"[SKIP] File missing or too small: {gz_path}")
        return

    log.info(f"[TRUNCATE] Clearing table {table_name} for fresh load...")
    cur.execute(f"TRUNCATE TABLE {table_name};")
    conn.commit()

    # Step 3: Prepare insertion
    col_placeholder = ', '.join(columns)
    val_placeholder = ', '.join(['%s'] * expected_col_count)
    insert_query = f"INSERT INTO {table_name} ({col_placeholder}) VALUES ({val_placeholder});"

    # Read and insert data
    row_count = 0
    batch_size = 10000 # Can be tweaked based on system resources
    batch = []
    log.info(f"[LOADING] Reading data from {gz_path}")
    with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
        next(f)  # Skip header
        for i, line in enumerate(f, start=1):
            values = [sanitize_value(v) for v in line.strip().split('\t')]

            if len(values) == expected_col_count:
                
                batch.append(values)
                row_count += 1

                if len(batch) >= batch_size:
                    cur.executemany(insert_query, batch)
                    conn.commit()
                    log.info(f"[INSERT] Inserted batch of {len(batch)} rows into {table_name} (total so far: {row_count})")
                    batch = []

            if i % 100000 == 0:
                log.info(f"[PROGRESS] Processed {i} lines...")

    # Leftover rows batch
    if batch:
        cur.executemany(insert_query, batch)
        conn.commit()
        log.info(f"[INSERT] Inserted final {len(batch)} rows into {table_name}...")

    # Sanity check
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    result = cur.fetchone()[0]
    log.info(f"[VERIFY] Row count in {table_name}: {result}")

    cur.close()
    conn.close()

    log.info(f"[LOAD DONE] Inserted {row_count} rows into {table_name}")

# -----------------------------
# DAG Definition
# -----------------------------
def create_imdb_dag():
    default_args = {
        'start_date': datetime(2025, 1, 1),
    }

    with DAG(
        dag_id="imdb_download_and_load_dag",
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False
    ) as dag:

        for file_key in IMDB_FILES:
            dl_task = PythonOperator(
                task_id=f"download_{file_key}",
                python_callable=download_imdb_data,
                op_args=[file_key],
            )

            ld_task = PythonOperator(
                task_id=f"load_{file_key}_to_postgres",
                python_callable=load_table_to_postgres,
                op_args=[file_key],
            )

            dl_task >> ld_task  # Ensure download before load

        return dag

# Register DAG
globals()['imdb_download_and_load_dag'] = create_imdb_dag()
