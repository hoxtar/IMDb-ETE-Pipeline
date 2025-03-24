import os
import gzip
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin # Allows Airflow-native logging for tasks
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Initialize logger for Airflow logging (Directly use log.info, log.error, etc.)
log = LoggingMixin().log

# -----------------------------
# Configuration Section
# -----------------------------
# Directory where IMDb files are downloaded
DOWNLOAD_DIR = '/opt/airflow/dags/files'
# Directory containing SQL table creation scripts
SCHEMA_DIR = '/opt/airflow/schemas'

# IMDb file mapping: {key: filename}
IMDB_FILES = {
    'title_basics': 'title.basics.tsv.gz',
    'title_akas': 'title.akas.tsv.gz',
    'title_ratings': 'title.ratings.tsv.gz',
    'name_basics': 'name.basics.tsv.gz',
    'title_crew': 'title.crew.tsv.gz',
    'title_episode': 'title.episode.tsv.gz',
    'title_principals': 'title.principals.tsv.gz'
}

# Base URL to download IMDb datasets
BASE_URL = 'https://datasets.imdbws.com/'

# Table-specific configurations: column list and expected column count
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

    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000: # Check if file is > 1 KB (Not empty)
        log.info(f"[SKIP] {filename} already exists.")
        return

    log.info(f"Downloading {filename} from {url}...")
    response = requests.get(url)
    response.raise_for_status()  # Raise error if download fails

    with open(filepath, 'wb') as f:
        f.write(response.content)
    log.info(f"[DONE] Saved {filename} to {filepath}")

# -----------------------------
# Load Task Function
# -----------------------------
def load_table_to_postgres(file_key):
    """
    Load data from IMDb file into PostgreSQL:
    1. Create table using schema SQL.
    2. Truncate table (idempotency).
    3. Load data from TSV file.
    """
    hook = PostgresHook(postgres_conn_id='my_postgres')  # Set in Airflow UI
    conn = hook.get_conn()
    cur = conn.cursor()

    config = TABLE_CONFIGS[file_key]
    table_name = f"imdb_{file_key}"
    columns = config['columns']
    expected_col_count = config['column_count']

    # Step 1: Run CREATE TABLE from SQL file
    schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
    with open(schema_file_path, 'r') as schema_file:
        create_sql = schema_file.read()
        cur.execute(create_sql)
        conn.commit()
        log.info(f"[TABLE READY] {table_name}")

    # Step 2: Check file existence and truncate table
    gz_path = os.path.join(DOWNLOAD_DIR, IMDB_FILES[file_key])
    if not os.path.exists(gz_path) or os.path.getsize(gz_path) < 1000:
        log.warning(f"[SKIP] File missing or empty: {gz_path}")
        return

    cur.execute(f"TRUNCATE TABLE {table_name};")
    conn.commit()
    log.info(f"[TRUNCATED] {table_name}")

    # Step 3: Prepare insertion
    col_placeholder = ', '.join(columns)
    val_placeholder = ', '.join(['%s'] * expected_col_count)
    insert_query = f"INSERT INTO {table_name} ({col_placeholder}) VALUES ({val_placeholder});"

    # Insert data row-by-row (can be optimized later with batch inserts)
    row_count = 0
    with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
        next(f)  # Skip header
        batch = []
        for line in f:
            values = line.strip().split('\t')
            if len(values) == expected_col_count:
                batch.append(values)

        # Bulk insert
        cur.executemany(insert_query, batch)

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"[LOAD DONE] {row_count} rows into {table_name}")

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

        download_tasks = []
        load_tasks = []

        for file_key in IMDB_FILES:
            # Create download task for each file
            dl_task = PythonOperator(
                task_id=f"download_{file_key}",
                python_callable=download_imdb_data,
                op_args=[file_key],
            )

            # Create load task for each file
            ld_task = PythonOperator(
                task_id=f"load_{file_key}_to_postgres",
                python_callable=load_table_to_postgres,
                op_args=[file_key],
            )

            download_tasks.append(dl_task)
            load_tasks.append(ld_task)

            dl_task >> ld_task  # Set dependency: download -> load

        return dag

# Create the DAG
globals()['imdb_download_and_load_dag'] = create_imdb_dag()
