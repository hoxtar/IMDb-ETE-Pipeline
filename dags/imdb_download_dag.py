"""
Airflow DAG: IMDb Data Download and Load via Postgres COPY
Author: Your Name
Description:
  This DAG downloads IMDb TSV files and loads them into a Postgres database 
  using the COPY command for efficient bulk ingestion. It demonstrates 
  best practices in logging, error handling, and a clear structure 
  for a robust data pipeline.
"""

import os
import gzip
import shutil
import requests

from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.postgres_hook import PostgresHook

# Initialize Airflow logger
log = LoggingMixin().log

# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

DOWNLOAD_DIR = os.environ.get('IMDB_DOWNLOAD_DIR', '/opt/airflow/dags/files')
SCHEMA_DIR = os.environ.get('IMDB_SCHEMA_DIR', '/opt/airflow/schemas')
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID', 'my_postgres')

BASE_URL = 'https://datasets.imdbws.com/'

IMDB_FILES = {
    'title_basics': 'title.basics.tsv.gz',
    'title_akas': 'title.akas.tsv.gz',
    'title_ratings': 'title.ratings.tsv.gz',
    'name_basics': 'name.basics.tsv.gz',
    'title_crew': 'title.crew.tsv.gz',
    'title_episode': 'title.episode.tsv.gz',
    'title_principals': 'title.principals.tsv.gz'
}

TABLE_CONFIGS = {
    'title_basics': {
        'columns': [
            'tconst', 'titleType', 'primaryTitle', 'originalTitle',
            'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres'
        ],
        'column_count': 9
    },
    'title_akas': {
        'columns': [
            'titleId', 'ordering', 'title', 'region', 'language',
            'types', 'attributes', 'isOriginalTitle'
        ],
        'column_count': 8
    },
    'title_ratings': {
        'columns': ['tconst', 'averageRating', 'numVotes'],
        'column_count': 3
    },
    'name_basics': {
        'columns': [
            'nconst', 'primaryName', 'birthYear', 'deathYear',
            'primaryProfession', 'knownForTitles'
        ],
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


# ----------------------------------------------------------------------------
# 2. Helper Functions
# ----------------------------------------------------------------------------

def create_table_if_not_exists(cur, schema_file_path: str, table_name: str):
    """
    Executes the SQL in schema_file_path to create a Postgres table if not already present.
    """
    log.info(f"[SCHEMA] Creating table if not exists: {table_name}")
    with open(schema_file_path, 'r') as schema_file:
        create_sql = schema_file.read()
    cur.execute(create_sql)


# ----------------------------------------------------------------------------
# 3. Task Functions
# ----------------------------------------------------------------------------

def download_imdb_data(file_key: str) -> None:
    """
    Downloads the IMDb file from the official URL. 
    Uses 'Last-Modified' headers to skip if local copy is up-to-date.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = IMDB_FILES[file_key]
    file_url = BASE_URL + filename
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    try:
        # HEAD request for Last-Modified
        head_resp = requests.head(file_url, timeout=30)
        head_resp.raise_for_status()

        remote_last_modified_str = head_resp.headers.get('Last-Modified')

        if os.path.exists(filepath):
            local_timestamp = os.path.getmtime(filepath)
            local_dt = datetime.utcfromtimestamp(local_timestamp)
            log.info(f"[LOCAL] {filepath} last modified: {local_dt}")

            if remote_last_modified_str:
                remote_dt = parsedate_to_datetime(remote_last_modified_str)
                log.info(f"[REMOTE] {filename} last modified: {remote_dt}")

                # Compare them directly as datetime objects
                if local_dt >= remote_dt:
                    log.info(f"[SKIP] Local is up-to-date. (Local: {local_dt}, Remote: {remote_dt})")
                    return
                else:
                    log.info(f"[RE-DOWNLOAD] Local is older. (Local: {local_dt}, Remote: {remote_dt})")
            else:
                # If there's no Last-Modified header, fallback to always re-download
                log.warning("[WARN] No 'Last-Modified' in response headers. Re-downloading anyway.")

        # If we reach this point:
        # - file doesn't exist, or
        # - remote is newer, or
        # - no Last-Modified in headers
        # => Do the download
        log.info(f"[DOWNLOAD] Fetching {filename} from {file_url}...")
        dl_resp = requests.get(file_url, timeout=120)
        dl_resp.raise_for_status()

        with open(filepath, 'wb') as f:
            f.write(dl_resp.content)
        size_kb = os.path.getsize(filepath) / 1024
        log.info(f"[DONE] Saved {filename} -> {filepath} ({size_kb:.2f} KB)")

    except requests.exceptions.RequestException as re:
        log.error(f"[ERROR] Request failed for {filename}: {re}")
        raise
    except Exception as e:
        log.error(f"[ERROR] General failure for {filename}: {e}")
        raise


def load_table_to_postgres_via_copy(file_key: str) -> None:
    """
    Loads IMDb TSV data into Postgres via COPY command for maximum performance.
    Steps:
      1) Create table if not exists.
      2) TRUNCATE the table for idempotent load.
      3) Decompress .gz into a temporary .tsv file (strip the header line).
      4) Use COPY to load, specifying null placeholder and tab delimiter.
      5) Validate row count in the DB matches the file lines.
    """
    table_name = f"imdb_{file_key}"
    config = TABLE_CONFIGS[file_key]
    columns = config['columns']
    expected_col_count = config['column_count']

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
    gz_path = os.path.join(DOWNLOAD_DIR, IMDB_FILES[file_key])

    # We'll store the decompressed file in the same directory but with .tsv extension
    uncompressed_tsv_path = os.path.join(DOWNLOAD_DIR, f"{file_key}.tsv")

    try:
        # 1) Create table if needed
        create_table_if_not_exists(cur, schema_file_path, table_name)
        conn.commit()

        # 2) Quick file check
        if not os.path.exists(gz_path) or os.path.getsize(gz_path) < 1000:
            log.warning(f"[SKIP] {gz_path} is missing or too small.")
            return

        # TRUNCATE the table
        log.info(f"[TRUNCATE] Clearing table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()

        # 3) Decompress and remove the header
        log.info(f"[DECOMPRESS] Creating temporary TSV: {uncompressed_tsv_path}")
        with gzip.open(gz_path, 'rt', encoding='utf-8') as gzfile, open(uncompressed_tsv_path, 'w', encoding='utf-8') as tsvfile:
            # Skip the header line once
            header = next(gzfile)
            log.info(f"[HEADER] Skipped: {header.strip()}")
            # Write the rest directly
            shutil.copyfileobj(gzfile, tsvfile)

        # Now let's count the lines in the uncompressed TSV
        with open(uncompressed_tsv_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        log.info(f"[INFO] {uncompressed_tsv_path} has {total_lines:,} data lines (excluding header).")

        # 4) COPY into Postgres
        # We specify NULL '\N' since IMDb uses \N for missing values
        copy_sql = f"""
            COPY {table_name} ({', '.join(columns)})
            FROM STDIN
            WITH (
                FORMAT csv,
                DELIMITER E'\t',
                NULL '\\N',
                ENCODING 'UTF8'
            );
        """
        log.info(f"[COPY] Inserting data into {table_name} via COPY...")
        with open(uncompressed_tsv_path, 'r', encoding='utf-8') as tsvfile:
            cur.copy_expert(copy_sql, tsvfile)
        conn.commit()
        log.info(f"[COPY DONE] Finished loading {table_name} via COPY.")

        # 5) Validate row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        db_count = cur.fetchone()[0]
        log.info(f"[VERIFY] DB row count: {db_count:,}. File lines: {total_lines:,}.")

        if db_count != total_lines:
            log.warning(
                f"[MISMATCH] Inserted {db_count:,} rows but file had {total_lines:,} lines."
            )

    except Exception as e:
        log.error(f"[ERROR] Loading data via COPY into {table_name} failed: {e}")
        raise
    finally:
        # Clean up local temp file if you want
        # os.remove(uncompressed_tsv_path)
        cur.close()
        conn.close()


# ----------------------------------------------------------------------------
# 4. DAG Definition
# ----------------------------------------------------------------------------

dag = DAG(
    dag_id="imdb_download_and_load_dag",
    default_args={
        'start_date': datetime(2025, 1, 1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=8),
    },
    schedule_interval="@daily",
    catchup=False,
)

with dag:
    for file_key in IMDB_FILES:
        download = PythonOperator(
            task_id=f"download_{file_key}",
            python_callable=download_imdb_data,
            op_args=[file_key]
        )

        load_via_copy = PythonOperator(
            task_id=f"load_{file_key}_to_postgres",
            python_callable=load_table_to_postgres_via_copy,
            op_args=[file_key]
        )

        download >> load_via_copy
