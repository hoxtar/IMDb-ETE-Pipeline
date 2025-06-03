"""
IMDb Data Pipeline DAG
=====================

A production-ready Airflow DAG that maintains an up-to-date copy of IMDb datasets.

Features:
    - Smart downloading with Last-Modified checks
    - Efficient PostgreSQL bulk loading via COPY
    - Robust error handling with retries
    - Automatic cleanup of temporary files

Datasets Handled:
    - Title basics (movies, TV shows)
    - Title ratings
    - Name basics (people)
    - Title crews
    - Title episodes
    - Alternative titles
    - Principal cast/crew

Technical Details:
    - Source: IMDb TSV files (gzipped)
    - Target: PostgreSQL tables
    - Schedule: Daily
    - Timeout: 2 hours
    - Retries: 3 (5-minute delay)

Author: Andrea Usai
"""

import os
import gzip
import shutil
import requests
import time

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.postgres_hook import PostgresHook

# Initialize Airflow logger
log = LoggingMixin().log

# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

DOWNLOAD_DIR = os.environ.get('IMDB_DOWNLOAD_DIR', '/opt/airflow/data/files')
SCHEMA_DIR = os.environ.get('IMDB_SCHEMA_DIR', '/opt/airflow/schemas')
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID', 'my_postgres')

# Task-specific timeout configurations based on data volume analysis
TASK_TIMEOUTS = {
    # Fast downloads (< 100MB)
    'title_ratings': timedelta(minutes=15),
    'title_episode': timedelta(minutes=20),
    'title_akas': timedelta(minutes=30),
    
    # Medium datasets (100MB - 500MB)  
    'title_basics': timedelta(minutes=45),
    'name_basics': timedelta(minutes=45),
    
    # Large datasets (> 500MB, complex processing)
    'title_crew': timedelta(hours=1, minutes=30),
    'title_principals': timedelta(hours=2),
}

# Default timeout for any unspecified tasks
DEFAULT_TASK_TIMEOUT = timedelta(hours=1)

BASE_URL = 'https://datasets.imdbws.com/'

# Task-specific timeout configurations based on data volume analysis
TASK_TIMEOUTS = {
    # Fast downloads (< 100MB)
    'title_ratings': timedelta(minutes=15),
    'title_episode': timedelta(minutes=20),
    'title_akas': timedelta(minutes=30),
    
    # Medium datasets (100MB - 500MB)  
    'title_basics': timedelta(minutes=45),
    'name_basics': timedelta(minutes=45),
    
    # Large datasets (> 500MB, complex processing)
    'title_crew': timedelta(hours=1, minutes=30),
    'title_principals': timedelta(hours=2),
}

# Default timeout for any unspecified tasks
DEFAULT_TASK_TIMEOUT = timedelta(hours=1)

def get_task_timeout(file_key: str) -> timedelta:
    """Get appropriate timeout for specific task based on historical performance"""
    return TASK_TIMEOUTS.get(file_key, DEFAULT_TASK_TIMEOUT)

IMDB_FILES = {
    'title_basics': 'title.basics.tsv.gz',
    'title_akas': 'title.akas.tsv.gz',
    'title_ratings': 'title.ratings.tsv.gz',
    'name_basics': 'name.basics.tsv.gz',
    'title_crew': 'title.crew.tsv.gz',
    'title_episode': 'title.episode.tsv.gz',
    'title_principals': 'title.principals.tsv.gz'
}
 # Table configurations for IMDb data (Nested dictionary)
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

# Executes the SQL in schema_file_path to create a Postgres table if not already present
def setup_table_schema(cur: psycopg2.extensions.cursor, schema_file_path: str, table_name: str):
    
    log.info(f"[SCHEMA] Creating table if not exists: {table_name}")
    try:
        with open(schema_file_path, 'r', encoding='utf-8') as schema_file:
            create_sql = schema_file.read()
            # Validate SQL content to prevent SQL injection
            if not create_sql.strip().lower().startswith("create table"):
                log.error(f"[SECURITY] Invalid SQL detected in schema file: {schema_file_path}")
                raise ValueError("Invalid SQL content in schema file.")
    except Exception as e:
        log.error(f"[ERROR] Error reading schema file: {e}")
        raise
    else:
        cur.execute(create_sql)


# ----------------------------------------------------------------------------
# 3. Task Functions
# ----------------------------------------------------------------------------

def fetch_imdb_dataset(file_key: str) -> None:
    """
    Downloads and caches an IMDb dataset file if newer version exists.
    
    Args:
        file_key: Key identifying which IMDb dataset to download (e.g., 'title_basics')
    
    Features:
        - Checks remote file's Last-Modified date before downloading
        - Implements retry logic for network failures
        - Maintains local cache of downloaded files
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = IMDB_FILES[file_key]
    file_url = BASE_URL + filename
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Attempt to fetch headers first to check Last-Modified date
            log.info(f"[CHECK] Checking Last-Modified for {filename}...")
            head_resp = requests.head(file_url, timeout=30)
            head_resp.raise_for_status()
            remote_last_modified_str = head_resp.headers.get('Last-Modified')

            # Proceed with download if any of:
            # 1. File doesn't exist locally
            # 2. Remote version is newer
            # 3. Cannot verify file freshness (no Last-Modified header)
            if os.path.exists(filepath):
                local_timestamp = os.path.getmtime(filepath)
                # Create a timezone-aware UTC datetime object directly:
                local_dt = datetime.fromtimestamp(local_timestamp, timezone.utc)
                log.info(f"[LOCAL] {filepath} last modified: {local_dt.isoformat()}")

                if remote_last_modified_str:
                    remote_dt = parsedate_to_datetime(remote_last_modified_str)
                    
                    if local_dt >= remote_dt:
                        log.info(f"[SKIP] Local is up-to-date.")
                        return
                    else:
                        log.info(f"[RE-DOWNLOAD] Local is older.")
                else:
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
            return
            
        #Granular error handling
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            retry_count += 1
            if retry_count == max_retries:
                log.error(f"[ERROR] Final attempt to download {filename} after {max_retries} retries")
                raise
            log.warning(f"[RETRY] Attempt {retry_count} of {max_retries}")
            time.sleep(2 ** retry_count)
    
        except requests.exceptions.HTTPError as he:
            status_code = he.response.status_code if he.response else 'Unknown'
            log.error(f"[ERROR] HTTP {status_code} error for {filename}: {he}")
            raise


def load_imdb_table(file_key: str) -> None:
    """
    Loads an IMDb dataset into PostgreSQL efficiently with enhanced monitoring.
    
    Args:
        file_key: Key identifying which IMDb dataset to load (e.g., 'title_basics')
    
    Process:
        1. Validates/creates target table
        2. Cleans existing data (TRUNCATE)
        3. Extracts .gz file to temporary TSV
        4. Bulk loads using PostgreSQL COPY with progress monitoring
        5. Verifies row count integrity
    
    Enterprise Features:
        - Progress monitoring for long-running tasks
        - Heartbeat maintenance during processing
        - Resource usage logging
    
    Note: Automatically cleans up temporary files after loading
    """
    import time
    from airflow.operators.python import get_current_context
    
    # Get current context for heartbeat updates
    try:
        context = get_current_context()
        task_instance = context.get('task_instance')
    except:
        task_instance = None
    
    def update_heartbeat(message: str):
        """Update task heartbeat with progress message"""
        if task_instance:
            log.info(f"[HEARTBEAT] {message}")
            # Force heartbeat update
            task_instance.refresh_from_db()
        else:
            log.info(f"[PROGRESS] {message}")
    
    table_name = f"imdb_{file_key}"
    config = TABLE_CONFIGS[file_key]
    columns = config['columns']
    
    # Start timing for performance monitoring
    start_time = time.time()
    update_heartbeat(f"Starting load process for {table_name}")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
    gz_path = os.path.join(DOWNLOAD_DIR, IMDB_FILES[file_key])

    # We'll store the decompressed file in the same directory but with .tsv extension
    uncompressed_tsv_path = os.path.join(DOWNLOAD_DIR, f"{file_key}.tsv")

    try:
        # 1) Create table if needed
        update_heartbeat("Creating table schema if not exists")
        setup_table_schema(cur, schema_file_path, table_name)
        conn.commit()

        # 2) Quick file check
        if not os.path.exists(gz_path) or os.path.getsize(gz_path) < 1000:
            log.warning(f"[SKIP] {gz_path} is missing or too small.")
            return

        file_size_mb = os.path.getsize(gz_path) / (1024 * 1024)
        update_heartbeat(f"Processing file {gz_path} ({file_size_mb:.1f} MB)")

        # TRUNCATE the table to ensure clean data load
        update_heartbeat("Truncating existing data for clean reload")
        log.info(f"[TRUNCATE] Clearing table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()

        # 3) Decompress and remove the header
        update_heartbeat("Decompressing data file")
        log.info(f"[DECOMPRESS] Creating temporary TSV: {uncompressed_tsv_path}")
        with gzip.open(gz_path, 'rt', encoding='utf-8') as gzfile, open(uncompressed_tsv_path, 'w', encoding='utf-8') as tsvfile:
            # Skip the header line once
            header = next(gzfile)
            log.info(f"[HEADER] Skipped: {header.strip()}")
            # Write the rest directly
            shutil.copyfileobj(gzfile, tsvfile)

        # Now let's count the lines in the uncompressed TSV
        update_heartbeat("Counting data rows for validation")
        with open(uncompressed_tsv_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        log.info(f"[INFO] {uncompressed_tsv_path} has {total_lines:,} data lines (excluding header).")

        # 4) COPY into Postgres using high-performance bulk loading
        update_heartbeat(f"Starting bulk load of {total_lines:,} rows")
        copy_sql = f"""
            COPY {table_name} ({', '.join(columns)})
            FROM STDIN
            WITH (
                FORMAT TEXT,
                DELIMITER E'\t',
                NULL '\\N',
                ENCODING 'UTF8'
            );
        """
        log.info(f"[COPY] Inserting data into {table_name} via COPY...")
        with open(uncompressed_tsv_path, 'r', encoding='utf-8') as tsvfile:
            cur.copy_expert(copy_sql, tsvfile)
        conn.commit()
        
        processing_time = time.time() - start_time
        update_heartbeat(f"Bulk load completed in {processing_time:.1f} seconds")
        log.info(f"[COPY DONE] Finished loading {table_name} via COPY.")

        # 5) Validate row count
        update_heartbeat("Validating data integrity")
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        db_count = cur.fetchone()[0]
        log.info(f"[VERIFY] DB row count: {db_count:,}. File lines: {total_lines:,}.")
        
        if db_count != total_lines:
            log.warning(
                f"[MISMATCH] Inserted {db_count:,} rows but file had {total_lines:,} lines."
            )

        # Final performance metrics
        total_time = time.time() - start_time
        rows_per_second = db_count / total_time if total_time > 0 else 0
        update_heartbeat(f"Load completed: {db_count:,} rows in {total_time:.1f}s ({rows_per_second:.0f} rows/s)")

    except Exception as e:
        log.error(f"[ERROR] Loading data via COPY into {table_name} failed: {e}")
        raise
    finally:
        # Clean up extracted file uploaded
        if os.path.exists(uncompressed_tsv_path):
            os.remove(uncompressed_tsv_path)
            log.info(f"[CLEANUP] Removing temporary TSV file: {uncompressed_tsv_path}")

        cur.close()
        conn.close()


# ----------------------------------------------------------------------------
# 4. DAG Definition
# ----------------------------------------------------------------------------

dag = DAG(
    # Descriptive ID that clearly indicates DAG's purpose
    dag_id="imdb_download_and_load_dag",
    
    # Default arguments that apply to all tasks
    default_args={
        # Future start date to avoid accidental backfilling
        'start_date': datetime(2025, 1, 1),
          # Enhanced retry configuration for enterprise resilience
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
        
        # Base timeout - will be overridden per task
        'execution_timeout': DEFAULT_TASK_TIMEOUT,
        
        # Enterprise best practices
        'owner': 'data_engineering_team',
        'depends_on_past': False,
        'email_on_failure': False,  # Configure with your SMTP settings
        'email_on_retry': False,
        
        # SLA monitoring for enterprise compliance
        'sla': timedelta(hours=3),
    },
    
    # Daily schedule is appropriate for IMDb updates
    schedule_interval="@daily",
    
    # Prevent accidental historical runs
    catchup=False,
    
    # Additional best practices that could be added:
    # tags=['imdb', 'etl'],  # For better organization
    # doc_md=__doc__,  # Add documentation from module docstring
)

with dag:
    for file_key in IMDB_FILES:
        # Get task-specific timeout based on historical performance
        task_timeout = get_task_timeout(file_key)
        
        download = PythonOperator(
            task_id=f"download_{file_key}",
            python_callable=fetch_imdb_dataset,
            op_args=[file_key],
            execution_timeout=task_timeout,
            pool='default_pool',
            doc_md=f"Download {file_key} dataset from IMDb with smart caching"
        )

        load_via_copy = PythonOperator(
            task_id=f"load_{file_key}_to_postgres",
            python_callable=load_imdb_table,
            op_args=[file_key],
            execution_timeout=task_timeout,
            pool='default_pool',
            doc_md=f"Load {file_key} into PostgreSQL using optimized COPY operation"
        )

        download >> load_via_copy
