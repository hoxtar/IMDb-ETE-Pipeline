"""
IMDb Data Pipeline with Cosmos DAG
=================================

A production-ready Airflow DAG that maintains an up-to-date copy of IMDb datasets
and transforms them using dbt via Astronomer Cosmos.

Features:
    - Smart downloading with Last-Modified checks
    - Efficient PostgreSQL bulk loading via COPY
    - dbt transformations via Cosmos
    - Robust error handling with retries
    - Automatic cleanup of temporary files

Pipeline Flow:
    1. Download IMDb datasets
    2. Load raw data into PostgreSQL
    3. Transform data using dbt models via Cosmos

Author: Andrea Usai
"""
# 1. Standard library imports (alphabetical)
import os
import gzip
import random
import requests
import shutil
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

# 2. Third-party imports
import psycopg2

# 3. Airflow imports (grouped by module)
from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

# 4. Framework-specific imports (Cosmos)
# Ensure all necessary Cosmos components are imported globally.
# These were previously commented out, leading to NameErrors (e.g., 'ProfileConfig' not defined)
# when the DAG was parsed. Uncommenting them makes them available throughout the script.
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import LoadMode

# Initialize Airflow logger
log = LoggingMixin().log

# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
# Ensure directories exist
os.makedirs(AIRFLOW_HOME, exist_ok=True)

DEFAULT_DOWNLOAD_DIR = os.path.join(AIRFLOW_HOME, 'data', 'files')
DEFAULT_SCHEMA_DIR   = os.path.join(AIRFLOW_HOME, 'schemas')

DOWNLOAD_DIR = os.environ.get('IMDB_DOWNLOAD_DIR', DEFAULT_DOWNLOAD_DIR)
SCHEMA_DIR = os.environ.get('IMDB_SCHEMA_DIR', DEFAULT_SCHEMA_DIR)
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID', 'db_conn')

# dbt project configuration
DBT_PROJECT_PATH = Path(__file__).parent / "dbt"
DBT_PROFILES_PATH = Path(__file__).parent / "dbt" / "profiles"

# Task-specific timeout configurations
TASK_TIMEOUTS = {
    'title_ratings': timedelta(minutes=15),
    'title_episode': timedelta(minutes=20),
    'title_akas': timedelta(minutes=30),
    'title_basics': timedelta(minutes=45),
    'name_basics': timedelta(minutes=45),
    'title_crew': timedelta(hours=1, minutes=30),
    'title_principals': timedelta(hours=2),
}

DEFAULT_TASK_TIMEOUT = timedelta(hours=1)

def get_task_timeout(file_key: str) -> timedelta:
    """Get appropriate timeout for specific task based on historical performance"""
    return TASK_TIMEOUTS.get(file_key, DEFAULT_TASK_TIMEOUT)

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
# 2. Helper Functions (Keep existing functions)
# ----------------------------------------------------------------------------

def setup_table_schema(cur: psycopg2.extensions.cursor, schema_file_path: str, table_name: str):
    log.info(f"[SCHEMA] Creating table if not exists: {table_name}")
    try:
        with open(schema_file_path, 'r', encoding='utf-8') as schema_file:
            create_sql = schema_file.read()
            if not create_sql.strip().lower().startswith("create table"):
                log.error(f"[SECURITY] Invalid SQL detected in schema file: {schema_file_path}")
                raise ValueError("Invalid SQL content in schema file.")
    except Exception as e:
        log.error(f"[ERROR] Error reading schema file: {e}")
        raise
    else:
        cur.execute(create_sql)

def fetch_imdb_dataset(file_key: str) -> None:
    """Downloads and caches an IMDb dataset file if newer version exists."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = IMDB_FILES[file_key]
    file_url = BASE_URL + filename
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    # Retry configuration
    max_retries = 3
    retry_count = 0
    base_delay = 2  # Base delay in seconds
    max_delay = 60  # Maximum delay cap in seconds

    while retry_count < max_retries:
        try:
            log.info(f"[CHECK] Checking Last-Modified for {filename}...")
            head_resp = requests.head(file_url, timeout=30)
            head_resp.raise_for_status()
            remote_last_modified_str = head_resp.headers.get('Last-Modified')

            if os.path.exists(filepath):
                local_timestamp = os.path.getmtime(filepath)
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

            log.info(f"[DOWNLOAD] Fetching {filename} from {file_url}...")
            dl_resp = requests.get(file_url, timeout=120)
            dl_resp.raise_for_status()

            with open(filepath, 'wb') as f:
                f.write(dl_resp.content)
            size_kb = os.path.getsize(filepath) / 1024
            log.info(f"[DONE] Saved {filename} -> {filepath} ({size_kb:.2f} KB)")
            return
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            retry_count += 1
            if retry_count == max_retries:
                log.error(f"[ERROR] Final attempt to download {filename} after {max_retries} retries")
                raise
            # Calculate delay with exponential backoff + jitter
            exponential_delay = base_delay * (2 ** (retry_count - 1))
            jitter = random.uniform(0, exponential_delay * 0.1)  # 10% jitter
            actual_delay = min(exponential_delay + jitter, max_delay)
            
            log.warning(f"[RETRY] Attempt {retry_count} of {max_retries}, waiting {actual_delay:.1f}s")
            time.sleep(actual_delay)
    
        except requests.exceptions.HTTPError as he:
            status_code = he.response.status_code if he.response else 'Unknown'
            log.error(f"[ERROR] HTTP {status_code} error for {filename}: {he}")
            raise

def load_imdb_table(file_key: str) -> None:
    """Loads an IMDb dataset into PostgreSQL efficiently with enhanced monitoring."""
    try:
        context = get_current_context()
        task_instance = context.get('task_instance')
    except:
        task_instance = None
    
    def update_heartbeat(message: str):
        if task_instance:
            log.info(f"[HEARTBEAT] {message}")
            task_instance.refresh_from_db()
        else:
            log.info(f"[PROGRESS] {message}")
    
    table_name = f"imdb_{file_key}"
    config = TABLE_CONFIGS[file_key]
    columns = config['columns']
    
    start_time = time.time()
    update_heartbeat(f"Starting load process for {table_name}")
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
    except AirflowNotFoundException:
        raise RuntimeError(f"Connection {POSTGRES_CONN_ID} not found. Please check your Airflow connections.")

    cur = conn.cursor()

    schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
    gz_path = os.path.join(DOWNLOAD_DIR, IMDB_FILES[file_key])
    uncompressed_tsv_path = os.path.join(DOWNLOAD_DIR, f"{file_key}.tsv")

    try:
        update_heartbeat("Creating table schema if not exists")
        setup_table_schema(cur, schema_file_path, table_name)
        conn.commit()

        if not os.path.exists(gz_path) or os.path.getsize(gz_path) < 1000:
            log.warning(f"[SKIP] {gz_path} is missing or too small.")
            return

        file_size_mb = os.path.getsize(gz_path) / (1024 * 1024)
        update_heartbeat(f"Processing file {gz_path} ({file_size_mb:.1f} MB)")

        update_heartbeat("Truncating existing data for clean reload")
        log.info(f"[TRUNCATE] Clearing table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()

        update_heartbeat("Decompressing data file")
        log.info(f"[DECOMPRESS] Creating temporary TSV: {uncompressed_tsv_path}")
        with gzip.open(gz_path, 'rt', encoding='utf-8') as gzfile, open(uncompressed_tsv_path, 'w', encoding='utf-8') as tsvfile:
            header = next(gzfile)
            log.info(f"[HEADER] Skipped: {header.strip()}")
            shutil.copyfileobj(gzfile, tsvfile)

        update_heartbeat("Counting data rows for validation")
        with open(uncompressed_tsv_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        log.info(f"[INFO] {uncompressed_tsv_path} has {total_lines:,} data lines (excluding header).")

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

        update_heartbeat("Validating data integrity")
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        db_count = cur.fetchone()[0]
        log.info(f"[VERIFY] DB row count: {db_count:,}. File lines: {total_lines:,}.")
        
        if db_count != total_lines:
            log.warning(
                f"[MISMATCH] Inserted {db_count:,} rows but file had {total_lines:,} lines."
            )

        total_time = time.time() - start_time
        rows_per_second = db_count / total_time if total_time > 0 else 0
        update_heartbeat(f"Load completed: {db_count:,} rows in {total_time:.1f}s ({rows_per_second:.0f} rows/s)")

    except Exception as e:
        log.error(f"[ERROR] Loading data via COPY into {table_name} failed: {e}")
        raise
    finally:
        if os.path.exists(uncompressed_tsv_path):
            os.remove(uncompressed_tsv_path)
            log.info(f"[CLEANUP] Removing temporary TSV file: {uncompressed_tsv_path}")

        cur.close()
        conn.close()

# ----------------------------------------------------------------------------
# 4. DAG Definition with Cosmos Integration
# ----------------------------------------------------------------------------

# dbt configuration for Cosmos
# profile_config = ProfileConfig(
#     profile_name="my_imdb_project",
#     target_name="dev",
#     profile_mapping=PostgresUserPasswordProfileMapping(
#         conn_id=POSTGRES_CONN_ID,
#         profile_args={"schema": "public"},
#     ),
# )

# Task-specific retry configurations
RETRY_CONFIGS = {
    'title_principals': {
        'retries': 5,
        'retry_delay': timedelta(minutes=10),
        'max_retry_delay': timedelta(hours=1),
    },
    'title_basics': {
        'retries': 4,
        'retry_delay': timedelta(minutes=7),
        'max_retry_delay': timedelta(minutes=45),
    },
    # Default for other tasks
    'default': {
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'max_retry_delay': timedelta(minutes=30),
    }
}

def get_retry_config(file_key: str) -> dict:
    """Get retry configuration for specific task"""
    return RETRY_CONFIGS.get(file_key, RETRY_CONFIGS['default'])

# Custom retry decorator with different timing strategies
def retry_with_custom_timing(strategy='exponential', max_retries=3, base_delay=1):
    """
    Custom retry decorator with different timing strategies
    
    Strategies:
    - 'fixed': Always same delay
    - 'linear': Linearly increasing delay  
    - 'exponential': Exponentially increasing delay
    - 'fibonacci': Fibonacci sequence delays
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise
                    
                    if strategy == 'fixed':
                        delay = base_delay
                    elif strategy == 'linear':
                        delay = base_delay * (attempt + 1)
                    elif strategy == 'exponential':
                        delay = base_delay * (2 ** attempt)
                    elif strategy == 'fibonacci':
                        fib = [1, 1] + [0] * attempt
                        for i in range(2, len(fib)):
                            fib[i] = fib[i-1] + fib[i-2]
                        delay = base_delay * fib[attempt]
                    
                    log.info(f"Retry {attempt + 1}/{max_retries} in {delay}s using {strategy} strategy")
                    time.sleep(delay)
            
        return wrapper
    return decorator

# Example usage:
# @retry_with_custom_timing(strategy='fibonacci', max_retries=4, base_delay=2)
# def your_function():
#     pass

# Create the main DAG
dag = DAG(
    dag_id="imdb_cosmos_pipeline",
    default_args={
        'start_date': datetime(2025, 1, 1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
        'execution_timeout': DEFAULT_TASK_TIMEOUT,
        'owner': 'data_engineering_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'sla': timedelta(hours=3),
    },
    schedule="@daily",
    catchup=False,
    description="IMDb data pipeline with dbt transformations via Cosmos",
    tags=['imdb', 'etl', 'dbt', 'cosmos'],
)

def create_dbt_task_group(load_tasks):
    profile_config = ProfileConfig(
        profile_name="my_imdb_project",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "public"},
        ),
    )

    # ProjectConfig defines the dbt project to be used.
    # The manifest_path is crucial when using LoadMode.DBT_MANIFEST.
    # It tells Cosmos where to find the pre-compiled dbt manifest file (manifest.json),
    # which is generated by running `dbt parse` or `dbt compile` in the dbt project.
    # Using the manifest significantly speeds up DAG parsing.
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json", # Added for LoadMode.DBT_MANIFEST
        models_relative_path="models",
        seeds_relative_path="seeds",
        snapshots_relative_path="snapshots",
    )

    # To address potential DagBag import timeouts, especially with complex dbt projects,
    # we switched the load_method to LoadMode.DBT_MANIFEST.
    # This method relies on a pre-compiled dbt manifest.json file, which significantly
    # speeds up DAG parsing by avoiding the need for Cosmos to parse the entire dbt project structure
    # at runtime. This requires that `dbt parse` or `dbt compile` is run before Airflow
    # tries to parse this DAG, ensuring the manifest.json is up-to-date.
    render_config = RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=["path:models/staging/imdb", "path:models/intermediate/imdb", "path:models/marts/imdb"],
    )

    tg = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
        render_config=render_config,
        
        # The 'full_refresh' argument is not a universally valid parameter for all operators
        # that Cosmos might generate from dbt models (e.g., DbtTestLocalOperator).
        # Its presence was causing an AirflowException: "Invalid arguments were passed...".
        # Removing it allows Cosmos to correctly pass only valid arguments to its underlying operators.
        operator_args={
            "install_deps": True,
            # "full_refresh": False, # Removed to prevent errors with operators like DbtTestLocalOperator
        },
        default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    )
    load_tasks >> tg
    return tg

with dag:
    # Create download and load tasks for each IMDb file
    download_tasks = []
    load_tasks = []
    
    for file_key in IMDB_FILES:
        task_timeout = get_task_timeout(file_key)
        retry_config = get_retry_config(file_key)
        
        download_task = PythonOperator(
            task_id=f"download_{file_key}",
            python_callable=fetch_imdb_dataset,
            op_args=[file_key],
            execution_timeout=task_timeout,
            retries=retry_config['retries'],
            retry_delay=retry_config['retry_delay'],
            max_retry_delay=retry_config['max_retry_delay'],
            pool='default_pool',
            doc_md=f"Download {file_key} dataset from IMDb with smart caching"
        )
        
        load_task = PythonOperator(
            task_id=f"load_{file_key}_to_postgres",
            python_callable=load_imdb_table,
            op_args=[file_key],
            execution_timeout=task_timeout,
            retries=retry_config['retries'],
            retry_delay=retry_config['retry_delay'],
            max_retry_delay=retry_config['max_retry_delay'],
            pool='default_pool',
            doc_md=f"Load {file_key} into PostgreSQL using optimized COPY operation"
        )
        download_task >> load_task
        download_tasks.append(download_task)
        load_tasks.append(load_task)      # dbt transformation task group using Cosmos

    create_dbt_task_group(load_tasks)
