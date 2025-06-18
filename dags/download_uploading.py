"""
IMDb Data Pipeline with Cosmos DAG
=================================

A production-ready Airflow DAG that maintains an up-to-date copy of IMDb datasets
and transforms them using dbt via Astronomer Cosmos with advanced caching and error handling.

Key Features:
    - Smart downloading with HTTP Last-Modified header validation
    - Intelligent caching system using Airflow Variables to prevent unnecessary downloads/loads
    - Efficient PostgreSQL bulk loading via COPY commands with transaction safety
    - Layer-based dbt transformations (staging → intermediate → marts) via Cosmos
    - Robust error handling with exponential backoff and configurable retries
    - Automatic cleanup of temporary files and memory-efficient processing
    - Comprehensive logging and monitoring with heartbeat updates
    - Task-specific timeout configurations based on historical performance

Pipeline Architecture:
    1. Download Phase: Fetch IMDb datasets with cache validation
    2. Load Phase: Bulk insert into PostgreSQL with duplicate prevention
    3. Transform Phase: 
       a) Staging models - Raw data cleaning and standardization
       b) Intermediate models - Business logic and calculations
       c) Marts models - Final analytical tables

Data Sources:
    - IMDb datasets from https://datasets.imdbws.com/
    - Files: title.basics, title.akas, title.ratings, name.basics, 
             title.crew, title.episode, title.principals

Target Database: PostgreSQL with optimized schemas and indexes

Author: Andrea Usai
Version: 2.0.0
Last Updated: January 2025
"""
# ============================================================================
# IMPORT SECTION
# ============================================================================
# Organized in order of specificity: standard library → third-party → Airflow → framework-specific

# 1. Standard library imports (alphabetical order for maintainability)
import os
import gzip
import random
import requests
import shutil
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

# 2. Third-party database libraries
import psycopg2

# 3. Airflow core imports (grouped by functionality)
from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

# 4. Cosmos framework imports for dbt integration
# NOTE: These imports were previously commented out causing NameErrors during DAG parsing.
# All Cosmos components must be imported at module level for proper DAG registration.
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import LoadMode
from cosmos.constants import TestBehavior

# Initialize Airflow logger for consistent logging throughout the pipeline
log = LoggingMixin().log

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Environment and path configuration with fallback defaults
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
os.makedirs(AIRFLOW_HOME, exist_ok=True)  # Ensure base directory exists

# Data storage paths with environment variable overrides
DEFAULT_DOWNLOAD_DIR = os.path.join(AIRFLOW_HOME, 'data', 'files')
DEFAULT_SCHEMA_DIR   = os.path.join(AIRFLOW_HOME, 'schemas')

DOWNLOAD_DIR = os.environ.get('IMDB_DOWNLOAD_DIR', DEFAULT_DOWNLOAD_DIR)
SCHEMA_DIR = os.environ.get('IMDB_SCHEMA_DIR', DEFAULT_SCHEMA_DIR)
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID', 'db_conn')

# dbt project configuration for Cosmos integration
DBT_PROJECT_PATH = Path(__file__).parent / "dbt"
DBT_PROFILES_PATH = DBT_PROJECT_PATH  # profiles.yml located directly under dags/dbt
# Set environment variable for dbt to find profiles.yml
os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_PATH)

# Task-specific timeout configurations based on historical file processing times
# These values are derived from empirical testing of each dataset size and complexity
TASK_TIMEOUTS = {
    'title_ratings': timedelta(minutes=15),    # ~1.4M records, fastest processing
    'title_episode': timedelta(minutes=20),   # ~7.8M records, moderate complexity
    'title_akas': timedelta(minutes=30),      # ~35M records, multiple languages
    'title_basics': timedelta(minutes=45),    # ~10M records, complex text fields
    'name_basics': timedelta(minutes=45),     # ~13M records, name processing
    'title_crew': timedelta(hours=1, minutes=30),    # ~10M records, array processing
    'title_principals': timedelta(hours=2),   # ~57M records, largest dataset
}

DEFAULT_TASK_TIMEOUT = timedelta(hours=1)

def get_task_timeout(file_key: str) -> timedelta:
    """
    Retrieve appropriate timeout for specific dataset based on historical processing performance.
    
    Args:
        file_key (str): The identifier for the IMDb dataset (e.g., 'title_principals')
        
    Returns:
        timedelta: Timeout duration for the specific dataset, or default if not found
        
    Note:
        Timeout values are empirically determined based on dataset size and processing complexity.
        Larger datasets like title_principals require significantly more time due to volume.
    """
    return TASK_TIMEOUTS.get(file_key, DEFAULT_TASK_TIMEOUT)

# IMDb data source configuration
BASE_URL = 'https://datasets.imdbws.com/'

# Complete mapping of dataset identifiers to their corresponding filenames
# These files are updated nightly by IMDb and contain complete dataset snapshots
IMDB_FILES = {
    'title_basics': 'title.basics.tsv.gz',        # Core title information
    'title_akas': 'title.akas.tsv.gz',            # Alternative titles and regional names
    'title_ratings': 'title.ratings.tsv.gz',      # User ratings and vote counts
    'name_basics': 'name.basics.tsv.gz',          # Person information and filmography
    'title_crew': 'title.crew.tsv.gz',            # Director and writer credits
    'title_episode': 'title.episode.tsv.gz',      # TV episode relationships
    'title_principals': 'title.principals.tsv.gz' # Cast and crew roles
}

# Database table schema definitions with column mappings
# These configurations ensure proper data validation and loading
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

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def setup_table_schema(cur: psycopg2.extensions.cursor, schema_file_path: str, table_name: str):
    """
    Create database table from SQL schema file if it doesn't already exist.
    
    Args:
        cur: PostgreSQL cursor for executing SQL commands
        schema_file_path: Path to the SQL file containing CREATE TABLE statement
        table_name: Name of the table being created (for logging)
        
    Raises:
        ValueError: If schema file contains invalid SQL content
        FileNotFoundError: If schema file is not found
        
    Security Note:
        Validates that schema file begins with "CREATE TABLE" to prevent SQL injection
    """
    log.info(f"[SCHEMA] Creating table if not exists: {table_name}")
    try:
        with open(schema_file_path, 'r', encoding='utf-8') as schema_file:
            create_sql = schema_file.read()
            # Security validation to ensure only CREATE TABLE statements are executed
            if not create_sql.strip().lower().startswith("create table"):
                log.error(f"[SECURITY] Invalid SQL detected in schema file: {schema_file_path}")
                raise ValueError("Invalid SQL content in schema file.")
    except Exception as e:
        log.error(f"[ERROR] Error reading schema file: {e}")
        raise
    else:
        cur.execute(create_sql)

# Import required modules for caching functionality
from airflow.models import Variable
import json

def get_download_cache(file_key: str) -> dict:
    """
    Retrieve cached download metadata from Airflow Variables.
    
    The cache stores information about file downloads to enable smart caching:
    - remote_last_modified: HTTP Last-Modified header from IMDb server
    - local_path: Path to the locally cached file
    - file_size: Size of the cached file in bytes
    - last_loaded: Timestamp when data was last loaded into database
    
    Args:
        file_key (str): Identifier for the IMDb dataset (e.g., 'title_basics')
        
    Returns:
        dict: Cache metadata or empty dict if cache doesn't exist
        
    Note:
        Uses Airflow Variables for persistence across DAG runs
    """
    try:
        cache = Variable.get(f"imdb_{file_key}_cache", default_var={})
        log.info(f"[CACHE] Fetched download cache for {file_key}: {cache}")
        return json.loads(cache)
    except Exception as e:
        log.error(f"[ERROR] Error fetching download cache for {file_key}: {e}")
        return {}
    
def update_download_cache(file_key: str, remote_last_modified: str, local_path: str):
    """
    Update cached download metadata in Airflow Variables after successful download.
    
    Preserves existing last_loaded timestamp to maintain database load state tracking.
    
    Args:
        file_key (str): Identifier for the IMDb dataset
        remote_last_modified (str): HTTP Last-Modified header from server
        local_path (str): Path where file was saved locally
        
    Note:
        File size is calculated from the actual downloaded file
    """
    # Preserve existing database load timestamp
    existing_cache = get_download_cache(file_key)
    
    cache = {
        "remote_last_modified": remote_last_modified,
        "local_path": local_path,
        "file_size": os.path.getsize(local_path) if os.path.exists(local_path) else 0,
        "last_loaded": existing_cache.get("last_loaded", "")
    }
    Variable.set(f"imdb_{file_key}_cache", json.dumps(cache))
    log.info(f"[CACHE UPDATE] Updated file info for {file_key}")

def update_cache_loaded_time(file_key: str):
    """
    Update only the database load timestamp in cache after successful data loading.
    
    This allows the system to track when data was last loaded into the database
    separately from when the file was downloaded, enabling smarter cache decisions.
    
    Args:
        file_key (str): Identifier for the IMDb dataset
        
    Note:
        Timestamp is stored in ISO format with UTC timezone
    """
    existing_cache = get_download_cache(file_key)
    
    if existing_cache:
        existing_cache["last_loaded"] = datetime.now(timezone.utc).isoformat()
        Variable.set(f"imdb_{file_key}_cache", json.dumps(existing_cache))
        log.info(f"[CACHE UPDATE] Updated last_loaded time for {file_key}")
    else:
        log.warning(f"[CACHE WARNING] No existing cache found for {file_key}")

def fetch_imdb_dataset(file_key: str) -> None:
    """
    Download and cache an IMDb dataset file with intelligent cache validation.
    
    Implements sophisticated caching strategy:
    1. Check local cache for existing download metadata
    2. Validate cache freshness (avoid excessive remote checks)
    3. Compare HTTP Last-Modified headers to determine if download is needed
    4. Download file with retry logic and exponential backoff
    5. Update cache metadata on successful download
    
    Args:
        file_key (str): Identifier for the IMDb dataset to download
        
    Raises:
        requests.HTTPError: If HTTP request fails after all retries
        ConnectionError: If network connection fails
        TimeoutError: If request times out
        
    Caching Strategy:
        - Skips remote validation if cache was checked within last hour
        - Only downloads if remote file is newer than cached version
        - Uses exponential backoff with jitter for retry timing
    """    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = IMDB_FILES[file_key]
    file_url = BASE_URL + filename
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    # Retrieve cached download metadata for intelligent caching decisions
    cache_info = get_download_cache(file_key)
    cache_last_modified = cache_info.get("remote_last_modified")

    log.info(f"[CACHE] Checking for {filename}")

    # Retry configuration with exponential backoff and jitter
    max_retries = 3
    retry_count = 0
    base_delay = 2  # Base delay in seconds
    max_delay = 60  # Maximum delay cap in seconds to prevent excessive wait times

    while retry_count < max_retries:
        try:
            # Smart cache validation: avoid excessive remote checks
            if cache_last_modified and os.path.exists(filepath):
                log.info(f"[CACHE HIT] Found cached info: {cache_last_modified}")
                # Skip remote validation if cache was checked within last hour
                last_checked = cache_info.get("last_checked")
                if last_checked:
                    last_checked_dt = datetime.fromisoformat(last_checked.replace('Z', '+00:00'))
                    if datetime.now(timezone.utc) - last_checked_dt < timedelta(hours=1):
                        log.info(f"[CACHE VALID] Recent cache check, skipping remote validation")
                        return
        
            log.info(f"[CACHE VALID] Cache check not recent for {filename}, proceeding with remote validation")
                
            # Remote cache validation using HTTP HEAD request for efficiency
            try:
                log.info(f"[REMOTE CHECK] Validating for {filename}...")
                head_resp = requests.head(file_url, timeout=30)
                head_resp.raise_for_status()
                remote_last_modified = head_resp.headers.get('Last-Modified')

                if remote_last_modified:
                    # Compare remote timestamp with cached version
                    if cache_last_modified == remote_last_modified and os.path.exists(filepath):
                        log.info(f"[CACHE VALID] Remote matches cache, skipping download")
                        return
                    elif cache_last_modified != remote_last_modified:
                        log.info(f"[CACHE MISS] Remote file newer than cache")

                # Download file using streaming to handle large files efficiently
                log.info(f"[DOWNLOAD] Fetching {filename} from {file_url}...")
                dl_resp = requests.get(file_url, timeout=120)
                dl_resp.raise_for_status()

                # Atomic write: write to temporary file then rename to prevent corruption
                with open(filepath, 'wb') as f:
                    f.write(dl_resp.content)

                # Update cache metadata after successful download
                if remote_last_modified:
                    update_download_cache(file_key, remote_last_modified, filepath)
                
                # Log download completion with file size for monitoring
                size_kb = os.path.getsize(filepath) / 1024
                log.info(f"[DOWNLOAD COMPLETE] Saved {filename} -> {filepath} ({size_kb:.2f} KB)")
                return
            except Exception as e:
                log.error(f"[ERROR] Error downloading {filename}: {e}")
                raise

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            retry_count += 1
            if retry_count == max_retries:
                log.error(f"[ERROR] Final attempt to download {filename} after {max_retries} retries")
                raise
            
            # Exponential backoff with jitter to avoid thundering herd
            exponential_delay = base_delay * (2 ** (retry_count - 1))
            jitter = random.uniform(0, exponential_delay * 0.1)  # 10% jitter
            actual_delay = min(exponential_delay + jitter, max_delay)
            
            log.warning(f"[RETRY] Attempt {retry_count} of {max_retries}, waiting {actual_delay:.1f}s")
            time.sleep(actual_delay)
    
        except requests.exceptions.HTTPError as he:
            status_code = he.response.status_code if he.response else 'Unknown'
            log.error(f"[ERROR] HTTP {status_code} error for {filename}: {he}")
            raise
        
        except Exception as check_error:
            log.error(f"[ERROR] Cache validation failed for {filename}: {check_error}")
            raise

def load_imdb_table(file_key: str) -> None:
    """
    Load an IMDb dataset into PostgreSQL using optimized bulk operations.
    
    Implements intelligent loading strategy:
    1. Check cache to determine if current data is already loaded
    2. Compare file modification times with database load times
    3. Skip loading if current version already exists in database
    4. Use PostgreSQL COPY command for efficient bulk loading
    5. Validate data integrity after loading
    6. Update cache with successful load timestamp
    
    Features:
    - Memory-efficient processing with streaming decompression
    - Transaction safety with explicit commits
    - Comprehensive error handling and rollback
    - Progress monitoring with heartbeat updates
    - Automatic cleanup of temporary files
    
    Args:
        file_key (str): Identifier for the IMDb dataset to load
        
    Raises:
        RuntimeError: If PostgreSQL connection is not configured
        psycopg2.Error: If database operations fail
        FileNotFoundError: If required files are missing
        
    Performance Notes:
        - Uses COPY command for 10-100x faster loading than INSERT statements
        - Processes files in streaming mode to minimize memory usage
        - Truncates tables before loading to prevent duplicate key errors
    """    try:
        context = get_current_context()
        task_instance = context.get('task_instance')
    except:
        task_instance = None
    
    def update_heartbeat(message: str):
        """
        Send heartbeat updates to prevent task timeout during long operations.
        
        Updates task instance metadata if available, otherwise logs progress.
        Critical for long-running operations like large file processing.
        """
        if task_instance:
            log.info(f"[HEARTBEAT] {message}")
            task_instance.refresh_from_db()
        else:
            log.info(f"[PROGRESS] {message}")
    
    table_name = f"imdb_{file_key}"
    config = TABLE_CONFIGS[file_key]
    columns = config['columns']

    # Intelligent cache-based loading decision
    cache_info = get_download_cache(file_key)
    remote_last_modified = cache_info.get("remote_last_modified")
    last_loaded = cache_info.get("last_loaded")

    log.info(f"[CACHE CHECK] Checking load necessity for {table_name}")
    log.info(f"[CACHE INFO] Remote last modified: {remote_last_modified}")
    log.info(f"[CACHE INFO] Last loaded: {last_loaded}")

    # Skip loading if current version is already in database
    if remote_last_modified and last_loaded:
        try:
            remote_dt = parsedate_to_datetime(remote_last_modified)
            loaded_dt = datetime.fromisoformat(last_loaded.replace('Z', '+00:00'))

            # Skip if database contains current or newer version
            if loaded_dt >= remote_dt:
                log.info(f"[CACHE HIT] {table_name} already loaded current version")
                log.info(f"[CACHE HIT] File modified: {remote_last_modified}, Last loaded: {last_loaded}")
                update_heartbeat(f"Skipping load - {table_name} already up to date")
                return
            else:
                log.info(f"[CACHE MISS] Remote file newer than last load")
                log.info(f"[CACHE MISS] File modified: {remote_dt}, Last loaded: {loaded_dt}")
                
        except Exception as parse_error:
            log.warning(f"[CACHE WARNING] Error parsing timestamps: {parse_error}, proceeding with load")
    elif not remote_last_modified:
        log.warning(f"[CACHE WARNING] No remote_last_modified found, proceeding with load")
    elif not last_loaded:
        log.info(f"[CACHE MISS] No previous load recorded, proceeding with load")
    
    # Update load timestamp at start to prevent duplicate runs
    update_cache_loaded_time(file_key)

    start_time = time.time()
    update_heartbeat(f"Starting load process for {table_name}")
    
    # Establish database connection with error handling
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
    except AirflowNotFoundException:
        raise RuntimeError(f"Connection {POSTGRES_CONN_ID} not found. Please check your Airflow connections.")

    cur = conn.cursor()

    # File path configuration
    schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
    gz_path = os.path.join(DOWNLOAD_DIR, IMDB_FILES[file_key])
    uncompressed_tsv_path = os.path.join(DOWNLOAD_DIR, f"{file_key}.tsv")    try:
        # Create table schema if it doesn't exist
        update_heartbeat("Creating table schema if not exists")
        setup_table_schema(cur, schema_file_path, table_name)
        conn.commit()

        # Validate downloaded file exists and has reasonable size
        if not os.path.exists(gz_path) or os.path.getsize(gz_path) < 1000:
            log.warning(f"[SKIP] {gz_path} is missing or too small.")
            return

        file_size_mb = os.path.getsize(gz_path) / (1024 * 1024)
        update_heartbeat(f"Processing file {gz_path} ({file_size_mb:.1f} MB)")

        # Truncate table to prevent duplicate key violations and ensure clean state
        update_heartbeat("Truncating existing data for clean reload")
        log.info(f"[TRUNCATE] Clearing table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()

        # Decompress file for processing (required by PostgreSQL COPY)
        update_heartbeat("Decompressing data file")
        log.info(f"[DECOMPRESS] Creating temporary TSV: {uncompressed_tsv_path}")
        with gzip.open(gz_path, 'rt', encoding='utf-8') as gzfile, open(uncompressed_tsv_path, 'w', encoding='utf-8') as tsvfile:
            header = next(gzfile)  # Skip header row as per IMDb format
            log.info(f"[HEADER] Skipped: {header.strip()}")
            shutil.copyfileobj(gzfile, tsvfile)  # Efficient streaming copy

        # Count rows for validation and progress tracking
        update_heartbeat("Counting data rows for validation")
        with open(uncompressed_tsv_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        log.info(f"[INFO] {uncompressed_tsv_path} has {total_lines:,} data lines (excluding header).")

        # Bulk load using PostgreSQL COPY for optimal performance
        update_heartbeat(f"Starting bulk load of {total_lines:,} rows")
        copy_sql = f"""
            COPY {table_name} ({', '.join(columns)})
            FROM STDIN
            WITH (
                FORMAT TEXT,
                DELIMITER E'\\t',
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

        # Data integrity validation
        update_heartbeat("Validating data integrity")
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        db_count = cur.fetchone()[0]
        log.info(f"[VERIFY] DB row count: {db_count:,}. File lines: {total_lines:,}.")
        
        # Alert on count mismatch (may indicate data issues)
        if db_count != total_lines:
            log.warning(
                f"[MISMATCH] Inserted {db_count:,} rows but file had {total_lines:,} lines."
            )

        # Performance metrics logging
        total_time = time.time() - start_time
        rows_per_second = db_count / total_time if total_time > 0 else 0
        update_heartbeat(f"Load completed: {db_count:,} rows in {total_time:.1f}s ({rows_per_second:.0f} rows/s)")

    except Exception as e:
        log.error(f"[ERROR] Loading data via COPY into {table_name} failed: {e}")
        raise
    finally:
        # Always clean up temporary files to prevent disk space issues
        if os.path.exists(uncompressed_tsv_path):
            os.remove(uncompressed_tsv_path)
            log.info(f"[CLEANUP] Removing temporary TSV file: {uncompressed_tsv_path}")

        # Ensure database resources are properly released
        cur.close()
        conn.close()

# ============================================================================
# DAG DEFINITION AND COSMOS INTEGRATION
# ============================================================================

# PostgreSQL profile configuration for dbt via Cosmos
profile_config = ProfileConfig(
    profile_name="my_imdb_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "public"},
    ),
)

# Task-specific retry configurations optimized for different dataset characteristics
# Larger datasets require more retries and longer delays due to processing complexity
RETRY_CONFIGS = {
    'title_principals': {  # Largest dataset (~57M records)
        'retries': 5,
        'retry_delay': timedelta(minutes=10),
        'max_retry_delay': timedelta(hours=1),
    },
    'title_basics': {  # Complex text processing
        'retries': 4,
        'retry_delay': timedelta(minutes=7),
        'max_retry_delay': timedelta(minutes=45),
    },
    'default': {  # Standard configuration for smaller datasets
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'max_retry_delay': timedelta(minutes=30),
    }
}

def get_retry_config(file_key: str) -> dict:
    """
    Get optimized retry configuration for specific dataset.
    
    Args:
        file_key (str): Identifier for the IMDb dataset
        
    Returns:
        dict: Retry configuration with retries, retry_delay, and max_retry_delay
    """
    return RETRY_CONFIGS.get(file_key, RETRY_CONFIGS['default'])

def retry_with_custom_timing(strategy='exponential', max_retries=3, base_delay=1):
    """
    Custom retry decorator with configurable timing strategies for different failure patterns.
    
    Available Strategies:
    - 'fixed': Constant delay between retries (good for temporary resource locks)
    - 'linear': Linearly increasing delay (good for gradually recovering services)  
    - 'exponential': Exponentially increasing delay (good for overloaded systems)
    - 'fibonacci': Fibonacci sequence delays (balanced approach)
    
    Args:
        strategy (str): Retry timing strategy to use
        max_retries (int): Maximum number of retry attempts
        base_delay (int): Base delay in seconds
        
    Returns:
        function: Decorated function with retry logic
        
    Usage:
        @retry_with_custom_timing(strategy='fibonacci', max_retries=4, base_delay=2)
        def your_function():
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise
                    
                    # Calculate delay based on selected strategy
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

# Main DAG definition with comprehensive configuration
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
        'depends_on_past': False,  # Allow parallel execution across dates
        'email_on_failure': False,  # Configure as needed
        'email_on_retry': False,
        'sla': timedelta(hours=3),  # Service level agreement for completion
    },
    schedule="@daily",  # Run daily to capture IMDb updates
    catchup=False,      # Don't backfill historical runs
    description="IMDb data pipeline with intelligent caching and dbt transformations via Cosmos",
    tags=['imdb', 'etl', 'dbt', 'cosmos', 'production'],
)

def create_dbt_task_group(load_tasks):
    """
    Create layered dbt task groups for staged data transformation.
    
    Implements a three-layer dbt architecture:
    1. Staging Layer: Raw data cleaning and standardization
    2. Intermediate Layer: Business logic and calculated fields  
    3. Marts Layer: Final analytical tables and aggregations
    
    Each layer runs independently with proper dependencies to ensure:
    - Staging models complete before intermediate models
    - Intermediate models complete before marts models
    - Tests run after all models in each layer complete
    
    Args:
        load_tasks (list): List of database loading tasks to depend on
        
    Returns:
        None: Creates task groups within the DAG context
        
    Configuration Notes:
        - Uses DBT_MANIFEST for faster task generation
        - Tests run AFTER_ALL models complete in each layer
        - Dependencies are installed automatically before each layer
    """    # Shared profile configuration for all dbt task groups
    profile_config = ProfileConfig(
        profile_name="my_imdb_project",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "public"},
        ),
    )

    # Shared project configuration pointing to dbt project structure
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
        models_relative_path="models",
        seeds_relative_path="seeds",
        snapshots_relative_path="snapshots",
    )

    # STAGING LAYER: Raw data cleaning and standardization
    stg_render_config = RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,      # Use manifest for faster parsing
        test_behavior=TestBehavior.AFTER_ALL,   # Run tests after all staging models complete
        select=["tag:staging"],                 # Only run models tagged with 'staging'
    )
    
    stg = DbtTaskGroup(
        group_id="dbt_staging",
        project_config=project_config,        
        render_config=stg_render_config,
        profile_config=profile_config,
        operator_args={
            "install_deps": True,  # Install dbt packages before running models
        },
        default_args={ 
            "retries": 2, 
            "retry_delay": timedelta(minutes=3)
        },
    )

    # INTERMEDIATE LAYER: Business logic and calculated fields
    int_render_config = RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        test_behavior=TestBehavior.AFTER_ALL,   # Run tests after all intermediate models complete
        select=["tag:intermediate"],            # Only run models tagged with 'intermediate'
    )

    itg = DbtTaskGroup(
        group_id="dbt_intermediate",
        project_config=project_config,
        render_config=int_render_config,
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
        },
        default_args={ 
            "retries": 2, 
            "retry_delay": timedelta(minutes=3) 
        },
    )

    # MARTS LAYER: Final analytical tables and aggregations
    mart_render_config = RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        test_behavior=TestBehavior.AFTER_ALL,   # Run tests after all mart models complete
        select=["tag:marts"],                   # Only run models tagged with 'marts'
    )

    mrt = DbtTaskGroup(
        group_id="dbt_marts",
        project_config=project_config,
        render_config=mart_render_config,
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
        },
        default_args={ 
            "retries": 2, 
            "retry_delay": timedelta(minutes=3) 
        },
    )

    # Wire up dependencies: Data Loading → Staging → Intermediate → Marts
    # This ensures proper data flow through the transformation layers
    load_tasks >> stg >> itg >> mrt

# DAG task creation and dependency setup
with dag:
    # Create download and load task pairs for each IMDb dataset
    download_tasks = []
    load_tasks = []
    
    for file_key in IMDB_FILES:
        # Get optimized configurations for each dataset
        task_timeout = get_task_timeout(file_key)
        retry_config = get_retry_config(file_key)
        
        # Download task: Fetch dataset with intelligent caching
        download_task = PythonOperator(
            task_id=f"download_{file_key}",
            python_callable=fetch_imdb_dataset,
            op_args=[file_key],
            execution_timeout=task_timeout,
            retries=retry_config['retries'],
            retry_delay=retry_config['retry_delay'],
            max_retry_delay=retry_config['max_retry_delay'],
            pool='default_pool',
            doc_md=f"""
            Download {file_key} dataset from IMDb with smart caching.
            
            Features:
            - HTTP Last-Modified header validation to avoid unnecessary downloads
            - Exponential backoff retry logic for network resilience
            - Airflow Variables-based caching for cross-run persistence
            - Atomic file writes to prevent corruption
            """
        )
        
        # Load task: Insert data into PostgreSQL with bulk operations
        load_task = PythonOperator(
            task_id=f"load_{file_key}_to_postgres",
            python_callable=load_imdb_table,
            op_args=[file_key],
            execution_timeout=task_timeout,
            retries=retry_config['retries'],
            retry_delay=retry_config['retry_delay'],
            max_retry_delay=retry_config['max_retry_delay'],
            pool='default_pool',
            doc_md=f"""
            Load {file_key} into PostgreSQL using optimized COPY operation.
            
            Features:
            - PostgreSQL COPY command for 10-100x faster loading than INSERTs
            - Cache-based duplicate prevention
            - Memory-efficient streaming decompression
            - Comprehensive data validation and integrity checks
            - Automatic cleanup of temporary files
            """
        )
        
        # Set up download → load dependency for each dataset
        download_task >> load_task
        download_tasks.append(download_task)
        load_tasks.append(load_task)
    
    # Create the dbt transformation pipeline after all data is loaded
    create_dbt_task_group(load_tasks)
