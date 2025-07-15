"""
IMDb Pipeline Configuration
==========================

Centralized configuration for the IMDb data pipeline.
All constants, timeouts, and configuration mappings.
"""

import os
from datetime import timedelta
from pathlib import Path

# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

# Environment and path configuration with fallback defaults
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
os.makedirs(AIRFLOW_HOME, exist_ok=True)

# Data storage paths with environment variable overrides
DEFAULT_DOWNLOAD_DIR = os.path.join(AIRFLOW_HOME, 'data', 'files')
DEFAULT_SCHEMA_DIR = os.path.join(AIRFLOW_HOME, 'schemas')

DOWNLOAD_DIR = os.environ.get('IMDB_DOWNLOAD_DIR', DEFAULT_DOWNLOAD_DIR)
SCHEMA_DIR = os.environ.get('IMDB_SCHEMA_DIR', DEFAULT_SCHEMA_DIR)
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID', 'db_conn')

# dbt project configuration for Cosmos integration
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt"
DBT_PROFILES_PATH = DBT_PROJECT_PATH
os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_PATH)

# ============================================================================
# IMDB DATA SOURCE CONFIGURATION
# ============================================================================

BASE_URL = 'https://datasets.imdbws.com/'

# Complete mapping of dataset identifiers to filenames
IMDB_FILES = {
    'title_basics': 'title.basics.tsv.gz',
    'title_akas': 'title.akas.tsv.gz',
    'title_ratings': 'title.ratings.tsv.gz',
    'name_basics': 'name.basics.tsv.gz',
    'title_crew': 'title.crew.tsv.gz',
    'title_episode': 'title.episode.tsv.gz',
    'title_principals': 'title.principals.tsv.gz'
}

# Database table schema definitions
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
# PERFORMANCE AND TIMEOUT CONFIGURATION
# ============================================================================

# Task timeouts based on empirical data
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

# Simplified retry configuration (using Airflow native features)
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = timedelta(minutes=5)

# Large dataset retry configuration
LARGE_DATASET_RETRIES = 5
LARGE_DATASET_RETRY_DELAY = timedelta(minutes=10)

# Datasets that need special retry handling
LARGE_DATASETS = {'title_principals', 'title_akas', 'title_crew'}

# Legacy retry configurations (for backward compatibility)
RETRY_CONFIGS = {
    'title_principals': {
        'retries': LARGE_DATASET_RETRIES,
        'retry_delay': LARGE_DATASET_RETRY_DELAY,
    },
    'title_basics': {
        'retries': 4,
        'retry_delay': timedelta(minutes=7),
    },
    'default': {
        'retries': DEFAULT_RETRIES,
        'retry_delay': DEFAULT_RETRY_DELAY,
    }
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_task_timeout(file_key: str) -> timedelta:
    """Get timeout for specific dataset."""
    return TASK_TIMEOUTS.get(file_key, DEFAULT_TASK_TIMEOUT)

def get_retry_config(file_key: str) -> dict:
    """Get retry configuration for specific dataset."""
    if file_key in LARGE_DATASETS:
        return {
            'retries': LARGE_DATASET_RETRIES,
            'retry_delay': LARGE_DATASET_RETRY_DELAY,
        }
    return {
        'retries': DEFAULT_RETRIES,
        'retry_delay': DEFAULT_RETRY_DELAY,
    }
