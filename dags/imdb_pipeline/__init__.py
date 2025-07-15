"""
IMDb Pipeline Package
====================

Modular, production-ready IMDb data pipeline with intelligent caching,
robust error handling, and dbt transformations.

This package provides a complete ETL solution for IMDb datasets with:
- Smart caching to avoid unnecessary downloads
- Robust error handling with proper retry logic
- PostgreSQL bulk loading with transaction safety
- dbt transformations via Astronomer Cosmos
- Comprehensive logging and monitoring

Modules:
- config: Configuration management and constants
- cache_manager: Intelligent caching system
- downloader: HTTP downloading with cache validation
- loader: PostgreSQL data loading with transactions
- dbt_tasks: dbt transformation orchestration
- utils: Common utility functions

Author: Andrea Usai
Version: 2.0.0
Last Updated: July 2025
"""

from .config import *
from .cache_manager import CacheManager
from .downloader import IMDbDownloader, download_imdb_dataset_task
from .loader import IMDbDataLoader, load_imdb_table_task
from .dbt_tasks import create_dbt_task_group_simple, create_dbt_task_groups_layered
from .utils import (
    update_heartbeat, add_jitter, format_bytes, format_duration,
    PerformanceTracker, get_processing_strategy
)

__version__ = "2.0.0"
__author__ = "Andrea Usai"

__all__ = [
    # Core classes
    'CacheManager',
    'IMDbDownloader', 
    'IMDbDataLoader',
    
    # Task callables
    'download_imdb_dataset_task',
    'load_imdb_table_task',
    'create_dbt_task_group_simple',
    'create_dbt_task_groups_layered',
    
    # Utility functions
    'update_heartbeat',
    'add_jitter',
    'format_bytes',
    'format_duration',
    'PerformanceTracker',
    'get_processing_strategy',
    
    # Constants (from config)
    'IMDB_FILES',
    'TABLE_CONFIGS',
    'TASK_TIMEOUTS',
    'RETRY_CONFIGS',
    'POSTGRES_CONN_ID',
    'DOWNLOAD_DIR',
    'SCHEMA_DIR'
]
