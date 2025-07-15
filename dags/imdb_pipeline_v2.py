"""
IMDb Data Pipeline DAG - Production Version
==========================================

A clean, modular Airflow DAG for processing IMDb datasets with:
- Intelligent caching and duplicate prevention
- Robust error handling using Airflow's native retry mechanisms
- PostgreSQL bulk loading with transaction safety
- dbt transformations via Astronomer Cosmos

This is the refactored version that uses modular components for better
maintainability, testing, and code organization.

Key Improvements:
- Removed custom retry decorators in favor of Airflow's built-in retry logic
- Fixed critical cache timing bug (cache updates only after successful load)
- Modular architecture with separate concerns
- Comprehensive error handling and logging
- Performance optimizations and monitoring

Author: Andrea Usai
Version: 2.0.0
Last Updated: January 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import our modular components
try:
    from imdb_pipeline.downloader import download_imdb_dataset_task
    from imdb_pipeline.loader import load_imdb_table_task  
    from imdb_pipeline.dbt_tasks import create_dbt_task_group_simple
    from imdb_pipeline.dbt_tasks import create_dbt_task_groups_layered
    from imdb_pipeline.config import (
        IMDB_FILES,
        get_task_timeout, 
        get_retry_config,
        POSTGRES_CONN_ID
    )
except ImportError:
    # Handle case when Airflow parses modules directly
    import sys
    import os
    dag_dir = os.path.dirname(__file__)
    sys.path.insert(0, dag_dir)
    
    from imdb_pipeline.downloader import download_imdb_dataset_task
    from imdb_pipeline.loader import load_imdb_table_task  
    from imdb_pipeline.dbt_tasks import create_dbt_task_group_simple
    from imdb_pipeline.dbt_tasks import create_dbt_task_groups_layered
    from imdb_pipeline.config import (
        IMDB_FILES,
        get_task_timeout, 
        get_retry_config,
        POSTGRES_CONN_ID
    )

# ============================================================================
# DAG DEFINITION
# ============================================================================

# Default task arguments with improved retry configuration
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(hours=6),  # Service Level Agreement
}

# Create the DAG
dag = DAG(
    dag_id='imdb_pipeline_v2',
    default_args=default_args,
    description='IMDb data pipeline with modular architecture and robust error handling',
    schedule='@daily',
    catchup=False,
    tags=['imdb', 'etl', 'dbt', 'production', 'v2'],
    max_active_runs=1,  # Prevent overlapping runs
    dagrun_timeout=timedelta(hours=6),  # Maximum DAG run time
    doc_md=__doc__,
)

# ============================================================================
# TASK CREATION
# ============================================================================

with dag:
    # Task lists for dependency management
    download_tasks = []
    load_tasks = []
    
    # Create download and load tasks for each IMDb dataset
    for file_key in IMDB_FILES:
        # Get optimized configuration for this dataset
        timeout = get_task_timeout(file_key)
        retry_config = get_retry_config(file_key)
        
        # Download task with smart caching
        download_task = PythonOperator(
            task_id=f'download_{file_key}',
            python_callable=download_imdb_dataset_task,
            op_args=[file_key],
            execution_timeout=timeout,
            retries=retry_config['retries'],
            retry_delay=retry_config['retry_delay'],
            pool='default_pool',
            doc_md=f"""
            **Download {file_key} dataset**
            
            Downloads the latest {file_key} dataset from IMDb with intelligent caching:
            - Validates HTTP Last-Modified headers to avoid unnecessary downloads
            - Uses Airflow Variables for persistent caching across runs
            - Implements file integrity validation
            - Provides comprehensive error handling and logging
            """,
        )
        
        # Load task with transaction safety
        load_task = PythonOperator(
            task_id=f'load_{file_key}',
            python_callable=load_imdb_table_task,
            op_args=[file_key],
            execution_timeout=timeout,
            retries=retry_config['retries'],
            retry_delay=retry_config['retry_delay'],
            pool='default_pool',
            doc_md=f"""
            **Load {file_key} into PostgreSQL**
            
            Loads the {file_key} dataset into PostgreSQL with optimizations:
            - Uses PostgreSQL COPY for maximum performance
            - Implements atomic transactions with proper rollback
            - Updates cache only after successful load (fixes timing bug)
            - Validates data integrity and row counts
            - Provides comprehensive error handling and cleanup
            """,
        )
        
        # Set up dependency: download must complete before load
        download_task >> load_task  # type: ignore
        
        # Add to task lists
        download_tasks.append(download_task)
        load_tasks.append(load_task)
    
    # Create dbt transformation pipeline
    # This runs after all data loading is complete
    dbt_tasks = create_dbt_task_group_simple(dag, load_tasks)

# ============================================================================
# TASK DOCUMENTATION
# ============================================================================

dag.doc_md = """
# IMDb Data Pipeline v2.0

## Overview
This DAG processes IMDb datasets with production-grade reliability and performance.

## Architecture
```
IMDb Downloads → Data Loading → dbt Transformations
     ↓               ↓                    ↓
  Smart Cache → PostgreSQL COPY → Layered Models
```

## Key Features
- **Smart Caching**: Avoids unnecessary downloads using HTTP headers
- **Robust Error Handling**: Uses Airflow's native retry mechanisms
- **Transaction Safety**: Cache updates only after successful database loads
- **Performance Optimized**: PostgreSQL COPY operations for bulk loading
- **Modular Design**: Separate modules for different concerns

## Data Flow
1. **Download Phase**: Fetch latest IMDb datasets with cache validation
2. **Load Phase**: Bulk insert into PostgreSQL with transaction safety
3. **Transform Phase**: dbt models create staging → intermediate → marts layers

## Monitoring
- Check task logs for performance metrics and error details
- Monitor Airflow Variables for cache status
- Use PostgreSQL queries to validate data integrity

## Datasets Processed
- `title_basics`: Core title information
- `title_akas`: Alternative titles and regional names
- `title_ratings`: User ratings and vote counts
- `name_basics`: Person information and filmography
- `title_crew`: Director and writer credits
- `title_episode`: TV episode relationships
- `title_principals`: Cast and crew roles

## Dependencies
- PostgreSQL database with connection ID: `{POSTGRES_CONN_ID}`
- dbt project located at: `dags/dbt/`
- Astronomer Cosmos for dbt integration

## Configuration
Task timeouts and retry configurations are optimized based on dataset size:
- Small datasets (ratings): 15 minutes, 3 retries
- Medium datasets (basics): 45 minutes, 4 retries  
- Large datasets (principals): 2 hours, 5 retries

## Error Handling
- Automatic retries with exponential backoff
- Comprehensive logging and error reporting
- Graceful degradation and cleanup
- Cache consistency protection

## Version History
- v2.0.0: Modular architecture, fixed cache timing bug, improved error handling
- v1.0.0: Original monolithic implementation
""".format(POSTGRES_CONN_ID=POSTGRES_CONN_ID)
