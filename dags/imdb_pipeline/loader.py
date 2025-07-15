"""
IMDb Data Loader Module
======================

Handles PostgreSQL data loading operations with robust error handling,
transaction safety, and performance optimization.

Key Features:
- PostgreSQL COPY operations for bulk loading
- Atomic transactions with proper rollback
- Cache updates only after successful load
- Memory-efficient streaming decompression
- Comprehensive data validation

Author: Andrea Usai
Version: 2.0.0
Last Updated: January 2025
"""

import os
import gzip
import time
import psycopg2
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from .config import (
        DOWNLOAD_DIR, SCHEMA_DIR, POSTGRES_CONN_ID, 
        IMDB_FILES, TABLE_CONFIGS
    )
    from .cache_manager import CacheManager
except ImportError:
    # Handle case when module is parsed directly by Airflow
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from config import (
        DOWNLOAD_DIR, SCHEMA_DIR, POSTGRES_CONN_ID, 
        IMDB_FILES, TABLE_CONFIGS
    )
    from cache_manager import CacheManager

# Initialize logger
log = LoggingMixin().log


class IMDbDataLoader:
    """
    Handles loading IMDb data files into PostgreSQL with transaction safety
    and performance optimization.
    """
    
    def __init__(self):
        self.cache_manager = CacheManager()
        self.postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    def setup_table_schema(self, cur: psycopg2.extensions.cursor, 
                          schema_file_path: str, table_name: str) -> None:
        """
        Create database table from SQL schema file if it doesn't already exist.
        
        Args:
            cur: PostgreSQL cursor for executing SQL commands
            schema_file_path: Path to the SQL file containing CREATE TABLE statement
            table_name: Name of the table being created (for logging)
            
        Raises:
            ValueError: If schema file contains invalid SQL content
            FileNotFoundError: If schema file is not found
        """
        try:
            if not os.path.exists(schema_file_path):
                raise FileNotFoundError(f"Schema file not found: {schema_file_path}")
            
            with open(schema_file_path, 'r', encoding='utf-8') as schema_file:
                schema_sql = schema_file.read().strip()
                
            if not schema_sql:
                raise ValueError(f"Schema file is empty: {schema_file_path}")
                
            # Execute schema creation (idempotent - CREATE TABLE IF NOT EXISTS)
            cur.execute(schema_sql)
            log.info(f"[SCHEMA] Ensured table {table_name} exists with proper schema")
            
        except Exception as e:
            log.error(f"[SCHEMA ERROR] Failed to create table {table_name}: {e}")
            raise
    
    def validate_gzip_file(self, filepath: str) -> bool:
        """
        Validate gzip file integrity before processing.
        
        Args:
            filepath: Path to the gzip file to validate
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        try:
            with gzip.open(filepath, 'rb') as f:
                # Try reading a small chunk to test file integrity
                f.read(1024)
                f.seek(0)  # Reset for actual processing
            return True
        except (EOFError, gzip.BadGzipFile) as e:
            log.error(f"[CORRUPTION] Invalid gzip file {filepath}: {e}")
            return False
    
    def validate_file_structure(self, file_key: str, first_line: str) -> bool:
        """
        Validate that the file has the expected column structure.
        
        Args:
            file_key: Identifier for the dataset
            first_line: First line of the file (header)
            
        Returns:
            bool: True if structure matches expected format
        """
        config = TABLE_CONFIGS.get(file_key)
        if not config:
            log.warning(f"[VALIDATION] No configuration found for {file_key}")
            return True  # Assume valid if no config
        
        headers = first_line.strip().split('\t')
        expected_count = config['column_count']
        expected_columns = config['columns']
        
        if len(headers) != expected_count:
            log.error(
                f"[VALIDATION ERROR] {file_key} has {len(headers)} columns, "
                f"expected {expected_count}"
            )
            return False
        
        # Validate column names match expected order
        for i, (actual, expected) in enumerate(zip(headers, expected_columns)):
            if actual != expected:
                log.error(
                    f"[VALIDATION ERROR] {file_key} column {i}: "
                    f"got '{actual}', expected '{expected}'"
                )
                return False
        
        log.info(f"[VALIDATION] {file_key} file structure is valid")
        return True
    
    def load_imdb_table(self, file_key: str) -> None:
        """
        Load IMDb dataset into PostgreSQL with comprehensive error handling.
        
        Features:
        - Cache-based duplicate prevention
        - PostgreSQL COPY for optimal performance
        - Atomic transactions with proper rollback
        - Memory-efficient streaming decompression
        - Data validation and integrity checks
        - Cache update ONLY after successful load
        
        Args:
            file_key: Identifier for the IMDb dataset to load
            
        Raises:
            FileNotFoundError: If source file doesn't exist
            psycopg2.Error: If database operation fails
            ValueError: If file validation fails
        """
        start_time = time.time()
        filename = IMDB_FILES[file_key]
        table_name = f"imdb_{file_key}"
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        schema_file_path = os.path.join(SCHEMA_DIR, f"{table_name}.sql")
        
        # Get the columns configuration
        config = TABLE_CONFIGS[file_key]
        columns = config['columns']

        log.info(f"[LOAD START] Loading {file_key} into {table_name}")
        
        # Check if file exists
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Source file not found: {filepath}")
        
        # Smart cache-based duplicate detection
        cache_info = self.cache_manager.get_download_cache(file_key)
        file_stats = os.stat(filepath)
        current_size = file_stats.st_size
        cached_size = cache_info.get("file_size", 0)
        last_loaded = cache_info.get("last_loaded")
        
        # Skip loading if file hasn't changed since last successful load
        if (last_loaded and 
            current_size == cached_size and 
            current_size > 0):
            log.info(
                f"[CACHE HIT] Skipping {file_key} - already loaded "
                f"(size: {current_size:,} bytes, last loaded: {last_loaded})"
            )
            return
        
        # Validate file integrity before processing
        if not self.validate_gzip_file(filepath):
            raise ValueError(f"Corrupted gzip file: {filepath}")
        
        # Create temporary uncompressed file for COPY operation
        uncompressed_tsv_path = f"{filepath}.tsv"
        
        try:
            # Database connection with transaction control
            with self.postgres_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Set up table schema (idempotent)
                    self.setup_table_schema(cur, schema_file_path, table_name)
                    
                    # Clear existing data for fresh load
                    cur.execute(f"TRUNCATE TABLE {table_name}")
                    log.info(f"[TRUNCATE] Cleared existing data from {table_name}")
                    
                    # Stream decompress and validate file structure
                    log.info(f"[DECOMPRESS] Extracting {filepath}")
                    total_lines = 0
                    
                    with gzip.open(filepath, 'rt', encoding='utf-8') as gz_file:
                        with open(uncompressed_tsv_path, 'w', encoding='utf-8') as tsv_file:
                            for line_num, line in enumerate(gz_file):
                                # Validate file structure using header
                                if line_num == 0:
                                    if not self.validate_file_structure(file_key, line):
                                        raise ValueError(f"Invalid file structure for {file_key}")
                                
                                # Skip header row for data loading
                                if line_num > 0:
                                    tsv_file.write(line)
                                    total_lines += 1
                                
                                # Progress updates for large files
                                if line_num % 1000000 == 0 and line_num > 0:
                                    log.info(f"[PROGRESS] Processed {line_num:,} lines")
                    
                    log.info(f"[DECOMPRESS COMPLETE] {total_lines:,} data rows extracted")
                    
                    # PostgreSQL COPY operation for maximum performance
                    copy_start = time.time()
                    
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
                    
                    copy_time = time.time() - copy_start
                    
                    # Verify data integrity - count inserted rows
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    db_count = cur.fetchone()[0]
                    
                    # Validate row count matches expectations
                    if db_count != total_lines:
                        raise ValueError(
                            f"Row count mismatch for {table_name}: "
                            f"inserted {db_count:,}, expected {total_lines:,}"
                        )
                    
                    # COMMIT the transaction - this is the critical success point
                    conn.commit()
                    
                    # SUCCESS: Only now update cache (after successful commit)
                    self.cache_manager.update_load_success(file_key)
                    
                    # Performance metrics
                    total_time = time.time() - start_time
                    rows_per_second = db_count / copy_time if copy_time > 0 else 0
                    
                    log.info(
                        f"[SUCCESS] {table_name}: {db_count:,} rows loaded in "
                        f"{total_time:.1f}s (COPY: {copy_time:.1f}s, "
                        f"{rows_per_second:.0f} rows/s)"
                    )
        
        except Exception as e:
            log.error(f"[ERROR] Failed to load {table_name}: {e}")
            # Note: No cache update on failure - this prevents inconsistent state
            raise
        
        finally:
            # Always clean up temporary files
            if os.path.exists(uncompressed_tsv_path):
                os.remove(uncompressed_tsv_path)
                log.info(f"[CLEANUP] Removed temporary file: {uncompressed_tsv_path}")


# Task callable function for Airflow
def load_imdb_table_task(file_key: str) -> None:
    """
    Airflow task callable for loading IMDb data.
    
    Args:
        file_key: Identifier for the dataset to load
    """
    loader = IMDbDataLoader()
    loader.load_imdb_table(file_key)
