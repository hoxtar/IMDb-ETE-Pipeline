"""
IMDb Dataset Downloader
======================

Handles downloading of IMDb datasets with intelligent caching and validation.
"""

import os
import gzip
import requests
import time
from datetime import datetime, timezone, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

# Import with fallback for Airflow parsing
try:
    from .config import BASE_URL, IMDB_FILES, DOWNLOAD_DIR
    from .cache_manager import CacheManager
except ImportError:
    # When Airflow parses this file directly
    import sys
    sys.path.append(os.path.dirname(__file__))
    from config import BASE_URL, IMDB_FILES, DOWNLOAD_DIR
    from cache_manager import CacheManager

log = LoggingMixin().log


class IMDbDownloader:
    """Handles downloading and validation of IMDb datasets."""
    
    def __init__(self):
        """Initialize downloader and ensure download directory exists."""
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    @staticmethod
    def validate_gzip_file(filepath: str) -> bool:
        """
        Validate gzip file integrity before processing.
        
        Args:
            filepath: Path to the gzip file to validate
            
        Returns:
            bool: True if file is valid, False if corrupted
        """
        try:
            with gzip.open(filepath, 'rb') as f:
                # Try reading a small chunk to test file integrity
                f.read(1024)
                f.seek(0)  # Reset for actual processing
            return True
        except (EOFError, gzip.BadGzipFile) as e:
            log.error(f"[VALIDATION] Invalid gzip file {filepath}: {e}")
            return False
    
    def _should_skip_download(self, file_key: str, filepath: str) -> bool:
        """
        Check if download can be skipped based on cache.
        
        Args:
            file_key: Dataset identifier
            filepath: Local file path
            
        Returns:
            bool: True if download can be skipped
        """
        cache_info = CacheManager.get_download_cache(file_key)
        cache_last_modified = cache_info.get("remote_last_modified")
        
        if not cache_last_modified or not os.path.exists(filepath):
            return False
        
        # Skip remote validation if cache was checked within last hour
        last_checked = cache_info.get("last_checked")
        if last_checked:
            try:
                last_checked_dt = datetime.fromisoformat(last_checked.replace('Z', '+00:00'))
                if datetime.now(timezone.utc) - last_checked_dt < timedelta(hours=1):
                    log.info(f"[CACHE] Recent check for {file_key}, skipping download")
                    return True
            except Exception:
                pass  # Fall through to remote check
        
        return False
    
    def _check_remote_file(self, file_url: str, file_key: str, filepath: str) -> bool:
        """
        Check if remote file is newer than cached version.
        
        Args:
            file_url: URL of the remote file
            file_key: Dataset identifier  
            filepath: Local file path
            
        Returns:
            bool: True if download is needed, False if cache is current
        """
        cache_info = CacheManager.get_download_cache(file_key)
        cache_last_modified = cache_info.get("remote_last_modified")
        
        try:
            log.info(f"[REMOTE] Checking remote file for {file_key}")
            head_resp = requests.head(file_url, timeout=30)
            head_resp.raise_for_status()
            remote_last_modified = head_resp.headers.get('Last-Modified')
            
            if remote_last_modified:
                if cache_last_modified == remote_last_modified and os.path.exists(filepath):
                    log.info(f"[CACHE] Remote matches cache for {file_key}")
                    return False
                else:
                    log.info(f"[CACHE] Remote file newer for {file_key}")
                    return True
            
            # No Last-Modified header, assume download needed
            return True
            
        except Exception as e:
            log.error(f"[REMOTE] Error checking remote file for {file_key}: {e}")
            raise
    
    def _download_file(self, file_url: str, filepath: str) -> str:
        """
        Download file from URL to local path.
        
        Args:
            file_url: URL to download from
            filepath: Local path to save file
            
        Returns:
            str: Last-Modified header from response
            
        Raises:
            requests.RequestException: If download fails
        """
        log.info(f"[DOWNLOAD] Starting download from {file_url}")
        
        response = requests.get(file_url, timeout=120)
        response.raise_for_status()
        
        # Atomic write to prevent corruption
        temp_path = f"{filepath}.tmp"
        with open(temp_path, 'wb') as f:
            f.write(response.content)
        
        # Validate downloaded file
        if not self.validate_gzip_file(temp_path):
            os.remove(temp_path)
            raise ValueError(f"Downloaded file {filepath} is corrupted")
        
        # Atomically replace the target file
        os.rename(temp_path, filepath)
        
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        log.info(f"[DOWNLOAD] Completed {filepath} ({size_mb:.1f} MB)")
        
        return response.headers.get('Last-Modified', '')
    
    def fetch_dataset(self, file_key: str) -> None:
        """
        Download IMDb dataset with intelligent caching.
        
        Main entry point for downloading datasets. Handles:
        - Cache validation to avoid unnecessary downloads
        - Remote file checking with Last-Modified headers
        - File integrity validation
        - Cache updates after successful download
        
        Args:
            file_key: Identifier for the IMDb dataset to download
            
        Raises:
            ValueError: If file_key is invalid or file is corrupted
            requests.RequestException: If download fails
        """
        if file_key not in IMDB_FILES:
            raise ValueError(f"Invalid file_key: {file_key}")
        
        filename = IMDB_FILES[file_key]
        file_url = BASE_URL + filename
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        
        log.info(f"[DOWNLOAD] Processing {file_key}")
        
        # Check if download can be skipped
        if self._should_skip_download(file_key, filepath):
            return
        
        # Check if remote file is newer
        if not self._check_remote_file(file_url, file_key, filepath):
            # Update last_checked timestamp even if no download needed
            cache_info = CacheManager.get_download_cache(file_key)
            if cache_info:
                CacheManager.update_download_cache(
                    file_key, 
                    cache_info.get("remote_last_modified", ""), 
                    filepath
                )
            return
        
        # Download file
        try:
            remote_last_modified = self._download_file(file_url, filepath)
            
            # Update cache after successful download
            CacheManager.update_download_cache(file_key, remote_last_modified, filepath)
            
        except Exception as e:
            log.error(f"[DOWNLOAD] Failed to download {file_key}: {e}")
            raise


# Convenience function for Airflow tasks
def fetch_imdb_dataset(file_key: str) -> None:
    """
    Airflow task function to download IMDb dataset.
    
    Args:
        file_key: Identifier for the IMDb dataset to download
    """
    downloader = IMDbDownloader()
    downloader.fetch_dataset(file_key)


# Task callable function for Airflow
def download_imdb_dataset_task(file_key: str) -> None:
    """
    Airflow task callable for downloading IMDb data.
    
    Args:
        file_key: Identifier for the dataset to download
    """
    downloader = IMDbDownloader()
    downloader.fetch_dataset(file_key)
