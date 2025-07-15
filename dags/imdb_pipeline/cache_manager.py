"""
IMDb Pipeline Cache Manager
==========================

Handles all caching operations for the IMDb pipeline.
Provides intelligent caching with proper state management.
"""

import json
from datetime import datetime, timezone
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class CacheManager:
    """Manages cache operations for IMDb pipeline with proper state tracking."""
    
    @staticmethod
    def get_download_cache(file_key: str) -> dict:
        """
        Retrieve cached download metadata from Airflow Variables.
        
        Args:
            file_key: Identifier for the IMDb dataset
            
        Returns:
            dict: Cache metadata or empty dict if cache doesn't exist
        """
        try:
            cache = Variable.get(f"imdb_{file_key}_cache", default_var="{}")
            cache_data = json.loads(cache) if cache else {}
            log.info(f"[CACHE] Retrieved cache for {file_key}: {cache_data}")
            return cache_data
        except Exception as e:
            log.error(f"[CACHE ERROR] Failed to fetch cache for {file_key}: {e}")
            return {}
    
    @staticmethod
    def update_download_cache(file_key: str, remote_last_modified: str, local_path: str):
        """
        Update cached download metadata after successful download.
        
        Preserves existing last_loaded timestamp to maintain database state.
        
        Args:
            file_key: Identifier for the IMDb dataset
            remote_last_modified: HTTP Last-Modified header from server
            local_path: Path where file was saved locally
        """
        import os
        
        # Preserve existing database load timestamp
        existing_cache = CacheManager.get_download_cache(file_key)
        
        cache = {
            "remote_last_modified": remote_last_modified,
            "local_path": local_path,
            "file_size": os.path.getsize(local_path) if os.path.exists(local_path) else 0,
            "last_loaded": existing_cache.get("last_loaded", ""),
            "last_checked": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            Variable.set(f"imdb_{file_key}_cache", json.dumps(cache))
            log.info(f"[CACHE] Updated download cache for {file_key}")
        except Exception as e:
            log.error(f"[CACHE ERROR] Failed to update cache for {file_key}: {e}")
            raise
    
    @staticmethod
    def update_load_success(file_key: str):
        """
        Update cache ONLY after successful database loading.
        
        CRITICAL: This should only be called after confirmed successful loading.
        
        Args:
            file_key: Identifier for the IMDb dataset
        """
        existing_cache = CacheManager.get_download_cache(file_key)
        
        if existing_cache:
            existing_cache["last_loaded"] = datetime.now(timezone.utc).isoformat()
            try:
                Variable.set(f"imdb_{file_key}_cache", json.dumps(existing_cache))
                log.info(f"[CACHE] Updated load success timestamp for {file_key}")
            except Exception as e:
                log.error(f"[CACHE ERROR] Failed to update load timestamp for {file_key}: {e}")
                raise
        else:
            log.warning(f"[CACHE] No existing cache found for {file_key}, cannot update load timestamp")
    
    @staticmethod
    def clear_cache(file_key: str):
        """
        Clear cache for specific dataset (useful for recovery scenarios).
        
        Args:
            file_key: Identifier for the IMDb dataset
        """
        try:
            Variable.delete(f"imdb_{file_key}_cache")
            log.info(f"[CACHE] Cleared cache for {file_key}")
        except Exception as e:
            log.warning(f"[CACHE] Cache clear failed for {file_key}: {e}")
    
    @staticmethod
    def is_load_needed(file_key: str) -> tuple[bool, str]:
        """
        Determine if database loading is needed based on cache state.
        
        Args:
            file_key: Identifier for the IMDb dataset
            
        Returns:
            tuple: (is_needed: bool, reason: str)
        """
        from email.utils import parsedate_to_datetime
        
        cache_info = CacheManager.get_download_cache(file_key)
        remote_last_modified = cache_info.get("remote_last_modified")
        last_loaded = cache_info.get("last_loaded")
        
        if not remote_last_modified:
            return True, "No remote modification time available"
        
        if not last_loaded:
            return True, "No previous load recorded"
        
        try:
            remote_dt = parsedate_to_datetime(remote_last_modified)
            loaded_dt = datetime.fromisoformat(last_loaded.replace('Z', '+00:00'))
            
            if loaded_dt >= remote_dt:
                return False, f"Current version already loaded (File: {remote_dt}, Loaded: {loaded_dt})"
            else:
                return True, f"Remote file newer than last load (File: {remote_dt}, Loaded: {loaded_dt})"
                
        except Exception as e:
            log.warning(f"[CACHE] Error parsing timestamps: {e}")
            return True, "Timestamp parsing failed, proceeding with load"
