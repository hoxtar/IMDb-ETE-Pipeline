"""
Utility Functions for IMDb Pipeline
==================================

Common utility functions used across the IMDb data pipeline.

Key Features:
- Heartbeat monitoring
- Performance metrics
- Data validation helpers
- Common constants and enums

Author: Andrea Usai
Version: 2.0.0
Last Updated: January 2025
"""

import time
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from airflow.utils.log.logging_mixin import LoggingMixin

# Initialize logger
log = LoggingMixin().log


def update_heartbeat(message: str, task_id: Optional[str] = None) -> None:
    """
    Log a heartbeat message with timestamp for monitoring long-running tasks.
    
    This helps monitor task progress and can be integrated with external
    monitoring systems for alerting on stuck tasks.
    
    Args:
        message: Descriptive message about current task status
        task_id: Optional task identifier for context
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    prefix = f"[{task_id}] " if task_id else ""
    log.info(f"[HEARTBEAT] {prefix}{message} at {timestamp}")


def add_jitter(delay: float, jitter_factor: float = 0.1) -> float:
    """
    Add random jitter to delay values to prevent thundering herd problems.
    
    This is especially important when multiple tasks might retry simultaneously
    and hit the same external resource.
    
    Args:
        delay: Base delay in seconds
        jitter_factor: Percentage of jitter to add (0.1 = 10%)
        
    Returns:
        float: Delay with random jitter added
    """
    jitter = delay * jitter_factor * random.random()
    return delay + jitter


def format_bytes(bytes_value: int) -> str:
    """
    Format byte values into human-readable strings.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        str: Formatted string (e.g., "1.5 MB", "2.3 GB")
    """
    value = float(bytes_value)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        str: Formatted duration (e.g., "2m 30s", "1h 15m")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def validate_file_path(filepath: str) -> bool:
    """
    Validate that a file path is safe and accessible.
    
    Args:
        filepath: Path to validate
        
    Returns:
        bool: True if path is valid and safe
    """
    import os
    from pathlib import Path
    
    try:
        path = Path(filepath)
        
        # Check for directory traversal attempts
        if '..' in str(path):
            log.warning(f"[SECURITY] Path traversal detected in: {filepath}")
            return False
        
        # Check if parent directory exists or can be created
        parent_dir = path.parent
        if not parent_dir.exists():
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                log.error(f"[VALIDATION] Cannot create directory {parent_dir}: {e}")
                return False
        
        return True
        
    except Exception as e:
        log.error(f"[VALIDATION] Invalid file path {filepath}: {e}")
        return False


def calculate_exponential_backoff(attempt: int, base_delay: float = 1.0, 
                                max_delay: float = 300.0, 
                                jitter: bool = True) -> float:
    """
    Calculate exponential backoff delay with optional jitter.
    
    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap in seconds
        jitter: Whether to add random jitter
        
    Returns:
        float: Calculated delay in seconds
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    
    if jitter:
        delay = add_jitter(delay)
    
    return delay


class PerformanceTracker:
    """
    Track and log performance metrics for tasks.
    """
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        self.start_time = None
        self.metrics = {}
    
    def start(self) -> None:
        """Start tracking performance."""
        self.start_time = time.time()
        log.info(f"[PERF] Starting {self.task_name}")
    
    def checkpoint(self, name: str, count: Optional[int] = None) -> None:
        """Record a checkpoint with optional count."""
        if self.start_time is None:
            log.warning(f"[PERF] Checkpoint '{name}' called before start()")
            return
        
        elapsed = time.time() - self.start_time
        self.metrics[name] = {'elapsed': elapsed, 'count': count}
        
        if count:
            rate = count / elapsed if elapsed > 0 else 0
            log.info(f"[PERF] {self.task_name}.{name}: {count:,} items in "
                    f"{format_duration(elapsed)} ({rate:.0f}/s)")
        else:
            log.info(f"[PERF] {self.task_name}.{name}: {format_duration(elapsed)}")
    
    def finish(self, total_count: Optional[int] = None) -> Dict[str, Any]:
        """Finish tracking and return metrics summary."""
        if self.start_time is None:
            log.warning(f"[PERF] finish() called before start()")
            return {}
        
        total_time = time.time() - self.start_time
        
        summary = {
            'task_name': self.task_name,
            'total_time': total_time,
            'total_count': total_count,
            'checkpoints': self.metrics
        }
        
        if total_count:
            rate = total_count / total_time if total_time > 0 else 0
            log.info(f"[PERF] {self.task_name} COMPLETED: {total_count:,} items "
                    f"in {format_duration(total_time)} ({rate:.0f}/s)")
        else:
            log.info(f"[PERF] {self.task_name} COMPLETED in {format_duration(total_time)}")
        
        return summary


# Common HTTP headers for web requests
DEFAULT_HEADERS = {
    'User-Agent': 'IMDb-Analytics-Pipeline/2.0.0 (Airflow ETL)',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

# File size thresholds for different processing strategies
SIZE_THRESHOLDS = {
    'small': 50 * 1024 * 1024,    # 50 MB
    'medium': 500 * 1024 * 1024,  # 500 MB
    'large': 2 * 1024 * 1024 * 1024  # 2 GB
}


def get_processing_strategy(file_size: int) -> str:
    """
    Determine processing strategy based on file size.
    
    Args:
        file_size: Size of file in bytes
        
    Returns:
        str: Processing strategy ('small', 'medium', 'large')
    """
    if file_size <= SIZE_THRESHOLDS['small']:
        return 'small'
    elif file_size <= SIZE_THRESHOLDS['medium']:
        return 'medium'
    else:
        return 'large'
