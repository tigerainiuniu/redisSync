"""
Redis Sync工具的实用函数。

提供错误处理、重试和数据验证的通用工具。
"""

import time
import logging
import functools
from typing import Any, Callable, Optional, Type, Union, List
from contextlib import contextmanager

from .exceptions import RedisSyncError, TimeoutError

logger = logging.getLogger(__name__)


def retry_on_exception(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Union[Type[Exception], tuple] = Exception,
    on_retry: Optional[Callable[[int, Exception], None]] = None
):
    """
    异常时重试函数的装饰器。

    参数:
        max_retries: 最大重试次数
        delay: 重试间隔的初始延迟（秒）
        backoff_factor: 每次重试后延迟的倍增因子
        exceptions: 要捕获并重试的异常类型
        on_retry: 每次重试时调用的回调函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    
                    if on_retry:
                        on_retry(attempt + 1, e)
                    
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
            
            # This should never be reached, but just in case
            raise last_exception
        
        return wrapper
    return decorator


@contextmanager
def timeout_context(timeout_seconds: float, operation_name: str = "Operation"):
    """
    Context manager for operation timeout.
    
    Args:
        timeout_seconds: Timeout in seconds
        operation_name: Name of the operation for error messages
    """
    import signal
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"{operation_name} timed out after {timeout_seconds} seconds")
    
    # Set up the timeout
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(int(timeout_seconds))
    
    try:
        yield
    finally:
        # Cancel the timeout
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def validate_redis_key(key: str) -> bool:
    """
    Validate Redis key format.
    
    Args:
        key: Redis key to validate
        
    Returns:
        True if key is valid, False otherwise
    """
    if not key:
        return False
    
    # Redis keys can be any binary string, but we'll check for common issues
    if len(key) > 512 * 1024 * 1024:  # 512MB limit
        return False
    
    # Check for null bytes (not allowed in Redis keys)
    if '\x00' in key:
        return False
    
    return True


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes value in human-readable format.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    格式化持续时间为可读格式。

    参数:
        seconds: 持续时间（秒）

    返回:
        格式化的字符串（例如："1小时30分45秒"）
    """
    if seconds < 60:
        return f"{seconds:.1f}秒"

    minutes = int(seconds // 60)
    seconds = seconds % 60

    if minutes < 60:
        return f"{minutes}分{seconds:.1f}秒"

    hours = minutes // 60
    minutes = minutes % 60

    if hours < 24:
        return f"{hours}小时{minutes}分{seconds:.0f}秒"

    days = hours // 24
    hours = hours % 24

    return f"{days}天{hours}小时{minutes}分"


def calculate_progress_rate(current: int, total: int, elapsed_time: float) -> tuple:
    """
    Calculate progress rate and estimated time remaining.
    
    Args:
        current: Current progress
        total: Total items
        elapsed_time: Elapsed time in seconds
        
    Returns:
        Tuple of (rate_per_second, eta_seconds)
    """
    if elapsed_time <= 0 or current <= 0:
        return 0.0, None
    
    rate = current / elapsed_time
    remaining = total - current
    
    if rate <= 0:
        return rate, None
    
    eta = remaining / rate
    return rate, eta


def safe_redis_command(client, command: str, *args, **kwargs) -> Any:
    """
    Execute Redis command with error handling.
    
    Args:
        client: Redis client
        command: Redis command name
        *args: Command arguments
        **kwargs: Additional keyword arguments
        
    Returns:
        Command result or None if failed
    """
    try:
        return client.execute_command(command, *args, **kwargs)
    except Exception as e:
        logger.error(f"Redis command {command} failed: {e}")
        return None


def batch_process(items: List[Any], batch_size: int, processor: Callable[[List[Any]], Any]):
    """
    Process items in batches.
    
    Args:
        items: List of items to process
        batch_size: Size of each batch
        processor: Function to process each batch
        
    Yields:
        Results from processor function
    """
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        try:
            yield processor(batch)
        except Exception as e:
            logger.error(f"Batch processing failed for batch {i//batch_size + 1}: {e}")
            raise


def sanitize_key_for_logging(key: str, max_length: int = 100) -> str:
    """
    Sanitize Redis key for safe logging.
    
    Args:
        key: Redis key
        max_length: Maximum length for logged key
        
    Returns:
        Sanitized key string
    """
    if not key:
        return "<empty>"
    
    # Replace non-printable characters
    sanitized = ''.join(c if c.isprintable() else f'\\x{ord(c):02x}' for c in key)
    
    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length - 3] + "..."
    
    return sanitized


def get_redis_memory_usage(client) -> dict:
    """
    Get Redis memory usage information.
    
    Args:
        client: Redis client
        
    Returns:
        Dictionary with memory usage information
    """
    try:
        info = client.info('memory')
        return {
            'used_memory': info.get('used_memory', 0),
            'used_memory_human': info.get('used_memory_human', '0B'),
            'used_memory_peak': info.get('used_memory_peak', 0),
            'used_memory_peak_human': info.get('used_memory_peak_human', '0B'),
            'total_system_memory': info.get('total_system_memory', 0),
            'total_system_memory_human': info.get('total_system_memory_human', '0B'),
            'memory_fragmentation_ratio': info.get('mem_fragmentation_ratio', 0.0)
        }
    except Exception as e:
        logger.error(f"Failed to get memory usage: {e}")
        return {}


def estimate_key_count(client, pattern: str = "*", sample_size: int = 1000) -> int:
    """
    Estimate total number of keys matching pattern.
    
    Args:
        client: Redis client
        pattern: Key pattern
        sample_size: Number of keys to sample for estimation
        
    Returns:
        Estimated key count
    """
    try:
        # Get total key count
        total_keys = client.dbsize()
        
        if pattern == "*":
            return total_keys
        
        # Sample keys to estimate pattern matches
        cursor = 0
        sampled_keys = 0
        matching_keys = 0
        
        while cursor != 0 or sampled_keys == 0:
            cursor, keys = client.scan(cursor=cursor, count=min(100, sample_size - sampled_keys))
            
            for key in keys:
                sampled_keys += 1
                # Simple pattern matching (could be improved)
                if pattern == "*" or pattern in key.decode() if isinstance(key, bytes) else pattern in key:
                    matching_keys += 1
                
                if sampled_keys >= sample_size:
                    break
            
            if sampled_keys >= sample_size:
                break
        
        if sampled_keys == 0:
            return 0
        
        # Estimate based on sample
        match_ratio = matching_keys / sampled_keys
        estimated_count = int(total_keys * match_ratio)
        
        logger.debug(f"Estimated {estimated_count} keys matching pattern '{pattern}' "
                    f"(sampled {sampled_keys} keys, {matching_keys} matches)")
        
        return estimated_count
        
    except Exception as e:
        logger.error(f"Failed to estimate key count: {e}")
        return 0


class ProgressTracker:
    """Track progress of long-running operations."""
    
    def __init__(self, total: int, operation_name: str = "Operation"):
        self.total = total
        self.current = 0
        self.operation_name = operation_name
        self.start_time = time.time()
        self.last_update = self.start_time
        
    def update(self, increment: int = 1):
        """Update progress."""
        self.current += increment
        self.last_update = time.time()
        
    def get_stats(self) -> dict:
        """Get current progress statistics."""
        elapsed = time.time() - self.start_time
        rate, eta = calculate_progress_rate(self.current, self.total, elapsed)
        
        return {
            'current': self.current,
            'total': self.total,
            'percentage': (self.current / max(self.total, 1)) * 100,
            'elapsed_time': elapsed,
            'rate_per_second': rate,
            'eta_seconds': eta,
            'eta_formatted': format_duration(eta) if eta else None
        }
    
    def log_progress(self, log_interval: float = 10.0):
        """Log progress if enough time has passed."""
        now = time.time()
        if now - self.last_update >= log_interval:
            stats = self.get_stats()
            logger.info(f"{self.operation_name}: {stats['current']}/{stats['total']} "
                       f"({stats['percentage']:.1f}%) - "
                       f"Rate: {stats['rate_per_second']:.1f}/s - "
                       f"ETA: {stats['eta_formatted'] or 'Unknown'}")
            self.last_update = now
