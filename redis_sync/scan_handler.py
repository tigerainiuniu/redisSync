"""
Redis SCAN命令处理器

使用Redis SCAN命令实现增量键迁移。
支持模式匹配、类型过滤和批处理。
"""

import redis
import logging
import time
from typing import Optional, Callable, List, Dict, Any, Iterator, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .sync_filters import KeySyncFilter

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from .key_sync import redis_values_equal, sync_key_with_dump_restore
from .exceptions import MigrationError

logger = logging.getLogger(__name__)
Key = Union[str, bytes]
MAX_PIPELINE_KEYS = 200


def _key_chunks(keys, batch_size: int = MAX_PIPELINE_KEYS):
    for offset in range(0, len(keys), batch_size):
        yield keys[offset:offset + batch_size]


def _type_str(val):
    return val.decode() if isinstance(val, bytes) else str(val)


class ScanHandler:
    """处理Redis SCAN命令进行增量键迁移。"""

    SCAN_MAX_RETRIES = 3
    SCAN_RETRY_DELAY = 0.1
    
    def __init__(self, source_client: redis.Redis, target_client: redis.Redis):
        """
        初始化SCAN处理器。

        参数:
            source_client: 源Redis客户端
            target_client: 目标Redis客户端
        """
        self.source_client = source_client
        self.target_client = target_client
        
    def scan_keys(self,
                  pattern: str = "*",
                  count: int = 1000,
                  key_type: Optional[str] = None) -> Iterator[List[Key]]:
        """
        使用SCAN命令从源Redis扫描键。

        参数:
            pattern: 要匹配的键模式
            count: 每次迭代返回的键数量
            key_type: 按键类型过滤 (string, list, set, zset, hash, stream)

        生成:
            匹配键的列表
        """
        cursor = 0
        while True:
            cursor, keys = self._scan_page(cursor, pattern, count)

            # Filter by type if specified
            if key_type and keys:
                filtered_keys = []
                for key in keys:
                    try:
                        if _type_str(self.source_client.type(key)) == key_type:
                            filtered_keys.append(key)
                    except Exception as e:
                        raise MigrationError(
                            f"Error checking type for key {key!r}: {e}"
                        ) from e
                keys = filtered_keys

            if keys:
                yield list(keys)

            if cursor == 0:
                break

    def _scan_page(self, cursor: int, pattern: str, count: int):
        last_error = None
        for attempt in range(1, self.SCAN_MAX_RETRIES + 1):
            try:
                return self.source_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=count,
                )
            except Exception as e:
                last_error = e
                if attempt < self.SCAN_MAX_RETRIES:
                    logger.warning(
                        "SCAN failed, retrying (%s/%s): %s",
                        attempt,
                        self.SCAN_MAX_RETRIES,
                        e,
                    )
                    if self.SCAN_RETRY_DELAY > 0:
                        time.sleep(self.SCAN_RETRY_DELAY * attempt)
        raise MigrationError(
            f"SCAN failed {self.SCAN_MAX_RETRIES} times: {last_error}"
        ) from last_error
    
    def migrate_keys(self, 
                    keys: List[Key],
                    batch_size: int = 100,
                    preserve_ttl: bool = True,
                    overwrite: bool = False,
                    progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        """
        Migrate a list of keys from source to target.
        
        Args:
            keys: List of keys to migrate
            batch_size: Number of keys to process in each batch
            preserve_ttl: Whether to preserve TTL values
            overwrite: Whether to overwrite existing keys
            progress_callback: Optional callback for progress updates
            
        Returns:
            Migration statistics
        """
        stats = {
            'total_keys': len(keys),
            'migrated_keys': 0,
            'failed_keys': 0,
            'skipped_keys': 0,
            'errors': []
        }
        
        logger.info(f"Starting migration of {len(keys)} keys")
        
        # Process keys in batches
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i + batch_size]
            batch_stats = self._migrate_batch(batch, preserve_ttl, overwrite)
            
            # Update statistics
            stats['migrated_keys'] += batch_stats['migrated']
            stats['failed_keys'] += batch_stats['failed']
            stats['skipped_keys'] += batch_stats['skipped']
            stats['errors'].extend(batch_stats['errors'])
            
            # Progress callback
            if progress_callback:
                progress_callback(stats['migrated_keys'], stats['total_keys'])
        
        logger.info(f"Migration completed. Migrated: {stats['migrated_keys']}, "
                   f"Failed: {stats['failed_keys']}, Skipped: {stats['skipped_keys']}")

        stats['success'] = stats['failed_keys'] == 0
        return stats
    
    def migrate_all_keys(self,
                        pattern: str = "*",
                        key_type: Optional[str] = None,
                        batch_size: int = 100,
                        scan_count: int = 1000,
                        preserve_ttl: bool = True,
                        overwrite: bool = False,
                        progress_callback: Optional[Callable[[int, int], None]] = None,
                        key_filter: Optional["KeySyncFilter"] = None) -> Dict[str, Any]:
        """
        Migrate all keys matching pattern from source to target.
        
        Args:
            pattern: Key pattern to match
            key_type: Filter by key type
            batch_size: Number of keys to process in each batch
            scan_count: Number of keys to return per SCAN iteration
            preserve_ttl: Whether to preserve TTL values
            overwrite: Whether to overwrite existing keys
            progress_callback: Optional callback for progress updates
            
        Returns:
            Migration statistics
        """
        stats = {
            'total_keys': 0,
            'migrated_keys': 0,
            'failed_keys': 0,
            'skipped_keys': 0,
            'errors': []
        }
        
        logger.info(f"Starting full migration with pattern: {pattern}")
        
        def _apply_filter(batch: List[Key]) -> List[Key]:
            if not key_filter or not batch:
                return batch
            return list(key_filter.filter_batch(self.source_client, list(batch)))

        # First pass: count total keys for progress tracking
        total_keys = 0
        if progress_callback:
            logger.info("Counting total keys...")
            for key_batch in self.scan_keys(pattern, scan_count, key_type):
                total_keys += len(_apply_filter(key_batch))
            stats['total_keys'] = total_keys
            logger.info(f"Found {total_keys} keys to migrate")
        
        # Second pass: migrate keys
        processed_keys = 0
        for key_batch in self.scan_keys(pattern, scan_count, key_type):
            if not key_batch:
                continue
            key_batch = _apply_filter(key_batch)
            if not key_batch:
                continue
                
            batch_stats = self._migrate_batch(key_batch, preserve_ttl, overwrite)
            
            # Update statistics
            stats['migrated_keys'] += batch_stats['migrated']
            stats['failed_keys'] += batch_stats['failed']
            stats['skipped_keys'] += batch_stats['skipped']
            stats['errors'].extend(batch_stats['errors'])
            
            processed_keys += len(key_batch)
            
            # Progress callback
            if progress_callback:
                progress_callback(processed_keys, total_keys or processed_keys)
        
        if not progress_callback:
            stats['total_keys'] = processed_keys
        
        logger.info(f"Full migration completed. Migrated: {stats['migrated_keys']}, "
                   f"Failed: {stats['failed_keys']}, Skipped: {stats['skipped_keys']}")

        stats['success'] = stats['failed_keys'] == 0
        return stats
    
    def _migrate_batch(self, 
                      keys: List[Key],
                      preserve_ttl: bool = True,
                      overwrite: bool = False) -> Dict[str, Any]:
        """
        Migrate a batch of keys.
        
        Args:
            keys: List of keys to migrate
            preserve_ttl: Whether to preserve TTL values
            overwrite: Whether to overwrite existing keys
            
        Returns:
            Batch migration statistics
        """
        stats = {
            'migrated': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        for key in keys:
            try:
                success = self._migrate_single_key(key, preserve_ttl, overwrite)
                if success:
                    stats['migrated'] += 1
                else:
                    stats['skipped'] += 1
                    
            except Exception as e:
                stats['failed'] += 1
                error_msg = f"Failed to migrate key '{key}': {e}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)
        
        return stats
    
    def _migrate_single_key(self, 
                           key: Key,
                           preserve_ttl: bool = True,
                           overwrite: bool = False) -> bool:
        """
        Migrate a single key from source to target.
        
        Args:
            key: Key to migrate
            preserve_ttl: Whether to preserve TTL
            overwrite: Whether to overwrite existing key
            
        Returns:
            True if migrated successfully, False if skipped
        """
        try:
            if not overwrite and self.target_client.exists(key):
                logger.debug("Key '%s' already exists in target, skipping", key)
                return False

            if not self.source_client.exists(key):
                logger.warning("Key '%s' doesn't exist in source", key)
                return False

            migrated = sync_key_with_dump_restore(
                self.source_client,
                self.target_client,
                key,
                overwrite=overwrite,
                preserve_ttl=preserve_ttl,
            )
            if migrated:
                logger.debug("Successfully migrated key '%s'", key)
            return migrated

        except Exception as e:
            logger.error("Error migrating key '%s': %s", key, e)
            raise
    
    def compare_keys(self,
                    pattern: str = "*",
                    sample_size: Optional[int] = None,
                    use_fast_mode: bool = True,
                    key_types: Optional[List[str]] = None,
                    key_filter: Optional["KeySyncFilter"] = None) -> Dict[str, Any]:
        """
        Compare keys between source and target Redis instances.

        Args:
            pattern: Key pattern to compare
            sample_size: Limit comparison to a sample of keys
            use_fast_mode: Use fast mode (Pipeline batch comparison, only check existence and type)

        Returns:
            Comparison results
        """
        logger.info(f"Comparing keys with pattern: {pattern}, fast_mode: {use_fast_mode}")

        if use_fast_mode:
            return self._compare_keys_fast(
                pattern, sample_size, key_types=key_types, key_filter=key_filter
            )
        else:
            return self._compare_keys_full(
                pattern, sample_size, key_types=key_types, key_filter=key_filter
            )

    def _verification_batches(
        self,
        pattern: str,
        key_types: Optional[List[str]],
        key_filter: Optional["KeySyncFilter"],
    ):
        allowed_types = set(key_types or [])
        for raw_batch in self.scan_keys(pattern):
            batch = list(raw_batch)
            if allowed_types and batch:
                typed_keys = []
                for chunk in _key_chunks(batch):
                    pipe = self.source_client.pipeline(transaction=False)
                    for key in chunk:
                        pipe.type(key)
                    raw_types = pipe.execute()
                    for value in raw_types:
                        if isinstance(value, BaseException):
                            raise value
                    typed_keys.extend(
                        key
                        for key, key_type in zip(chunk, raw_types)
                        if _type_str(key_type) in allowed_types
                    )
                batch = typed_keys
            if key_filter and batch:
                batch = list(key_filter.filter_batch(self.source_client, batch))
            if batch:
                yield batch

    def _compare_keys_fast(
        self,
        pattern: str,
        sample_size: Optional[int],
        key_types: Optional[List[str]] = None,
        key_filter: Optional["KeySyncFilter"] = None,
    ) -> Dict[str, Any]:
        """快速验证模式：只检查键是否存在和类型是否匹配（使用Pipeline批量处理）"""
        results = {
            'total_compared': 0,
            'matching_keys': 0,
            'missing_in_target': 0,
            'type_mismatches': 0,
            'errors': []
        }

        compared_count = 0
        batch_size = MAX_PIPELINE_KEYS

        for key_batch in self._verification_batches(pattern, key_types, key_filter):
            if sample_size and compared_count >= sample_size:
                break

            for offset in range(0, len(key_batch), batch_size):
                keys_to_check = key_batch[offset:offset + batch_size]
                if sample_size:
                    remaining = sample_size - compared_count
                    keys_to_check = keys_to_check[:remaining]
                if not keys_to_check:
                    break
                try:
                    # 使用Pipeline批量检查目标Redis
                    target_pipe = self.target_client.pipeline(transaction=False)
                    for key in keys_to_check:
                        target_pipe.exists(key)
                        target_pipe.type(key)

                    target_results = target_pipe.execute()

                # 使用Pipeline批量检查源Redis（只获取类型）
                    source_pipe = self.source_client.pipeline(transaction=False)
                    for key in keys_to_check:
                        source_pipe.type(key)

                    source_types = source_pipe.execute()

                # 解析结果
                    for i, key in enumerate(keys_to_check):
                        exists_in_target = target_results[i * 2]
                        target_type = target_results[i * 2 + 1]
                        source_type = source_types[i]

                        results['total_compared'] += 1
                        compared_count += 1

                        command_errors = [
                            (command, value)
                            for command, value in (
                                ('target EXISTS', exists_in_target),
                                ('target TYPE', target_type),
                                ('source TYPE', source_type),
                            )
                            if isinstance(value, BaseException)
                        ]
                        if command_errors:
                            details = '; '.join(
                                f"{command}: {error}"
                                for command, error in command_errors
                            )
                            results['errors'].append(
                                f"Error comparing key {key!r}: {details}"
                            )
                            continue

                        st = _type_str(source_type)
                        tt = _type_str(target_type)

                        if not exists_in_target:
                            results['missing_in_target'] += 1
                        elif st != tt:
                            results['type_mismatches'] += 1
                        else:
                            results['matching_keys'] += 1

                except Exception as e:
                    error_msg = f"Error comparing batch: {e}"
                    results['errors'].append(error_msg)
                    results['total_compared'] += len(keys_to_check)
                    compared_count += len(keys_to_check)
                    logger.error(error_msg)

                if sample_size and compared_count >= sample_size:
                    break

        logger.info(f"Fast comparison completed. Compared {results['total_compared']} keys")
        return results

    def _compare_keys_full(
        self,
        pattern: str,
        sample_size: Optional[int],
        key_types: Optional[List[str]] = None,
        key_filter: Optional["KeySyncFilter"] = None,
    ) -> Dict[str, Any]:
        """完整验证模式：检查键存在、类型、值（逐个处理，慢）"""
        results = {
            'total_compared': 0,
            'matching_keys': 0,
            'missing_in_target': 0,
            'value_mismatches': 0,
            'ttl_mismatches': 0,
            'type_mismatches': 0,
            'errors': []
        }

        compared_count = 0
        for key_batch in self._verification_batches(pattern, key_types, key_filter):
            for key in key_batch:
                if sample_size and compared_count >= sample_size:
                    break

                try:
                    comparison = self._compare_single_key(key)
                    results['total_compared'] += 1

                    if comparison['exists_in_target']:
                        if (
                            comparison['values_match']
                            and comparison['types_match']
                            and comparison['ttl_match']
                        ):
                            results['matching_keys'] += 1
                        else:
                            if not comparison['values_match']:
                                results['value_mismatches'] += 1
                            if not comparison['types_match']:
                                results['type_mismatches'] += 1

                        if not comparison['ttl_match']:
                            results['ttl_mismatches'] += 1
                    else:
                        results['missing_in_target'] += 1

                    compared_count += 1

                except Exception as e:
                    error_msg = f"Error comparing key '{key}': {e}"
                    results['errors'].append(error_msg)
                    results['total_compared'] += 1
                    compared_count += 1
                    logger.error(error_msg)

            if sample_size and compared_count >= sample_size:
                break

        logger.info(f"Full comparison completed. Compared {results['total_compared']} keys")
        return results
    
    def _compare_single_key(self, key: str) -> Dict[str, Any]:
        """Compare a single key between source and target."""
        result = {
            'exists_in_target': False,
            'types_match': False,
            'values_match': False,
            'ttl_match': False
        }
        
        # Check if key exists in target
        if not self.target_client.exists(key):
            return result
        
        result['exists_in_target'] = True
        
        # Compare types
        source_type = _type_str(self.source_client.type(key))
        target_type = _type_str(self.target_client.type(key))
        result['types_match'] = source_type == target_type
        
        if not result['types_match']:
            return result
        
        result['values_match'] = redis_values_equal(
            self.source_client,
            self.target_client,
            key,
            key_type=source_type,
        )
        
        # Compare TTL
        source_ttl = self.source_client.ttl(key)
        target_ttl = self.target_client.ttl(key)
        # Allow some tolerance for TTL comparison (within 2 seconds)
        result['ttl_match'] = abs(source_ttl - target_ttl) <= 2 if source_ttl > 0 and target_ttl > 0 else source_ttl == target_ttl
        
        return result
