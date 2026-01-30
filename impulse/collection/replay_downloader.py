"""
High-level replay download orchestration.

Coordinates the download workflow: API client → Storage backend → Database tracking.
Provides progress reporting and error handling.
"""

import logging
from typing import Dict, Optional, List, Callable
from dataclasses import dataclass

from impulse.collection.ballchasing_client import BallchasingClient
from impulse.collection.storage import StorageBackend
from impulse.collection.database import ImpulseDB
from impulse.collection.utils import (
    flatten_group_tree,
    build_path_components,
    sanitize_path_component,
    extract_replay_metadata,
    save_group_tree,
    load_group_tree,
    delete_group_tree_cache
)

logger = logging.getLogger('impulse.collection')


@dataclass
class DownloadProgress:
    """Progress information for download operations."""
    current: int
    total: int
    replay_id: str
    status: str  # 'downloading', 'uploading', 'complete', 'skipped', 'failed'
    message: str
    storage_key: Optional[str] = None
    error: Optional[str] = None


@dataclass
class DownloadResult:
    """Result of a download operation."""
    total_replays: int
    successful: int
    skipped: int
    failed: int
    total_bytes: int
    storage_keys: List[str]
    failed_replays: List[Dict]  # List of {replay_id, error} dicts


class ReplayDownloader:
    """
    High-level replay download orchestrator.

    Coordinates downloads from Ballchasing to any storage backend, with
    database tracking and progress reporting.

    Example:
        >>> from impulse.collection import ReplayDownloader, BallchasingClient
        >>> from impulse.collection.storage import S3Backend
        >>> from impulse.collection.database import ImpulseDB
        >>>
        >>> client = BallchasingClient()
        >>> storage = S3Backend()
        >>> db = ImpulseDB()
        >>>
        >>> downloader = ReplayDownloader(client, storage, db)
        >>> result = downloader.download_group('rlcs-2024-abc123')
    """

    def __init__(
        self,
        client: BallchasingClient,
        storage: StorageBackend,
        db: Optional[ImpulseDB] = None,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None
    ):
        """
        Initialize replay downloader.

        Args:
            client: Ballchasing API client
            storage: Storage backend (S3, local, etc.)
            db: Optional database for tracking (enables deduplication and resume)
            progress_callback: Optional callback for progress updates
        """
        self.client = client
        self.storage = storage
        self.db = db
        self.progress_callback = progress_callback or self._default_progress_callback

    def _default_progress_callback(self, progress: DownloadProgress):
        """Default progress callback that logs to console."""
        indent = "  "
        print(f"[{progress.current}/{progress.total}] {progress.replay_id}")
        print(f"{indent}{progress.message}")

        if progress.storage_key:
            print(f"{indent}Storage key: {progress.storage_key}")

        if progress.error:
            print(f"{indent}Error: {progress.error}")

    def download_group(
        self,
        group_id: str,
        path_prefix: Optional[List[str]] = None,
        include_root_in_path: bool = True,
        use_cache: bool = True,
        only_replay_ids: Optional[List[str]] = None
    ) -> DownloadResult:
        """
        Download all replays from a Ballchasing group to storage.

        This method:
        1. Builds the group tree (or loads from cache)
        2. Registers replays in database (if enabled)
        3. Downloads each replay from Ballchasing
        4. Saves to storage backend
        5. Updates database with completion status

        Args:
            group_id: Ballchasing group ID
            path_prefix: Optional prefix for storage paths (e.g., ['replays', 'rlcs'])
            include_root_in_path: Whether to include root group name in storage path
            use_cache: If True, load cached tree if available and save tree after building.
                       This saves API calls on retries. Default True.
            only_replay_ids: If provided, only download these specific replay IDs.
                            Useful for retrying failed downloads.

        Returns:
            DownloadResult with statistics

        Example:
            >>> result = downloader.download_group(
            ...     'rlcs-2024-abc123',
            ...     path_prefix=['replays', 'rlcs', '2024']
            ... )
            >>> print(f"Downloaded {result.successful}/{result.total_replays} replays")
        """
        logger.info(f"Starting download for group: {group_id}")

        # Try to load cached tree first
        tree = None
        if use_cache:
            tree = load_group_tree(group_id)
            if tree:
                logger.info("Loaded group tree from cache")
                print("Using cached group tree (no API calls needed)")

        # Build group tree if not cached
        if tree is None:
            logger.info("Building group tree...")

            def tree_progress(message, depth):
                indent = "  " * depth
                print(f"{indent}{message}")

            tree = self.client.build_group_tree(group_id, progress_callback=tree_progress)

            # Save tree to cache for future retries
            if use_cache:
                cache_path = save_group_tree(tree, group_id)
                logger.info(f"Group tree cached at: {cache_path}")
                print(f"Group tree cached for future retries")

        # Flatten to replay list
        logger.info("Flattening tree structure...")
        replay_list = flatten_group_tree(tree)
        total_in_tree = len(replay_list)
        logger.info(f"Found {total_in_tree} total replays in tree")

        # Filter to specific replay IDs if requested (for retries)
        if only_replay_ids:
            filter_set = set(only_replay_ids)
            replay_list = [(r, p) for r, p in replay_list if r['id'] in filter_set]
            logger.info(f"Filtered to {len(replay_list)} replays for retry")
            print(f"Retrying {len(replay_list)} specific replays")

        total_replays = len(replay_list)

        # Register group and replays in database (skip if filtering for retry)
        if self.db and not only_replay_ids:
            self.db.register_group_download(tree['id'], tree['name'], total_in_tree)

            logger.info("Registering replays in database...")
            new_count = 0
            existing_count = 0

            for replay, _ in replay_list:
                is_new = self.db.add_replay(replay['id'], replay)
                if is_new:
                    new_count += 1
                else:
                    existing_count += 1

            print(f"  New replays: {new_count}")
            print(f"  Already in database: {existing_count}")

        # Download replays
        logger.info("Downloading replays...")
        print()
        print("=" * 60)
        print("DOWNLOADING REPLAYS")
        print("=" * 60)
        print()

        successful = 0
        skipped = 0
        failed = 0
        total_bytes = 0
        storage_keys = []
        failed_replays = []

        root_name = tree.get('name', group_id)

        for i, (replay, group_path) in enumerate(replay_list, 1):
            replay_id = replay['id']

            # Build storage path components
            if path_prefix:
                components = path_prefix.copy()
            else:
                components = []

            # Add group hierarchy to path
            path_parts = build_path_components(
                group_path,
                root_name,
                include_root=include_root_in_path
            )
            components.extend(path_parts)

            # Get storage key for this replay
            storage_key = self.storage.get_storage_key(replay_id, components)

            # Check database first (resume capability)
            if self.db and self.db.is_replay_downloaded(replay_id):
                self.progress_callback(DownloadProgress(
                    current=i,
                    total=total_replays,
                    replay_id=replay_id,
                    status='skipped',
                    message='Already downloaded (database check)',
                    storage_key=storage_key
                ))
                skipped += 1
                print()
                continue

            # Check storage directly (double-check)
            if self.storage.replay_exists(replay_id, components):
                size = self.storage.get_replay_size(replay_id, components)

                # Update database if it was out of sync
                if self.db:
                    self.db.mark_downloaded(replay_id, storage_key, size)

                self.progress_callback(DownloadProgress(
                    current=i,
                    total=total_replays,
                    replay_id=replay_id,
                    status='skipped',
                    message='Already in storage (direct check)',
                    storage_key=storage_key
                ))
                skipped += 1
                print()
                continue

            # Download and save replay
            try:
                # Download from Ballchasing
                self.progress_callback(DownloadProgress(
                    current=i,
                    total=total_replays,
                    replay_id=replay_id,
                    status='downloading',
                    message='Downloading from Ballchasing...'
                ))

                replay_bytes = self.client.download_replay_bytes(replay_id)
                file_size = len(replay_bytes)
                file_size_mb = file_size / (1024 * 1024)

                print(f"  Downloaded: {file_size_mb:.2f} MB")

                # Prepare metadata
                metadata = extract_replay_metadata(replay)
                metadata['group_id'] = group_id

                # Save to storage
                self.progress_callback(DownloadProgress(
                    current=i,
                    total=total_replays,
                    replay_id=replay_id,
                    status='uploading',
                    message='Saving to storage...'
                ))

                save_result = self.storage.save_replay(
                    replay_id,
                    replay_bytes,
                    components,
                    metadata
                )

                if not save_result['success']:
                    raise Exception(save_result.get('error', 'Storage save failed'))

                print(f"  Saved to storage")

                # Update database
                if self.db:
                    self.db.mark_downloaded(replay_id, storage_key, file_size)

                successful += 1
                total_bytes += file_size
                storage_keys.append(storage_key)

                self.progress_callback(DownloadProgress(
                    current=i,
                    total=total_replays,
                    replay_id=replay_id,
                    status='complete',
                    message='Complete',
                    storage_key=storage_key
                ))

                # Rate limit status every 50 replays
                if i % 50 == 0:
                    status = self.client.get_rate_limit_status()
                    print(f"\nRate Limit Status:")
                    print(f"  Requests this hour: {status['requests_this_hour']}")
                    print(f"  Window resets in: {status['window_resets_in_minutes']:.1f} minutes")
                    print()

                print()

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to download {replay_id}: {error_msg}")

                self.progress_callback(DownloadProgress(
                    current=i,
                    total=total_replays,
                    replay_id=replay_id,
                    status='failed',
                    message='Failed',
                    error=error_msg
                ))

                if self.db:
                    self.db.mark_replay_failed(replay_id, error_msg)

                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': error_msg})
                print()
                continue

        # Summary
        print()
        print("=" * 60)
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"Total replays: {total_replays}")
        print(f"Successfully downloaded: {successful}")
        print(f"Skipped (already had): {skipped}")
        print(f"Failed: {failed}")
        print(f"Total size: {total_bytes / (1024**2):.2f} MB ({total_bytes / (1024**3):.2f} GB)")

        # Storage statistics
        if path_prefix:
            stats_prefix = path_prefix
        else:
            stats_prefix = [sanitize_path_component(root_name)]

        storage_stats = self.storage.get_storage_stats(stats_prefix)
        print()
        print("STORAGE STATISTICS")
        print("-" * 60)
        print(f"Total objects in storage: {storage_stats.get('total_replays', 0)}")
        print(f"Total storage: {storage_stats.get('total_gb', 0):.2f} GB")

        # Database statistics
        if self.db:
            print()
            print("DATABASE STATISTICS")
            print("-" * 60)
            db_stats = self.db.get_stats()
            for key, value in db_stats.items():
                print(f"{key}: {value}")

        return DownloadResult(
            total_replays=total_replays,
            successful=successful,
            skipped=skipped,
            failed=failed,
            total_bytes=total_bytes,
            storage_keys=storage_keys,
            failed_replays=failed_replays
        )

    def retry_failed_downloads(
        self,
        group_id: str,
        failed_replays: List[Dict],
        path_prefix: Optional[List[str]] = None,
        include_root_in_path: bool = True
    ) -> DownloadResult:
        """
        Retry downloading failed replays using the cached group tree.

        A convenience wrapper around download_group() that accepts the
        failed_replays list from a previous DownloadResult.

        Args:
            group_id: Ballchasing group ID (must have a cached tree)
            failed_replays: The failed_replays list from a previous DownloadResult
                           (list of dicts with 'replay_id' key)
            path_prefix: Optional prefix for storage paths
            include_root_in_path: Whether to include root group name in storage path

        Returns:
            DownloadResult with statistics for the retry attempt

        Example:
            >>> result = downloader.download_group('rlcs-2024-abc123')
            >>> if result.failed_replays:
            ...     retry_result = downloader.retry_failed_downloads(
            ...         'rlcs-2024-abc123',
            ...         result.failed_replays
            ...     )
        """
        replay_ids = [r['replay_id'] for r in failed_replays]
        return self.download_group(
            group_id=group_id,
            path_prefix=path_prefix,
            include_root_in_path=include_root_in_path,
            use_cache=True,
            only_replay_ids=replay_ids
        )
