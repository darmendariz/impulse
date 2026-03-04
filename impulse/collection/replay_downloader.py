"""
High-level replay download orchestration.

Coordinates the download workflow: API client → Storage backend → Database tracking.
Provides progress reporting and error handling.
"""

import logging
from typing import Dict, Optional, List
from dataclasses import dataclass

from impulse.collection.ballchasing_client import BallchasingClient
from impulse.collection.storage import StorageBackend, LocalBackend
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
        storage: StorageBackend = LocalBackend(),
        db: Optional[ImpulseDB] = None
    ):
        """
        Initialize replay downloader.

        Args:
            client: Ballchasing API client
            storage: Storage backend (S3, local, etc.)
            db: Optional database for tracking (enables deduplication and resume)
        """
        self.client = client
        self.storage = storage
        self.db = db

    def download_group(
        self,
        group_id: str,
        path_prefix: Optional[List[str]] = None,
        include_root_in_path: bool = True,
        use_cache: bool = True,
        only_replay_ids: Optional[List[str]] = None,
        force: bool = False
    ) -> DownloadResult:
        """
        Download all replays from a Ballchasing group to storage.

        This method:
        1. Checks if the group was already fully downloaded (unless force=True)
        2. Builds the group tree (or loads from cache)
        3. Registers group and replays in database (if enabled)
        4. Downloads each replay from Ballchasing
        5. Saves to storage backend
        6. Updates database with completion status

        Args:
            group_id: Ballchasing group ID
            path_prefix: Optional prefix for storage paths (e.g., ['replays', 'rlcs'])
            include_root_in_path: Whether to include root group name in storage path
            use_cache: If True, load cached tree if available and save tree after building.
                       This saves API calls on retries. Default True.
            only_replay_ids: If provided, only download these specific replay IDs.
                            Used internally by retry_failed_downloads().
            force: If True, re-download even if the group is marked as complete in the
                   database. Default False.

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

        # Early exit: skip if already complete (only on full group downloads)
        if self.db and not only_replay_ids and not force:
            group_info = self.db.get_group_info(group_id)
            if group_info and group_info['download_status'] == 'complete':
                print(
                    f"Group '{group_info['name']}' already fully downloaded "
                    f"({group_info['successful_count']} replays, {group_info['completed_at']}). "
                    f"Use force=True to re-download."
                )
                return DownloadResult(
                    total_replays=group_info['replay_count'],
                    successful=0,
                    skipped=group_info['replay_count'],
                    failed=0,
                    total_bytes=0,
                    storage_keys=[],
                    failed_replays=[]
                )

        # Load or build group tree
        tree = None
        if use_cache:
            tree = load_group_tree(group_id)

        if tree is None:
            print(f"Building group tree for {group_id}...")
            tree = self.client.build_group_tree(group_id)
            if use_cache:
                save_group_tree(tree, group_id)

        # Flatten to replay list
        replay_list = flatten_group_tree(tree)
        total_in_tree = len(replay_list)

        # Filter to specific replay IDs if requested (for retries)
        if only_replay_ids:
            filter_set = set(only_replay_ids)
            replay_list = [(r, p) for r, p in replay_list if r['id'] in filter_set]

        total_replays = len(replay_list)

        # Register group and replays in database (skip if filtering for retry)
        if self.db and not only_replay_ids:
            storage_path = "/".join(path_prefix) if path_prefix else None
            self.db.register_group_start(
                tree['id'], tree['name'], total_in_tree,
                storage_path=storage_path,
                include_root_in_path=include_root_in_path
            )
            new_count = sum(
                1 for replay, _ in replay_list
                if self.db.add_replay(replay['id'], replay, group_id=tree['id'])
            )
            logger.info(f"Registered {new_count} new replays, {total_in_tree - new_count} already known")

        print(f"Downloading {total_replays} replays...")

        successful = 0
        skipped = 0
        failed = 0
        total_bytes = 0
        storage_keys = []
        failed_replays = []
        width = len(str(total_replays))
        root_name = tree.get('name', group_id)

        for i, (replay, group_path) in enumerate(replay_list, 1):
            replay_id = replay['id']
            counter = f"[{i:{width}}/{total_replays}]"

            components = (path_prefix.copy() if path_prefix else []) + build_path_components(
                group_path, root_name, include_root=include_root_in_path
            )
            storage_key = self.storage.get_storage_key(replay_id, components)

            # Check database first (resume capability)
            if self.db and self.db.is_replay_downloaded(replay_id):
                print(f"{counter} {replay_id}  skipped")
                skipped += 1
                continue

            # Check storage directly (double-check / sync)
            if self.storage.replay_exists(replay_id, components):
                size = self.storage.get_replay_size(replay_id, components)
                if self.db:
                    self.db.mark_downloaded(replay_id, storage_key, size)
                print(f"{counter} {replay_id}  skipped")
                skipped += 1
                continue

            # Download and save
            try:
                replay_bytes = self.client.download_replay_bytes(replay_id)
                file_size = len(replay_bytes)

                metadata = extract_replay_metadata(replay)
                metadata['group_id'] = group_id

                save_result = self.storage.save_replay(replay_id, replay_bytes, components, metadata)
                if not save_result['success']:
                    raise Exception(save_result.get('error', 'Storage save failed'))

                if self.db:
                    self.db.mark_downloaded(replay_id, storage_key, file_size)

                successful += 1
                total_bytes += file_size
                storage_keys.append(storage_key)

                mb = file_size / (1024 * 1024)
                print(f"{counter} {replay_id}  {mb:.2f} MB")

                if i % 50 == 0:
                    rl = self.client.get_rate_limit_status()
                    print(f"  [rate limit] {rl['requests_this_hour']}/200 requests used, "
                          f"resets in {rl['window_resets_in_minutes']:.0f} min")

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to download {replay_id}: {error_msg}")
                if self.db:
                    self.db.mark_replay_failed(replay_id, error_msg)
                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': error_msg})
                print(f"{counter} {replay_id}  FAILED: {error_msg}")

        # Finalize group status in database
        if self.db and not only_replay_ids:
            self.db.finalize_group_download(tree['id'], successful, failed, skipped)

        # Summary
        total_mb = total_bytes / (1024 ** 2)
        print(f"Done. Downloaded: {successful}  Skipped: {skipped}  Failed: {failed}  ({total_mb:.1f} MB)")

        stats_prefix = path_prefix if path_prefix else [sanitize_path_component(root_name)]
        storage_stats = self.storage.get_storage_stats(stats_prefix)
        print(f"Storage: {storage_stats.get('total_replays', 0)} files, "
              f"{storage_stats.get('total_gb', 0):.2f} GB total")

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
        path_prefix: Optional[List[str]] = None,
        include_root_in_path: Optional[bool] = None
    ) -> DownloadResult:
        """
        Retry all failed replays for a group, using stored state from the database.

        Looks up failed replays directly from the database so this can be called
        at any time after a partial download without needing to preserve the
        original DownloadResult.

        Path options (path_prefix, include_root_in_path) are loaded from the
        database record written during the original download. Override them here
        only if you intentionally want to store the retried replays in a different
        location.

        Args:
            group_id: Ballchasing group ID
            path_prefix: Override the stored path prefix (list of path components).
                         If None, the original path is restored from the database.
            include_root_in_path: Override the stored include_root_in_path setting.
                                  If None, the original setting is restored from the database.

        Returns:
            DownloadResult with statistics for the retry attempt

        Raises:
            ValueError: If no database is configured, or the group is not found.

        Example:
            >>> result = downloader.download_group('rlcs-2024-abc123')
            >>> # Later, in the same or a new session:
            >>> retry_result = downloader.retry_failed_downloads('rlcs-2024-abc123')
        """
        if not self.db:
            raise ValueError("A database is required to use retry_failed_downloads.")

        group_info = self.db.get_group_info(group_id)
        if not group_info:
            raise ValueError(
                f"Group '{group_id}' not found in database. "
                "Run download_group() first."
            )

        failed = self.db.get_failed_replays_for_group(group_id)
        if not failed:
            print(f"No failed replays for group '{group_info['name']}'.")
            return DownloadResult(
                total_replays=0, successful=0, skipped=0, failed=0,
                total_bytes=0, storage_keys=[], failed_replays=[]
            )

        failed_ids = [r['replay_id'] for r in failed]
        print(f"Retrying {len(failed_ids)} failed replay(s) for group '{group_info['name']}'...")

        # Restore original path settings from the database unless overridden
        if path_prefix is None and group_info.get('storage_path'):
            path_prefix = group_info['storage_path'].split('/')

        if include_root_in_path is None:
            include_root_in_path = bool(group_info.get('include_root_in_path', True))

        return self.download_group(
            group_id=group_id,
            path_prefix=path_prefix,
            include_root_in_path=include_root_in_path,
            use_cache=True,
            only_replay_ids=failed_ids
        )
