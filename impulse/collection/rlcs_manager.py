"""
RLCS season management and download orchestration.

This module provides a high-level interface for downloading complete RLCS seasons
from Ballchasing to local or S3 storage.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import json
from pathlib import Path

from impulse.collection.storage import S3Backend


class RLCSManager:
    """
    Manager for RLCS season downloads and metadata.

    Provides a unified interface for downloading complete RLCS seasons to local
    or S3 storage, with support for dry-run mode, progress tracking, and logging.

    Usage:
        # Download to S3
        rlcs = RLCSManager(storage_type='s3')
        rlcs.download_season('2024')

        # Download to local storage
        rlcs = RLCSManager(storage_type='local', output_dir='./replays')
        rlcs.download_season('2024')

        # List available seasons
        rlcs.list_seasons()

        # Get season info without downloading
        info = RLCSManager.get_season_info('2024')
    """

    # Average replay size in MB (loose estimate, updated 12-16-2025)
    # TODO: Update with computed average from actual downloads
    AVG_REPLAY_SIZE_MB = 1.8

    # RLCS Season Ballchasing Group IDs
    SEASONS = {
        '21-22': {
            'group_id': 'rlcs-21-22-jl7xcwxrpc',
            'name': 'RLCS 2021-2022',
            'estimated_replay_count': 5915,
            'estimated_size_gb': 5915 * AVG_REPLAY_SIZE_MB / 1000,
            'is_active': False,
            'last_updated': '2025-12-16'
        },
        '22-23': {
            'group_id': 'rlcs-22-23-jjc408bdu4',
            'name': 'RLCS 2022-2023',
            'estimated_replay_count': 15443,
            'estimated_size_gb': 15443 * AVG_REPLAY_SIZE_MB / 1000,
            'is_active': False,
            'last_updated': '2025-12-16'
        },
        '2024': {
            'group_id': 'rlcs-2024-jsvrszynst',
            'name': 'RLCS 2024',
            'estimated_replay_count': 7324,
            'estimated_size_gb': 7324 * AVG_REPLAY_SIZE_MB / 1000,
            'is_active': False,
            'last_updated': '2025-12-16'
        },
        '2025': {
            'group_id': 'rlcs-2025-7ielfd7uhx',
            'name': 'RLCS 2025',
            'estimated_replay_count': 7038,
            'estimated_size_gb': 7038 * AVG_REPLAY_SIZE_MB / 1000,
            'is_active': False,
            'last_updated': '2025-12-16'
        },
        '2026': {
            'group_id': 'rlcs-2026-d3chsz8nje',
            'name': 'RLCS 2026',
            'estimated_replay_count': 834,
            'estimated_size_gb': 834 * AVG_REPLAY_SIZE_MB / 1024,
            'is_active': True,
            'last_updated': '2025-12-16'
        }
    }

    def __init__(
        self,
        storage_type: str = 's3',
        path_prefix: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
        use_database: bool = True
    ):
        """
        Initialize RLCS manager.

        Args:
            storage_type: Storage backend ('s3' or 'local')
            path_prefix: S3 path prefix (default: ['replays', 'rlcs'])
            output_dir: Local output directory (required if storage_type='local')
            use_database: Whether to use database for tracking (default: True)
        """
        if storage_type not in ['s3', 'local']:
            raise ValueError(f"storage_type must be 's3' or 'local', got: {storage_type}")

        if storage_type == 'local' and not output_dir:
            raise ValueError("output_dir is required when storage_type='local'")

        self.storage_type = storage_type
        self.path_prefix = path_prefix or ['replays', 'rlcs']
        self.output_dir = output_dir
        self.use_database = use_database

    @classmethod
    def get_season_info(cls, season_key: str) -> Dict[str, Any]:
        """
        Get metadata for a specific season.

        Args:
            season_key: Season identifier (e.g., '2024', '21-22')

        Returns:
            Dictionary with season metadata

        Raises:
            KeyError: If season not found
        """
        if season_key not in cls.SEASONS:
            available = ', '.join(cls.SEASONS.keys())
            raise KeyError(f"Season '{season_key}' not found. Available: {available}")

        return cls.SEASONS[season_key]

    @classmethod
    def get_available_seasons(cls) -> List[str]:
        """
        Get list of available season keys.

        Returns:
            List of season identifiers
        """
        return list(cls.SEASONS.keys())

    def print_season_info(self, season_key: str) -> None:
        """
        Print detailed information about a season.

        Args:
            season_key: Season identifier (e.g., '2024')
        """
        season = self.get_season_info(season_key)

        print("=" * 60)
        print(f"RLCS {season_key} Season Download")
        print("=" * 60)
        print(f"Season Name: {season['name']}")
        print(f"Group ID: {season['group_id']}")
        print(f"Estimated Replays: {season['estimated_replay_count']:,}")
        print(f"Estimated Size: {season['estimated_size_gb']:.1f} GB")
        print(f"Active Season: {season['is_active']} (as of {season['last_updated']})")
        print()

    def list_seasons(self) -> None:
        """Print information about all available seasons."""
        print()
        print("=" * 60)
        print("Available RLCS Seasons:")
        for season_key, season_data in self.SEASONS.items():
            print(f"\n  Season Key: {season_key}")
            print(f"  Season Name: {season_data['name']}")
            print(f"  Group ID: {season_data['group_id']}")
            print(f"  Estimated replay count: {season_data['estimated_replay_count']:,} replays")
            print(f"  Estimated total download size: {season_data['estimated_size_gb']:.1f} GB")
            print(f"  Active Season: {season_data['is_active']} (as of {season_data['last_updated']})")
        print("\nUse download_season(season_key) to download a specific season.")
        print()
        print("=" * 60)

    def download_season(
        self,
        season_key: str,
        dry_run: bool = False,
        confirm: bool = True,
        storage_type: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> Optional[Any]:
        """
        Download a complete RLCS season.

        Args:
            season_key: Season identifier (e.g., '2024')
            dry_run: If True, preview without downloading
            confirm: If True, prompt for user confirmation (default: True)
            storage_type: Override instance storage_type ('s3' or 'local')
            output_dir: Override instance output_dir (for local storage)

        Returns:
            Download result object if successful, None if cancelled/dry-run
        """
        # Validate season
        try:
            season = self.get_season_info(season_key)
        except KeyError as e:
            print(f"✗ {e}")
            return None

        # Determine storage configuration
        storage = storage_type or self.storage_type
        out_dir = output_dir or self.output_dir

        if storage == 'local' and not out_dir:
            raise ValueError("output_dir is required for local storage")

        # Print season info
        self.print_season_info(season_key)

        if dry_run:
            print("DRY RUN MODE - No actual download")
            print(f"Storage: {storage}")
            if storage == 'local':
                print(f"Output directory: {out_dir}")
            else:
                print(f"S3 path prefix: {'/'.join(self.path_prefix + [season_key])}")
            return None

        # Confirm with user
        if confirm:
            print(f"WARNING: This will download {season['estimated_replay_count']} replays to {storage} storage."
                  f"Estimated download size: {season['estimated_size_gb']:.1f} GB!")
            if storage == 's3':
                print("Make sure you're running on EC2 (not locally)")
            print()
            response = input("Continue? (yes/no): ")

            if response.lower() not in ['yes', 'y']:
                print("Download cancelled")
                return None

        # Log start time
        start_time = datetime.now(timezone.utc)
        print(f"\nStarted: {start_time.isoformat()}")
        print()

        try:
            # Import here to avoid circular dependency
            from impulse.collection import download_group

            # Download based on storage type
            if storage == 'local':
                result = download_group(
                    group_id=season['group_id'],
                    storage_type='local',
                    output_dir=out_dir,
                    use_database=self.use_database
                )
            else:  # s3
                path_prefix = self.path_prefix + [season_key]
                result = download_group(
                    group_id=season['group_id'],
                    storage_type='s3',
                    path_prefix=path_prefix,
                    use_database=self.use_database
                )

            # Log completion
            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time

            self._print_completion_summary(start_time, end_time, duration, result)

            # Save completion log
            log_file = self._save_completion_log(
                season_key, season, start_time, end_time, duration, result
            )

            # Upload log to S3 if using S3 storage
            if storage == 's3':
                try:
                    s3_backend = S3Backend()
                    s3_backend.s3_manager.upload_file(log_file, f"logs/{log_file}")
                    print(f"Log backed up to S3")
                except Exception as e:
                    print(f"Warning: Could not upload log to S3: {e}")

            return result

        except KeyboardInterrupt:
            print("\n\n   Download interrupted by user")
            print("Run the same command again to resume (database tracks progress)")
            return None
        except Exception as e:
            print(f"\n\n Download failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _print_completion_summary(
        self,
        start_time: datetime,
        end_time: datetime,
        duration: Any,
        result: Any
    ) -> None:
        """Print download completion summary."""
        print()
        print("=" * 60)
        print("DOWNLOAD COMPLETE")
        print("=" * 60)
        print(f"Started: {start_time.isoformat()}")
        print(f"Finished: {end_time.isoformat()}")
        print(f"Duration: {duration}")
        print()
        print(f"Total replays: {result.total_replays}")
        print(f"Successfully uploaded: {result.successful}")
        print(f"Skipped: {result.skipped}")
        print(f"Failed: {result.failed}")
        print(f"Total size: {result.total_bytes / (1024**3):.2f} GB")
        print()

    def _save_completion_log(
        self,
        season_key: str,
        season: Dict[str, Any],
        start_time: datetime,
        end_time: datetime,
        duration: Any,
        result: Any
    ) -> str:
        """
        Save download completion log to JSON file.

        Returns:
            Path to saved log file
        """
        log_entry = {
            'season': season_key,
            'group_id': season['group_id'],
            'started': start_time.isoformat(),
            'finished': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
            'storage_type': self.storage_type,
            'results': {
                'total_replays': result.total_replays,
                'successful': result.successful,
                'skipped': result.skipped,
                'failed': result.failed,
                'total_bytes': result.total_bytes,
                'failed_replays': result.failed_replays
            }
        }

        log_file = f"download_log_{season_key}_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, 'w') as f:
            json.dump(log_entry, f, indent=2)

        print(f"\nLog saved: {log_file}")
        return log_file
