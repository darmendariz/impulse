"""
Script to download full RLCS season replay files.

Provides a command-line interface for downloading complete RLCS
seasons from Ballchasing to local or S3 storage using the RLCSManager.

Usage:
    # List available seasons for download
    python rlcs_downloader.py --list

    # Download to S3 (default)
    python rlcs_downloader.py --season 2024
    python rlcs_downloader.py --season 2024 --dry-run  # Preview only

    # Download to local storage
    python rlcs_downloader.py --season 2024 --storage local --output-dir ./replays

    # Show estimates only
    python rlcs_downloader.py --season 2024 --estimate-only
"""

import argparse
from impulse.collection import RLCSManager


def main():
    parser = argparse.ArgumentParser(
        description="Download complete RLCS seasons from Ballchasing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available seasons
  python rlcs_downloader.py --list

  # Download to S3
  python rlcs_downloader.py --season 2024
  python rlcs_downloader.py --season 2024 --dry-run

  # Download to local storage
  python rlcs_downloader.py --season 2024 --storage local --output-dir ./replays

  # Show estimates
  python rlcs_downloader.py --season 2024 --estimate-only
        """
    )

    parser.add_argument(
        '--season',
        type=str,
        help='Season year to download (e.g., 2024, 21-22)'
    )
    parser.add_argument(
        '--storage',
        type=str,
        default='s3',
        choices=['s3', 'local'],
        help='Storage backend (default: s3)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for local storage (required if --storage local)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be downloaded without downloading'
    )
    parser.add_argument(
        '--estimate-only',
        action='store_true',
        help='Only show estimates'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available seasons'
    )
    parser.add_argument(
        '--no-confirm',
        action='store_true',
        help='Skip confirmation prompt (use with caution)'
    )

    args = parser.parse_args()

    # Handle list command
    if args.list:
        manager = RLCSManager()
        manager.list_seasons()
        return

    # Validate season is provided for download/estimate commands
    if not args.season:
        parser.print_help()
        return

    # Validate local storage requirements
    if args.storage == 'local' and not args.output_dir and not args.estimate_only:
        parser.error('--output-dir is required when using --storage local')

    # Create RLCSManager instance
    try:
        if args.storage == 'local':
            manager = RLCSManager(
                storage_type='local',
                output_dir=args.output_dir
            )
        else:  # s3
            manager = RLCSManager(storage_type='s3')

    except ValueError as e:
        print(f" Configuration error: {e}")
        return

    # Handle estimate-only command
    if args.estimate_only:
        manager.print_season_info(args.season)
        return

    # Download season
    manager.download_season(
        season_key=args.season,
        dry_run=args.dry_run,
        confirm=not args.no_confirm
    )


if __name__ == "__main__":
    main()
