"""
Production script to download full RLCS season replay files to AWS S3 bucket

Usage:
    python download_rlcs_season.py --season 2024
    python download_rlcs_season.py --season 2024 --dry-run  # Preview only
"""

import argparse
from datetime import timezone, datetime
import json

# New simplified collection API
from impulse.collection import download_group
from impulse.collection.storage import S3Backend


# RLCS Season Ballchasing Group IDs
# TODO: Refine this value based on actual data.
avg_replay_size_mb = 1.8    # Average replay size in MB. Loose estimate updated 12-16-2025.
# TODO: Add previous seasons going back to RLCS S1. 
RLCS_SEASONS = {
    '21-22': {
        'group_id': 'rlcs-21-22-jl7xcwxrpc',
        'name': 'RLCS 2021-2022',
        'estimated_replay_count': 5915,
        'estimated_size_gb': 5915 * avg_replay_size_mb / 1000,
        'is_active': False,           # Whether the season is currently ongoing
        'last_updated': '2025-12-16'  # Date of last update to season info
    },
    '22-23': {
        'group_id': 'rlcs-22-23-jjc408bdu4',
        'name': 'RLCS 2022-2023',
        'estimated_replay_count': 15443,
        'estimated_size_gb': 15443 * avg_replay_size_mb / 1000,
        'is_active': False,
        'last_updated': '2025-12-16'
    },
    '2024': {
        'group_id': 'rlcs-2024-jsvrszynst',  
        'name': 'RLCS 2024',
        'estimated_replay_count': 7324,
        'estimated_size_gb': 7324 * avg_replay_size_mb / 1000,
        'is_active': False,
        'last_updated': '2025-12-16'
    },
    '2025': {
        'group_id': 'rlcs-2025-7ielfd7uhx',  
        'name': 'RLCS 2025',
        'estimated_replay_count': 7038,  
        'estimated_size_gb': 7038 * avg_replay_size_mb / 1000,
        'is_active': False,
        'last_updated': '2025-12-16'
    },
    '2026': {
        'group_id': 'rlcs-2026-d3chsz8nje',  
        'name': 'RLCS 2026',
        'estimated_replay_count': 834,  # TODO: Update when season finishes. Current to 12-16-2025.
        'estimated_size_gb': 834 * avg_replay_size_mb / 1024,
        'is_active': True,
        'last_updated': '2025-12-16'
    }
}

def print_season_info(season_key: str):
    """Print information about a season"""
    season = RLCS_SEASONS[season_key]
    
    print("="*60)
    print(f"RLCS {season_key} Season Download")
    print("="*60)
    print(f"Season Name: {season['name']}")
    print(f"Group ID: {season['group_id']}")
    print(f"Estimated Replays: {season['estimated_replay_count']:,}")
    print(f"Estimated Size: {season['estimated_size_gb']:.1f} GB")
    print(f"Active Season: {season['is_active']} (as of {season['last_updated']})")
    print()

def download_season(season_key: str, dry_run: bool = False):
    """
    Download a complete RLCS season to S3.
    
    Args:
        season_key: Season year (e.g., '21-22' or '2024')
        dry_run: If True, only show what would be downloaded
    """
    if season_key not in RLCS_SEASONS:
        print(f"✗ Season {season_key} not found")
        print(f"Available seasons: {', '.join(RLCS_SEASONS.keys())}")
        return
    
    season = RLCS_SEASONS[season_key]
    
    # Print info and costs
    print_season_info(season_key)
    
    if dry_run:
        print("DRY RUN MODE - No actual download")
        print("Run without --dry-run to start download")
        return
    
    # Confirm with user
    print(f"WARNING: This will download {season['estimated_replay_count']} replays ({season['estimated_size_gb']:.1f} GB)!")
    print("Make sure you're running on EC2 (not locally)")
    print()
    response = input("Continue? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("Download cancelled")
        return
    
    # Log start time
    start_time = datetime.now(timezone.utc)
    print(f"\nStarted: {start_time.isoformat()}")
    print()

    # Download with custom S3 prefix
    path_prefix = ['replays', 'rlcs', season_key]

    try:
        # Use new simplified API - automatically handles all initialization
        result = download_group(
            group_id=season['group_id'],
            storage_type='s3',
            path_prefix=path_prefix,
            use_database=True
        )
        
        # Log completion
        end_time = datetime.now(timezone.utc)
        duration = end_time - start_time

        print()
        print("="*60)
        print("DOWNLOAD COMPLETE")
        print("="*60)
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

        # Save completion log
        log_entry = {
            'season': season_key,
            'group_id': season['group_id'],
            'started': start_time.isoformat(),
            'finished': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
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

        # Upload log to S3
        s3_backend = S3Backend()
        s3_backend.s3_manager.upload_file(log_file, f"logs/{log_file}")
        print(f"Log backed up to S3")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Download interrupted by user")
        print("Run the same command again to resume (database tracks progress)")
    except Exception as e:
        print(f"\n\n✗ Download failed: {e}")
        import traceback
        traceback.print_exc()


def list_available_seasons():
    """List all available seasons"""
    print()
    print("="*60)
    print("Available RLCS Seasons:")
    for season_key, season_data in RLCS_SEASONS.items():
        print(f"\n  Season Key: {season_key}")
        print(f"  Season Name: {season_data['name']}")
        print(f"  Group ID: {season_data['group_id']}")
        print(f"  Estimated replay count: {season_data['estimated_replay_count']:,} replays") 
        print(f"  Estimated total download size: {season_data['estimated_size_gb']:.1f} GB")
        print(f"  Active Season: {season_data['is_active']} (as of {season_data['last_updated']})")
    print("\nUse --season <season_key> to download a specific season.")
    print()
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download complete RLCS seasons to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 rlcs_downloader.py --list
  python3 rlcs_downloader.py --season 2024 --dry-run
  python3 rlcs_downloader.py --season 2024
  python3 rlcs_downloader.py --season 2024 --estimate-only
        """
    )
    
    parser.add_argument('--season', type=str, help='Season year to download (e.g., 2024)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be downloaded without downloading')
    parser.add_argument('--estimate-only', action='store_true', help='Only show cost estimates')
    parser.add_argument('--list', action='store_true', help='List available seasons')
    
    args = parser.parse_args()
    
    if args.list:
        list_available_seasons()
    elif args.season:
        if args.estimate_only:
            print_season_info(args.season)
        else:
            download_season(args.season, dry_run=args.dry_run)
    else:
        parser.print_help()
