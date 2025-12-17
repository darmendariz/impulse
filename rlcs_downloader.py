"""
Production script to download full RLCS season replay files to AWS S3 bucket

Usage:
    python download_rlcs_season.py --season 2024
    python download_rlcs_season.py --season 2024 --dry-run  # Preview only
"""

import argparse
from datetime import timezone
from ballchasing import Ballchasing
from impulse_db import ImpulseDB
from s3_manager import S3Manager
from datetime import datetime


# RLCS Season Ballchasing Group IDs
avg_replay_size_mb = 1.8    # Average replay size in MB. Loose estimate updated 12-16-2025.
                            # TODO: Refine this value based on actual data.
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


def estimate_costs(num_replays: int, size_gb: float) -> dict:
    """
    Estimate AWS costs for downloading and storing replays.
    
    Args:
        num_replays: Number of replays to download
        size_gb: Total size in GB
    
    Returns:
        Dict with cost breakdown
    """
    # TODO: Refine estimates based on actual usage patterns. Numbers supplied by Claude Sonnet 4.5 on 12-16-2025.
    # EC2 costs (t3.micro)
    hours_needed = num_replays / 3600  # ~1 second per replay
    ec2_hourly_rate = 0.0104  # us-east-1 on-demand
    ec2_spot_rate = 0.0031    # ~70% discount with spot
    
    ec2_cost_ondemand = hours_needed * ec2_hourly_rate
    ec2_cost_spot = hours_needed * ec2_spot_rate
    
    # S3 storage costs (Standard)
    s3_storage_monthly = size_gb * 0.023  # $0.023/GB/month
    s3_storage_yearly = s3_storage_monthly * 12
    
    # Data transfer (Ballchasing -> EC2 -> S3)
    # Ingress to EC2: FREE
    # EC2 -> S3 (same region): FREE
    data_transfer_cost = 0.0
    
    # Total first month
    total_first_month = ec2_cost_ondemand + s3_storage_monthly
    total_first_month_spot = ec2_cost_spot + s3_storage_monthly
    
    return {
        'ec2_hours': round(hours_needed, 2),
        'ec2_cost_ondemand': round(ec2_cost_ondemand, 2),
        'ec2_cost_spot': round(ec2_cost_spot, 2),
        's3_storage_monthly': round(s3_storage_monthly, 2),
        's3_storage_yearly': round(s3_storage_yearly, 2),
        'data_transfer': data_transfer_cost,
        'total_first_month_ondemand': round(total_first_month, 2),
        'total_first_month_spot': round(total_first_month_spot, 2),
        'ongoing_monthly': round(s3_storage_monthly, 2)
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
    
def print_cost_estimates(season_key: str):
    """Print cost estimates for downloading and storing a season"""
    season = RLCS_SEASONS[season_key]
    # Calculate costs
    costs = estimate_costs(season['estimated_replay_count'], season['estimated_size_gb'])
    
    print("COST ESTIMATES")
    print("-"*60)
    print(f"EC2 Runtime: {costs['ec2_hours']} hours")
    print(f"  On-Demand: ${costs['ec2_cost_ondemand']}")
    print(f"  Spot Instance: ${costs['ec2_cost_spot']} (recommended)")
    print()
    print(f"S3 Storage: {season['estimated_size_gb']:.1f} GB")
    print(f"  Monthly: ${costs['s3_storage_monthly']}")
    print(f"  Yearly: ${costs['s3_storage_yearly']}")
    print()
    print(f"Data Transfer: ${costs['data_transfer']} (FREE!)")
    print()
    print("TOTAL COSTS:")
    print(f"  First month (on-demand): ${costs['total_first_month_ondemand']}")
    print(f"  First month (spot): ${costs['total_first_month_spot']}")
    print(f"  Ongoing (monthly): ${costs['ongoing_monthly']}")
    print("="*60)
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
    
    # Initialize services
    print("\nInitializing services...")
    bc = Ballchasing()
    db = ImpulseDB()
    s3 = S3Manager()
    
    # Check S3 bucket exists
    s3.create_bucket_if_needed()
    
    # Log start time
    start_time = datetime.now(timezone.utc)
    print(f"\nStarted: {start_time.isoformat()}")
    print()
    
    # Download with custom S3 prefix
    s3_prefix = f"replays/rlcs/{season_key}"
    
    try:
        results = bc.download_group_to_s3(
            group_id=season['group_id'],
            s3_prefix=s3_prefix
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
        print(f"Total replays: {results['total']}")
        print(f"Successfully uploaded: {results['successful']}")
        print(f"Skipped: {results['skipped']}")
        print(f"Failed: {results['failed']}")
        print(f"Total size: {results['total_bytes'] / (1024**3):.2f} GB")
        print()
        print(f"S3 Location: s3://{results['s3_bucket']}/{s3_prefix}/")
        
        # Save completion log
        log_entry = {
            'season': season_key,
            'group_id': season['group_id'],
            'started': start_time.isoformat(),
            'finished': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
            'results': results
        }
        
        import json
        log_file = f"download_log_{season_key}_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, 'w') as f:
            json.dump(log_entry, f, indent=2)
        
        print(f"\nLog saved: {log_file}")
        
        # Upload log to S3
        s3.upload_file(log_file, f"logs/{log_file}")
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
