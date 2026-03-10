"""
Download an RLCS season from Ballchasing to S3 (or local storage).

Edit the configuration block below, then run:
    python scripts/download_season.py

Re-running this script is safe — already-downloaded replays are skipped and
any previously failed replays will be retried automatically.
"""

from impulse.collection import BallchasingClient, ReplayDownloader
from impulse.collection.storage import S3Backend, LocalBackend
from impulse.collection.database import ImpulseDB
from impulse.collection.s3_manager import S3Manager
from impulse.collection.rlcs_manager import RLCSManager
from impulse.config.collection_config import CollectionConfig

# =============================================================================
# Configuration — edit before each run
# =============================================================================

# Season to download. Available keys: '21-22', '22-23', '2024', '2025', '2026'
SEASON = '2024'

# Storage backend: 's3' or 'local'
STORAGE_TYPE = 's3'

# Path prefix applied under the storage root.
# S3:    files stored at         {PATH_PREFIX}/{SEASON}/{group_hierarchy}/{id}.replay
# Local: files stored at {LOCAL_BASE_DIR}/{PATH_PREFIX}/{SEASON}/{group_hierarchy}/{id}.replay
PATH_PREFIX = ['replays', 'raw', 'rlcs']

# Base directory for local storage (ignored when STORAGE_TYPE = 's3')
LOCAL_BASE_DIR = '.'

# Path to SQLite database for download tracking and resume
DATABASE_PATH = './impulse.db'

# Ballchasing API rate limits
# Free tier:   1 req/sec, 200 req/hour
# Donor tier:  up to 200 req/sec, no hourly cap (set RATE_LIMIT_PER_HOUR to None)
RATE_LIMIT_PER_SECOND = 2
RATE_LIMIT_PER_HOUR = None       # Set to None to disable hourly cap (donor tier)

# Retry failed downloads after the main run.
# MAX_RETRIES attempts are made; stops early if all failures are recovered.
AUTO_RETRY = True
MAX_RETRIES = 3

# =============================================================================

season_info = RLCSManager.get_season_info(SEASON)
print(f"Season:            {season_info['name']}")
print(f"Group ID:          {season_info['group_id']}")
print(f"Estimated replays: {season_info['estimated_replay_count']:,}")
print(f"Estimated size:    {season_info['estimated_size_gb']:.1f} GB")
print()

config = CollectionConfig.from_env()
config.rate_limit_per_second = RATE_LIMIT_PER_SECOND
config.rate_limit_per_hour = RATE_LIMIT_PER_HOUR

client = BallchasingClient(config)

if STORAGE_TYPE == 's3':
    storage = S3Backend()
    s3_manager = storage.s3_manager
elif STORAGE_TYPE == 'local':
    storage = LocalBackend(base_dir=LOCAL_BASE_DIR)
    s3_manager = None
else:
    raise ValueError(f"Unknown STORAGE_TYPE: '{STORAGE_TYPE}'. Must be 's3' or 'local'.")

db = ImpulseDB(DATABASE_PATH, s3_manager=s3_manager)

downloader = ReplayDownloader(client, storage, db)

result = downloader.download_group(
    group_id=season_info['group_id'],
    path_prefix=PATH_PREFIX + [SEASON],
    is_rlcs=True
)

if AUTO_RETRY and result.failed > 0:
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\nRetry {attempt}/{MAX_RETRIES}: {result.failed} failed replay(s)...")
        result = downloader.retry_failed_downloads(season_info['group_id'])
        if result.failed == 0:
            print("All replays recovered.")
            break
    else:
        print(f"Max retries ({MAX_RETRIES}) reached. {result.failed} replay(s) could not be recovered.")
