from impulse.collection import BallchasingClient, ReplayDownloader
from impulse.collection.storage import LocalBackend
from impulse.collection.database import ImpulseDB
from impulse.config.collection_config import CollectionConfig

PATH_PREFIX = ['replays', 'raw', 'rlcs', '2024', 'world-championship']
LOCAL_BASE_DIR = '.'
DATABASE_PATH = './impulse.db'
RATE_LIMIT_PER_SECOND = 2
RATE_LIMIT_PER_HOUR = None
AUTO_RETRY = True
MAX_RETRIES = 3

config = CollectionConfig.from_env()
config.rate_limit_per_second = RATE_LIMIT_PER_SECOND
config.rate_limit_per_hour = RATE_LIMIT_PER_HOUR

client = BallchasingClient(config)
db = ImpulseDB(DATABASE_PATH)
storage = LocalBackend(base_dir=LOCAL_BASE_DIR)

downloader = ReplayDownloader(client, storage, db)

group_id = 'world-championship-md058mxx2x'
result = downloader.download_group(
    group_id=group_id, 
    path_prefix=PATH_PREFIX, 
    is_rlcs=True
    )

if AUTO_RETRY and result.failed > 0:
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\nRetry {attempt}/{MAX_RETRIES}: {result.failed} failed replay(s)...")
        result = downloader.retry_failed_downloads(group_id)
        if result.failed == 0:
            print("All replays recovered.")
            break
    else:
        print(f"Max retries ({MAX_RETRIES}) reached. {result.failed} replay(s) could not be recovered.")
