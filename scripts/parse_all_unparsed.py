"""
Parse all downloaded-but-unparsed replay files from S3.

Pulls the latest database from S3, parses all unparsed replays (downloading
each raw .replay from S3, parsing locally, uploading the parquet + metadata
back to S3), and pushes the updated database to S3 when done.

Edit the configuration block below, then run:
    python scripts/parse_all_unparsed.py

Re-running is safe — already-parsed replays are skipped automatically.
"""

from impulse.parsing import ParsingPipeline, ReplayParser
from impulse.collection.database import ImpulseDB
from impulse.collection.s3_manager import S3Manager

# =============================================================================
# Configuration 
# =============================================================================

# Path to local SQLite database (will be pulled from S3 on startup)
DATABASE_PATH = './impulse.db'

# Local directory for parsed output files (parquet + metadata JSON).
# Files are uploaded to S3 after each replay; this is just working storage.
OUTPUT_DIR = './replays/parsed'

# Parsing frame rate
FPS = 30.0

# Feature preset: 'standard', 'minimal', or 'all'
PRESET = 'standard'

# Retry failed parses after the main run
AUTO_RETRY = True
MAX_RETRIES = 3

# Parse at most this many replays (None = all). Useful for testing.
LIMIT = None

# =============================================================================

s3_manager = S3Manager()
db = ImpulseDB(DATABASE_PATH, s3_manager=s3_manager)

print("Pulling latest database version from S3...")
db.pull()

parser = ReplayParser.from_preset(PRESET, fps=FPS)
pipeline = ParsingPipeline(parser, db, s3_manager=s3_manager)

result = pipeline.parse_unparsed(OUTPUT_DIR, limit=LIMIT)

if AUTO_RETRY and result.failed > 0:
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\nRetry {attempt}/{MAX_RETRIES}: {result.failed} failed replay(s)...")
        result = pipeline.retry_failed_parses(OUTPUT_DIR)
        if result.failed == 0:
            print("All replays recovered.")
            break
    else:
        print(f"Max retries ({MAX_RETRIES}) reached. {result.failed} replay(s) could not be recovered.")
