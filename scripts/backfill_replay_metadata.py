"""
One-off backfill: populate new metadata columns in raw_replays from Ballchasing API.

Fetches full replay metadata for all downloaded replays where the new columns
(duration, overtime, map, season, goals, uploader, etc.) are NULL, and updates
the database rows in place.

At 2 req/sec (donor tier) ~7k replays takes roughly 1 hour.
Re-running is safe — only rows still missing data are fetched.

Usage:
    python scripts/backfill_replay_metadata.py
    python scripts/backfill_replay_metadata.py --db-path /path/to/impulse.db
    python scripts/backfill_replay_metadata.py --dry-run     # print counts only
"""

import argparse
import sqlite3
import time
from pathlib import Path

from impulse.collection.ballchasing_client import BallchasingClient
from impulse.config.collection_config import CollectionConfig

RATE_LIMIT_PER_SECOND = 2
RATE_LIMIT_PER_HOUR = None  # No hourly cap (donor tier)


def get_unfilled_replay_ids(conn: sqlite3.Connection) -> list[str]:
    """Return IDs of downloaded replays that are missing the new metadata columns."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT replay_id FROM raw_replays
        WHERE is_downloaded = 1
          AND duration IS NULL
        ORDER BY downloaded_at
    """)
    return [row[0] for row in cursor.fetchall()]


def update_row(conn: sqlite3.Connection, replay_id: str, meta: dict):
    """Update a single raw_replays row with metadata fetched from the API."""
    blue = meta.get('blue') or {}
    orange = meta.get('orange') or {}
    min_rank = meta.get('min_rank') or {}
    max_rank = meta.get('max_rank') or {}
    uploader = meta.get('uploader') or {}

    conn.execute("""
        UPDATE raw_replays SET
            duration          = ?,
            overtime          = ?,
            overtime_seconds  = ?,
            map_code          = ?,
            map_name          = ?,
            match_type        = ?,
            team_size         = ?,
            season            = ?,
            season_type       = ?,
            blue_goals        = ?,
            orange_goals      = ?,
            min_rank_division = ?,
            max_rank_division = ?,
            uploader_name     = ?,
            uploader_steam_id = ?
        WHERE replay_id = ?
    """, (
        meta.get('duration'),
        int(bool(meta.get('overtime', False))),
        meta.get('overtime_seconds'),
        meta.get('map_code'),
        meta.get('map_name'),
        meta.get('match_type'),
        meta.get('team_size'),
        meta.get('season'),
        meta.get('season_type'),
        blue.get('goals'),
        orange.get('goals'),
        min_rank.get('division'),
        max_rank.get('division'),
        uploader.get('name'),
        uploader.get('steam_id'),
        replay_id,
    ))


def run(db_path: str, dry_run: bool):
    path = Path(db_path)
    if not path.exists():
        print(f"✗ Database not found: {path}")
        return

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    replay_ids = get_unfilled_replay_ids(conn)
    total = len(replay_ids)

    if total == 0:
        print("Nothing to backfill — all rows already have metadata.")
        conn.close()
        return

    print(f"Replays to backfill: {total}")

    if dry_run:
        print("Dry run — exiting without fetching.")
        conn.close()
        return

    config = CollectionConfig.from_env()
    config.rate_limit_per_second = RATE_LIMIT_PER_SECOND
    config.rate_limit_per_hour = RATE_LIMIT_PER_HOUR
    client = BallchasingClient(config)

    successful = 0
    failed = 0
    width = len(str(total))

    for i, replay_id in enumerate(replay_ids, 1):
        try:
            meta = client.get_replay_metadata(replay_id)
            update_row(conn, replay_id, meta)
            conn.commit()
            successful += 1

            if i % 100 == 0 or i == total:
                print(f"[{i:{width}}/{total}]  {successful} updated, {failed} failed")

        except Exception as e:
            failed += 1
            print(f"[{i:{width}}/{total}]  FAILED {replay_id}: {e}")

    conn.close()
    print(f"\nDone. Updated: {successful}  Failed: {failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill new metadata columns in raw_replays.")
    parser.add_argument("--db-path", default="./impulse.db", help="Path to impulse.db")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only, no API calls")
    args = parser.parse_args()
    run(args.db_path, args.dry_run)
