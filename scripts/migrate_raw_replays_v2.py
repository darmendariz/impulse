"""
One-off migration: add new metadata columns to raw_replays table.

Adds fields that were available from the Ballchasing API but not previously
stored: duration, overtime, map, match_type, team_size, season, goals,
rank divisions, and uploader info.

Run once on the EC2 instance database, then discard.

Usage:
    python scripts/migrate_raw_replays_v2.py
    python scripts/migrate_raw_replays_v2.py --db-path /path/to/impulse.db
"""

import sqlite3
import argparse
from pathlib import Path


NEW_COLUMNS = [
    ("duration", "INTEGER"),
    ("overtime", "BOOLEAN DEFAULT 0"),
    ("overtime_seconds", "INTEGER"),
    ("map_code", "TEXT"),
    ("map_name", "TEXT"),
    ("match_type", "TEXT"),
    ("team_size", "INTEGER"),
    ("season", "INTEGER"),
    ("season_type", "TEXT"),
    ("blue_goals", "INTEGER"),
    ("orange_goals", "INTEGER"),
    ("min_rank_division", "INTEGER"),
    ("max_rank_division", "INTEGER"),
    ("uploader_name", "TEXT"),
    ("uploader_steam_id", "TEXT"),
]


def migrate(db_path: str):
    path = Path(db_path)
    if not path.exists():
        print(f"✗ Database not found: {path}")
        return

    print(f"Migrating: {path}")
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    added = []
    skipped = []
    for col_name, col_type in NEW_COLUMNS:
        try:
            cursor.execute(f"ALTER TABLE raw_replays ADD COLUMN {col_name} {col_type}")
            added.append(col_name)
        except sqlite3.OperationalError:
            skipped.append(col_name)

    conn.commit()
    conn.close()

    if added:
        print(f"✓ Added columns: {', '.join(added)}")
    if skipped:
        print(f"  Already present: {', '.join(skipped)}")
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate raw_replays table to v2 schema.")
    parser.add_argument("--db-path", default="./impulse.db", help="Path to impulse.db")
    args = parser.parse_args()
    migrate(args.db_path)
