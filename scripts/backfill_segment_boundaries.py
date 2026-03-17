"""
Backfill segment boundaries for existing parsed replays.

Iterates over all parsed replays in the database that don't yet have
segment boundaries computed, loads each Parquet file, computes boundaries,
and updates the database.

Usage:
    # Local files
    python scripts/backfill_segment_boundaries.py --db-path ./impulse.db --data-dir ./replays/parsed

    # S3 files (downloads to temp, computes, deletes)
    python scripts/backfill_segment_boundaries.py --db-path ./impulse.db --s3

    # Limit to N replays (for testing)
    python scripts/backfill_segment_boundaries.py --db-path ./impulse.db --data-dir ./replays/parsed --limit 100
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from impulse.collection.database import ImpulseDB
from impulse.preprocessing.segmentation import find_segment_boundaries, serialize_boundaries


def backfill(db: ImpulseDB, data_dir: str = None, s3_manager=None, limit: int = None):
    """
    Compute and store segment boundaries for replays missing them.

    Args:
        db: ImpulseDB instance.
        data_dir: Local directory containing parsed Parquet files.
        s3_manager: Optional S3Manager for downloading from S3.
        limit: Max replays to process.
    """
    replays = db.get_replays_without_boundaries(limit=limit)

    if not replays:
        print("All parsed replays already have segment boundaries.")
        return

    total = len(replays)
    successful = 0
    failed = 0
    width = len(str(total))
    start_time = time.time()

    print(f"Backfilling segment boundaries for {total} replays...")

    for i, replay_info in enumerate(replays, 1):
        replay_id = replay_info['replay_id']
        output_path = replay_info['output_path']

        try:
            parquet_path = _resolve_path(output_path, data_dir, s3_manager, replay_id)
            if parquet_path is None:
                print(f"[{i:{width}}/{total}] {replay_id}  SKIPPED: file not found")
                failed += 1
                continue

            df = pd.read_parquet(parquet_path)
            boundaries = find_segment_boundaries(df)
            db.update_segment_boundaries(replay_id, serialize_boundaries(boundaries))

            successful += 1
            if i % 50 == 0 or i == total:
                elapsed = time.time() - start_time
                rate = i / elapsed
                print(f"[{i:{width}}/{total}] {successful} done, {failed} failed ({rate:.1f} replays/sec)")

        except Exception as e:
            failed += 1
            print(f"[{i:{width}}/{total}] {replay_id}  FAILED: {e}")

    elapsed = time.time() - start_time
    print(f"\nDone. {successful} updated, {failed} failed in {elapsed:.1f}s")


def _resolve_path(output_path: str, data_dir: str, s3_manager, replay_id: str):
    """Resolve a Parquet file path from output_path, data_dir, or S3."""
    path = Path(output_path)

    # Try absolute path
    if path.is_absolute() and path.exists():
        return str(path)

    # Try relative to data_dir
    if data_dir:
        for candidate in (Path(data_dir) / path, Path(data_dir) / path.name,
                          Path(data_dir) / f"{replay_id}.parquet"):
            if candidate.exists():
                return str(candidate)

    # Try S3
    if s3_manager:
        return f"s3://{s3_manager.s3_bucket_name}/{output_path}"

    return None


def main():
    parser = argparse.ArgumentParser(description="Backfill segment boundaries for parsed replays")
    parser.add_argument('--db-path', default='./impulse.db', help='Path to impulse.db')
    parser.add_argument('--data-dir', help='Local directory with parsed Parquet files')
    parser.add_argument('--s3', action='store_true', help='Load Parquet files from S3')
    parser.add_argument('--limit', type=int, help='Max replays to process')
    args = parser.parse_args()

    db = ImpulseDB(db_path=args.db_path)

    s3_manager = None
    if args.s3:
        from impulse.collection.s3_manager import S3Manager
        s3_manager = S3Manager()

    if not args.data_dir and not args.s3:
        print("Error: must provide --data-dir or --s3")
        sys.exit(1)

    backfill(db, data_dir=args.data_dir, s3_manager=s3_manager, limit=args.limit)


if __name__ == '__main__':
    main()
