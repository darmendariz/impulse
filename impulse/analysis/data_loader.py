"""
Data Loader for EDA

Loads parsed replay data (Parquet files) with support for:
- Random sampling using the impulse.db for efficiency
- Lazy loading (load replays on demand)
- Metadata access
"""

import sqlite3
import random
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator, Tuple
import pandas as pd
import json


class ReplayDataset:
    """
    Dataset interface for parsed replays.

    Uses the impulse.db to efficiently sample replays without scanning directories.
    Loads Parquet files on demand.

    Usage:
        dataset = ReplayDataset('./replays/parsed', './replays/parsed/parsed_replays.db')

        # Load a random sample for EDA
        sample = dataset.load_sample(n=50)

        # Iterate over all replays
        for replay_id, df, metadata in dataset:
            process(df)
    """

    def __init__(self,
                 data_dir: str,
                 db_path: str = "./impulse.db",
                 use_db: bool = True):
        """
        Initialize the dataset.

        Args:
            data_dir: Directory containing .parquet files
            db_path: Path to impulse.db for replay metadata
            use_db: If True, use DB for replay list; if False, scan directory
        """
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)
        self.use_db = use_db and self.db_path.exists()

        self._replay_ids: Optional[List[str]] = None
        self._parquet_files: Optional[Dict[str, Path]] = None

    @property
    def replay_ids(self) -> List[str]:
        """Get list of available replay IDs."""
        if self._replay_ids is None:
            self._scan_replays()
        return self._replay_ids

    @property
    def parquet_files(self) -> Dict[str, Path]:
        """Get mapping of replay_id -> parquet file path."""
        if self._parquet_files is None:
            self._scan_replays()
        return self._parquet_files

    def _scan_replays(self):
        """Scan for available replays."""
        self._parquet_files = {}

        # Scan directory for parquet files
        for pq_file in self.data_dir.rglob("*.parquet"):
            replay_id = pq_file.stem
            self._parquet_files[replay_id] = pq_file

        # If using DB, filter to only downloaded replays
        if self.use_db:
            db_replay_ids = self._get_replay_ids_from_db()
            # Keep only replays that exist both in DB and as files
            self._replay_ids = [
                rid for rid in db_replay_ids
                if rid in self._parquet_files
            ]
        else:
            self._replay_ids = list(self._parquet_files.keys())

        print(f"Found {len(self._replay_ids)} replays in {self.data_dir}")

    def _get_replay_ids_from_db(self) -> List[str]:
        """Get downloaded replay IDs from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT replay_id FROM raw_replays
            WHERE download_status = 'downloaded'
        """)
        ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return ids

    def __len__(self) -> int:
        """Number of available replays."""
        return len(self.replay_ids)

    def __iter__(self) -> Iterator[Tuple[str, pd.DataFrame, Optional[Dict]]]:
        """Iterate over all replays, yielding (replay_id, dataframe, metadata)."""
        for replay_id in self.replay_ids:
            df, metadata = self.load_replay(replay_id)
            if df is not None:
                yield replay_id, df, metadata

    def load_replay(self, replay_id: str) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
        """
        Load a single replay by ID.

        Args:
            replay_id: Replay identifier

        Returns:
            Tuple of (DataFrame, metadata_dict) or (None, None) if not found
        """
        if replay_id not in self.parquet_files:
            print(f"Warning: Replay {replay_id} not found")
            return None, None

        parquet_path = self.parquet_files[replay_id]
        df = pd.read_parquet(parquet_path)

        # Try to load metadata
        metadata_path = parquet_path.with_suffix('.metadata.json')
        metadata = None
        if metadata_path.exists():
            with open(metadata_path) as f:
                metadata = json.load(f)

        return df, metadata

    def load_sample(self,
                    n: int = 50,
                    seed: Optional[int] = None) -> List[Tuple[str, pd.DataFrame, Optional[Dict]]]:
        """
        Load a random sample of replays.

        Args:
            n: Number of replays to sample
            seed: Random seed for reproducibility

        Returns:
            List of (replay_id, DataFrame, metadata) tuples
        """
        if seed is not None:
            random.seed(seed)

        n = min(n, len(self.replay_ids))
        sampled_ids = random.sample(self.replay_ids, n)

        results = []
        for replay_id in sampled_ids:
            df, metadata = self.load_replay(replay_id)
            if df is not None:
                results.append((replay_id, df, metadata))

        print(f"Loaded {len(results)} replays")
        return results

    def get_replay_info_from_db(self, replay_id: str) -> Optional[Dict]:
        """
        Get replay info from database (without loading parquet).

        Args:
            replay_id: Replay identifier

        Returns:
            Dict with replay metadata from DB, or None
        """
        if not self.use_db:
            return None

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM raw_replays WHERE replay_id = ?
        """, (replay_id,))
        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def sample_replay_ids(self, n: int, seed: Optional[int] = None) -> List[str]:
        """
        Get random sample of replay IDs without loading data.

        Useful for planning batch operations.

        Args:
            n: Number of IDs to sample
            seed: Random seed

        Returns:
            List of replay IDs
        """
        if seed is not None:
            random.seed(seed)

        n = min(n, len(self.replay_ids))
        return random.sample(self.replay_ids, n)
