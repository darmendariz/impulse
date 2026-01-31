"""
Data Loader for EDA

Loads parsed replay data (Parquet files) with support for:
- Random sampling using the parsed_replays table in impulse.db
- Lazy loading (load replays on demand)
- Metadata access from both DB and JSON sidecar files
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

    Uses the parsed_replays table in impulse.db to efficiently access replay data.
    The table stores output_path directly, so no directory scanning is needed.

    Usage:
        # Using database (recommended)
        dataset = ReplayDataset(db_path='./impulse.db')

        # Or scan a directory (fallback if no DB)
        dataset = ReplayDataset(data_dir='./parsed_replays')

        # Load a random sample for EDA
        sample = dataset.load_sample(n=50)

        # Iterate over all replays
        for replay_id, df, metadata in dataset:
            process(df)
    """

    def __init__(self,
                 db_path: Optional[str] = "./impulse.db",
                 data_dir: Optional[str] = None):
        """
        Initialize the dataset.

        Args:
            db_path: Path to impulse.db containing parsed_replays table.
                     If provided and exists, uses DB for replay lookup.
            data_dir: Directory containing .parquet files.
                      Used as fallback if db_path not provided or doesn't exist.
        """
        self.db_path = Path(db_path) if db_path else None
        self.data_dir = Path(data_dir) if data_dir else None

        # Determine mode: DB or directory scan
        self.use_db = self.db_path is not None and self.db_path.exists()

        if not self.use_db and self.data_dir is None:
            raise ValueError("Must provide either db_path (existing) or data_dir")

        self._replay_info: Optional[Dict[str, Dict]] = None

    @property
    def replay_info(self) -> Dict[str, Dict]:
        """
        Get mapping of replay_id -> info dict.

        Info dict contains: output_path, frame_count, feature_count, fps, etc.
        """
        if self._replay_info is None:
            self._load_replay_info()
        return self._replay_info

    @property
    def replay_ids(self) -> List[str]:
        """Get list of available replay IDs."""
        return list(self.replay_info.keys())

    def _load_replay_info(self):
        """Load replay information from DB or directory scan."""
        self._replay_info = {}

        if self.use_db:
            self._load_from_db()
        else:
            self._scan_directory()

    def _load_from_db(self):
        """Load replay info from parsed_replays table."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT replay_id, output_path, fps, frame_count, feature_count,
                   file_size_bytes, parsed_at, metadata
            FROM parsed_replays
            WHERE parse_status = 'parsed'
        """)

        for row in cursor.fetchall():
            row_dict = dict(row)
            replay_id = row_dict['replay_id']
            output_path = row_dict.get('output_path')

            if not output_path:
                continue

            # Handle relative paths by checking data_dir
            path = Path(output_path)
            if not path.is_absolute() and self.data_dir:
                # DB stores relative paths like 'replays/parsed/XXX.parquet'
                # Try resolving using just the filename in data_dir
                resolved_path = self.data_dir / path.name
                if resolved_path.exists():
                    row_dict['output_path'] = str(resolved_path)
                    self._replay_info[replay_id] = row_dict
            elif path.exists():
                self._replay_info[replay_id] = row_dict

        conn.close()
        print(f"Found {len(self._replay_info)} parsed replays in database")

    def _scan_directory(self):
        """Fallback: scan directory for parquet files."""
        for pq_file in self.data_dir.rglob("*.parquet"):
            replay_id = pq_file.stem
            self._replay_info[replay_id] = {
                'replay_id': replay_id,
                'output_path': str(pq_file),
                'frame_count': None,
                'feature_count': None,
                'fps': None,
            }

        print(f"Found {len(self._replay_info)} parquet files in {self.data_dir}")

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
        if replay_id not in self.replay_info:
            print(f"Warning: Replay {replay_id} not found")
            return None, None

        info = self.replay_info[replay_id]
        parquet_path = Path(info['output_path'])

        if not parquet_path.exists():
            print(f"Warning: Parquet file not found: {parquet_path}")
            return None, None

        df = pd.read_parquet(parquet_path)

        # Build metadata from DB info + JSON sidecar if available
        metadata = {
            'replay_id': replay_id,
            'frame_count': info.get('frame_count'),
            'feature_count': info.get('feature_count'),
            'fps': info.get('fps'),
            'parsed_at': info.get('parsed_at'),
        }

        # Try to load JSON sidecar metadata
        metadata_path = parquet_path.with_suffix('.metadata.json')
        if metadata_path.exists():
            with open(metadata_path) as f:
                json_metadata = json.load(f)
                metadata.update(json_metadata)

        # Parse DB metadata JSON if present
        if info.get('metadata'):
            try:
                db_metadata = json.loads(info['metadata'])
                metadata.update(db_metadata)
            except (json.JSONDecodeError, TypeError):
                pass

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

    def get_replay_info(self, replay_id: str) -> Optional[Dict]:
        """
        Get replay info without loading the parquet file.

        Args:
            replay_id: Replay identifier

        Returns:
            Dict with replay info from DB, or None if not found
        """
        return self.replay_info.get(replay_id)

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

    def get_frame_count_summary(self) -> Dict[str, Any]:
        """
        Get summary of frame counts from DB without loading parquet files.

        Only works when using DB mode.

        Returns:
            Dict with frame count statistics
        """
        if not self.use_db:
            return {'error': 'Frame count summary requires database mode'}

        frame_counts = [
            info['frame_count']
            for info in self.replay_info.values()
            if info.get('frame_count') is not None
        ]

        if not frame_counts:
            return {'error': 'No frame count data available'}

        import numpy as np
        frame_counts = np.array(frame_counts)

        return {
            'count': len(frame_counts),
            'mean': float(frame_counts.mean()),
            'std': float(frame_counts.std()),
            'min': int(frame_counts.min()),
            'max': int(frame_counts.max()),
            'median': float(np.median(frame_counts)),
            'total_frames': int(frame_counts.sum()),
        }
