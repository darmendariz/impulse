"""
Replay Dataset - Data Access Layer

Provides a clean interface for loading and working with parsed replay data.
This module bridges the parsing output (Parquet files) and downstream usage
(analysis, training, inference).

Key classes:
    ReplayData: Container for a single replay's frame data and metadata.
    ReplayDataset: Interface for accessing collections of parsed replays.

The dataset supports both small collections (load all into memory) and large
collections (lazy iteration, batched loading) for memory efficiency.
"""

import sqlite3
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator
import pandas as pd
import json


@dataclass
class ReplayData:
    """
    A loaded replay with its frame data and metadata.

    Attributes:
        replay_id: Unique identifier for this replay
        frames: DataFrame containing the parsed frame data
        metadata: Dictionary of metadata (team info, match details, etc.)

    Metadata fields can be accessed directly as attributes:
        replay.team_size  # equivalent to replay.metadata.get('team_size')
    """
    replay_id: str
    frames: pd.DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        """Allow accessing metadata fields as attributes."""
        metadata = self.__dict__.get('metadata', {})
        if metadata and name in metadata:
            return metadata[name]
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")


class ReplayDataset:
    """
    Dataset interface for accessing parsed replays.

    Acts as a data access layer between parsed Parquet files and downstream
    consumers (analysis, training, etc.). Supports efficient access patterns
    for both small and large collections.

    Data sources:
        - Database mode (recommended): Uses parsed_replays table in impulse.db
          for fast lookup without directory scanning.
        - Directory mode (fallback): Scans a directory for .parquet files.

    Loading strategies:
        - load_all(): Load everything into memory (small datasets)
        - load_sample(n): Load a random subset (EDA, prototyping)
        - __iter__(): Lazy iteration, one replay at a time (memory-efficient)
        - iter_batches(n): Batched iteration (large-scale processing)

    Usage:
        # Initialize with database (recommended)
        dataset = ReplayDataset(db_path='./impulse.db', data_dir='./parsed')

        # Or scan a directory directly
        dataset = ReplayDataset(data_dir='./parsed_replays')

        # Small dataset: load everything
        replays = dataset.load_all()
        for replay in replays:
            print(replay.team_size)
            process(replay.frames)

        # Large dataset: iterate one at a time
        for replay in dataset:
            process(replay.frames)

        # Large dataset: process in batches
        for batch in dataset.iter_batches(batch_size=100):
            process_batch(batch)

        # Random sample for EDA
        sample = dataset.load_sample(n=50, seed=42)
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

    def __iter__(self) -> Iterator[ReplayData]:
        """Iterate over all replays, yielding ReplayData objects one at a time."""
        for replay_id in self.replay_ids:
            replay = self.load_replay(replay_id)
            if replay is not None:
                yield replay

    def load_replay(self, replay_id: str) -> Optional[ReplayData]:
        """
        Load a single replay by ID.

        Args:
            replay_id: Replay identifier

        Returns:
            ReplayData object, or None if not found
        """
        if replay_id not in self.replay_info:
            print(f"Warning: Replay {replay_id} not found")
            return None

        info = self.replay_info[replay_id]
        parquet_path = Path(info['output_path'])

        if not parquet_path.exists():
            print(f"Warning: Parquet file not found: {parquet_path}")
            return None

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

        return ReplayData(replay_id=replay_id, frames=df, metadata=metadata)

    def load_sample(self, n: int = 50, seed: Optional[int] = None) -> List[ReplayData]:
        """
        Load a random sample of replays.

        Args:
            n: Number of replays to sample
            seed: Random seed for reproducibility

        Returns:
            List of ReplayData objects
        """
        if seed is not None:
            random.seed(seed)

        n = min(n, len(self.replay_ids))
        sampled_ids = random.sample(self.replay_ids, n)

        results = []
        for replay_id in sampled_ids:
            replay = self.load_replay(replay_id)
            if replay is not None:
                results.append(replay)

        print(f"Loaded {len(results)} replays")
        return results

    def load_all(self) -> List[ReplayData]:
        """
        Load all replays in the dataset.

        Returns:
            List of ReplayData objects
        """
        results = []
        for replay_id in self.replay_ids:
            replay = self.load_replay(replay_id)
            if replay is not None:
                results.append(replay)

        print(f"Loaded {len(results)} replays")
        return results

    def iter_batches(self, batch_size: int = 100) -> Iterator[List[ReplayData]]:
        """
        Iterate over replays in batches.

        Memory-efficient for large datasets: only one batch is in memory at a time.

        Args:
            batch_size: Number of replays per batch

        Yields:
            Lists of ReplayData objects, each list up to batch_size
        """
        batch = []
        for replay_id in self.replay_ids:
            replay = self.load_replay(replay_id)
            if replay is not None:
                batch.append(replay)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

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
