"""
Replay Dataset - Data Access Layer

Provides a clean interface for loading and working with parsed replay data.
This module bridges the parsing output (Parquet files) and downstream usage
(analysis, training, inference).

Key classes:
    ReplayData: Container for a single replay's frame data and metadata.
    ReplayDataset: Interface for accessing collections of parsed replays.
    WindowedReplayDataset: Fixed-length sliding window dataset for model training.
"""

import json
import random
import sqlite3
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from impulse.collection.s3_manager import S3Manager


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
        - load_all(): Load everything into memory (small datasets only)
        - load_sample(n): Load a random subset (EDA, prototyping)
        - __iter__(): Lazy iteration, one replay at a time (memory-efficient)
        - iter_batches(n): Batched iteration (large-scale processing)

    S3 support:
        When parsed files live on S3, provide an s3_manager. If cache_dir is
        also set, files are downloaded once and reused on subsequent accesses.
        Without cache_dir, each load streams directly from S3 via s3:// URI
        (requires the s3fs package).

    Usage:
        # Local files
        dataset = ReplayDataset(data_dir='./parsed_replays')

        # S3 files, stream each load directly (good for EDA)
        dataset = ReplayDataset(db_path='./impulse.db', s3_manager=s3)

        # S3 files, cache locally for training (download once, read many)
        dataset = ReplayDataset(db_path='./impulse.db', s3_manager=s3,
                                cache_dir='/data/parsed')
    """

    def __init__(
        self,
        db_path: Optional[str] = "./impulse.db",
        data_dir: Optional[str] = None,
        s3_manager: Optional["S3Manager"] = None,
        cache_dir: Optional[str] = None,
    ):
        """
        Args:
            db_path: Path to impulse.db. Used for fast replay lookup when it exists.
            data_dir: Directory containing .parquet files. Used as fallback when
                      db_path is not provided or does not exist.
            s3_manager: S3Manager instance. Required when parsed files live on S3.
            cache_dir: Local directory for caching S3 downloads. When set, files
                       downloaded from S3 are saved here and reused on subsequent
                       loads, avoiding redundant S3 requests.
        """
        self.db_path = Path(db_path) if db_path else None
        self.data_dir = Path(data_dir) if data_dir else None
        self.s3_manager = s3_manager
        self.cache_dir = Path(cache_dir) if cache_dir else None

        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.use_db = self.db_path is not None and self.db_path.exists()

        if not self.use_db and self.data_dir is None:
            raise ValueError("Must provide either db_path (existing) or data_dir")

        self._replay_info: Optional[Dict[str, Dict]] = None

    # -------------------------------------------------------------------------
    # Replay info loading
    # -------------------------------------------------------------------------

    @property
    def replay_info(self) -> Dict[str, Dict]:
        """Mapping of replay_id -> info dict (output_path, frame_count, fps, etc.)."""
        if self._replay_info is None:
            self._load_replay_info()
        return self._replay_info

    @property
    def replay_ids(self) -> List[str]:
        """List of available replay IDs."""
        return list(self.replay_info.keys())

    def _load_replay_info(self):
        self._replay_info = {}
        if self.use_db:
            self._load_from_db()
        else:
            self._scan_directory()

    def _load_from_db(self):
        """Load replay info from parsed_replays table."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT replay_id, output_path, fps, frame_count, feature_count,
                       file_size_bytes, parsed_at, metadata
                FROM parsed_replays
                WHERE parse_status = 'parsed'
            """)
            for row in cursor.fetchall():
                row_dict = dict(row)
                if row_dict.get('output_path'):
                    self._replay_info[row_dict['replay_id']] = row_dict
        finally:
            conn.close()
        print(f"Found {len(self._replay_info)} parsed replays in database")

    def _scan_directory(self):
        """Fallback: scan a directory for parquet files."""
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

    # -------------------------------------------------------------------------
    # Path resolution
    # -------------------------------------------------------------------------

    def _resolve_parquet_path(self, replay_id: str, output_path: str) -> Optional[str]:
        """
        Determine where to load a parquet file from.

        Resolution order:
          1. Local cache (cache_dir/{replay_id}.parquet), if cache_dir is set
          2. Absolute local path, if it exists on disk
          3. Relative path under data_dir, if data_dir is set
          4. S3: download to cache_dir (if set), or return s3:// URI for direct
             streaming (requires s3fs)

        Returns a file path or s3:// URI to pass directly to pd.read_parquet,
        or None if the file cannot be located.
        """
        # 1. Local cache
        if self.cache_dir:
            cached = self.cache_dir / f"{replay_id}.parquet"
            if cached.exists():
                return str(cached)

        path = Path(output_path)

        # 2. Absolute local path
        if path.is_absolute():
            if path.exists():
                return str(path)
            # Absolute path recorded but file missing — fall through to S3
        else:
            # 3. Relative path under data_dir — try full relative path first,
            #    then just the filename (handles both nested and flat layouts)
            if self.data_dir:
                for candidate in (self.data_dir / path, self.data_dir / path.name):
                    if candidate.exists():
                        return str(candidate)

        # 4. S3 (output_path is stored as a bare S3 key in the DB)
        if self.s3_manager:
            s3_key = output_path
            if self.cache_dir:
                cached = self.cache_dir / f"{replay_id}.parquet"
                if self.s3_manager.download_file(s3_key, str(cached)):
                    return str(cached)
                print(f"Warning: S3 download failed for {replay_id} ({s3_key})")
                return None
            else:
                # Stream directly — requires s3fs package
                return f"s3://{self.s3_manager.s3_bucket_name}/{s3_key}"

        print(f"Warning: Cannot locate parquet file for {replay_id}: {output_path}")
        return None

    # -------------------------------------------------------------------------
    # Loading
    # -------------------------------------------------------------------------

    def load_replay(self, replay_id: str) -> Optional[ReplayData]:
        """
        Load a single replay by ID.

        Returns:
            ReplayData, or None if the replay cannot be found or loaded.
        """
        if replay_id not in self.replay_info:
            print(f"Warning: Replay {replay_id} not found in dataset")
            return None

        info = self.replay_info[replay_id]
        parquet_path = self._resolve_parquet_path(replay_id, info['output_path'])
        if parquet_path is None:
            return None

        try:
            df = pd.read_parquet(parquet_path)
        except Exception as e:
            print(f"Warning: Failed to load {replay_id}: {e}")
            return None

        metadata = {
            'replay_id': replay_id,
            'frame_count': info.get('frame_count'),
            'feature_count': info.get('feature_count'),
            'fps': info.get('fps'),
            'parsed_at': info.get('parsed_at'),
        }

        # Merge JSON metadata stored in DB
        if info.get('metadata'):
            try:
                metadata.update(json.loads(info['metadata']))
            except (json.JSONDecodeError, TypeError):
                pass

        # Merge JSON sidecar file if present (local paths only)
        if not parquet_path.startswith('s3://'):
            sidecar = Path(parquet_path).with_suffix('.metadata.json')
            if sidecar.exists():
                with open(sidecar) as f:
                    metadata.update(json.load(f))

        return ReplayData(replay_id=replay_id, frames=df, metadata=metadata)

    def load_sample(self, n: int = 50, seed: Optional[int] = None) -> List[ReplayData]:
        """
        Load a random sample of replays.

        Args:
            n: Number of replays to sample.
            seed: Random seed for reproducibility. Does not affect global random state.

        Returns:
            List of ReplayData objects.
        """
        rng = random.Random(seed)
        n = min(n, len(self.replay_ids))
        sampled_ids = rng.sample(self.replay_ids, n)

        results = [r for rid in sampled_ids if (r := self.load_replay(rid)) is not None]
        print(f"Loaded {len(results)}/{n} replays")
        return results

    def load_all(self) -> List[ReplayData]:
        """
        Load all replays into memory.

        Only suitable for small datasets. For large collections use __iter__(),
        iter_batches(), or WindowedReplayDataset instead.
        """
        n = len(self)
        if n > 500:
            print(f"Warning: loading {n} replays into memory. "
                  "For large datasets, prefer iter_batches() or WindowedReplayDataset.")

        results = [r for rid in self.replay_ids if (r := self.load_replay(rid)) is not None]
        print(f"Loaded {len(results)}/{n} replays")
        return results

    def __len__(self) -> int:
        return len(self.replay_ids)

    def __iter__(self) -> Iterator[ReplayData]:
        """Iterate over all replays lazily, one at a time."""
        for replay_id in self.replay_ids:
            replay = self.load_replay(replay_id)
            if replay is not None:
                yield replay

    def iter_batches(self, batch_size: int = 100) -> Iterator[List[ReplayData]]:
        """
        Iterate over replays in batches. Only one batch is held in memory at a time.

        Args:
            batch_size: Number of replays per batch.

        Yields:
            Lists of ReplayData objects.
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

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def get_replay_info(self, replay_id: str) -> Optional[Dict]:
        """Get replay metadata from DB without loading the parquet file."""
        return self.replay_info.get(replay_id)

    def sample_replay_ids(self, n: int, seed: Optional[int] = None) -> List[str]:
        """
        Sample replay IDs without loading any data.

        Args:
            n: Number of IDs to sample.
            seed: Random seed. Does not affect global random state.
        """
        rng = random.Random(seed)
        n = min(n, len(self.replay_ids))
        return rng.sample(self.replay_ids, n)

    def get_frame_count_summary(self) -> Dict[str, Any]:
        """
        Summarize frame count distribution using DB metadata. No file I/O.

        Only available in database mode.
        """
        if not self.use_db:
            return {'error': 'Frame count summary requires database mode'}

        counts = np.array([
            info['frame_count']
            for info in self.replay_info.values()
            if info.get('frame_count') is not None
        ])

        if len(counts) == 0:
            return {'error': 'No frame count data available'}

        return {
            'count': int(len(counts)),
            'mean': float(counts.mean()),
            'std': float(counts.std()),
            'min': int(counts.min()),
            'max': int(counts.max()),
            'median': float(np.median(counts)),
            'total_frames': int(counts.sum()),
        }


class WindowedReplayDataset:
    """
    Fixed-length sliding window dataset over parsed replays.

    Indexes all possible (replay_id, start_frame) windows at construction time
    using frame counts from the database — no file I/O needed until the first
    sample is requested. Replay arrays are cached in a bounded LRU cache to
    minimize redundant reads across windows from the same replay.

    Compatible with torch.utils.data.DataLoader. Each sample is a float32
    numpy array of shape (window_size, num_features). PyTorch's default collate
    converts these to tensors automatically.

    Note on DataLoader workers: each worker process gets its own copy of this
    dataset (and its own cache). With shuffle=True, windows from the same replay
    may be spread across different workers, reducing cache effectiveness. If
    training throughput is bottlenecked by data loading, consider increasing
    cache_size or pre-downloading all files to a local cache_dir.

    Example:
        dataset = ReplayDataset(db_path='./impulse.db', s3_manager=s3,
                                cache_dir='/data/parsed')
        windowed = WindowedReplayDataset(dataset, window_size=128, stride=16)
        print(f"{len(windowed):,} total windows")

        # With PyTorch DataLoader
        loader = DataLoader(windowed, batch_size=256, num_workers=8,
                            shuffle=True, pin_memory=True)
        for batch in loader:
            # batch: tensor of shape (256, 128, num_features)
            train_step(batch)
    """

    def __init__(
        self,
        dataset: ReplayDataset,
        window_size: int,
        stride: int = 1,
        cache_size: int = 16,
    ):
        """
        Args:
            dataset: ReplayDataset to draw windows from.
            window_size: Number of frames per window.
            stride: Step size between consecutive window start positions.
                    stride=1: dense, maximally overlapping windows.
                    stride=window_size: non-overlapping windows.
            cache_size: Max number of replay arrays held in memory per worker.
                        Each entry is approximately frame_count × features × 4 bytes.
                        Default of 16 is conservative; increase if RAM allows.
        """
        self.dataset = dataset
        self.window_size = window_size
        self.stride = stride
        self._cache_size = cache_size
        self._cache: OrderedDict[str, np.ndarray] = OrderedDict()
        # Index stored as two parallel int32 arrays to minimize memory.
        # _replay_ids_list maps integer -> replay_id string.
        # _index_replay: int32 array of indices into _replay_ids_list.
        # _index_start:  int32 array of window start frames.
        self._replay_ids_list: List[str] = []
        self._index_replay: np.ndarray = np.empty(0, dtype=np.int32)
        self._index_start: np.ndarray = np.empty(0, dtype=np.int32)
        self._build_index()

    def _build_index(self):
        """
        Build the window index from DB frame counts. No parquet file I/O.

        Uses parallel int32 numpy arrays rather than a list of Python tuples to
        keep memory overhead manageable at low strides. A list of 65M Python
        tuples (stride=1, ~7k replays) would consume ~5GB; the array approach
        uses ~520MB for the same index.

        Replays with fewer frames than window_size are skipped.
        """
        replay_indices: List[int] = []
        start_frames: List[int] = []
        skipped = 0

        for replay_idx, (replay_id, info) in enumerate(self.dataset.replay_info.items()):
            self._replay_ids_list.append(replay_id)
            frame_count = info.get('frame_count')
            if not frame_count or frame_count < self.window_size:
                skipped += 1
                continue
            starts = range(0, frame_count - self.window_size + 1, self.stride)
            replay_indices.extend([replay_idx] * len(starts))
            start_frames.extend(starts)

        self._index_replay = np.array(replay_indices, dtype=np.int32)
        self._index_start = np.array(start_frames, dtype=np.int32)

        used = len(self.dataset) - skipped
        print(
            f"WindowedReplayDataset: {len(self._index_replay):,} windows from "
            f"{used}/{len(self.dataset)} replays "
            f"(window_size={self.window_size}, stride={self.stride})"
        )

    def _get_array(self, replay_id: str) -> np.ndarray:
        """Load a replay as a float32 numpy array, using the LRU cache."""
        if replay_id in self._cache:
            self._cache.move_to_end(replay_id)
            return self._cache[replay_id]

        replay = self.dataset.load_replay(replay_id)
        if replay is None:
            raise RuntimeError(f"Failed to load replay {replay_id}")

        arr = replay.frames.to_numpy(dtype=np.float32)

        if len(self._cache) >= self._cache_size:
            self._cache.popitem(last=False)
        self._cache[replay_id] = arr
        return arr

    def __len__(self) -> int:
        return len(self._index_replay)

    def __getitem__(self, idx: int) -> np.ndarray:
        """
        Return a single window as a float32 array of shape (window_size, features).
        """
        replay_id = self._replay_ids_list[self._index_replay[idx]]
        start = int(self._index_start[idx])
        arr = self._get_array(replay_id)
        return arr[start:start + self.window_size]
