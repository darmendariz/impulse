"""
PyTorch Dataset for training on preprocessed replay segments.

Builds a window index from segment boundaries stored in the database,
applies a PreprocessingPipeline on-the-fly, and serves fixed-length
windows (or variable-length whole segments) to a PyTorch DataLoader.

Requires PyTorch: install with ``pip install impulse[training]``.
"""

from collections import OrderedDict
from typing import List, Optional, Tuple

import numpy as np

try:
    import torch
    from torch.utils.data import Dataset
except ImportError:
    raise ImportError(
        "PyTorch is required for impulse.training. "
        "Install with: pip install impulse[training]  or  uv sync --extra training"
    )

from impulse.replay_dataset import ReplayDataset
from impulse.preprocessing.pipeline import PreprocessingPipeline
from impulse.preprocessing.segmentation import deserialize_boundaries


class ReplayTrainingDataset(Dataset):
    """
    Segment-aware windowed dataset for PyTorch training.

    Reads segment boundaries from the database (no Parquet I/O at construction),
    expands them into fixed-length windows, and applies a PreprocessingPipeline
    on-the-fly when windows are accessed.

    For variable-length segments (e.g., LSTM with packing), set window_size=None.

    Example (fixed windows):
        dataset = ReplayTrainingDataset(
            dataset=replay_dataset,
            pipeline=pipeline,
            replay_ids=train_ids,
            window_size=128,
            stride=64,
        )
        loader = DataLoader(dataset, batch_size=256, num_workers=4, shuffle=True)

    Example (variable-length segments for LSTM):
        dataset = ReplayTrainingDataset(
            dataset=replay_dataset,
            pipeline=pipeline,
            replay_ids=train_ids,
            window_size=None,  # whole segments
        )
        loader = DataLoader(dataset, batch_size=32, collate_fn=pad_collate)
    """

    def __init__(
        self,
        dataset: ReplayDataset,
        pipeline: PreprocessingPipeline,
        replay_ids: List[str],
        window_size: Optional[int] = 128,
        stride: int = 64,
        cache_size: int = 16,
    ):
        """
        Args:
            dataset: ReplayDataset for loading Parquet files.
            pipeline: PreprocessingPipeline to apply on-the-fly.
            replay_ids: Replay IDs to include (e.g., train split).
            window_size: Frames per window. None for whole segments.
            stride: Step between consecutive windows. Ignored if window_size is None.
            cache_size: Max replays to cache in memory (LRU).
        """
        self.dataset = dataset
        self.pipeline = pipeline
        self.window_size = window_size
        self.stride = stride
        self._cache_size = cache_size
        self._cache: OrderedDict[str, np.ndarray] = OrderedDict()

        self._index: List[Tuple[str, int, int]] = []
        self._build_index(replay_ids)

    def _build_index(self, replay_ids: List[str]):
        """Build the (replay_id, start, end) index from DB segment boundaries."""
        skipped_no_boundaries = 0

        for replay_id in replay_ids:
            info = self.dataset.replay_info.get(replay_id)
            if info is None:
                continue

            boundaries_json = info.get('segment_boundaries')
            if not boundaries_json:
                skipped_no_boundaries += 1
                continue

            boundaries = deserialize_boundaries(boundaries_json)

            if self.window_size is None:
                # Whole segments
                for start, end in boundaries:
                    self._index.append((replay_id, start, end))
            else:
                # Fixed-length windows within each segment
                for seg_start, seg_end in boundaries:
                    seg_len = seg_end - seg_start
                    if seg_len < self.window_size:
                        continue
                    for w_start in range(seg_start, seg_end - self.window_size + 1, self.stride):
                        self._index.append((replay_id, w_start, w_start + self.window_size))

        if skipped_no_boundaries > 0:
            print(
                f"Warning: {skipped_no_boundaries} replays skipped "
                f"(no segment boundaries in DB). Run backfill script."
            )

        mode = "whole segments" if self.window_size is None else f"windows (size={self.window_size}, stride={self.stride})"
        print(
            f"ReplayTrainingDataset: {len(self._index):,} {mode} "
            f"from {len(replay_ids)} replays"
        )

    def _get_processed_array(self, replay_id: str) -> np.ndarray:
        """Load, preprocess, and cache a replay as a float32 numpy array."""
        if replay_id in self._cache:
            self._cache.move_to_end(replay_id)
            return self._cache[replay_id]

        replay = self.dataset.load_replay(replay_id)
        if replay is None:
            raise RuntimeError(f"Failed to load replay {replay_id}")

        processed = self.pipeline(replay.frames)
        arr = processed.to_numpy(dtype=np.float32)

        if len(self._cache) >= self._cache_size:
            self._cache.popitem(last=False)
        self._cache[replay_id] = arr
        return arr

    def __len__(self) -> int:
        return len(self._index)

    def __getitem__(self, idx: int) -> torch.Tensor:
        """
        Return a single window/segment as a float32 tensor.

        Shape: (window_size, num_features) for fixed windows,
               (segment_length, num_features) for variable-length.
        """
        replay_id, start, end = self._index[idx]
        arr = self._get_processed_array(replay_id)
        return torch.from_numpy(arr[start:end].copy())
