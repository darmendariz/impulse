"""
Replay segmentation and data splitting utilities.

Segments replays at kickoff resets (which occur after goals) to identify
continuous gameplay periods. Returns lightweight boundary indices rather
than copying frame data.

Usage:
    from impulse.preprocessing import find_segment_boundaries, split_replay_ids

    # Find segment boundaries (just index pairs, no data copying)
    boundaries = find_segment_boundaries(replay.frames)
    # [(0, 450), (460, 1200), (1210, 3500)]

    # Split replay IDs for train/val/test
    train_ids, val_ids, test_ids = split_replay_ids(dataset.replay_ids)
"""

import json
import random
from typing import List, Tuple

import pandas as pd

from impulse.preprocessing.kickoff_setup_detection import (
    kickoff_setup_frames,
    continuous_frame_ranges,
)


def find_segment_boundaries(
    frames: pd.DataFrame,
    min_segment_frames: int = 10,
) -> List[Tuple[int, int]]:
    """
    Find continuous gameplay segment boundaries in a replay.

    Detects kickoff setup frames (ball at origin with zero velocity) and
    returns the frame index ranges between them. Kickoff frames are excluded
    from all segments.

    Args:
        frames: Replay DataFrame with ball position/velocity columns.
        min_segment_frames: Minimum number of frames for a segment to be
            included. Segments shorter than this are discarded.

    Returns:
        List of (start, end_exclusive) tuples representing continuous
        gameplay periods. Use these to slice into the frames DataFrame
        or a preprocessed array: ``frames.iloc[start:end]``.
    """
    total_frames = len(frames)

    ks_frames = kickoff_setup_frames(frames)

    if ks_frames.empty:
        # No kickoff resets — entire replay is one segment
        if total_frames >= min_segment_frames:
            return [(0, total_frames)]
        return []

    kickoff_ranges = continuous_frame_ranges(ks_frames)

    boundaries: List[Tuple[int, int]] = []

    # First segment: frame 0 to start of first kickoff
    boundaries.append((0, kickoff_ranges[0][0]))

    # Middle segments: between consecutive kickoff ranges
    for i in range(1, len(kickoff_ranges)):
        seg_start = kickoff_ranges[i - 1][1] + 1
        seg_end = kickoff_ranges[i][0]
        boundaries.append((seg_start, seg_end))

    # Last segment: after last kickoff to end of replay
    last_kickoff_end = kickoff_ranges[-1][1] + 1
    if last_kickoff_end < total_frames:
        boundaries.append((last_kickoff_end, total_frames))

    # Filter by minimum length
    return [(s, e) for s, e in boundaries if e - s >= min_segment_frames]


def serialize_boundaries(boundaries: List[Tuple[int, int]]) -> str:
    """Serialize segment boundaries to a JSON string for database storage."""
    return json.dumps(boundaries)


def deserialize_boundaries(data: str) -> List[Tuple[int, int]]:
    """Deserialize segment boundaries from a JSON string."""
    return [tuple(pair) for pair in json.loads(data)]


def split_replay_ids(
    replay_ids: List[str],
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    seed: int = 42,
) -> Tuple[List[str], List[str], List[str]]:
    """
    Split replay IDs into train, validation, and test sets.

    The split is deterministic for a given seed and input list.

    Args:
        replay_ids: List of replay IDs to split.
        train_ratio: Fraction of replays for training.
        val_ratio: Fraction of replays for validation.
        test_ratio: Fraction of replays for testing.
        seed: Random seed for reproducibility.

    Returns:
        Tuple of (train_ids, val_ids, test_ids).

    Raises:
        ValueError: If ratios don't sum to approximately 1.0.
    """
    if abs(train_ratio + val_ratio + test_ratio - 1.0) > 1e-6:
        raise ValueError(
            f"Ratios must sum to 1.0, got {train_ratio + val_ratio + test_ratio:.6f}"
        )

    ids = list(replay_ids)
    rng = random.Random(seed)
    rng.shuffle(ids)

    n = len(ids)
    train_end = int(n * train_ratio)
    val_end = train_end + int(n * val_ratio)

    return ids[:train_end], ids[train_end:val_end], ids[val_end:]
