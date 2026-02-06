"""
Impulse Preprocessing Module

Transforms parsed replay data into ML-ready formats.

Key components:
    ReplaySegment: Container for a continuous gameplay segment
    SegmentedDataset: Dataset with train/val/test splits at the replay level
    segment_replay: Segment a single replay at kickoff resets
    segment_replays: Segment multiple replays
    split_replay_ids: Deterministic train/val/test split of replay IDs
"""

from impulse.preprocessing.segmentation import (
    ReplaySegment,
    SegmentedDataset,
    segment_replay,
    segment_replays,
    split_replay_ids,
)

__all__ = [
    'ReplaySegment',
    'SegmentedDataset',
    'segment_replay',
    'segment_replays',
    'split_replay_ids',
]
