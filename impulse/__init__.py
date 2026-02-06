"""
Impulse - Rocket League Replay Data Pipeline

A library for collecting, parsing, and analyzing Rocket League replay data.

Main interfaces:
    ReplayDataset: Load and access parsed replay data
    ReplayData: Container for a single replay's frames and metadata
    SegmentedDataset: Segmented dataset with train/val/test splits
    ReplaySegment: Container for a continuous gameplay segment
"""

from impulse.replay_dataset import ReplayDataset, ReplayData
from impulse.preprocessing.segmentation import SegmentedDataset, ReplaySegment

__all__ = [
    'ReplayDataset',
    'ReplayData',
    'SegmentedDataset',
    'ReplaySegment',
]
