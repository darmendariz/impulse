"""
Impulse - Rocket League Replay Data Pipeline

A library for collecting, parsing, preprocessing, and training on
Rocket League replay data.

Main interfaces:
    ReplayDataset: Load and access parsed replay data
    ReplayData: Container for a single replay's frames and metadata
    PreprocessingPipeline: Chain of transforms for feature selection and normalization
    FeatureSelector: Select features by preset or column list
    PhysicalNormalizer: Normalize features using known physical bounds
    find_segment_boundaries: Identify continuous gameplay periods
    split_replay_ids: Deterministic train/val/test split
"""

from impulse.replay_dataset import ReplayDataset, ReplayData
from impulse.preprocessing.pipeline import PreprocessingPipeline
from impulse.preprocessing.transforms import FeatureSelector, PhysicalNormalizer
from impulse.preprocessing.segmentation import find_segment_boundaries, split_replay_ids

__all__ = [
    'ReplayDataset',
    'ReplayData',
    'PreprocessingPipeline',
    'FeatureSelector',
    'PhysicalNormalizer',
    'find_segment_boundaries',
    'split_replay_ids',
]
