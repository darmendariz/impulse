"""
Impulse Preprocessing Module

Transforms parsed replay data into ML-ready formats.

Key components:
    PreprocessingPipeline: Chain of transforms (DataFrame -> DataFrame)
    FeatureSelector: Select features by preset or column list
    PhysicalNormalizer: Normalize features using physical bounds (with inverse)
    find_segment_boundaries: Identify continuous gameplay segments
    split_replay_ids: Deterministic train/val/test split of replay IDs
"""

from impulse.preprocessing.pipeline import PreprocessingPipeline
from impulse.preprocessing.transforms import FeatureSelector, PhysicalNormalizer
from impulse.preprocessing.segmentation import (
    find_segment_boundaries,
    serialize_boundaries,
    deserialize_boundaries,
)

__all__ = [
    'PreprocessingPipeline',
    'FeatureSelector',
    'PhysicalNormalizer',
    'find_segment_boundaries',
    'serialize_boundaries',
    'deserialize_boundaries',
]
