"""
Impulse - Rocket League Replay Data Pipeline

A library for collecting, parsing, and analyzing Rocket League replay data.

Main interfaces:
    ReplayDataset: Load and access parsed replay data
    ReplayData: Container for a single replay's frames and metadata
"""

from impulse.replay_dataset import ReplayDataset, ReplayData

__all__ = [
    'ReplayDataset',
    'ReplayData',
]
