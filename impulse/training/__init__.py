"""
Impulse Training Module

PyTorch-compatible dataset for training on preprocessed replay data.
Requires PyTorch: install with ``pip install impulse[training]`` or ``uv sync --extra training``.

Key classes:
    ReplayTrainingDataset: Segment-aware windowed dataset for PyTorch DataLoader.
"""

from impulse.training.dataset import ReplayTrainingDataset

__all__ = ['ReplayTrainingDataset']
