"""
Replay Segmentation - Split replays into continuous gameplay segments.

Segments replays at kickoff resets (which occur after goals) to produce
continuous gameplay periods suitable for ML training (e.g., ball path prediction).

Kickoff reset frames (ball stationary at origin) are excluded from segments
since they contain no meaningful ball dynamics.

Usage:
    from impulse.preprocessing import segment_replay, SegmentedDataset
    from impulse import ReplayDataset

    # Segment a single replay
    dataset = ReplayDataset(db_path='./impulse.db', data_dir='./replays/parsed')
    replay = dataset.load_replay('some-id')
    segments = segment_replay(replay)

    # Full segmented dataset with train/val/test splits
    seg_dataset = SegmentedDataset(dataset, seed=42)
    for segment in seg_dataset.train_segments():
        # train on each segment's frames 
"""

import random
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Iterator, Tuple

import pandas as pd

from impulse.replay_dataset import ReplayData, ReplayDataset
from impulse.preprocessing.kickoff_setup_detection import (
    kickoff_setup_frames,
    continuous_frame_ranges,
)


@dataclass
class ReplaySegment:
    """
    A continuous segment of gameplay from a single replay.

    Represents the frames between kickoff resets, excluding the kickoff
    setup frames themselves (where the ball is stationary at the origin).

    Attributes:
        replay_id: ID of the source replay
        segment_index: 0-based index of this segment within the replay
        frames: DataFrame slice containing the segment's frame data (index reset to 0)
        original_start_frame: Start index in the source replay's frames DataFrame
        original_end_frame: Exclusive end index in the source replay's frames DataFrame
        metadata: Replay metadata plus segment-specific info
    """
    replay_id: str
    segment_index: int
    frames: pd.DataFrame
    original_start_frame: int
    original_end_frame: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.frames)

    @property
    def num_frames(self) -> int:
        return len(self.frames)


def segment_replay(
    replay: ReplayData,
    min_segment_frames: int = 10,
) -> List[ReplaySegment]:
    """
    Segment a replay into continuous gameplay segments separated by kickoff resets.

    Detects kickoff setup frames (ball at origin with zero velocity) and splits
    the replay at those boundaries. Kickoff frames are excluded from all segments.

    Args:
        replay: A loaded ReplayData object
        min_segment_frames: Minimum number of frames for a segment to be included.
            Segments shorter than this are discarded.

    Returns:
        List of ReplaySegment objects, one per continuous gameplay period.
    """
    frames = replay.frames
    total_frames = len(frames)

    # Detect kickoff setup frames
    ks_frames = kickoff_setup_frames(frames)

    if ks_frames.empty:
        # No kickoff resets detected — entire replay is one segment
        segment_frames = frames.copy()
        segment_frames.index = range(len(segment_frames))
        return [ReplaySegment(
            replay_id=replay.replay_id,
            segment_index=0,
            frames=segment_frames,
            original_start_frame=0,
            original_end_frame=total_frames,
            metadata={
                **replay.metadata,
                'segment_index': 0,
                'num_segments': 1,
                'total_replay_frames': total_frames,
            },
        )]

    kickoff_ranges = continuous_frame_ranges(ks_frames)

    # Build segment boundaries: list of (start, end_exclusive) tuples for gameplay
    segment_boundaries: List[Tuple[int, int]] = []

    # First segment: frame 0 to start of first kickoff
    segment_boundaries.append((0, kickoff_ranges[0][0]))

    # Middle segments: between consecutive kickoff ranges
    for i in range(1, len(kickoff_ranges)):
        seg_start = kickoff_ranges[i - 1][1] + 1
        seg_end = kickoff_ranges[i][0]
        segment_boundaries.append((seg_start, seg_end))

    # Last segment: after last kickoff to end of replay
    last_kickoff_end = kickoff_ranges[-1][1] + 1
    if last_kickoff_end < total_frames:
        segment_boundaries.append((last_kickoff_end, total_frames))

    # Build ReplaySegment objects
    segments: List[ReplaySegment] = []
    for start, end in segment_boundaries:
        if end - start < min_segment_frames:
            continue

        segment_frames = frames.iloc[start:end].copy()
        segment_frames.index = range(len(segment_frames))

        segments.append(ReplaySegment(
            replay_id=replay.replay_id,
            segment_index=len(segments),
            frames=segment_frames,
            original_start_frame=start,
            original_end_frame=end,
            metadata={
                **replay.metadata,
                'segment_index': len(segments),
                'total_replay_frames': total_frames,
            },
        ))

    # Update num_segments in metadata now that we know the final count
    for seg in segments:
        seg.metadata['num_segments'] = len(segments)

    return segments


def segment_replays(
    replays: List[ReplayData],
    min_segment_frames: int = 10,
) -> List[ReplaySegment]:
    """
    Segment multiple replays into continuous gameplay segments.

    Args:
        replays: List of ReplayData objects
        min_segment_frames: Minimum frames per segment

    Returns:
        Flat list of all ReplaySegment objects across all replays.
    """
    segments: List[ReplaySegment] = []
    for replay in replays:
        segments.extend(segment_replay(replay, min_segment_frames))
    return segments


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
        replay_ids: List of replay IDs to split
        train_ratio: Fraction of replays for training
        val_ratio: Fraction of replays for validation
        test_ratio: Fraction of replays for testing
        seed: Random seed for reproducibility

    Returns:
        Tuple of (train_ids, val_ids, test_ids)

    Raises:
        ValueError: If ratios don't sum to approximately 1.0
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

    train_ids = ids[:train_end]
    val_ids = ids[train_end:val_end]
    test_ids = ids[val_end:]

    return train_ids, val_ids, test_ids


class SegmentedDataset:
    """
    Dataset of replay segments for ML training.

    Wraps a ReplayDataset and segments each replay at kickoff reset events.
    Handles train/val/test splitting at the replay level to prevent
    data leakage (all segments from one replay belong to the same split).

    Usage:
        from impulse import ReplayDataset
        from impulse.preprocessing import SegmentedDataset

        dataset = ReplayDataset(db_path='./impulse.db', data_dir='./replays/parsed')
        seg_dataset = SegmentedDataset(dataset, seed=42)

        for segment in seg_dataset.train_segments():
            # train on each segment's frames

        seg_dataset.print_summary()
    """

    def __init__(
        self,
        dataset: ReplayDataset,
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        test_ratio: float = 0.15,
        seed: int = 42,
        min_segment_frames: int = 10,
    ):
        """
        Initialize the segmented dataset.

        Args:
            dataset: Source ReplayDataset to segment
            train_ratio: Fraction of replays for training
            val_ratio: Fraction of replays for validation
            test_ratio: Fraction of replays for testing
            seed: Random seed for split reproducibility
            min_segment_frames: Minimum frames per segment
        """
        self.dataset = dataset
        self.min_segment_frames = min_segment_frames

        self.train_ids, self.val_ids, self.test_ids = split_replay_ids(
            dataset.replay_ids,
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            test_ratio=test_ratio,
            seed=seed,
        )

    def _segments_for_ids(self, replay_ids: List[str]) -> Iterator[ReplaySegment]:
        """Lazily load and segment replays by ID."""
        for replay_id in replay_ids:
            replay = self.dataset.load_replay(replay_id)
            if replay is not None:
                yield from segment_replay(replay, self.min_segment_frames)

    def train_segments(self) -> Iterator[ReplaySegment]:
        """Lazily iterate over training segments."""
        return self._segments_for_ids(self.train_ids)

    def val_segments(self) -> Iterator[ReplaySegment]:
        """Lazily iterate over validation segments."""
        return self._segments_for_ids(self.val_ids)

    def test_segments(self) -> Iterator[ReplaySegment]:
        """Lazily iterate over test segments."""
        return self._segments_for_ids(self.test_ids)

    def train_segments_list(self) -> List[ReplaySegment]:
        """Load all training segments into a list."""
        return list(self.train_segments())

    def val_segments_list(self) -> List[ReplaySegment]:
        """Load all validation segments into a list."""
        return list(self.val_segments())

    def test_segments_list(self) -> List[ReplaySegment]:
        """Load all test segments into a list."""
        return list(self.test_segments())

    def print_summary(self) -> None:
        """Print summary statistics for each split."""
        print("=" * 60)
        print("SEGMENTED DATASET SUMMARY")
        print("=" * 60)

        for split_name, replay_ids in [
            ("Train", self.train_ids),
            ("Validation", self.val_ids),
            ("Test", self.test_ids),
        ]:
            segments = list(self._segments_for_ids(replay_ids))
            frame_counts = [len(seg) for seg in segments]
            total_frames = sum(frame_counts)

            print(f"\n{split_name}:")
            print(f"  Replays:        {len(replay_ids)}")
            print(f"  Segments:       {len(segments)}")
            print(f"  Total frames:   {total_frames}")
            if frame_counts:
                print(f"  Frames/segment: min={min(frame_counts)}, "
                      f"max={max(frame_counts)}, "
                      f"mean={total_frames / len(frame_counts):.0f}")

        print()
