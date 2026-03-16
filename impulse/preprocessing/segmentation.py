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

    # Pattern 1: full dataset with train/val/test splits (lazy loading)
    seg_dataset = SegmentedDataset(dataset, train_ratio=0.7, val_ratio=0.15,
                                   test_ratio=0.15, seed=42)
    for segment in seg_dataset.train_segments():
        pass  # train on each segment's frames

    # Pattern 2: sample n replays, no split, lazy loading
    seg_dataset = SegmentedDataset.from_sample(dataset, n=200, seed=42)
    for segment in seg_dataset.all_segments():
        pass

    # Pattern 3: pre-loaded replay list, no file I/O during segmentation
    replay_list = dataset.load_sample(200, seed=42)
    seg_dataset = SegmentedDataset.from_replay_list(replay_list)
    segments = seg_dataset.all_segments_list()

    seg_dataset.print_summary()
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
    Dataset of replay segments with optional train/val/test splitting.

    Wraps a ReplayDataset (lazy loading) or a pre-loaded list of ReplayData
    objects (eager). Segments each replay at kickoff reset events.

    When train/val/test ratios are provided, splits at the replay level to
    prevent data leakage (all segments from one replay belong to the same
    split). When no ratios are provided, all segments are accessible via
    all_segments().

    Usage patterns:

        Pattern 1 — full dataset, with splits (lazy loading):
            seg_dataset = SegmentedDataset(dataset, train_ratio=0.7,
                                           val_ratio=0.15, test_ratio=0.15)

        Pattern 2 — sampled replays, no split (no file I/O at construction):
            seg_dataset = SegmentedDataset.from_sample(dataset, n=200, seed=42)

        Pattern 3 — pre-loaded replays, no split (from List[ReplayData]):
            seg_dataset = SegmentedDataset.from_replay_list(replay_list)
    """

    def __init__(
        self,
        dataset: ReplayDataset,
        train_ratio: Optional[float] = None,
        val_ratio: Optional[float] = None,
        test_ratio: Optional[float] = None,
        seed: int = 42,
        min_segment_frames: int = 10,
    ):
        """
        Initialize a SegmentedDataset from a ReplayDataset.

        When train_ratio, val_ratio, and test_ratio are all None, no split is
        performed and all replays are accessible via all_segments(). When any
        ratio is provided, missing ones default to 0.0 and all ratios must
        sum to 1.0.

        Args:
            dataset: Source ReplayDataset to segment
            train_ratio: Fraction of replays for training (None = no split)
            val_ratio: Fraction of replays for validation (None = no split)
            test_ratio: Fraction of replays for testing (None = no split)
            seed: Random seed for split reproducibility
            min_segment_frames: Minimum frames per segment
        """
        self._setup(
            dataset=dataset,
            replay_dict=None,
            replay_ids=list(dataset.replay_ids),
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            test_ratio=test_ratio,
            seed=seed,
            min_segment_frames=min_segment_frames,
        )

    def _setup(
        self,
        dataset: Optional[ReplayDataset],
        replay_dict: Optional[Dict[str, Any]],
        replay_ids: List[str],
        train_ratio: Optional[float],
        val_ratio: Optional[float],
        test_ratio: Optional[float],
        seed: int,
        min_segment_frames: int,
    ) -> None:
        """Set all instance attributes. Called by __init__ and factory methods."""
        self.dataset = dataset
        self._replay_dict = replay_dict
        self.all_ids = list(replay_ids)
        self.min_segment_frames = min_segment_frames

        if train_ratio is None and val_ratio is None and test_ratio is None:
            self.train_ids: List[str] = []
            self.val_ids: List[str] = []
            self.test_ids: List[str] = []
        else:
            tr = train_ratio if train_ratio is not None else 0.0
            vr = val_ratio if val_ratio is not None else 0.0
            te = test_ratio if test_ratio is not None else 0.0
            self.train_ids, self.val_ids, self.test_ids = split_replay_ids(
                replay_ids,
                train_ratio=tr,
                val_ratio=vr,
                test_ratio=te,
                seed=seed,
            )

    @classmethod
    def from_sample(
        cls,
        dataset: ReplayDataset,
        n: int,
        seed: int = 42,
        train_ratio: Optional[float] = None,
        val_ratio: Optional[float] = None,
        test_ratio: Optional[float] = None,
        min_segment_frames: int = 10,
    ) -> 'SegmentedDataset':
        """
        Build a SegmentedDataset by sampling n replay IDs from a ReplayDataset.

        No file I/O occurs at construction time; replays are loaded lazily
        when iterating over segments. The sample is deterministic for a given
        seed.

        Args:
            dataset: Source ReplayDataset to sample from
            n: Number of replay IDs to sample
            seed: Random seed for both sampling and optional split
            train_ratio: Fraction for training (None = no split)
            val_ratio: Fraction for validation (None = no split)
            test_ratio: Fraction for testing (None = no split)
            min_segment_frames: Minimum frames per segment

        Returns:
            A new SegmentedDataset instance
        """
        rng = random.Random(seed)
        ids = list(dataset.replay_ids)
        sampled_ids = rng.sample(ids, min(n, len(ids)))

        instance = cls.__new__(cls)
        instance._setup(
            dataset=dataset,
            replay_dict=None,
            replay_ids=sampled_ids,
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            test_ratio=test_ratio,
            seed=seed,
            min_segment_frames=min_segment_frames,
        )
        return instance

    @classmethod
    def from_replay_list(
        cls,
        replay_list: List[Any],
        train_ratio: Optional[float] = None,
        val_ratio: Optional[float] = None,
        test_ratio: Optional[float] = None,
        seed: int = 42,
        min_segment_frames: int = 10,
    ) -> 'SegmentedDataset':
        """
        Build a SegmentedDataset from a pre-loaded list of ReplayData objects.

        No file I/O occurs at all — replays are already in memory. Useful when
        you have already called dataset.load_sample() and want to segment and
        iterate without any further disk or network access. Frames may be
        pre-processed (e.g. columns dropped, normalized) before passing in.

        Args:
            replay_list: List of ReplayData objects already in memory
            train_ratio: Fraction for training (None = no split)
            val_ratio: Fraction for validation (None = no split)
            test_ratio: Fraction for testing (None = no split)
            seed: Random seed for optional split
            min_segment_frames: Minimum frames per segment

        Returns:
            A new SegmentedDataset instance
        """
        replay_dict = {r.replay_id: r for r in replay_list}

        instance = cls.__new__(cls)
        instance._setup(
            dataset=None,
            replay_dict=replay_dict,
            replay_ids=list(replay_dict.keys()),
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            test_ratio=test_ratio,
            seed=seed,
            min_segment_frames=min_segment_frames,
        )
        return instance

    def _load_replay(self, replay_id: str) -> Optional[Any]:
        """
        Load a replay by ID, dispatching to the appropriate source.

        Uses _replay_dict when created from a pre-loaded list, otherwise
        delegates to self.dataset.load_replay().
        """
        if self._replay_dict is not None:
            return self._replay_dict.get(replay_id)
        return self.dataset.load_replay(replay_id)

    def _segments_for_ids(self, replay_ids: List[str]) -> Iterator[ReplaySegment]:
        """Lazily load and segment replays by ID."""
        for replay_id in replay_ids:
            replay = self._load_replay(replay_id)
            if replay is not None:
                yield from segment_replay(replay, self.min_segment_frames)

    def all_segments(self) -> Iterator[ReplaySegment]:
        """Lazily iterate over all segments regardless of split."""
        return self._segments_for_ids(self.all_ids)

    def all_segments_list(self) -> List[ReplaySegment]:
        """Load all segments into a list regardless of split."""
        return list(self.all_segments())

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
        """Print summary statistics. Shows per-split stats when a split was
        configured, or overall stats when no split was configured."""
        print("=" * 60)
        print("SEGMENTED DATASET SUMMARY")
        print("=" * 60)

        has_split = bool(self.train_ids or self.val_ids or self.test_ids)

        if has_split:
            splits = [
                ("Train", self.train_ids),
                ("Validation", self.val_ids),
                ("Test", self.test_ids),
            ]
        else:
            splits = [("All (no split)", self.all_ids)]

        for split_name, replay_ids in splits:
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
