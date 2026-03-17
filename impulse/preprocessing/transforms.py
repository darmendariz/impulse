"""
Preprocessing transforms for parsed replay data.

Stateless, composable transforms that operate on DataFrames. Each transform
takes a DataFrame and returns a new DataFrame (no mutation of the input).

Classes:
    FeatureSelector: Select columns by preset or explicit list.
    PhysicalNormalizer: Normalize features using known Rocket League physical bounds.
"""

from typing import Dict, List, Optional

import pandas as pd

from impulse.config.feature_config import get_feature_columns, get_normalization_divisors


class FeatureSelector:
    """
    Select features from a replay DataFrame by preset or explicit column list.

    Usage:
        selector = FeatureSelector.from_preset('physics')
        selected_df = selector(replay.frames)

        selector = FeatureSelector(columns=['Ball - position x', 'p0_position x'])
        selected_df = selector(replay.frames)
    """

    def __init__(self, columns: List[str]):
        """
        Args:
            columns: Explicit list of column names to keep.
        """
        self.columns = columns

    @classmethod
    def from_preset(cls, preset: str = 'physics', num_players: int = 6) -> 'FeatureSelector':
        """
        Create a FeatureSelector from a named preset.

        Args:
            preset: One of 'physics', 'minimal', 'full'.
            num_players: Number of players (6 for 3v3).
        """
        return cls(columns=get_feature_columns(preset, num_players))

    def __call__(self, frames: pd.DataFrame) -> pd.DataFrame:
        """
        Select columns from the DataFrame.

        Args:
            frames: Replay DataFrame with all columns.

        Returns:
            New DataFrame with only the selected columns.

        Raises:
            KeyError: If any requested columns are missing from the DataFrame.
        """
        missing = [c for c in self.columns if c not in frames.columns]
        if missing:
            raise KeyError(
                f"Columns not found in DataFrame: {missing[:5]}"
                f"{'...' if len(missing) > 5 else ''}"
            )
        return frames[self.columns].copy()

    def __repr__(self) -> str:
        return f"FeatureSelector(num_columns={len(self.columns)})"


class PhysicalNormalizer:
    """
    Normalize features using known Rocket League physical bounds.

    Divides each column by its known physical maximum so values fall
    approximately in [-1, 1] or [0, 1]. Quaternion columns are already
    in [-1, 1] and are left unchanged.

    Caches the divisor mapping after the first call so subsequent calls
    with the same schema skip the column-matching step.

    Usage:
        normalizer = PhysicalNormalizer()
        normalized_df = normalizer(selected_df)

        # Inverse: convert back to physical units
        raw_df = normalizer.inverse(normalized_df)
    """

    def __init__(self, bounds: Optional[Dict[str, float]] = None):
        """
        Args:
            bounds: Optional custom divisor mapping (column_name -> divisor).
                    If None, divisors are auto-detected from column names
                    using the standard physical bounds.
        """
        self._custom_bounds = bounds
        self._cached_divisors: Optional[Dict[str, float]] = None
        self._cached_schema: Optional[List[str]] = None

    def _get_divisors(self, columns: List[str]) -> Dict[str, float]:
        """Get or compute the divisor mapping, caching for repeated calls."""
        col_tuple = list(columns)
        if self._cached_schema == col_tuple and self._cached_divisors is not None:
            return self._cached_divisors

        if self._custom_bounds is not None:
            self._cached_divisors = self._custom_bounds
        else:
            self._cached_divisors = get_normalization_divisors(columns)
        self._cached_schema = col_tuple
        return self._cached_divisors

    def __call__(self, frames: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the DataFrame.

        Args:
            frames: DataFrame with feature columns.

        Returns:
            New DataFrame with normalized values.
        """
        divisors = self._get_divisors(list(frames.columns))
        result = frames.copy()
        for col, divisor in divisors.items():
            if col in result.columns:
                result[col] = result[col] / divisor
        return result

    def inverse(self, frames: pd.DataFrame) -> pd.DataFrame:
        """
        Reverse normalization: multiply by physical bounds.

        Args:
            frames: Normalized DataFrame.

        Returns:
            New DataFrame with values in original physical units.
        """
        divisors = self._get_divisors(list(frames.columns))
        result = frames.copy()
        for col, divisor in divisors.items():
            if col in result.columns:
                result[col] = result[col] * divisor
        return result

    def __repr__(self) -> str:
        if self._cached_divisors is not None:
            return f"PhysicalNormalizer(num_columns_normalized={len(self._cached_divisors)})"
        return "PhysicalNormalizer()"
