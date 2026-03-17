"""
Preprocessing pipeline for chaining transforms on replay DataFrames.

The pipeline applies a sequence of transforms (FeatureSelector, PhysicalNormalizer,
or any callable DataFrame -> DataFrame) and supports inverse transformation for
converting model outputs back to physical units.

Usage:
    from impulse.preprocessing import PreprocessingPipeline, FeatureSelector, PhysicalNormalizer

    pipeline = PreprocessingPipeline([
        FeatureSelector.from_preset('physics'),
        PhysicalNormalizer(),
    ])

    processed = pipeline(replay.frames)
    raw_values = pipeline.inverse(predicted_df)
"""

from typing import List, Union

import pandas as pd

from impulse.preprocessing.transforms import FeatureSelector, PhysicalNormalizer


# Any callable that takes a DataFrame and returns a DataFrame
Transform = Union[FeatureSelector, PhysicalNormalizer]


class PreprocessingPipeline:
    """
    Chain of transforms applied sequentially to a replay DataFrame.

    Each transform must be callable with signature (pd.DataFrame) -> pd.DataFrame.
    Transforms that support .inverse() (like PhysicalNormalizer) are used in
    reverse order when calling pipeline.inverse().

    Args:
        steps: Ordered list of transforms to apply.
    """

    def __init__(self, steps: List[Transform]):
        self.steps = steps

    def __call__(self, frames: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transforms in order.

        Args:
            frames: Raw replay DataFrame.

        Returns:
            Transformed DataFrame.
        """
        result = frames
        for step in self.steps:
            result = step(result)
        return result

    def inverse(self, frames: pd.DataFrame) -> pd.DataFrame:
        """
        Apply inverse transforms in reverse order.

        Only steps with an .inverse() method are applied. Steps without
        .inverse() (like FeatureSelector) are skipped — they are not
        reversible and don't need to be.

        Args:
            frames: Transformed/predicted DataFrame.

        Returns:
            DataFrame with values in original physical units.
        """
        result = frames
        for step in reversed(self.steps):
            if hasattr(step, 'inverse'):
                result = step.inverse(result)
        return result

    @property
    def feature_columns(self) -> List[str]:
        """
        Return the output column list, if the first step is a FeatureSelector.

        Useful for knowing the expected feature order without running the pipeline.
        """
        for step in self.steps:
            if isinstance(step, FeatureSelector):
                return list(step.columns)
        return []

    def __repr__(self) -> str:
        step_names = [type(s).__name__ for s in self.steps]
        return f"PreprocessingPipeline(steps={step_names})"
