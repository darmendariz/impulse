"""
Impulse Configuration Module

Configuration for parsing, pipeline settings, and feature definitions.
"""

from impulse.config.feature_config import (
    PHYSICAL_BOUNDS,
    get_feature_columns,
    get_normalization_divisors,
)

__all__ = [
    'PHYSICAL_BOUNDS',
    'get_feature_columns',
    'get_normalization_divisors',
]
