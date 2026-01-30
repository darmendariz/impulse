"""
Impulse Analysis Module

Exploratory data analysis for parsed replay data.

Usage:
    from impulse.analysis import ReplayDataset, summarize_dataset, plot_sequence_length_distribution

    # Load data
    dataset = ReplayDataset('./parsed_replays')
    sample = dataset.load_sample(n=50)

    # Analyze
    summary = summarize_dataset(sample)
    print_summary(summary)

    # Visualize
    plot_sequence_length_distribution(sample)
"""

from impulse.analysis.data_loader import ReplayDataset
from impulse.analysis.eda import (
    # Dataset summary
    summarize_dataset,
    print_summary,
    # Feature analysis
    compute_feature_stats,
    get_sequence_lengths,
    # Visualization
    plot_sequence_length_distribution,
    plot_feature_distributions,
    plot_time_series,
    plot_trajectory_2d,
    plot_correlation_matrix,
    # Ball-specific analysis
    compute_ball_velocity_magnitude,
    detect_impacts,
    analyze_impacts,
    plot_velocity_with_impacts,
    plot_time_between_impacts,
)

__all__ = [
    # Data loading
    'ReplayDataset',
    # Dataset summary
    'summarize_dataset',
    'print_summary',
    # Feature analysis
    'compute_feature_stats',
    'get_sequence_lengths',
    # Visualization
    'plot_sequence_length_distribution',
    'plot_feature_distributions',
    'plot_time_series',
    'plot_trajectory_2d',
    'plot_correlation_matrix',
    # Ball-specific
    'compute_ball_velocity_magnitude',
    'detect_impacts',
    'analyze_impacts',
    'plot_velocity_with_impacts',
    'plot_time_between_impacts',
]
