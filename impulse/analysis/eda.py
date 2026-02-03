"""
Exploratory Data Analysis for Replay Data

Functions for analyzing parsed replay data, including:
- Dataset-level statistics
- Feature distributions
- Time series analysis
- Visualization

Designed for use in Jupyter notebooks.
"""

from typing import List, Dict, Any, Optional, Tuple, TYPE_CHECKING
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

if TYPE_CHECKING:
    from impulse.replay_dataset import ReplayData

# Optional imports with fallbacks
try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False


# =============================================================================
# Dataset Summary
# =============================================================================

def summarize_dataset(replays: "List[ReplayData]") -> Dict[str, Any]:
    """
    Compute summary statistics for a collection of replays.

    Args:
        replays: List of ReplayData objects from ReplayDataset

    Returns:
        Dict with summary statistics
    """
    if not replays:
        return {"error": "No replays provided"}

    frame_counts = []
    feature_counts = []
    player_counts = []

    for replay in replays:
        frame_counts.append(len(replay.frames))
        feature_counts.append(len(replay.frames.columns))

        # Count actual players (non-NaN player columns)
        if replay.metadata and 'parsing_info' in replay.metadata:
            player_counts.append(replay.metadata['parsing_info'].get('num_players', 0))
        else:
            # Estimate from columns
            player_cols = [c for c in replay.frames.columns if c.startswith('p') and '_' in c]
            if player_cols:
                max_player_idx = max(int(c.split('_')[0][1:]) for c in player_cols)
                player_counts.append(max_player_idx + 1)

    frame_counts = np.array(frame_counts)

    return {
        'num_replays': len(replays),
        'total_frames': int(frame_counts.sum()),
        'frame_counts': {
            'mean': float(frame_counts.mean()),
            'std': float(frame_counts.std()),
            'min': int(frame_counts.min()),
            'max': int(frame_counts.max()),
            'median': float(np.median(frame_counts)),
            'q25': float(np.percentile(frame_counts, 25)),
            'q75': float(np.percentile(frame_counts, 75)),
        },
        'feature_count': feature_counts[0] if feature_counts else 0,
        'player_counts': dict(zip(*np.unique(player_counts, return_counts=True))) if player_counts else {},
        'feature_names': list(replays[0].frames.columns) if replays else [],
    }


def print_summary(summary: Dict[str, Any]):
    """Pretty print a dataset summary."""
    print("=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)
    print(f"Replays:        {summary['num_replays']}")
    print(f"Total frames:   {summary['total_frames']:,}")
    print(f"Features:       {summary['feature_count']}")
    print()
    print("Frame counts per replay:")
    fc = summary['frame_counts']
    print(f"  Mean:   {fc['mean']:.1f}")
    print(f"  Std:    {fc['std']:.1f}")
    print(f"  Min:    {fc['min']}")
    print(f"  Max:    {fc['max']}")
    print(f"  Median: {fc['median']:.1f}")
    print()
    print("Player distribution:")
    for num_players, count in sorted(summary['player_counts'].items()):
        print(f"  {num_players} players: {count} replays")
    print("=" * 60)


# =============================================================================
# Feature Statistics
# =============================================================================

def compute_feature_stats(replays: "List[ReplayData]",
                          features: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Compute statistics for each feature across all replays.

    Args:
        replays: List of ReplayData objects
        features: Optional list of feature names to analyze (default: all)

    Returns:
        DataFrame with statistics per feature
    """
    if not replays:
        return pd.DataFrame()

    # Determine features to analyze
    all_features = list(replays[0].frames.columns)
    if features is None:
        features = [f for f in all_features if f != 'frame']

    stats = []
    for feature in features:
        # Concatenate this feature from all replays
        values = []
        for replay in replays:
            if feature in replay.frames.columns:
                values.append(replay.frames[feature].values)

        if not values:
            continue

        all_values = np.concatenate(values)
        valid_values = all_values[~np.isnan(all_values) & ~np.isinf(all_values)]

        if len(valid_values) == 0:
            continue

        stats.append({
            'feature': feature,
            'count': len(valid_values),
            'nan_count': int(np.isnan(all_values).sum()),
            'inf_count': int(np.isinf(all_values).sum()),
            'mean': float(np.mean(valid_values)),
            'std': float(np.std(valid_values)),
            'min': float(np.min(valid_values)),
            'max': float(np.max(valid_values)),
            'median': float(np.median(valid_values)),
            'q25': float(np.percentile(valid_values, 25)),
            'q75': float(np.percentile(valid_values, 75)),
        })

    return pd.DataFrame(stats)


# =============================================================================
# Sequence Length Analysis
# =============================================================================

def get_sequence_lengths(replays: "List[ReplayData]") -> pd.DataFrame:
    """
    Get frame counts for all replays.

    Args:
        replays: List of ReplayData objects

    Returns:
        DataFrame with replay_id and frame_count columns
    """
    data = []
    for replay in replays:
        duration = replay.metadata.get('duration_seconds') if replay.metadata else None

        data.append({
            'replay_id': replay.replay_id,
            'frame_count': len(replay.frames),
            'duration_seconds': duration,
        })

    return pd.DataFrame(data)


# =============================================================================
# Visualization
# =============================================================================

def plot_sequence_length_distribution(replays: "List[ReplayData]",
                                      bins: int = 30,
                                      figsize: Tuple[int, int] = (10, 4)) -> plt.Figure:
    """
    Plot histogram of replay lengths (frame counts).

    Args:
        replays: List of ReplayData objects
        bins: Number of histogram bins
        figsize: Figure size

    Returns:
        matplotlib Figure
    """
    frame_counts = [len(replay.frames) for replay in replays]

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    # Histogram
    axes[0].hist(frame_counts, bins=bins, edgecolor='black', alpha=0.7)
    axes[0].set_xlabel('Frame Count')
    axes[0].set_ylabel('Number of Replays')
    axes[0].set_title('Replay Length Distribution')
    axes[0].axvline(np.mean(frame_counts), color='red', linestyle='--',
                    label=f'Mean: {np.mean(frame_counts):.0f}')
    axes[0].axvline(np.median(frame_counts), color='orange', linestyle='--',
                    label=f'Median: {np.median(frame_counts):.0f}')
    axes[0].legend()

    # Box plot
    axes[1].boxplot(frame_counts, vert=True)
    axes[1].set_ylabel('Frame Count')
    axes[1].set_title('Replay Length Box Plot')

    plt.tight_layout()
    return fig


def plot_feature_distributions(replays: "List[ReplayData]",
                               features: List[str],
                               figsize: Optional[Tuple[int, int]] = None,
                               bins: int = 50) -> plt.Figure:
    """
    Plot histograms for specified features.

    Args:
        replays: List of ReplayData objects
        features: List of feature names to plot
        figsize: Optional figure size (auto-calculated if None)
        bins: Number of histogram bins

    Returns:
        matplotlib Figure
    """
    n_features = len(features)
    n_cols = min(3, n_features)
    n_rows = (n_features + n_cols - 1) // n_cols

    if figsize is None:
        figsize = (4 * n_cols, 3 * n_rows)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
    if n_features == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for i, feature in enumerate(features):
        # Concatenate feature from all replays
        values = []
        for replay in replays:
            if feature in replay.frames.columns:
                values.append(replay.frames[feature].dropna().values)

        if values:
            all_values = np.concatenate(values)
            all_values = all_values[~np.isinf(all_values)]

            axes[i].hist(all_values, bins=bins, edgecolor='black', alpha=0.7)
            axes[i].set_title(feature, fontsize=10)
            axes[i].set_xlabel('Value')
            axes[i].set_ylabel('Count')

    # Hide unused subplots
    for i in range(n_features, len(axes)):
        axes[i].set_visible(False)

    plt.tight_layout()
    return fig


def plot_time_series(df: pd.DataFrame,
                     features: List[str],
                     title: str = "Time Series",
                     figsize: Tuple[int, int] = (12, 6),
                     alpha: float = 0.8) -> plt.Figure:
    """
    Plot time series for specified features from a single replay.

    Args:
        df: DataFrame for one replay
        features: List of feature names to plot
        title: Plot title
        figsize: Figure size
        alpha: Line transparency

    Returns:
        matplotlib Figure
    """
    fig, ax = plt.subplots(figsize=figsize)

    x = df['frame'] if 'frame' in df.columns else range(len(df))

    for feature in features:
        if feature in df.columns:
            ax.plot(x, df[feature], label=feature, alpha=alpha)

    ax.set_xlabel('Frame')
    ax.set_ylabel('Value')
    ax.set_title(title)
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    return fig


def plot_trajectory_2d(df: pd.DataFrame,
                       x_col: str = 'Ball - position x',
                       y_col: str = 'Ball - position y',
                       color_by: Optional[str] = None,
                       title: str = "Ball Trajectory (Top View)",
                       figsize: Tuple[int, int] = (10, 8)) -> plt.Figure:
    """
    Plot 2D trajectory (e.g., top-down view of ball movement).

    Args:
        df: DataFrame for one replay
        x_col: Column name for x position
        y_col: Column name for y position
        color_by: Optional column to color points by (e.g., 'frame' for time)
        title: Plot title
        figsize: Figure size

    Returns:
        matplotlib Figure
    """
    fig, ax = plt.subplots(figsize=figsize)

    x = df[x_col].values
    y = df[y_col].values

    if color_by and color_by in df.columns:
        c = df[color_by].values
        scatter = ax.scatter(x, y, c=c, cmap='viridis', s=1, alpha=0.5)
        plt.colorbar(scatter, ax=ax, label=color_by)
    else:
        ax.plot(x, y, linewidth=0.5, alpha=0.7)
        ax.scatter(x[0], y[0], color='green', s=100, marker='o', label='Start', zorder=5)
        ax.scatter(x[-1], y[-1], color='red', s=100, marker='x', label='End', zorder=5)
        ax.legend()

    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    ax.set_title(title)
    ax.set_aspect('equal')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    return fig


def plot_correlation_matrix(replays: "List[ReplayData]",
                            features: Optional[List[str]] = None,
                            figsize: Tuple[int, int] = (12, 10)) -> plt.Figure:
    """
    Plot correlation matrix for features.

    Args:
        replays: List of ReplayData objects
        features: Optional list of features (default: auto-select numeric ball features)
        figsize: Figure size

    Returns:
        matplotlib Figure
    """
    # Concatenate all replays
    dfs = [replay.frames for replay in replays]
    combined = pd.concat(dfs, ignore_index=True)

    # Select features
    if features is None:
        # Default to ball features
        features = [c for c in combined.columns
                    if 'Ball' in c and combined[c].dtype in ['float64', 'float32', 'int64']]

    if not features:
        features = [c for c in combined.columns
                    if combined[c].dtype in ['float64', 'float32', 'int64']][:20]

    corr = combined[features].corr()

    fig, ax = plt.subplots(figsize=figsize)

    if HAS_SEABORN:
        sns.heatmap(corr, annot=False, cmap='coolwarm', center=0,
                    square=True, ax=ax, cbar_kws={'shrink': 0.8})
    else:
        im = ax.imshow(corr, cmap='coolwarm', vmin=-1, vmax=1)
        plt.colorbar(im, ax=ax, shrink=0.8)
        ax.set_xticks(range(len(features)))
        ax.set_yticks(range(len(features)))
        ax.set_xticklabels(features, rotation=45, ha='right')
        ax.set_yticklabels(features)

    ax.set_title('Feature Correlation Matrix')
    plt.tight_layout()
    return fig


# =============================================================================
# Ball-Specific Analysis
# =============================================================================

def compute_ball_velocity_magnitude(df: pd.DataFrame) -> np.ndarray:
    """
    Compute ball velocity magnitude from velocity components.

    Args:
        df: DataFrame with ball velocity columns

    Returns:
        Array of velocity magnitudes
    """
    vx = df.get('Ball - linear velocity x', pd.Series([0] * len(df))).values
    vy = df.get('Ball - linear velocity y', pd.Series([0] * len(df))).values
    vz = df.get('Ball - linear velocity z', pd.Series([0] * len(df))).values

    return np.sqrt(vx**2 + vy**2 + vz**2)


def detect_impacts(df: pd.DataFrame,
                   threshold: float = 500.0) -> np.ndarray:
    """
    Detect ball impact frames based on velocity change.

    Args:
        df: DataFrame with ball velocity columns
        threshold: Minimum velocity change to count as impact (uu/s)

    Returns:
        Array of frame indices where impacts occurred
    """
    velocity_mag = compute_ball_velocity_magnitude(df)
    delta_v = np.abs(np.diff(velocity_mag))

    impact_frames = np.where(delta_v > threshold)[0] + 1  # +1 because diff shifts by 1
    return impact_frames


def analyze_impacts(replays: "List[ReplayData]",
                    threshold: float = 500.0) -> Dict[str, Any]:
    """
    Analyze ball impact patterns across replays.

    Args:
        replays: List of ReplayData objects
        threshold: Velocity change threshold for impact detection

    Returns:
        Dict with impact statistics
    """
    all_time_between = []
    impacts_per_replay = []

    for replay in replays:
        impact_frames = detect_impacts(replay.frames, threshold)
        impacts_per_replay.append(len(impact_frames))

        if len(impact_frames) > 1:
            time_between = np.diff(impact_frames)
            all_time_between.extend(time_between)

    all_time_between = np.array(all_time_between)

    return {
        'threshold': threshold,
        'total_impacts': sum(impacts_per_replay),
        'impacts_per_replay': {
            'mean': float(np.mean(impacts_per_replay)),
            'std': float(np.std(impacts_per_replay)),
            'min': int(np.min(impacts_per_replay)),
            'max': int(np.max(impacts_per_replay)),
        },
        'frames_between_impacts': {
            'mean': float(np.mean(all_time_between)) if len(all_time_between) > 0 else 0,
            'std': float(np.std(all_time_between)) if len(all_time_between) > 0 else 0,
            'median': float(np.median(all_time_between)) if len(all_time_between) > 0 else 0,
            'min': int(np.min(all_time_between)) if len(all_time_between) > 0 else 0,
            'max': int(np.max(all_time_between)) if len(all_time_between) > 0 else 0,
        },
        'raw_time_between': all_time_between,
    }


def plot_velocity_with_impacts(df: pd.DataFrame,
                               threshold: float = 500.0,
                               figsize: Tuple[int, int] = (14, 5)) -> plt.Figure:
    """
    Plot ball velocity magnitude with impact points highlighted.

    Args:
        df: DataFrame for one replay
        threshold: Impact detection threshold
        figsize: Figure size

    Returns:
        matplotlib Figure
    """
    velocity_mag = compute_ball_velocity_magnitude(df)
    impact_frames = detect_impacts(df, threshold)

    fig, ax = plt.subplots(figsize=figsize)

    frames = df['frame'].values if 'frame' in df.columns else np.arange(len(df))

    ax.plot(frames, velocity_mag, linewidth=0.5, alpha=0.8, label='Velocity magnitude')
    ax.scatter(frames[impact_frames], velocity_mag[impact_frames],
               color='red', s=20, alpha=0.7, label=f'Impacts (n={len(impact_frames)})')

    ax.set_xlabel('Frame')
    ax.set_ylabel('Velocity (uu/s)')
    ax.set_title(f'Ball Velocity with Impacts (threshold={threshold})')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    return fig


def plot_time_between_impacts(replays: "List[ReplayData]",
                              threshold: float = 500.0,
                              fps: float = 10.0,
                              bins: int = 50,
                              figsize: Tuple[int, int] = (10, 4)) -> plt.Figure:
    """
    Plot distribution of time between impacts.

    Args:
        replays: List of ReplayData objects
        threshold: Impact detection threshold
        fps: Frames per second (for converting to seconds)
        bins: Number of histogram bins
        figsize: Figure size

    Returns:
        matplotlib Figure
    """
    impact_stats = analyze_impacts(replays, threshold)
    time_between = impact_stats['raw_time_between'] / fps  # Convert to seconds

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    # Histogram
    axes[0].hist(time_between, bins=bins, edgecolor='black', alpha=0.7)
    axes[0].set_xlabel('Time Between Impacts (seconds)')
    axes[0].set_ylabel('Count')
    axes[0].set_title('Distribution of Time Between Impacts')
    axes[0].axvline(np.mean(time_between), color='red', linestyle='--',
                    label=f'Mean: {np.mean(time_between):.2f}s')
    axes[0].legend()

    # Log histogram (to see long tail)
    axes[1].hist(time_between, bins=bins, edgecolor='black', alpha=0.7)
    axes[1].set_xlabel('Time Between Impacts (seconds)')
    axes[1].set_ylabel('Count (log scale)')
    axes[1].set_title('Time Between Impacts (Log Scale)')
    axes[1].set_yscale('log')

    plt.tight_layout()
    return fig
