"""
Feature configuration for downstream ML consumption.

Defines the physical bounds, feature groups, and presets used for
feature selection and normalization of parsed replay data.

Column naming conventions (from subtr-actor via ParseResultFormatter):
    Ball columns:   "Ball - position x", "Ball - linear velocity y", etc.
    Player columns: "p0_position x", "p1_boost level", etc. (p0-p5 for 3v3)
    Game state:     "frame", "current time", "frame time", "seconds remaining"
"""

from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Physical bounds for normalization
# ---------------------------------------------------------------------------

PHYSICAL_BOUNDS: Dict[str, float] = {
    'position_x': 4096.0,
    'position_y': 5120.0,
    'position_z': 2044.0,
    'ball_linear_velocity': 6000.0,
    'ball_angular_velocity': 6.0,
    'player_linear_velocity': 2300.0,
    'player_angular_velocity': 5.5,
    'boost': 255.0,
}


# ---------------------------------------------------------------------------
# Feature group definitions
# ---------------------------------------------------------------------------

_AXES_XYZ = ['x', 'y', 'z']
_AXES_QUAT = ['x', 'y', 'z', 'w']


def _ball_cols(feature: str, axes: List[str]) -> List[str]:
    return [f'Ball - {feature} {a}' for a in axes]


def _player_cols(feature: str, axes: List[str], num_players: int = 6) -> List[str]:
    return [
        f'p{i}_{feature} {a}'
        for i in range(num_players)
        for a in axes
    ]


def _player_scalar_cols(feature: str, num_players: int = 6) -> List[str]:
    return [f'p{i}_{feature}' for i in range(num_players)]


def get_feature_columns(preset: str = 'physics', num_players: int = 6) -> List[str]:
    """
    Return an ordered list of column names for a feature preset.

    Args:
        preset: One of 'physics', 'minimal', 'full'.
        num_players: Number of players (6 for 3v3).

    Returns:
        Ordered list of column names.

    Raises:
        ValueError: If preset is unknown.
    """
    if preset == 'physics':
        return _physics_columns(num_players)
    elif preset == 'minimal':
        return _minimal_columns(num_players)
    elif preset == 'full':
        return _full_columns(num_players)
    else:
        raise ValueError(
            f"Unknown preset '{preset}'. Available: 'physics', 'minimal', 'full'"
        )


def _physics_columns(num_players: int) -> List[str]:
    """Physics + boost features (drops game state, action flags, demolished)."""
    cols: List[str] = []
    # Ball
    cols += _ball_cols('position', _AXES_XYZ)
    cols += _ball_cols('linear velocity', _AXES_XYZ)
    cols += _ball_cols('angular velocity', _AXES_XYZ)
    cols += _ball_cols('quaternion', _AXES_QUAT)
    # Players
    for i in range(num_players):
        p = f'p{i}_'
        cols += [f'{p}position {a}' for a in _AXES_XYZ]
        cols += [f'{p}linear velocity {a}' for a in _AXES_XYZ]
        cols += [f'{p}angular velocity {a}' for a in _AXES_XYZ]
        cols += [f'{p}quaternion {a}' for a in _AXES_QUAT]
        cols.append(f'{p}boost level')
    return cols


def _minimal_columns(num_players: int) -> List[str]:
    """Ball + player position, velocity, and boost only (no rotation columns)."""
    cols: List[str] = []
    cols += _ball_cols('position', _AXES_XYZ)
    cols += _ball_cols('linear velocity', _AXES_XYZ)
    for i in range(num_players):
        p = f'p{i}_'
        cols += [f'{p}position {a}' for a in _AXES_XYZ]
        cols += [f'{p}linear velocity {a}' for a in _AXES_XYZ]
        cols.append(f'{p}boost level')
    return cols


def _full_columns(num_players: int) -> List[str]:
    """All columns from the standard parse preset."""
    cols: List[str] = []
    # Game state
    cols += ['frame', 'current time', 'frame time', 'seconds remaining']
    # Ball
    cols += _ball_cols('position', _AXES_XYZ)
    cols += _ball_cols('linear velocity', _AXES_XYZ)
    cols += _ball_cols('angular velocity', _AXES_XYZ)
    cols += _ball_cols('quaternion', _AXES_QUAT)
    # Players
    for i in range(num_players):
        p = f'p{i}_'
        cols += [f'{p}position {a}' for a in _AXES_XYZ]
        cols += [f'{p}linear velocity {a}' for a in _AXES_XYZ]
        cols += [f'{p}angular velocity {a}' for a in _AXES_XYZ]
        cols += [f'{p}quaternion {a}' for a in _AXES_QUAT]
        cols.append(f'{p}boost level')
        cols += [f'{p}dodge active', f'{p}jump active', f'{p}double jump active']
        cols.append(f'{p}player demolished by')
    return cols


def get_normalization_divisors(columns: List[str]) -> Dict[str, float]:
    """
    Map each column name to its normalization divisor based on physical bounds.

    Columns not matching any known pattern (e.g., quaternions, game state) are
    omitted from the result — they should be left unchanged.

    Args:
        columns: List of column names from a DataFrame.

    Returns:
        Dict mapping column name -> divisor. Divide column values by the divisor
        to normalize to approximately [-1, 1] or [0, 1].
    """
    divisors: Dict[str, float] = {}

    for col in columns:
        divisor = _classify_divisor(col)
        if divisor is not None:
            divisors[col] = divisor

    return divisors


def _classify_divisor(col: str) -> Optional[float]:
    """Determine the normalization divisor for a single column name."""
    is_ball = col.startswith('Ball - ')

    if 'position x' in col:
        return PHYSICAL_BOUNDS['position_x']
    if 'position y' in col:
        return PHYSICAL_BOUNDS['position_y']
    if 'position z' in col:
        return PHYSICAL_BOUNDS['position_z']
    if 'linear velocity' in col:
        if is_ball:
            return PHYSICAL_BOUNDS['ball_linear_velocity']
        return PHYSICAL_BOUNDS['player_linear_velocity']
    if 'angular velocity' in col:
        if is_ball:
            return PHYSICAL_BOUNDS['ball_angular_velocity']
        return PHYSICAL_BOUNDS['player_angular_velocity']
    if 'boost level' in col:
        return PHYSICAL_BOUNDS['boost']

    # Quaternions, game state, action flags: no normalization
    return None
