"""
Utility functions for the collection module.

Contains helper functions for path sanitization, tree operations, and other
common tasks used across the collection module.
"""

import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional


def sanitize_path_component(name: str) -> str:
    """
    Sanitize a string for safe use in file paths.

    Removes or replaces invalid characters that might cause issues
    in filesystem paths or S3 keys.

    Args:
        name: String to sanitize (e.g., group name, replay title)

    Returns:
        Sanitized string safe for use in paths

    Example:
        >>> sanitize_path_component("RLCS 2024: Worlds")
        'RLCS 2024 Worlds'
        >>> sanitize_path_component("Team/Name<Bad>")
        'Team_Name_Bad_'
    """
    # Characters that are invalid in file paths or problematic in S3
    invalid_chars = '<>:"/\\|?*'

    for char in invalid_chars:
        name = name.replace(char, '_')

    # Remove leading/trailing dots and spaces
    name = name.strip('. ')

    return name


def flatten_group_tree(tree: Dict, path: List[str] = None) -> List[Tuple[Dict, List[str]]]:
    """
    Flatten a hierarchical group tree into a list of (replay, path) tuples.

    Takes a nested group structure and returns a flat list where each replay
    is paired with its full hierarchical path.

    Args:
        tree: Hierarchical tree structure with 'name', 'replays', and 'children'
        path: Current path (used for recursion, should be None on initial call)

    Returns:
        List of (replay_dict, path_components) tuples

    Example:
        >>> tree = {
        ...     'name': 'RLCS 2024',
        ...     'replays': [],
        ...     'children': [{
        ...         'name': 'Worlds',
        ...         'replays': [{'id': 'abc123', 'title': 'Grand Finals'}],
        ...         'children': []
        ...     }]
        ... }
        >>> flatten_group_tree(tree)
        [({'id': 'abc123', ...}, ['RLCS 2024', 'Worlds'])]
    """
    if path is None:
        path = []

    result = []

    # Add replays from this node with their full path
    for replay in tree.get('replays', []):
        result.append((replay, path + [tree['name']]))

    # Recurse into children
    for child in tree.get('children', []):
        result.extend(flatten_group_tree(child, path + [tree['name']]))

    return result


def build_path_components(group_path: List[str], root_name: str,
                         include_root: bool = True) -> List[str]:
    """
    Build sanitized path components from a group hierarchy path.

    Args:
        group_path: List of group names from the hierarchy
        root_name: Name of the root group
        include_root: Whether to include root in the path

    Returns:
        List of sanitized path components

    Example:
        >>> build_path_components(['RLCS 2024', 'Worlds', 'Day 1'], 'RLCS 2024')
        ['RLCS 2024', 'Worlds', 'Day 1']
        >>> build_path_components(['RLCS 2024', 'Worlds'], 'RLCS 2024', include_root=False)
        ['Worlds']
    """
    # Sanitize all components
    sanitized = [sanitize_path_component(p) for p in group_path]

    if not include_root and sanitized and sanitized[0] == sanitize_path_component(root_name):
        # Remove root from path
        sanitized = sanitized[1:]

    return sanitized


def format_bytes(size_bytes: int) -> str:
    """
    Format byte size into human-readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 GB", "234.2 MB")

    Example:
        >>> format_bytes(1500000000)
        '1.40 GB'
        >>> format_bytes(5000000)
        '4.77 MB'
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / (1024**2):.2f} MB"
    else:
        return f"{size_bytes / (1024**3):.2f} GB"


def extract_replay_metadata(ballchasing_replay: Dict) -> Dict:
    """
    Extract relevant metadata from a Ballchasing replay response.

    Normalizes the Ballchasing API response format into a consistent
    metadata dictionary for storage.

    Args:
        ballchasing_replay: Replay dict from Ballchasing API

    Returns:
        Normalized metadata dict

    Example:
        >>> bc_replay = {
        ...     'id': 'abc123',
        ...     'title': 'Grand Finals',
        ...     'blue': {'name': 'Team A'},
        ...     'orange': {'name': 'Team B'},
        ...     'date': '2024-12-01T10:00:00'
        ... }
        >>> extract_replay_metadata(bc_replay)
        {'replay_id': 'abc123', 'title': 'Grand Finals', ...}
    """
    blue = ballchasing_replay.get('blue', {})
    orange = ballchasing_replay.get('orange', {})

    return {
        'replay_id': ballchasing_replay.get('id'),
        'title': ballchasing_replay.get('replay_title', ballchasing_replay.get('title', 'Unknown')),
        'blue_team': blue.get('name', 'Unknown'),
        'orange_team': orange.get('name', 'Unknown'),
        'date': ballchasing_replay.get('date', 'Unknown'),
        'source': 'ballchasing'
    }


def get_tree_cache_path(group_id: str, cache_dir: Optional[Path] = None) -> Path:
    """
    Get the cache file path for a group tree.

    Args:
        group_id: Ballchasing group ID
        cache_dir: Optional directory for cache files (defaults to ./replays/raw/cache)

    Returns:
        Path to the cache file
    """
    if cache_dir is None:
        cache_dir = Path("./replays/raw/cache")

    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"group_tree_{group_id}.json"


def save_group_tree(tree: Dict, group_id: str, cache_dir: Optional[Path] = None) -> Path:
    """
    Save a group tree to a JSON cache file.

    Args:
        tree: Group tree structure from build_group_tree()
        group_id: Ballchasing group ID
        cache_dir: Optional directory for cache files

    Returns:
        Path to the saved cache file

    Example:
        >>> tree = client.build_group_tree('rlcs-2024-abc123')
        >>> cache_path = save_group_tree(tree, 'rlcs-2024-abc123')
        >>> print(f"Tree cached at: {cache_path}")
    """
    cache_path = get_tree_cache_path(group_id, cache_dir)

    with open(cache_path, 'w') as f:
        json.dump(tree, f, indent=2)

    return cache_path


def load_group_tree(group_id: str, cache_dir: Optional[Path] = None) -> Optional[Dict]:
    """
    Load a group tree from a JSON cache file.

    Args:
        group_id: Ballchasing group ID
        cache_dir: Optional directory for cache files

    Returns:
        Cached tree structure, or None if cache doesn't exist

    Example:
        >>> tree = load_group_tree('rlcs-2024-abc123')
        >>> if tree:
        ...     replay_list = flatten_group_tree(tree)
    """
    cache_path = get_tree_cache_path(group_id, cache_dir)

    if not cache_path.exists():
        return None

    with open(cache_path, 'r') as f:
        return json.load(f)


def delete_group_tree_cache(group_id: str, cache_dir: Optional[Path] = None) -> bool:
    """
    Delete a cached group tree file.

    Args:
        group_id: Ballchasing group ID
        cache_dir: Optional directory for cache files

    Returns:
        True if file was deleted, False if it didn't exist
    """
    cache_path = get_tree_cache_path(group_id, cache_dir)

    if cache_path.exists():
        cache_path.unlink()
        return True

    return False
