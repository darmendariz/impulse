"""
Prototype script for parsing Rocket League replay files using subtr-actor.

This script demonstrates:
1. Loading a replay file from disk
2. Parsing it with subtr-actor into NumPy arrays
3. Understanding the ndarray structure and feature columns
4. Extracting metadata about the replay
5. Basic data inspection and validation

The subtr-actor library converts replay files into NumPy arrays where each row
represents a frame and each column represents a feature (ball position, player
position, boost levels, etc.). This format is ideal for machine learning.

Usage:
    python prototype_parse_replay.py path/to/replay.replay
    python prototype_parse_replay.py path/to/replay.replay --fps 30
    python prototype_parse_replay.py path/to/replay.replay --show-available-features
"""

import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import numpy as np
import subtr_actor


def get_available_features() -> Tuple[list, list]:
    """
    Get lists of available global and player features.
    
    This queries the subtr-actor library to understand what features can be extracted.
    
    Returns:
        Tuple of (global_features, player_features) as lists of strings
    """
    try:
        # The get_column_headers function returns available feature names
        headers_info = subtr_actor.get_column_headers()
        
        # Based on the docs, these correspond to feature adders that can be used
        # Common global features include: ball position, velocity, rotation, game time
        # Common player features include: player position, velocity, boost, jump state
        
        return headers_info
    except Exception as e:
        print(f"Warning: Could not retrieve feature headers: {e}")
        return ([], [])


def parse_replay_to_ndarray(replay_path: str, 
                            global_features: Optional[list] = None,
                            player_features: Optional[list] = None,
                            fps: float = 10.0) -> Tuple[Dict[str, Any], np.ndarray]:
    """
    Parse replay file into NumPy array format.
    
    This is the primary parsing function that converts the replay into
    a structured numerical array suitable for machine learning.
    
    Args:
        replay_path: Path to the .replay file
        global_features: List of global feature adders (e.g., ball data)
        player_features: List of player feature adders (e.g., player positions)
        fps: Frames per second to sample at (default: 10.0)
        
    Returns:
        Tuple of (metadata_dict, features_ndarray)
    """
    print(f"\nParsing replay at {fps} FPS...")
    
    try:
        # Call subtr_actor's main parsing function
        result = subtr_actor.get_ndarray_with_info_from_replay_filepath(
            replay_path,
            global_features,
            player_features,
            fps
        )
        
        # Output is a tuple: (metadata, ndarray)
        metadata, ndarray = result
        
        print("✓ Parsing successful")
        print(f"  Array shape: {ndarray.shape}")
        print(f"  Array dtype: {ndarray.dtype}")
        
        return metadata, ndarray
        
    except Exception as e:
        print(f"✗ Parsing failed: {e}")
        raise


def inspect_metadata(metadata: Dict[str, Any]) -> None:
    """
    Display metadata information about the replay.
    
    Args:
        metadata: Metadata dictionary returned from parsing
    """
    print("\n" + "="*60)
    print("REPLAY METADATA")
    print("="*60)
    
    print(f"\nMetadata keys available: {list(metadata.keys())}")
    print("\nMetadata contents:")
    
    for key, value in metadata.items():
        if isinstance(value, (list, dict)) and len(str(value)) > 100:
            print(f"  {key}: {type(value).__name__} (length: {len(value)})")
        else:
            print(f"  {key}: {value}")


def analyze_ndarray_structure(ndarray: np.ndarray, 
                              metadata: Dict[str, Any],
                              fps: float) -> None:
    """
    Analyze and display information about the ndarray structure.
    
    Args:
        ndarray: The features array from parsing
        metadata: Metadata dictionary
        fps: Frames per second used in parsing
    """
    print("\n" + "="*60)
    print("NDARRAY STRUCTURE ANALYSIS")
    print("="*60)
    
    num_frames, num_features = ndarray.shape
    
    print(f"\nArray Dimensions:")
    print(f"  Frames (rows): {num_frames}")
    print(f"  Features (columns): {num_features}")
    print(f"  Data type: {ndarray.dtype}")
    print(f"  Memory usage: {ndarray.nbytes / 1024:.2f} KB")
    
    # Calculate duration
    duration_seconds = num_frames / fps
    print(f"\nTemporal Information:")
    print(f"  Sampling rate: {fps} FPS")
    print(f"  Duration: {duration_seconds:.2f} seconds ({duration_seconds/60:.2f} minutes)")
    
    # Check for missing data
    num_nan = np.isnan(ndarray).sum()
    num_inf = np.isinf(ndarray).sum()
    print(f"\nData Quality:")
    print(f"  NaN values: {num_nan}")
    print(f"  Inf values: {num_inf}")
    
    if num_nan > 0 or num_inf > 0:
        print("  ⚠ Warning: Array contains NaN or Inf values")


def display_sample_frames(ndarray: np.ndarray, 
                         num_samples: int = 5,
                         feature_names: Optional[list] = None) -> None:
    """
    Display sample frames from the ndarray.
    
    Args:
        ndarray: The features array
        num_samples: Number of frames to display
        feature_names: Optional list of feature names for columns
    """
    print("\n" + "="*60)
    print(f"SAMPLE FRAMES (first {num_samples} frames)")
    print("="*60)
    
    num_frames = min(num_samples, ndarray.shape[0])
    
    for i in range(num_frames):
        frame = ndarray[i]
        print(f"\nFrame {i}:")
        
        if feature_names and len(feature_names) == len(frame):
            # Display with feature names
            for feat_name, value in zip(feature_names[:10], frame[:10]):  # Show first 10 features
                print(f"  {feat_name}: {value:.4f}")
            if len(frame) > 10:
                print(f"  ... ({len(frame) - 10} more features)")
        else:
            # Display first 10 values without names
            print(f"  First 10 values: {frame[:10]}")
            if len(frame) > 10:
                print(f"  ... ({len(frame) - 10} more features)")


def save_sample_data(ndarray: np.ndarray, 
                    metadata: Dict[str, Any],
                    output_path: str,
                    num_sample_frames: int = 100) -> None:
    """
    Save sample data to disk for inspection.
    
    Args:
        ndarray: The features array
        metadata: Metadata dictionary
        output_path: Base path for output files
        num_sample_frames: Number of frames to save
    """
    print("\n" + "="*60)
    print("SAVING SAMPLE DATA")
    print("="*60)
    
    base_path = Path(output_path)
    
    # Save metadata as JSON
    metadata_file = base_path.with_suffix('.metadata.json')
    with open(metadata_file, 'w') as f:
        # Convert numpy types to Python types for JSON serialization
        def convert_numpy(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj
        
        json_metadata = {k: convert_numpy(v) for k, v in metadata.items()}
        json.dump(json_metadata, f, indent=2)
    
    print(f"✓ Metadata saved: {metadata_file}")
    
    # Save sample frames as NPY file
    sample_frames = ndarray[:num_sample_frames]
    npy_file = base_path.with_suffix('.sample.npy')
    np.save(npy_file, sample_frames)
    
    print(f"✓ Sample array saved: {npy_file}")
    print(f"  Sample shape: {sample_frames.shape}")
    print(f"  File size: {npy_file.stat().st_size / 1024:.2f} KB")
    
    # Save full array statistics
    stats_file = base_path.with_suffix('.stats.json')
    stats = {
        'shape': ndarray.shape,
        'dtype': str(ndarray.dtype),
        'memory_kb': float(ndarray.nbytes / 1024),
        'has_nan': bool(np.isnan(ndarray).any()),
        'has_inf': bool(np.isinf(ndarray).any()),
    }
    
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    print(f"✓ Statistics saved: {stats_file}")


def show_available_features_list():
    """Display available features that can be extracted."""
    print("\n" + "="*60)
    print("AVAILABLE FEATURES")
    print("="*60)
    
    print("\nQuerying available features from subtr-actor...")
    
    try:
        features_info = get_available_features()
        
        if features_info:
            print("\nAvailable features:")
            print(features_info)
        else:
            print("\nCould not retrieve feature list automatically.")
            print("\nBased on the documentation, common features include:")
            print("\nGlobal Features (Ball/Game State):")
            print("  - BallRigidBody: Ball position, velocity, rotation")
            print("  - CurrentTime: Game time")
            print("  ...")
            
            print("\nPlayer Features:")
            print("  - PlayerRigidBody: Player position, velocity, rotation")
            print("  - PlayerBoost: Boost amount (0-100)")
            print("  - PlayerAnyJump: Jump button state")
            print("  ...")
            
            print("\nFor full list, see:")
            print("https://docs.rs/subtr-actor/latest/subtr_actor/collector/ndarray/index.html")
    
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main function to run the prototype parsing workflow."""
    
    parser = argparse.ArgumentParser(
        description="Parse Rocket League replays into NumPy arrays",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('replay_path', nargs='?', help='Path to .replay file')
    parser.add_argument('--fps', type=float, default=10.0, 
                       help='Frames per second for sampling (default: 10.0)')
    parser.add_argument('--show-available-features', action='store_true',
                       help='Show available features and exit')
    parser.add_argument('--global-features', nargs='*',
                       help='Global features to extract (e.g., BallRigidBody CurrentTime)')
    parser.add_argument('--player-features', nargs='*',
                       help='Player features to extract (e.g., PlayerRigidBody PlayerBoost)')
    
    args = parser.parse_args()
    
    print("="*60)
    print("SUBTR-ACTOR PROTOTYPE PARSER (NumPy Array Mode)")
    print("="*60)
    
    # Handle --show-available-features flag
    if args.show_available_features:
        show_available_features_list()
        return
    
    # Check if replay path provided
    if not args.replay_path:
        print("\nError: No replay file specified")
        parser.print_help()
        sys.exit(1)
    
    replay_path = args.replay_path
    
    try:
        # Verify file exists
        path = Path(replay_path)
        if not path.exists():
            raise FileNotFoundError(f"Replay file not found: {replay_path}")
        
        print(f"\nReplay file: {path}")
        print(f"File size: {path.stat().st_size / 1024:.2f} KB")
        print(f"Sampling rate: {args.fps} FPS")
        
        # Parse the replay
        # Note: If global_features and player_features are None, the library
        # will use default features
        metadata, ndarray = parse_replay_to_ndarray(
            replay_path,
            global_features=args.global_features,
            player_features=args.player_features,
            fps=args.fps
        )
        
        # Analyze the results
        inspect_metadata(metadata)
        analyze_ndarray_structure(ndarray, metadata, args.fps)
        display_sample_frames(ndarray, num_samples=5)
        
        # Save sample data
        output_base = path.stem + "_parsed"
        save_sample_data(ndarray, metadata, output_base, num_sample_frames=100)
        
        print("\n" + "="*60)
        print("PARSING COMPLETE")
        print("="*60)
        print("\nFiles created:")
        print(f"  - {output_base}.metadata.json  (replay metadata)")
        print(f"  - {output_base}.sample.npy     (first 100 frames)")
        
        print("\nNext Steps:")
        print("1. Examine the metadata JSON to understand replay information")
        print("2. Load the .npy file in Python to inspect frame data:")
        print(f"   >>> import numpy as np")
        print(f"   >>> data = np.load('{output_base}.sample.npy')")
        print("3. Experiment with different --fps values (10, 30, 60)")
        print("4. Try specifying custom features with --global-features and --player-features")
        print("5. Use --show-available-features to see what can be extracted")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
