"""
Parse Result Formatter Module

Formats ParseResult objects (from ReplayParser) into structured data formats.

Key functionality:
    - Validates parsed data quality (frame counts, player counts, NaN/Inf detection)
    - Deduplicates redundant features from parsed arrays
    - Standardizes schema by padding to fixed player count
    - Converts to Pandas DataFrame or Parquet files
    - Extracts and cleans replay metadata
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import json
import numpy as np
import pandas as pd
from impulse.parsing.replay_parser import ParseResult
from impulse.config.pipeline_config import PipelineConfig
from impulse.config.parsing_config import VALID_FEATURE_ADDERS


@dataclass
class FormatResult:
    """Result of formatting parsed data."""
    success: bool
    replay_id: str
    dataframe: Optional[pd.DataFrame]
    metadata: Optional[Dict[str, Any]]
    num_rows: int
    num_columns: int
    num_players: int
    parquet_path: Optional[str] = None
    metadata_path: Optional[str] = None
    parquet_size_bytes: int = 0
    metadata_size_bytes: int = 0
    validation_warnings: List[str] = field(default_factory=list)
    has_nan: bool = False
    has_inf: bool = False
    nan_count: int = 0
    inf_count: int = 0
    error: Optional[str] = None


class ParseResultFormatter:
    """
    Formats ParseResult objects into structured data (DataFrame/Parquet).

    Handles:
    - Pipeline quality validation (frame counts, player counts, NaN/Inf detection)
    - Feature deduplication (removes redundant position/rotation columns)
    - Schema standardization (pads to max player count for fixed schema)
    - DataFrame creation with proper column names
    - Parquet export with metadata
    - Player mapping extraction
    """
    
    def __init__(self, max_players: int = PipelineConfig.SCHEMA_MAX_PLAYERS):
        """
        Initialize the formatter.

        Args:
            max_players: Number of player columns to allocate in Parquet schema
        """
        self.max_players = max_players

    def validate_quality(self,
                        parse_result: ParseResult,
                        config: PipelineConfig = PipelineConfig()) -> Tuple[bool, List[str], Dict[str, Any]]:
        """
        Validate parsed data meets pipeline quality standards.

        Args:
            parse_result: Result from ReplayParser
            config: Pipeline configuration with validation thresholds

        Returns:
            Tuple of (is_valid, warnings, validation_info)
            - is_valid: False only for hard failures (frame/player count out of bounds)
            - warnings: List of warning messages (NaN/Inf detection, etc.)
            - validation_info: Dict with validation statistics
        """
        warnings = []
        validation_info = {
            'has_nan': False,
            'has_inf': False,
            'nan_count': 0,
            'inf_count': 0
        }

        # Frame count validation (hard failure)
        if parse_result.num_frames < config.MIN_FRAMES:
            return False, [f"Too few frames: {parse_result.num_frames} < {config.MIN_FRAMES}"], validation_info

        if parse_result.num_frames > config.MAX_FRAMES:
            return False, [f"Too many frames: {parse_result.num_frames} > {config.MAX_FRAMES}"], validation_info

        # Player count validation (hard failure)
        if parse_result.num_players < config.MIN_PLAYERS:
            return False, [f"Too few players: {parse_result.num_players} < {config.MIN_PLAYERS}"], validation_info

        if parse_result.num_players > config.MAX_PLAYERS:
            return False, [f"Too many players: {parse_result.num_players} > {config.MAX_PLAYERS}"], validation_info

        # NaN detection (warning only)
        if np.isnan(parse_result.array).any():
            nan_count = int(np.isnan(parse_result.array).sum())
            validation_info['has_nan'] = True
            validation_info['nan_count'] = nan_count
            warnings.append(f"Array contains {nan_count} NaN values")

        # Inf detection (warning only)
        if np.isinf(parse_result.array).any():
            inf_count = int(np.isinf(parse_result.array).sum())
            validation_info['has_inf'] = True
            validation_info['inf_count'] = inf_count
            warnings.append(f"Array contains {inf_count} Inf values")

        # Column count validation (warning only)
        if parse_result.global_features and parse_result.player_features:
            expected_cols = self._get_expected_column_count(
                parse_result.global_features,
                parse_result.player_features,
                parse_result.num_players
            )
            if parse_result.num_features != expected_cols:
                warnings.append(
                    f"Column count mismatch: expected {expected_cols}, got {parse_result.num_features}"
                )

        return True, warnings, validation_info

    def _get_expected_column_count(self,
                                   global_features: List[str],
                                   player_features: List[str],
                                   num_players: int) -> int:
        """
        Calculate expected column count before deduplication.

        Args:
            global_features: List of global feature names
            player_features: List of player feature names
            num_players: Number of players

        Returns:
            Expected number of columns in output array
        """
        global_cols = sum(
            len(VALID_FEATURE_ADDERS['global'][feat])
            for feat in global_features
        )

        player_cols = sum(
            len(VALID_FEATURE_ADDERS['player'][feat])
            for feat in player_features
        )

        return global_cols + (player_cols * num_players)

    def format(self, parse_result: ParseResult, config: PipelineConfig = PipelineConfig()) -> FormatResult:
        """
        Format a parse result into a structured DataFrame.

        Args:
            parse_result: Result from ReplayParser
            config: Pipeline configuration for validation and deduplication

        Returns:
            FormatResult with formatted data or error information
        """
        if not parse_result.success:
            return FormatResult(
                success=False,
                replay_id=Path(parse_result.replay_path).stem,
                dataframe=None,
                metadata=None,
                num_rows=-1,
                num_columns=-1,
                num_players=-1,
                error=f"Cannot format failed parse: {parse_result.error}"
            )

        # Validate quality
        is_valid, warnings, validation_info = self.validate_quality(parse_result, config)
        if not is_valid:
            # Hard validation failure (frame count or player count out of bounds)
            return FormatResult(
                success=False,
                replay_id=Path(parse_result.replay_path).stem,
                dataframe=None,
                metadata=None,
                num_rows=parse_result.num_frames,
                num_columns=parse_result.num_features,
                num_players=parse_result.num_players,
                validation_warnings=warnings,
                has_nan=validation_info['has_nan'],
                has_inf=validation_info['has_inf'],
                nan_count=validation_info['nan_count'],
                inf_count=validation_info['inf_count'],
                error=f"Validation failed: {warnings[0] if warnings else 'Unknown error'}"
            )

        try:
            # Extract replay ID
            replay_id = self._extract_replay_id(parse_result.metadata)

            # Deduplicate features (pass feature lists directly)
            deduplicated_array, column_names = self._deduplicate_features(
                parse_result.array,
                parse_result.global_features,
                parse_result.player_features,
                parse_result.num_players,
                config
            )

            # Pad to max players
            padded_array, padded_columns = self._pad_to_max_players(
                deduplicated_array,
                column_names,
                parse_result.num_players
            )

            # Create DataFrame
            df = pd.DataFrame(padded_array, columns=padded_columns)

            # Add frame index column
            df.insert(0, 'frame', range(len(df)))

            # Extract metadata
            clean_metadata = self._extract_metadata(parse_result)

            return FormatResult(
                success=True,
                replay_id=replay_id,
                dataframe=df,
                metadata=clean_metadata,
                num_rows=len(df),
                num_columns=len(df.columns),
                num_players=parse_result.num_players,
                validation_warnings=warnings,  # Include warnings even on success
                has_nan=validation_info['has_nan'],
                has_inf=validation_info['has_inf'],
                nan_count=validation_info['nan_count'],
                inf_count=validation_info['inf_count']
            )

        except Exception as e:
            return FormatResult(
                success=False,
                replay_id=Path(parse_result.replay_path).stem,
                dataframe=None,
                metadata=None,
                num_rows=0,
                num_columns=0,
                num_players=0,
                error=f"Formatting failed: {str(e)}"
            )
    
    def save_to_parquet(self,
                       format_result: FormatResult,
                       output_dir: str,
                       compression: str = PipelineConfig.PARQUET_COMPRESSION) -> FormatResult:
        """
        Save formatted data to Parquet file.
        
        Args:
            format_result: Result from format()
            output_dir: Directory to save files
            compression: Compression algorithm ('snappy', 'gzip', 'zstd')
            
        Returns:
            Updated FormatResult with file paths and sizes
        """
        if not format_result.success:
            return format_result
        
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Save Parquet
            parquet_file = output_path / f"{format_result.replay_id}.parquet"
            format_result.dataframe.to_parquet(
                parquet_file,
                compression=compression,
                index=False
            )
            format_result.parquet_path = str(parquet_file)
            format_result.parquet_size_bytes = parquet_file.stat().st_size
            
            # Save metadata
            metadata_file = output_path / f"{format_result.replay_id}.metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(format_result.metadata, f, indent=2)
            format_result.metadata_path = str(metadata_file)
            format_result.metadata_size_bytes = metadata_file.stat().st_size
            
            return format_result
            
        except Exception as e:
            format_result.success = False
            format_result.error = f"Save failed: {str(e)}"
            return format_result
    
    def _deduplicate_features(self,
                             array: np.ndarray,
                             global_features: List[str],
                             player_features: List[str],
                             num_players: int,
                             config: PipelineConfig = PipelineConfig()) -> Tuple[np.ndarray, List[str]]:
        """
        Deduplicate redundant features from RigidBody and RigidBodyQuaternions.

        Uses PipelineConfig settings to determine which columns to keep:
        - KEEP_QUATERNIONS: Keep quaternion columns from RigidBodyQuaternions
        - KEEP_EULER_ANGLES: Keep Euler angle columns from RigidBody
        - KEEP_VELOCITIES: Keep velocity columns from RigidBody
        - DEDUPLICATE_POSITION: Skip redundant position columns

        Strategy for 'standard' preset (RigidBody + RigidBodyQuaternions):
        - Position (x, y, z): Taken from first occurrence (RigidBody)
        - Euler angles: Kept only if KEEP_EULER_ANGLES=True
        - Quaternions: Kept only if KEEP_QUATERNIONS=True (default)
        - Velocities: Always kept from RigidBody

        Args:
            array: Raw ndarray from parser
            global_features: List of global feature names used in parsing
            player_features: List of player feature names used in parsing
            num_players: Number of players
            config: Pipeline configuration

        Returns:
            Tuple of (deduplicated_array, column_names)
        """
        output_columns = []
        output_data = []
        ndarray_idx = 0

        # Check if we have both RigidBody and RigidBodyQuaternions for globals
        has_ball_rigid_body = 'BallRigidBody' in global_features
        has_ball_quaternions = 'BallRigidBodyQuaternions' in global_features

        # Process global features
        for feature in global_features:
            feature_cols = VALID_FEATURE_ADDERS['global'][feature]

            if feature == 'BallRigidBody':
                # BallRigidBody columns: pos(3), rot(3), lin_vel(3), ang_vel(3) = 12 total
                for i, col in enumerate(feature_cols):
                    # Position: indices 0-2
                    if i < 3:
                        output_columns.append(col)
                        output_data.append(array[:, ndarray_idx])
                        ndarray_idx += 1
                    # Euler angles: indices 3-5
                    elif i < 6:
                        # Keep Euler angles only if configured AND quaternions not preferred
                        if config.KEEP_EULER_ANGLES and not (has_ball_quaternions and config.KEEP_QUATERNIONS):
                            output_columns.append(col)
                            output_data.append(array[:, ndarray_idx])
                        ndarray_idx += 1
                    # Velocities: indices 6-11
                    else:
                        if config.KEEP_VELOCITIES:
                            output_columns.append(col)
                            output_data.append(array[:, ndarray_idx])
                        ndarray_idx += 1

            elif feature == 'BallRigidBodyQuaternions':
                # BallRigidBodyQuaternions columns: pos(3), quat(4) = 7 total
                for i, col in enumerate(feature_cols):
                    # Position: indices 0-2 (skip if deduplicating)
                    if i < 3:
                        if not (has_ball_rigid_body and config.DEDUPLICATE_POSITION):
                            output_columns.append(col)
                            output_data.append(array[:, ndarray_idx])
                        ndarray_idx += 1
                    # Quaternions: indices 3-6
                    else:
                        if config.KEEP_QUATERNIONS:
                            output_columns.append(col)
                            output_data.append(array[:, ndarray_idx])
                        ndarray_idx += 1

            else:
                # Other features: take as-is
                for col in feature_cols:
                    output_columns.append(col)
                    output_data.append(array[:, ndarray_idx])
                    ndarray_idx += 1

        # Check if we have both RigidBody and RigidBodyQuaternions for players
        has_player_rigid_body = 'PlayerRigidBody' in player_features
        has_player_quaternions = 'PlayerRigidBodyQuaternions' in player_features

        # Process player features
        for player_idx in range(num_players):
            for feature in player_features:
                feature_cols = VALID_FEATURE_ADDERS['player'][feature]

                if feature == 'PlayerRigidBody':
                    # Same structure as BallRigidBody
                    for i, col in enumerate(feature_cols):
                        # Position: indices 0-2
                        if i < 3:
                            output_columns.append(f"p{player_idx}_{col}")
                            output_data.append(array[:, ndarray_idx])
                            ndarray_idx += 1
                        # Euler angles: indices 3-5
                        elif i < 6:
                            if config.KEEP_EULER_ANGLES and not (has_player_quaternions and config.KEEP_QUATERNIONS):
                                output_columns.append(f"p{player_idx}_{col}")
                                output_data.append(array[:, ndarray_idx])
                            ndarray_idx += 1
                        # Velocities: indices 6-11
                        else:
                            if config.KEEP_VELOCITIES:
                                output_columns.append(f"p{player_idx}_{col}")
                                output_data.append(array[:, ndarray_idx])
                            ndarray_idx += 1

                elif feature == 'PlayerRigidBodyQuaternions':
                    # Same structure as BallRigidBodyQuaternions
                    for i, col in enumerate(feature_cols):
                        # Position: indices 0-2
                        if i < 3:
                            if not (has_player_rigid_body and config.DEDUPLICATE_POSITION):
                                output_columns.append(f"p{player_idx}_{col}")
                                output_data.append(array[:, ndarray_idx])
                            ndarray_idx += 1
                        # Quaternions: indices 3-6
                        else:
                            if config.KEEP_QUATERNIONS:
                                output_columns.append(f"p{player_idx}_{col}")
                                output_data.append(array[:, ndarray_idx])
                            ndarray_idx += 1

                else:
                    # Other features: take as-is
                    for col in feature_cols:
                        output_columns.append(f"p{player_idx}_{col}")
                        output_data.append(array[:, ndarray_idx])
                        ndarray_idx += 1

        deduplicated_array = np.column_stack(output_data)
        return deduplicated_array, output_columns

    def _pad_to_max_players(self,
                           array: np.ndarray,
                           column_names: List[str],
                           current_players: int) -> Tuple[np.ndarray, List[str]]:
        """
        Pad array to maximum player count with NaN values.
        
        Args:
            array: Deduplicated array
            column_names: Column names
            current_players: Current number of players
            
        Returns:
            Tuple of (padded_array, padded_column_names)
        """
        if current_players >= self.max_players:
            return array, column_names
        
        # Find player feature template (from player 0)
        player_features = [col[3:] for col in column_names if col.startswith('p0_')]
        
        # Add null columns for missing players
        padded_columns = column_names.copy()
        null_arrays = []
        
        for player_idx in range(current_players, self.max_players):
            for feature_name in player_features:
                padded_columns.append(f"p{player_idx}_{feature_name}")
                null_arrays.append(np.full(array.shape[0], np.nan))
        
        if null_arrays:
            null_data = np.column_stack(null_arrays)
            padded_array = np.hstack([array, null_data])
        else:
            padded_array = array
        
        return padded_array, padded_columns
    
    def _extract_replay_id(self, metadata: Dict[str, Any]) -> str:
        """Extract replay ID from metadata."""
        replay_meta = metadata.get('replay_meta', {})
        all_headers = dict(replay_meta.get('all_headers', []))
        return all_headers.get('Id', 'unknown')
    
    def _extract_metadata(self, parse_result: ParseResult) -> Dict[str, Any]:
        """
        Extract clean metadata for storage.
        
        Args:
            parse_result: Parse result with raw metadata
            
        Returns:
            Cleaned metadata dict
        """
        metadata = parse_result.metadata
        replay_meta = metadata.get('replay_meta', {})
        all_headers = dict(replay_meta.get('all_headers', []))
        
        return {
            'source_file': parse_result.replay_path,
            'replay_id': all_headers.get('Id'),
            'replay_name': all_headers.get('ReplayName'),
            'date': all_headers.get('Date'),
            'map': all_headers.get('MapName'),
            'match_type': all_headers.get('MatchType'),
            'team_size': all_headers.get('TeamSize'),
            'num_frames': all_headers.get('NumFrames'),
            'fps': parse_result.fps,
            'duration_seconds': parse_result.duration_seconds,
            'team_0_score': all_headers.get('Team0Score'),
            'team_1_score': all_headers.get('Team1Score'),
            'goals': all_headers.get('Goals', []),
            'highlights': all_headers.get('HighLights', []),
            'game_version': all_headers.get('BuildVersion'),
            'player_mapping': self._create_player_mapping(replay_meta),
            'parsing_info': {
                'num_players': parse_result.num_players,
                'num_frames_parsed': parse_result.num_frames,
                'num_features': parse_result.num_features
            }
        }
    
    def _create_player_mapping(self, replay_meta: Dict[str, Any]) -> Dict[int, Optional[Dict]]:
        """
        Create player slot to player info mapping.
        
        Args:
            replay_meta: Replay metadata dict
            
        Returns:
            Dict mapping slot index to player info
        """
        player_mapping = {}
        
        team_zero = replay_meta.get('team_zero', [])
        team_one = replay_meta.get('team_one', [])
        
        slot_idx = 0
        for player in team_zero:
            player_mapping[slot_idx] = {
                'name': player.get('name'),
                'team': 0,
                'steam_id': player.get('remote_id', {}).get('Steam'),
                'stats': player.get('stats', {})
            }
            slot_idx += 1
        
        for player in team_one:
            player_mapping[slot_idx] = {
                'name': player.get('name'),
                'team': 1,
                'steam_id': player.get('remote_id', {}).get('Steam'),
                'stats': player.get('stats', {})
            }
            slot_idx += 1
        
        # Fill remaining slots with None
        for i in range(slot_idx, self.max_players):
            player_mapping[i] = None
        
        return player_mapping
    
    def __repr__(self) -> str:
        return f"ParseResultFormatter(max_players={self.max_players})"
