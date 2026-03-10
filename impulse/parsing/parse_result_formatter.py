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

import dataclasses
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
    skipped: bool = False
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
    
    def __init__(self):
        pass

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
            # Use filename stem as replay_id for consistent naming
            replay_id = Path(parse_result.replay_path).stem

            # Deduplicate features (pass feature lists directly)
            deduplicated_array, column_names = self._deduplicate_features(
                parse_result.array,
                parse_result.global_features,
                parse_result.player_features,
                parse_result.num_players,
                config
            )

            # Create DataFrame
            df = pd.DataFrame(deduplicated_array, columns=column_names)

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

            # Save metadata
            metadata_file = output_path / f"{format_result.replay_id}.metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(format_result.metadata, f, indent=2)

            return dataclasses.replace(
                format_result,
                parquet_path=str(parquet_file),
                parquet_size_bytes=parquet_file.stat().st_size,
                metadata_path=str(metadata_file),
                metadata_size_bytes=metadata_file.stat().st_size,
            )

        except Exception as e:
            return dataclasses.replace(
                format_result,
                success=False,
                error=f"Save failed: {str(e)}"
            )
    
    def _classify_column(self, raw_col: str) -> str:
        """
        Classify a raw column name (from VALID_FEATURE_ADDERS) by semantic type.

        Strips known entity prefixes ('Ball - ', 'i ') before matching, so the
        same classification applies to global and player features alike.

        Returns one of: 'position', 'euler_rotation', 'quaternion', 'velocity', 'other'
        """
        name = raw_col
        for prefix in ('Ball - ', 'i '):
            if name.startswith(prefix):
                name = name[len(prefix):]
                break

        if 'quaternion' in name:
            return 'quaternion'
        if 'velocity' in name:
            return 'velocity'
        if name in ('position x', 'position y', 'position z'):
            return 'position'
        if name in ('rotation x', 'rotation y', 'rotation z'):
            return 'euler_rotation'
        return 'other'

    def _deduplicate_features(self,
                             array: np.ndarray,
                             global_features: List[str],
                             player_features: List[str],
                             num_players: int,
                             config: PipelineConfig = PipelineConfig()) -> Tuple[np.ndarray, List[str]]:
        """
        Filter and deduplicate columns from the parsed ndarray.

        For each feature's columns (looked up from VALID_FEATURE_ADDERS):
        - Skips columns whose full name has already been output (handles position
          overlap between e.g. BallRigidBody and BallRigidBodyQuaternions)
        - Applies config-based type filters (KEEP_EULER_ANGLES, KEEP_QUATERNIONS,
          KEEP_VELOCITIES) using semantic column classification
        - Always advances the ndarray index, whether or not the column is included

        Works for any combination of feature adders, not just the standard preset.

        Args:
            array: Raw ndarray from parser
            global_features: List of global feature names used in parsing
            player_features: List of player feature names used in parsing
            num_players: Number of players
            config: Pipeline configuration

        Returns:
            Tuple of (filtered_array, column_names)
        """
        output_columns = []
        output_data = []
        seen_columns: set = set()
        ndarray_idx = 0

        def process_feature(raw_cols: List[str], prefix: str = '') -> None:
            nonlocal ndarray_idx
            for raw_col in raw_cols:
                full_col = f"{prefix}{raw_col}" if prefix else raw_col
                col_type = self._classify_column(raw_col)
                include = (
                    full_col not in seen_columns
                    and (col_type != 'velocity' or config.KEEP_VELOCITIES)
                    and (col_type != 'euler_rotation' or config.KEEP_EULER_ANGLES)
                    and (col_type != 'quaternion' or config.KEEP_QUATERNIONS)
                )
                if include:
                    output_columns.append(full_col)
                    output_data.append(array[:, ndarray_idx])
                    seen_columns.add(full_col)
                ndarray_idx += 1

        for feature in global_features:
            process_feature(VALID_FEATURE_ADDERS['global'][feature])

        for player_idx in range(num_players):
            for feature in player_features:
                process_feature(VALID_FEATURE_ADDERS['player'][feature], prefix=f'p{player_idx}_')

        return np.column_stack(output_data), output_columns

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
            'replay_id': Path(parse_result.replay_path).stem,
            'source_file': parse_result.replay_path,
            'ballchasing_id': all_headers.get('Id'),
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
        
        return player_mapping
    
    def __repr__(self) -> str:
        return "ParseResultFormatter()"
