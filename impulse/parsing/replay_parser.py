"""
Replay Parser Module

Wraps the subtr-actor library to parse Rocket League replay files.
Handles the complexity of calling subtr-actor and extracting structured data from replays.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Optional
import numpy as np
import subtr_actor
from impulse.config.parsing_config import ParsingConfig, FEATURE_PRESETS


@dataclass
class ParseResult:
    """Result of parsing a replay file."""
    success: bool
    replay_path: str
    metadata: Optional[Dict[str, Any]]
    array: Optional[np.ndarray]
    num_frames: int
    num_features: int
    num_players: int
    fps: float
    error: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate replay duration in seconds."""
        if self.num_frames > 0 and self.fps > 0:
            return self.num_frames / self.fps
        return 0.0


class ReplayParser:
    """
    Parser for Rocket League replay files using subtr-actor.

    This class wraps the subtr-actor library and provides a clean interface for parsing replay files with specified features at a given frame sampling rate.
    """
    
    def __init__(self, global_features: List[str], player_features: List[str], fps: float = ParsingConfig.DEFAULT_FPS):
        """
        Initialize the replay parser.
        
        Args:
            global_features: List of global feature adders (e.g., 'BallRigidBody')
            player_features: List of player feature adders (e.g., 'PlayerBoost')
            fps: Frames per second to sample at
            
        Raises:
            ValueError: If feature adders are invalid or FPS is out of range
        """
        # Validate features
        ParsingConfig.validate_features(global_features, player_features)
        
        # Validate FPS
        if not ParsingConfig.MIN_FPS <= fps <= ParsingConfig.MAX_FPS:
            raise ValueError(
                f"FPS must be between {ParsingConfig.MIN_FPS} and {ParsingConfig.MAX_FPS}, "
                f"got {fps}"
            )
        
        self.global_features = global_features
        self.player_features = player_features
        self.fps = fps
    
    @classmethod
    def from_preset(cls, preset_name: str, fps: float = ParsingConfig.DEFAULT_FPS):
        """
        Create parser from a feature preset.
        
        Args:
            preset_name: Name of preset ('minimal', 'standard', 'comprehensive')
            fps: Frames per second to sample at
            
        Returns:
            ReplayParser instance
            
        Example:
            parser = ReplayParser.from_preset('standard', fps=30.0)
        """
        preset = ParsingConfig.get_preset(preset_name)
        return cls(
            global_features=preset['global'],
            player_features=preset['player'],
            fps=fps
        )
    
    def parse_file(self, replay_path: str) -> ParseResult:
        """
        Parse a replay file from disk.
        
        Args:
            replay_path: Path to .replay file
            
        Returns:
            ParseResult with parsed data or error information
        """
        replay_path = str(Path(replay_path).resolve())
        
        # Check file exists
        if not Path(replay_path).exists():
            return ParseResult(
                success=False,
                replay_path=replay_path,
                metadata=None,
                array=None,
                num_frames=-1,
                num_features=-1,
                num_players=-1,
                fps=self.fps,
                error=f"File not found: {replay_path}"
            )
        
        try:
            # Call subtr-actor
            metadata, array = subtr_actor.get_ndarray_with_info_from_replay_filepath(
                replay_path,
                self.global_features,
                self.player_features,
                self.fps
            )
            
            # Extract player count from metadata
            num_players = self._count_players(metadata)

            # Validate result
            validation_error = self._validate_parse_result(array)
            if validation_error:
                return ParseResult(
                    success=False,
                    replay_path=replay_path,
                    metadata=metadata,
                    array=array,
                    num_frames=array.shape[0] if array is not None else 0,
                    num_features=array.shape[1] if array is not None else 0,
                    num_players=num_players,
                    fps=self.fps,
                    error=validation_error
                )
            
            return ParseResult(
                success=True,
                replay_path=replay_path,
                metadata=metadata,
                array=array,
                num_frames=array.shape[0],
                num_features=array.shape[1],
                num_players=num_players,
                fps=self.fps
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                replay_path=replay_path,
                metadata=None,
                array=None,
                num_frames=-1,
                num_features=-1,
                num_players=-1,
                fps=self.fps,
                error=f"Parsing failed: {str(e)}"
            )
    
    def _count_players(self, metadata: Dict[str, Any]) -> int:
        """
        Extract player count from metadata.
        
        Args:
            metadata: Metadata dict from subtr-actor
            
        Returns:
            Number of players in the replay
        """
        replay_meta = metadata.get('replay_meta', {})
        team_zero = replay_meta.get('team_zero', [])
        team_one = replay_meta.get('team_one', [])
        return len(team_zero) + len(team_one)
    
    def _validate_parse_result(self, array: np.ndarray) -> Optional[str]:
        """
        Validate that subtr-actor parsing succeeded.

        Args:
            array: Parsed feature array

        Returns:
            Error message if parsing failed, None otherwise
        """
        if array is None:
            return "Parsing returned None"

        if array.ndim != 2:
            return f"Expected 2D array, got {array.ndim}D"

        if array.shape[0] == 0 or array.shape[1] == 0:
            return "Array has zero frames or features"

        return None
