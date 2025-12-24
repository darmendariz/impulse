"""
Configuration file for parsing using subtr_actor. 
Purpose: handle extraction settings for parsing.  
"""

from typing import Dict, List

# Valid feature adders from subtr-actor documentation for 
#       subtr_actor.get_ndarray_with_info_from_replay_filepath() 
# method.
# Taken from https://docs.rs/subtr-actor/latest/subtr_actor/collector/ndarray/index.html. 
# 
# {
#     "global": {
#         feature_adder_name: [list of columns returned in the ndarray]
#         },
#     "player": {
#         feature_adder_name: [list of columns returned in the ndarray]
#         }
# }
VALID_FEATURE_ADDERS = {
    "global": {
        "BallRigidBody": [
            'Ball - position x', 
            'Ball - position y', 
            'Ball - position z', 
            'Ball - rotation x', 
            'Ball - rotation y', 
            'Ball - rotation z', 
            'Ball - linear velocity x', 
            'Ball - linear velocity y', 
            'Ball - linear velocity z', 
            'Ball - angular velocity x', 
            'Ball - angular velocity y', 
            'Ball - angular velocity z'
            ],
        "BallRigidBodyNoVelocities": [
            'Ball - position x', 
            'Ball - position y', 
            'Ball - position z', 
            'Ball - rotation x', 
            'Ball - rotation y', 
            'Ball - rotation z', 
            'Ball - rotation w'
            ],
        "BallRigidBodyQuaternions": [
            'Ball - position x', 
            'Ball - position y', 
            'Ball - position z', 
            'Ball - quaternion x', 
            'Ball - quaternion y', 
            'Ball - quaternion z', 
            'Ball - quaternion w'
            ],
        "CurrentTime": [
            'current time'
            ],
        "FrameTime": [
            'frame time'
            ],
        "SecondsRemaining": [
            'seconds remaining'
            ],
        "InterpolatedBallRigidBodyNoVelocities": [
            'Ball - position x', 
            'Ball - position y', 
            'Ball - position z', 
            'Ball - rotation x', 
            'Ball - rotation y', 
            'Ball - rotation z', 
            'Ball - rotation w'
            ],
        "VelocityAddedBallRigidBodyNoVelocities": [
            'Ball - position x', 
            'Ball - position y', 
            'Ball - position z', 
            'Ball - rotation x', 
            'Ball - rotation y', 
            'Ball - rotation z', 
            'Ball - rotation w'
            ],
    },
    "player": {
        "PlayerAnyJump": [
            'any_jump_active'
            ],
        "PlayerBoost": [
            'boost level'
            ],
        "PlayerDemolishedBy": [
            'player demolished by'
            ],
        "PlayerJump": [
            'dodge active', 
            'jump active', 
            'double jump active'
            ],
        "PlayerRigidBody": [
            'position x', 
            'position y', 
            'position z', 
            'rotation x', 
            'rotation y', 
            'rotation z', 
            'linear velocity x', 
            'linear velocity y', 
            'linear velocity z', 
            'angular velocity x', 
            'angular velocity y', 
            'angular velocity z'
            ],
        "PlayerRigidBodyNoVelocities": [
            'position x', 
            'position y', 
            'position z', 
            'rotation x', 
            'rotation y', 
            'rotation z', 
            'rotation w'
            ],
        "PlayerRigidBodyQuaternions": [
            'position x', 
            'position y', 
            'position z', 
            'quaternion x', 
            'quaternion y', 
            'quaternion z', 
            'quaternion w'
            ],
        "VelocityAddedPlayerRigidBodyNoVelocities": [
            'position x', 
            'position y', 
            'position z', 
            'rotation x', 
            'rotation y', 
            'rotation z', 
            'rotation w'
            ],
        "InterpolatedPlayerRigidBodyNoVelocities": [
            'i position x', 
            'i position y', 
            'i position z', 
            'i rotation x', 
            'i rotation y', 
            'i rotation z', 
            'i rotation w'
            ],
    }
}

# Feature presets for common parsing use cases.
FEATURE_PRESETS = {
    # Standard: Basic game and player state, ball/player positions, rotations, velocities.
    # ____RigidBody and ____RigidBodyQuaternions contain redundant position and rotation data and should be deduplicated, but RigidBody is needed to extract velocity data.
    'standard': {
        'global' : [
            'CurrentTime', 
            'FrameTime', 
            'SecondsRemaining', 
            'BallRigidBody',                 
            'BallRigidBodyQuaternions'
        ],
        'player' : [
            'PlayerRigidBody',
            'PlayerRigidBodyQuaternions',
            'PlayerBoost',
            'PlayerJump',
            'PlayerDemolishedBy'
        ]
    },
    # Minimal: minimal game and player state, and ball/player rigid body physics
    'minimal' : {
        'global' : [
            'CurrentTime', 
            'BallRigidBody'
        ],
        'player' : [
            'PlayerRigidBody',
            'PlayerBoost',
            'PlayerAnyJump',
            'PlayerDemolishedBy'
        ]
    },
    # All: All available feature adders from subtr_actor
    'all' : {
        'global': 
            VALID_FEATURE_ADDERS['global'].keys(),
        'player': 
            VALID_FEATURE_ADDERS['player'].keys()
    }
}


class ParsingConfig:
    """Configuration and utilities for replay parsing using subtr_actor."""
    
    # Parsing settings and validation constants
    DEFAULT_FEATURE_PRESET = 'standard'
    DEFAULT_FPS = 10.0          # Default frame sampling rate for parsing replays
    MIN_FPS = 1.0
    MAX_FPS = 120.0             # TODO: determine default fps at which replay files are created by the game client 
                                # (I think it's 30 but not sure). Once determined, MAX_FPS should be set to that value.
    
    
    @classmethod
    def get_preset(cls, preset_name: str) -> Dict[str, List[str]]:
        """Get feature preset by name.
        
        Args:
            preset_name: Name of preset ('minimal', 'standard', 'comprehensive')
            
        Returns:
            Dict with 'global' and 'player' feature lists
            
        Raises:
            ValueError: If preset name not found
        """
        if preset_name not in FEATURE_PRESETS:
            raise ValueError(
                f"Unknown preset '{preset_name}'. "
                f"Available: {list(FEATURE_PRESETS.keys())}"
            )
        return FEATURE_PRESETS[preset_name]
    
    @classmethod
    def validate_features(cls, global_features: List[str], player_features: List[str]) -> None:
        """Validate feature names before parsing.
        
        Args:
            global_features: List of global feature names
            player_features: List of player feature names
            
        Raises:
            ValueError: If any feature name is invalid
        """
        valid_global = set(VALID_FEATURE_ADDERS['global'].keys())
        valid_player = set(VALID_FEATURE_ADDERS['player'].keys())
        
        for feature in global_features:
            if feature not in valid_global:
                raise ValueError(
                    f"Invalid global feature '{feature}'. "
                    f"Valid options: {sorted(valid_global)}"
                )
        
        for feature in player_features:
            if feature not in valid_player:
                raise ValueError(
                    f"Invalid player feature '{feature}'. "
                    f"Valid options: {sorted(valid_player)}"
                )
    
    @classmethod
    def get_column_names(cls, feature_adder_name: str, feature_adder_type: str) -> List[str]:
        """Get returned column names for a feature adder.
        
        Args:
            feature_adder_name: Name of the feature adder
            feature_adder_type: 'global' or 'player'
            
        Returns:
            List of column names returned by subtr_actor.get_ndarray_with_info_from_replay_filepath() for that feature adder
            
        Raises:
            ValueError: If feature adder not found
        """
        if feature_adder_type not in ['global', 'player']:
            raise ValueError(f"feature_adder_type must be 'global' or 'player', got '{feature_adder_type}'")

        valid_adders = VALID_FEATURE_ADDERS[feature_adder_type]
        if feature_adder_name not in valid_adders:
            raise ValueError(
                f"Feature adder '{feature_adder_name}' not found in {feature_adder_type} features. "
                f"Valid options: {sorted(valid_adders.keys())}"
            )

        return valid_adders[feature_adder_name]
