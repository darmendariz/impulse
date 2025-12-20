"""
Valid feature adders for subtr_actor.get_ndarray_with_info_from_replay_filepath() method. Subtr_actor will parse these features from the replay file and add them as columns to the returned ndarray.

Taken from https://docs.rs/subtr-actor/latest/subtr_actor/collector/ndarray/index.html. 

valid_feature_adders has the following structure:
{
    "global": {
        feature_adder_name: [list of columns returned in the ndarray]
        },
    "player": {
        feature_adder_name: [list of columns returned in the ndarray]
        }
}

valid_feature_adders['global'].keys() returns a list of valid global feature adders.
valid_feature_adders['player'].keys() returns a list of valid player feature adders.
"""

valid_feature_adders = {
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
        "InterpolatedBallRigidBodyNoVelocities": [
            'Ball - position x', 
            'Ball - position y', 
            'Ball - position z', 
            'Ball - rotation x', 
            'Ball - rotation y', 
            'Ball - rotation z', 
            'Ball - rotation w'
            ],
        "SecondsRemaining": [
            'seconds remaining'
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
