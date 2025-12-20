"""
Valid feature adders for subtr_actor.get_ndarray_with_info_from_replay_filepath() method. Subtr_actor will parse these features from the replay file and add them as columns to the returned ndarray.

Taken from https://docs.rs/subtr-actor/latest/subtr_actor/collector/ndarray/index.html. 
"""

valid_feature_adders = {
    "global": {
        "BallRigidBody": [],
        "BallRigidBodyNoVelocities": [],
        "BallRigidBodyQuaternions": [],
        "CurrentTime": [],
        "FrameTime": [],
        "InterpolatedBallRigidBodyNoVelocities": [],
        "NoVelocities": [],
        "SecondsRemaining": [],
        "VelocityAddedBallRigidBodyNoVelocities": []
    },
    "player": {
        "InterpolatedPlayerRigidBody": [],
        "PlayerAnyJump": [],
        "PlayerBoost": [],
        "PlayerDemolishedBy": [],
        "PlayerJump": [],
        "PlayerRigidBody": [],
        "PlayerRigidBodyNoVelocities": [],
        "PlayerRigidBodyQuaternions": [],
        "VelocityAddedPlayerRigidBody": [],
    }
}
