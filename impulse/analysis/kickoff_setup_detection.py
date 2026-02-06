"""This module contains functions for detecting kickoff setups in the ReplayData.frames array of a parsed replay.

Since the ball sits motionless at the center of the field during the kickoff setup, detection of kickoff setup frames is based on detecting continuous ranges of frames where the ball's position and velocity in the x and y directions are all zero.

The functions in this module can be used to identify these ranges of frames in a replay and return them as a list of tuples containing the start and end frame indices of each range.

Note: One might notice that `Ball - position z` is never equal to `0.0`, but is typically `92.75`(for kickoff frames) and occasionally is a little higher; moreover, for the rows where `Ball - position z > 92.75`, we have that `Ball - linear velocity z` is nonzero. The z-coordinate of `92.75` is explained by the radius of the ball and the fact that its coordinates are the coordinates for its center. The nonzero values of the z-component of linear velocity and the values of the ball's z-coordinates greater than its radius are explained by the fact when the game resets to the kickoff setup configuration, it places the ball (and each of the players' cars) ever so slightly above the ground so that they have a subtle drop-in effect."""

import pandas as pd

BALL_POS_X_Y_COLS = ['Ball - position x', 'Ball - position y']
BALL_LINVEL_X_Y_COLS = ['Ball - linear velocity x', 'Ball - linear velocity y']
BALL_POS_VEL_Z_COLS = ['Ball - position z', 'Ball - linear velocity z']
BALL_POS_VEL_X_Y_COLS = BALL_POS_X_Y_COLS + BALL_LINVEL_X_Y_COLS
BALL_POS_VEL_COLS = BALL_POS_VEL_X_Y_COLS + BALL_POS_VEL_Z_COLS

def kickoff_setup_frames(frames: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame containing the frames of the kickoff setup."""
    kickoff_setup_frames = frames[BALL_POS_VEL_COLS]
    boolean_mask = kickoff_setup_frames[BALL_POS_VEL_X_Y_COLS] == 0.0
    rows_where_true = boolean_mask.all(axis=1)
    kickoff_setup_frames = kickoff_setup_frames.loc[kickoff_setup_frames.index[rows_where_true]]

    return kickoff_setup_frames

def continuous_frame_ranges(kickoff_setup_frames: pd.DataFrame) -> list[tuple[int, int]]:
    """Returns a list of tuples containing the start and end frame numbers of continuous frame ranges in the kickoff setup dataframe."""
    cts_frame_ranges = []
    
    i = 0
    for index in kickoff_setup_frames.index:
        if i == 0:
            start = index
        elif index != kickoff_setup_frames.index[i-1] + 1:
            cts_frame_ranges.append((start, int(kickoff_setup_frames.index[i-1])))
            start = index
        i+=1
    cts_frame_ranges.append((start, int(kickoff_setup_frames.index[i-1])))

    return cts_frame_ranges
    
