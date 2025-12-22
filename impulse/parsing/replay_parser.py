"""
Core parsing module. Wraps the subtr-actor library and handles the complexity of replay parsing logic.
"""
from typing import List

class ReplayParser:
    def __init__(self, global_features: List[str] = None, player_features: List[str] = None, fps: float = 10.0):
        self.global_features = global_features
        self.player_features = player_features
        self.fps = fps

    
