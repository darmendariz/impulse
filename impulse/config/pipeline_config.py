from dataclasses import dataclass, field

@dataclass
class PipelineConfig:
    """Configuration for the replay processing pipeline.
    
    Can be instantiated with custom values or use defaults.
    """
    
    # Parquet schema configuration
    max_players: int = 8
    
    # Parquet storage configuration
    parquet_compression: str = 'snappy'
    s3_parsed_prefix: str = 'replays-parsed'
    s3_raw_prefix: str = 'replays-raw'
    
    # Data quality validation
    min_frames: int = 100
    max_frames: int = 100000
    min_players: int = 2
    max_players_actual: int = 8
    
    # Feature deduplication strategy for rigid body physics
    deduplicate_position: bool = True
    keep_quaternions: bool = True
    keep_euler_angles: bool = False  
    keep_velocities: bool = True

    # Retry logic
    max_retries: int = 3
    retry_delay_seconds: int = 60
