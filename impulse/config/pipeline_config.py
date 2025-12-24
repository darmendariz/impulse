"""
Configures various settings for a replay processing pipeline. Includes settings for specifying data quality and schema standards. 
Purpose: Enforce pipeline quality standards and prepare parsed data for storage. 
"""

from dataclasses import dataclass, field

@dataclass
class PipelineConfig:
    """Configuration for the replay processing pipeline.
    
    Can be instantiated with custom values or use defaults.
    """
    
    # Parquet schema configuration
    SCHEMA_MAX_PLAYERS: int = 8  # Number of player columns to allocate in Parquet schema
    
    # Parquet storage configuration
    PARQUET_COMPRESSION: str = 'snappy'
    S3_PARSED_PREFIX: str = 'replays-parsed'
    S3_RAW_PREFIX: str = 'replays-raw'

    # Data quality validation
    MIN_FRAMES: int = 100
    MAX_FRAMES: int = 100000
    MIN_PLAYERS: int = 2
    MAX_PLAYERS: int = 8  # Maximum players allowed in a replay for validation

    # Feature deduplication strategy for rigid body physics
    DEDUPLICATE_POSITION: bool = True
    KEEP_QUATERNIONS: bool = True
    KEEP_EULER_ANGLES: bool = False  
    KEEP_VELOCITIES: bool = True

    # Retry logic
    MAX_RETRIES: int = 3
    RETRY_DELAY_SECONDS: int = 60
