"""
Impulse Collection Module - Rocket League Replay Data Collection

This module provides tools for downloading and organizing Rocket League replay files
from Ballchasing.com, with support for multiple storage backends (local, S3) and
database tracking for deduplication and resume capability.

## Quick Start

### Download to Local Storage
```python
from impulse.collection import download_group

result = download_group(
    group_id='rlcs-2024-abc123',
    storage_type='local',
    output_dir='./replays'
)
```

### Download to S3
```python
from impulse.collection import download_group

result = download_group(
    group_id='rlcs-2024-abc123',
    storage_type='s3',
    path_prefix=['replays', 'rlcs', '2024']
)
```

### Advanced Usage with Full Control
```python
from impulse.collection import (
    BallchasingClient,
    ReplayDownloader,
    S3Backend,
    ImpulseDB
)
from impulse.config.collection_config import CollectionConfig

# Configure
config = CollectionConfig.from_env()

# Initialize components
client = BallchasingClient(config)
storage = S3Backend()
db = ImpulseDB()

# Create downloader
downloader = ReplayDownloader(client, storage, db)

# Download
result = downloader.download_group('rlcs-2024-abc123')
```
"""

from impulse.collection.ballchasing_client import BallchasingClient
from impulse.collection.replay_downloader import (
    ReplayDownloader,
    DownloadProgress,
    DownloadResult
)
from impulse.collection.storage import (
    StorageBackend,
    LocalBackend,
    S3Backend
)
from impulse.collection.database import ImpulseDB
from impulse.collection.rate_limiter import RateLimiter
from impulse.collection.rlcs_manager import RLCSManager

from impulse.config.collection_config import CollectionConfig
from typing import Optional, List


def download_group(
    group_id: str,
    storage_type: str = 'local',
    output_dir: Optional[str] = None,
    path_prefix: Optional[List[str]] = None,
    use_database: bool = True,
    database_path: str = "./impulse.db",
    config: Optional[CollectionConfig] = None,
    progress_callback = None
) -> DownloadResult:
    """
    Convenience function to download a Ballchasing group with minimal setup.

    This is the recommended high-level function for most users. It handles all
    the initialization and configuration automatically.

    Args:
        group_id: Ballchasing group ID to download
        storage_type: 'local' or 's3' (default: 'local')
        output_dir: Output directory for local storage (default: './replays')
        path_prefix: Path prefix for storage organization (e.g., ['replays', 'rlcs', '2024'])
        use_database: Enable database tracking for deduplication (default: True)
        database_path: Path to SQLite database (default: './impulse.db')
        config: Optional CollectionConfig (defaults to loading from environment)
        progress_callback: Optional callback function for progress updates

    Returns:
        DownloadResult with statistics

    Raises:
        ValueError: If storage_type is invalid or required config is missing

    Examples:
        Download to local directory:
        >>> result = download_group('rlcs-2024-abc123', storage_type='local')

        Download to S3 with custom path:
        >>> result = download_group(
        ...     'rlcs-2024-abc123',
        ...     storage_type='s3',
        ...     path_prefix=['replays', 'rlcs', '2024']
        ... )
    """
    # Load configuration
    if config is None:
        config = CollectionConfig.from_env()

    if use_database:
        config.database_path = database_path

    # Initialize client
    client = BallchasingClient(config)

    # Initialize storage backend
    if storage_type == 'local':
        if output_dir is None:
            output_dir = './replays/raw'
        storage = LocalBackend(base_dir=output_dir)

    elif storage_type == 's3':
        config.validate_for_s3()
        storage = S3Backend(
            aws_region=config.aws_region,
            s3_bucket_name=config.s3_bucket_name
        )

    else:
        raise ValueError(
            f"Invalid storage_type: {storage_type}. "
            f"Must be 'local' or 's3'"
        )

    # Initialize database
    db = ImpulseDB(config.database_path) if use_database else None

    # Create downloader
    downloader = ReplayDownloader(
        client=client,
        storage=storage,
        db=db,
        progress_callback=progress_callback
    )

    # Download group
    return downloader.download_group(
        group_id=group_id,
        path_prefix=path_prefix
    )


__all__ = [
    # High-level convenience function
    'download_group',

    # Core classes
    'BallchasingClient',
    'ReplayDownloader',
    'ImpulseDB',
    'RateLimiter',
    'RLCSManager',

    # Storage backends
    'StorageBackend',
    'LocalBackend',
    'S3Backend',

    # Data classes
    'DownloadProgress',
    'DownloadResult',

    # Configuration
    'CollectionConfig',
]
