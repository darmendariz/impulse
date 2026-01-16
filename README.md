# Impulse

A Python library for collecting, parsing, and processing raw Rocket League `.replay` files into structured data for general analysis or machine learning tasks.

## Overview

Impulse provides a complete and customizable pipeline for downloading Rocket League `.replay` files from [Ballchasing.com](https://ballchasing.com/) and extracting ML-ready data from them at frame-level precision. It handles the infrastructure concerns so you can easily create clean datasets from Ballchasing's repository of 140+ million replays in a few lines of code. 

The library is designed to scale. With it, you can download and process tens of thousands of replays with AWS storage/compute service integration, database registration, automatic progress tracking for big downloading/processing jobs, failure recovery, and configurable data quality validation.

## Feature overview

**Collection**
- Ballchasing.com API client with automatic rate limiting and group tree parsing in order to download all replays in all child groups of a parent group
- Supports multiple storage backends (local filesystem or AWS S3)
- Run long downloading/processing jobs on an AWS EC2 instance to comply with Ballchasing rate limits and stream files directly to S3 bucket
- SQLite tracking for deduplication and download registration
- Handles interruptions gracefully—resume downloads where you left off

**Parsing**
- High-performance parsing via [subtr-actor](https://github.com/rlrml/subtr-actor) (Rust-based parsing library)
- Configurable feature extraction: frame-by-frame ball physics, player positions/velocities, boost, etc.
- Multiple output formats: NumPy arrays, pandas DataFrames, Parquet files
- Built-in data quality validation (frame/player bounds, NaN/Inf detection)
- Feature deduplication and database schema standardization

**Configuration**
- `CollectionConfig`: API keys, storage settings, rate limits
- `ParsingConfig`: Feature extraction presets, custom feature selection, FPS sampling settings
- `PipelineConfig`: Data quality thresholds, feature deduplication strategy, database schema settings

## Installation

**Prerequisites**
- Python 3.12+
- Ballchasing API key - [get one here](https://ballchasing.com/upload) (optional, if downloading replays)
- AWS credentials (optional, for S3 storage)

**Install**
```bash
git clone https://github.com/darmendariz/impulse.git
cd impulse
uv sync
```

**Environment Setup**

Create a `.env` file in the project root:
```bash
BALLCHASING_API_KEY=your_api_key_here

# Optional (required for S3 storage)
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your-bucket-name
```

## Quick Start

### Download replays to local storage

```python
from impulse.collection import download_group

result = download_group(
    group_id='insert-ballchasing-group-id',
    storage_type='local',
    output_dir='./replays'
)

print(f"Downloaded {result.successful} replays")
```

### Parse a replay file

```python
from impulse.parsing import ReplayParser

parser = ReplayParser.from_preset('standard', fps=10.0)     # standard feature preset
result = parser.parse_file('./replays/my_replay.replay')

if result.success:
    array = result.array  # NumPy array, shape: (frames, features)
    metadata = result.metadata

    print(f"Shape: {array.shape}")
    print(f"Duration: {result.duration_seconds:.1f}s")
```

### Custom feature extraction with DataFrame/Parquet output

```python
from impulse.parsing import ReplayParser
from impulse.parsing.parse_result_formatter import ParseResultFormatter

# Select specific features for your use case
parser = ReplayParser(
    global_features=['BallRigidBody', 'SecondsRemaining'],
    player_features=['PlayerRigidBody', 'PlayerBoost', 'PlayerAnyJump'],
    fps=30.0
)

result = parser.parse_file('./replays/my_replay.replay')

if result.success:
    # Format to DataFrame with validation and deduplication
    formatter = ParseResultFormatter()
    formatted = formatter.format(result)

    df = formatted.dataframe
    print(f"DataFrame shape: {df.shape}")

    # Save to Parquet
    formatter.save_to_parquet(formatted, './output')
```

## Configuration

### Collection Configuration

```python
from impulse.collection import CollectionConfig

# Load from environment
config = CollectionConfig.from_env()

# Or configure programmatically
config = CollectionConfig(
    ballchasing_api_key='your_key',
    aws_region='us-east-1',
    s3_bucket_name='your-bucket',
    database_path='./my_project.db',
    rate_limit_per_hour=200  # Ballchasing free tier
)
```

### Parsing Configuration

```python
from impulse.config.parsing_config import ParsingConfig, FEATURE_PRESETS

# Use a preset
preset = ParsingConfig.get_preset('standard')  # or 'minimal', 'all'

# Validate custom features before parsing
ParsingConfig.validate_features(
    global_features=['BallRigidBody', 'CurrentTime'],
    player_features=['PlayerRigidBody', 'PlayerBoost']
)
```

### Pipeline Configuration

```python
from impulse.config.pipeline_config import PipelineConfig

config = PipelineConfig(
    # Quality validation
    MIN_FRAMES=100,
    MAX_FRAMES=100000,
    MIN_PLAYERS=2,
    MAX_PLAYERS=8,

    # Feature deduplication
    DEDUPLICATE_POSITION=True,
    KEEP_QUATERNIONS=True,
    KEEP_VELOCITIES=True,

    # Output settings
    PARQUET_COMPRESSION='snappy'
)
```

## Related Libraries

| Library | Language | Purpose |
|---------|----------|---------|
| [boxcars](https://github.com/nickbabcock/boxcars) | Rust | Low-level replay parsing—decodes the binary `.replay` format |
| [subtr-actor](https://github.com/rlrml/subtr-actor) | Rust | Extracts frame-by-frame actor data into NumPy arrays (built on boxcars) |
