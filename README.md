# Impulse

An end-to-end data pipeline and ML toolkit for Rocket League replay analysis. Impulse handles the full lifecycle—from collecting raw `.replay` files at scale, to parsing them into structured time-series data, to preprocessing and serving training-ready datasets to PyTorch.

## Overview

Rocket League replays contain rich, high-frequency time-series data: 3D positions, velocities, rotations, and game state for every player and the ball at every frame. Impulse provides the infrastructure to turn this unstructured binary data into clean, normalized, ML-ready datasets.

The library is built around a modular pipeline architecture:

```
Ballchasing API → raw `.replay`s → Storage (Local / S3) → Parsing → Parquet + JSON
                                                                     ↓
                                                                Preprocessing
                                                                    ↓
                                                            PyTorch DataLoader
```

Each stage is independently usable, composable, and designed to scale to tens of thousands of replays with cloud storage integration, database-tracked state, and failure recovery.

## Architecture

### Module Structure

| Module | Purpose |
|--------|---------|
| `impulse.collection` | Download raw replay files from Ballchasing.com with rate limiting, deduplication, and S3/local storage options|
| `impulse.parsing` | Parse binary `.replay` files into DataFrames/Parquet via [subtr-actor](https://github.com/rlrml/subtr-actor) (Rust) |
| `impulse.preprocessing` | Composable feature selection, physical normalization, and gameplay segmentation |
| `impulse.training` | Segment-aware windowed PyTorch `Dataset` with on-the-fly preprocessing |
| `impulse.config` | Centralized configuration: feature presets, physical bounds, pipeline settings |
| `impulse.visualization` | Interactive Jupyter replay viewer (3D field + 2D feature plots) |

### Key Design Decisions

- **Streaming architecture**: Replays stream from API to storage via in-memory buffers—no intermediate disk I/O
- **Database-tracked state**: SQLite tracks every download, parse, and segment boundary for resume capability upon interruptions, deduplication, and easier preprocessing
- **Physical normalization**: Features are normalized by known Rocket League physical bounds (arena dimensions, max velocities) rather than data-derived statistics, making normalization deterministic and invertible
- **Segment-aware training**: Gameplay is automatically segmented at kickoff boundaries so training windows never span discontinuities induced by kickoff/goal resets 
- **Lazy, memory-efficient access**: Replay data is loaded on-demand; only the active working set lives in memory
- **PyTorch as optional dependency**: The training module is import-guarded; the core pipeline works without it

## Quick Start

### Collect

```python
from impulse.collection import download_group

result = download_group(
    group_id='ballchasing-group-id',
    storage_type='local',
    output_dir='./replays'
)
```

### Parse

```python
from impulse.parsing import ReplayParser

parser = ReplayParser.from_preset('standard', fps=30.0)
result = parser.parse_file('./replays/my_replay.replay')

array = result.array          # NumPy array, shape: (frames, features)
metadata = result.metadata    # Match info, teams, duration, etc.
```

### Preprocess and Train

```python
from impulse import ReplayDataset, PreprocessingPipeline, FeatureSelector, PhysicalNormalizer
from impulse import split_replay_ids
from impulse.training import ReplayTrainingDataset
from torch.utils.data import DataLoader

# Load parsed replay data
dataset = ReplayDataset(db_path='./impulse.db', data_dir='./replays/parsed')

# Build preprocessing pipeline
pipeline = PreprocessingPipeline([
    FeatureSelector.from_preset('physics'),
    PhysicalNormalizer()
])

# Split and create training dataset
train_ids, val_ids, test_ids = split_replay_ids(dataset.replay_ids)

train_dataset = ReplayTrainingDataset(
    dataset=dataset,
    pipeline=pipeline,
    replay_ids=train_ids,
    window_size=128,
    stride=64,
)

loader = DataLoader(train_dataset, batch_size=256, num_workers=4, shuffle=True)
```

## Pipeline Details

### Collection

Downloads replays from the [Ballchasing.com](https://ballchasing.com/) API (~150 million replays available) with:

- **Rate limiting**: Automatic enforcement of API limits (1 req/sec, 200 req/hour free tier or higher with donor tier) with pause/resume
- **Recursive group traversal**: Walks nested tournament group hierarchies to download all child replays
- **Storage backends**: Local filesystem or AWS S3 (streaming upload, no intermediate disk)
- **State tracking**: SQLite database registers every download—skip duplicates, resume interrupted jobs, retry failures
- **S3 integration**: Auto-detects EC2 IAM roles vs local credentials; includes database backup to S3

### Parsing

Extracts frame-level time-series data from binary `.replay` files using [subtr-actor](https://github.com/rlrml/subtr-actor) (Rust library with Python bindings):

- **Configurable feature extraction**: Choose from presets (`minimal`, `standard`, `all`) or specify exact features (ball physics, player rigid body, boost, jump states, etc.)
- **Output formats**: NumPy arrays, pandas DataFrames, Parquet files with Snappy compression
- **Data quality validation**: Frame/player count bounds, NaN/Inf detection, feature deduplication
- **Automatic segmentation**: Computes and stores gameplay segment boundaries at parse time (see Preprocessing)
- **Batch processing**: `ParsingPipeline` orchestrates bulk parsing with database tracking of successes and failures

### Preprocessing

Composable transforms that prepare parsed replay data for ML:

- **`FeatureSelector`**: Select columns by preset (`physics`, `minimal`, `full`) or explicit list. Presets are defined in a centralized feature config with known column orderings per player count.
- **`PhysicalNormalizer`**: Deterministic normalization using Rocket League's known physical bounds (arena: 8192x10240x2044 units, max ball velocity: 6000 uu/s, etc.). Supports `.inverse()` for converting model outputs back to physical units.
- **`PreprocessingPipeline`**: Chains any sequence of `(DataFrame) -> DataFrame` callables. Supports forward and inverse transformation.
- **Gameplay segmentation**: Detects kickoff-setup frames (ball at origin with zero velocity) and identifies continuous gameplay segments between them. Boundaries are stored as JSON in the database—no data copying.

### Training

PyTorch integration for model training:

- **`ReplayTrainingDataset`**: A `torch.utils.data.Dataset` that generates fixed-size windows from gameplay segments across a set of replays
- **Segment-aware windowing**: Windows are built from pre-computed segment boundaries (read from DB at construction—no Parquet I/O). Windows never cross segment boundaries, preventing models from learning kickoff-reset artifacts.
- **On-the-fly preprocessing**: The `PreprocessingPipeline` is applied when windows are accessed, not upfront
- **LRU caching**: Loaded and preprocessed replays are cached in memory; evicted when the cache is full
- **Flexible modes**: Fixed-length windows with configurable stride, or variable-length whole segments

### Data Access

`ReplayDataset` provides a clean interface for loading parsed replay data:

- **Database mode** (recommended): Uses the `parsed_replays` table for fast lookup without directory scanning
- **Directory mode**: Falls back to scanning a directory for `.parquet` files
- **S3 support**: Download-and-cache or direct streaming via `s3://` URIs
- **Access patterns**: Single replay, random sample, lazy iteration, batched iteration, or ID-based subset
- **Train/val/test splitting**: `split_replay_ids()` provides deterministic, reproducible splits

## Installation

**Prerequisites**: Python 3.12+

```bash
git clone https://github.com/darmendariz/impulse.git
cd impulse
uv sync                        # core dependencies
uv sync --extra training       # + PyTorch for training module
```

**Environment** (`.env` in project root):
```bash
BALLCHASING_API_KEY=your_key   # required for downloading replays
AWS_REGION=us-east-1           # optional, for S3 storage
S3_BUCKET_NAME=your-bucket     # optional, for S3 storage
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Replay parsing | [subtr-actor](https://github.com/rlrml/subtr-actor) / [boxcars](https://github.com/nickbabcock/boxcars) (Rust) |
| Data processing | NumPy, pandas, PyArrow |
| Storage | AWS S3 (boto3), local filesystem |
| Database | SQLite |
| ML framework | PyTorch (optional) |
| Visualization | Matplotlib, ipywidgets |
| Package management | [uv](https://github.com/astral-sh/uv) |

## Project Status

This is an active personal project. The data pipeline (collection through preprocessing) is production-tested on the full RLCS 2024 dataset: 7,300+ replays, 72M+ frames, ~21 GB of parsed time-series data.

The pipeline is actively used for data science work in a companion [analysis repo](https://github.com/darmendariz/impulse-analysis), which includes:

- **Exploratory data analysis**: Dataset-level statistics from the database, single-replay deep dives, feature distributions and autocorrelation across 200-replay samples, and segmentation analysis
- **Baseline modeling**: Next-frame physics prediction — naive (predict no change) and linear regression baselines, with per-feature MSE analysis showing 59–75% improvement on ball position prediction over the trivial baseline
- **Experiment tracking**: Runs logged to Weights & Biases; notebooks rendered as a Quarto site

Next steps: scaling to the full dataset with PyTorch (MLP, LSTM, Transformer architectures) for multi-step sequence prediction.

## Related Libraries

| Library | Language | Purpose |
|---------|----------|---------|
| [boxcars](https://github.com/nickbabcock/boxcars) | Rust | Low-level replay parsing—decodes the binary `.replay` format |
| [subtr-actor](https://github.com/rlrml/subtr-actor) | Rust | Extracts frame-by-frame actor data into NumPy arrays (built on boxcars) |
