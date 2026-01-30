"""
Impulse Parsing Module - Rocket League Replay Parsing

This module provides tools for parsing Rocket League replay files into
structured data formats (NumPy arrays, DataFrames, Parquet files) using
the subtr-actor library.

## Quick Start

### Parse a single replay
```python
from impulse.parsing import ReplayParser, ParseResultFormatter

parser = ReplayParser.from_preset('standard', fps=10.0)
result = parser.parse_file('./replay.replay')

formatter = ParseResultFormatter()
format_result = formatter.format(result)
format_result = formatter.save_to_parquet(format_result, './output')
```

### Parse with database tracking
```python
from impulse.parsing import ParsingPipeline, ReplayParser
from impulse.collection.database import ImpulseDB

parser = ReplayParser.from_preset('standard', fps=10.0)
db = ImpulseDB()
pipeline = ParsingPipeline(parser, db)

# Parse a single replay
result = pipeline.parse_replay('./replay.replay', './output')

# Parse all unparsed replays
result = pipeline.parse_unparsed('./raw_replays', './output')
```
"""

from impulse.parsing.replay_parser import ReplayParser, ParseResult
from impulse.parsing.parse_result_formatter import ParseResultFormatter, FormatResult
from impulse.parsing.parsing_pipeline import (
    ParsingPipeline,
    PipelineProgress,
    PipelineResult
)

__all__ = [
    # Core parser
    'ReplayParser',
    'ParseResult',

    # Formatter
    'ParseResultFormatter',
    'FormatResult',

    # Pipeline orchestrator
    'ParsingPipeline',
    'PipelineProgress',
    'PipelineResult',
]
