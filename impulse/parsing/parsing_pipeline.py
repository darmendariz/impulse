"""
Replay Parsing Pipeline

Orchestrates the parsing workflow: raw replay -> parsed data -> database tracking.
Coordinates ReplayParser and ParseResultFormatter with database registration.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Callable

from impulse.parsing.replay_parser import ReplayParser, ParseResult
from impulse.parsing.parse_result_formatter import ParseResultFormatter, FormatResult
from impulse.collection.database import ImpulseDB
from impulse.config.parsing_config import ParsingConfig
from impulse.config.pipeline_config import PipelineConfig

logger = logging.getLogger('impulse.parsing')


@dataclass
class PipelineProgress:
    """Progress information for parsing operations."""
    current: int
    total: int
    replay_id: str
    status: str  # 'parsing', 'formatting', 'saving', 'complete', 'skipped', 'failed'
    message: str
    output_path: Optional[str] = None
    error: Optional[str] = None


@dataclass
class PipelineResult:
    """Result of a parsing pipeline run."""
    total_replays: int
    successful: int
    skipped: int
    failed: int
    total_frames: int
    total_bytes: int
    output_paths: List[str]
    failed_replays: List[Dict]  # List of {replay_id, error} dicts


class ParsingPipeline:
    """
    High-level replay parsing orchestrator.

    Coordinates parsing from raw replays to formatted output, with
    database tracking and progress reporting.

    Example:
        >>> from impulse.parsing import ParsingPipeline, ReplayParser
        >>> from impulse.collection.database import ImpulseDB
        >>>
        >>> parser = ReplayParser.from_preset('standard', fps=10.0)
        >>> db = ImpulseDB()
        >>> pipeline = ParsingPipeline(parser, db)
        >>>
        >>> # Parse a single replay
        >>> result = pipeline.parse_replay('./replay.replay', './output')
        >>>
        >>> # Parse multiple replays
        >>> result = pipeline.parse_replays(replay_paths, './output')
        >>>
        >>> # Parse all unparsed replays from database
        >>> result = pipeline.parse_unparsed('./raw_replays', './output')
    """

    def __init__(
        self,
        parser: ReplayParser,
        db: Optional[ImpulseDB] = None,
        formatter: Optional[ParseResultFormatter] = None,
        progress_callback: Optional[Callable[[PipelineProgress], None]] = None
    ):
        """
        Initialize the parsing pipeline.

        Args:
            parser: ReplayParser instance configured with features and FPS
            db: Optional database for tracking (enables deduplication and registration)
            formatter: Optional ParseResultFormatter (uses default if not provided)
            progress_callback: Optional callback for progress updates
        """
        self.parser = parser
        self.db = db
        self.formatter = formatter or ParseResultFormatter()
        self.progress_callback = progress_callback or self._default_progress_callback

    def _default_progress_callback(self, progress: PipelineProgress):
        """Default progress callback that logs to console."""
        indent = "  "
        print(f"[{progress.current}/{progress.total}] {progress.replay_id}")
        print(f"{indent}{progress.message}")

        if progress.output_path:
            print(f"{indent}Output: {progress.output_path}")

        if progress.error:
            print(f"{indent}Error: {progress.error}")

    def parse_replay(
        self,
        replay_path: str,
        output_dir: str,
        compression: str = PipelineConfig.PARQUET_COMPRESSION
    ) -> FormatResult:
        """
        Parse a single replay file and save to output directory.

        Args:
            replay_path: Path to .replay file
            output_dir: Directory to save output files
            compression: Parquet compression algorithm

        Returns:
            FormatResult with parsing outcome
        """
        replay_path = Path(replay_path)
        replay_id = replay_path.stem

        # Check if already parsed
        if self.db and self.db.is_replay_parsed(replay_id):
            logger.info(f"Skipping {replay_id}: already parsed")
            return FormatResult(
                success=True,
                replay_id=replay_id,
                dataframe=None,
                metadata=None,
                num_rows=0,
                num_columns=0,
                num_players=0,
                error="Already parsed (skipped)"
            )

        # Parse
        parse_result = self.parser.parse_file(str(replay_path))

        if not parse_result.success:
            if self.db:
                self.db.mark_parse_failed(replay_id, replay_id, parse_result.error)
            return FormatResult(
                success=False,
                replay_id=replay_id,
                dataframe=None,
                metadata=None,
                num_rows=0,
                num_columns=0,
                num_players=0,
                error=parse_result.error
            )

        # Format
        format_result = self.formatter.format(parse_result)

        if not format_result.success:
            if self.db:
                self.db.mark_parse_failed(replay_id, replay_id, format_result.error)
            return format_result

        # Save to parquet
        format_result = self.formatter.save_to_parquet(format_result, output_dir, compression)

        if not format_result.success:
            if self.db:
                self.db.mark_parse_failed(replay_id, replay_id, format_result.error)
            return format_result

        # Register in database (use raw_replay_id as primary key for consistent tracking)
        if self.db:
            metadata_json = json.dumps(format_result.metadata) if format_result.metadata else None
            self.db.add_parsed_replay(
                replay_id=replay_id,
                raw_replay_id=replay_id,
                output_path=format_result.parquet_path,
                output_format='parquet',
                fps=self.parser.fps,
                frame_count=format_result.num_rows,
                feature_count=format_result.num_columns,
                file_size_bytes=format_result.parquet_size_bytes,
                metadata=metadata_json
            )

        return format_result

    def parse_replays(
        self,
        replay_paths: List[str],
        output_dir: str,
        compression: str = PipelineConfig.PARQUET_COMPRESSION
    ) -> PipelineResult:
        """
        Parse multiple replay files.

        Args:
            replay_paths: List of paths to .replay files
            output_dir: Directory to save output files
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics
        """
        total = len(replay_paths)
        successful = 0
        skipped = 0
        failed = 0
        total_frames = 0
        total_bytes = 0
        output_paths = []
        failed_replays = []

        print()
        print("=" * 60)
        print("PARSING REPLAYS")
        print("=" * 60)
        print()

        for i, replay_path in enumerate(replay_paths, 1):
            replay_path = Path(replay_path)
            replay_id = replay_path.stem

            # Check if already parsed
            if self.db and self.db.is_replay_parsed(replay_id):
                self.progress_callback(PipelineProgress(
                    current=i,
                    total=total,
                    replay_id=replay_id,
                    status='skipped',
                    message='Already parsed (database check)'
                ))
                skipped += 1
                print()
                continue

            try:
                # Parse
                self.progress_callback(PipelineProgress(
                    current=i,
                    total=total,
                    replay_id=replay_id,
                    status='parsing',
                    message='Parsing replay...'
                ))

                parse_result = self.parser.parse_file(str(replay_path))

                if not parse_result.success:
                    raise Exception(parse_result.error)

                # Format
                self.progress_callback(PipelineProgress(
                    current=i,
                    total=total,
                    replay_id=replay_id,
                    status='formatting',
                    message='Formatting data...'
                ))

                format_result = self.formatter.format(parse_result)

                if not format_result.success:
                    raise Exception(format_result.error)

                # Save
                self.progress_callback(PipelineProgress(
                    current=i,
                    total=total,
                    replay_id=replay_id,
                    status='saving',
                    message='Saving to parquet...'
                ))

                format_result = self.formatter.save_to_parquet(
                    format_result, output_dir, compression
                )

                if not format_result.success:
                    raise Exception(format_result.error)

                # Register in database (use raw_replay_id as primary key for consistent tracking)
                if self.db:
                    metadata_json = json.dumps(format_result.metadata) if format_result.metadata else None
                    self.db.add_parsed_replay(
                        replay_id=replay_id,
                        raw_replay_id=replay_id,
                        output_path=format_result.parquet_path,
                        output_format='parquet',
                        fps=self.parser.fps,
                        frame_count=format_result.num_rows,
                        feature_count=format_result.num_columns,
                        file_size_bytes=format_result.parquet_size_bytes,
                        metadata=metadata_json
                    )

                successful += 1
                total_frames += format_result.num_rows
                total_bytes += format_result.parquet_size_bytes
                output_paths.append(format_result.parquet_path)

                self.progress_callback(PipelineProgress(
                    current=i,
                    total=total,
                    replay_id=replay_id,
                    status='complete',
                    message=f'Complete ({format_result.num_rows} frames)',
                    output_path=format_result.parquet_path
                ))
                print()

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to parse {replay_id}: {error_msg}")

                self.progress_callback(PipelineProgress(
                    current=i,
                    total=total,
                    replay_id=replay_id,
                    status='failed',
                    message='Failed',
                    error=error_msg
                ))

                if self.db:
                    self.db.mark_parse_failed(replay_id, replay_id, error_msg)

                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': error_msg})
                print()

        # Summary
        print()
        print("=" * 60)
        print("PARSING SUMMARY")
        print("=" * 60)
        print(f"Total replays: {total}")
        print(f"Successfully parsed: {successful}")
        print(f"Skipped (already parsed): {skipped}")
        print(f"Failed: {failed}")
        print(f"Total frames: {total_frames:,}")
        print(f"Total output size: {total_bytes / (1024**2):.2f} MB")

        if self.db:
            print()
            print("DATABASE STATISTICS")
            print("-" * 60)
            stats = self.db.get_parse_stats()
            for key, value in stats.items():
                print(f"{key}: {value}")

        return PipelineResult(
            total_replays=total,
            successful=successful,
            skipped=skipped,
            failed=failed,
            total_frames=total_frames,
            total_bytes=total_bytes,
            output_paths=output_paths,
            failed_replays=failed_replays
        )

    def parse_unparsed(
        self,
        raw_replays_dir: str,
        output_dir: str,
        limit: Optional[int] = None,
        compression: str = PipelineConfig.PARQUET_COMPRESSION
    ) -> PipelineResult:
        """
        Parse all downloaded replays that haven't been parsed yet.

        Queries the database for unparsed replays and processes them.

        Args:
            raw_replays_dir: Directory containing raw .replay files
            output_dir: Directory to save output files
            limit: Maximum number of replays to parse (None for all)
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics

        Raises:
            ValueError: If database is not configured
        """
        if not self.db:
            raise ValueError("Database required for parse_unparsed(). "
                           "Initialize ParsingPipeline with a database.")

        # Get unparsed replays from database
        unparsed = self.db.get_unparsed_replays(limit=limit)

        if not unparsed:
            print("No unparsed replays found.")
            return PipelineResult(
                total_replays=0,
                successful=0,
                skipped=0,
                failed=0,
                total_frames=0,
                total_bytes=0,
                output_paths=[],
                failed_replays=[]
            )

        print(f"Found {len(unparsed)} unparsed replays")

        # Build file paths
        raw_dir = Path(raw_replays_dir)
        replay_paths = []

        for replay_info in unparsed:
            replay_id = replay_info['replay_id']
            # Try to find the replay file
            # First check storage_key if available
            storage_key = replay_info.get('storage_key')
            if storage_key:
                # storage_key might be a full path or relative path
                replay_path = raw_dir / storage_key
                if not replay_path.exists():
                    # Try just the filename
                    replay_path = raw_dir / f"{replay_id}.replay"
            else:
                replay_path = raw_dir / f"{replay_id}.replay"

            # Search recursively if not found
            if not replay_path.exists():
                matches = list(raw_dir.rglob(f"{replay_id}.replay"))
                if matches:
                    replay_path = matches[0]

            if replay_path.exists():
                replay_paths.append(str(replay_path))
            else:
                logger.warning(f"Replay file not found for {replay_id}")

        if not replay_paths:
            print("No replay files found on disk.")
            return PipelineResult(
                total_replays=0,
                successful=0,
                skipped=0,
                failed=0,
                total_frames=0,
                total_bytes=0,
                output_paths=[],
                failed_replays=[]
            )

        print(f"Found {len(replay_paths)} replay files on disk")

        return self.parse_replays(replay_paths, output_dir, compression)

    def retry_failed_parses(
        self,
        raw_replays_dir: str,
        output_dir: str,
        compression: str = PipelineConfig.PARQUET_COMPRESSION
    ) -> PipelineResult:
        """
        Retry parsing replays that previously failed.

        Args:
            raw_replays_dir: Directory containing raw .replay files
            output_dir: Directory to save output files
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics

        Raises:
            ValueError: If database is not configured
        """
        if not self.db:
            raise ValueError("Database required for retry_failed_parses(). "
                           "Initialize ParsingPipeline with a database.")

        # Get failed parses
        failed = self.db.get_failed_parses()

        if not failed:
            print("No failed parses to retry.")
            return PipelineResult(
                total_replays=0,
                successful=0,
                skipped=0,
                failed=0,
                total_frames=0,
                total_bytes=0,
                output_paths=[],
                failed_replays=[]
            )

        print(f"Found {len(failed)} failed parses to retry")

        # Build file paths
        raw_dir = Path(raw_replays_dir)
        replay_paths = []

        for parse_info in failed:
            replay_id = parse_info['raw_replay_id']
            replay_path = raw_dir / f"{replay_id}.replay"

            # Search recursively if not found
            if not replay_path.exists():
                matches = list(raw_dir.rglob(f"{replay_id}.replay"))
                if matches:
                    replay_path = matches[0]

            if replay_path.exists():
                replay_paths.append(str(replay_path))
            else:
                logger.warning(f"Replay file not found for {replay_id}")

        if not replay_paths:
            print("No replay files found for failed parses.")
            return PipelineResult(
                total_replays=0,
                successful=0,
                skipped=0,
                failed=0,
                total_frames=0,
                total_bytes=0,
                output_paths=[],
                failed_replays=[]
            )

        return self.parse_replays(replay_paths, output_dir, compression)
