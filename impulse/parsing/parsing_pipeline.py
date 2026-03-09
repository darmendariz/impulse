"""
Replay Parsing Pipeline

Orchestrates the parsing workflow: raw replay -> parsed data -> database tracking.
Coordinates ReplayParser and ParseResultFormatter with database registration.
"""

import json
import logging
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from impulse.parsing.replay_parser import ReplayParser, ParseResult
from impulse.parsing.parse_result_formatter import ParseResultFormatter, FormatResult
from impulse.collection.database import ImpulseDB
from impulse.config.parsing_config import ParsingConfig
from impulse.config.pipeline_config import PipelineConfig

if TYPE_CHECKING:
    from impulse.collection.s3_manager import S3Manager

logger = logging.getLogger('impulse.parsing')


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
        >>> # Parse multiple replays from local paths
        >>> result = pipeline.parse_replays(replay_paths, './output')
        >>>
        >>> # Parse all unparsed replays (local)
        >>> result = pipeline.parse_unparsed('./output', raw_replays_dir='./raw')
        >>>
        >>> # Parse all unparsed replays from S3
        >>> pipeline_s3 = ParsingPipeline(parser, db, s3_manager=s3)
        >>> result = pipeline_s3.parse_unparsed('./output')
    """

    def __init__(
        self,
        parser: ReplayParser,
        db: Optional[ImpulseDB] = None,
        formatter: Optional[ParseResultFormatter] = None,
        s3_manager: Optional["S3Manager"] = None
    ):
        """
        Initialize the parsing pipeline.

        Args:
            parser: ReplayParser instance configured with features and FPS
            db: Optional database for tracking (enables deduplication and registration)
            formatter: Optional ParseResultFormatter (uses default if not provided)
            s3_manager: Optional S3Manager for downloading raw replays from S3 and
                        uploading parsed output (parquet + metadata JSON) back to S3
        """
        self.parser = parser
        self.db = db
        self.formatter = formatter or ParseResultFormatter()
        self.s3_manager = s3_manager

    @contextmanager
    def _temp_replay_from_s3(self, storage_key: str, replay_id: str):
        """
        Download a raw replay from S3 to a named temp file and clean up on exit.

        The temp file is named {replay_id}.replay so that parse_replay() correctly
        derives the replay ID from the filename. Cleanup is guaranteed via finally,
        even if an exception occurs during parsing.

        Args:
            storage_key: S3 key of the raw replay file
            replay_id: Replay ID (used to name the temp file)

        Yields:
            Path to the temp replay file
        """
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_path = tmp_dir / f"{replay_id}.replay"
        try:
            success = self.s3_manager.download_file(storage_key, str(tmp_path))
            if not success:
                raise RuntimeError(f"Failed to download {storage_key} from S3")
            yield tmp_path
        finally:
            tmp_path.unlink(missing_ok=True)
            tmp_dir.rmdir()

    def _upload_to_s3(
        self,
        format_result: FormatResult,
        raw_storage_key: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Upload parsed parquet and metadata JSON files to S3.

        Mirrors the raw storage key structure when raw_storage_key is provided:
            raw:    replays/rlcs/2024/.../{replay_id}.replay
            parsed: replays-parsed/rlcs/2024/.../{replay_id}.parquet
                    replays-parsed/rlcs/2024/.../{replay_id}.metadata.json

        Falls back to a flat structure otherwise:
            replays-parsed/{replay_id}.parquet
            replays-parsed/{replay_id}.metadata.json

        Args:
            format_result: Successful FormatResult with local parquet_path and metadata_path
            raw_storage_key: Optional raw replay S3 key to mirror the path structure

        Returns:
            Dict with 'parquet_key' and 'metadata_key' S3 keys

        Raises:
            RuntimeError: If either upload fails
        """
        config = PipelineConfig()

        if raw_storage_key:
            # Strip the first path component (e.g. 'replays') and replace with parsed prefix
            path_without_prefix = '/'.join(raw_storage_key.split('/')[1:])
            base_key = f"{config.S3_PARSED_PREFIX}/{path_without_prefix.replace('.replay', '')}"
        else:
            base_key = f"{config.S3_PARSED_PREFIX}/{format_result.replay_id}"

        parquet_key = f"{base_key}.parquet"
        metadata_key = f"{base_key}.metadata.json"

        parquet_result = self.s3_manager.upload_file(format_result.parquet_path, parquet_key)
        if not parquet_result['success']:
            raise RuntimeError(f"S3 upload failed for parquet: {parquet_result.get('error')}")

        metadata_result = self.s3_manager.upload_file(format_result.metadata_path, metadata_key)
        if not metadata_result['success']:
            raise RuntimeError(f"S3 upload failed for metadata: {metadata_result.get('error')}")

        return {'parquet_key': parquet_key, 'metadata_key': metadata_key}

    def parse_replay(
        self,
        replay_path: str,
        output_dir: str,
        compression: str = PipelineConfig.PARQUET_COMPRESSION,
        raw_storage_key: Optional[str] = None
    ) -> FormatResult:
        """
        Parse a single replay file, save output locally, and optionally upload to S3.

        Args:
            replay_path: Path to .replay file
            output_dir: Directory to save output files
            compression: Parquet compression algorithm
            raw_storage_key: Optional S3 key of the source raw replay. When provided
                             and s3_manager is configured, the parsed output is uploaded
                             to S3 mirroring the raw key's path structure.

        Returns:
            FormatResult with parsing outcome. result.skipped=True if already parsed.
        """
        replay_path = Path(replay_path)
        replay_id = replay_path.stem

        # Check if already parsed
        if self.db and self.db.is_replay_parsed(replay_id):
            logger.info(f"Skipping {replay_id}: already parsed")
            return FormatResult(
                success=True,
                skipped=True,
                replay_id=replay_id,
                dataframe=None,
                metadata=None,
                num_rows=0,
                num_columns=0,
                num_players=0,
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

        # Save to parquet locally
        format_result = self.formatter.save_to_parquet(format_result, output_dir, compression)

        if not format_result.success:
            if self.db:
                self.db.mark_parse_failed(replay_id, replay_id, format_result.error)
            return format_result

        # Upload to S3 if configured
        output_path = format_result.parquet_path
        if self.s3_manager:
            try:
                s3_keys = self._upload_to_s3(format_result, raw_storage_key)
                output_path = s3_keys['parquet_key']
            except Exception as e:
                if self.db:
                    self.db.mark_parse_failed(replay_id, replay_id, str(e))
                format_result.success = False
                format_result.error = str(e)
                return format_result

        # Register in database
        if self.db:
            metadata_json = json.dumps(format_result.metadata) if format_result.metadata else None
            self.db.add_parsed_replay(
                replay_id=replay_id,
                raw_replay_id=replay_id,
                output_path=output_path,
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
        Parse multiple replay files from local paths.

        For S3-sourced replays, use parse_unparsed() instead, which handles
        temp file management and S3 upload automatically.

        Args:
            replay_paths: List of paths to .replay files
            output_dir: Directory to save output files
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics
        """
        total = len(replay_paths)
        successful = skipped = failed = 0
        total_frames = total_bytes = 0
        output_paths = []
        failed_replays = []
        width = len(str(total))

        print(f"Parsing {total} replays...")

        for i, replay_path in enumerate(replay_paths, 1):
            replay_id = Path(replay_path).stem
            counter = f"[{i:{width}}/{total}]"

            try:
                format_result = self.parse_replay(replay_path, output_dir, compression)
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to parse {replay_id}: {error_msg}")
                if self.db:
                    self.db.mark_parse_failed(replay_id, replay_id, error_msg)
                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': error_msg})
                print(f"{counter} {replay_id}  FAILED: {error_msg}")
                continue

            if format_result.skipped:
                skipped += 1
                print(f"{counter} {replay_id}  skipped")
            elif not format_result.success:
                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': format_result.error})
                print(f"{counter} {replay_id}  FAILED: {format_result.error}")
            else:
                successful += 1
                total_frames += format_result.num_rows
                total_bytes += format_result.parquet_size_bytes
                output_path = format_result.parquet_path or ''
                output_paths.append(output_path)
                mb = format_result.parquet_size_bytes / (1024 * 1024)
                print(f"{counter} {replay_id}  {format_result.num_rows} frames  {mb:.2f} MB")

        return self._finish_batch(
            total, successful, skipped, failed,
            total_frames, total_bytes, output_paths, failed_replays
        )

    def parse_unparsed(
        self,
        output_dir: str,
        raw_replays_dir: Optional[str] = None,
        limit: Optional[int] = None,
        compression: str = PipelineConfig.PARQUET_COMPRESSION
    ) -> PipelineResult:
        """
        Parse all downloaded replays that haven't been parsed yet.

        When an S3Manager is configured, each replay is downloaded from S3 using
        the storage_key recorded in the database, parsed, and the output is uploaded
        back to S3. The temp file is cleaned up after each replay.

        When no S3Manager is configured, raw_replays_dir is required to locate
        replay files on disk.

        Args:
            output_dir: Directory to save parsed output files
            raw_replays_dir: Local directory containing raw .replay files.
                             Required when s3_manager is not configured.
            limit: Maximum number of replays to parse (None for all)
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics

        Raises:
            ValueError: If database is not configured, or if s3_manager is not
                        set and raw_replays_dir is not provided
        """
        if not self.db:
            raise ValueError("Database required for parse_unparsed(). "
                             "Initialize ParsingPipeline with a database.")

        if not self.s3_manager and not raw_replays_dir:
            raise ValueError("raw_replays_dir is required when s3_manager is not configured.")

        unparsed = self.db.get_unparsed_replays(limit=limit)

        if not unparsed:
            print("No unparsed replays found.")
            return PipelineResult(
                total_replays=0, successful=0, skipped=0, failed=0,
                total_frames=0, total_bytes=0, output_paths=[], failed_replays=[]
            )

        print(f"Found {len(unparsed)} unparsed replays")

        if self.s3_manager:
            return self._parse_from_s3(unparsed, output_dir, compression)
        else:
            replay_paths = self._resolve_local_paths(unparsed, raw_replays_dir)
            if not replay_paths:
                print("No replay files found on disk.")
                return PipelineResult(
                    total_replays=0, successful=0, skipped=0, failed=0,
                    total_frames=0, total_bytes=0, output_paths=[], failed_replays=[]
                )
            print(f"Found {len(replay_paths)} replay files on disk")
            return self.parse_replays(replay_paths, output_dir, compression)

    def retry_failed_parses(
        self,
        output_dir: str,
        raw_replays_dir: Optional[str] = None,
        compression: str = PipelineConfig.PARQUET_COMPRESSION
    ) -> PipelineResult:
        """
        Retry parsing replays that previously failed.

        When an S3Manager is configured, replays are downloaded from S3.
        Otherwise, raw_replays_dir is required to locate files on disk.

        Args:
            output_dir: Directory to save output files
            raw_replays_dir: Local directory containing raw .replay files.
                             Required when s3_manager is not configured.
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics

        Raises:
            ValueError: If database is not configured, or if s3_manager is not
                        set and raw_replays_dir is not provided
        """
        if not self.db:
            raise ValueError("Database required for retry_failed_parses(). "
                             "Initialize ParsingPipeline with a database.")

        if not self.s3_manager and not raw_replays_dir:
            raise ValueError("raw_replays_dir is required when s3_manager is not configured.")

        failed = self.db.get_failed_parses()

        if not failed:
            print("No failed parses to retry.")
            return PipelineResult(
                total_replays=0, successful=0, skipped=0, failed=0,
                total_frames=0, total_bytes=0, output_paths=[], failed_replays=[]
            )

        print(f"Retrying {len(failed)} failed parse(s)...")

        if self.s3_manager:
            return self._parse_from_s3(failed, output_dir, compression)
        else:
            replay_paths = self._resolve_local_paths(
                failed, raw_replays_dir, id_key='raw_replay_id'
            )
            if not replay_paths:
                print("No replay files found for failed parses.")
                return PipelineResult(
                    total_replays=0, successful=0, skipped=0, failed=0,
                    total_frames=0, total_bytes=0, output_paths=[], failed_replays=[]
                )
            return self.parse_replays(replay_paths, output_dir, compression)

    def _parse_from_s3(
        self,
        replay_infos: List[Dict],
        output_dir: str,
        compression: str
    ) -> PipelineResult:
        """
        Parse replays by downloading each from S3, parsing, and uploading output.

        Replays are processed one at a time; the temp file for each raw replay is
        cleaned up before the next download begins.

        Args:
            replay_infos: List of dicts from get_unparsed_replays() or get_failed_parses(),
                          each containing 'replay_id' and 'storage_key'
            output_dir: Directory to save parsed output files locally
            compression: Parquet compression algorithm

        Returns:
            PipelineResult with statistics
        """
        total = len(replay_infos)
        successful = skipped = failed = 0
        total_frames = total_bytes = 0
        output_paths = []
        failed_replays = []
        width = len(str(total))

        print(f"Parsing {total} replays from S3...")

        for i, replay_info in enumerate(replay_infos, 1):
            replay_id = replay_info['replay_id']
            storage_key = replay_info.get('storage_key')
            counter = f"[{i:{width}}/{total}]"

            if not storage_key:
                error_msg = "No storage_key recorded in database"
                logger.warning(f"Skipping {replay_id}: {error_msg}")
                if self.db:
                    self.db.mark_parse_failed(replay_id, replay_id, error_msg)
                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': error_msg})
                print(f"{counter} {replay_id}  FAILED: {error_msg}")
                continue

            try:
                with self._temp_replay_from_s3(storage_key, replay_id) as tmp_path:
                    format_result = self.parse_replay(
                        str(tmp_path), output_dir, compression,
                        raw_storage_key=storage_key
                    )
                # temp file is cleaned up here, before next download

            except Exception as e:
                # Download failure (parse_replay was never called)
                error_msg = str(e)
                logger.error(f"Failed to download {replay_id}: {error_msg}")
                if self.db:
                    self.db.mark_parse_failed(replay_id, replay_id, error_msg)
                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': error_msg})
                print(f"{counter} {replay_id}  FAILED: {error_msg}")
                continue

            # parse_replay() returns a FormatResult for all outcomes (no exceptions)
            if format_result.skipped:
                skipped += 1
                print(f"{counter} {replay_id}  skipped")
            elif not format_result.success:
                # DB already marked as failed inside parse_replay()
                failed += 1
                failed_replays.append({'replay_id': replay_id, 'error': format_result.error})
                print(f"{counter} {replay_id}  FAILED: {format_result.error}")
            else:
                successful += 1
                total_frames += format_result.num_rows
                total_bytes += format_result.parquet_size_bytes
                output_paths.append(format_result.parquet_path or '')
                mb = format_result.parquet_size_bytes / (1024 * 1024)
                print(f"{counter} {replay_id}  {format_result.num_rows} frames  {mb:.2f} MB")

        return self._finish_batch(
            total, successful, skipped, failed,
            total_frames, total_bytes, output_paths, failed_replays
        )

    def _resolve_local_paths(
        self,
        replay_infos: List[Dict],
        raw_replays_dir: str,
        id_key: str = 'replay_id'
    ) -> List[str]:
        """
        Resolve local file paths for a list of replay records.

        Tries storage_key-based path first, then {replay_id}.replay, then a
        recursive glob as a last resort.

        Args:
            replay_infos: List of replay info dicts
            raw_replays_dir: Base directory to search for .replay files
            id_key: Dict key to use as the replay ID ('replay_id' or 'raw_replay_id')

        Returns:
            List of resolved file paths for replays found on disk
        """
        raw_dir = Path(raw_replays_dir)
        replay_paths = []

        for replay_info in replay_infos:
            replay_id = replay_info[id_key]
            storage_key = replay_info.get('storage_key')
            replay_path = None

            if storage_key:
                candidate = raw_dir / storage_key
                if candidate.exists():
                    replay_path = candidate

            if not replay_path:
                candidate = raw_dir / f"{replay_id}.replay"
                if candidate.exists():
                    replay_path = candidate

            if not replay_path:
                matches = list(raw_dir.rglob(f"{replay_id}.replay"))
                if matches:
                    replay_path = matches[0]

            if replay_path:
                replay_paths.append(str(replay_path))
            else:
                logger.warning(f"Replay file not found for {replay_id}")

        return replay_paths

    def _finish_batch(
        self,
        total: int,
        successful: int,
        skipped: int,
        failed: int,
        total_frames: int,
        total_bytes: int,
        output_paths: List[str],
        failed_replays: List[Dict]
    ) -> PipelineResult:
        """Print batch summary, push DB to S3 if configured, and return PipelineResult."""
        total_mb = total_bytes / (1024 ** 2)
        print(f"Done. Parsed: {successful}  Skipped: {skipped}  Failed: {failed}  "
              f"({total_frames:,} frames, {total_mb:.1f} MB)")

        if self.db:
            stats = self.db.get_parse_stats()
            print(f"DB totals — parsed: {stats.get('parsed', 0)}  "
                  f"failed: {stats.get('failed', 0)}  "
                  f"pending: {stats.get('pending', 0)}")

            if self.db.s3_manager:
                try:
                    self.db.push()
                except Exception as e:
                    logger.warning(f"Database push to S3 failed: {e}")

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
