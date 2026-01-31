"""
Impulse Database Manager - SQLite database for replay file tracking

Tracks both raw replay downloads and parsed replay data in a single database.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from contextlib import contextmanager


class ImpulseDB:
    """Manages SQLite database for replay tracking (raw and parsed)."""

    def __init__(self, db_path: str = "./impulse.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_database()
        print(f"Database initialized: {self.db_path}")

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def init_database(self):
        """Create all tables if they don't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Table 1: Groups - track Ballchasing groups we've synced
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    group_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    downloaded_at TEXT,
                    replay_count INTEGER DEFAULT 0
                )
            """)

            # Table 2: Raw Replays - tracks downloaded .replay files
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_replays (
                    replay_id TEXT PRIMARY KEY,
                    title TEXT,
                    date TEXT,
                    blue_team TEXT,
                    orange_team TEXT,
                    storage_key TEXT,
                    file_size_bytes INTEGER,
                    downloaded_at TEXT,
                    is_downloaded BOOLEAN DEFAULT 0,
                    download_status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    CHECK (download_status IN ('pending', 'downloaded', 'failed'))
                )
            """)

            # Table 3: Parsed Replays - tracks parsed replay data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS parsed_replays (
                    replay_id TEXT PRIMARY KEY,
                    raw_replay_id TEXT,
                    output_path TEXT,
                    output_format TEXT,
                    fps REAL,
                    frame_count INTEGER,
                    feature_count INTEGER,
                    file_size_bytes INTEGER,
                    parsed_at TEXT,
                    parse_status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    metadata TEXT,
                    FOREIGN KEY (raw_replay_id) REFERENCES raw_replays(replay_id),
                    CHECK (parse_status IN ('pending', 'parsed', 'failed'))
                )
            """)

            # Indexes for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_replays_downloaded
                ON raw_replays(is_downloaded)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_replays_status
                ON raw_replays(download_status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_parsed_replays_status
                ON parsed_replays(parse_status)
            """)

    # =========================================================================
    # Raw Replay Methods (Download Tracking)
    # =========================================================================

    def add_replay(self, replay_id: str, ballchasing_metadata: Dict) -> bool:
        """
        Add a raw replay to the database.

        Returns True if this is a NEW replay, False if it already existed.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT replay_id FROM raw_replays WHERE replay_id = ?", (replay_id,))
            if cursor.fetchone() is not None:
                return False

            blue = ballchasing_metadata.get('blue', {})
            orange = ballchasing_metadata.get('orange', {})

            cursor.execute("""
                INSERT INTO raw_replays (replay_id, title, date, blue_team, orange_team, is_downloaded)
                VALUES (?, ?, ?, ?, ?, 0)
            """, (
                replay_id,
                ballchasing_metadata.get('replay_title', 'Unknown'),
                ballchasing_metadata.get('date'),
                blue.get('name', 'Unknown'),
                orange.get('name', 'Unknown')
            ))

            return True

    def register_group_download(self, group_id: str, name: str, replay_count: int):
        """Track that we've downloaded this group from Ballchasing."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO groups (group_id, name, downloaded_at, replay_count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    downloaded_at = excluded.downloaded_at,
                    replay_count = excluded.replay_count
            """, (group_id, name, datetime.now(timezone.utc).isoformat(), replay_count))

    def is_replay_downloaded(self, replay_id: str) -> bool:
        """Check if raw replay has been downloaded already."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT is_downloaded FROM raw_replays
                WHERE replay_id = ? AND is_downloaded = 1
            """, (replay_id,))
            return cursor.fetchone() is not None

    def mark_downloaded(self, replay_id: str, storage_key: str, file_size: int):
        """Mark a raw replay as downloaded with its storage location."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE raw_replays
                SET storage_key = ?,
                    file_size_bytes = ?,
                    downloaded_at = ?,
                    is_downloaded = 1,
                    download_status = 'downloaded'
                WHERE replay_id = ?
            """, (storage_key, file_size, datetime.now(timezone.utc).isoformat(), replay_id))

    def mark_replay_failed(self, replay_id: str, error_message: str = None):
        """Mark a raw replay download as failed."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE raw_replays
                SET download_status = 'failed',
                    error_message = ?
                WHERE replay_id = ?
            """, (error_message, replay_id))

    def get_failed_replays(self) -> List[Dict]:
        """Get all raw replays that failed to download."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT replay_id, title, error_message
                FROM raw_replays
                WHERE download_status = 'failed'
            """)
            return [dict(row) for row in cursor.fetchall()]

    def get_downloaded_replays(self, limit: Optional[int] = None) -> List[Dict]:
        """Get all successfully downloaded raw replays."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT replay_id, title, date, blue_team, orange_team,
                       storage_key, file_size_bytes, downloaded_at
                FROM raw_replays
                WHERE is_downloaded = 1
            """
            if limit:
                query += f" LIMIT {limit}"
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Parsed Replay Methods
    # =========================================================================

    def add_parsed_replay(
        self,
        replay_id: str,
        raw_replay_id: str,
        output_path: str,
        output_format: str,
        fps: float,
        frame_count: int,
        feature_count: int,
        file_size_bytes: int,
        metadata: Optional[str] = None
    ) -> bool:
        """
        Register a successfully parsed replay.

        If the replay was previously marked as failed, updates it to parsed status.

        Args:
            replay_id: Unique ID for the parsed output (can be same as raw_replay_id)
            raw_replay_id: ID of the source raw replay
            output_path: Path to the parsed output file
            output_format: Format of output (e.g., 'parquet', 'numpy')
            fps: Frames per second used for parsing
            frame_count: Number of frames in the parsed data
            feature_count: Number of features in the parsed data
            file_size_bytes: Size of the output file
            metadata: Optional JSON string with additional metadata

        Returns:
            True if this is a new entry, False if updating existing entry
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO parsed_replays (
                    replay_id, raw_replay_id, output_path, output_format,
                    fps, frame_count, feature_count, file_size_bytes,
                    parsed_at, parse_status, metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'parsed', ?)
                ON CONFLICT(replay_id) DO UPDATE SET
                    output_path = excluded.output_path,
                    output_format = excluded.output_format,
                    fps = excluded.fps,
                    frame_count = excluded.frame_count,
                    feature_count = excluded.feature_count,
                    file_size_bytes = excluded.file_size_bytes,
                    parsed_at = excluded.parsed_at,
                    parse_status = 'parsed',
                    error_message = NULL,
                    metadata = excluded.metadata
            """, (
                replay_id, raw_replay_id, output_path, output_format,
                fps, frame_count, feature_count, file_size_bytes,
                datetime.now(timezone.utc).isoformat(), metadata
            ))

            return cursor.rowcount > 0

    def is_replay_parsed(self, replay_id: str) -> bool:
        """Check if a replay has been parsed already."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT replay_id FROM parsed_replays
                WHERE replay_id = ? AND parse_status = 'parsed'
            """, (replay_id,))
            return cursor.fetchone() is not None

    def mark_parse_failed(self, replay_id: str, raw_replay_id: str, error_message: str = None):
        """Mark a replay parsing as failed."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO parsed_replays (replay_id, raw_replay_id, parse_status, error_message)
                VALUES (?, ?, 'failed', ?)
                ON CONFLICT(replay_id) DO UPDATE SET
                    parse_status = 'failed',
                    error_message = excluded.error_message
            """, (replay_id, raw_replay_id, error_message))

    def get_unparsed_replays(self, limit: Optional[int] = None) -> List[Dict]:
        """Get downloaded replays that haven't been successfully parsed yet."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT r.replay_id, r.title, r.storage_key, r.file_size_bytes
                FROM raw_replays r
                LEFT JOIN parsed_replays p ON r.replay_id = p.raw_replay_id
                    AND p.parse_status = 'parsed'
                WHERE r.is_downloaded = 1 AND p.replay_id IS NULL
            """
            if limit:
                query += f" LIMIT {limit}"
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def get_parsed_replays(self, limit: Optional[int] = None) -> List[Dict]:
        """Get all successfully parsed replays."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT replay_id, raw_replay_id, output_path, output_format,
                       fps, frame_count, feature_count, file_size_bytes, parsed_at
                FROM parsed_replays
                WHERE parse_status = 'parsed'
            """
            if limit:
                query += f" LIMIT {limit}"
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def get_failed_parses(self) -> List[Dict]:
        """Get all replays that failed to parse."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT replay_id, raw_replay_id, error_message
                FROM parsed_replays
                WHERE parse_status = 'failed'
            """)
            return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self) -> Dict:
        """Get statistics for raw replays."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) as total FROM raw_replays")
            total = cursor.fetchone()['total']

            cursor.execute("SELECT COUNT(*) as downloaded FROM raw_replays WHERE is_downloaded = 1")
            downloaded = cursor.fetchone()['downloaded']

            cursor.execute("SELECT SUM(file_size_bytes) as bytes FROM raw_replays WHERE is_downloaded = 1")
            total_bytes = cursor.fetchone()['bytes'] or 0

            cursor.execute("SELECT COUNT(*) as failed FROM raw_replays WHERE download_status = 'failed'")
            failed = cursor.fetchone()['failed']

            return {
                'total_replays': total,
                'downloaded': downloaded,
                'failed': failed,
                'pending': total - downloaded - failed,
                'storage_mb': round(total_bytes / (1024**2), 2),
                'storage_gb': round(total_bytes / (1024**3), 2)
            }

    def get_parse_stats(self) -> Dict:
        """Get statistics for parsed replays."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) as total FROM parsed_replays")
            total = cursor.fetchone()['total']

            cursor.execute("SELECT COUNT(*) as parsed FROM parsed_replays WHERE parse_status = 'parsed'")
            parsed = cursor.fetchone()['parsed']

            cursor.execute("SELECT SUM(file_size_bytes) as bytes FROM parsed_replays WHERE parse_status = 'parsed'")
            total_bytes = cursor.fetchone()['bytes'] or 0

            cursor.execute("SELECT COUNT(*) as failed FROM parsed_replays WHERE parse_status = 'failed'")
            failed = cursor.fetchone()['failed']

            cursor.execute("SELECT SUM(frame_count) as frames FROM parsed_replays WHERE parse_status = 'parsed'")
            total_frames = cursor.fetchone()['frames'] or 0

            return {
                'total_entries': total,
                'parsed': parsed,
                'failed': failed,
                'total_frames': total_frames,
                'storage_mb': round(total_bytes / (1024**2), 2),
                'storage_gb': round(total_bytes / (1024**3), 2)
            }

    def get_full_stats(self) -> Dict:
        """Get combined statistics for raw and parsed replays."""
        return {
            'raw': self.get_stats(),
            'parsed': self.get_parse_stats()
        }
