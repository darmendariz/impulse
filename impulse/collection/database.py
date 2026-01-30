"""
Impulse Database Manager - Simple SQLite database for replay file tracking
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from contextlib import contextmanager


class ImpulseDB:
    """Manages SQLite database for replay tracking"""
    
    def __init__(self, db_path: str = "./replays/raw/raw_replays.db"):
        self.db_path = Path(db_path)
        self.init_database()
        print(f"✓ Database initialized: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dicts
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def init_database(self):
        """Create groups and replay tables if they don't exist.
        Groups table tracks synced Ballchasing groups.
        Replays table tracks individual replays and their download status."""
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
            
            # Table 2: Replays - one row per unique replay
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS replays (
                    replay_id TEXT PRIMARY KEY,
                    title TEXT,
                    date TEXT,
                    blue_team TEXT,
                    orange_team TEXT,
                    s3_key TEXT,
                    file_size_bytes INTEGER,
                    downloaded_at TEXT,
                    is_downloaded BOOLEAN DEFAULT 0,
                    download_status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    CHECK (download_status IN ('pending', 'downloaded', 'failed'))
                )
            """)
            
            # Index for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_replays_downloaded 
                ON replays(is_downloaded)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_replays_status 
                ON replays(download_status)
            """)
    
    def add_replay(self, replay_id: str, ballchasing_metadata: Dict) -> bool:
        """
        Add a replay to the database.
        Returns True if this is a NEW replay, False if it already existed.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if it exists
            cursor.execute("SELECT replay_id FROM replays WHERE replay_id = ?", (replay_id,))
            already_exists = cursor.fetchone() is not None
            
            if already_exists:
                return False  # Already have this replay
            
            # Extract basic info from ballchasing metadata
            blue = ballchasing_metadata.get('blue', {})
            orange = ballchasing_metadata.get('orange', {})
            
            # Insert new replay
            cursor.execute("""
                INSERT INTO replays (replay_id, title, date, blue_team, orange_team, is_downloaded)
                VALUES (?, ?, ?, ?, ?, 0)
            """, (
                replay_id,
                ballchasing_metadata.get('replay_title', 'Unknown'),
                ballchasing_metadata.get('date'),
                blue.get('name', 'Unknown'),
                orange.get('name', 'Unknown')
            ))
            
            return True  # New replay added
        
    def register_group_download(self, group_id: str, name: str, replay_count: int):
        """Track that we've downloaded this group from Ballchasing.
        
        Args:
            group_id: Ballchasing group ID
            name: Group name
            replay_count: Number of replays in the group
        """
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
        """Check if replay has been downloaded already"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT is_downloaded FROM replays 
                WHERE replay_id = ? AND is_downloaded = 1
            """, (replay_id,))
            return cursor.fetchone() is not None

    def mark_downloaded(self, replay_id: str, s3_key: str, file_size: int):
        """
        Mark a replay as downloaded with its S3 location.
        
        Args:
            replay_id: Replay ID
            s3_key: S3 key where replay is stored
            file_size: File size in bytes
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE replays
                SET s3_key = ?,
                    file_size_bytes = ?,
                    downloaded_at = ?,
                    is_downloaded = 1,
                    download_status = 'downloaded'
                WHERE replay_id = ?
            """, (s3_key, file_size, datetime.now(timezone.utc).isoformat(), replay_id))

    def mark_replay_failed(self, replay_id: str, error_message: str = None):
        """
        Mark a replay download as failed.
        
        Args:
            replay_id: Replay ID that failed
            error_message: Optional error description
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE replays
                SET download_status = 'failed',
                    error_message = ?
                WHERE replay_id = ?
            """, (error_message, replay_id))

    def get_failed_replays(self, group_id: str = None) -> list:
        """
        Get all replays that failed to download. Useful for retrying failed downloads.
        
        Args:
            group_id: Optional group ID to filter by
            
        Returns:
            List of failed replay dicts
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT replay_id, title, error_message 
                FROM replays 
                WHERE download_status = 'failed'
            """)
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
    
    def get_stats(self) -> Dict:
        """Get simple statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) as total FROM replays")
            total = cursor.fetchone()['total']
            
            cursor.execute("SELECT COUNT(*) as downloaded FROM replays WHERE is_downloaded = 1")
            downloaded = cursor.fetchone()['downloaded']
            
            cursor.execute("SELECT SUM(file_size_bytes) as bytes FROM replays WHERE is_downloaded = 1")
            total_bytes = cursor.fetchone()['bytes'] or 0

            cursor.execute("SELECT COUNT(*) as failed FROM replays WHERE download_status = 'failed'")
            failed = cursor.fetchone()['failed']
            
            return {
                'total_replays': total,
                'downloaded': downloaded,
                'failed': failed,
                'pending': total - downloaded - failed,
                'storage_mb': round(total_bytes / (1024**2), 2),
                'storage_gb': round(total_bytes / (1024**3), 2)
            }
