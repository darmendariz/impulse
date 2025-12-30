"""
Unit tests for impulse.collection.database module.

Tests database operations for replay tracking and statistics.
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
from impulse.collection.database import ImpulseDB


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    # Create temp file
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    # Create database
    db = ImpulseDB(db_path=path)

    yield db

    # Cleanup
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def sample_metadata():
    """Sample Ballchasing metadata for testing."""
    return {
        'replay_title': 'Test Match',
        'date': '2024-01-15T10:30:00Z',
        'blue': {
            'name': 'Blue Team'
        },
        'orange': {
            'name': 'Orange Team'
        }
    }


@pytest.mark.unit
class TestImpulseDatabaseInitialization:
    """Test database initialization."""

    def test_creates_database_file(self):
        """Database file should be created on initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            assert not db_path.exists()

            db = ImpulseDB(db_path=str(db_path))

            assert db_path.exists()

    def test_creates_groups_table(self, temp_db):
        """Should create groups table with correct schema."""
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()

            # Check table exists
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='groups'
            """)
            assert cursor.fetchone() is not None

            # Check columns
            cursor.execute("PRAGMA table_info(groups)")
            columns = {row['name'] for row in cursor.fetchall()}

            assert 'group_id' in columns
            assert 'name' in columns
            assert 'downloaded_at' in columns
            assert 'replay_count' in columns

    def test_creates_replays_table(self, temp_db):
        """Should create replays table with correct schema."""
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()

            # Check table exists
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='replays'
            """)
            assert cursor.fetchone() is not None

            # Check columns
            cursor.execute("PRAGMA table_info(replays)")
            columns = {row['name'] for row in cursor.fetchall()}

            expected_columns = {
                'replay_id', 'title', 'date', 'blue_team', 'orange_team',
                's3_key', 'file_size_bytes', 'downloaded_at', 'is_downloaded',
                'download_status', 'error_message'
            }
            assert expected_columns.issubset(columns)

    def test_creates_indexes(self, temp_db):
        """Should create indexes for performance."""
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='index' AND tbl_name='replays'
            """)
            indexes = {row['name'] for row in cursor.fetchall()}

            assert 'idx_replays_downloaded' in indexes
            assert 'idx_replays_status' in indexes

    def test_uses_correct_db_path(self):
        """Should use the provided database path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_path = Path(tmpdir) / "custom.db"

            db = ImpulseDB(db_path=str(custom_path))

            assert db.db_path == custom_path


@pytest.mark.unit
class TestAddReplay:
    """Test add_replay() method."""

    def test_adds_new_replay(self, temp_db, sample_metadata):
        """Should add new replay and return True."""
        is_new = temp_db.add_replay('replay-123', sample_metadata)

        assert is_new is True

        # Verify it was added
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row is not None
            assert row['replay_id'] == 'replay-123'
            assert row['title'] == 'Test Match'
            assert row['blue_team'] == 'Blue Team'
            assert row['orange_team'] == 'Orange Team'
            assert row['is_downloaded'] == 0
            assert row['download_status'] == 'pending'

    def test_returns_false_for_duplicate(self, temp_db, sample_metadata):
        """Should return False when adding duplicate replay."""
        # Add once
        temp_db.add_replay('replay-123', sample_metadata)

        # Try to add again
        is_new = temp_db.add_replay('replay-123', sample_metadata)

        assert is_new is False

    def test_does_not_create_duplicate_rows(self, temp_db, sample_metadata):
        """Should not create duplicate rows for same replay_id."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.add_replay('replay-123', sample_metadata)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM replays WHERE replay_id = ?", ('replay-123',))
            count = cursor.fetchone()['count']

            assert count == 1

    def test_handles_missing_metadata_fields(self, temp_db):
        """Should handle metadata with missing optional fields."""
        minimal_metadata = {
            'replay_title': 'Minimal Replay'
        }

        is_new = temp_db.add_replay('replay-456', minimal_metadata)

        assert is_new is True

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('replay-456',))
            row = cursor.fetchone()

            assert row['title'] == 'Minimal Replay'
            assert row['blue_team'] == 'Unknown'
            assert row['orange_team'] == 'Unknown'

    def test_extracts_team_names_correctly(self, temp_db, sample_metadata):
        """Should correctly extract team names from nested metadata."""
        is_new = temp_db.add_replay('replay-789', sample_metadata)

        assert is_new is True

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT blue_team, orange_team FROM replays WHERE replay_id = ?", ('replay-789',))
            row = cursor.fetchone()

            assert row['blue_team'] == 'Blue Team'
            assert row['orange_team'] == 'Orange Team'


@pytest.mark.unit
class TestRegisterGroupDownload:
    """Test register_group_download() method."""

    def test_registers_new_group(self, temp_db):
        """Should register a new group."""
        temp_db.register_group_download('group-abc', 'RLCS 2024', 150)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM groups WHERE group_id = ?", ('group-abc',))
            row = cursor.fetchone()

            assert row is not None
            assert row['group_id'] == 'group-abc'
            assert row['name'] == 'RLCS 2024'
            assert row['replay_count'] == 150
            assert row['downloaded_at'] is not None

    def test_updates_existing_group(self, temp_db):
        """Should update group if it already exists."""
        # Register initially
        temp_db.register_group_download('group-abc', 'RLCS 2024', 100)

        # Register again with updated count
        temp_db.register_group_download('group-abc', 'RLCS 2024', 150)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM groups WHERE group_id = ?", ('group-abc',))
            row = cursor.fetchone()

            assert row['replay_count'] == 150

    def test_does_not_create_duplicate_groups(self, temp_db):
        """Should not create duplicate group entries."""
        temp_db.register_group_download('group-abc', 'RLCS 2024', 100)
        temp_db.register_group_download('group-abc', 'RLCS 2024', 150)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM groups WHERE group_id = ?", ('group-abc',))
            count = cursor.fetchone()['count']

            assert count == 1

    def test_updates_download_timestamp(self, temp_db):
        """Should update downloaded_at timestamp on re-registration."""
        with patch('impulse.collection.database.datetime') as mock_datetime:
            # First registration
            first_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = first_time
            temp_db.register_group_download('group-abc', 'RLCS 2024', 100)

            # Second registration
            second_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = second_time
            temp_db.register_group_download('group-abc', 'RLCS 2024', 100)

            with temp_db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT downloaded_at FROM groups WHERE group_id = ?", ('group-abc',))
                row = cursor.fetchone()

                # Should have second timestamp
                assert row['downloaded_at'] == second_time.isoformat()


@pytest.mark.unit
class TestIsReplayDownloaded:
    """Test is_replay_downloaded() method."""

    def test_returns_false_for_nonexistent_replay(self, temp_db):
        """Should return False for replay that doesn't exist."""
        assert temp_db.is_replay_downloaded('nonexistent') is False

    def test_returns_false_for_pending_replay(self, temp_db, sample_metadata):
        """Should return False for replay that hasn't been downloaded."""
        temp_db.add_replay('replay-123', sample_metadata)

        assert temp_db.is_replay_downloaded('replay-123') is False

    def test_returns_true_for_downloaded_replay(self, temp_db, sample_metadata):
        """Should return True for downloaded replay."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_downloaded('replay-123', 's3://bucket/key', 1024)

        assert temp_db.is_replay_downloaded('replay-123') is True

    def test_returns_false_for_failed_replay(self, temp_db, sample_metadata):
        """Should return False for failed replay."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_replay_failed('replay-123', 'Download error')

        assert temp_db.is_replay_downloaded('replay-123') is False


@pytest.mark.unit
class TestMarkDownloaded:
    """Test mark_downloaded() method."""

    def test_marks_replay_as_downloaded(self, temp_db, sample_metadata):
        """Should update replay status to downloaded."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_downloaded('replay-123', 's3://bucket/replay.replay', 2048)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row['is_downloaded'] == 1
            assert row['download_status'] == 'downloaded'
            assert row['s3_key'] == 's3://bucket/replay.replay'
            assert row['file_size_bytes'] == 2048
            assert row['downloaded_at'] is not None

    def test_stores_s3_key(self, temp_db, sample_metadata):
        """Should store S3 key correctly."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_downloaded('replay-123', 'replays/rlcs/2024/replay-123.replay', 1024)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT s3_key FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row['s3_key'] == 'replays/rlcs/2024/replay-123.replay'

    def test_stores_file_size(self, temp_db, sample_metadata):
        """Should store file size in bytes."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_downloaded('replay-123', 's3://key', 123456)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_size_bytes FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row['file_size_bytes'] == 123456

    def test_sets_download_timestamp(self, temp_db, sample_metadata):
        """Should set downloaded_at timestamp."""
        temp_db.add_replay('replay-123', sample_metadata)

        before = datetime.now(timezone.utc)
        temp_db.mark_downloaded('replay-123', 's3://key', 1024)
        after = datetime.now(timezone.utc)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT downloaded_at FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            timestamp = datetime.fromisoformat(row['downloaded_at'])
            assert before <= timestamp <= after


@pytest.mark.unit
class TestMarkReplayFailed:
    """Test mark_replay_failed() method."""

    def test_marks_replay_as_failed(self, temp_db, sample_metadata):
        """Should update replay status to failed."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_replay_failed('replay-123', 'Network error')

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row['download_status'] == 'failed'
            assert row['error_message'] == 'Network error'

    def test_stores_error_message(self, temp_db, sample_metadata):
        """Should store error message."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_replay_failed('replay-123', 'API timeout')

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT error_message FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row['error_message'] == 'API timeout'

    def test_handles_none_error_message(self, temp_db, sample_metadata):
        """Should handle None error message gracefully."""
        temp_db.add_replay('replay-123', sample_metadata)
        temp_db.mark_replay_failed('replay-123', None)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            assert row['download_status'] == 'failed'
            assert row['error_message'] is None


@pytest.mark.unit
class TestGetFailedReplays:
    """Test get_failed_replays() method."""

    def test_returns_empty_list_when_no_failures(self, temp_db):
        """Should return empty list when no replays have failed."""
        failed = temp_db.get_failed_replays()

        assert failed == []

    def test_returns_failed_replays(self, temp_db, sample_metadata):
        """Should return list of failed replays."""
        # Add replays
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)
        temp_db.add_replay('replay-3', sample_metadata)

        # Mark some as failed
        temp_db.mark_replay_failed('replay-1', 'Error 1')
        temp_db.mark_replay_failed('replay-3', 'Error 3')

        # Mark one as downloaded
        temp_db.mark_downloaded('replay-2', 's3://key', 1024)

        failed = temp_db.get_failed_replays()

        assert len(failed) == 2
        failed_ids = {r['replay_id'] for r in failed}
        assert failed_ids == {'replay-1', 'replay-3'}

    def test_includes_error_messages(self, temp_db, sample_metadata):
        """Should include error messages in results."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.mark_replay_failed('replay-1', 'Connection timeout')

        failed = temp_db.get_failed_replays()

        assert len(failed) == 1
        assert failed[0]['error_message'] == 'Connection timeout'

    def test_includes_replay_title(self, temp_db, sample_metadata):
        """Should include replay title in results."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.mark_replay_failed('replay-1', 'Error')

        failed = temp_db.get_failed_replays()

        assert len(failed) == 1
        assert failed[0]['title'] == 'Test Match'


@pytest.mark.unit
class TestGetStats:
    """Test get_stats() method."""

    def test_returns_zero_stats_for_empty_database(self, temp_db):
        """Should return zeros when database is empty."""
        stats = temp_db.get_stats()

        assert stats['total_replays'] == 0
        assert stats['downloaded'] == 0
        assert stats['failed'] == 0
        assert stats['pending'] == 0
        assert stats['storage_mb'] == 0
        assert stats['storage_gb'] == 0

    def test_calculates_total_replays(self, temp_db, sample_metadata):
        """Should calculate total number of replays."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)
        temp_db.add_replay('replay-3', sample_metadata)

        stats = temp_db.get_stats()

        assert stats['total_replays'] == 3

    def test_calculates_downloaded_count(self, temp_db, sample_metadata):
        """Should calculate number of downloaded replays."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)
        temp_db.add_replay('replay-3', sample_metadata)

        temp_db.mark_downloaded('replay-1', 's3://key1', 1024)
        temp_db.mark_downloaded('replay-2', 's3://key2', 2048)

        stats = temp_db.get_stats()

        assert stats['downloaded'] == 2

    def test_calculates_failed_count(self, temp_db, sample_metadata):
        """Should calculate number of failed replays."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)
        temp_db.add_replay('replay-3', sample_metadata)

        temp_db.mark_replay_failed('replay-1', 'Error')

        stats = temp_db.get_stats()

        assert stats['failed'] == 1

    def test_calculates_pending_count(self, temp_db, sample_metadata):
        """Should calculate number of pending replays."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)
        temp_db.add_replay('replay-3', sample_metadata)

        temp_db.mark_downloaded('replay-1', 's3://key', 1024)
        temp_db.mark_replay_failed('replay-2', 'Error')

        stats = temp_db.get_stats()

        assert stats['pending'] == 1

    def test_calculates_storage_size(self, temp_db, sample_metadata):
        """Should calculate total storage size."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)

        # 100 MB and 200 MB files
        temp_db.mark_downloaded('replay-1', 's3://key1', 100 * 1024 * 1024)
        temp_db.mark_downloaded('replay-2', 's3://key2', 200 * 1024 * 1024)

        stats = temp_db.get_stats()

        assert stats['storage_mb'] == 300.0
        assert stats['storage_gb'] == 0.29  # 300 MB ≈ 0.29 GB (rounded to 2 decimals)

    def test_only_counts_downloaded_file_sizes(self, temp_db, sample_metadata):
        """Should only count file sizes for downloaded replays."""
        temp_db.add_replay('replay-1', sample_metadata)
        temp_db.add_replay('replay-2', sample_metadata)

        temp_db.mark_downloaded('replay-1', 's3://key1', 1024 * 1024)
        # replay-2 remains pending

        stats = temp_db.get_stats()

        assert stats['storage_mb'] == 1.0


@pytest.mark.unit
class TestGetConnection:
    """Test get_connection() context manager."""

    def test_commits_on_success(self, temp_db, sample_metadata):
        """Should commit transaction on success."""
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO replays (replay_id, title, is_downloaded)
                VALUES (?, ?, 0)
            """, ('test-replay', 'Test'))

        # Verify commit happened
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('test-replay',))
            assert cursor.fetchone() is not None

    def test_rolls_back_on_error(self, temp_db):
        """Should rollback transaction on error."""
        with pytest.raises(Exception):
            with temp_db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO replays (replay_id, title, is_downloaded)
                    VALUES (?, ?, 0)
                """, ('test-replay', 'Test'))

                # Force an error
                raise Exception("Simulated error")

        # Verify rollback happened
        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('test-replay',))
            assert cursor.fetchone() is None

    def test_closes_connection(self, temp_db):
        """Should close connection after context exits."""
        conn_ref = None

        with temp_db.get_connection() as conn:
            conn_ref = conn

        # Connection should be closed
        with pytest.raises(Exception):
            conn_ref.execute("SELECT 1")

    def test_returns_rows_as_dicts(self, temp_db, sample_metadata):
        """Should return rows as dict-like objects."""
        temp_db.add_replay('replay-123', sample_metadata)

        with temp_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM replays WHERE replay_id = ?", ('replay-123',))
            row = cursor.fetchone()

            # Should be able to access by column name
            assert row['replay_id'] == 'replay-123'
            assert row['title'] == 'Test Match'
