"""
Smoke tests to verify pytest infrastructure is working.

These tests don't test actual functionality - they just verify:
- pytest runs
- fixtures work
- imports work
- basic setup is correct

Run with: pytest tests/test_smoke.py -v
"""

import pytest
from pathlib import Path

# Test that imports work
from impulse.collection.database import ImpulseDB
from impulse.collection.storage import LocalBackend, S3Backend
from impulse.collection.ballchasing_client import BallchasingClient
from impulse.collection.rate_limiter import RateLimiter
from impulse.config.collection_config import CollectionConfig
from impulse.collection.utils import (
    sanitize_path_component,
    flatten_group_tree,
    extract_replay_metadata
)


class TestImports:
    """Verify all modules can be imported."""

    def test_imports_succeed(self):
        """Basic smoke test - if we got here, imports worked."""
        assert True


class TestFixtures:
    """Verify pytest fixtures work correctly."""

    def test_test_config_fixture(self, test_config):
        """Test that test_config fixture provides a config object."""
        assert isinstance(test_config, CollectionConfig)
        assert test_config.ballchasing_api_key == "test_fake_api_key_12345"
        assert test_config.rate_limit_per_hour == 200

    def test_temp_database_fixture(self, temp_database):
        """Test that temp_database fixture provides a working database."""
        assert isinstance(temp_database, ImpulseDB)
        assert Path(temp_database.db_path).exists()

        # Verify database has tables
        with temp_database.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            assert 'groups' in tables
            assert 'replays' in tables

    def test_temp_database_with_sample_data_fixture(self, temp_database_with_sample_data):
        """Test that sample data fixture provides populated database."""
        db = temp_database_with_sample_data

        # Check that sample data exists
        stats = db.get_stats()
        assert stats['total_replays'] == 10
        assert stats['downloaded'] == 5
        assert stats['failed'] == 1
        assert stats['pending'] == 4

    def test_temp_local_storage_fixture(self, temp_local_storage):
        """Test that temp_local_storage fixture provides a LocalBackend."""
        assert isinstance(temp_local_storage, LocalBackend)
        assert Path(temp_local_storage.base_dir).exists()

    def test_mock_ballchasing_client_fixture(self, mock_ballchasing_client):
        """Test that mock client fixture is properly configured."""
        # Verify it's a mock
        assert hasattr(mock_ballchasing_client, 'get_group_info')

        # Verify mock responses
        group_info = mock_ballchasing_client.get_group_info('test-id')
        assert group_info['id'] == 'test-group-123'
        assert group_info['name'] == 'Test RLCS Group'

        replays = mock_ballchasing_client.get_replays_from_group('test-id')
        assert len(replays) == 2
        assert replays[0]['id'] == 'replay-1'

    def test_sample_replay_metadata_fixture(self, sample_replay_metadata):
        """Test that sample metadata fixture provides valid data."""
        assert 'id' in sample_replay_metadata
        assert 'blue' in sample_replay_metadata
        assert 'orange' in sample_replay_metadata
        assert sample_replay_metadata['blue']['name'] == 'Team Vitality'

    def test_sample_group_tree_fixture(self, sample_group_tree):
        """Test that sample group tree fixture provides valid structure."""
        assert 'id' in sample_group_tree
        assert 'name' in sample_group_tree
        assert 'children' in sample_group_tree
        assert 'replays' in sample_group_tree
        assert len(sample_group_tree['children']) == 2


class TestPytestMarkers:
    """Verify pytest markers are working."""

    @pytest.mark.unit
    def test_unit_marker_works(self):
        """Test that @pytest.mark.unit works."""
        assert True

    @pytest.mark.integration
    def test_integration_marker_works(self):
        """Test that @pytest.mark.integration works."""
        assert True

    @pytest.mark.slow
    def test_slow_marker_works(self):
        """Test that @pytest.mark.slow works."""
        assert True


class TestBasicFunctionality:
    """Verify basic module functionality works."""

    def test_sanitize_path_component_works(self):
        """Smoke test for path sanitization utility."""
        result = sanitize_path_component("Test: Name")
        assert result == "Test_ Name"

    def test_database_basic_operations_work(self, temp_database):
        """Smoke test for database operations."""
        db = temp_database

        # Add a replay
        replay_data = {
            'id': 'test-replay',
            'replay_title': 'Test Match',
            'blue': {'name': 'Blue Team'},
            'orange': {'name': 'Orange Team'}
        }
        is_new = db.add_replay('test-replay', replay_data)
        assert is_new is True

        # Check if exists
        is_downloaded = db.is_replay_downloaded('test-replay')
        assert is_downloaded is False

        # Mark as downloaded
        db.mark_downloaded('test-replay', 's3://bucket/test.replay', 1000)

        # Verify
        is_downloaded = db.is_replay_downloaded('test-replay')
        assert is_downloaded is True

    def test_config_from_dict_works(self):
        """Smoke test for config creation."""
        config = CollectionConfig.from_dict({
            'ballchasing_api_key': 'test-key',
            'database_path': './test.db'
        })
        assert config.ballchasing_api_key == 'test-key'
        assert config.database_path == './test.db'

    def test_local_backend_basic_operations_work(self, temp_local_storage):
        """Smoke test for local storage backend."""
        storage = temp_local_storage

        # Save a replay
        result = storage.save_replay(
            replay_id='test-replay',
            data=b'fake replay content',
            path_components=['test', 'path'],
            metadata={'source': 'test'}
        )

        assert result['success'] is True
        assert 'storage_key' in result

        # Check if exists
        exists = storage.replay_exists('test-replay', ['test', 'path'])
        assert exists is True

        # Get size
        size = storage.get_replay_size('test-replay', ['test', 'path'])
        assert size == len(b'fake replay content')


class TestTmpPathFixture:
    """Verify pytest's tmp_path fixture works (used by our fixtures)."""

    def test_tmp_path_creates_directory(self, tmp_path):
        """Test that tmp_path provides a temporary directory."""
        assert tmp_path.exists()
        assert tmp_path.is_dir()

        # Create a file in it
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        assert test_file.exists()

        # Verify we can read it
        content = test_file.read_text()
        assert content == "test content"


# Run this file directly for quick smoke test
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
