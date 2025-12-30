"""
Shared pytest fixtures for all tests.

This file is automatically loaded by pytest and makes fixtures available
to all test files without needing to import them.
"""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock

from impulse.collection.database import ImpulseDB
from impulse.collection.ballchasing_client import BallchasingClient
from impulse.collection.storage import LocalBackend, S3Backend
from impulse.collection.rate_limiter import RateLimiter
from impulse.config.collection_config import CollectionConfig


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def test_config():
    """Provide a test configuration with fake credentials."""
    return CollectionConfig(
        ballchasing_api_key="test_fake_api_key_12345",
        aws_region="us-east-1",
        s3_bucket_name="test-impulse-bucket",
        database_path="./test_impulse.db",
        rate_limit_per_second=1.0,
        rate_limit_per_hour=200
    )


@pytest.fixture
def test_config_with_real_aws():
    """Provide config with real AWS credentials from environment."""
    return CollectionConfig.from_env()


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest.fixture
def temp_database(tmp_path):
    """
    Provide a temporary database that is automatically cleaned up.

    Each test gets a fresh database in a unique temporary directory.
    """
    db_path = tmp_path / "test_impulse.db"
    db = ImpulseDB(str(db_path))
    yield db
    # Cleanup happens automatically when tmp_path is deleted


@pytest.fixture
def temp_database_with_sample_data(temp_database):
    """
    Provide a temporary database pre-populated with sample data.
    """
    db = temp_database

    # Add sample group
    db.register_group_download(
        group_id="test-group-123",
        name="Test RLCS Group",
        replay_count=10
    )

    # Add sample replays
    for i in range(10):
        replay_data = {
            'id': f'replay-{i}',
            'replay_title': f'Match {i}',
            'blue': {'name': 'Team Blue'},
            'orange': {'name': 'Team Orange'},
            'date': '2024-01-01T10:00:00'
        }
        db.add_replay(f'replay-{i}', replay_data)

    # Mark some as downloaded
    for i in range(5):
        db.mark_downloaded(
            f'replay-{i}',
            f's3://bucket/replays/replay-{i}.replay',
            1000000 + i
        )

    # Mark one as failed
    db.mark_replay_failed('replay-9', 'Network error')

    return db


# ============================================================================
# Storage Fixtures
# ============================================================================

@pytest.fixture
def temp_local_storage(tmp_path):
    """
    Provide a LocalBackend using a temporary directory.

    Automatically cleaned up after test.
    """
    storage_dir = tmp_path / "replays"
    backend = LocalBackend(base_dir=str(storage_dir))
    yield backend
    # Cleanup happens automatically when tmp_path is deleted


@pytest.fixture
def s3_test_backend(test_config_with_real_aws):
    """
    Provide an S3Backend for testing with real AWS.

    Uses test prefix 'test-data/pytest/' for isolation.
    Automatically cleans up test objects after test.
    """
    backend = S3Backend(
        aws_region=test_config_with_real_aws.aws_region,
        s3_bucket_name=test_config_with_real_aws.s3_bucket_name
    )

    test_prefix = "test-data/pytest"

    yield backend, test_prefix

    # Cleanup: Delete all test objects
    try:
        _cleanup_s3_test_objects(backend, test_prefix)
    except Exception as e:
        print(f"Warning: S3 cleanup failed: {e}")


def _cleanup_s3_test_objects(backend: S3Backend, prefix: str):
    """Helper to delete all objects under test prefix."""
    objects = backend.s3_manager.list_objects(prefix=prefix, max_keys=1000)

    for obj_key in objects:
        try:
            backend.s3_manager.s3_client.delete_object(
                Bucket=backend.bucket_name,
                Key=obj_key
            )
        except Exception as e:
            print(f"Failed to delete {obj_key}: {e}")


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_ballchasing_client():
    """
    Provide a mock BallchasingClient for testing without real API calls.

    Pre-configured with common responses.
    """
    client = Mock(spec=BallchasingClient)

    # Mock get_group_info
    client.get_group_info.return_value = {
        'id': 'test-group-123',
        'name': 'Test RLCS Group',
        'created': '2024-01-01T00:00:00',
    }

    # Mock get_child_groups
    client.get_child_groups.return_value = [
        {'id': 'child-1', 'name': 'Child Group 1'},
        {'id': 'child-2', 'name': 'Child Group 2'},
    ]

    # Mock get_replays_from_group
    client.get_replays_from_group.return_value = [
        {
            'id': 'replay-1',
            'replay_title': 'Match 1',
            'blue': {'name': 'Team Blue'},
            'orange': {'name': 'Team Orange'},
            'date': '2024-01-01T10:00:00'
        },
        {
            'id': 'replay-2',
            'replay_title': 'Match 2',
            'blue': {'name': 'Team Blue'},
            'orange': {'name': 'Team Orange'},
            'date': '2024-01-01T11:00:00'
        },
    ]

    # Mock download_replay_bytes
    client.download_replay_bytes.return_value = b"fake replay file content"

    # Mock build_group_tree
    client.build_group_tree.return_value = {
        'id': 'test-group-123',
        'name': 'Test RLCS Group',
        'children': [],
        'replays': [
            {'id': 'replay-1', 'replay_title': 'Match 1'},
            {'id': 'replay-2', 'replay_title': 'Match 2'},
        ]
    }

    return client


@pytest.fixture
def mock_rate_limiter():
    """Provide a mock RateLimiter that doesn't actually wait."""
    limiter = Mock(spec=RateLimiter)
    limiter.wait_if_needed.return_value = None
    limiter.get_status.return_value = {
        'requests_this_hour': 0,
        'requests_remaining': 200,
        'window_resets_in_seconds': 3600,
        'window_resets_in_minutes': 60.0
    }
    return limiter


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture
def sample_replay_metadata():
    """Provide sample replay metadata from Ballchasing API."""
    return {
        'id': 'abc123def456',
        'replay_title': 'RLCS Grand Finals - Game 5',
        'blue': {
            'name': 'Team Vitality',
            'players': [
                {'name': 'Kaydop'},
                {'name': 'Fairy Peak'},
                {'name': 'Alpha54'}
            ]
        },
        'orange': {
            'name': 'G2 Esports',
            'players': [
                {'name': 'JKnaps'},
                {'name': 'Chicago'},
                {'name': 'Atomic'}
            ]
        },
        'date': '2024-12-01T20:30:00',
        'duration': 300,
        'playlist_name': 'Ranked Standard',
    }


@pytest.fixture
def sample_group_tree():
    """Provide sample group tree structure."""
    return {
        'id': 'root-group',
        'name': 'RLCS 2024',
        'children': [
            {
                'id': 'worlds-group',
                'name': 'Worlds',
                'children': [],
                'replays': [
                    {'id': 'replay-1', 'replay_title': 'Match 1'},
                    {'id': 'replay-2', 'replay_title': 'Match 2'},
                ]
            },
            {
                'id': 'regional-group',
                'name': 'Regional 1',
                'children': [],
                'replays': [
                    {'id': 'replay-3', 'replay_title': 'Match 3'},
                ]
            }
        ],
        'replays': []
    }


# ============================================================================
# Path Fixtures
# ============================================================================

@pytest.fixture
def sample_path_components():
    """Provide sample path components for testing."""
    return ['replays', 'rlcs', '2024', 'worlds', 'day-1']
