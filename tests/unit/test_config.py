"""
Unit tests for impulse.config.collection_config module.

Tests for configuration loading, validation, and creation.
"""

import pytest
import os
from unittest.mock import patch
from impulse.config.collection_config import CollectionConfig


@pytest.mark.unit
class TestCollectionConfigFromDict:
    """Test creating config from dictionary."""

    def test_creates_config_with_all_fields(self):
        """Config should be created with all provided fields."""
        config_dict = {
            'ballchasing_api_key': 'test-key-123',
            'aws_region': 'us-west-2',
            's3_bucket_name': 'my-bucket',
            'database_path': './custom.db',
            'rate_limit_per_second': 2.0,
            'rate_limit_per_hour': 400
        }

        config = CollectionConfig.from_dict(config_dict)

        assert config.ballchasing_api_key == 'test-key-123'
        assert config.aws_region == 'us-west-2'
        assert config.s3_bucket_name == 'my-bucket'
        assert config.database_path == './custom.db'
        assert config.rate_limit_per_second == 2.0
        assert config.rate_limit_per_hour == 400

    def test_creates_config_with_minimal_fields(self):
        """Config should work with only required field."""
        config_dict = {
            'ballchasing_api_key': 'test-key-123'
        }

        config = CollectionConfig.from_dict(config_dict)

        assert config.ballchasing_api_key == 'test-key-123'
        assert config.aws_region is None
        assert config.s3_bucket_name is None
        assert config.database_path == './impulse.db'  # Default
        assert config.rate_limit_per_second == 1.0  # Default
        assert config.rate_limit_per_hour == 200  # Default

    def test_uses_default_values(self):
        """Unspecified fields should use default values."""
        config = CollectionConfig.from_dict({'ballchasing_api_key': 'key'})

        assert config.database_path == './impulse.db'
        assert config.rate_limit_per_second == 1.0
        assert config.rate_limit_per_hour == 200


@pytest.mark.unit
class TestCollectionConfigFromEnv:
    """Test creating config from environment variables."""

    def test_loads_from_environment(self):
        """Config should load from environment variables."""
        env_vars = {
            'BALLCHASING_API_KEY': 'env-key-456',
            'AWS_REGION': 'eu-west-1',
            'S3_BUCKET_NAME': 'env-bucket'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = CollectionConfig.from_env()

            assert config.ballchasing_api_key == 'env-key-456'
            assert config.aws_region == 'eu-west-1'
            assert config.s3_bucket_name == 'env-bucket'

    def test_loads_with_only_required_env_var(self):
        """Config should work with only BALLCHASING_API_KEY set."""
        env_vars = {
            'BALLCHASING_API_KEY': 'only-key'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = CollectionConfig.from_env()

            assert config.ballchasing_api_key == 'only-key'
            assert config.aws_region is None
            assert config.s3_bucket_name is None

    def test_raises_error_when_api_key_missing(self):
        """Should raise ValueError when BALLCHASING_API_KEY is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                CollectionConfig.from_env()

            assert 'BALLCHASING_API_KEY' in str(exc_info.value)
            assert 'not found' in str(exc_info.value)

    def test_aws_vars_are_optional(self):
        """AWS variables should be optional (only needed for S3)."""
        env_vars = {
            'BALLCHASING_API_KEY': 'key-123'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = CollectionConfig.from_env()

            # Should not raise error
            assert config.aws_region is None
            assert config.s3_bucket_name is None


@pytest.mark.unit
class TestCollectionConfigValidation:
    """Test config validation methods."""

    def test_validate_for_s3_passes_with_aws_vars(self):
        """Validation should pass when AWS vars are set."""
        config = CollectionConfig(
            ballchasing_api_key='key',
            aws_region='us-east-1',
            s3_bucket_name='my-bucket'
        )

        # Should not raise
        config.validate_for_s3()

    def test_validate_for_s3_fails_without_region(self):
        """Validation should fail when aws_region is missing."""
        config = CollectionConfig(
            ballchasing_api_key='key',
            aws_region=None,
            s3_bucket_name='my-bucket'
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate_for_s3()

        assert 'AWS_REGION' in str(exc_info.value)

    def test_validate_for_s3_fails_without_bucket(self):
        """Validation should fail when s3_bucket_name is missing."""
        config = CollectionConfig(
            ballchasing_api_key='key',
            aws_region='us-east-1',
            s3_bucket_name=None
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate_for_s3()

        assert 'S3_BUCKET_NAME' in str(exc_info.value)

    def test_validate_for_s3_fails_without_both(self):
        """Validation should fail when both AWS vars are missing."""
        config = CollectionConfig(
            ballchasing_api_key='key',
            aws_region=None,
            s3_bucket_name=None
        )

        with pytest.raises(ValueError):
            config.validate_for_s3()


@pytest.mark.unit
class TestCollectionConfigRepresentation:
    """Test config string representation."""

    def test_repr_masks_api_key(self):
        """String representation should mask API key for security."""
        config = CollectionConfig(
            ballchasing_api_key='secret-key-12345',
            aws_region='us-east-1'
        )

        repr_str = repr(config)

        # API key should be masked
        assert 'secret-key-12345' not in repr_str
        assert '2345' in repr_str  # Last 4 chars shown

        # Other fields should be visible
        assert 'us-east-1' in repr_str

    def test_repr_handles_none_api_key(self):
        """Repr should handle None API key gracefully."""
        config = CollectionConfig(
            ballchasing_api_key=None,
            aws_region='us-east-1'
        )

        repr_str = repr(config)

        # Should not crash
        assert 'ballchasing_api_key' in repr_str

    def test_repr_shows_all_config_fields(self):
        """Repr should show all configuration fields."""
        config = CollectionConfig(
            ballchasing_api_key='test-key',
            aws_region='us-east-1',
            s3_bucket_name='my-bucket',
            database_path='./test.db',
            rate_limit_per_second=2.0,
            rate_limit_per_hour=400
        )

        repr_str = repr(config)

        assert 'aws_region' in repr_str
        assert 's3_bucket_name' in repr_str
        assert 'database_path' in repr_str
        assert 'rate_limits' in repr_str or 'rate_limit' in repr_str


@pytest.mark.unit
class TestCollectionConfigDataclass:
    """Test dataclass behavior of CollectionConfig."""

    def test_is_dataclass(self):
        """Config should be a dataclass."""
        from dataclasses import is_dataclass
        assert is_dataclass(CollectionConfig)

    def test_supports_equality(self):
        """Two configs with same values should be equal."""
        config1 = CollectionConfig(ballchasing_api_key='key', aws_region='us-east-1')
        config2 = CollectionConfig(ballchasing_api_key='key', aws_region='us-east-1')

        assert config1 == config2

    def test_different_configs_not_equal(self):
        """Configs with different values should not be equal."""
        config1 = CollectionConfig(ballchasing_api_key='key1')
        config2 = CollectionConfig(ballchasing_api_key='key2')

        assert config1 != config2

    def test_can_access_fields_as_attributes(self):
        """Config fields should be accessible as attributes."""
        config = CollectionConfig(
            ballchasing_api_key='key',
            aws_region='us-east-1'
        )

        assert config.ballchasing_api_key == 'key'
        assert config.aws_region == 'us-east-1'


@pytest.mark.unit
class TestCollectionConfigDefaults:
    """Test default values."""

    def test_default_database_path(self):
        """Default database path should be ./impulse.db."""
        config = CollectionConfig(ballchasing_api_key='key')
        assert config.database_path == './impulse.db'

    def test_default_rate_limit_per_second(self):
        """Default rate limit should be 1.0 req/sec."""
        config = CollectionConfig(ballchasing_api_key='key')
        assert config.rate_limit_per_second == 1.0

    def test_default_rate_limit_per_hour(self):
        """Default rate limit should be 200 req/hour."""
        config = CollectionConfig(ballchasing_api_key='key')
        assert config.rate_limit_per_hour == 200

    def test_aws_fields_default_to_none(self):
        """AWS fields should default to None (optional)."""
        config = CollectionConfig(ballchasing_api_key='key')
        assert config.aws_region is None
        assert config.s3_bucket_name is None
