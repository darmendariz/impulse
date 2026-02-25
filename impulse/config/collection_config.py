"""
Configuration management for the collection module.

Centralizes environment variable loading and configuration management.
Supports both environment-based and programmatic configuration for testing.
"""

from dotenv import load_dotenv
import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class CollectionConfig:
    """
    Configuration for the Impulse collection module.

    Attributes:
        ballchasing_api_key: API key for Ballchasing.com
        aws_region: AWS region for S3 storage
        s3_bucket_name: S3 bucket name for replay storage
        database_path: Path to SQLite database file
        rate_limit_per_second: Max requests per second to Ballchasing API
        rate_limit_per_hour: Max requests per hour to Ballchasing API
    """

    ballchasing_api_key: str
    aws_region: Optional[str] = None
    s3_bucket_name: Optional[str] = None
    database_path: str = "./impulse.db"
    rate_limit_per_second: int = 1
    rate_limit_per_hour: int = 200

    @classmethod
    def from_env(cls) -> 'CollectionConfig':
        """
        Load configuration from environment variables (.env file).

        Required environment variables:
            - BALLCHASING_API_KEY: Your Ballchasing API key

        Optional environment variables:
            - AWS_REGION: AWS region (required for S3 storage)
            - S3_BUCKET_NAME: S3 bucket name (required for S3 storage)

        Returns:
            CollectionConfig instance

        Raises:
            ValueError: If required environment variables are missing
        """
        load_dotenv()

        ballchasing_api_key = os.environ.get("BALLCHASING_API_KEY")
        if not ballchasing_api_key:
            raise ValueError(
                "BALLCHASING_API_KEY not found in environment variables. "
                "Please add it to your .env file in the project root."
            )

        # AWS credentials are optional (only needed for S3 backend)
        aws_region = os.environ.get("AWS_REGION")
        s3_bucket_name = os.environ.get("S3_BUCKET_NAME")

        return cls(
            ballchasing_api_key=ballchasing_api_key,
            aws_region=aws_region,
            s3_bucket_name=s3_bucket_name
        )

    @classmethod
    def from_dict(cls, config_dict: dict) -> 'CollectionConfig':
        """
        Create configuration from a dictionary.

        Useful for testing or programmatic configuration.

        Args:
            config_dict: Dictionary with configuration values

        Returns:
            CollectionConfig instance

        Example:
            >>> config = CollectionConfig.from_dict({
            ...     'ballchasing_api_key': 'test_key',
            ...     'database_path': './test.db'
            ... })
        """
        return cls(**config_dict)

    def validate_for_s3(self) -> None:
        """
        Validate that required S3 configuration is present.

        Raises:
            ValueError: If AWS_REGION or S3_BUCKET_NAME is missing
        """
        if not self.aws_region:
            raise ValueError(
                "AWS_REGION not configured. Required for S3 storage backend."
            )
        if not self.s3_bucket_name:
            raise ValueError(
                "S3_BUCKET_NAME not configured. Required for S3 storage backend."
            )

    def __repr__(self) -> str:
        """String representation with masked API key."""
        return (
            f"CollectionConfig(\n"
            f"  ballchasing_api_key=***{self.ballchasing_api_key[-4:] if self.ballchasing_api_key else 'None'},\n"
            f"  aws_region={self.aws_region},\n"
            f"  s3_bucket_name={self.s3_bucket_name},\n"
            f"  database_path={self.database_path},\n"
            f"  rate_limits={self.rate_limit_per_second}/sec, {self.rate_limit_per_hour}/hour\n"
            f")"
        )
