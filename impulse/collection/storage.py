"""
Storage backends for replay file storage.

Provides a unified interface for different storage backends (local filesystem, S3, etc.)
All backends implement the StorageBackend abstract base class.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, BinaryIO
from pathlib import Path
import io

# Import existing managers (will be wrapped)
from impulse.collection.s3_manager import S3Manager


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.

    All storage backends must implement these methods to provide a consistent
    interface for saving, retrieving, and managing replay files.
    """

    @abstractmethod
    def save_replay(self, replay_id: str, data: bytes, path_components: List[str],
                    metadata: Optional[Dict] = None) -> Dict:
        """
        Save a replay file to storage.

        Args:
            replay_id: Unique replay identifier
            data: Raw replay file bytes
            path_components: List of path components for hierarchical organization
                           e.g., ['replays', 'rlcs', '2024', 'worlds']
            metadata: Optional metadata to attach to the file

        Returns:
            Dict with 'success' (bool), 'storage_key' (str), 'size_bytes' (int)
        """
        pass

    @abstractmethod
    def replay_exists(self, replay_id: str, path_components: List[str]) -> bool:
        """
        Check if a replay file exists in storage.

        Args:
            replay_id: Unique replay identifier
            path_components: Path components where replay should be located

        Returns:
            True if replay exists
        """
        pass

    @abstractmethod
    def get_replay_size(self, replay_id: str, path_components: List[str]) -> int:
        """
        Get the size of a stored replay file in bytes.

        Args:
            replay_id: Unique replay identifier
            path_components: Path components where replay is located

        Returns:
            Size in bytes, or 0 if not found
        """
        pass

    @abstractmethod
    def list_replays(self, path_prefix: List[str]) -> List[str]:
        """
        List all replay IDs under a given path prefix.

        Args:
            path_prefix: Path components to search under

        Returns:
            List of replay IDs
        """
        pass

    @abstractmethod
    def get_storage_stats(self, path_prefix: List[str]) -> Dict:
        """
        Get storage statistics for replays under a path prefix.

        Args:
            path_prefix: Path components to calculate stats for

        Returns:
            Dict with 'total_replays', 'total_bytes', 'total_mb', 'total_gb'
        """
        pass

    @abstractmethod
    def get_storage_key(self, replay_id: str, path_components: List[str]) -> str:
        """
        Get the full storage key/path for a replay.

        Args:
            replay_id: Unique replay identifier
            path_components: Path components

        Returns:
            Full storage key (S3 key, file path, etc.)
        """
        pass


class LocalBackend(StorageBackend):
    """
    Local filesystem storage backend.

    Stores replay files in a local directory structure.
    """

    def __init__(self, base_dir: str = "./replays"):
        """
        Initialize local storage backend.

        Args:
            base_dir: Base directory for storing replays
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save_replay(self, replay_id: str, data: bytes, path_components: List[str],
                   metadata: Optional[Dict] = None) -> Dict:
        """Save replay to local filesystem."""
        try:
            # Build full path
            replay_dir = self.base_dir / Path(*path_components)
            replay_dir.mkdir(parents=True, exist_ok=True)

            filepath = replay_dir / f"{replay_id}.replay"

            # Write file
            filepath.write_bytes(data)

            # Optionally save metadata as JSON sidecar
            if metadata:
                import json
                metadata_path = replay_dir / f"{replay_id}.metadata.json"
                metadata_path.write_text(json.dumps(metadata, indent=2))

            return {
                'success': True,
                'storage_key': str(filepath.relative_to(self.base_dir)),
                'size_bytes': len(data),
                'full_path': str(filepath)
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'storage_key': None,
                'size_bytes': 0
            }

    def replay_exists(self, replay_id: str, path_components: List[str]) -> bool:
        """Check if replay exists locally."""
        filepath = self.base_dir / Path(*path_components) / f"{replay_id}.replay"
        return filepath.exists()

    def get_replay_size(self, replay_id: str, path_components: List[str]) -> int:
        """Get replay file size."""
        filepath = self.base_dir / Path(*path_components) / f"{replay_id}.replay"
        if filepath.exists():
            return filepath.stat().st_size
        return 0

    def list_replays(self, path_prefix: List[str]) -> List[str]:
        """List all replay IDs under a path prefix."""
        search_dir = self.base_dir / Path(*path_prefix)
        if not search_dir.exists():
            return []

        replay_ids = []
        for replay_file in search_dir.rglob("*.replay"):
            # Extract replay ID (filename without extension)
            replay_ids.append(replay_file.stem)

        return replay_ids

    def get_storage_stats(self, path_prefix: List[str]) -> Dict:
        """Get storage statistics."""
        search_dir = self.base_dir / Path(*path_prefix)
        if not search_dir.exists():
            return {
                'total_replays': 0,
                'total_bytes': 0,
                'total_mb': 0.0,
                'total_gb': 0.0
            }

        total_bytes = 0
        total_replays = 0

        for replay_file in search_dir.rglob("*.replay"):
            total_bytes += replay_file.stat().st_size
            total_replays += 1

        return {
            'total_replays': total_replays,
            'total_bytes': total_bytes,
            'total_mb': round(total_bytes / (1024**2), 2),
            'total_gb': round(total_bytes / (1024**3), 2)
        }

    def get_storage_key(self, replay_id: str, path_components: List[str]) -> str:
        """Get full file path."""
        filepath = self.base_dir / Path(*path_components) / f"{replay_id}.replay"
        return str(filepath.relative_to(self.base_dir))


class S3Backend(StorageBackend):
    """
    AWS S3 storage backend.

    Wraps the existing S3Manager to provide the StorageBackend interface.
    """

    def __init__(self, s3_manager: S3Manager = None, aws_region: str = None,
                 s3_bucket_name: str = None):
        """
        Initialize S3 storage backend.

        Args:
            s3_manager: Optional existing S3Manager instance (for dependency injection)
            aws_region: AWS region (defaults to env var, ignored if s3_manager provided)
            s3_bucket_name: S3 bucket name (defaults to env var, ignored if s3_manager provided)
        """
        if s3_manager:
            self.s3_manager = s3_manager
        else:
            self.s3_manager = S3Manager(aws_region, s3_bucket_name)
            # Ensure bucket exists
            self.s3_manager.create_bucket_if_needed()

        self.bucket_name = self.s3_manager.s3_bucket_name

    def save_replay(self, replay_id: str, data: bytes, path_components: List[str],
                   metadata: Optional[Dict] = None) -> Dict:
        """Save replay to S3."""
        # Build S3 key from path components
        s3_key = '/'.join(path_components) + f'/{replay_id}.replay'

        # Upload to S3
        result = self.s3_manager.upload_bytes(data, s3_key, metadata)

        return result

    def replay_exists(self, replay_id: str, path_components: List[str]) -> bool:
        """Check if replay exists in S3."""
        s3_key = '/'.join(path_components) + f'/{replay_id}.replay'
        return self.s3_manager.object_exists(s3_key)

    def get_replay_size(self, replay_id: str, path_components: List[str]) -> int:
        """Get replay file size from S3."""
        s3_key = '/'.join(path_components) + f'/{replay_id}.replay'
        return self.s3_manager.get_object_size(s3_key)

    def list_replays(self, path_prefix: List[str]) -> List[str]:
        """List all replay IDs under a path prefix in S3."""
        prefix = '/'.join(path_prefix) + '/' if path_prefix else ''

        # Get all objects with this prefix
        objects = self.s3_manager.list_objects(prefix, max_keys=10000)

        # Extract replay IDs
        replay_ids = []
        for obj_key in objects:
            if obj_key.endswith('.replay'):
                # Extract filename
                filename = obj_key.split('/')[-1]
                replay_id = filename.replace('.replay', '')
                replay_ids.append(replay_id)

        return replay_ids

    def get_storage_stats(self, path_prefix: List[str]) -> Dict:
        """Get S3 storage statistics."""
        prefix = '/'.join(path_prefix) + '/' if path_prefix else ''
        return self.s3_manager.get_storage_stats(prefix)

    def get_storage_key(self, replay_id: str, path_components: List[str]) -> str:
        """Get S3 key."""
        return '/'.join(path_components) + f'/{replay_id}.replay'

    def backup_database(self, db_path: str, backup_prefix: str = "database-backups") -> Dict:
        """
        Convenience method to backup database to S3.

        Args:
            db_path: Path to local database file
            backup_prefix: S3 prefix for backups

        Returns:
            Dict with backup info
        """
        return self.s3_manager.backup_database(db_path, backup_prefix)
