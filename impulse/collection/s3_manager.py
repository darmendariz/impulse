"""
S3 Module for Impulse
Handles all S3 operations including streaming replay uploads and database backups
"""

from dotenv import load_dotenv
import os
import boto3
from typing import Dict, BinaryIO
from pathlib import Path
import io


class S3Manager:
    """Manages S3 uploads, downloads, and database backups"""
    
    def __init__(self, aws_region: str = None, s3_bucket_name: str = None):
        """
        Initialize S3 manager with bucket and region info.
        Automatically detects EC2 (IAM role) vs local (credentials).
        
        Args:
            aws_region: AWS region (default: from .env)
            s3_bucket_name: S3 bucket name (default: from .env)
        """
        self.aws_region = aws_region if aws_region else self._get_env_var("AWS_REGION")
        self.s3_bucket_name = s3_bucket_name if s3_bucket_name else self._get_env_var("S3_BUCKET_NAME")
        
        # Initialize S3 client
        # On EC2: Uses IAM role automatically
        # Local: Uses credentials from ~/.aws/credentials or environment variables
        try:
            self.s3_client = boto3.client('s3', region_name=self.aws_region)
            
            # Test credentials by trying to list buckets
            self.s3_client.list_buckets()
            
            # Determine if we're on EC2 or local
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                # Check if it's an assumed role (EC2) or user credentials (local)
                if hasattr(credentials, 'method') and credentials.method == 'iam-role':
                    print(f"✓ S3 Manager initialized (EC2 IAM Role)")
                else:
                    print(f"✓ S3 Manager initialized (Local Credentials)")
            
            print(f"  Region: {self.aws_region}")
            print(f"  Bucket: {self.s3_bucket_name}")
            
        except Exception as e:
            print(f"\n✗ Failed to initialize S3 client: {e}")
            print("\nTroubleshooting:")
            print("  On EC2: Make sure IAM role is attached")
            print("  Locally: Run 'aws configure' to set up credentials")
            print("  Or set environment variables:")
            print("    export AWS_ACCESS_KEY_ID=your_key")
            print("    export AWS_SECRET_ACCESS_KEY=your_secret")
            raise
    
    def _get_env_var(self, key: str) -> str:
        """Load environment variable from .env"""
        load_dotenv()
        value = os.environ.get(key)
        if not value:
            raise ValueError(f"{key} not found in environment variables")
        return value
    
    def bucket_exists(self) -> bool:
        """Check if the S3 bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.s3_bucket_name)
            return True
        except:
            return False
    
    def create_bucket_if_needed(self) -> None:
        """Create S3 bucket if it doesn't exist"""
        if self.bucket_exists():
            print(f"✓ Bucket {self.s3_bucket_name} exists")
            return
        
        print(f"Creating bucket {self.s3_bucket_name}...")
        try:
            if self.aws_region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=self.s3_bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.s3_bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                )
            print(f"✓ Bucket created successfully")
        except Exception as e:
            print(f"✗ Failed to create bucket: {e}")
            raise
    
    def upload_fileobj(self, file_obj: BinaryIO, s3_key: str, metadata: Dict = None) -> Dict:
        """
        Upload a file object (in-memory) to S3.
        This is the core method to stream replay data directly from Ballchasing to S3, bypassing the need to first download locally to disk.
        
        Args:
            file_obj: File-like object (BytesIO or similar)
            s3_key: S3 object key (path within bucket)
            metadata: Optional metadata dict to attach to object
            
        Returns:
            Dict with upload info (s3_key, size, etc.)
        """
        try:
            # Prepare upload kwargs
            extra_args = {}
            if metadata:
                # S3 metadata must be strings
                extra_args['Metadata'] = {k: str(v) for k, v in metadata.items()}
            
            # Get file size
            file_obj.seek(0, 2)  # Seek to end
            file_size = file_obj.tell()
            file_obj.seek(0)  # Seek back to start
            
            # Upload directly from memory to S3
            self.s3_client.upload_fileobj(
                file_obj,
                self.s3_bucket_name,
                s3_key,
                ExtraArgs=extra_args if extra_args else None
            )
            
            return {
                's3_key': s3_key,
                'bucket': self.s3_bucket_name,
                'size_bytes': file_size,
                'success': True
            }
            
        except Exception as e:
            return {
                's3_key': s3_key,
                'bucket': self.s3_bucket_name,
                'error': str(e),
                'success': False
            }
    
    def upload_bytes(self, data: bytes, s3_key: str, metadata: Dict = None) -> Dict:
        """
        Upload raw bytes to S3 (convenience wrapper around upload_fileobj).
        
        Args:
            data: Raw bytes
            s3_key: S3 object key
            metadata: Optional metadata
            
        Returns:
            Dict with upload info
        """
        file_obj = io.BytesIO(data)
        return self.upload_fileobj(file_obj, s3_key, metadata)
    
    def upload_file(self, local_path: str, s3_key: str, metadata: Dict = None) -> Dict:
        """
        Upload a local file to S3 (for database backups, etc.).
        
        Args:
            local_path: Path to local file
            s3_key: S3 object key
            metadata: Optional metadata
            
        Returns:
            Dict with upload info
        """
        try:
            path = Path(local_path)
            if not path.exists():
                return {
                    's3_key': s3_key,
                    'error': f"File not found: {local_path}",
                    'success': False
                }
            
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = {k: str(v) for k, v in metadata.items()}
            
            self.s3_client.upload_file(
                str(path),
                self.s3_bucket_name,
                s3_key,
                ExtraArgs=extra_args if extra_args else None
            )
            
            return {
                's3_key': s3_key,
                'bucket': self.s3_bucket_name,
                'size_bytes': path.stat().st_size,
                'success': True
            }
            
        except Exception as e:
            return {
                's3_key': s3_key,
                'error': str(e),
                'success': False
            }
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3 to local disk.
        
        Args:
            s3_key: S3 object key
            local_path: Where to save locally
            
        Returns:
            True if successful
        """
        try:
            path = Path(local_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(
                self.s3_bucket_name,
                s3_key,
                str(path)
            )
            return True
            
        except Exception as e:
            print(f"✗ Download failed: {e}")
            return False
    
    def object_exists(self, s3_key: str) -> bool:
        """Check if an object exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    def get_object_size(self, s3_key: str) -> int:
        """Get size of an S3 object in bytes"""
        try:
            response = self.s3_client.head_object(Bucket=self.s3_bucket_name, Key=s3_key)
            return response['ContentLength']
        except:
            return 0
    
    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> list:
        """
        List objects in S3 with given prefix.
        
        Args:
            prefix: S3 key prefix to filter (e.g., "replays/worlds-2024/")
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except Exception as e:
            print(f"✗ List objects failed: {e}")
            return []
    
    def backup_database(self, db_path: str, s3_prefix: str = "database-backups") -> Dict:
        """
        Backup SQLite database to S3 with timestamp.
        
        Args:
            db_path: Path to local database file
            s3_prefix: S3 prefix for backups
            
        Returns:
            Dict with backup info
        """
        from datetime import datetime, timezone
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db_name = Path(db_path).stem
        s3_key = f"{s3_prefix}/{db_name}_{timestamp}.db"
        
        result = self.upload_file(db_path, s3_key, metadata={
            'backup_timestamp': timestamp,
            'database_name': db_name
        })
        
        if result['success']:
            print(f"✓ Database backed up to s3://{self.s3_bucket_name}/{s3_key}")
        else:
            print(f"✗ Database backup failed: {result.get('error')}")
        
        return result
    
    def get_storage_stats(self, prefix: str = "") -> Dict:
        """
        Get storage statistics for objects with given prefix.
        
        Args:
            prefix: S3 key prefix (e.g., "replays/")
            
        Returns:
            Dict with total size, count, etc.
        """
        try:
            total_size = 0
            count = 0
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket_name, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        count += 1
            
            return {
                'total_objects': count,
                'total_bytes': total_size,
                'total_mb': round(total_size / (1024**2), 2),
                'total_gb': round(total_size / (1024**3), 2),
                'prefix': prefix
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'prefix': prefix
            }

