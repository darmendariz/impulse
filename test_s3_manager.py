from s3_manager import S3Manager

if __name__ == "__main__":
    print("Testing S3Manager...\n")
    
    # Initialize
    s3 = S3Manager()
    
    # Test 1: Check bucket exists
    print("\nTest 1: Checking bucket...")
    exists = s3.bucket_exists()
    print(f"  Bucket exists: {exists}")
    
    # Test 2: Upload test bytes
    print("\nTest 2: Uploading test data...")
    test_data = b"Hello from Impulse!"
    result = s3.upload_bytes(test_data, "test/hello.txt", metadata={'test': 'true'})
    print(f"  Upload success: {result['success']}")
    
    # Test 3: Check if object exists
    print("\nTest 3: Checking object exists...")
    exists = s3.object_exists("test/hello.txt")
    print(f"  Object exists: {exists}")
    
    # Test 4: Get storage stats
    print("\nTest 4: Storage statistics...")
    stats = s3.get_storage_stats("test/")
    print(f"  Objects: {stats.get('total_objects', 0)}")
    print(f"  Size: {stats.get('total_mb', 0)} MB")
    
    # Cleanup
    print("\nCleaning up test file...")
    s3.s3_client.delete_object(Bucket=s3.s3_bucket_name, Key="test/hello.txt")
    
    print("\n✓ All tests completed!")
