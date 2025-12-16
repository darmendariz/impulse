"""
Test script for S3 integration

Run this to test the complete flow of replay files from:
Ballchasing → Memory → S3 → Database
"""

from ballchasing import Ballchasing
from s3_manager import S3Manager
from impulse_db import ImpulseDB


def test_s3_setup():
    """Test 1: Verify S3 is accessible"""
    print("="*60)
    print("TEST 1: S3 Setup")
    print("="*60)
    
    try:
        s3 = S3Manager()
        s3.create_bucket_if_needed()
        
        # Test upload
        result = s3.upload_bytes(b"test", "test/hello.txt")
        if result['success']:
            print("✓ S3 upload works")
            # Cleanup
            s3.s3_client.delete_object(Bucket=s3.s3_bucket_name, Key="test/hello.txt")
        else:
            print(f"✗ S3 upload failed: {result.get('error')}")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ S3 setup failed: {e}")
        return False


def test_database():
    """Test 2: Verify database works with S3 schema"""
    print("\n" + "="*60)
    print("TEST 2: Database with S3 Schema")
    print("="*60)
    
    try:
        db = ImpulseDB("./test_impulse.db")
        
        # Add a test replay
        test_replay = {
            'id': 'test-s3-replay',
            'replay_title': 'Test Match',
            'blue': {'name': 'Blue Team'},
            'orange': {'name': 'Orange Team'}
        }
        
        is_new = db.add_replay('test-s3-replay', test_replay)
        print(f"  Added replay (is_new={is_new})")
        
        # Mark as downloaded with S3 key
        db.mark_downloaded('test-s3-replay', 's3://bucket/test.replay', 1000000)
        print(f"  Marked as downloaded")
        
        # Verify it's marked
        is_downloaded = db.is_replay_downloaded('test-s3-replay')
        print(f"  Is downloaded: {is_downloaded}")
        
        if is_downloaded:
            print("✓ Database works with S3 schema")
            return True
        else:
            print("✗ Database not working correctly")
            return False
            
    except Exception as e:
        print(f"✗ Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_small_download():
    """Test 3: Download a small group to S3"""
    print("\n" + "="*60)
    print("TEST 3: Small Group Download to S3")
    print("="*60)
    
    # Get a small test group ID from user
    print("\nEnter a SMALL test group ID (5-10 replays max):")
    print("Example: Find a small subgroup from any RLCS event")
    test_group = input("Group ID: ").strip()
    
    if not test_group:
        print("✗ No group ID provided, skipping")
        return False
    
    try:
        bc = Ballchasing()
        
        # Run the download
        results = bc.download_group_to_s3(test_group)
        
        print(f"\n{'='*60}")
        print("TEST RESULTS")
        print(f"{'='*60}")
        print(f"Total: {results['total']}")
        print(f"Successful: {results['successful']}")
        print(f"Skipped: {results['skipped']}")
        print(f"Failed: {results['failed']}")
        
        if results['successful'] > 0 or results['skipped'] == results['total']:
            print("\n✓ S3 download works!")
            return True
        else:
            print("\n✗ No replays downloaded")
            return False
            
    except Exception as e:
        print(f"✗ Download test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n" + "="*60)
    print("IMPULSE S3 INTEGRATION TEST SUITE")
    print("="*60)
    print()
    
    # Run tests
    test1 = test_s3_setup()
    if not test1:
        print("\n⚠️  S3 setup failed. Fix this before continuing.")
        exit(1)
    
    test2 = test_database()
    if not test2:
        print("\n⚠️  Database test failed. Check impulse_db.py.")
        exit(1)
    
    # Optional: test actual download
    print("\n" + "="*60)
    print("Ready to test actual download to S3")
    print("="*60)
    response = input("\nRun live download test? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        test3 = test_small_download()
    else:
        print("Skipping live download test")
        test3 = None
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"S3 Setup: {'✓ PASS' if test1 else '✗ FAIL'}")
    print(f"Database: {'✓ PASS' if test2 else '✗ FAIL'}")
    if test3 is not None:
        print(f"Live Download: {'✓ PASS' if test3 else '✗ FAIL'}")
    
    if test1 and test2:
        print("\n✓ Ready for production use!")
    else:
        print("\n⚠️  Fix failing tests before using in production")
