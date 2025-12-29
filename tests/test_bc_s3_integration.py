"""
Test script for S3 integration

Run this to test the complete flow of replay files from:
Ballchasing → Memory → S3 → Database

Updated to use the new collection API.
"""

from impulse.collection import download_group, BallchasingClient, S3Backend
from impulse.collection.database import ImpulseDB


def test_s3_setup():
    """Test 1: Verify S3 is accessible"""
    print("="*60)
    print("TEST 1: S3 Setup")
    print("="*60)

    try:
        s3_backend = S3Backend()

        # Test upload
        result = s3_backend.s3_manager.upload_bytes(b"test", "test/hello.txt")
        if result['success']:
            print("PASS: S3 upload works")
            # Cleanup
            s3_backend.s3_manager.s3_client.delete_object(
                Bucket=s3_backend.bucket_name,
                Key="test/hello.txt"
            )
        else:
            print(f"FAIL: S3 upload failed: {result.get('error')}")
            return False

        return True

    except Exception as e:
        print(f"FAIL: S3 setup failed: {e}")
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
            print("PASS: Database works with S3 schema")
            return True
        else:
            print("FAIL: Database not working correctly")
            return False

    except Exception as e:
        print(f"FAIL: Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_api_client():
    """Test 3: Verify API client works"""
    print("\n" + "="*60)
    print("TEST 3: Ballchasing API Client")
    print("="*60)

    try:
        client = BallchasingClient()

        # Test getting a well-known public group
        # Using RLCS 2024 root group as a test
        test_group_id = 'rlcs-2024-jsvrszynst'

        print(f"  Testing with group: {test_group_id}")
        group_info = client.get_group_info(test_group_id)

        print(f"  Group name: {group_info.get('name')}")
        print("PASS: API client works")
        return True

    except Exception as e:
        print(f"FAIL: API client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_small_download():
    """Test 4: Download a small group to S3"""
    print("\n" + "="*60)
    print("TEST 4: Small Group Download to S3")
    print("="*60)

    # Get a small test group ID from user
    print("\nEnter a SMALL test group ID (5-10 replays max):")
    print("Example: Find a small subgroup from any RLCS event")
    test_group = input("Group ID: ").strip()

    if not test_group:
        print("SKIP: No group ID provided, skipping")
        return False

    try:
        # Use new simplified API
        result = download_group(
            group_id=test_group,
            storage_type='s3',
            path_prefix=['test', 'integration'],
            use_database=True
        )

        print(f"\n{'='*60}")
        print("TEST RESULTS")
        print(f"{'='*60}")
        print(f"Total: {result.total_replays}")
        print(f"Successful: {result.successful}")
        print(f"Skipped: {result.skipped}")
        print(f"Failed: {result.failed}")

        if result.successful > 0 or result.skipped == result.total_replays:
            print("\nPASS: S3 download works")
            return True
        else:
            print("\nFAIL: No replays downloaded")
            return False

    except Exception as e:
        print(f"FAIL: Download test failed: {e}")
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
        print("\nWARNING: S3 setup failed. Fix this before continuing.")
        exit(1)

    test2 = test_database()
    if not test2:
        print("\nWARNING: Database test failed. Check database.py.")
        exit(1)

    test3 = test_api_client()
    if not test3:
        print("\nWARNING: API client test failed. Check ballchasing_client.py.")
        exit(1)

    # Optional: test actual download
    print("\n" + "="*60)
    print("Ready to test actual download to S3")
    print("="*60)
    response = input("\nRun live download test? (yes/no): ")

    if response.lower() in ['yes', 'y']:
        test4 = test_small_download()
    else:
        print("Skipping live download test")
        test4 = None

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"S3 Setup: {'PASS' if test1 else 'FAIL'}")
    print(f"Database: {'PASS' if test2 else 'FAIL'}")
    print(f"API Client: {'PASS' if test3 else 'FAIL'}")
    if test4 is not None:
        print(f"Live Download: {'PASS' if test4 else 'FAIL'}")

    if test1 and test2 and test3:
        print("\nReady for production use")
    else:
        print("\nFix failing tests before using in production")
