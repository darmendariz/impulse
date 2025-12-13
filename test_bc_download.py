"""Test script for ballchasing.py functionality, namely downloading replays from a specified group."""

import ballchasing

def run_simple_test(test_group_id:str):
    """
    Simple test function to verify basic functionality.
    Tests with a small known group before running on full dataset.
    """
    print("RUNNING SIMPLE TESTS")
    print("="*60)
    
    try:
        downloader = ballchasing.Ballchasing()
        print("✓ Initialized successfully")
        
        # Test 1: Can we connect to ballchasing?
        print("\nTest 1: Testing API connection...")
        response = downloader.bc_session.get(f"{downloader.bc_base_url}/")
        assert response.status_code == 200, "API connection failed"
        print("✓ API connection successful")
        
        # Test 2: Can we get group info?
        print("\nTest 2: Testing group info retrieval...")
        test_group = test_group_id
        group_info = downloader.get_group_info(test_group)
        assert 'name' in group_info, "Group info missing 'name' field"
        print(f"✓ Group info retrieved: {group_info['name']}")
        
        # Test 3: Can we get child groups?
        print("\nTest 3: Testing child group retrieval...")
        children = downloader.get_child_groups(test_group)
        print(f"✓ Found {len(children)} child groups")
        
        # Test 4: Can we build a small tree (just one level)?
        print("\nTest 4: Testing tree building (limited depth)...")
        if children:
            first_child = children[0]
            print(f"  Testing with first child: {first_child['name']}")
            subtree = downloader.build_group_tree(first_child['id'], depth=0)
            replay_count = len(subtree['replays']) + sum(len(c['replays']) for c in subtree['children'])
            print(f"✓ Subtree built with {replay_count} replays")
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED!")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":

    test_group_id = input("Supply a test group id: ")
    # Run tests first
    print("Step 1: Running tests...\n")
    if not run_simple_test(test_group_id):
        print("\nTests failed. Please fix issues before downloading.")
        exit(1)
    
    # Ask user if they want to proceed with full download
    print("\n" + "="*60)
    print("Tests passed! Ready to download replays.")
    print("="*60)
    response = input("\nDownload replays? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        downloader = ballchasing.Ballchasing()
        output_dir = f"./replays/"
        
        results = downloader.download_group_replays(test_group_id, output_dir)
        
        print("\n✓ Download complete!")
        print(f"Files saved to: {results['output_dir']}")
    else:
        print("Download cancelled.")
