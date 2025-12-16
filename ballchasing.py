from dotenv import load_dotenv
import os
import requests
import time
from typing import List, Dict, Tuple
from pathlib import Path
from impulse_db import ImpulseDB

class Ballchasing:

    def __init__(self, bc_api_key:str=None, aws_region:str=None, s3_bucket_name:str=None):
        self.bc_api_key = bc_api_key if bc_api_key else self._get_bc_api_key()
        self.bc_base_url = "https://ballchasing.com/api"
        self.bc_headers = {"Authorization": self.bc_api_key}
        self.bc_session = requests.Session()
        self.bc_session.headers.update(self.bc_headers)

    def _get_bc_api_key(self) -> str:
        """Retrieve Ballchasing API key from environment variables."""
        load_dotenv()
        bc_api_key = os.environ.get("BALLCHASING_API_KEY")
        if not bc_api_key:
            raise ValueError("BALLCHASING_API_KEY not found in environment variables. Please store your Ballchasing API key in a .env file in the project root.")
        return bc_api_key
    
    def get_group_info(self, group_id: str) -> Dict:
        """Fetch Ballchasing group metadata. Return basic info such as group id, link, name, date created, creator, and players, as well as player stats such as boost, movement, positioning, etc. from replays in the group. Note: Response does not return any information about child groups.
        
        See https://ballchasing.com/doc/api#replay-groups-group-get.
        """
        url = f"{self.bc_base_url}/groups/{group_id}"
        response = self.bc_session.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_child_groups(self, parent_group_id: str) -> List[Dict]:
        """Get all child groups of a parent group using the `/groups` endpoint on Ballchasing.
        
        See https://ballchasing.com/doc/api#replay-groups-groups-get.  
        """
        children = []
        count = 200
        after = None
        
        while True:
            params = {'group': parent_group_id, 'count': count}
            if after:
                params['after'] = after
            
            url = f"{self.bc_base_url}/groups"
            response = self.bc_session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            batch = data.get('list', [])
            children.extend(batch)
            
            # Check pagination
            if 'next' not in data or len(batch) < count:
                break
            
            after = batch[-1]['id'] if batch else None
            time.sleep(0.5)
        
        return children
    
    def build_group_tree(self, group_id: str, depth: int = 0) -> Dict:
        """
        Recursively build a tree structure of groups and replays.
        
        Returns a dict with structure:
        `{
            'id': 'group-id',
            'name': 'Group Name',
            'children': [...]  # List of child group dicts
            'replays': [...]   # List of replay dicts (only in leaf groups)
        }`
        """
        indent = "  " * depth
        print(f"{indent}Exploring group: {group_id}")
        
        # Get group info
        group_info = self.get_group_info(group_id)
        group_name = group_info.get('name', 'Unknown')
        print(f"{indent}  Name: {group_name}")
        
        tree = {
            'id': group_id,
            'name': group_name,
            'children': [],
            'replays': []
        }
        
        # Check for child groups within the group
        child_groups = self.get_child_groups(group_id)
        
        if child_groups:
            # This group has subgroups - recurse into them
            print(f"{indent}  Found {len(child_groups)} subgroups")
            for child in child_groups:
                child_id = child['id']
                child_tree = self.build_group_tree(child_id, depth + 1)
                tree['children'].append(child_tree)
        else:
            # This is a leaf group - fetch its replays
            print(f"{indent}  Leaf group - fetching replays...")
            replays = self.get_replays_from_group(group_id, depth)
            tree['replays'] = replays
            print(f"{indent}  Found {len(replays)} replays")
        
        time.sleep(0.5)  # Rate limiting
        return tree
    
    def get_replays_from_group(self, group_id: str, depth: int = 0) -> List[Dict]:
        """Fetch all replay metadata from a specific group (non-recursive).
        
        See https://ballchasing.com/doc/api#replays-replays-get.
        """
        indent = "  " * depth
        replays = []
        
        # Fetch replays with pagination
        count = 200  # Max per page
        after = None
        page = 1
        
        while True:
            params = {'group': group_id, 'count': count}
            if after:
                params['after'] = after
                
            response = self.bc_session.get(f"{self.bc_base_url}/replays", params=params)
            response.raise_for_status()
            data = response.json()
            
            batch = data.get('list', [])
            replays.extend(batch)
            
            if len(batch) > 0:
                print(f"{indent}    Page {page}: {len(batch)} replays (Total: {len(replays)})")
            
            # Check if there are more pages
            if len(batch) < count or 'next' not in data:
                break
                
            after = data.get('next')
            page += 1
            time.sleep(0.5)  # Rate limiting
        
        return replays

    def flatten_tree_to_replay_list(self, tree: Dict, path: List[str] = None) -> List[Tuple[Dict, List[str]]]:
        """
        Flatten the tree into a list of (replay, path) tuples.
        
        Returns: [(replay_dict, ['parent', 'child', 'grandchild']), ...]
        """
        if path is None:
            path = []
        
        result = []
        
        # Add replays from this node
        for replay in tree['replays']:
            result.append((replay, path + [tree['name']]))
        
        # Recurse into children
        for child in tree['children']:
            result.extend(self.flatten_tree_to_replay_list(child, path + [tree['name']]))
        
        return result

    def sanitize_path_component(self, name: str) -> str:
        """Sanitize a group/replay name for use in file paths."""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        name = name.strip('. ')
        return name
    
    def download_replay(self, replay_id: str, output_dir: Path) -> Path:
        """
        Download a single replay file to local directory.
        
        Args:
            replay_id: The ballchasing replay ID
            output_dir: Directory to save the replay file
            
        Returns:
            Path to the downloaded file

        See https://ballchasing.com/doc/api#replays-replay-get-1.
        """
        url = f"{self.bc_base_url}/replays/{replay_id}/file"
        response = self.bc_session.get(url)
        response.raise_for_status()
        
        filepath = output_dir / f"{replay_id}.replay"
        filepath.write_bytes(response.content)
        return filepath

    def download_group_replays(self, group_id: str, output_base_dir: str = "./replays", use_database: bool = True) -> Dict:
        """
        Download all replays from a group (and subgroups) to local directory.
        Track downloads in a local database for deduplication.
        
        Args:
            group_id: The ballchasing group ID
            output_base_dir: Base directory for downloads
            use_database: Whether to register replay info in a database for deduplication (default: True)
            
        Returns:
            Dictionary with download statistics
        """
        output_base = Path(output_base_dir)
        output_base.mkdir(parents=True, exist_ok=True)
        
        # Initialize database if enabled
        db = ImpulseDB() if use_database else None
        
        print("="*60)
        print("IMPULSE: Ballchasing Replay Downloader")
        if use_database:
            print("Database: ENABLED (deduplication active)")
        else:
            print("Database: DISABLED")
        print("="*60)
        print()
        
        # Build group tree
        print("Building group tree...")
        print()
        tree = self.build_group_tree(group_id)
        
        # Flatten to replay list
        print()
        print("Flattening tree structure...")
        replay_list = self.flatten_tree_to_replay_list(tree)
        print(f"✓ Found {len(replay_list)} total replays")

        # Register the group we're downloading to the groups table in the database
        if db:
            db.register_group_download(tree['id'], tree['name'], len(replay_list))

        # Use the root group's name as the top-level directory
        root_name = tree.get('name') or group_id
        root_sanitized = self.sanitize_path_component(root_name)
        output_base = output_base / root_sanitized
        output_base.mkdir(parents=True, exist_ok=True)
        
        # Add replays to database (if enabled)
        if db:
            print("\nRegistering replays in database...")
            new_replays = 0
            existing_replays = 0
            
            for replay, _ in replay_list:
                is_new = db.add_replay(replay['id'], replay)
                if is_new:
                    new_replays += 1
                else:
                    existing_replays += 1
            
            print(f"  New replays: {new_replays}")
            print(f"  Already in database: {existing_replays}")
        
        # Download all replays
        print()
        print("="*60)
        print(f"DOWNLOADING REPLAYS")
        print("="*60)
        print()
        
        successful = 0
        failed = 0
        skipped = 0
        downloaded_files = []
        
        for i, (replay, group_path) in enumerate(replay_list, 1):
            replay_id = replay['id']
            
            # Build directory path from group hierarchy
            if group_path and len(group_path) > 0:
                relative_components = group_path[1:]  # drop root
            else:
                relative_components = []
            sanitized_path = [self.sanitize_path_component(p) for p in relative_components]
            rel_dir = Path(*sanitized_path) if sanitized_path else Path(".")
            output_dir = output_base / rel_dir
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Print progress
            full_rel = Path(root_sanitized) / rel_dir
            print(f"[{i}/{len(replay_list)}] {replay_id}")
            print(f"  Path: {full_rel}")
            
            # Check database first (if enabled)
            if db and db.is_replay_downloaded(replay_id):
                print(f"  ⊘ Already downloaded (registered in database), skipping")
                skipped += 1
                print()
                continue
            
            try:
                # Download replay
                print(f"  Downloading...")
                filepath = self.download_replay(replay_id, output_dir)
                file_size = filepath.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                print(f"  ✓ Saved: {filepath} ({file_size_mb:.2f} MB)")
                
                # Mark as downloaded in database
                if db:
                    db.mark_downloaded(replay_id, str(filepath.relative_to(output_base.parent)), file_size)
                
                successful += 1
                downloaded_files.append(str(filepath))
                
                # Rate limiting
                time.sleep(1)
                print()
                
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                failed += 1
                print()
                continue
        
        # Print summary
        print("="*60)
        print("DOWNLOAD SUMMARY")
        print("="*60)
        print(f"Total replays: {len(replay_list)}")
        print(f"Successfully downloaded: {successful}")
        print(f"Skipped (already had): {skipped}")
        print(f"Failed: {failed}")
        print(f"Output directory: {output_base.parent.absolute()}/{root_sanitized}")
        
        # Database statistics
        if db:
            print()
            print("DATABASE STATISTICS")
            print("-"*60)
            stats = db.get_stats()
            for key, value in stats.items():
                print(f"{key}: {value}")
        
        return {
            'total': len(replay_list),
            'successful': successful,
            'skipped': skipped,
            'failed': failed,
            'output_dir': str(output_base.absolute()),
            'files': downloaded_files
        }
