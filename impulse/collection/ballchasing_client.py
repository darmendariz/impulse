"""
Ballchasing API client.

Pure API client for interacting with the Ballchasing.com API.
Handles only HTTP requests and responses - no orchestration logic.
"""

import requests
import time
from requests_ratelimiter import LimiterSession
from typing import List, Dict, Optional
from impulse.config.collection_config import CollectionConfig


class BallchasingClient:
    """
    Pure API client for Ballchasing.com.

    Handles HTTP requests to the Ballchasing API without any orchestration,
    storage, or database logic. Use this class when you need low-level API access.

    For high-level download workflows, use ReplayDownloader instead.
    """

    def __init__(self, config: CollectionConfig = None, rate_limit_per_second: int = None, rate_limit_per_hour: int = None):
        """
        Initialize Ballchasing API client.

        Args:
            config: Configuration object (defaults to loading from environment)
        """
        if config is None:
            config = CollectionConfig.from_env()

        self.config = config
        self.api_key = config.ballchasing_api_key
        self.base_url = "https://ballchasing.com/api"
        self.rate_limit_per_second = rate_limit_per_second if rate_limit_per_second else config.rate_limit_per_second
        self.rate_limit_per_hour = rate_limit_per_hour if rate_limit_per_hour else config.rate_limit_per_hour


        # Create HTTP session with auth headers
        self.session = LimiterSession(per_second=self.rate_limit_per_second, per_hour=self.rate_limit_per_hour)
        self.session.headers.update({"Authorization": self.api_key})

    def get_group_info(self, group_id: str) -> Dict:
        """
        Fetch metadata for a Ballchasing group.

        Returns basic info such as group id, link, name, date created, creator,
        and player stats from replays in the group.

        Note: Response does not include information about child groups.

        Args:
            group_id: Ballchasing group ID

        Returns:
            Dict with group metadata

        Raises:
            requests.HTTPError: If the API request fails

        API Documentation:
            https://ballchasing.com/doc/api#replay-groups-group-get
        """
        url = f"{self.base_url}/groups/{group_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_child_groups(self, parent_group_id: str) -> List[Dict]:
        """
        Get all child groups of a parent group.

        Handles pagination automatically to fetch all child groups.

        Args:
            parent_group_id: Parent group ID

        Returns:
            List of child group dicts

        Raises:
            requests.HTTPError: If the API request fails

        API Documentation:
            https://ballchasing.com/doc/api#replay-groups-groups-get
        """
        children = []
        count = 200  # Max per page
        after = None

        while True:

            params = {'group': parent_group_id, 'count': count}
            if after:
                params['after'] = after

            url = f"{self.base_url}/groups"
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            batch = data.get('list', [])
            children.extend(batch)

            # Check for more pages
            if 'next' not in data or len(batch) < count:
                break

            after = batch[-1]['id'] if batch else None
            time.sleep(0.5)  # Brief pause between pagination requests

        return children

    def get_replays_from_group(self, group_id: str) -> List[Dict]:
        """
        Fetch all replay metadata from a specific group (non-recursive).

        Handles pagination automatically to fetch all replays.

        Args:
            group_id: Ballchasing group ID

        Returns:
            List of replay metadata dicts

        Raises:
            requests.HTTPError: If the API request fails

        API Documentation:
            https://ballchasing.com/doc/api#replays-replays-get
        """
        replays = []
        count = 200  # Max per page
        after = None

        while True:
            params = {'group': group_id, 'count': count}
            if after:
                params['after'] = after

            url = f"{self.base_url}/replays"
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            batch = data.get('list', [])
            replays.extend(batch)

            # Check for more pages
            if len(batch) < count or 'next' not in data:
                break

            after = data.get('next')
            time.sleep(0.5)  # Brief pause between pagination requests

        return replays

    def download_replay_bytes(self, replay_id: str) -> bytes:
        """
        Download a replay file as raw bytes.

        Args:
            replay_id: Ballchasing replay ID

        Returns:
            Raw replay file bytes

        Raises:
            requests.HTTPError: If the API request fails

        API Documentation:
            https://ballchasing.com/doc/api#replays-replay-get-1
        """
        url = f"{self.base_url}/replays/{replay_id}/file"
        response = self.session.get(url)
        response.raise_for_status()
        return response.content

    def get_replay_metadata(self, replay_id: str) -> Dict:
        """
        Get metadata for a specific replay.

        Args:
            replay_id: Ballchasing replay ID

        Returns:
            Dict with replay metadata

        Raises:
            requests.HTTPError: If the API request fails

        API Documentation:
            https://ballchasing.com/doc/api#replays-replay-get
        """
        url = f"{self.base_url}/replays/{replay_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def build_group_tree(self, group_id: str, depth: int = 0,
                        progress_callback: Optional[callable] = None) -> Dict:
        """
        Recursively build a tree structure of groups and replays.

        Args:
            group_id: Root group ID to start from
            depth: Current recursion depth (used internally)
            progress_callback: Optional callback function(message, depth) for progress updates

        Returns:
            Dict with structure:
            {
                'id': 'group-id',
                'name': 'Group Name',
                'children': [...],  # List of child group dicts
                'replays': [...]    # List of replay dicts (only in leaf groups)
            }

        Raises:
            requests.HTTPError: If any API request fails
        """
        # Get group info
        group_info = self.get_group_info(group_id)
        group_name = group_info.get('name', 'Unknown')

        if progress_callback:
            progress_callback(f"Exploring: {group_name}", depth)

        tree = {
            'id': group_id,
            'name': group_name,
            'children': [],
            'replays': []
        }

        # Check for child groups
        child_groups = self.get_child_groups(group_id)

        if child_groups:
            # Has subgroups - recurse
            if progress_callback:
                progress_callback(f"Found {len(child_groups)} subgroups", depth)

            for child in child_groups:
                child_id = child['id']
                child_tree = self.build_group_tree(child_id, depth + 1, progress_callback)
                tree['children'].append(child_tree)
        else:
            # Leaf group - fetch replays
            if progress_callback:
                progress_callback("Fetching replays...", depth)

            replays = self.get_replays_from_group(group_id)
            tree['replays'] = replays

            if progress_callback:
                progress_callback(f"Found {len(replays)} replays", depth)

        time.sleep(0.5)  # Brief pause between groups
        return tree
