"""
Unit tests for impulse.collection.utils module.

Tests for utility functions: path sanitization, tree operations,
metadata extraction, etc.
"""

import pytest
from impulse.collection.utils import (
    sanitize_path_component,
    flatten_group_tree,
    build_path_components,
    format_bytes,
    extract_replay_metadata
)


@pytest.mark.unit
class TestSanitizePathComponent:
    """Test path sanitization utility function."""

    def test_replaces_invalid_characters_with_underscore(self):
        """Invalid filesystem characters should be replaced with underscore."""
        # Test each invalid character
        assert sanitize_path_component("name<test") == "name_test"
        assert sanitize_path_component("name>test") == "name_test"
        assert sanitize_path_component("name:test") == "name_test"
        assert sanitize_path_component('name"test') == "name_test"
        assert sanitize_path_component("name/test") == "name_test"
        assert sanitize_path_component("name\\test") == "name_test"
        assert sanitize_path_component("name|test") == "name_test"
        assert sanitize_path_component("name?test") == "name_test"
        assert sanitize_path_component("name*test") == "name_test"

    def test_replaces_multiple_invalid_characters(self):
        """Multiple invalid characters should all be replaced."""
        assert sanitize_path_component("<>:|?*") == "______"
        assert sanitize_path_component("RLCS 2024: Worlds") == "RLCS 2024_ Worlds"

    def test_strips_leading_and_trailing_dots_and_spaces(self):
        """Leading/trailing dots and spaces should be removed."""
        assert sanitize_path_component("  test  ") == "test"
        assert sanitize_path_component("..test..") == "test"
        assert sanitize_path_component(". test .") == "test"
        assert sanitize_path_component("  .  test  .  ") == "test"

    def test_handles_empty_string(self):
        """Empty string should remain empty."""
        assert sanitize_path_component("") == ""

    def test_handles_string_with_only_invalid_chars(self):
        """String with only invalid chars should become underscores."""
        assert sanitize_path_component(":::") == "___"

    def test_preserves_valid_characters(self):
        """Valid characters should not be modified."""
        assert sanitize_path_component("ValidName123") == "ValidName123"
        assert sanitize_path_component("Name-with_hyphens") == "Name-with_hyphens"
        assert sanitize_path_component("Name (with) parentheses") == "Name (with) parentheses"

    def test_real_world_examples(self):
        """Test with real-world group/replay names."""
        assert sanitize_path_component("RLCS 2024: Worlds") == "RLCS 2024_ Worlds"
        assert sanitize_path_component("Team Name/Player") == "Team Name_Player"
        assert sanitize_path_component("Match #1 <Finals>") == "Match #1 _Finals_"


@pytest.mark.unit
class TestFlattenGroupTree:
    """Test group tree flattening utility."""

    def test_flatten_single_level_with_replays(self):
        """Single-level tree with replays should flatten correctly."""
        tree = {
            'name': 'Root',
            'replays': [
                {'id': 'replay-1', 'title': 'Match 1'},
                {'id': 'replay-2', 'title': 'Match 2'}
            ],
            'children': []
        }

        result = flatten_group_tree(tree)

        assert len(result) == 2
        assert result[0][0]['id'] == 'replay-1'
        assert result[0][1] == ['Root']
        assert result[1][0]['id'] == 'replay-2'
        assert result[1][1] == ['Root']

    def test_flatten_nested_tree(self):
        """Nested tree should preserve hierarchy in paths."""
        tree = {
            'name': 'Root',
            'replays': [],
            'children': [{
                'name': 'Child',
                'replays': [{'id': 'replay-1'}],
                'children': []
            }]
        }

        result = flatten_group_tree(tree)

        assert len(result) == 1
        assert result[0][0]['id'] == 'replay-1'
        assert result[0][1] == ['Root', 'Child']

    def test_flatten_deeply_nested_tree(self):
        """Deeply nested tree should maintain full path."""
        tree = {
            'name': 'Root',
            'replays': [],
            'children': [{
                'name': 'Level1',
                'replays': [],
                'children': [{
                    'name': 'Level2',
                    'replays': [{'id': 'replay-1'}],
                    'children': []
                }]
            }]
        }

        result = flatten_group_tree(tree)

        assert len(result) == 1
        assert result[0][1] == ['Root', 'Level1', 'Level2']

    def test_flatten_tree_with_multiple_branches(self):
        """Tree with multiple branches should include all replays."""
        tree = {
            'name': 'Root',
            'replays': [{'id': 'replay-root'}],
            'children': [
                {
                    'name': 'Branch1',
                    'replays': [{'id': 'replay-1'}],
                    'children': []
                },
                {
                    'name': 'Branch2',
                    'replays': [{'id': 'replay-2'}],
                    'children': []
                }
            ]
        }

        result = flatten_group_tree(tree)

        assert len(result) == 3
        # Root replay
        assert result[0][0]['id'] == 'replay-root'
        assert result[0][1] == ['Root']
        # Branch1 replay
        assert result[1][0]['id'] == 'replay-1'
        assert result[1][1] == ['Root', 'Branch1']
        # Branch2 replay
        assert result[2][0]['id'] == 'replay-2'
        assert result[2][1] == ['Root', 'Branch2']

    def test_flatten_empty_tree(self):
        """Tree with no replays should return empty list."""
        tree = {
            'name': 'Root',
            'replays': [],
            'children': []
        }

        result = flatten_group_tree(tree)

        assert result == []

    def test_flatten_preserves_replay_data(self):
        """All replay metadata should be preserved."""
        tree = {
            'name': 'Root',
            'replays': [{
                'id': 'replay-1',
                'title': 'Match 1',
                'date': '2024-01-01',
                'blue': {'name': 'Team A'},
                'orange': {'name': 'Team B'}
            }],
            'children': []
        }

        result = flatten_group_tree(tree)

        replay = result[0][0]
        assert replay['id'] == 'replay-1'
        assert replay['title'] == 'Match 1'
        assert replay['date'] == '2024-01-01'
        assert replay['blue']['name'] == 'Team A'


@pytest.mark.unit
class TestBuildPathComponents:
    """Test path component building utility."""

    def test_includes_all_components_by_default(self):
        """All path components should be included by default."""
        result = build_path_components(
            group_path=['RLCS 2024', 'Worlds', 'Day 1'],
            root_name='RLCS 2024'
        )

        assert result == ['RLCS 2024', 'Worlds', 'Day 1']

    def test_excludes_root_when_specified(self):
        """Root should be excluded when include_root=False."""
        result = build_path_components(
            group_path=['RLCS 2024', 'Worlds', 'Day 1'],
            root_name='RLCS 2024',
            include_root=False
        )

        assert result == ['Worlds', 'Day 1']

    def test_sanitizes_all_components(self):
        """All path components should be sanitized."""
        result = build_path_components(
            group_path=['RLCS: 2024', 'Worlds/Finals', 'Day 1'],
            root_name='RLCS: 2024'
        )

        assert result == ['RLCS_ 2024', 'Worlds_Finals', 'Day 1']

    def test_handles_single_component(self):
        """Single component path should work."""
        result = build_path_components(
            group_path=['RLCS 2024'],
            root_name='RLCS 2024'
        )

        assert result == ['RLCS 2024']

    def test_handles_empty_path(self):
        """Empty path should return empty list."""
        result = build_path_components(
            group_path=[],
            root_name='RLCS 2024'
        )

        assert result == []

    def test_excludes_root_only_if_it_matches(self):
        """Root exclusion should only work if root matches first component."""
        result = build_path_components(
            group_path=['Different Name', 'Worlds'],
            root_name='RLCS 2024',
            include_root=False
        )

        # Root doesn't match, so nothing excluded
        assert result == ['Different Name', 'Worlds']


@pytest.mark.unit
class TestFormatBytes:
    """Test byte formatting utility."""

    def test_formats_bytes(self):
        """Small values should be formatted as bytes."""
        assert format_bytes(0) == "0 B"
        assert format_bytes(500) == "500 B"
        assert format_bytes(1023) == "1023 B"

    def test_formats_kilobytes(self):
        """Values in KB range should be formatted as KB."""
        assert format_bytes(1024) == "1.00 KB"
        assert format_bytes(1536) == "1.50 KB"
        assert format_bytes(10240) == "10.00 KB"

    def test_formats_megabytes(self):
        """Values in MB range should be formatted as MB."""
        assert format_bytes(1048576) == "1.00 MB"  # 1024^2
        assert format_bytes(5242880) == "5.00 MB"  # 5 * 1024^2

    def test_formats_gigabytes(self):
        """Values in GB range should be formatted as GB."""
        assert format_bytes(1073741824) == "1.00 GB"  # 1024^3
        assert format_bytes(1610612736) == "1.50 GB"  # 1.5 * 1024^3

    def test_precision_two_decimal_places(self):
        """All formatted values should have 2 decimal places."""
        assert format_bytes(1500) == "1.46 KB"
        assert format_bytes(1500000) == "1.43 MB"
        assert format_bytes(1500000000) == "1.40 GB"


@pytest.mark.unit
class TestExtractReplayMetadata:
    """Test replay metadata extraction utility."""

    def test_extracts_basic_fields(self):
        """Basic fields should be extracted correctly."""
        ballchasing_replay = {
            'id': 'abc123',
            'replay_title': 'Grand Finals',
            'blue': {'name': 'Team A'},
            'orange': {'name': 'Team B'},
            'date': '2024-01-01T10:00:00'
        }

        result = extract_replay_metadata(ballchasing_replay)

        assert result['replay_id'] == 'abc123'
        assert result['title'] == 'Grand Finals'
        assert result['blue_team'] == 'Team A'
        assert result['orange_team'] == 'Team B'
        assert result['date'] == '2024-01-01T10:00:00'
        assert result['source'] == 'ballchasing'

    def test_handles_missing_title_field(self):
        """Should handle missing replay_title gracefully."""
        ballchasing_replay = {
            'id': 'abc123',
            'title': 'Alternative Title Field',
            'blue': {'name': 'Team A'},
            'orange': {'name': 'Team B'},
            'date': '2024-01-01'
        }

        result = extract_replay_metadata(ballchasing_replay)

        assert result['title'] == 'Alternative Title Field'

    def test_handles_missing_team_names(self):
        """Missing team names should default to 'Unknown'."""
        ballchasing_replay = {
            'id': 'abc123',
            'replay_title': 'Test',
            'blue': {},
            'orange': {}
        }

        result = extract_replay_metadata(ballchasing_replay)

        assert result['blue_team'] == 'Unknown'
        assert result['orange_team'] == 'Unknown'

    def test_handles_missing_blue_orange_fields(self):
        """Missing blue/orange dicts should default to 'Unknown'."""
        ballchasing_replay = {
            'id': 'abc123',
            'replay_title': 'Test'
        }

        result = extract_replay_metadata(ballchasing_replay)

        assert result['blue_team'] == 'Unknown'
        assert result['orange_team'] == 'Unknown'

    def test_handles_missing_date(self):
        """Missing date should default to 'Unknown'."""
        ballchasing_replay = {
            'id': 'abc123',
            'blue': {'name': 'Team A'},
            'orange': {'name': 'Team B'}
        }

        result = extract_replay_metadata(ballchasing_replay)

        assert result['date'] == 'Unknown'

    def test_always_includes_source(self):
        """Source field should always be 'ballchasing'."""
        ballchasing_replay = {
            'id': 'abc123'
        }

        result = extract_replay_metadata(ballchasing_replay)

        assert result['source'] == 'ballchasing'

    def test_real_world_example(self, sample_replay_metadata):
        """Test with realistic Ballchasing API response."""
        result = extract_replay_metadata(sample_replay_metadata)

        assert result['replay_id'] == 'abc123def456'
        assert result['title'] == 'RLCS Grand Finals - Game 5'
        assert result['blue_team'] == 'Team Vitality'
        assert result['orange_team'] == 'G2 Esports'
        assert result['source'] == 'ballchasing'
