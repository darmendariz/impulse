"""
Unit tests for impulse.collection.rate_limiter module.

Tests rate limiting behavior including per-second and per-hour limits.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
from impulse.collection.rate_limiter import RateLimiter


@pytest.mark.unit
class TestRateLimiterInitialization:
    """Test RateLimiter initialization."""

    def test_initializes_with_default_values(self):
        """RateLimiter should initialize with default Ballchasing limits."""
        limiter = RateLimiter()

        assert limiter.requests_per_second == 1.0
        assert limiter.requests_per_hour == 200
        assert limiter.last_request_time is None
        assert limiter.hourly_window_start is None
        assert limiter.requests_this_hour == 0

    def test_initializes_with_custom_values(self):
        """RateLimiter should accept custom rate limits."""
        limiter = RateLimiter(requests_per_second=2.0, requests_per_hour=500)

        assert limiter.requests_per_second == 2.0
        assert limiter.requests_per_hour == 500

    def test_starts_with_no_requests_tracked(self):
        """RateLimiter should start with clean state."""
        limiter = RateLimiter()

        assert limiter.requests_this_hour == 0
        assert limiter.last_request_time is None
        assert limiter.hourly_window_start is None


@pytest.mark.unit
class TestRateLimiterPerSecondLimit:
    """Test per-second rate limiting."""

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_first_request_does_not_sleep(self, mock_datetime, mock_sleep):
        """First request should not trigger any sleep."""
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = now

        limiter = RateLimiter()
        limiter.wait_if_needed()

        mock_sleep.assert_not_called()

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_sleeps_when_requests_too_fast(self, mock_datetime, mock_sleep):
        """Should sleep when requests come faster than 1/sec."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # First request at 12:00:00
        mock_datetime.now.return_value = start_time
        limiter = RateLimiter(requests_per_second=1.0)
        limiter.wait_if_needed()

        # Second request at 12:00:00.5 (only 0.5 seconds later)
        mock_datetime.now.return_value = start_time + timedelta(seconds=0.5)
        limiter.wait_if_needed()

        # Should sleep for ~0.5 seconds to maintain 1 req/sec
        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        assert 0.4 < sleep_time < 0.6  # Allow small margin

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_no_sleep_when_sufficient_time_passed(self, mock_datetime, mock_sleep):
        """Should not sleep if enough time has passed between requests."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # First request
        mock_datetime.now.return_value = start_time
        limiter = RateLimiter(requests_per_second=1.0)
        limiter.wait_if_needed()

        mock_sleep.reset_mock()

        # Second request 1.5 seconds later (more than minimum interval)
        mock_datetime.now.return_value = start_time + timedelta(seconds=1.5)
        limiter.wait_if_needed()

        # Should not sleep for per-second limit
        mock_sleep.assert_not_called()

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_respects_custom_per_second_rate(self, mock_datetime, mock_sleep):
        """Should respect custom requests_per_second value."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # First request
        mock_datetime.now.return_value = start_time
        limiter = RateLimiter(requests_per_second=2.0)  # 2 req/sec = 0.5 sec interval
        limiter.wait_if_needed()

        # Second request 0.3 seconds later (less than 0.5 sec minimum)
        mock_datetime.now.return_value = start_time + timedelta(seconds=0.3)
        limiter.wait_if_needed()

        # Should sleep for ~0.2 seconds (0.5 - 0.3)
        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        assert 0.15 < sleep_time < 0.25


@pytest.mark.unit
class TestRateLimiterPerHourLimit:
    """Test per-hour rate limiting."""

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_tracks_requests_per_hour(self, mock_datetime, mock_sleep):
        """Should track number of requests in current hour."""
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = now

        limiter = RateLimiter()

        assert limiter.requests_this_hour == 0

        limiter.wait_if_needed()
        assert limiter.requests_this_hour == 1

        limiter.wait_if_needed()
        assert limiter.requests_this_hour == 2

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_pauses_when_hourly_limit_reached(self, mock_datetime, mock_sleep):
        """Should pause when hourly limit is reached."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = start_time

        limiter = RateLimiter(requests_per_second=1000, requests_per_hour=5)

        # Make 5 requests to hit the limit
        for i in range(5):
            limiter.wait_if_needed()

        assert limiter.requests_this_hour == 5
        mock_sleep.reset_mock()

        # 6th request should trigger long sleep
        # Advance time by 10 seconds
        mock_datetime.now.return_value = start_time + timedelta(seconds=10)
        limiter.wait_if_needed()

        # Should sleep for remaining time in hour (3600 - 10 + 1 buffer)
        mock_sleep.assert_called()
        sleep_time = mock_sleep.call_args[0][0]
        assert 3590 <= sleep_time <= 3592  # 3591 seconds (with 1 sec buffer)

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_resets_counter_after_hour(self, mock_datetime, mock_sleep):
        """Should reset hourly counter after an hour has passed."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = start_time

        limiter = RateLimiter(requests_per_second=1000)

        # Make some requests
        for i in range(5):
            limiter.wait_if_needed()

        assert limiter.requests_this_hour == 5

        # Advance time by more than an hour
        mock_datetime.now.return_value = start_time + timedelta(hours=1, seconds=10)
        limiter.wait_if_needed()

        # Counter should be reset to 1 (current request)
        assert limiter.requests_this_hour == 1

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_initializes_hourly_window_on_first_request(self, mock_datetime, mock_sleep):
        """Should initialize hourly window on first request."""
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = now

        limiter = RateLimiter()

        assert limiter.hourly_window_start is None

        limiter.wait_if_needed()

        assert limiter.hourly_window_start == now


@pytest.mark.unit
class TestRateLimiterStatus:
    """Test get_status() method."""

    def test_status_before_any_requests(self):
        """Status should show no requests before any are made."""
        limiter = RateLimiter(requests_per_hour=200)
        status = limiter.get_status()

        assert status['requests_this_hour'] == 0
        assert status['requests_remaining'] == 200
        assert status['window_resets_in_seconds'] is None

    @patch('impulse.collection.rate_limiter.datetime')
    def test_status_after_requests(self, mock_datetime):
        """Status should reflect current request count."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = start_time

        limiter = RateLimiter(requests_per_second=1000, requests_per_hour=200)

        # Make 10 requests
        for i in range(10):
            limiter.wait_if_needed()

        # Check status 30 minutes into the hour
        mock_datetime.now.return_value = start_time + timedelta(minutes=30)
        status = limiter.get_status()

        assert status['requests_this_hour'] == 10
        assert status['requests_remaining'] == 190
        assert 1790 <= status['window_resets_in_seconds'] <= 1810  # ~1800 seconds (30 min)
        assert 29 < status['window_resets_in_minutes'] < 31  # ~30 minutes

    @patch('impulse.collection.rate_limiter.datetime')
    def test_status_shows_zero_time_when_window_expired(self, mock_datetime):
        """Status should show 0 reset time when window has expired."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = start_time

        limiter = RateLimiter()
        limiter.wait_if_needed()

        # Check status after hour has passed
        mock_datetime.now.return_value = start_time + timedelta(hours=2)
        status = limiter.get_status()

        assert status['window_resets_in_seconds'] == 0


@pytest.mark.unit
class TestRateLimiterEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_handles_exact_interval_boundary(self, mock_datetime, mock_sleep):
        """Should handle requests exactly at minimum interval."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_datetime.now.return_value = start_time
        limiter = RateLimiter(requests_per_second=1.0)
        limiter.wait_if_needed()

        mock_sleep.reset_mock()

        # Request exactly 1 second later
        mock_datetime.now.return_value = start_time + timedelta(seconds=1.0)
        limiter.wait_if_needed()

        # Should not sleep
        mock_sleep.assert_not_called()

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_handles_requests_at_exact_hourly_limit(self, mock_datetime, mock_sleep):
        """Should trigger pause on request that exceeds hourly limit."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = start_time

        limiter = RateLimiter(requests_per_second=1000, requests_per_hour=3)

        # Make exactly 3 requests (the limit)
        for i in range(3):
            limiter.wait_if_needed()

        assert limiter.requests_this_hour == 3
        mock_sleep.reset_mock()

        # 4th request should trigger pause
        limiter.wait_if_needed()

        # Should have called sleep for the hourly pause
        mock_sleep.assert_called()

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_multiple_hourly_windows(self, mock_datetime, mock_sleep):
        """Should handle multiple hourly window transitions."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = start_time

        limiter = RateLimiter(requests_per_second=1000, requests_per_hour=5)

        # First hour - make 5 requests
        for i in range(5):
            limiter.wait_if_needed()

        assert limiter.requests_this_hour == 5

        # Move to second hour
        mock_datetime.now.return_value = start_time + timedelta(hours=1, seconds=1)
        limiter.wait_if_needed()

        # Should reset to 1
        assert limiter.requests_this_hour == 1

        # Move to third hour
        mock_datetime.now.return_value = start_time + timedelta(hours=2, seconds=1)
        limiter.wait_if_needed()

        # Should reset to 1 again
        assert limiter.requests_this_hour == 1

    @patch('impulse.collection.rate_limiter.time.sleep')
    @patch('impulse.collection.rate_limiter.datetime')
    def test_fractional_requests_per_second(self, mock_datetime, mock_sleep):
        """Should handle fractional requests_per_second (e.g., 0.5 = 1 req per 2 sec)."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_datetime.now.return_value = start_time
        limiter = RateLimiter(requests_per_second=0.5)  # 1 request per 2 seconds
        limiter.wait_if_needed()

        # Request 1 second later (should sleep 1 more second)
        mock_datetime.now.return_value = start_time + timedelta(seconds=1.0)
        limiter.wait_if_needed()

        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        assert 0.9 < sleep_time < 1.1  # Should sleep ~1 second
