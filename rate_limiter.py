from datetime import datetime, timedelta, timezone
import time
from typing import Optional


class RateLimiter:
    """
    Rate limiter for Ballchasing API.
    
    Limits:
    - 1 request per second
    - 200 requests per hour (free tier)

    Patreon patrons get higher limits, but this is not implemented here.
    """
    
    def __init__(self, requests_per_second: float = 1.0, requests_per_hour: int = 200):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_second: Maximum requests per second (default: 1)
            requests_per_hour: Maximum requests per hour (default: 200)
        """
        self.requests_per_second = requests_per_second
        self.requests_per_hour = requests_per_hour
        
        self.last_request_time: Optional[datetime] = None
        self.hourly_window_start: Optional[datetime] = None
        self.requests_this_hour = 0
    
    def wait_if_needed(self):
        """
        Wait if necessary to comply with rate limits.
        Handles both per-second and per-hour limits.
        """
        now = datetime.now(timezone.utc)
        
        # Initialize on first request
        if self.hourly_window_start is None:
            self.hourly_window_start = now
            self.requests_this_hour = 0
        
        # Check if we need to reset hourly counter
        time_since_window_start = (now - self.hourly_window_start).total_seconds()
        if time_since_window_start >= 3600:
            # Hour has passed, reset counter
            self.hourly_window_start = now
            self.requests_this_hour = 0
            print(f"\n  ⏱️  Hourly rate limit window reset")
        
        # Check hourly limit
        if self.requests_this_hour >= self.requests_per_hour:
            # Hit hourly limit, need to wait
            time_until_reset = 3600 - time_since_window_start
            print(f"\n  ⚠️  Hit hourly rate limit ({self.requests_per_hour} requests/hour)")
            print(f"  ⏸️  Pausing for {time_until_reset/60:.1f} minutes until rate limit resets...")
            print(f"  ⏰  Will resume at: {(now + timedelta(seconds=time_until_reset)).strftime('%H:%M:%S UTC')}")
            
            # Sleep until window resets
            time.sleep(time_until_reset + 1)  # +1 second buffer
            
            # Reset counter
            self.hourly_window_start = datetime.now(timezone.utc)
            self.requests_this_hour = 0
            print(f"  ✓ Resuming downloads...")
        
        # Check per-second limit
        if self.last_request_time is not None:
            time_since_last = (now - self.last_request_time).total_seconds()
            min_interval = 1.0 / self.requests_per_second
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                time.sleep(sleep_time)
        
        # Update tracking
        self.last_request_time = datetime.now(timezone.utc)
        self.requests_this_hour += 1
    
    def get_status(self) -> dict:
        """Get current rate limiter status"""
        now = datetime.now(timezone.utc)
        
        if self.hourly_window_start is None:
            return {
                'requests_this_hour': 0,
                'requests_remaining': self.requests_per_hour,
                'window_resets_in_seconds': None
            }
        
        time_since_window = (now - self.hourly_window_start).total_seconds()
        time_until_reset = max(0, 3600 - time_since_window)
        
        return {
            'requests_this_hour': self.requests_this_hour,
            'requests_remaining': self.requests_per_hour - self.requests_this_hour,
            'window_resets_in_seconds': time_until_reset,
            'window_resets_in_minutes': time_until_reset / 60
        }
