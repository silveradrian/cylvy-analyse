import time
import threading
from typing import Callable, Optional
import asyncio

class RateLimiter:
    """
    A modern rate limiter that works with current Python versions.
    Limits the number of calls to a function within a time period.
    """
    
    def __init__(self, max_calls: int, period: float = 1.0, callback: Optional[Callable] = None):
        """
        Initialize the rate limiter.
        
        Args:
            max_calls: Maximum number of calls allowed within the period
            period: Time period in seconds
            callback: Optional callback function to be called when rate limit is hit
        """
        self.max_calls = max_calls
        self.period = period
        self.callback = callback
        self.calls = []
        self.lock = threading.RLock()
        
    def __call__(self, func):
        """Decorator usage"""
        def wrapped(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapped
        
    def __enter__(self):
        """Context manager entry"""
        with self.lock:
            # Remove calls that are outside the period
            current_time = time.time()
            self.calls = [call_time for call_time in self.calls 
                        if current_time - call_time <= self.period]
            
            # Wait if we've reached the maximum calls
            if len(self.calls) >= self.max_calls:
                # Calculate wait time based on the oldest call
                if self.calls:
                    wait_time = self.period - (current_time - self.calls[0])
                    if wait_time > 0:
                        if self.callback:
                            self.callback(wait_time)
                        time.sleep(wait_time)
                
                # Update calls list after waiting
                self.calls = [call_time for call_time in self.calls 
                            if time.time() - call_time <= self.period]
            
            # Add the current call
            self.calls.append(time.time())
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        pass

    async def __aenter__(self):
        """Async context manager entry"""
        with self.lock:
            current_time = time.time()
            self.calls = [call_time for call_time in self.calls 
                        if current_time - call_time <= self.period]
            
            if len(self.calls) >= self.max_calls:
                if self.calls:
                    wait_time = self.period - (current_time - self.calls[0])
                    if wait_time > 0:
                        if self.callback:
                            self.callback(wait_time)
                        await asyncio.sleep(wait_time)
                
                self.calls = [call_time for call_time in self.calls 
                            if time.time() - call_time <= self.period]
            
            self.calls.append(time.time())
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        pass
