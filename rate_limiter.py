"""
Rate limiter for controlling request frequency
"""

import asyncio
import time
from typing import Optional


class RateLimiter:
    """Simple rate limiter using token bucket algorithm"""
    
    def __init__(self, min_delay: float = 2.0, max_concurrent: int = 1):
        self.min_delay = min_delay
        self.max_concurrent = max_concurrent
        self.last_request_time: Optional[float] = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire permission to make a request"""
        async with self._semaphore:
            async with self._lock:
                if self.last_request_time is not None:
                    elapsed = time.time() - self.last_request_time
                    if elapsed < self.min_delay:
                        wait_time = self.min_delay - elapsed
                        await asyncio.sleep(wait_time)
                
                self.last_request_time = time.time()


class ProxyRotator:
    """Rotates through a list of proxies"""
    
    def __init__(self, proxies: list = None):
        self.proxies = proxies or []
        self._current_index = 0
        self._lock = asyncio.Lock()
    
    async def get_next_proxy(self) -> Optional[str]:
        """Get the next proxy in rotation"""
        if not self.proxies:
            return None
        
        async with self._lock:
            proxy = self.proxies[self._current_index]
            self._current_index = (self._current_index + 1) % len(self.proxies)
            return proxy
    
    @property
    def has_proxies(self) -> bool:
        return len(self.proxies) > 1
