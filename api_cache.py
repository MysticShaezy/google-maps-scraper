"""
Persistent disk-based cache for Google Places API responses.
Avoids paying for the same API call twice by caching responses on disk.
"""

import hashlib
import json
import os
import time
from typing import Optional


class APIResponseCache:
    """
    Disk-based cache for API responses. Keyed by request URL + params hash.
    Default TTL is 24 hours (API results don't change frequently).
    """

    def __init__(self, cache_dir: str = "output/.api_cache", ttl_seconds: int = 86400):
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_seconds
        os.makedirs(cache_dir, exist_ok=True)

    def _cache_key(self, url: str, params: dict) -> str:
        """Generate a deterministic cache key from URL and params."""
        # Remove the API key from params before hashing (same result regardless of key)
        clean_params = {k: v for k, v in sorted(params.items()) if k != 'key'}
        raw = f"{url}|{json.dumps(clean_params, sort_keys=True)}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _cache_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def get(self, url: str, params: dict) -> Optional[dict]:
        """
        Retrieve a cached response if it exists and hasn't expired.
        Returns None on cache miss.
        """
        key = self._cache_key(url, params)
        path = self._cache_path(key)

        if not os.path.exists(path):
            return None

        try:
            with open(path, 'r', encoding='utf-8') as f:
                entry = json.load(f)

            if time.time() - entry.get('cached_at', 0) > self.ttl_seconds:
                os.remove(path)
                return None

            return entry.get('response')
        except (json.JSONDecodeError, OSError):
            return None

    def put(self, url: str, params: dict, response: dict):
        """Store an API response in the cache."""
        key = self._cache_key(url, params)
        path = self._cache_path(key)

        entry = {
            'cached_at': time.time(),
            'url': url,
            'response': response
        }

        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(entry, f)
        except OSError:
            pass  # Silently fail on cache write errors

    def clear(self):
        """Remove all cached responses."""
        for filename in os.listdir(self.cache_dir):
            if filename.endswith('.json'):
                try:
                    os.remove(os.path.join(self.cache_dir, filename))
                except OSError:
                    pass

    @property
    def size(self) -> int:
        """Number of cached entries."""
        try:
            return len([f for f in os.listdir(self.cache_dir) if f.endswith('.json')])
        except OSError:
            return 0
