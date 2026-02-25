"""
Tests for API response cache and scraper optimizations.
"""

import json
import os
import shutil
import time
import pytest
import asyncio

# Test the API cache
from api_cache import APIResponseCache


class TestAPIResponseCache:
    """Tests for the persistent disk cache."""

    def setup_method(self):
        self.cache_dir = "/tmp/test_api_cache"
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)
        self.cache = APIResponseCache(cache_dir=self.cache_dir, ttl_seconds=60)

    def teardown_method(self):
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)

    def test_cache_miss(self):
        """Cache miss returns None."""
        result = self.cache.get("https://example.com/api", {"q": "test"})
        assert result is None

    def test_cache_put_and_get(self):
        """Put then get returns the cached response."""
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {"query": "restaurants", "location": "40.7,-74.0", "radius": 1000, "key": "SECRET"}
        response = {"status": "OK", "results": [{"name": "Test Place"}]}

        self.cache.put(url, params, response)
        cached = self.cache.get(url, params)

        assert cached is not None
        assert cached["status"] == "OK"
        assert cached["results"][0]["name"] == "Test Place"

    def test_cache_ignores_api_key(self):
        """Cache key doesn't depend on the API key value."""
        url = "https://maps.googleapis.com/api"
        params_a = {"query": "test", "key": "KEY_A"}
        params_b = {"query": "test", "key": "KEY_B"}
        response = {"status": "OK"}

        self.cache.put(url, params_a, response)
        cached = self.cache.get(url, params_b)

        assert cached is not None
        assert cached["status"] == "OK"

    def test_cache_ttl_expiry(self):
        """Expired entries are not returned."""
        cache = APIResponseCache(cache_dir=self.cache_dir, ttl_seconds=1)
        url = "https://example.com/api"
        params = {"q": "test"}
        cache.put(url, params, {"status": "OK"})

        # Entry is fresh
        assert cache.get(url, params) is not None

        # Wait for expiry
        time.sleep(1.5)
        assert cache.get(url, params) is None

    def test_cache_different_params(self):
        """Different params produce different cache entries."""
        url = "https://example.com/api"
        self.cache.put(url, {"q": "a"}, {"result": "A"})
        self.cache.put(url, {"q": "b"}, {"result": "B"})

        assert self.cache.get(url, {"q": "a"})["result"] == "A"
        assert self.cache.get(url, {"q": "b"})["result"] == "B"

    def test_cache_size(self):
        """Cache size reflects number of entries."""
        assert self.cache.size == 0
        self.cache.put("https://ex.com", {"a": 1}, {"r": 1})
        assert self.cache.size == 1
        self.cache.put("https://ex.com", {"a": 2}, {"r": 2})
        assert self.cache.size == 2

    def test_cache_clear(self):
        """Clear removes all entries."""
        self.cache.put("https://ex.com", {"a": 1}, {"r": 1})
        self.cache.put("https://ex.com", {"a": 2}, {"r": 2})
        assert self.cache.size == 2
        self.cache.clear()
        assert self.cache.size == 0


class TestScraperConfig:
    """Tests for scraper configuration and usage tracking."""

    def test_api_usage_stats_initial(self):
        from scraper import APIUsageStats
        stats = APIUsageStats()
        assert stats.text_search_calls == 0
        assert stats.place_details_calls == 0
        assert stats.estimated_cost_usd == 0.0
        assert stats.estimated_savings_usd == 0.0

    def test_api_usage_stats_cost_calculation(self):
        from scraper import APIUsageStats
        stats = APIUsageStats(text_search_calls=1000, place_details_calls=1000)
        # 1000 text searches at $32/1000 + 1000 details at $17/1000
        assert stats.estimated_cost_usd == 49.0

    def test_api_usage_stats_savings(self):
        from scraper import APIUsageStats
        stats = APIUsageStats(
            text_search_calls=500,
            text_search_cache_hits=500,
            place_details_calls=500,
            place_details_cache_hits=500
        )
        assert stats.estimated_savings_usd == stats.estimated_cost_usd

    def test_api_usage_summary(self):
        from scraper import APIUsageStats
        stats = APIUsageStats(text_search_calls=10, text_search_cache_hits=5)
        summary = stats.summary()
        assert "10 text searches" in summary
        assert "5 cache hits" in summary
        assert "Est. cost:" in summary

    def test_scraper_has_usage_tracker(self):
        """GoogleMapsScraper should have a usage stats tracker."""
        from scraper import GoogleMapsScraper, APIUsageStats
        scraper = GoogleMapsScraper()
        assert isinstance(scraper.usage, APIUsageStats)

    def test_scraper_has_api_cache(self):
        """GoogleMapsScraper should have an API response cache."""
        from scraper import GoogleMapsScraper
        from api_cache import APIResponseCache
        scraper = GoogleMapsScraper()
        assert isinstance(scraper._api_cache, APIResponseCache)


class TestTextSearchQueryOptimization:
    """Verify the text search no longer embeds 'near lat,lng' in the query."""

    def test_search_query_does_not_contain_near(self):
        """The query param should NOT include 'near lat,lng' - location biasing
        is done via the location+radius params instead."""
        import inspect
        from scraper import GoogleMapsScraper
        source = inspect.getsource(GoogleMapsScraper.search_tile)
        # The old pattern was: f"{query} near {tile_center_lat},{tile_center_lng}"
        # The new pattern is: 'query': query  (just the search term)
        assert "'query': query" in source or '"query": query' in source
        assert "near {tile_center_lat}" not in source
