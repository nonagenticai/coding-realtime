"""Tests for AnalysisCache."""

from unittest.mock import patch

import pytest

from services.analysis_cache import AnalysisCache


@pytest.fixture
def cache():
    return AnalysisCache(max_local_entries=5)


def test_has_content_changed_new_file(cache):
    assert cache.has_content_changed("test.py", "print('hello')") is True


def test_has_content_changed_same_content(cache):
    cache.has_content_changed("test.py", "print('hello')")
    assert cache.has_content_changed("test.py", "print('hello')") is False


def test_has_content_changed_different_content(cache):
    cache.has_content_changed("test.py", "print('hello')")
    assert cache.has_content_changed("test.py", "print('world')") is True


def test_eviction_when_over_limit(cache):
    """Cache with max 5 entries should evict oldest on 6th insert."""
    for i in range(6):
        cache.has_content_changed(f"file{i}.py", f"content-{i}")
    assert len(cache._local_cache) == 5


async def test_clear_cache(cache):
    cache.has_content_changed("test.py", "content")
    count = await cache.clear_cache()
    assert count == 1
    assert len(cache._local_cache) == 0


async def test_get_stats_fallback(cache):
    """Stats come from local counters even if the client factory would fail."""
    cache.has_content_changed("test.py", "content")
    cache.has_content_changed("test.py", "content")  # hit

    with patch(
        "clients.coding_analysis.get_coding_analysis_client",
        side_effect=Exception("unreachable"),
    ):
        stats = await cache.get_stats()
    assert stats["source"] == "local_adapter"
    assert stats["total_hits"] == 1
    assert stats["total_misses"] == 1
