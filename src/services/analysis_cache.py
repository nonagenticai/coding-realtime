"""Analysis cache adapter for the coding-realtime service.

HTTP adapter version of the monolith's analysis_cache module.  Provides
the same ``get_analysis_cache()`` function and ``AnalysisCache`` interface.

Locally it keeps a thin in-memory dict for deduplication.  Statistics
are fetched from the coding-analysis service on demand.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class AnalysisCache:
    """Lightweight analysis cache adapter.

    The real caching (SQLite-backed, LRU eviction, TTL) lives inside
    the coding-analysis microservice.  This adapter provides:

    - A minimal in-memory content-hash cache for quick deduplication
      within a single realtime session.
    - Local hit/miss statistics via get_stats().

    coding-analysis no longer exposes cache statistics (its aiosqlite scan
    cache and /api/quality/cache/* routes were deleted in coding-analysis#106),
    so the stats reported here are this adapter's own counters and nothing else.
    """

    def __init__(self, max_local_entries: int = 500) -> None:
        self._local_cache: dict[str, str] = {}  # file_path -> content_hash
        self._max_local_entries = max_local_entries
        self._hits = 0
        self._misses = 0
        self._initialized = False
        self._start_time = time.monotonic()

    async def initialize(self) -> None:
        """No-op -- the real cache lives in coding-analysis."""
        self._initialized = True

    async def get_stats(self) -> dict[str, Any]:
        """Return this adapter's local content-hash cache statistics.

        Keys match the monolith's get_stats() shape; ``source`` says where the
        numbers come from so a consumer never mistakes them for upstream stats.
        """
        total = self._hits + self._misses
        hit_rate = self._hits / max(total, 1)
        return {
            "total_entries": len(self._local_cache),
            "total_hits": self._hits,
            "total_misses": self._misses,
            "total_requests": total,
            "hit_rate": hit_rate,
            "miss_rate": 1 - hit_rate,
            "cache_size_mb": 0.0,
            "languages": {},
            "uptime_seconds": time.monotonic() - self._start_time,
            "cache_directory": "",
            "source": "local_adapter",
        }

    def has_content_changed(self, file_path: str, content: str) -> bool:
        """Check whether file content has changed since last seen.

        This is a local-only fast check used by the realtime connection
        manager to avoid re-analyzing unchanged files.

        Returns:
            True if the content is new/changed, False if identical.
        """
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]
        previous_hash = self._local_cache.get(file_path)

        if previous_hash == content_hash:
            self._hits += 1
            return False

        # Evict oldest if over limit (simple FIFO via dict ordering)
        if len(self._local_cache) >= self._max_local_entries:
            oldest_key = next(iter(self._local_cache))
            del self._local_cache[oldest_key]

        self._local_cache[file_path] = content_hash
        self._misses += 1
        return True

    async def get_cache_stats(self) -> Any:
        """Alias for get_stats() for monolith interface compat."""
        return await self.get_stats()

    async def clear_cache(self, language: str | None = None) -> int:
        """Clear local cache entries."""
        count = len(self._local_cache)
        self._local_cache.clear()
        self._hits = 0
        self._misses = 0
        return count

    async def shutdown(self) -> None:
        """Clean up resources."""
        self._local_cache.clear()


# ------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------

_global_cache: AnalysisCache | None = None


def get_analysis_cache() -> AnalysisCache:
    """Get or create the global analysis cache adapter instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = AnalysisCache()
        logger.info("Initialized analysis cache adapter")
    return _global_cache
