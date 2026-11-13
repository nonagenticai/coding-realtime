"""
Performance Monitoring Service

Tracks performance metrics for code analysis operations across languages.
Provides insights into analysis speed, cache effectiveness, and system performance.
"""

from __future__ import annotations

import logging
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class PerformanceOperationType(StrEnum):
    """Types of operations to monitor."""

    PARSE_FILE = "parse_file"
    ANALYZE_COMPLEXITY = "analyze_complexity"
    EXTRACT_ENTITIES = "extract_entities"
    EXTRACT_RELATIONSHIPS = "extract_relationships"
    SEMANTIC_ANALYSIS = "semantic_analysis"
    REALTIME_ANALYSIS = "realtime_analysis"
    BATCH_ANALYSIS = "batch_analysis"
    CACHE_OPERATION = "cache_operation"


# Type alias for backward compatibility
OperationType = PerformanceOperationType


@dataclass
class OperationMetric:
    """A single operation performance metric."""

    language: str
    operation_type: PerformanceOperationType
    duration_seconds: float
    timestamp: datetime
    success: bool = True
    file_size_bytes: int | None = None
    entity_count: int | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ServiceLanguageStats:
    """Performance statistics for a specific language."""

    language: str
    total_operations: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    min_duration: float = float("inf")
    max_duration: float = 0.0
    success_rate: float = 1.0
    operations_per_second: float = 0.0
    recent_operations: int = 0


@dataclass
class SystemStats:
    """Overall system performance statistics."""

    total_operations: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    operations_per_second: float = 0.0
    cache_hit_rate: float = 0.0
    success_rate: float = 1.0
    active_languages: int = 0
    uptime_seconds: float = 0.0


class ServicePerformanceMonitor:
    """Service for monitoring and tracking performance metrics."""

    def __init__(self, max_history: int = 1000):
        """Initialize the performance monitor.

        Args:
            max_history: Maximum number of metrics to keep in memory
        """
        self.max_history = max_history
        self.start_time = time.time()

        # Store recent metrics for each language
        self.metrics: dict[str, deque[OperationMetric]] = defaultdict(
            lambda: deque(maxlen=max_history)
        )

        # Aggregated statistics
        self.language_stats: dict[str, ServiceLanguageStats] = {}
        self.operation_stats: dict[str, dict[str, Any]] = defaultdict(dict)

        # Performance tracking
        self.total_operations = 0
        self.total_duration = 0.0
        self.success_count = 0

        logger.info("Performance monitor initialized")

    async def track_language_operation(
        self,
        language: str,
        operation_type: str,
        duration_seconds: float,
        success: bool = True,
        file_size_bytes: int | None = None,
        entity_count: int | None = None,
        error_message: str | None = None,
        **metadata,
    ) -> None:
        """Track a language operation performance metric.

        Args:
            language: Programming language being analyzed
            operation_type: Type of operation (parse, analyze, etc.)
            duration_seconds: Operation duration in seconds
            success: Whether the operation succeeded
            file_size_bytes: Size of the analyzed file
            entity_count: Number of entities found
            error_message: Error message if operation failed
            **metadata: Additional metadata
        """

        try:
            # Convert operation type
            op_type = PerformanceOperationType(operation_type)
        except ValueError:
            op_type = OperationType.PARSE_FILE

        # Create metric
        metric = OperationMetric(
            language=language,
            operation_type=op_type,
            duration_seconds=duration_seconds,
            timestamp=datetime.now(UTC),
            success=success,
            file_size_bytes=file_size_bytes,
            entity_count=entity_count,
            error_message=error_message,
            metadata=metadata,
        )

        # Store metric
        self.metrics[language].append(metric)

        # Update aggregated statistics
        self._update_language_stats(language)
        self._update_operation_stats(language, op_type, metric)

        # Update global counters
        self.total_operations += 1
        self.total_duration += duration_seconds
        if success:
            self.success_count += 1

        logger.debug("Tracked %s %s: %.3fs", language, operation_type, duration_seconds)

    def _update_language_stats(self, language: str) -> None:
        """Update statistics for a specific language."""

        language_metrics = self.metrics[language]
        if not language_metrics:
            return

        # Calculate statistics
        durations = [m.duration_seconds for m in language_metrics]
        successes = [m.success for m in language_metrics]

        # Recent operations (last 100)
        recent_metrics = list(language_metrics)[-100:]
        recent_durations = [m.duration_seconds for m in recent_metrics]

        stats = ServiceLanguageStats(
            language=language,
            total_operations=len(language_metrics),
            total_duration=sum(durations),
            avg_duration=statistics.mean(durations) if durations else 0.0,
            min_duration=min(durations) if durations else 0.0,
            max_duration=max(durations) if durations else 0.0,
            success_rate=sum(successes) / len(successes) if successes else 1.0,
            operations_per_second=(
                len(recent_durations) / sum(recent_durations)
                if recent_durations and sum(recent_durations) > 0
                else 0.0
            ),
            recent_operations=len(recent_metrics),
        )

        self.language_stats[language] = stats

    def _update_operation_stats(
        self,
        language: str,
        operation_type: PerformanceOperationType,
        metric: OperationMetric,
    ) -> None:
        """Update statistics for a specific operation type."""

        op_key = f"{language}_{operation_type.value}"

        if op_key not in self.operation_stats:
            self.operation_stats[op_key] = {
                "count": 0,
                "total_duration": 0.0,
                "avg_duration": 0.0,
                "success_count": 0,
                "last_execution": None,
            }

        stats = self.operation_stats[op_key]
        stats["count"] += 1
        stats["total_duration"] += metric.duration_seconds
        stats["avg_duration"] = stats["total_duration"] / stats["count"]

        if metric.success:
            stats["success_count"] += 1

        stats["last_execution"] = metric.timestamp.isoformat()
        stats["success_rate"] = stats["success_count"] / stats["count"]

    async def get_language_stats(self, language: str) -> ServiceLanguageStats | None:
        """Get performance statistics for a specific language."""
        return self.language_stats.get(language)

    async def get_all_language_stats(self) -> dict[str, ServiceLanguageStats]:
        """Get performance statistics for all languages."""
        return self.language_stats.copy()

    async def get_system_stats(self) -> SystemStats:
        """Get overall system performance statistics."""

        uptime = time.time() - self.start_time

        return SystemStats(
            total_operations=self.total_operations,
            total_duration=self.total_duration,
            avg_duration=self.total_duration / max(self.total_operations, 1),
            operations_per_second=self.total_operations / max(uptime, 1),
            success_rate=self.success_count / max(self.total_operations, 1),
            active_languages=len(self.language_stats),
            uptime_seconds=uptime,
        )

    async def get_operation_stats(self, language: str | None = None) -> dict[str, Any]:
        """Get statistics for specific operations.

        Args:
            language: Filter by language (optional)

        Returns:
            Dictionary of operation statistics
        """

        if language:
            # Filter by language
            filtered_stats = {
                key: stats
                for key, stats in self.operation_stats.items()
                if key.startswith(f"{language}_")
            }
            return filtered_stats

        return self.operation_stats.copy()

    async def get_recent_metrics(
        self, language: str | None = None, limit: int = 100
    ) -> list[OperationMetric]:
        """Get recent performance metrics.

        Args:
            language: Filter by language (optional)
            limit: Maximum number of metrics to return

        Returns:
            List of recent metrics
        """

        if language:
            return list(self.metrics[language])[-limit:]

        # Get recent metrics from all languages
        all_metrics = []
        for lang_metrics in self.metrics.values():
            all_metrics.extend(list(lang_metrics))

        # Sort by timestamp and return most recent
        all_metrics.sort(key=lambda m: m.timestamp, reverse=True)
        return all_metrics[:limit]

    async def get_performance_summary(self) -> dict[str, Any]:
        """Get a comprehensive performance summary."""

        system_stats = await self.get_system_stats()
        language_stats = await self.get_all_language_stats()

        # Calculate top performers
        top_languages = sorted(
            language_stats.items(),
            key=lambda x: x[1].operations_per_second,
            reverse=True,
        )[:5]

        # Calculate slowest operations
        slowest_operations: list[dict[str, Any]] = []
        for language, lang_stats in language_stats.items():
            if lang_stats.max_duration > 0:
                slowest_operations.append(
                    {
                        "language": language,
                        "max_duration": lang_stats.max_duration,
                        "avg_duration": lang_stats.avg_duration,
                    }
                )

        slowest_operations.sort(key=lambda x: x["max_duration"], reverse=True)
        slowest_operations = slowest_operations[:5]

        return {
            "system": {
                "total_operations": system_stats.total_operations,
                "avg_duration": system_stats.avg_duration,
                "operations_per_second": system_stats.operations_per_second,
                "success_rate": system_stats.success_rate,
                "uptime_hours": system_stats.uptime_seconds / 3600,
                "active_languages": system_stats.active_languages,
            },
            "top_performing_languages": [
                {
                    "language": lang,
                    "operations_per_second": stats.operations_per_second,
                    "success_rate": stats.success_rate,
                    "total_operations": stats.total_operations,
                }
                for lang, stats in top_languages
                if stats.operations_per_second > 0
            ],
            "slowest_operations": slowest_operations,
            "languages": {
                lang: {
                    "total_operations": stats.total_operations,
                    "avg_duration": stats.avg_duration,
                    "success_rate": stats.success_rate,
                    "operations_per_second": stats.operations_per_second,
                }
                for lang, stats in language_stats.items()
            },
        }

    async def clear_metrics(self, language: str | None = None) -> None:
        """Clear performance metrics.

        Args:
            language: Language to clear (clears all if None)
        """

        if language:
            if language in self.metrics:
                self.metrics[language].clear()
            if language in self.language_stats:
                del self.language_stats[language]

            # Clear operation stats for this language
            keys_to_remove = [
                key for key in self.operation_stats.keys() if key.startswith(f"{language}_")
            ]
            for key in keys_to_remove:
                del self.operation_stats[key]

            logger.info("Cleared performance metrics for %s", language)
        else:
            self.metrics.clear()
            self.language_stats.clear()
            self.operation_stats.clear()
            self.total_operations = 0
            self.total_duration = 0.0
            self.success_count = 0

            logger.info("Cleared all performance metrics")


# Global performance monitor instance
_performance_monitor: ServicePerformanceMonitor | None = None


def get_performance_monitor() -> ServicePerformanceMonitor:
    """Get or create the global performance monitor."""
    global _performance_monitor

    if _performance_monitor is None:
        _performance_monitor = ServicePerformanceMonitor()
        logger.info("Initialized global performance monitor")

    return _performance_monitor


async def track_operation(
    language: str, operation_type: str, duration_seconds: float, **kwargs
) -> None:
    """Convenience function to track an operation."""
    monitor = get_performance_monitor()
    await monitor.track_language_operation(language, operation_type, duration_seconds, **kwargs)
