"""Tests for ServicePerformanceMonitor."""

import pytest

from services.performance_monitor import (
    ServiceLanguageStats,
    ServicePerformanceMonitor,
    SystemStats,
)


@pytest.fixture
def monitor():
    return ServicePerformanceMonitor()


async def test_track_operation(monitor):
    await monitor.track_language_operation("python", "parse_file", 0.5)
    assert monitor.total_operations == 1
    assert monitor.total_duration == 0.5


async def test_get_language_stats(monitor):
    await monitor.track_language_operation("python", "parse_file", 0.5)
    await monitor.track_language_operation("python", "parse_file", 1.0)
    stats = await monitor.get_language_stats("python")
    assert isinstance(stats, ServiceLanguageStats)
    assert stats.total_operations == 2
    assert stats.avg_duration == pytest.approx(0.75)


async def test_get_language_stats_unknown_returns_none(monitor):
    assert await monitor.get_language_stats("cobol") is None


async def test_get_system_stats(monitor):
    await monitor.track_language_operation("python", "parse_file", 0.5, success=True)
    await monitor.track_language_operation("javascript", "parse_file", 1.0, success=False)
    stats = await monitor.get_system_stats()
    assert isinstance(stats, SystemStats)
    assert stats.total_operations == 2
    assert stats.active_languages == 2
    assert stats.success_rate == 0.5


async def test_get_all_language_stats(monitor):
    await monitor.track_language_operation("python", "parse_file", 0.5)
    await monitor.track_language_operation("javascript", "parse_file", 1.0)
    all_stats = await monitor.get_all_language_stats()
    assert "python" in all_stats
    assert "javascript" in all_stats


async def test_clear_metrics(monitor):
    await monitor.track_language_operation("python", "parse_file", 0.5)
    await monitor.clear_metrics()
    assert monitor.total_operations == 0
    assert await monitor.get_language_stats("python") is None


# ---- RT-3 additions ----


async def test_unknown_op_type_falls_back_to_parse_file(monitor):
    """An invalid operation_type string should fall back to PARSE_FILE."""
    await monitor.track_language_operation("python", "totally_bogus_op", 0.42)
    assert monitor.total_operations == 1
    # The metric should be stored under parse_file due to fallback
    ops = await monitor.get_operation_stats("python")
    assert "python_parse_file" in ops


async def test_track_language_operation_with_metadata(monitor):
    """Metadata kwargs are stored on the metric."""
    await monitor.track_language_operation(
        "rust",
        "parse_file",
        0.3,
        success=True,
        file_size_bytes=1024,
        entity_count=10,
        custom_key="custom_val",
    )
    metrics = await monitor.get_recent_metrics("rust")
    assert len(metrics) == 1
    m = metrics[0]
    assert m.file_size_bytes == 1024
    assert m.entity_count == 10
    assert m.metadata["custom_key"] == "custom_val"


async def test_track_language_operation_failure(monitor):
    """Failed operations record error_message and update success_count."""
    await monitor.track_language_operation(
        "go",
        "parse_file",
        1.0,
        success=False,
        error_message="parse error",
    )
    assert monitor.total_operations == 1
    assert monitor.success_count == 0
    stats = await monitor.get_language_stats("go")
    assert stats.success_rate == 0.0
    ops = await monitor.get_operation_stats("go")
    assert ops["go_parse_file"]["success_rate"] == 0.0


async def test_update_language_stats_empty_metrics(monitor):
    """_update_language_stats returns early when language has no metrics."""
    # Calling directly with a language that has an empty deque
    monitor._update_language_stats("empty_lang")
    assert "empty_lang" not in monitor.language_stats


async def test_update_operation_stats_creates_and_updates(monitor):
    """_update_operation_stats initializes then increments stats."""
    await monitor.track_language_operation("java", "extract_entities", 0.5, success=True)
    await monitor.track_language_operation("java", "extract_entities", 1.5, success=False)

    ops = await monitor.get_operation_stats("java")
    java_extract = ops["java_extract_entities"]
    assert java_extract["count"] == 2
    assert java_extract["avg_duration"] == pytest.approx(1.0)
    assert java_extract["success_count"] == 1
    assert java_extract["success_rate"] == 0.5
    assert java_extract["last_execution"] is not None


async def test_get_operation_stats_no_filter(monitor):
    """get_operation_stats with no language returns all stats."""
    await monitor.track_language_operation("python", "parse_file", 0.1)
    await monitor.track_language_operation("go", "parse_file", 0.2)
    all_ops = await monitor.get_operation_stats()
    assert "python_parse_file" in all_ops
    assert "go_parse_file" in all_ops


async def test_get_recent_metrics_all_languages(monitor):
    """get_recent_metrics with no language returns sorted metrics from all languages."""
    await monitor.track_language_operation("python", "parse_file", 0.1)
    await monitor.track_language_operation("go", "parse_file", 0.2)
    await monitor.track_language_operation("rust", "parse_file", 0.3)
    recent = await monitor.get_recent_metrics(limit=2)
    assert len(recent) == 2
    # Should be sorted most recent first
    assert recent[0].timestamp >= recent[1].timestamp


async def test_get_performance_summary(monitor):
    """get_performance_summary includes system, languages, top_performing, slowest."""
    await monitor.track_language_operation("python", "parse_file", 0.5, success=True)
    await monitor.track_language_operation("go", "analyze_complexity", 2.0, success=True)
    summary = await monitor.get_performance_summary()

    assert summary["system"]["total_operations"] == 2
    assert summary["system"]["active_languages"] == 2
    assert "python" in summary["languages"]
    assert "go" in summary["languages"]
    assert isinstance(summary["top_performing_languages"], list)
    assert isinstance(summary["slowest_operations"], list)


async def test_clear_metrics_for_specific_language(monitor):
    """clear_metrics(language) only clears that language's data."""
    await monitor.track_language_operation("python", "parse_file", 0.5)
    await monitor.track_language_operation("go", "parse_file", 1.0)

    await monitor.clear_metrics("python")

    assert await monitor.get_language_stats("python") is None
    assert await monitor.get_language_stats("go") is not None
    # Global counters are NOT reset by per-language clear
    assert monitor.total_operations == 2
