from __future__ import annotations

import math

from services import performance_monitor as pm_module
from services.performance_monitor import (
    ServicePerformanceMonitor,
    get_performance_monitor,
    track_operation,
)

# ---- Split from monster test: test_performance_monitor_summary_and_filtered_operations ----


async def test_track_operations_records_per_language():
    """Each tracked operation is stored in the correct language's metric deque."""
    monitor = ServicePerformanceMonitor(max_history=10)
    await monitor.track_language_operation("python", "parse_file", 1.0, success=True)
    await monitor.track_language_operation("python", "not-a-real-op", 3.0, success=False)
    await monitor.track_language_operation("go", "analyze_complexity", 2.0, success=True)

    assert len(monitor.metrics["python"]) == 2
    assert len(monitor.metrics["go"]) == 1

    # Verify python metrics have correct durations and success flags
    py_metrics = list(monitor.metrics["python"])
    assert py_metrics[0].duration_seconds == 1.0
    assert py_metrics[0].success is True
    assert py_metrics[1].duration_seconds == 3.0
    assert py_metrics[1].success is False

    # Verify go metric
    go_metrics = list(monitor.metrics["go"])
    assert go_metrics[0].duration_seconds == 2.0
    assert go_metrics[0].success is True


async def test_get_language_stats_returns_correct_counts():
    """get_language_stats returns accurate aggregates for a given language."""
    monitor = ServicePerformanceMonitor(max_history=10)
    await monitor.track_language_operation("python", "parse_file", 1.0, success=True)
    await monitor.track_language_operation("python", "not-a-real-op", 3.0, success=False)

    stats = await monitor.get_language_stats("python")
    assert stats is not None
    assert stats.total_operations == 2
    assert stats.total_duration == 4.0
    assert stats.avg_duration == 2.0
    assert stats.min_duration == 1.0
    assert stats.max_duration == 3.0
    assert stats.success_rate == 0.5
    # ops_per_second = 2 / (1.0 + 3.0) = 0.5
    assert stats.operations_per_second == 0.5


async def test_get_recent_metrics_filtered_by_language():
    """get_recent_metrics with a language filter returns only that language's metrics."""
    monitor = ServicePerformanceMonitor(max_history=10)
    await monitor.track_language_operation("python", "parse_file", 1.0, success=True)
    await monitor.track_language_operation("python", "not-a-real-op", 3.0, success=False)
    await monitor.track_language_operation("go", "analyze_complexity", 2.0, success=True)

    recent_python = await monitor.get_recent_metrics("python", limit=5)
    assert len(recent_python) == 2
    assert all(m.language == "python" for m in recent_python)
    assert recent_python[0].duration_seconds == 1.0
    assert recent_python[1].duration_seconds == 3.0


async def test_get_recent_metrics_returns_all_when_unfiltered():
    """get_recent_metrics without a language returns metrics from all languages, limited and sorted."""
    monitor = ServicePerformanceMonitor(max_history=10)
    await monitor.track_language_operation("python", "parse_file", 1.0, success=True)
    await monitor.track_language_operation("python", "not-a-real-op", 3.0, success=False)
    await monitor.track_language_operation("go", "analyze_complexity", 2.0, success=True)

    # Limit to 2 — should return the 2 most recent by timestamp (descending)
    recent_all = await monitor.get_recent_metrics(limit=2)
    assert len(recent_all) == 2
    # Most-recent-first ordering: timestamps must be non-increasing
    assert recent_all[0].timestamp >= recent_all[1].timestamp


async def test_get_summary_includes_all_tracked_languages():
    """get_performance_summary reflects every tracked language and correct totals."""
    monitor = ServicePerformanceMonitor(max_history=10)
    await monitor.track_language_operation("python", "parse_file", 1.0, success=True)
    await monitor.track_language_operation("python", "not-a-real-op", 3.0, success=False)
    await monitor.track_language_operation("go", "analyze_complexity", 2.0, success=True)

    summary = await monitor.get_performance_summary()

    assert summary["system"]["total_operations"] == 3
    assert summary["system"]["active_languages"] == 2
    assert set(summary["languages"]) == {"python", "go"}
    assert summary["languages"]["python"]["total_operations"] == 2
    assert summary["languages"]["go"]["total_operations"] == 1

    # slowest_operations sorted by max_duration desc: python(3.0) before go(2.0)
    assert len(summary["slowest_operations"]) == 2
    assert summary["slowest_operations"][0]["language"] == "python"
    assert summary["slowest_operations"][0]["max_duration"] == 3.0
    assert summary["slowest_operations"][1]["language"] == "go"
    assert summary["slowest_operations"][1]["max_duration"] == 2.0


async def test_clear_metrics_removes_only_target_language():
    """Clearing one language leaves other languages intact."""
    monitor = ServicePerformanceMonitor(max_history=10)
    await monitor.track_language_operation("python", "parse_file", 1.0, success=True)
    await monitor.track_language_operation("python", "not-a-real-op", 3.0, success=False)
    await monitor.track_language_operation("go", "analyze_complexity", 2.0, success=True)

    await monitor.clear_metrics("python")

    assert await monitor.get_language_stats("python") is None
    assert len(list(monitor.metrics["python"])) == 0

    # go remains untouched
    go_stats = await monitor.get_language_stats("go")
    assert go_stats is not None
    assert go_stats.total_operations == 1
    assert go_stats.total_duration == 2.0


# ---- Fixed weak tests ----


async def test_language_stats_operations_per_second():
    """operations_per_second equals count / sum(durations) for recent operations."""
    monitor = ServicePerformanceMonitor()
    await monitor.track_language_operation("go", "parse_file", 2.0)

    stats = await monitor.get_language_stats("go")
    assert stats is not None
    # 1 operation / 2.0s = 0.5 ops/s
    assert math.isclose(stats.operations_per_second, 0.5, rel_tol=1e-9)


async def test_summary_top_performing_filters_zero_ops_per_sec():
    """top_performing_languages includes only languages with ops_per_second > 0."""
    monitor = ServicePerformanceMonitor()
    # duration 0.001 => ops_per_second = 1/0.001 = 1000.0 (non-zero, so included)
    await monitor.track_language_operation("python", "parse_file", 0.001)

    summary = await monitor.get_performance_summary()

    top = summary["top_performing_languages"]
    assert len(top) == 1
    assert top[0]["language"] == "python"
    assert math.isclose(top[0]["operations_per_second"], 1000.0, rel_tol=1e-9)
    assert top[0]["total_operations"] == 1
    assert top[0]["success_rate"] == 1.0


# ---- Kept as-is: good existing tests ----


async def test_clear_metrics_removes_operation_stats_for_language():
    """Clearing a language removes its operation_stats keys."""
    monitor = ServicePerformanceMonitor()
    await monitor.track_language_operation("python", "parse_file", 0.1)
    await monitor.track_language_operation("python", "analyze_complexity", 0.2)
    await monitor.track_language_operation("go", "parse_file", 0.3)

    await monitor.clear_metrics("python")

    ops = await monitor.get_operation_stats()
    python_keys = [k for k in ops if k.startswith("python_")]
    go_keys = [k for k in ops if k.startswith("go_")]
    assert python_keys == []
    assert len(go_keys) == 1


# ---- RT-3 additions: global singleton & track_operation ----


async def test_get_performance_monitor_singleton():
    """get_performance_monitor returns the same instance on repeated calls."""
    pm_module._performance_monitor = None
    try:
        first = get_performance_monitor()
        second = get_performance_monitor()
        assert first is second
    finally:
        pm_module._performance_monitor = None


async def test_track_operation_convenience():
    """track_operation() delegates to the global monitor."""
    pm_module._performance_monitor = None
    try:
        await track_operation("python", "parse_file", 0.1, success=True)
        monitor = get_performance_monitor()
        assert monitor.total_operations == 1
    finally:
        pm_module._performance_monitor = None


async def test_unknown_op_type_fallback_via_track_language_operation():
    """Invalid op type string falls back to PARSE_FILE; stored under parse_file key."""
    monitor = ServicePerformanceMonitor()
    await monitor.track_language_operation("python", "nonexistent_operation", 0.5)
    ops = await monitor.get_operation_stats("python")
    # Fallback maps to parse_file
    assert "python_parse_file" in ops
    assert ops["python_parse_file"]["count"] == 1
