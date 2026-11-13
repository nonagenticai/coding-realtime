from __future__ import annotations

from unittest.mock import patch

from services import performance_optimizer as optimizer_module
from services.performance_optimizer import (
    SystemPerformanceOptimizer,
    get_system_performance_optimizer,
)


async def test_performance_optimizer_applies_cleanup_and_singleton():
    optimizer_module._performance_optimizer = None
    optimizer = SystemPerformanceOptimizer()
    optimizer.performance_cache = {
        "old": (0.0, {"cached": True}),
        "new": (7200.0, {"cached": False}),
    }

    with patch(
        "services.performance_optimizer.time.time",
        side_effect=[7200.0, 7200.0, 7201.0, 7201.5],
    ):
        result = await optimizer.optimize_system_performance()

    assert result.optimization_applied is True
    assert result.performance_gain == 8.0
    assert result.resource_savings["memory_freed_mb"] == 0.1
    assert "Applied 1 optimizations" in result.recommendations[0]
    assert "old" not in optimizer.performance_cache

    singleton = get_system_performance_optimizer()
    assert singleton is get_system_performance_optimizer()


async def test_performance_optimizer_handles_failure():
    optimizer = SystemPerformanceOptimizer()
    optimizer.performance_cache = {"bad": object()}  # invalid shape for tuple unpacking

    result = await optimizer.optimize_system_performance()

    assert result.optimization_applied is False
    assert result.performance_gain == 0.0
    assert "Optimization error:" in result.recommendations[0]
