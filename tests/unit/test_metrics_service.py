from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from services import metrics_service as metrics_module
from services.metrics_service import MetricsService, get_metrics_service


@pytest.fixture(autouse=True)
def _reset_global_metrics_service():
    metrics_module._metrics_service = None
    yield
    metrics_module._metrics_service = None


async def test_metrics_service_records_and_reports_analytics(tmp_path: Path):
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    record_id = await service.record_test_execution(
        {
            "user_id": "u1",
            "test_id": "t1",
            "success": True,
            "language": "python",
            "execution_time": 1.25,
            "metadata": {"suite": "unit"},
        }
    )
    assert record_id

    quality_id = await service.record_quality_analysis(
        {
            "user_id": "u1",
            "analysis_id": "a1",
            "overall_score": 91.5,
            "total_issues": 3,
            "language": "python",
            "critical_issues": 1,
            "major_issues": 1,
            "minor_issues": 1,
            "metadata": {"tool": "lint"},
        }
    )
    assert quality_id

    test_stats = await service.get_test_execution_analytics(user_id="u1", days=30)
    quality_stats = await service.get_quality_analysis_analytics(user_id="u1", days=30)

    assert test_stats["total_executions"] == 1
    assert test_stats["success_rate"] == 100.0
    assert test_stats["language_breakdown"] == {"python": 1}
    assert quality_stats["total_analyses"] == 1
    assert quality_stats["avg_quality_score"] == 91.5
    assert quality_stats["issue_breakdown"]["total"] == 3
    assert service.get_metrics_service() is service


async def test_metrics_service_handles_empty_and_error_paths(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    empty_test_stats = await service.get_test_execution_analytics(user_id="missing", days=1)
    empty_quality_stats = await service.get_quality_analysis_analytics(user_id="missing", days=1)
    assert empty_test_stats["total_executions"] == 0
    assert empty_quality_stats["total_analyses"] == 0

    def _boom(*_args, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(metrics_module.aiosqlite, "connect", _boom)
    assert "error" in await service.get_test_execution_analytics()
    assert "error" in await service.get_quality_analysis_analytics()


def test_metrics_service_initialization_and_singleton(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "metrics.db"

    with patch.object(MetricsService, "_initialize_database_schema_sync") as init_sync:
        service = MetricsService(str(db_path))
    init_sync.assert_called_once_with()
    assert service.metrics_db_path == str(db_path)

    with patch.object(metrics_module.os, "makedirs", side_effect=RuntimeError("no perms")):
        MetricsService(str(tmp_path / "nested" / "metrics.db"))

    first = get_metrics_service(str(db_path))
    second = get_metrics_service(str(tmp_path / "other.db"))
    assert first is second


# ---- RT-3 additions ----


def test_sync_schema_init_failure_logs_and_continues(tmp_path: Path):
    """_initialize_database_schema_sync exception is caught, service still created."""
    with patch.object(
        MetricsService,
        "_initialize_database_schema_sync",
        side_effect=RuntimeError("disk full"),
    ):
        service = MetricsService(str(tmp_path / "metrics.db"))
    # Service object exists despite init failure
    assert service.metrics_db_path == str(tmp_path / "metrics.db")


async def test_async_schema_init_failure_logs_and_continues(tmp_path: Path):
    """_initialize_database_schema catches exceptions gracefully."""
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    # Patch aiosqlite.connect to fail during async schema init
    with patch.object(
        metrics_module.aiosqlite, "connect", side_effect=RuntimeError("async init fail")
    ):
        # Should not raise
        await service._initialize_database_schema()


async def test_record_test_execution_defaults(tmp_path: Path):
    """record_test_execution fills missing fields with defaults."""
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    # Pass minimal dict — defaults should fill in
    record_id = await service.record_test_execution({})
    assert isinstance(record_id, str) and len(record_id) > 0

    stats = await service.get_test_execution_analytics()
    assert stats["total_executions"] == 1
    assert stats["success_rate"] == 0.0  # default success=False
    assert stats["language_breakdown"] == {"unknown": 1}


async def test_record_quality_analysis_defaults(tmp_path: Path):
    """record_quality_analysis fills missing fields with defaults."""
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    record_id = await service.record_quality_analysis({})
    assert isinstance(record_id, str) and len(record_id) > 0

    stats = await service.get_quality_analysis_analytics()
    assert stats["total_analyses"] == 1
    assert stats["avg_quality_score"] == 0  # overall_score is None


async def test_analytics_without_user_filter(tmp_path: Path):
    """Analytics methods return data when user_id is None."""
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    await service.record_test_execution(
        {"user_id": "u1", "test_id": "t1", "success": True, "language": "go", "execution_time": 2}
    )
    await service.record_test_execution(
        {
            "user_id": "u2",
            "test_id": "t2",
            "success": False,
            "language": "go",
            "execution_time": 3,
        }
    )

    # No user_id filter — should aggregate across all users
    stats = await service.get_test_execution_analytics(user_id=None, days=30)
    assert stats["total_executions"] == 2
    assert stats["success_rate"] == 50.0
    assert stats["user_id"] is None


async def test_db_failure_during_record_raises(tmp_path: Path, monkeypatch):
    """record_test_execution and record_quality_analysis raise on db failure."""
    db_path = tmp_path / "metrics.db"
    service = MetricsService(str(db_path))

    def _boom(*_a, **_k):
        raise RuntimeError("db write fail")

    monkeypatch.setattr(metrics_module.aiosqlite, "connect", _boom)

    with pytest.raises(RuntimeError, match="db write fail"):
        await service.record_test_execution({"user_id": "u1"})

    with pytest.raises(RuntimeError, match="db write fail"):
        await service.record_quality_analysis({"user_id": "u1"})


def test_singleton_returns_same_instance(tmp_path: Path):
    """get_metrics_service returns the same singleton on repeated calls."""
    first = get_metrics_service(str(tmp_path / "a.db"))
    second = get_metrics_service(str(tmp_path / "b.db"))
    assert first is second
