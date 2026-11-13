"""Tests for WebSocketMetricsCollector."""

import pytest

from services.websocket_metrics import (
    ContainerMetrics,
    WebSocketMetricsCollector,
)


@pytest.fixture
def collector():
    return WebSocketMetricsCollector()


async def test_track_connection_opened(collector):
    await collector.track_connection_opened("c1", "conn-1")
    metrics = await collector.get_container_metrics("c1")
    assert metrics.connections.active_connections == 1
    assert metrics.connections.total_connections_created == 1


async def test_track_connection_closed(collector):
    await collector.track_connection_opened("c1", "conn-1")
    await collector.track_connection_closed("c1", "conn-1", duration_seconds=5.0)
    metrics = await collector.get_container_metrics("c1")
    assert metrics.connections.active_connections == 0
    assert metrics.connections.total_connections_closed == 1


async def test_track_message_sent(collector):
    await collector.track_message_sent("c1")
    metrics = await collector.get_container_metrics("c1")
    assert metrics.messages.total_messages_sent == 1


async def test_track_message_received(collector):
    await collector.track_message_received("c1")
    metrics = await collector.get_container_metrics("c1")
    assert metrics.messages.total_messages_received == 1


async def test_track_analysis_execution(collector):
    await collector.track_analysis_execution("c1", 50.0, success=True)
    await collector.track_analysis_execution("c1", 100.0, success=False)
    metrics = await collector.get_container_metrics("c1")
    assert metrics.analysis.total_analyses == 2
    assert metrics.analysis.successful_analyses == 1
    assert metrics.analysis.failed_analyses == 1


async def test_track_error(collector):
    await collector.track_error("c1", "ConnectionError", "timeout")
    metrics = await collector.get_container_metrics("c1")
    assert metrics.errors.total_errors == 1
    assert metrics.errors.errors_by_type["ConnectionError"] == 1


async def test_get_container_metrics_unknown_returns_empty(collector):
    metrics = await collector.get_container_metrics("unknown")
    assert isinstance(metrics, ContainerMetrics)
    assert metrics.connections.active_connections == 0


async def test_get_global_stats(collector):
    await collector.track_connection_opened("c1", "conn-1")
    await collector.track_message_sent("c1")
    stats = await collector.get_global_stats()
    assert stats["total_containers_tracked"] == 1
    assert stats["total_active_connections"] == 1
    assert stats["total_messages_processed"] >= 1
    assert stats["total_errors"] == 0


async def test_connection_lifecycle_updates_metrics(collector):
    """Opening and closing a connection should correctly update active count."""
    await collector.track_connection_opened("c1", "conn-1")
    await collector.track_connection_opened("c1", "conn-2")
    metrics = await collector.get_container_metrics("c1")
    assert metrics.connections.active_connections == 2
    assert metrics.connections.total_connections_created == 2

    await collector.track_connection_closed("c1", "conn-1", duration_seconds=10.0)
    metrics = await collector.get_container_metrics("c1")
    assert metrics.connections.active_connections == 1
    assert metrics.connections.total_connections_closed == 1

    await collector.track_connection_closed("c1", "conn-2", duration_seconds=5.0)
    metrics = await collector.get_container_metrics("c1")
    assert metrics.connections.active_connections == 0
    assert metrics.connections.total_connections_closed == 2


async def test_error_tracking_by_type(collector):
    """Multiple errors of different types should be tracked separately."""
    await collector.track_error("c1", "ConnectionError", "timeout")
    await collector.track_error("c1", "ConnectionError", "refused")
    await collector.track_error("c1", "ParseError", "bad syntax")
    metrics = await collector.get_container_metrics("c1")
    assert metrics.errors.total_errors == 3
    assert metrics.errors.errors_by_type["ConnectionError"] == 2
    assert metrics.errors.errors_by_type["ParseError"] == 1
