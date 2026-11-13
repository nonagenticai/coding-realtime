"""Tests for WebSocketHealthMonitor."""

from unittest.mock import AsyncMock

import pytest
from fastapi.websockets import WebSocketState

from services.websocket_health_monitor import (
    ConnectionHealth,
    ContainerHealthStats,
    WebSocketHealthMonitor,
)


@pytest.fixture
def monitor():
    return WebSocketHealthMonitor(
        ping_interval_seconds=9999,  # prevent background ping loops
        ping_timeout_seconds=10,
        max_failed_pings=3,
        stale_threshold_seconds=300,
    )


@pytest.fixture
def ws():
    w = AsyncMock()
    w.client_state = WebSocketState.CONNECTED
    w.send_json = AsyncMock()
    return w


async def test_register_connection(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    health = await monitor.get_connection_health("conn-1")
    assert health is not None
    assert health.health == ConnectionHealth.HEALTHY


async def test_unregister_connection(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    assert await monitor.unregister_connection("conn-1") is True
    assert await monitor.get_connection_health("conn-1") is None


async def test_unregister_unknown_returns_false(monitor):
    assert await monitor.unregister_connection("nope") is False


async def test_get_connection_health_returns_none(monitor):
    assert await monitor.get_connection_health("nope") is None


async def test_get_container_health(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    health = await monitor.get_container_health("c1")
    assert isinstance(health, ContainerHealthStats)
    assert health.total_connections == 1
    assert health.healthy_connections == 1


async def test_get_container_health_unknown_returns_none(monitor):
    assert await monitor.get_container_health("nope") is None


async def test_get_all_container_health(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    ws2.send_json = AsyncMock()
    await monitor.register_connection("conn-2", "c2", ws2)
    all_health = await monitor.get_all_container_health()
    assert "c1" in all_health
    assert "c2" in all_health


async def test_get_global_health_stats(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    stats = await monitor.get_global_health_stats()
    assert stats["total_connections"] == 1
    assert stats["healthy_connections"] == 1


async def test_update_activity(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    assert await monitor.update_activity("conn-1") is True
    assert await monitor.update_activity("nonexistent") is False


async def test_handle_pong(monitor, ws):
    await monitor.register_connection("conn-1", "c1", ws)
    health_before = await monitor.get_connection_health("conn-1")
    assert health_before.total_pongs_received == 0

    assert await monitor.handle_pong("conn-1", "ping-1") is True

    health_after = await monitor.get_connection_health("conn-1")
    assert health_after.total_pongs_received == 1
    # Pong should keep the connection healthy
    assert health_after.health == ConnectionHealth.HEALTHY
    # last_activity should be updated
    assert health_after.last_activity >= health_before.last_activity


async def test_handle_pong_unknown_returns_false(monitor):
    assert await monitor.handle_pong("nope", "ping-1") is False


async def test_multiple_pongs_increment_counter(monitor, ws):
    """Receiving multiple pongs should increment the counter each time."""
    await monitor.register_connection("conn-1", "c1", ws)
    await monitor.handle_pong("conn-1", "ping-1")
    await monitor.handle_pong("conn-1", "ping-2")
    await monitor.handle_pong("conn-1", "ping-3")
    health = await monitor.get_connection_health("conn-1")
    assert health.total_pongs_received == 3
