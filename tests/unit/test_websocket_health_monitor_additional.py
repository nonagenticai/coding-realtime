from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.websockets import WebSocketState

from services import websocket_health_monitor as monitor_module
from services.websocket_health_monitor import (
    ConnectionHealth,
    ConnectionHealthStatus,
    WebSocketHealthMonitor,
    cleanup_task_runner,
    get_health_monitor,
)


@pytest.fixture(autouse=True)
def _reset_health_monitor_singleton():
    monitor_module._health_monitor = None
    yield
    monitor_module._health_monitor = None


def _patch_create_task():
    """Patch asyncio.create_task so register_connection doesn't spawn real tasks."""
    created_tasks: list[asyncio.Future] = []

    def _fake_task(coro):
        coro.close()
        task = asyncio.get_running_loop().create_future()
        task.cancel()
        created_tasks.append(task)
        return task

    return patch(
        "services.websocket_health_monitor.asyncio.create_task",
        side_effect=_fake_task,
    ), created_tasks


def _make_monitor(**overrides) -> WebSocketHealthMonitor:
    defaults = {
        "ping_interval_seconds": 1,
        "ping_timeout_seconds": 1,
        "max_failed_pings": 2,
        "stale_threshold_seconds": 5,
    }
    defaults.update(overrides)
    return WebSocketHealthMonitor(**defaults)


async def _register(monitor: WebSocketHealthMonitor, connection_id: str, container: str, ws):
    """Register a connection with create_task patched out."""
    patcher, _ = _patch_create_task()
    with patcher:
        await monitor.register_connection(connection_id, container, ws)


# ---------------------------------------------------------------------------
# Split from: test_health_monitor_ping_paths_and_cleanup_stats
# ---------------------------------------------------------------------------


async def test_register_connection_sets_healthy_status(mock_websocket):
    monitor = _make_monitor()
    await _register(monitor, "c1", "container-a", mock_websocket)

    status = await monitor.get_connection_health("c1")
    assert status is not None
    assert status.health == ConnectionHealth.HEALTHY
    assert status.connection_id == "c1"
    assert status.container_name == "container-a"
    assert status.failed_pings == 0
    assert status.total_pings_sent == 0
    assert status.total_pongs_received == 0
    assert status.is_responsive is True


async def test_send_ping_updates_pings_sent_count(mock_websocket):
    monitor = _make_monitor()
    await _register(monitor, "c1", "container-a", mock_websocket)

    await monitor._send_ping("c1", mock_websocket)

    status = await monitor.get_connection_health("c1")
    assert status is not None
    assert status.total_pings_sent == 1


async def test_handle_pong_updates_pongs_received_count(mock_websocket):
    monitor = _make_monitor()
    await _register(monitor, "c1", "container-a", mock_websocket)

    # Send a ping first so there is ping history for latency calculation
    await monitor._send_ping("c1", mock_websocket)

    result = await monitor.handle_pong("c1", "ignored")
    assert result is True

    status = await monitor.get_connection_health("c1")
    assert status is not None
    assert status.total_pongs_received == 1
    assert status.average_latency_ms is not None


async def test_detect_stale_by_failed_pings(mock_websocket):
    monitor = _make_monitor()
    await _register(monitor, "c1", "container-a", mock_websocket)

    status = await monitor.get_connection_health("c1")
    assert status is not None
    status.failed_pings = 2
    status.last_activity = datetime.now(UTC) - timedelta(seconds=10)

    stale = await monitor.detect_stale_connections("container-a")
    assert stale == ["c1"]


async def test_cleanup_stale_removes_connections(mock_websocket):
    monitor = _make_monitor()
    patcher, created_tasks = _patch_create_task()
    with patcher:
        await monitor.register_connection("c1", "container-a", mock_websocket)

    status = await monitor.get_connection_health("c1")
    assert status is not None
    status.failed_pings = 2
    status.last_activity = datetime.now(UTC) - timedelta(seconds=10)

    cleaned = await monitor.cleanup_stale_connections("container-a")
    assert cleaned == 1
    assert await monitor.get_connection_health("c1") is None
    assert len(created_tasks) == 1


async def test_cleanup_container_removes_all_for_container(mock_websocket):
    monitor = _make_monitor()
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    ws2.send_json = AsyncMock()
    ws2.close = AsyncMock()

    await _register(monitor, "c1", "container-a", mock_websocket)
    await _register(monitor, "c2", "container-a", ws2)

    # Mark both as stale
    for cid in ("c1", "c2"):
        status = await monitor.get_connection_health(cid)
        assert status is not None
        status.failed_pings = 2
        status.last_activity = datetime.now(UTC) - timedelta(seconds=10)

    cleaned = await monitor.cleanup_stale_connections("container-a")
    assert cleaned == 2
    assert await monitor.get_connection_health("c1") is None
    assert await monitor.get_connection_health("c2") is None


async def test_singleton_returns_same_instance():
    singleton1 = get_health_monitor()
    singleton2 = get_health_monitor()
    assert singleton1 is singleton2


# ---------------------------------------------------------------------------
# Split from: test_health_monitor_send_ping_error_variants_and_global_stats
# ---------------------------------------------------------------------------


def _make_health_status(
    connection_id: str,
    container_name: str,
    **overrides: Any,
) -> ConnectionHealthStatus:
    defaults: dict[str, Any] = {
        "connection_id": connection_id,
        "container_name": container_name,
        "health": ConnectionHealth.HEALTHY,
        "last_activity": datetime.now(UTC),
        "connection_age_seconds": 0,
    }
    defaults.update(overrides)
    return ConnectionHealthStatus(**defaults)


async def test_send_ping_disconnected_ws_marks_disconnected():
    monitor = WebSocketHealthMonitor(ping_timeout_seconds=1, max_failed_pings=2)
    health = _make_health_status("c1", "container-a")
    monitor._connection_health["c1"] = health
    monitor._container_connections["container-a"].add("c1")

    ws = AsyncMock()
    ws.client_state = WebSocketState.DISCONNECTED
    ws.send_json = AsyncMock()

    await monitor._send_ping("c1", ws)
    assert health.health == ConnectionHealth.DISCONNECTED
    assert health.is_responsive is False


async def test_send_ping_timeout_increments_failed_pings():
    monitor = WebSocketHealthMonitor(ping_timeout_seconds=1, max_failed_pings=2)
    health = _make_health_status("c1", "container-a")
    monitor._connection_health["c1"] = health
    monitor._container_connections["container-a"].add("c1")

    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()

    with patch("services.websocket_health_monitor.asyncio.wait_for", side_effect=TimeoutError()):
        await monitor._send_ping("c1", ws)

    assert health.failed_pings == 1
    assert health.health == ConnectionHealth.DEGRADED


async def test_send_ping_runtime_error_increments_failed_pings():
    monitor = WebSocketHealthMonitor(ping_timeout_seconds=1, max_failed_pings=2)
    health = _make_health_status("c1", "container-a")
    monitor._connection_health["c1"] = health
    monitor._container_connections["container-a"].add("c1")

    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()

    # First timeout brings failed_pings to 1
    with patch("services.websocket_health_monitor.asyncio.wait_for", side_effect=TimeoutError()):
        await monitor._send_ping("c1", ws)

    assert health.failed_pings == 1

    # RuntimeError brings failed_pings to 2
    with patch(
        "services.websocket_health_monitor.asyncio.wait_for",
        side_effect=RuntimeError("send failed"),
    ):
        await monitor._send_ping("c1", ws)

    assert health.failed_pings == 2
    assert health.health == ConnectionHealth.DEGRADED


async def test_global_health_stats_includes_all_containers():
    monitor = WebSocketHealthMonitor(ping_timeout_seconds=1, max_failed_pings=2)
    health = _make_health_status(
        "c1",
        "container-a",
        health=ConnectionHealth.STALE,
        average_latency_ms=5.0,
    )
    monitor._connection_health["c1"] = health
    monitor._container_connections["container-a"].add("c1")

    stats = await monitor.get_global_health_stats()
    assert stats["total_connections"] == 1
    assert stats["total_containers"] == 1
    assert stats["stale_connections"] == 1
    assert stats["healthy_connections"] == 0
    assert stats["degraded_connections"] == 0
    assert stats["average_latency_ms"] == 5.0


async def test_handle_pong_missing_connection_returns_false():
    monitor = WebSocketHealthMonitor(ping_timeout_seconds=1, max_failed_pings=2)
    result = await monitor.handle_pong("missing", "ignored")
    assert result is False


# ---------------------------------------------------------------------------
# Split from: test_health_monitor_ping_loop_and_cleanup_runner
# ---------------------------------------------------------------------------


async def test_ping_loop_sends_pings_to_registered_connections():
    monitor = WebSocketHealthMonitor(ping_interval_seconds=0)
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED

    monitor._connection_health["c1"] = _make_health_status("c1", "container-a")

    calls = {"sleep": 0}

    async def _fake_sleep(_seconds):
        if calls["sleep"] == 0:
            calls["sleep"] += 1
            # Clear health so the loop exits on its next iteration
            monitor._connection_health.clear()
            return None
        raise asyncio.CancelledError()

    with patch("services.websocket_health_monitor.asyncio.sleep", side_effect=_fake_sleep):
        await monitor._ping_loop("c1", ws)

    # sleep was called at least once before the loop exited
    assert calls["sleep"] == 1


async def test_cleanup_runner_exits_on_cancel():
    async def _runner_sleep(_seconds):
        raise asyncio.CancelledError()

    cleanup_mock = AsyncMock()
    with patch("services.websocket_health_monitor.asyncio.sleep", side_effect=_runner_sleep):
        await cleanup_task_runner(cleanup_mock, cleanup_interval_seconds=0)

    # cleanup_stale_connections should never have been called because sleep
    # raises CancelledError before the cleanup code runs
    cleanup_mock.cleanup_stale_connections.assert_not_called()
