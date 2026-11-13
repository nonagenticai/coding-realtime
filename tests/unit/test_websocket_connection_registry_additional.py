from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from fastapi.websockets import WebSocketState

from services import websocket_connection_registry as registry_module
from services.websocket_connection_registry import (
    WebSocketConnectionRegistry,
    cleanup_task_runner,
    get_websocket_registry,
)


@pytest.fixture(autouse=True)
def _reset_registry_singletons():
    registry_module._registry = None
    registry_module._rate_limiter = None
    yield
    registry_module._registry = None
    registry_module._rate_limiter = None


def _make_ws(*, connected: bool = True) -> AsyncMock:
    """Create a mock WebSocket with configurable connection state."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED if connected else WebSocketState.DISCONNECTED
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    return ws


# ---------------------------------------------------------------------------
# Split from: test_registry_cleanup_broadcast_and_global_stats
# ---------------------------------------------------------------------------


async def test_register_multiple_connections_tracks_containers(connection_registry):
    """Registering connections across multiple containers updates the container index."""
    ws1 = _make_ws()
    ws2 = _make_ws()

    await connection_registry.register_connection(ws1, "c1", connection_id="c1-1")
    await connection_registry.register_connection(ws2, "c1", connection_id="c1-2")
    await connection_registry.register_connection(ws1, "c2", connection_id="c2-1")

    assert await connection_registry.get_container_connection_count("c1") == 2
    assert await connection_registry.get_container_connection_count("c2") == 1
    assert sorted(await connection_registry.get_all_container_names()) == ["c1", "c2"]


async def test_set_container_connection_limit(connection_registry):
    """set_container_limit stores a positive limit; zero raises ValueError."""
    connection_registry.set_container_limit("c1", 5)
    assert connection_registry._get_container_limit("c1") == 5

    with pytest.raises(ValueError, match="must be positive"):
        connection_registry.set_container_limit("c1", 0)


async def test_cleanup_stale_connections_removes_dead(connection_registry):
    """cleanup_stale_connections closes and removes connections past the timeout."""
    registry = WebSocketConnectionRegistry(enable_rate_limiting=False, connection_timeout_seconds=5)
    ws = _make_ws(connected=True)

    conn_id = await registry.register_connection(ws, "c1", connection_id="c1-1")

    # Artificially age this connection past the 5-second timeout
    conn_info = await registry.get_connection(conn_id)
    assert conn_info is not None
    conn_info.last_activity = datetime.now(UTC) - timedelta(seconds=10)

    stale_cleaned = await registry.cleanup_stale_connections()

    assert stale_cleaned == 1
    ws.close.assert_awaited_once()
    assert await registry.get_connection(conn_id) is None


async def test_broadcast_to_container_delivers_messages(connection_registry):
    """broadcast_to_container sends to all CONNECTED websockets in that container."""
    ws_connected = _make_ws(connected=True)
    ws_disconnected = _make_ws(connected=False)

    await connection_registry.register_connection(ws_connected, "c1", connection_id="c1-conn")
    await connection_registry.register_connection(ws_disconnected, "c1", connection_id="c1-disc")

    sent = await connection_registry.broadcast_to_container("c1", {"type": "ping"})

    assert sent == 1
    ws_connected.send_json.assert_awaited_once_with({"type": "ping"})
    ws_disconnected.send_json.assert_not_awaited()


async def test_broadcast_to_all_delivers_to_all_containers(connection_registry):
    """broadcast_to_all returns per-container send counts for every container."""
    ws1 = _make_ws(connected=True)
    ws2 = _make_ws(connected=True)

    await connection_registry.register_connection(ws1, "c1", connection_id="c1-1")
    await connection_registry.register_connection(ws2, "c2", connection_id="c2-1")

    results = await connection_registry.broadcast_to_all({"type": "global"})

    assert results == {"c1": 1, "c2": 1}
    ws1.send_json.assert_awaited_once_with({"type": "global"})
    ws2.send_json.assert_awaited_once_with({"type": "global"})


async def test_cleanup_container_removes_all_connections(connection_registry):
    """cleanup_container_connections removes every connection for the given container."""
    ws1 = _make_ws(connected=True)
    ws2 = _make_ws(connected=True)

    await connection_registry.register_connection(ws1, "c1", connection_id="c1-1")
    await connection_registry.register_connection(ws2, "c1", connection_id="c1-2")

    removed = await connection_registry.cleanup_container_connections("c1")

    assert removed == 2
    assert await connection_registry.get_container_connection_count("c1") == 0
    ws1.close.assert_awaited_once()
    ws2.close.assert_awaited_once()


async def test_get_all_stats_returns_per_container_data(connection_registry):
    """get_all_stats returns ContainerConnectionStats keyed by container name."""
    ws = _make_ws(connected=True)

    await connection_registry.register_connection(ws, "c1", connection_id="c1-1")
    await connection_registry.register_connection(ws, "c2", connection_id="c2-1")

    stats = await connection_registry.get_all_stats()

    assert set(stats.keys()) == {"c1", "c2"}
    assert stats["c1"].active_connections == 1
    assert stats["c1"].total_connections_created == 1
    assert stats["c2"].active_connections == 1
    assert stats["c2"].total_connections_created == 1


async def test_get_global_stats_lifetime_counters():
    """get_global_stats tracks total_created and total_disconnected across containers."""
    registry = WebSocketConnectionRegistry(enable_rate_limiting=False)
    ws = _make_ws(connected=True)

    await registry.register_connection(ws, "c1", connection_id="c1-1")
    await registry.register_connection(ws, "c1", connection_id="c1-2")
    await registry.register_connection(ws, "c2", connection_id="c2-1")

    # Unregister one connection so we have an exact disconnected count
    await registry.unregister_connection("c1-1")

    stats = await registry.get_global_stats()

    assert stats["total_connections_created_lifetime"] == 3
    assert stats["total_connections_disconnected_lifetime"] == 1
    assert stats["total_active_connections"] == 2
    assert stats["total_containers_with_connections"] == 2


# ---------------------------------------------------------------------------
# Split from: test_registry_rate_limit_paths_and_cleanup_task_runner
# ---------------------------------------------------------------------------


async def test_rate_limited_registry_blocks_over_limit():
    """When the rate limiter denies a connection, register_connection raises ValueError."""
    limiter = AsyncMock()
    limiter.check_connection_limit = AsyncMock(return_value=False)
    setattr(registry_module, "_rate_limiter", limiter)

    registry = WebSocketConnectionRegistry(enable_rate_limiting=True)
    ws = _make_ws(connected=True)

    with pytest.raises(ValueError, match="Rate limit exceeded"):
        await registry.register_connection(ws, "blocked")

    limiter.check_connection_limit.assert_awaited_once_with("blocked")


async def test_rate_limited_registry_allows_within_limit():
    """When the rate limiter allows a connection, it is registered and the limiter is notified."""
    limiter = AsyncMock()
    limiter.check_connection_limit = AsyncMock(return_value=True)
    setattr(registry_module, "_rate_limiter", limiter)

    registry = WebSocketConnectionRegistry(enable_rate_limiting=True)
    ws = _make_ws(connected=True)

    connection_id = await registry.register_connection(ws, "allowed")

    assert connection_id is not None
    limiter.register_connection.assert_awaited_once_with("allowed")
    assert await registry.get_connection(connection_id) is not None


async def test_unregister_connection_cleans_up():
    """unregister_connection removes the connection and notifies the rate limiter."""
    limiter = AsyncMock()
    limiter.check_connection_limit = AsyncMock(return_value=True)
    setattr(registry_module, "_rate_limiter", limiter)

    registry = WebSocketConnectionRegistry(enable_rate_limiting=True)
    ws = _make_ws(connected=True)

    connection_id = await registry.register_connection(ws, "allowed")
    result = await registry.unregister_connection(connection_id)

    assert result is True
    limiter.unregister_connection.assert_awaited_once_with("allowed")
    assert await registry.get_connection(connection_id) is None


async def test_singleton_returns_same_instance():
    """get_websocket_registry returns the same instance on repeated calls."""
    singleton = get_websocket_registry()
    assert singleton is get_websocket_registry()


async def test_cleanup_task_runner_exits_on_cancel():
    """cleanup_task_runner invokes cleanup then exits when cancelled."""
    calls = {"count": 0}

    async def _fake_sleep(_seconds):
        if calls["count"] == 0:
            calls["count"] += 1
            return None
        raise asyncio.CancelledError()

    runner_registry = AsyncMock()
    runner_registry.cleanup_stale_connections = AsyncMock(return_value=2)

    original_sleep = registry_module.asyncio.sleep
    setattr(registry_module.asyncio, "sleep", _fake_sleep)
    try:
        await cleanup_task_runner(runner_registry, cleanup_interval_seconds=0)
    finally:
        setattr(registry_module.asyncio, "sleep", original_sleep)

    runner_registry.cleanup_stale_connections.assert_awaited_once()
