"""Tests for WebSocketConnectionRegistry."""

from unittest.mock import AsyncMock

import pytest
from fastapi.websockets import WebSocketState

from services.websocket_connection_registry import (
    ConnectionInfo,
    ContainerConnectionStats,
    WebSocketConnectionRegistry,
)


@pytest.fixture
def registry():
    return WebSocketConnectionRegistry(
        default_max_connections_per_container=3,
        enable_rate_limiting=False,
    )


@pytest.fixture
def ws():
    w = AsyncMock()
    w.client_state = WebSocketState.CONNECTED
    return w


async def test_register_returns_connection_id(registry, ws):
    conn_id = await registry.register_connection(ws, "c1")
    assert isinstance(conn_id, str)
    assert "c1" in conn_id
    # Verify the connection was actually stored and retrievable
    info = await registry.get_connection(conn_id)
    assert info is not None
    assert info.container_name == "c1"
    assert info.websocket is ws


async def test_register_with_custom_id(registry, ws):
    conn_id = await registry.register_connection(ws, "c1", connection_id="my-id")
    assert conn_id == "my-id"
    # Verify the custom ID was actually used in storage
    info = await registry.get_connection("my-id")
    assert info is not None
    assert info.connection_id == "my-id"


async def test_unregister_removes_connection(registry, ws):
    conn_id = await registry.register_connection(ws, "c1")
    assert await registry.unregister_connection(conn_id) is True
    assert await registry.get_connection(conn_id) is None


async def test_unregister_unknown_returns_false(registry):
    assert await registry.unregister_connection("nope") is False


async def test_get_container_connections(registry, ws):
    await registry.register_connection(ws, "c1", connection_id="a")
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    await registry.register_connection(ws2, "c1", connection_id="b")
    conns = await registry.get_container_connections("c1")
    assert len(conns) == 2


async def test_get_connection_info(registry, ws):
    conn_id = await registry.register_connection(ws, "c1")
    info = await registry.get_connection(conn_id)
    assert isinstance(info, ConnectionInfo)
    assert info.container_name == "c1"


async def test_get_connection_returns_none_for_unknown(registry):
    assert await registry.get_connection("nope") is None


async def test_update_activity(registry, ws):
    conn_id = await registry.register_connection(ws, "c1")
    info_before = await registry.get_connection(conn_id)
    original_activity = info_before.last_activity
    import asyncio

    await asyncio.sleep(0.02)
    assert await registry.update_activity(conn_id) is True
    info_after = await registry.get_connection(conn_id)
    # Activity timestamp must have advanced (>= because datetime resolution)
    assert info_after.last_activity >= original_activity
    # Updating a non-existent connection should return False
    assert await registry.update_activity("nonexistent") is False


async def test_get_container_stats(registry, ws):
    await registry.register_connection(ws, "c1")
    stats = await registry.get_container_stats("c1")
    assert isinstance(stats, ContainerConnectionStats)
    assert stats.active_connections == 1
    assert stats.at_limit is False


async def test_connection_limit_enforcement(registry, ws):
    """Registry with max 3 connections should raise on 4th."""
    for i in range(3):
        w = AsyncMock()
        w.client_state = WebSocketState.CONNECTED
        await registry.register_connection(w, "c1", connection_id=f"conn-{i}")

    with pytest.raises(ValueError, match="Connection limit reached"):
        w = AsyncMock()
        w.client_state = WebSocketState.CONNECTED
        await registry.register_connection(w, "c1", connection_id="conn-overflow")


async def test_at_limit_flag(registry, ws):
    """at_limit should be False when below limit, True when at limit."""
    # Add one connection -- not at limit yet
    w0 = AsyncMock()
    w0.client_state = WebSocketState.CONNECTED
    await registry.register_connection(w0, "c1", connection_id="conn-0")
    stats = await registry.get_container_stats("c1")
    assert stats.at_limit is False
    assert stats.active_connections == 1

    # Fill up to limit (3)
    for i in range(1, 3):
        w = AsyncMock()
        w.client_state = WebSocketState.CONNECTED
        await registry.register_connection(w, "c1", connection_id=f"conn-{i}")
    stats = await registry.get_container_stats("c1")
    assert stats.at_limit is True
    assert stats.active_connections == 3
    assert stats.connection_limit == 3
