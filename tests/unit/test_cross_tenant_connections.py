"""Cross-tenant isolation for the connection list and the stale-connection cleanup.

Dossier coding.runtime GAP-1 / GAP-2: ``GET /api/realtime/connections`` listed
EVERY tenant's connection ids and watched file paths, and ``POST
/api/ws-health/cleanup`` (no container) force-closed EVERY tenant's stale
connections — both bound ``tenant_id`` and never used it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from main import app
from routers.realtime._manager import ConnectionManager
from services.websocket_health_monitor import WebSocketHealthMonitor
from utils.websocket_utils import _remember_ws_tenant, ws_tenant


@pytest.fixture
def as_tenant_a():
    from coding_client.auth import require_tenant

    app.dependency_overrides[require_tenant] = lambda: "tenant-a"
    yield
    app.dependency_overrides.pop(require_tenant, None)


def _ws() -> AsyncMock:
    return AsyncMock()


async def test_list_connections_excludes_other_tenant(as_tenant_a):
    cm = ConnectionManager()
    with patch(
        "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
    ):
        key_a = await cm.connect(_ws(), "conn-a", "box-a", tenant_id="tenant-a")
        key_b = await cm.connect(_ws(), "conn-b", "box-b", tenant_id="tenant-b")
    cm.watch_file(key_a, "/a/main.py")
    cm.watch_file(key_b, "/b/secret.py")

    cache = AsyncMock()
    cache.get_stats = AsyncMock(return_value=None)
    with (
        patch("routers.realtime.connections.connection_manager", cm),
        patch("routers.realtime.connections.get_analysis_cache", return_value=cache),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/api/realtime/connections")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    ids = [c["connection_id"] for c in data["connections"]]
    assert ids == ["conn-a"]
    assert "/b/secret.py" not in resp.text
    assert data["active_connections"] == 1
    assert data["total_watched_files"] == 1


async def _stale_monitor() -> WebSocketHealthMonitor:
    monitor = WebSocketHealthMonitor(ping_interval_seconds=3600, stale_threshold_seconds=1)
    await monitor.register_connection("conn-a", "box-a", _ws(), tenant_id="tenant-a")
    await monitor.register_connection("conn-b", "box-b", _ws(), tenant_id="tenant-b")
    old = datetime.now(UTC) - timedelta(hours=1)
    for status in monitor._connection_health.values():
        status.last_activity = old
    return monitor


async def test_cleanup_without_container_leaves_other_tenant_connections(as_tenant_a):
    monitor = await _stale_monitor()
    try:
        with patch("routers.websocket_health.get_health_monitor", return_value=monitor):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
                resp = await ac.post("/api/ws-health/cleanup")

        assert resp.status_code == 200, resp.text
        assert resp.json()["cleaned_connections"] == 1
        assert "conn-a" not in monitor._connection_health
        assert "conn-b" in monitor._connection_health  # tenant-b's stale conn untouched
    finally:
        await monitor.cleanup_stale_connections()


async def test_cleanup_of_foreign_container_is_refused(as_tenant_a):
    monitor = await _stale_monitor()
    try:
        with (
            patch("routers.websocket_health.get_health_monitor", return_value=monitor),
            patch(
                "routers.websocket_health.validate_and_get_container_for_tenant",
                AsyncMock(side_effect=HTTPException(status_code=404, detail="not found")),
            ),
        ):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
                resp = await ac.post("/api/ws-health/cleanup?container_name=box-b")

        assert resp.status_code == 404
        assert "conn-b" in monitor._connection_health
    finally:
        await monitor.cleanup_stale_connections()


def test_handshake_tenant_roundtrips_through_scope():
    class _W:
        scope: dict = {}

    w = _W()
    w.scope = {}
    assert ws_tenant(w) is None
    _remember_ws_tenant(w, "tenant-a")
    assert ws_tenant(w) == "tenant-a"
