"""Cross-tenant isolation for the REST monitoring surface (follow-up to #54).

Every store behind these endpoints is a pod-wide singleton keyed by container
name or connection id, shared by ALL tenants. Each endpoint bound ``tenant_id``
and then ignored it, so any licensed caller could read every other tenant's
container names, connection health, message rates, analysis/error counts,
rate-limit stats and violations.

The stores here are REAL instances (health monitor, metrics collector, rate
limiter). Ownership is decided by the REAL ``validate_and_get_container_for_tenant``
against a fake coding-containers client that knows who owns what — so a router
that stops asking, or asks with the wrong tenant, fails.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from services.websocket_health_monitor import WebSocketHealthMonitor
from services.websocket_metrics import WebSocketMetricsCollector
from services.websocket_rate_limiter import RateLimitViolation, WebSocketRateLimiter

TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
OWNER = {"box-a": TENANT_A, "box-b": TENANT_B}


class _FakeContainers:
    """coding-containers: a container is visible only to the tenant that owns it."""

    async def get_container(self, name: str, tenant_id: str | None = None):
        if OWNER.get(name) == tenant_id:
            return {"status": "success", "container": {"container_name": name, "status": "running"}}
        return None


@pytest.fixture
def as_tenant_a():
    from coding_client.auth import require_tenant

    app.dependency_overrides[require_tenant] = lambda: TENANT_A
    with patch("utils.container_validation._get_containers_client", return_value=_FakeContainers()):
        yield
    app.dependency_overrides.pop(require_tenant, None)


async def _get(path: str):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        return await ac.get(path)


# ── /api/ws-health ───────────────────────────────────────────────────────────


@pytest.fixture
async def monitor():
    m = WebSocketHealthMonitor(ping_interval_seconds=3600)
    await m.register_connection("conn-a", "box-a", AsyncMock(), tenant_id=TENANT_A)
    await m.register_connection("conn-b", "box-b", AsyncMock(), tenant_id=TENANT_B)
    await m.register_connection("conn-b2", "box-b", AsyncMock(), tenant_id=TENANT_B)
    with patch("routers.websocket_health.get_health_monitor", return_value=m):
        yield m
    for cid in list(m._connection_health):
        await m.unregister_connection(cid)


async def test_ws_health_containers_lists_only_callers_containers(as_tenant_a, monitor):
    resp = await _get("/api/ws-health/containers")
    assert resp.status_code == 200, resp.text
    assert list(resp.json()) == ["box-a"]
    assert resp.json()["box-a"]["total_connections"] == 1


async def test_ws_health_connection_of_other_tenant_is_404(as_tenant_a, monitor):
    assert (await _get("/api/ws-health/connection/conn-a")).status_code == 200
    resp = await _get("/api/ws-health/connection/conn-b")
    assert resp.status_code == 404, resp.text
    assert "box-b" not in resp.text


async def test_ws_health_global_counts_only_callers_connections(as_tenant_a, monitor):
    resp = await _get("/api/ws-health/global")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_connections"] == 1
    assert data["total_containers"] == 1


# ── /api/realtime/metrics ────────────────────────────────────────────────────


@pytest.fixture
async def collector():
    c = WebSocketMetricsCollector()
    await c.track_connection_opened("box-a", "conn-a")
    await c.track_message_received("box-a")
    await c.track_connection_opened("box-b", "conn-b")
    for _ in range(5):
        await c.track_message_received("box-b")
    await c.track_error("box-b", "send_error", "tenant-b secret error")
    with patch("routers.realtime.metrics.get_websocket_metrics_collector", return_value=c):
        yield c


async def test_realtime_metrics_lists_and_totals_only_callers_containers(as_tenant_a, collector):
    resp = await _get("/api/realtime/metrics")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert [m["container_name"] for m in data["container_metrics"]] == ["box-a"]
    g = data["global_stats"]
    assert g["total_containers_tracked"] == 1
    assert g["total_active_connections"] == 1
    assert g["total_messages_processed"] == 1
    assert g["total_errors"] == 0
    assert "box-b" not in resp.text


async def test_realtime_metrics_of_foreign_container_is_404(as_tenant_a, collector):
    """The per-container endpoint used to log the ownership failure and serve anyway."""
    assert (await _get("/api/realtime/metrics/box-a")).status_code == 200
    resp = await _get("/api/realtime/metrics/box-b")
    assert resp.status_code == 404, resp.text
    assert "secret error" not in resp.text


# ── /api/stats, /api/violations, /api/global-stats ───────────────────────────


@pytest.fixture
async def limiter():
    lim = WebSocketRateLimiter()
    await lim.register_connection("box-a")
    await lim.register_connection("box-b")
    await lim.register_connection("box-b")
    lim.set_container_message_limit("box-b", 3.0)
    now = datetime.now(UTC)
    async with lim._lock:
        for name in ("box-a", "box-b", "box-b"):
            lim._record_violation(
                RateLimitViolation(
                    container_name=name,
                    violation_type="message_rate",
                    timestamp=now,
                    current_value=99.0,
                    limit_value=10.0,
                )
            )
    with patch("routers.websocket_rate_limits.get_websocket_rate_limiter", return_value=lim):
        yield lim


async def test_stats_lists_only_callers_containers(as_tenant_a, limiter):
    resp = await _get("/api/stats")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert list(data["stats"]) == ["box-a"]
    assert data["total_containers"] == 1


async def test_violations_of_foreign_container_is_404(as_tenant_a, limiter):
    assert (await _get("/api/violations?container_name=box-a")).status_code == 200
    resp = await _get("/api/violations?container_name=box-b")
    assert resp.status_code == 404, resp.text


async def test_violations_without_filter_returns_only_callers(as_tenant_a, limiter):
    resp = await _get("/api/violations")
    assert resp.status_code == 200, resp.text
    names = [v["container_name"] for v in resp.json()["violations"]]
    assert names == ["box-a"]


async def test_global_stats_aggregate_only_callers_containers(as_tenant_a, limiter):
    resp = await _get("/api/global-stats")
    assert resp.status_code == 200, resp.text
    g = resp.json()["global_stats"]
    assert g["total_containers"] == 1
    assert g["total_active_connections"] == 1
    assert g["total_violations"] == 1
    # custom_limits is keyed by container NAME — tenant-b's must not appear.
    assert g["custom_limits"]["message_limits"] == {}
    assert "box-b" not in resp.text
    # The service-wide default config is not tenant data and stays.
    assert g["config"]["messages_per_second"] == limiter.config.messages_per_second


# ── /api/realtime/connections shows the client's own id, not the registry key ─


async def test_connection_list_shows_client_id_not_registry_key(as_tenant_a):
    from routers.realtime._manager import ConnectionManager

    cm = ConnectionManager()
    with patch(
        "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
    ):
        await cm.connect(AsyncMock(), "shared", "box-a", tenant_id=TENANT_A)
        await cm.connect(AsyncMock(), "shared", "box-b", tenant_id=TENANT_B)
    cache = AsyncMock()
    cache.get_stats = AsyncMock(return_value=None)
    with (
        patch("routers.realtime.connections.connection_manager", cm),
        patch("routers.realtime.connections.get_analysis_cache", return_value=cache),
    ):
        resp = await _get("/api/realtime/connections")
    assert resp.status_code == 200, resp.text
    assert [c["connection_id"] for c in resp.json()["connections"]] == ["shared"]
    assert TENANT_A not in resp.text


# ── main.py API description ──────────────────────────────────────────────────


def test_api_description_does_not_claim_the_tenant_header_is_trusted():
    """Production sets TRUST_TENANT_HEADER=false; the docs must say what really happens."""
    desc = app.description
    assert "trusted as presented.**" not in desc
    assert "TRUST_TENANT_HEADER=false" in desc
    assert "`X-Tenant-Id` is NOT a credential" in desc
    assert "`10.0.0.0/8`" not in desc  # the old TRUSTED_CIDRS default is long gone
