"""Tests for websocket_health router endpoints."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app


@pytest.fixture
def tenant_override():
    """Override require_tenant dependency to return a test tenant."""
    from coding_client.auth import require_tenant

    app.dependency_overrides[require_tenant] = lambda: "test-tenant"
    yield
    app.dependency_overrides.pop(require_tenant, None)


@pytest.fixture
def mock_health_status():
    """Mock ConnectionHealthStatus object."""
    status = MagicMock()
    status.connection_id = "c1_abc"
    status.container_name = "c1"
    status.health = "healthy"
    status.last_ping_sent = datetime(2025, 1, 1, tzinfo=UTC)
    status.last_pong_received = datetime(2025, 1, 1, tzinfo=UTC)
    status.last_activity = datetime(2025, 1, 1, tzinfo=UTC)
    status.average_latency_ms = 10.0
    status.failed_pings = 0
    status.total_pings_sent = 100
    status.total_pongs_received = 100
    status.connection_age_seconds = 3600.0
    status.is_responsive = True
    return status


@pytest.fixture
def mock_container_health():
    """Mock ContainerHealthStats object."""
    stats = MagicMock()
    stats.container_name = "c1"
    stats.total_connections = 3
    stats.healthy_connections = 2
    stats.degraded_connections = 1
    stats.stale_connections = 0
    stats.average_latency_ms = 12.0
    stats.total_failed_pings = 1
    stats.last_cleanup = None
    return stats


@pytest.fixture
def mock_connection_info():
    """Mock ConnectionInfo object from the registry."""
    conn = MagicMock()
    conn.connection_id = "c1_abc"
    conn.container_name = "c1"
    conn.connected_at = datetime(2025, 1, 1, 9, 0, 0, tzinfo=UTC)
    conn.last_activity = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    return conn


# ---------------------------------------------------------------------------
# GET /connection/{connection_id} -> ConnectionHealthResponse (12 fields)
# ---------------------------------------------------------------------------


async def test_get_connection_health(tenant_override, mock_health_status):
    """Verify all 12 fields of ConnectionHealthResponse are returned correctly."""
    mock_monitor = AsyncMock()
    mock_monitor.get_connection_health = AsyncMock(return_value=mock_health_status)

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/connection/c1_abc")

    assert resp.status_code == 200
    data = resp.json()

    assert data["connection_id"] == "c1_abc"
    assert data["container_name"] == "c1"
    assert data["health"] == "healthy"
    assert data["last_ping_sent"] == "2025-01-01T00:00:00+00:00"
    assert data["last_pong_received"] == "2025-01-01T00:00:00+00:00"
    assert data["last_activity"] == "2025-01-01T00:00:00+00:00"
    assert data["average_latency_ms"] == 10.0
    assert data["failed_pings"] == 0
    assert data["total_pings_sent"] == 100
    assert data["total_pongs_received"] == 100
    assert data["connection_age_seconds"] == 3600.0
    assert data["is_responsive"] is True

    # Exactly 12 fields — no more, no fewer
    assert set(data.keys()) == {
        "connection_id",
        "container_name",
        "health",
        "last_ping_sent",
        "last_pong_received",
        "last_activity",
        "average_latency_ms",
        "failed_pings",
        "total_pings_sent",
        "total_pongs_received",
        "connection_age_seconds",
        "is_responsive",
    }


async def test_get_connection_health_not_found(tenant_override):
    """A missing connection_id should return 404."""
    mock_monitor = AsyncMock()
    mock_monitor.get_connection_health = AsyncMock(return_value=None)

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/connection/missing")

    assert resp.status_code == 404
    error = resp.json()["error"]
    assert error["code"] == "HTTP_404"
    assert "missing" in error["message"]
    assert "Connection" in error["message"]


# ---------------------------------------------------------------------------
# GET /container/{container_name} -> ContainerHealthResponse (8 fields)
# ---------------------------------------------------------------------------


async def test_get_container_health(tenant_override, mock_container_health):
    """Verify all 8 fields of ContainerHealthResponse are returned correctly."""
    mock_monitor = AsyncMock()
    mock_monitor.get_container_health = AsyncMock(return_value=mock_container_health)

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant", new_callable=AsyncMock
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/container/c1")

    assert resp.status_code == 200
    data = resp.json()

    assert data["container_name"] == "c1"
    assert data["total_connections"] == 3
    assert data["healthy_connections"] == 2
    assert data["degraded_connections"] == 1
    assert data["stale_connections"] == 0
    assert data["average_latency_ms"] == 12.0
    assert data["total_failed_pings"] == 1
    assert data["last_cleanup"] is None

    assert set(data.keys()) == {
        "container_name",
        "total_connections",
        "healthy_connections",
        "degraded_connections",
        "stale_connections",
        "average_latency_ms",
        "total_failed_pings",
        "last_cleanup",
    }


async def test_get_container_health_with_last_cleanup(tenant_override):
    """Verify last_cleanup is serialized when present."""
    stats = MagicMock()
    stats.container_name = "c2"
    stats.total_connections = 5
    stats.healthy_connections = 5
    stats.degraded_connections = 0
    stats.stale_connections = 0
    stats.average_latency_ms = 8.5
    stats.total_failed_pings = 0
    stats.last_cleanup = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)

    mock_monitor = AsyncMock()
    mock_monitor.get_container_health = AsyncMock(return_value=stats)

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant", new_callable=AsyncMock
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/container/c2")

    assert resp.status_code == 200
    data = resp.json()
    assert data["last_cleanup"] == "2025-06-15T12:00:00+00:00"
    assert data["container_name"] == "c2"
    assert data["total_connections"] == 5


async def test_get_container_health_not_found(tenant_override):
    """A container with no active connections should return 404."""
    mock_monitor = AsyncMock()
    mock_monitor.get_container_health = AsyncMock(return_value=None)

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant", new_callable=AsyncMock
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/container/missing")

    assert resp.status_code == 404
    error = resp.json()["error"]
    assert error["code"] == "HTTP_404"
    assert "missing" in error["message"]


# ---------------------------------------------------------------------------
# GET /container/{container_name}/connections -> ContainerConnectionsResponse
# ---------------------------------------------------------------------------


async def test_get_container_connections(
    tenant_override, mock_connection_info, mock_health_status, mock_container_health
):
    """Verify ContainerConnectionsResponse structure: status, message, container_name,
    total_connections, connections list, health_summary, timestamp, request_id."""
    mock_registry = AsyncMock()
    mock_registry.get_container_connections = AsyncMock(return_value=[mock_connection_info])

    mock_monitor = AsyncMock()
    mock_monitor.get_connection_health = AsyncMock(return_value=mock_health_status)
    mock_monitor.get_container_health = AsyncMock(return_value=mock_container_health)

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant", new_callable=AsyncMock
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/container/c1/connections")

    assert resp.status_code == 200
    data = resp.json()

    # Top-level BaseResponse fields
    assert data["status"] == "success"
    assert data["message"] == "Retrieved 1 active connections for container 'c1'"
    assert "timestamp" in data
    assert "request_id" in data

    # ContainerConnectionsResponse-specific fields
    assert data["container_name"] == "c1"
    assert data["total_connections"] == 1

    # connections list: verify the single ActiveConnectionInfo entry
    assert len(data["connections"]) == 1
    conn = data["connections"][0]
    assert conn["connection_id"] == "c1_abc"
    assert conn["container_name"] == "c1"
    assert conn["health"] == "healthy"
    assert conn["connected_at"] == "2025-01-01T09:00:00+00:00"
    assert conn["last_activity"] == "2025-01-01T10:00:00+00:00"
    assert isinstance(conn["connection_age_seconds"], (int, float))
    assert conn["average_latency_ms"] == 10.0
    assert conn["is_responsive"] is True

    # health_summary: verify all 8 ContainerHealthResponse fields
    hs = data["health_summary"]
    assert hs["container_name"] == "c1"
    assert hs["total_connections"] == 3
    assert hs["healthy_connections"] == 2
    assert hs["degraded_connections"] == 1
    assert hs["stale_connections"] == 0
    assert hs["average_latency_ms"] == 12.0
    assert hs["total_failed_pings"] == 1
    assert hs["last_cleanup"] is None


async def test_get_container_connections_no_health_stats(
    tenant_override, mock_connection_info, mock_health_status
):
    """When the health monitor has no container stats, a default health_summary is created."""
    mock_registry = AsyncMock()
    mock_registry.get_container_connections = AsyncMock(return_value=[mock_connection_info])

    mock_monitor = AsyncMock()
    mock_monitor.get_connection_health = AsyncMock(return_value=mock_health_status)
    mock_monitor.get_container_health = AsyncMock(return_value=None)

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant", new_callable=AsyncMock
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/container/c1/connections")

    assert resp.status_code == 200
    hs = resp.json()["health_summary"]
    assert hs["container_name"] == "c1"
    assert hs["total_connections"] == 1  # falls back to len(connections)
    assert hs["healthy_connections"] == 0
    assert hs["degraded_connections"] == 0
    assert hs["stale_connections"] == 0
    assert hs["average_latency_ms"] is None
    assert hs["total_failed_pings"] == 0
    assert hs["last_cleanup"] is None


async def test_get_container_connections_not_found(tenant_override):
    """When the registry returns no connections, return 404."""
    mock_registry = AsyncMock()
    mock_registry.get_container_connections = AsyncMock(return_value=[])

    mock_monitor = AsyncMock()

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant", new_callable=AsyncMock
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/container/empty/connections")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /global -> GlobalHealthResponse
# ---------------------------------------------------------------------------


async def test_get_global_health(tenant_override):
    """Verify all fields in GlobalHealthResponse including configuration sub-dict."""
    mock_monitor = AsyncMock()
    mock_monitor.get_global_health_stats = AsyncMock(
        return_value={
            "total_connections": 10,
            "total_containers": 2,
            "healthy_connections": 8,
            "degraded_connections": 1,
            "stale_connections": 1,
            "average_latency_ms": 15.0,
            "total_pings_sent": 500,
            "total_pongs_received": 490,
            "total_failed_pings": 10,
            "ping_success_rate": 98.0,
            "ping_interval_seconds": 30,
            "ping_timeout_seconds": 10,
            "max_failed_pings": 3,
            "stale_threshold_seconds": 300,
        }
    )

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/global")

    assert resp.status_code == 200
    data = resp.json()

    # BaseResponse fields
    assert data["status"] == "success"
    assert data["message"] == "Global health statistics retrieved successfully"
    assert "timestamp" in data
    assert "request_id" in data

    # GlobalHealthResponse top-level stats
    assert data["total_connections"] == 10
    assert data["total_containers"] == 2
    assert data["healthy_connections"] == 8
    assert data["degraded_connections"] == 1
    assert data["stale_connections"] == 1
    assert data["average_latency_ms"] == 15.0
    assert data["total_pings_sent"] == 500
    assert data["total_pongs_received"] == 490
    assert data["total_failed_pings"] == 10
    assert data["ping_success_rate"] == 98.0

    # configuration sub-dict — verify every key and value
    config = data["configuration"]
    assert config == {
        "ping_interval_seconds": 30,
        "ping_timeout_seconds": 10,
        "max_failed_pings": 3,
        "stale_threshold_seconds": 300,
    }

    # Verify we have all expected top-level keys
    expected_keys = {
        "status",
        "message",
        "timestamp",
        "request_id",
        "total_connections",
        "total_containers",
        "healthy_connections",
        "degraded_connections",
        "stale_connections",
        "average_latency_ms",
        "total_pings_sent",
        "total_pongs_received",
        "total_failed_pings",
        "ping_success_rate",
        "configuration",
    }
    assert set(data.keys()) == expected_keys


# ---------------------------------------------------------------------------
# POST /cleanup -> CleanupResponse
# ---------------------------------------------------------------------------


async def test_cleanup_stale_connections(tenant_override):
    """Verify all CleanupResponse fields when cleaning up globally (no container_name)."""
    mock_monitor = AsyncMock()
    mock_monitor.cleanup_stale_connections = AsyncMock(return_value=3)

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post("/api/ws-health/cleanup")

    assert resp.status_code == 200
    data = resp.json()

    # BaseResponse fields
    assert data["status"] == "success"
    assert data["message"] == "Cleaned up 3 stale connections"
    assert "timestamp" in data
    assert "request_id" in data

    # CleanupResponse-specific fields
    assert data["cleaned_connections"] == 3
    assert data["container_name"] is None

    # Verify the monitor was called correctly
    mock_monitor.cleanup_stale_connections.assert_awaited_once_with(
        container_name=None, auto_close=True, tenant_id="test-tenant"
    )


async def test_cleanup_stale_connections_with_container_filter(tenant_override):
    """Verify cleanup with a container_name query parameter."""
    mock_monitor = AsyncMock()
    mock_monitor.cleanup_stale_connections = AsyncMock(return_value=1)

    with (
        patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant",
            AsyncMock(return_value={"container_name": "my-container"}),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post("/api/ws-health/cleanup?container_name=my-container")

    assert resp.status_code == 200
    data = resp.json()

    assert data["status"] == "success"
    assert data["message"] == "Cleaned up 1 stale connections for container 'my-container'"
    assert data["cleaned_connections"] == 1
    assert data["container_name"] == "my-container"
    assert "timestamp" in data
    assert "request_id" in data

    mock_monitor.cleanup_stale_connections.assert_awaited_once_with(
        container_name="my-container", auto_close=True, tenant_id="test-tenant"
    )


async def test_cleanup_zero_stale_connections(tenant_override):
    """Verify cleanup when there is nothing to clean."""
    mock_monitor = AsyncMock()
    mock_monitor.cleanup_stale_connections = AsyncMock(return_value=0)

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post("/api/ws-health/cleanup")

    assert resp.status_code == 200
    data = resp.json()
    assert data["message"] == "Cleaned up 0 stale connections"
    assert data["cleaned_connections"] == 0
    assert data["container_name"] is None


# ---------------------------------------------------------------------------
# GET /containers -> dict[str, ContainerHealthResponse]
# ---------------------------------------------------------------------------


async def test_get_all_container_health(tenant_override, mock_container_health):
    """Verify the nested ContainerHealthResponse object is fully populated."""
    mock_monitor = AsyncMock()
    mock_monitor.get_all_container_health = AsyncMock(return_value={"c1": mock_container_health})

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/containers")

    assert resp.status_code == 200
    data = resp.json()

    # Only "c1" in the dict
    assert list(data.keys()) == ["c1"]

    c1 = data["c1"]
    assert c1["container_name"] == "c1"
    assert c1["total_connections"] == 3
    assert c1["healthy_connections"] == 2
    assert c1["degraded_connections"] == 1
    assert c1["stale_connections"] == 0
    assert c1["average_latency_ms"] == 12.0
    assert c1["total_failed_pings"] == 1
    assert c1["last_cleanup"] is None

    assert set(c1.keys()) == {
        "container_name",
        "total_connections",
        "healthy_connections",
        "degraded_connections",
        "stale_connections",
        "average_latency_ms",
        "total_failed_pings",
        "last_cleanup",
    }


async def test_get_all_container_health_multiple_containers(tenant_override):
    """Verify the endpoint works with multiple containers, each fully validated."""
    c1 = MagicMock()
    c1.container_name = "c1"
    c1.total_connections = 3
    c1.healthy_connections = 2
    c1.degraded_connections = 1
    c1.stale_connections = 0
    c1.average_latency_ms = 12.0
    c1.total_failed_pings = 1
    c1.last_cleanup = None

    c2 = MagicMock()
    c2.container_name = "c2"
    c2.total_connections = 7
    c2.healthy_connections = 5
    c2.degraded_connections = 1
    c2.stale_connections = 1
    c2.average_latency_ms = 25.3
    c2.total_failed_pings = 4
    c2.last_cleanup = datetime(2025, 3, 10, 8, 0, 0, tzinfo=UTC)

    mock_monitor = AsyncMock()
    mock_monitor.get_all_container_health = AsyncMock(return_value={"c1": c1, "c2": c2})

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/containers")

    assert resp.status_code == 200
    data = resp.json()
    assert set(data.keys()) == {"c1", "c2"}

    # Validate c1
    assert data["c1"]["container_name"] == "c1"
    assert data["c1"]["total_connections"] == 3
    assert data["c1"]["healthy_connections"] == 2
    assert data["c1"]["degraded_connections"] == 1
    assert data["c1"]["stale_connections"] == 0
    assert data["c1"]["average_latency_ms"] == 12.0
    assert data["c1"]["total_failed_pings"] == 1
    assert data["c1"]["last_cleanup"] is None

    # Validate c2
    assert data["c2"]["container_name"] == "c2"
    assert data["c2"]["total_connections"] == 7
    assert data["c2"]["healthy_connections"] == 5
    assert data["c2"]["degraded_connections"] == 1
    assert data["c2"]["stale_connections"] == 1
    assert data["c2"]["average_latency_ms"] == 25.3
    assert data["c2"]["total_failed_pings"] == 4
    assert data["c2"]["last_cleanup"] == "2025-03-10T08:00:00+00:00"


async def test_get_all_container_health_empty(tenant_override):
    """Verify the endpoint returns an empty dict when there are no containers."""
    mock_monitor = AsyncMock()
    mock_monitor.get_all_container_health = AsyncMock(return_value={})

    with patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/ws-health/containers")

    assert resp.status_code == 200
    assert resp.json() == {}
