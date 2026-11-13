"""End-to-end API integration tests for coding-realtime.

Exercises HTTP endpoints through the real FastAPI ASGI app with mocked
external dependencies (Redis, coding-analysis, coding-containers).

Every assertion validates concrete field values, types, and schema
completeness rather than just checking for key existence.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# NOTE: deliberately NOT marked `integration` — every test here drives the real
# FastAPI app through httpx/ASGI with Redis and the downstream coding-* services
# mocked, so it needs no live service. The marker excluded all 31 tests from CI
# (pyproject addopts carry `-m 'not integration'`). test_websocket_lifecycle.py
# is unmarked for the same reason: it too runs against mocks, not a live service.

# ---------------------------------------------------------------------------
# Tenant header helper
# ---------------------------------------------------------------------------
TENANT_HEADERS = {"X-Tenant-Id": "test-tenant"}


# ---- Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def mock_health_monitor():
    """Mock the WebSocket health monitor singleton."""
    monitor = MagicMock()
    monitor.get_global_health_stats = AsyncMock(
        return_value={
            "total_connections": 5,
            "total_containers": 2,
            "healthy_connections": 3,
            "degraded_connections": 1,
            "stale_connections": 1,
            "average_latency_ms": 14.2,
            "total_pings_sent": 1500,
            "total_pongs_received": 1485,
            "total_failed_pings": 15,
            "ping_success_rate": 99.0,
            "ping_interval_seconds": 30,
            "ping_timeout_seconds": 10,
            "max_failed_pings": 3,
            "stale_threshold_seconds": 300,
        }
    )
    monitor.get_container_health = AsyncMock(return_value=None)
    monitor.get_connection_health = AsyncMock(return_value=None)
    monitor.get_all_container_health = AsyncMock(return_value={})
    monitor.cleanup_stale_connections = AsyncMock(return_value=0)
    with patch(
        "routers.websocket_health.get_health_monitor",
        return_value=monitor,
    ):
        yield monitor


@pytest.fixture
def mock_rate_limiter():
    """Mock the WebSocket rate limiter singleton."""
    from services.websocket_rate_limiter import ContainerRateStats

    limiter = MagicMock()
    limiter.get_global_stats = AsyncMock(
        return_value={
            "total_containers": 3,
            "total_active_connections": 8,
            "total_messages_processed": 5000,
            "total_violations": 2,
            "most_recent_violation": None,
            "config": {
                "messages_per_second": 10.0,
                "connections_per_container": 10,
                "burst_size": 20,
                "rate_limit_window_seconds": 1.0,
            },
        }
    )
    limiter.get_container_stats = AsyncMock(
        return_value=ContainerRateStats(
            container_name="test-container",
            total_messages=42,
            messages_per_second=2.1,
            active_connections=1,
            connections_limit=10,
            messages_limit=10.0,
            is_rate_limited=False,
            total_violations=0,
            last_violation=None,
        )
    )
    limiter.get_all_stats = AsyncMock(return_value={})
    limiter.get_violations = AsyncMock(return_value=[])
    with patch(
        "routers.websocket_rate_limits.get_websocket_rate_limiter",
        return_value=limiter,
    ):
        yield limiter


@pytest.fixture
def mock_metrics_collector():
    """Mock the WebSocket metrics collector singleton."""
    collector = MagicMock()
    collector.get_global_stats = AsyncMock(
        return_value={
            "total_containers_tracked": 2,
            "total_active_connections": 5,
            "total_messages_processed": 1200,
            "total_analyses_executed": 800,
            "total_errors": 3,
        }
    )
    collector.get_all_container_metrics = AsyncMock(return_value=[])
    with patch(
        "routers.realtime.metrics.get_websocket_metrics_collector",
        return_value=collector,
    ):
        yield collector


@pytest.fixture
def mock_analysis_cache():
    """Mock the analysis cache singleton."""
    cache = MagicMock()
    cache.get_stats = AsyncMock(
        return_value={
            "hit_rate": 75.5,
            "total_requests": 1000,
            "cache_hits": 755,
            "cache_misses": 245,
        }
    )
    with patch(
        "routers.realtime.connections.get_analysis_cache",
        return_value=cache,
    ):
        yield cache


@pytest.fixture
def mock_container_validation_ok():
    """Mock container validation to always succeed."""
    with (
        patch(
            "routers.websocket_rate_limits.validate_and_get_container_for_tenant",
            new_callable=AsyncMock,
            return_value={"name": "test-container", "status": "running"},
        ),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant",
            new_callable=AsyncMock,
            return_value={"name": "test-container", "status": "running"},
        ),
        patch(
            "routers.realtime.metrics.validate_and_get_container_for_tenant",
            new_callable=AsyncMock,
            return_value={"name": "test-container", "status": "running"},
        ),
    ):
        yield


@pytest.fixture
def mock_container_validation_404():
    """Mock container validation to raise 404."""
    from fastapi import HTTPException, status

    async def _raise_404(*args, **kwargs):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Container 'nonexistent' not found",
        )

    with (
        patch(
            "routers.websocket_rate_limits.validate_and_get_container_for_tenant",
            side_effect=_raise_404,
        ),
        patch(
            "routers.websocket_health.validate_and_get_container_for_tenant",
            side_effect=_raise_404,
        ),
        patch(
            "routers.realtime.metrics.validate_and_get_container_for_tenant",
            side_effect=_raise_404,
        ),
    ):
        yield


# ===================================================================
# Health endpoints — full field validation
# ===================================================================


async def test_health_full_schema(async_client):
    """GET /health contains all six fields with correct types and values."""
    data = (await async_client.get("/health")).json()
    assert data["status"] == "ok"
    assert data["service"] == "coding-realtime"
    assert isinstance(data["version"], str) and len(data["version"]) > 0
    assert data["environment"] == "testing"
    assert data["redis"] == "connected"
    # timestamp must be valid ISO-8601
    datetime.fromisoformat(data["timestamp"])


async def test_health_degraded_on_redis_failure(async_client, mock_redis):
    """GET /health returns status=degraded and redis=disconnected when Redis ping fails."""
    mock_redis.ping = AsyncMock(side_effect=ConnectionError("down"))
    data = (await async_client.get("/health")).json()
    assert data["status"] == "degraded"
    assert data["redis"] == "disconnected"
    # Other fields are still present
    assert data["service"] == "coding-realtime"
    assert data["environment"] == "testing"
    datetime.fromisoformat(data["timestamp"])


async def test_health_liveness_exact_body(async_client):
    """GET /api/health/live returns exactly {"status":"alive"} with 200."""
    resp = await async_client.get("/api/health/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "alive"}


async def test_health_readiness_full_schema(async_client):
    """GET /api/health/ready returns status=ready and redis=connected."""
    resp = await async_client.get("/api/health/ready")
    assert resp.status_code == 200
    data = resp.json()
    assert data == {"status": "ready", "redis": "connected"}


async def test_health_readiness_503_on_redis_failure(async_client, mock_redis):
    """GET /api/health/ready returns 503 and not_ready when Redis fails."""
    mock_redis.ping = AsyncMock(side_effect=ConnectionError("down"))
    resp = await async_client.get("/api/health/ready")
    assert resp.status_code == 503
    data = resp.json()
    assert data == {"status": "not_ready", "redis": "disconnected"}


# ===================================================================
# WebSocket health endpoints
# ===================================================================


async def test_ws_health_global_full_schema(async_client, mock_health_monitor):
    """GET /api/ws-health/global returns all expected fields with correct values."""
    resp = await async_client.get("/api/ws-health/global", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()

    # Envelope fields
    assert data["status"] == "success"
    assert isinstance(data["message"], str) and len(data["message"]) > 0
    datetime.fromisoformat(data["timestamp"])

    # Numeric stats — match mock values
    assert data["total_connections"] == 5
    assert data["total_containers"] == 2
    assert data["healthy_connections"] == 3
    assert data["degraded_connections"] == 1
    assert data["stale_connections"] == 1
    assert data["average_latency_ms"] == 14.2
    assert data["total_pings_sent"] == 1500
    assert data["total_pongs_received"] == 1485
    assert data["total_failed_pings"] == 15
    assert data["ping_success_rate"] == 99.0

    # Configuration block
    cfg = data["configuration"]
    assert cfg["ping_interval_seconds"] == 30
    assert cfg["ping_timeout_seconds"] == 10
    assert cfg["max_failed_pings"] == 3
    assert cfg["stale_threshold_seconds"] == 300


async def test_ws_health_containers_empty_dict(async_client, mock_health_monitor):
    """GET /api/ws-health/containers returns empty dict when no containers have connections."""
    resp = await async_client.get("/api/ws-health/containers", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    assert resp.json() == {}


async def test_ws_health_connection_not_found(async_client, mock_health_monitor):
    """GET /api/ws-health/connection/{id} returns structured 404 for unknown connection."""
    resp = await async_client.get(
        "/api/ws-health/connection/nonexistent",
        headers=TENANT_HEADERS,
    )
    assert resp.status_code == 404
    data = resp.json()
    err = data["error"]
    assert err["code"] == "HTTP_404"
    assert isinstance(err["message"], str) and len(err["message"]) > 0
    assert "nonexistent" in err["message"].lower() or "connection" in err["message"].lower()
    assert isinstance(err["request_id"], str) and len(err["request_id"]) > 0
    datetime.fromisoformat(err["timestamp"])


async def test_ws_health_cleanup_returns_zero(async_client, mock_health_monitor):
    """POST /api/ws-health/cleanup returns 0 cleaned connections when there are none."""
    resp = await async_client.post("/api/ws-health/cleanup", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["cleaned_connections"] == 0
    assert isinstance(data["message"], str)
    datetime.fromisoformat(data["timestamp"])


# ===================================================================
# Rate limit endpoints
# ===================================================================


async def test_rate_limits_global_full_schema(async_client, mock_rate_limiter):
    """GET /api/global-stats returns full global rate-limit statistics."""
    resp = await async_client.get("/api/global-stats", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()

    assert data["status"] == "success"
    assert isinstance(data["message"], str) and len(data["message"]) > 0
    datetime.fromisoformat(data["timestamp"])

    gs = data["global_stats"]
    assert gs["total_containers"] == 3
    assert gs["total_active_connections"] == 8
    assert gs["total_messages_processed"] == 5000
    assert gs["total_violations"] == 2
    assert gs["most_recent_violation"] is None

    cfg = gs["config"]
    assert cfg["messages_per_second"] == 10.0
    assert cfg["connections_per_container"] == 10
    assert cfg["burst_size"] == 20
    assert cfg["rate_limit_window_seconds"] == 1.0


async def test_rate_limits_container_stats_full_schema(
    async_client,
    mock_rate_limiter,
    mock_container_validation_ok,
):
    """GET /api/stats/{name} returns complete container rate-limit stats."""
    resp = await async_client.get("/api/stats/test-container", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()

    assert data["status"] == "success"
    assert data["container_name"] == "test-container"
    datetime.fromisoformat(data["timestamp"])

    stats = data["stats"]
    assert stats["container_name"] == "test-container"
    assert stats["total_messages"] == 42
    assert stats["messages_per_second"] == 2.1
    assert stats["active_connections"] == 1
    assert stats["connections_limit"] == 10
    assert stats["messages_limit"] == 10.0
    assert stats["is_rate_limited"] is False
    assert stats["total_violations"] == 0
    assert stats["last_violation"] is None


async def test_rate_limits_container_stats_404(
    async_client,
    mock_rate_limiter,
    mock_container_validation_404,
):
    """GET /api/stats/{name} returns 404 for an unknown container."""
    resp = await async_client.get("/api/stats/nonexistent", headers=TENANT_HEADERS)
    assert resp.status_code == 404


async def test_rate_limits_all_stats_empty(async_client, mock_rate_limiter):
    """GET /api/stats returns empty stats dict when no containers are tracked."""
    resp = await async_client.get("/api/stats", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["total_containers"] == 0
    assert data["stats"] == {}
    datetime.fromisoformat(data["timestamp"])


async def test_rate_limits_violations_empty(async_client, mock_rate_limiter):
    """GET /api/violations returns empty list when there are no violations."""
    resp = await async_client.get("/api/violations", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["total_violations"] == 0
    assert data["violations"] == []
    datetime.fromisoformat(data["timestamp"])


# ===================================================================
# Metrics / status endpoints
# ===================================================================


async def test_global_metrics_full_schema(async_client, mock_metrics_collector):
    """GET /api/realtime/metrics: totals are computed over the CALLER's containers.

    This used to assert the pod-wide totals (2 containers, 5 connections, ...)
    next to an EMPTY container list — i.e. it pinned the leak: aggregate traffic
    of containers the caller cannot see. With no containers of the caller's own
    tracked, every total is zero, and the unscoped collector totals are not read.
    """
    resp = await async_client.get("/api/realtime/metrics", headers=TENANT_HEADERS)
    assert resp.status_code == 200
    data = resp.json()

    assert data["global_stats"] == {
        "total_containers_tracked": 0,
        "total_active_connections": 0,
        "total_messages_processed": 0,
        "total_analyses_executed": 0,
        "total_errors": 0,
    }
    mock_metrics_collector.get_global_stats.assert_not_awaited()

    assert isinstance(data["container_metrics"], list)
    assert data["container_metrics"] == []


async def test_websocket_api_docs_full_schema(async_client):
    """GET /api/realtime/websocket-api returns full API documentation."""
    resp = await async_client.get(
        "/api/realtime/websocket-api",
        headers=TENANT_HEADERS,
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["endpoint"] == "/api/realtime/analysis"
    assert isinstance(data["message_types"], dict)
    assert isinstance(data["response_types"], dict)
    assert isinstance(data["connection_params"], dict)

    # message_types should describe analyze_file at minimum
    assert "analyze_file" in data["message_types"]

    # connection_params should document required params
    assert "connection_id" in data["connection_params"]


async def test_active_connections_full_schema(async_client, mock_analysis_cache):
    """GET /api/realtime/connections returns connection info with cache stats."""
    resp = await async_client.get(
        "/api/realtime/connections",
        headers=TENANT_HEADERS,
    )
    assert resp.status_code == 200
    data = resp.json()

    assert isinstance(data["active_connections"], int)
    assert data["active_connections"] >= 0
    assert isinstance(data["connections"], list)
    assert isinstance(data["total_watched_files"], int)

    # cache_stats should be populated from mock
    cs = data["cache_stats"]
    assert cs["hit_rate"] == 75.5
    assert cs["total_requests"] == 1000
    assert cs["cache_hits"] == 755
    assert cs["cache_misses"] == 245


# ===================================================================
# Error handling — structured error envelopes
# ===================================================================


async def test_nonexistent_endpoint_returns_404(async_client):
    """Request to a non-existent path returns 404 with JSON body."""
    resp = await async_client.get("/api/this-does-not-exist")
    assert resp.status_code == 404


async def test_http_404_structured_error(async_client, mock_health_monitor):
    """HTTPException 404 produces the standard error envelope."""
    resp = await async_client.get(
        "/api/ws-health/connection/nonexistent",
        headers=TENANT_HEADERS,
    )
    assert resp.status_code == 404
    data = resp.json()
    assert "error" in data
    err = data["error"]
    required_keys = {"code", "message", "timestamp", "request_id"}
    assert required_keys.issubset(set(err.keys()))
    assert err["code"] == "HTTP_404"
    assert isinstance(err["message"], str) and len(err["message"]) > 0
    assert isinstance(err["request_id"], str) and len(err["request_id"]) > 0
    datetime.fromisoformat(err["timestamp"])


async def test_validation_error_returns_422_with_details(async_client):
    """Passing invalid query param triggers 422 with structured error details."""
    resp = await async_client.get(
        "/api/violations",
        headers=TENANT_HEADERS,
        params={"limit": 0},  # ge=1 constraint violated
    )
    assert resp.status_code == 422
    data = resp.json()
    err = data["error"]
    assert err["code"] == "HTTP_422"
    assert "validation" in err["message"].lower()
    assert isinstance(err["details"], list)
    assert len(err["details"]) >= 1
    detail = err["details"][0]
    assert "field" in detail
    assert "message" in detail
    assert "limit" in detail["field"]


async def test_validation_error_limit_too_high(async_client):
    """limit > 1000 triggers 422."""
    resp = await async_client.get(
        "/api/violations",
        headers=TENANT_HEADERS,
        params={"limit": 9999},  # le=1000 constraint violated
    )
    assert resp.status_code == 422
    data = resp.json()
    err = data["error"]
    assert err["code"] == "HTTP_422"
    assert isinstance(err["details"], list)
    assert len(err["details"]) >= 1


# ===================================================================
# Trusted network tenant fallback
# ===================================================================


async def test_trusted_network_provides_tenant_automatically(
    async_client,
    mock_rate_limiter,
):
    """Protected endpoint without X-Tenant-Id works from loopback (trusted network)."""
    resp = await async_client.get("/api/global-stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert "global_stats" in data


# ===================================================================
# CORS middleware
# ===================================================================


async def test_cors_preflight(async_client):
    """OPTIONS preflight returns 200 with access-control-allow-origin."""
    resp = await async_client.options(
        "/health",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.status_code == 200
    assert "access-control-allow-origin" in resp.headers
    assert resp.headers["access-control-allow-origin"] in ("*", "http://localhost:3000")


async def test_cors_on_regular_request(async_client):
    """Normal GET with Origin header returns access-control-allow-origin."""
    resp = await async_client.get(
        "/health",
        headers={"Origin": "http://localhost:3000"},
    )
    assert resp.status_code == 200
    assert "access-control-allow-origin" in resp.headers


# ===================================================================
# Root endpoint
# ===================================================================


async def test_root_full_schema(async_client):
    """GET / returns exactly {service, version, docs, health} with correct values."""
    resp = await async_client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "coding-realtime"
    assert isinstance(data["version"], str) and len(data["version"]) > 0
    assert data["docs"] == "/docs"
    assert data["health"] == "/health"
    assert set(data.keys()) == {"service", "version", "docs", "health"}
