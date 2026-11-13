"""Integration tests for HTTP endpoints — exercises the real FastAPI app.

Validates response schemas, field values, types, and edge cases for every
top-level HTTP endpoint defined in main.py (/, /health, /openapi.json) and
the health sub-router (/api/health/live, /api/health/ready, /api/health/detailed).
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

# NOTE: deliberately NOT marked `integration` — every test here drives the real
# FastAPI app through httpx/ASGI with mocked dependencies, so it needs no live
# service. The marker excluded all 22 tests from CI (pyproject addopts carry
# `-m 'not integration'`). test_websocket_lifecycle.py is unmarked for the
# same reason (it mocks its dependencies too), and
# tests/unit/test_ci_selects_lifecycle_tests.py fails if CI stops selecting it.


# ---------------------------------------------------------------------------
# All operation_ids that must appear in the OpenAPI schema
# ---------------------------------------------------------------------------
EXPECTED_OPERATION_IDS = frozenset(
    {
        "health_check",
        "get_root",
        "health_live",
        "health_ready",
        "health_detailed",
        "get_connection_health",
        "get_ws_container_health",
        "get_container_connections",
        "get_global_health",
        "cleanup_stale_connections",
        "list_container_health",
        "get_container_rate_limit_stats",
        "list_rate_limit_stats",
        "list_rate_limit_violations",
        "get_global_rate_limit_stats",
        "list_active_connections",
        "get_websocket_api_docs",
        "get_container_ws_metrics",
        "get_global_ws_metrics",
    }
)

# ---------------------------------------------------------------------------
# Expected API paths — every registered route should appear in the schema
# ---------------------------------------------------------------------------
EXPECTED_PATHS = frozenset(
    {
        "/",
        "/health",
        "/api/health/live",
        "/api/health/ready",
        "/api/health/detailed",
        "/api/ws-health/connection/{connection_id}",
        "/api/ws-health/container/{container_name}",
        "/api/ws-health/container/{container_name}/connections",
        "/api/ws-health/global",
        "/api/ws-health/cleanup",
        "/api/ws-health/containers",
        "/api/stats/{container_name}",
        "/api/stats",
        "/api/violations",
        "/api/global-stats",
        "/api/realtime/connections",
        "/api/realtime/websocket-api",
        "/api/realtime/metrics/{container_name}",
        "/api/realtime/metrics",
    }
)


# ===================================================================
# GET /  (root)
# ===================================================================


async def test_root_status_code_and_content_type(async_client):
    """GET / returns 200 with application/json content type."""
    resp = await async_client.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"


async def test_root_service_name(async_client):
    """GET / reports the correct service name."""
    data = (await async_client.get("/")).json()
    assert data["service"] == "coding-realtime"


async def test_root_version_is_string(async_client):
    """GET / version field is a non-empty string."""
    data = (await async_client.get("/")).json()
    assert isinstance(data["version"], str)
    assert len(data["version"]) > 0


async def test_root_docs_url(async_client):
    """GET / docs field points to /docs when docs are enabled."""
    data = (await async_client.get("/")).json()
    assert data["docs"] == "/docs"


async def test_root_health_url(async_client):
    """GET / health field points to /health."""
    data = (await async_client.get("/")).json()
    assert data["health"] == "/health"


async def test_root_contains_exactly_expected_keys(async_client):
    """GET / response contains exactly {service, version, docs, health}."""
    data = (await async_client.get("/")).json()
    assert set(data.keys()) == {"service", "version", "docs", "health"}


# ===================================================================
# GET /health
# ===================================================================


async def test_health_status_code(async_client):
    """GET /health returns 200."""
    resp = await async_client.get("/health")
    assert resp.status_code == 200


async def test_health_all_fields_present(async_client):
    """GET /health response contains all expected fields."""
    data = (await async_client.get("/health")).json()
    expected_keys = {"status", "service", "version", "environment", "redis", "timestamp"}
    assert set(data.keys()) == expected_keys


async def test_health_status_ok_with_mock_redis(async_client):
    """GET /health reports status=ok because mock Redis ping succeeds."""
    data = (await async_client.get("/health")).json()
    assert data["status"] == "ok"


async def test_health_service_name(async_client):
    """GET /health service field matches 'coding-realtime'."""
    data = (await async_client.get("/health")).json()
    assert data["service"] == "coding-realtime"


async def test_health_version_is_string(async_client):
    """GET /health version is a non-empty string."""
    data = (await async_client.get("/health")).json()
    assert isinstance(data["version"], str)
    assert len(data["version"]) > 0


async def test_health_environment(async_client):
    """GET /health environment matches mock_settings value."""
    data = (await async_client.get("/health")).json()
    assert data["environment"] == "testing"


async def test_health_redis_connected(async_client):
    """GET /health reports redis=connected when mock Redis ping succeeds."""
    data = (await async_client.get("/health")).json()
    assert data["redis"] == "connected"


async def test_health_timestamp_is_valid_iso8601(async_client):
    """GET /health timestamp is a parseable ISO-8601 datetime string."""
    data = (await async_client.get("/health")).json()
    ts = data["timestamp"]
    assert isinstance(ts, str)
    # datetime.fromisoformat handles the standard ISO-8601 formats
    parsed = datetime.fromisoformat(ts)
    assert parsed.year >= 2025


async def test_health_degraded_when_redis_ping_fails(async_client, mock_redis):
    """GET /health reports status=degraded when Redis ping raises."""
    mock_redis.ping = AsyncMock(side_effect=ConnectionError("boom"))
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["redis"] == "disconnected"


# ===================================================================
# GET /api/health/live
# ===================================================================


async def test_health_live_status_code(async_client):
    """GET /api/health/live returns 200."""
    resp = await async_client.get("/api/health/live")
    assert resp.status_code == 200


async def test_health_live_returns_alive(async_client):
    """GET /api/health/live body is exactly {"status": "alive"}."""
    data = (await async_client.get("/api/health/live")).json()
    assert data == {"status": "alive"}


# ===================================================================
# GET /api/health/ready
# ===================================================================


async def test_health_ready_status_code(async_client):
    """GET /api/health/ready returns 200 when Redis is up."""
    resp = await async_client.get("/api/health/ready")
    assert resp.status_code == 200


async def test_health_ready_reports_ready_with_redis(async_client):
    """GET /api/health/ready body is {"status":"ready","redis":"connected"} when mock Redis is up."""
    data = (await async_client.get("/api/health/ready")).json()
    assert data["status"] == "ready"
    assert data["redis"] == "connected"


async def test_health_ready_returns_503_when_redis_fails(async_client, mock_redis):
    """GET /api/health/ready returns 503 with not_ready when Redis ping raises."""
    mock_redis.ping = AsyncMock(side_effect=ConnectionError("down"))
    resp = await async_client.get("/api/health/ready")
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "not_ready"
    assert data["redis"] == "disconnected"


# ===================================================================
# GET /openapi.json — schema completeness
# ===================================================================


async def test_openapi_status_code(async_client):
    """GET /openapi.json returns 200."""
    resp = await async_client.get("/openapi.json")
    assert resp.status_code == 200


async def test_openapi_top_level_keys(async_client):
    """OpenAPI schema contains standard top-level keys."""
    data = (await async_client.get("/openapi.json")).json()
    for key in ("openapi", "info", "paths"):
        assert key in data, f"Missing top-level key: {key}"


async def test_openapi_info_title(async_client):
    """OpenAPI info.title matches the service name."""
    data = (await async_client.get("/openapi.json")).json()
    assert data["info"]["title"] == "coding-realtime"


async def test_openapi_all_expected_paths_present(async_client):
    """Every expected API path appears in the OpenAPI schema."""
    data = (await async_client.get("/openapi.json")).json()
    actual_paths = set(data["paths"].keys())
    missing = EXPECTED_PATHS - actual_paths
    assert not missing, f"Missing paths in OpenAPI schema: {missing}"


async def test_openapi_all_operation_ids_present(async_client):
    """Every expected operation_id appears somewhere in the OpenAPI schema."""
    data = (await async_client.get("/openapi.json")).json()
    found_ids: set[str] = set()
    for _path, methods in data["paths"].items():
        for _method, spec in methods.items():
            if isinstance(spec, dict) and "operationId" in spec:
                found_ids.add(spec["operationId"])
    missing = EXPECTED_OPERATION_IDS - found_ids
    assert not missing, f"Missing operation_ids: {missing}"


async def test_openapi_no_duplicate_operation_ids(async_client):
    """All operation_ids in the OpenAPI schema are unique."""
    data = (await async_client.get("/openapi.json")).json()
    seen: list[str] = []
    for _path, methods in data["paths"].items():
        for _method, spec in methods.items():
            if isinstance(spec, dict) and "operationId" in spec:
                seen.append(spec["operationId"])
    assert len(seen) == len(set(seen)), (
        f"Duplicate operation_ids: {[x for x in seen if seen.count(x) > 1]}"
    )


async def test_openapi_health_check_operation(async_client):
    """The /health GET endpoint has operation_id 'health_check'."""
    data = (await async_client.get("/openapi.json")).json()
    assert data["paths"]["/health"]["get"]["operationId"] == "health_check"


async def test_openapi_root_operation(async_client):
    """The / GET endpoint has operation_id 'get_root'."""
    data = (await async_client.get("/openapi.json")).json()
    assert data["paths"]["/"]["get"]["operationId"] == "get_root"
