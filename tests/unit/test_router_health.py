"""Tests for health probe endpoints."""

import json
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture
def mock_redis_connected():
    """Mock a connected Redis instance."""
    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)
    return mock


@pytest.fixture
def mock_redis_disconnected():
    """Mock a Redis that fails on ping."""
    mock = AsyncMock()
    mock.ping = AsyncMock(side_effect=ConnectionError("refused"))
    return mock


# ── /live ──────────────────────────────────────────────────────────


async def test_liveness_returns_alive():
    from routers.health import liveness

    result = await liveness()
    assert result == {"status": "alive"}


async def test_liveness_returns_exactly_one_key():
    """Liveness must return only 'status' — no extra fields that could confuse probes."""
    from routers.health import liveness

    result = await liveness()
    assert list(result.keys()) == ["status"]


# ── /ready ─────────────────────────────────────────────────────────


async def test_readiness_redis_connected(mock_redis_connected):
    import main
    from routers.health import readiness

    original = main._redis
    main._redis = mock_redis_connected
    try:
        resp = await readiness()
        assert resp.status_code == 200
        body = json.loads(bytes(resp.body))
        assert body == {"status": "ready", "redis": "connected"}
    finally:
        main._redis = original


async def test_readiness_redis_disconnected(mock_redis_disconnected):
    import main
    from routers.health import readiness

    original = main._redis
    main._redis = mock_redis_disconnected
    try:
        resp = await readiness()
        assert resp.status_code == 503
        body = json.loads(bytes(resp.body))
        assert body == {"status": "not_ready", "redis": "disconnected"}
    finally:
        main._redis = original


async def test_readiness_redis_none():
    """When _redis is None (not yet initialised), readiness must report 503."""
    import main
    from routers.health import readiness

    original = main._redis
    main._redis = None
    try:
        resp = await readiness()
        assert resp.status_code == 503
        body = json.loads(bytes(resp.body))
        assert body["status"] == "not_ready"
        assert body["redis"] == "disconnected"
    finally:
        main._redis = original


async def test_readiness_redis_unexpected_exception():
    """Any exception from ping (not just ConnectionError) must yield 503."""
    import main
    from routers.health import readiness

    mock = AsyncMock()
    mock.ping = AsyncMock(side_effect=TimeoutError("timed out"))

    original = main._redis
    main._redis = mock
    try:
        resp = await readiness()
        assert resp.status_code == 503
        body = json.loads(bytes(resp.body))
        assert body == {"status": "not_ready", "redis": "disconnected"}
    finally:
        main._redis = original


# ── /detailed ──────────────────────────────────────────────────────

# Helper to build analysis/containers mocks and patches


def _make_service_mocks(*, analysis_healthy: bool, containers_healthy: bool):
    """Return (mock_analysis, mock_containers, patch_analysis, patch_containers)."""
    mock_analysis = AsyncMock()
    mock_analysis.health_check = AsyncMock(return_value=analysis_healthy)
    mock_containers = AsyncMock()
    mock_containers.health_check = AsyncMock(return_value=containers_healthy)

    patch_analysis = patch(
        "clients.coding_analysis.get_coding_analysis_client",
        return_value=mock_analysis,
    )
    patch_containers = patch(
        "clients.coding_containers.get_coding_containers_client",
        return_value=mock_containers,
    )
    return mock_analysis, mock_containers, patch_analysis, patch_containers


def _assert_detailed_structure(result: dict) -> None:
    """Assert the response contains exactly the expected top-level keys with correct types."""
    assert set(result.keys()) == {"status", "service", "version", "uptime_seconds", "checks"}
    assert isinstance(result["status"], str)
    assert isinstance(result["service"], str)
    assert isinstance(result["version"], str)
    assert isinstance(result["uptime_seconds"], float)
    assert isinstance(result["checks"], dict)
    assert set(result["checks"].keys()) == {"redis", "coding_analysis", "coding_containers"}


async def test_detailed_all_healthy(mock_redis_connected):
    """When every dependency is up, status is 'healthy' and every check passes."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=True, containers_healthy=True)

    original = main._redis
    main._redis = mock_redis_connected
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "healthy"
    assert result["service"] == "coding-realtime"
    assert result["version"] == "1.0.0"
    assert result["uptime_seconds"] > 0
    assert result["checks"]["redis"] == "connected"
    assert result["checks"]["coding_analysis"] is True
    assert result["checks"]["coding_containers"] is True


async def test_detailed_redis_down():
    """Redis None -> status 'degraded', but analysis and containers still report True."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=True, containers_healthy=True)

    original = main._redis
    main._redis = None
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "degraded"
    assert result["service"] == "coding-realtime"
    assert result["checks"]["redis"] == "disconnected"
    # The other two dependencies are still reachable
    assert result["checks"]["coding_analysis"] is True
    assert result["checks"]["coding_containers"] is True


async def test_detailed_analysis_down(mock_redis_connected):
    """Only coding_analysis is down -> status 'degraded', with the specific check False."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=False, containers_healthy=True)

    original = main._redis
    main._redis = mock_redis_connected
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "degraded"
    assert result["checks"]["redis"] == "connected"
    assert result["checks"]["coding_analysis"] is False
    assert result["checks"]["coding_containers"] is True


async def test_detailed_containers_down(mock_redis_connected):
    """Only coding_containers is down -> status 'degraded', with the specific check False."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=True, containers_healthy=False)

    original = main._redis
    main._redis = mock_redis_connected
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "degraded"
    assert result["checks"]["redis"] == "connected"
    assert result["checks"]["coding_analysis"] is True
    assert result["checks"]["coding_containers"] is False


async def test_detailed_all_down():
    """Every dependency is down -> status 'degraded', every check reports failure."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=False, containers_healthy=False)

    original = main._redis
    main._redis = None
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "degraded"
    assert result["service"] == "coding-realtime"
    assert result["version"] == "1.0.0"
    assert result["uptime_seconds"] > 0
    assert result["checks"]["redis"] == "disconnected"
    assert result["checks"]["coding_analysis"] is False
    assert result["checks"]["coding_containers"] is False


async def test_detailed_analysis_client_throws(mock_redis_connected):
    """If get_coding_analysis_client().health_check() raises, the check falls back to False."""
    import main
    from routers.health import detailed

    mock_analysis = AsyncMock()
    mock_analysis.health_check = AsyncMock(side_effect=RuntimeError("connection refused"))
    mock_containers = AsyncMock()
    mock_containers.health_check = AsyncMock(return_value=True)

    original = main._redis
    main._redis = mock_redis_connected
    try:
        with (
            patch(
                "clients.coding_analysis.get_coding_analysis_client",
                return_value=mock_analysis,
            ),
            patch(
                "clients.coding_containers.get_coding_containers_client",
                return_value=mock_containers,
            ),
        ):
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "degraded"
    assert result["checks"]["coding_analysis"] is False
    assert result["checks"]["coding_containers"] is True


async def test_detailed_redis_ping_exception(mock_redis_disconnected):
    """Redis exists but ping raises -> redis check reports 'disconnected'."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=True, containers_healthy=True)

    original = main._redis
    main._redis = mock_redis_disconnected
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    _assert_detailed_structure(result)
    assert result["status"] == "degraded"
    assert result["checks"]["redis"] == "disconnected"
    assert result["checks"]["coding_analysis"] is True
    assert result["checks"]["coding_containers"] is True


async def test_detailed_uptime_is_positive():
    """uptime_seconds must be a positive number (the process has been alive for >0 sec)."""
    import main
    from routers.health import detailed

    _, _, pa, pc = _make_service_mocks(analysis_healthy=True, containers_healthy=True)

    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)

    original = main._redis
    main._redis = mock
    try:
        with pa, pc:
            result = await detailed(tenant_id="test-tenant")
    finally:
        main._redis = original

    assert result["uptime_seconds"] > 0
    # uptime_seconds is rounded to 2 decimal places per implementation
    assert result["uptime_seconds"] == round(result["uptime_seconds"], 2)
