"""Tests for websocket_rate_limits router endpoints."""

from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from services.websocket_rate_limiter import ContainerRateStats, RateLimitViolation


@pytest.fixture
def tenant_override():
    """Override require_tenant dependency."""
    from coding_client.auth import require_tenant

    app.dependency_overrides[require_tenant] = lambda: "test-tenant"
    yield
    app.dependency_overrides.pop(require_tenant, None)


@pytest.fixture
def container_stats():
    """Real ContainerRateStats instance."""
    return ContainerRateStats(
        container_name="c1",
        total_messages=500,
        messages_per_second=5.0,
        active_connections=2,
        connections_limit=10,
        messages_limit=10.0,
        is_rate_limited=False,
        total_violations=0,
        last_violation=None,
    )


@pytest.fixture
def rate_limited_stats():
    """ContainerRateStats for a rate-limited container with a last_violation."""
    return ContainerRateStats(
        container_name="c2",
        total_messages=9500,
        messages_per_second=15.3,
        active_connections=8,
        connections_limit=10,
        messages_limit=10.0,
        is_rate_limited=True,
        total_violations=7,
        last_violation=datetime(2026, 3, 20, 14, 30, 0, tzinfo=UTC),
    )


@pytest.fixture
def sample_violation():
    """A single RateLimitViolation instance."""
    return RateLimitViolation(
        container_name="c1",
        violation_type="message_rate",
        timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        current_value=15.0,
        limit_value=10.0,
        details={"message": "Message rate limit exceeded", "burst_size": 20},
    )


@pytest.fixture
def global_stats_data():
    """Global stats dict matching the 4 expected fields."""
    return {
        "total_containers": 3,
        "total_active_connections": 10,
        "total_messages_processed": 5000,
        "total_violations": 2,
    }


@contextmanager
def _patch_rate_limiter(mock_limiter, owned=None):
    """Patch the rate limiter singleton and the caller's container ownership.

    These tests are about response SHAPE, so by default the caller owns every
    container the limiter knows (``owned=None``). Tenant scoping itself is
    measured against real stores in ``test_cross_tenant_endpoints.py``.
    """
    # tracked_container_names is a SYNC method on the real limiter.
    mock_limiter.tracked_container_names = MagicMock(return_value={"c1", "c2"})

    async def _owned(names, _tenant):
        names = set(names)
        return names if owned is None else names & set(owned)

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "routers.websocket_rate_limits.get_websocket_rate_limiter",
                return_value=mock_limiter,
            )
        )
        stack.enter_context(
            patch("routers.websocket_rate_limits.containers_owned_by_tenant", side_effect=_owned)
        )
        yield


def _patch_container_validation():
    """Return a patch context manager for container tenant validation."""
    return patch(
        "routers.websocket_rate_limits.validate_and_get_container_for_tenant",
        new_callable=AsyncMock,
    )


def _assert_stats_fields(stats_dict, expected: ContainerRateStats):
    """Assert every field in a serialized ContainerRateStats matches the expected object."""
    assert stats_dict["container_name"] == expected.container_name
    assert stats_dict["total_messages"] == expected.total_messages
    assert stats_dict["messages_per_second"] == expected.messages_per_second
    assert stats_dict["active_connections"] == expected.active_connections
    assert stats_dict["connections_limit"] == expected.connections_limit
    assert stats_dict["messages_limit"] == expected.messages_limit
    assert stats_dict["is_rate_limited"] == expected.is_rate_limited
    assert stats_dict["total_violations"] == expected.total_violations
    if expected.last_violation is None:
        assert stats_dict["last_violation"] is None
    else:
        assert stats_dict["last_violation"] is not None


def _assert_violation_fields(violation_dict, expected: RateLimitViolation):
    """Assert every field in a serialized RateLimitViolation matches the expected object."""
    assert violation_dict["container_name"] == expected.container_name
    assert violation_dict["violation_type"] == expected.violation_type
    assert violation_dict["timestamp"] is not None
    assert violation_dict["current_value"] == expected.current_value
    assert violation_dict["limit_value"] == expected.limit_value
    assert violation_dict["details"] == expected.details


def _assert_zeroed_stats(stats_dict, container_name: str):
    """Assert all numeric fields in the fallback stats are zeroed out."""
    assert stats_dict["container_name"] == container_name
    assert stats_dict["total_messages"] == 0
    assert stats_dict["messages_per_second"] == 0.0
    assert stats_dict["active_connections"] == 0
    assert stats_dict["connections_limit"] == 0
    assert stats_dict["messages_limit"] == 0.0
    assert stats_dict["is_rate_limited"] is False
    assert stats_dict["total_violations"] == 0
    assert stats_dict["last_violation"] is None


# ---------------------------------------------------------------------------
# GET /api/stats/{container_name}
# ---------------------------------------------------------------------------


async def test_get_container_rate_limit_stats(tenant_override, container_stats):
    """Verify all fields in a successful container stats response."""
    mock_limiter = AsyncMock()
    mock_limiter.get_container_stats = AsyncMock(return_value=container_stats)

    with _patch_rate_limiter(mock_limiter), _patch_container_validation():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/stats/c1")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["container_name"] == "c1"
    assert "Rate limit statistics retrieved" in data["message"]
    assert "c1" in data["message"]
    assert "timestamp" in data
    assert "request_id" in data
    _assert_stats_fields(data["stats"], container_stats)


async def test_get_container_rate_limit_stats_rate_limited(tenant_override, rate_limited_stats):
    """Verify all fields when container is actively rate-limited with violations."""
    mock_limiter = AsyncMock()
    mock_limiter.get_container_stats = AsyncMock(return_value=rate_limited_stats)

    with _patch_rate_limiter(mock_limiter), _patch_container_validation():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/stats/c2")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["container_name"] == "c2"
    _assert_stats_fields(data["stats"], rate_limited_stats)
    # Ensure last_violation is serialized (not None)
    assert data["stats"]["last_violation"] is not None
    assert data["stats"]["is_rate_limited"] is True
    assert data["stats"]["total_violations"] == 7


async def test_get_container_stats_error_handling(tenant_override):
    """Error path returns status=error, error message, and zeroed-out fallback stats."""
    mock_limiter = AsyncMock()
    mock_limiter.get_container_stats = AsyncMock(side_effect=RuntimeError("service down"))

    with _patch_rate_limiter(mock_limiter), _patch_container_validation():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/stats/c1")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "error"
    assert "service down" in data["message"]
    assert "Error retrieving rate limit statistics" in data["message"]
    assert data["container_name"] == "c1"
    _assert_zeroed_stats(data["stats"], "c1")


# ---------------------------------------------------------------------------
# GET /api/stats  (all containers)
# ---------------------------------------------------------------------------


async def test_get_all_rate_limit_stats(tenant_override, container_stats, rate_limited_stats):
    """Verify total_containers, status, message, and full stats for each container."""
    mock_limiter = AsyncMock()
    mock_limiter.get_all_stats = AsyncMock(
        return_value={"c1": container_stats, "c2": rate_limited_stats}
    )

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/stats")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["message"] == "Rate limit statistics retrieved for all containers"
    assert data["total_containers"] == 2
    assert "timestamp" in data
    assert "request_id" in data

    # Verify each container's full stats
    assert "c1" in data["stats"]
    assert "c2" in data["stats"]
    _assert_stats_fields(data["stats"]["c1"], container_stats)
    _assert_stats_fields(data["stats"]["c2"], rate_limited_stats)


async def test_get_all_rate_limit_stats_empty(tenant_override):
    """Verify response when no containers are tracked."""
    mock_limiter = AsyncMock()
    mock_limiter.get_all_stats = AsyncMock(return_value={})

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/stats")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["total_containers"] == 0
    assert data["stats"] == {}


async def test_get_all_stats_error_handling(tenant_override):
    """Error path returns status=error, error message, zero containers, and empty stats dict."""
    mock_limiter = AsyncMock()
    mock_limiter.get_all_stats = AsyncMock(side_effect=RuntimeError("redis unavailable"))

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/stats")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "error"
    assert "redis unavailable" in data["message"]
    assert "Error retrieving rate limit statistics" in data["message"]
    assert data["total_containers"] == 0
    assert data["stats"] == {}


# ---------------------------------------------------------------------------
# GET /api/violations
# ---------------------------------------------------------------------------


async def test_get_violations_empty(tenant_override):
    """Empty violations list returns correct status, message, count, and empty list."""
    mock_limiter = AsyncMock()
    mock_limiter.get_violations = AsyncMock(return_value=[])

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/violations")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["message"] == "Rate limit violations retrieved successfully"
    assert data["total_violations"] == 0
    assert data["violations"] == []
    assert "timestamp" in data
    assert "request_id" in data


async def test_get_violations_with_container_filter(tenant_override, sample_violation):
    """Filtered violations returns correct count, message, and full violation content."""
    mock_limiter = AsyncMock()
    mock_limiter.get_violations = AsyncMock(return_value=[sample_violation])

    with _patch_rate_limiter(mock_limiter), _patch_container_validation():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/violations", params={"container_name": "c1"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert "c1" in data["message"]
    assert data["total_violations"] == 1

    violation = data["violations"][0]
    _assert_violation_fields(violation, sample_violation)

    # Confirm the limiter was called with the right filter
    mock_limiter.get_violations.assert_awaited_once_with(
        container_name="c1", limit=100, container_names={"c1"}
    )


async def test_get_violations_without_container_filter(tenant_override, sample_violation):
    """Unfiltered violations returns generic message and full violation content."""
    second_violation = RateLimitViolation(
        container_name="c2",
        violation_type="connection_limit",
        timestamp=datetime(2025, 6, 15, 8, 0, 0, tzinfo=UTC),
        current_value=11.0,
        limit_value=10.0,
        details={"message": "Connection limit exceeded"},
    )
    mock_limiter = AsyncMock()
    mock_limiter.get_violations = AsyncMock(return_value=[sample_violation, second_violation])

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/violations")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["message"] == "Rate limit violations retrieved successfully"
    assert data["total_violations"] == 2
    assert len(data["violations"]) == 2

    _assert_violation_fields(data["violations"][0], sample_violation)
    _assert_violation_fields(data["violations"][1], second_violation)

    mock_limiter.get_violations.assert_awaited_once_with(
        container_name=None, limit=100, container_names={"c1", "c2"}
    )


async def test_get_violations_error_handling(tenant_override):
    """Error path returns status=error, error message, zero count, and empty violations list."""
    mock_limiter = AsyncMock()
    mock_limiter.get_violations = AsyncMock(side_effect=RuntimeError("storage corrupt"))

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/violations")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "error"
    assert "storage corrupt" in data["message"]
    assert "Error retrieving rate limit violations" in data["message"]
    assert data["total_violations"] == 0
    assert data["violations"] == []


# ---------------------------------------------------------------------------
# GET /api/global-stats
# ---------------------------------------------------------------------------


async def test_get_global_stats(tenant_override, global_stats_data):
    """Verify all 4 fields in global_stats dict and response envelope."""
    mock_limiter = AsyncMock()
    mock_limiter.get_global_stats = AsyncMock(return_value=global_stats_data)

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/global-stats")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["message"] == "Global rate limit statistics retrieved successfully"
    assert "timestamp" in data
    assert "request_id" in data

    gs = data["global_stats"]
    assert gs["total_containers"] == 3
    assert gs["total_active_connections"] == 10
    assert gs["total_messages_processed"] == 5000
    assert gs["total_violations"] == 2


async def test_get_global_stats_error_handling(tenant_override):
    """Error path returns status=error, error message, and empty global_stats dict."""
    mock_limiter = AsyncMock()
    mock_limiter.get_global_stats = AsyncMock(side_effect=RuntimeError("limiter not initialized"))

    with _patch_rate_limiter(mock_limiter):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/global-stats")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "error"
    assert "limiter not initialized" in data["message"]
    assert "Error retrieving global rate limit statistics" in data["message"]
    assert data["global_stats"] == {}
