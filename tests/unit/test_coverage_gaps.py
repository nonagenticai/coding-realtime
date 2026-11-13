"""Tests targeting coverage gaps across modules.

Covers:
- container_validation edge cases (76% -> higher)
- parameter_validation edge cases (82% -> higher)
- main.py shutdown paths
- models/common PaginatedResponse validator
- models/errors timestamp serializer
- responses timeout_error
- router error/exception branches
- services analysis_cache initialize + get_cache_stats + shutdown
- websocket_connection_registry lazy imports, serializer, stats
- websocket_health_monitor serializers
- websocket_metrics reset + global + all metrics
- path_resolution validate_path_in_container_sync ValueError branch

"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# ---------------------------------------------------------------------------
# container_validation
# ---------------------------------------------------------------------------


class TestContainerValidation:
    """Covers lines in container_validation.py that were missing."""

    async def test_validate_and_get_container_for_tenant_success(self):
        """Test successful container validation passes args correctly."""
        from utils.container_validation import validate_and_get_container_for_tenant

        mock_client = AsyncMock()
        # The REAL coding-containers body is the `ContainerDetailResponse`
        # ENVELOPE, whose own `status` is "success". Faking a flat container here
        # is what let the envelope reach consumers unnoticed.
        mock_client.get_container = AsyncMock(
            return_value={
                "status": "success",
                "container": {"container_name": "c1", "status": "running"},
            }
        )
        with patch("utils.container_validation._get_containers_client", return_value=mock_client):
            result = await validate_and_get_container_for_tenant("c1", "t1")

            # Verify the mock was called with the correct container name AND tenant_id
            mock_client.get_container.assert_called_once_with("c1", tenant_id="t1")
            # The CONTAINER is returned, not the envelope.
            assert result["container_name"] == "c1"
            assert result["status"] == "running"

    async def test_validate_and_get_container_for_tenant_not_found(self):
        """Test container not found raises 404 with container name in detail."""
        from utils.container_validation import validate_and_get_container_for_tenant

        mock_client = AsyncMock()
        mock_client.get_container = AsyncMock(return_value=None)
        with patch("utils.container_validation._get_containers_client", return_value=mock_client):
            with pytest.raises(HTTPException) as exc_info:
                await validate_and_get_container_for_tenant("missing", "t1")
            assert exc_info.value.status_code == 404
            # Verify the error detail includes the container name
            assert "missing" in exc_info.value.detail

    async def test_validate_and_get_container_for_tenant_generic_error(self):
        """Test generic error during validation raises 404 (not 500)."""
        from utils.container_validation import validate_and_get_container_for_tenant

        mock_client = AsyncMock()
        mock_client.get_container = AsyncMock(side_effect=RuntimeError("boom"))
        with patch("utils.container_validation._get_containers_client", return_value=mock_client):
            with pytest.raises(HTTPException) as exc_info:
                await validate_and_get_container_for_tenant("c1", "t1")
            # Generic error maps to 404, not 500 (BOLA protection)
            assert exc_info.value.status_code == 404
            assert "c1" in exc_info.value.detail

    async def test_validate_and_get_container_no_tenant(self):
        """Test legacy admin validation passes tenant_id=None to the client."""
        from utils.container_validation import validate_and_get_container

        mock_client = AsyncMock()
        mock_client.get_container = AsyncMock(
            return_value={
                "status": "success",
                "container": {"container_name": "c1", "status": "running"},
            }
        )
        with patch("utils.container_validation._get_containers_client", return_value=mock_client):
            result = await validate_and_get_container("c1")

            # Verify tenant_id=None is passed through (admin/internal path)
            mock_client.get_container.assert_called_once_with("c1", tenant_id=None)
            assert result["container_name"] == "c1"
            assert result["status"] == "running"

    def test_get_containers_client_lazy_init(self):
        """Test lazy initialization returns a CodingContainersClient and is a singleton."""
        import utils.container_validation as cv
        from clients.coding_containers import CodingContainersClient

        original = cv._containers_client
        cv._containers_client = None
        try:
            client = cv._get_containers_client()
            # Verify the correct TYPE, not just "is not None"
            assert isinstance(client, CodingContainersClient)

            # Verify calling it twice returns the same instance (singleton)
            client2 = cv._get_containers_client()
            assert client is client2
        finally:
            cv._containers_client = original

    def test_container_not_found_exception(self):
        """ContainerNotFoundException stores name and produces proper str."""
        from utils.container_validation import ContainerNotFoundException

        exc = ContainerNotFoundException("test-c")
        assert exc.container_name == "test-c"
        assert isinstance(exc, Exception)
        # Default message includes quoted container name
        assert "test-c" in str(exc)
        assert "not found" in str(exc).lower()

    def test_container_not_found_exception_custom_message(self):
        from utils.container_validation import ContainerNotFoundException

        exc = ContainerNotFoundException("test-c", message="custom msg")
        assert exc.message == "custom msg"
        # str() should use the custom message
        assert str(exc) == "custom msg"

    def test_container_access_denied_exception(self):
        """ContainerAccessDeniedException inherits from Exception with proper str."""
        from utils.container_validation import ContainerAccessDeniedException

        exc = ContainerAccessDeniedException("test-c")
        assert exc.container_name == "test-c"
        assert isinstance(exc, Exception)
        assert "test-c" in str(exc)
        assert "access denied" in str(exc).lower()

    def test_container_access_denied_custom_message(self):
        from utils.container_validation import ContainerAccessDeniedException

        exc = ContainerAccessDeniedException("test-c", message="no")
        assert exc.message == "no"
        assert str(exc) == "no"

    def test_invalid_container_state_exception(self):
        """InvalidContainerStateException stores state and includes it in str."""
        from utils.container_validation import InvalidContainerStateException

        exc = InvalidContainerStateException("test-c", state="stopped")
        assert exc.container_name == "test-c"
        assert exc.state == "stopped"
        assert isinstance(exc, Exception)
        assert "stopped" in str(exc)
        assert "test-c" in str(exc)

    def test_invalid_container_state_no_state(self):
        from utils.container_validation import InvalidContainerStateException

        exc = InvalidContainerStateException("test-c")
        assert exc.state is None
        assert "invalid state" in str(exc).lower()

    def test_handle_container_not_found(self):
        """handle_container_not_found produces HTTPException with full detail dict."""
        from utils.container_validation import handle_container_not_found

        exc = handle_container_not_found("c1")
        assert exc.status_code == 404
        assert isinstance(exc, HTTPException)
        # Verify the detail dict structure
        detail = cast(dict, exc.detail)
        assert detail["error"] == "ContainerNotFound"
        assert detail["container_name"] == "c1"
        assert "c1" in detail["message"]

    def test_handle_container_access_denied(self):
        """handle_container_access_denied produces HTTPException with full detail dict."""
        from utils.container_validation import handle_container_access_denied

        exc = handle_container_access_denied("c1")
        assert exc.status_code == 403
        assert isinstance(exc, HTTPException)
        detail = cast(dict, exc.detail)
        assert detail["error"] == "ContainerAccessDenied"
        assert detail["container_name"] == "c1"
        assert "permission" in detail["message"].lower()

    def test_handle_invalid_container_state(self):
        """handle_invalid_container_state produces HTTPException with state in detail."""
        from utils.container_validation import handle_invalid_container_state

        exc = handle_invalid_container_state("c1", "stopped")
        assert exc.status_code == 409
        assert isinstance(exc, HTTPException)
        detail = cast(dict, exc.detail)
        assert detail["error"] == "InvalidContainerState"
        assert detail["container_name"] == "c1"
        assert detail["current_state"] == "stopped"
        assert "stopped" in detail["message"]


# ---------------------------------------------------------------------------
# parameter_validation
# ---------------------------------------------------------------------------


class TestParameterValidation:
    """Covers gap lines in parameter_validation.py."""

    def test_get_null_string_error_message_non_null_value(self):
        """For non-null-like values, message includes BOTH value AND parameter name."""
        from utils.parameter_validation import _get_null_string_error_message

        msg = _get_null_string_error_message("some-value", "param_name")
        # Exact format: "Parameter '{param_name}' cannot be '{value}'"
        assert "param_name" in msg
        assert "some-value" in msg
        assert msg == "Parameter 'param_name' cannot be 'some-value'"

    def test_get_null_string_error_message_null_like_value(self):
        """For null-like values, message format is different."""
        from utils.parameter_validation import _get_null_string_error_message

        msg = _get_null_string_error_message("null", "my_param")
        assert msg == "Invalid parameter value: 'null'"

    def test_validate_path_parameter_empty_string_after_strip(self):
        from utils.parameter_validation import validate_path_parameter

        with pytest.raises(HTTPException) as exc_info:
            validate_path_parameter("   ", parameter_name="test", allow_none=False)
        assert exc_info.value.status_code == 400

    def test_validate_path_parameter_returns_stripped_path(self):
        """Validate that a valid path string returns a proper Path object."""
        from utils.parameter_validation import validate_path_parameter

        result = validate_path_parameter("/home/user/project", parameter_name="root")
        assert isinstance(result, Path)
        assert result == Path("/home/user/project")

    def test_validate_path_parameter_strips_whitespace(self):
        """Path with whitespace is stripped before conversion."""
        from utils.parameter_validation import validate_path_parameter

        result = validate_path_parameter("  /home/user/project  ", parameter_name="root")
        assert isinstance(result, Path)
        assert result == Path("/home/user/project")

    def test_validate_path_parameter_must_exist_missing(self, tmp_path):
        from utils.parameter_validation import validate_path_parameter

        with pytest.raises(HTTPException) as exc_info:
            validate_path_parameter(
                str(tmp_path / "nonexistent.txt"),
                parameter_name="test",
                must_exist=True,
            )
        assert exc_info.value.status_code == 400

    def test_validate_path_parameter_null_like_rejected(self):
        """Null-like strings ('null', 'none', etc.) are rejected with 400."""
        from utils.parameter_validation import validate_path_parameter

        with pytest.raises(HTTPException) as exc_info:
            validate_path_parameter("null", parameter_name="test")
        assert exc_info.value.status_code == 400

    def test_validate_project_root_in_test_paths(self):
        """Known test paths are returned as Path objects."""
        from utils.parameter_validation import validate_project_root

        result = validate_project_root("/home/user/myproject")
        assert isinstance(result, Path)
        assert result == Path("/home/user/myproject")

    def test_validate_project_root_none_allowed(self):
        """None is allowed when allow_none=True (default)."""
        from utils.parameter_validation import validate_project_root

        result = validate_project_root(None)
        assert result is None

    def test_validate_project_root_none_rejected(self):
        """None is rejected when allow_none=False."""
        from utils.parameter_validation import validate_project_root

        with pytest.raises(HTTPException) as exc_info:
            validate_project_root(None, allow_none=False)
        assert exc_info.value.status_code == 400

    def test_validate_project_root_regular_path(self):
        """Non-test paths go through validate_path_parameter."""
        from utils.parameter_validation import validate_project_root

        result = validate_project_root("/some/custom/path")
        assert isinstance(result, Path)
        assert result == Path("/some/custom/path")


# ---------------------------------------------------------------------------
# models/common - PaginatedResponse validator
# ---------------------------------------------------------------------------


class TestPaginatedResponse:
    def test_missing_pagination_keys_raises_validation_error(self):
        """Verify the EXACT validation error references pagination field."""
        from pydantic import ValidationError

        from models.common import PaginatedResponse, ResponseStatus

        with pytest.raises(ValidationError) as exc_info:
            PaginatedResponse(
                status=ResponseStatus.SUCCESS,
                message="ok",
                pagination={"page": 1},
                items=[],
            )
        errors = exc_info.value.errors()
        # Pydantic should report exactly one error on the pagination field
        pagination_errors = [e for e in errors if "pagination" in str(e.get("loc", ""))]
        assert len(pagination_errors) == 1
        assert pagination_errors[0]["type"] == "value_error"

    def test_valid_pagination(self):
        """Verify ALL pagination fields and items are accessible."""
        from models.common import PaginatedResponse, ResponseStatus

        resp = PaginatedResponse(
            status=ResponseStatus.SUCCESS,
            message="ok",
            pagination={"page": 1, "limit": 10, "total": 100, "pages": 10},
            items=["a", "b"],
        )
        assert resp.pagination["page"] == 1
        assert resp.pagination["limit"] == 10
        assert resp.pagination["total"] == 100
        assert resp.pagination["pages"] == 10
        # Verify items are stored correctly
        assert resp.items == ["a", "b"]
        assert resp.status == ResponseStatus.SUCCESS
        assert resp.message == "ok"

    def test_pagination_extra_keys_allowed(self):
        """Extra keys beyond required are allowed."""
        from models.common import PaginatedResponse, ResponseStatus

        resp = PaginatedResponse(
            status=ResponseStatus.SUCCESS,
            message="ok",
            pagination={"page": 1, "limit": 10, "total": 100, "pages": 10, "has_next": True},
            items=[],
        )
        assert resp.pagination["has_next"] is True


# ---------------------------------------------------------------------------
# models/errors - timestamp serializer
# ---------------------------------------------------------------------------


class TestErrorModels:
    def test_serialize_naive_timestamp(self):
        """Naive datetime gets UTC tzinfo and is serialized to valid ISO format."""
        from models.errors import StandardErrorResponse

        resp = StandardErrorResponse(
            message="oops",
            timestamp=datetime(2025, 1, 1, 12, 0, 0),
            request_id="req_123",
            path="/api/test",
        )
        dumped = resp.model_dump()
        ts_str = dumped["timestamp"]
        # Parse back to verify it's valid ISO format
        parsed = datetime.fromisoformat(ts_str)
        assert parsed.year == 2025
        assert parsed.month == 1
        assert parsed.day == 1
        assert parsed.hour == 12
        # UTC should have been applied to the naive datetime
        assert parsed.tzinfo is not None

    def test_serialize_utc_timestamp(self):
        """UTC-aware datetime is serialized correctly."""
        from models.errors import StandardErrorResponse

        ts = datetime(2025, 6, 15, 8, 30, 0, tzinfo=UTC)
        resp = StandardErrorResponse(
            message="fail",
            timestamp=ts,
            request_id="req_456",
            path="/api/other",
        )
        dumped = resp.model_dump()
        parsed = datetime.fromisoformat(dumped["timestamp"])
        assert parsed.year == 2025
        assert parsed.month == 6
        assert parsed.day == 15

    def test_serialize_offset_timestamp(self):
        """Timezone-aware (non-UTC) datetime is serialized without modification."""
        from models.errors import StandardErrorResponse

        tz_plus5 = timezone(timedelta(hours=5))
        ts = datetime(2025, 3, 20, 14, 0, 0, tzinfo=tz_plus5)
        resp = StandardErrorResponse(
            message="err",
            timestamp=ts,
            request_id="req_789",
            path="/api/stuff",
        )
        dumped = resp.model_dump()
        parsed = datetime.fromisoformat(dumped["timestamp"])
        assert parsed.tzinfo is not None


# ---------------------------------------------------------------------------
# responses - timeout_error
# ---------------------------------------------------------------------------


class TestResponses:
    def test_timeout_error(self):
        """Verify full HTTPException structure for timeout_error."""
        from responses import timeout_error

        exc = timeout_error("took too long")
        assert exc.status_code == 504
        assert isinstance(exc, HTTPException)
        # Verify the detail dict structure
        detail = cast(dict, exc.detail)
        assert detail["status"] == "error"
        assert detail["message"] == "took too long"
        assert detail["error_code"] == "TIMEOUT"
        assert "details" in detail

    def test_timeout_error_default_message(self):
        """Default message when none provided."""
        from responses import timeout_error

        exc = timeout_error()
        assert exc.status_code == 504
        detail = cast(dict, exc.detail)
        assert detail["message"] == "Request timed out"


# ---------------------------------------------------------------------------
# analysis_cache - initialize, get_cache_stats, shutdown
# ---------------------------------------------------------------------------


class TestAnalysisCache:
    async def test_initialize(self):
        """After initialize(), _initialized is True and cache is functional."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache()
        assert cache._initialized is False
        await cache.initialize()
        assert cache._initialized is True

    async def test_has_content_changed_tracks_hits_and_misses(self):
        """Put items in cache, verify hit/miss counters."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache()
        # First access: should be a miss (content changed)
        assert cache.has_content_changed("file.py", "print('hello')") is True
        assert cache._misses == 1
        assert cache._hits == 0

        # Same content again: should be a hit (no change)
        assert cache.has_content_changed("file.py", "print('hello')") is False
        assert cache._hits == 1
        assert cache._misses == 1

        # Different content: should be a miss
        assert cache.has_content_changed("file.py", "print('world')") is True
        assert cache._misses == 2

    async def test_get_cache_stats_reflects_actual_entries(self):
        """get_cache_stats returns counts that match actual cache state."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache()

        # Empty cache
        stats = await cache.get_cache_stats()
        assert stats["total_entries"] == 0
        assert stats["hit_rate"] == 0.0
        assert stats["source"] == "local_adapter"

        # Add items
        cache.has_content_changed("a.py", "aaa")
        cache.has_content_changed("b.py", "bbb")
        stats = await cache.get_cache_stats()
        assert stats["total_entries"] == 2
        assert stats["total_misses"] == 2
        assert stats["total_requests"] == 2

    async def test_get_cache_stats_alias(self):
        """get_cache_stats is an alias for get_stats and returns the same result."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache()
        cache.has_content_changed("x.py", "code")
        stats_via_alias = await cache.get_cache_stats()
        stats_direct = await cache.get_stats()
        assert stats_via_alias["total_entries"] == stats_direct["total_entries"]
        assert stats_via_alias["total_misses"] == stats_direct["total_misses"]

    async def test_shutdown(self):
        """After shutdown, cache is empty."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache()
        cache._local_cache["a"] = "b"
        cache._local_cache["c"] = "d"
        assert len(cache._local_cache) == 2
        await cache.shutdown()
        assert len(cache._local_cache) == 0

    async def test_get_stats_never_calls_coding_analysis(self):
        """Stats are local only: coding-analysis deleted /api/quality/cache/stats
        (coding-analysis#106), and a swallowed 404 used to report hit_rate 0."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache()
        cache.has_content_changed("f.py", "code")  # 1 miss
        cache.has_content_changed("f.py", "code")  # 1 hit
        # A raising factory would be swallowed by a try/except fallback, so assert
        # on the call itself rather than on an exception.
        with patch("clients.coding_analysis.get_coding_analysis_client") as factory:
            stats = await cache.get_stats()
        factory.assert_not_called()
        assert stats["source"] == "local_adapter"
        assert stats["total_entries"] == 1
        assert stats["total_hits"] == 1
        assert stats["total_misses"] == 1
        assert stats["hit_rate"] == 0.5

    async def test_cache_eviction(self):
        """When cache exceeds max_local_entries, oldest entry is evicted."""
        from services.analysis_cache import AnalysisCache

        cache = AnalysisCache(max_local_entries=2)
        cache.has_content_changed("a.py", "a")
        cache.has_content_changed("b.py", "b")
        assert len(cache._local_cache) == 2

        # Adding a third should evict "a.py"
        cache.has_content_changed("c.py", "c")
        assert len(cache._local_cache) == 2
        assert "a.py" not in cache._local_cache
        assert "b.py" in cache._local_cache
        assert "c.py" in cache._local_cache


# ---------------------------------------------------------------------------
# websocket_connection_registry - serializer, lazy imports
# ---------------------------------------------------------------------------


class TestConnectionRegistry:
    def test_connection_info_serializer(self):
        """Serialized datetime fields are valid ISO format strings."""
        from services.websocket_connection_registry import ConnectionInfo

        now = datetime.now(UTC)
        info = ConnectionInfo(
            connection_id="c1",
            container_name="cnt",
            websocket=MagicMock(),
            connected_at=now,
            last_activity=now,
        )
        dumped = info.model_dump()
        # Verify they are strings
        assert isinstance(dumped["connected_at"], str)
        assert isinstance(dumped["last_activity"], str)
        # Verify they parse back to valid datetimes
        parsed_connected = datetime.fromisoformat(dumped["connected_at"])
        parsed_activity = datetime.fromisoformat(dumped["last_activity"])
        assert parsed_connected.year == now.year
        assert parsed_activity.year == now.year

    def test_connection_info_serializer_none(self):
        """Test serializer with None datetime returns None."""
        from services.websocket_connection_registry import ConnectionInfo

        info = ConnectionInfo(
            connection_id="c1",
            container_name="cnt",
            websocket=MagicMock(),
            connected_at=datetime.now(UTC),
            last_activity=datetime.now(UTC),
        )
        result = info.serialize_datetime(None)
        assert result is None

    async def test_get_all_container_names(self, connection_registry, mock_websocket):
        """get_all_container_names returns the exact set of registered containers."""
        cid1 = await connection_registry.register_connection(
            websocket=mock_websocket,
            container_name="cnt-alpha",
        )
        mock_ws2 = AsyncMock()
        mock_ws2.send_json = AsyncMock()
        mock_ws2.client_state = __import__(
            "fastapi.websockets", fromlist=["WebSocketState"]
        ).WebSocketState.CONNECTED
        cid2 = await connection_registry.register_connection(
            websocket=mock_ws2,
            container_name="cnt-beta",
        )

        names = await connection_registry.get_all_container_names()
        # Verify exact set, not just "in"
        assert set(names) == {"cnt-alpha", "cnt-beta"}

        await connection_registry.unregister_connection(cid1)
        await connection_registry.unregister_connection(cid2)

    async def test_get_container_connection_count(self, connection_registry, mock_websocket):
        cid = await connection_registry.register_connection(
            websocket=mock_websocket,
            container_name="test-cnt",
        )
        count = await connection_registry.get_container_connection_count("test-cnt")
        assert count == 1
        await connection_registry.unregister_connection(cid)

    async def test_get_all_stats(self, connection_registry, mock_websocket):
        cid = await connection_registry.register_connection(
            websocket=mock_websocket,
            container_name="test-cnt",
        )
        stats = await connection_registry.get_all_stats()
        assert "test-cnt" in stats
        assert stats["test-cnt"].active_connections == 1
        await connection_registry.unregister_connection(cid)

    async def test_broadcast_to_container(self, connection_registry, mock_websocket):
        """Broadcast actually delivers the message to the websocket."""
        cid = await connection_registry.register_connection(
            websocket=mock_websocket,
            container_name="test-cnt",
        )
        msg = {"type": "test", "data": "hello"}
        count = await connection_registry.broadcast_to_container("test-cnt", msg)
        assert count == 1
        # Verify send_json was called with the actual message
        mock_websocket.send_json.assert_called_with(msg)
        await connection_registry.unregister_connection(cid)

    async def test_broadcast_to_all(self, connection_registry, mock_websocket):
        """broadcast_to_all returns correct counts keyed by container name."""
        mock_ws2 = AsyncMock()
        mock_ws2.send_json = AsyncMock()
        mock_ws2.client_state = __import__(
            "fastapi.websockets", fromlist=["WebSocketState"]
        ).WebSocketState.CONNECTED

        cid1 = await connection_registry.register_connection(
            websocket=mock_websocket,
            container_name="cnt-a",
        )
        cid2 = await connection_registry.register_connection(
            websocket=mock_ws2,
            container_name="cnt-b",
        )

        msg = {"type": "test"}
        results = await connection_registry.broadcast_to_all(msg)
        # Verify exact container names and counts
        assert set(results.keys()) == {"cnt-a", "cnt-b"}
        assert results["cnt-a"] == 1
        assert results["cnt-b"] == 1

        await connection_registry.unregister_connection(cid1)
        await connection_registry.unregister_connection(cid2)


# ---------------------------------------------------------------------------
# websocket_health_monitor - serializer edge cases
# ---------------------------------------------------------------------------


class TestHealthMonitorSerializers:
    def test_ping_status_serializer_none(self):
        from services.websocket_health_monitor import PingStatus

        ps = PingStatus(sent_at=datetime.now(UTC))
        assert ps.serialize_datetime(None) is None

    def test_ping_status_serializer_with_datetime(self):
        """PingStatus serializes actual datetimes to valid ISO strings."""
        from services.websocket_health_monitor import PingStatus

        now = datetime(2025, 6, 15, 10, 30, 0, tzinfo=UTC)
        ps = PingStatus(sent_at=now, received_at=now)
        dumped = ps.model_dump()
        parsed_sent = datetime.fromisoformat(dumped["sent_at"])
        parsed_received = datetime.fromisoformat(dumped["received_at"])
        assert parsed_sent == now
        assert parsed_received == now

    def test_connection_health_status_serializer_none(self):
        from services.websocket_health_monitor import ConnectionHealth, ConnectionHealthStatus

        status = ConnectionHealthStatus(
            connection_id="c1",
            container_name="cnt",
            health=ConnectionHealth.HEALTHY,
            last_activity=datetime.now(UTC),
            connection_age_seconds=10.0,
        )
        assert status.serialize_datetime(None) is None

    def test_connection_health_status_serializer_with_datetime(self):
        """ConnectionHealthStatus serializes datetimes correctly."""
        from services.websocket_health_monitor import ConnectionHealth, ConnectionHealthStatus

        now = datetime(2025, 3, 1, 12, 0, 0, tzinfo=UTC)
        status = ConnectionHealthStatus(
            connection_id="c1",
            container_name="cnt",
            health=ConnectionHealth.HEALTHY,
            last_activity=now,
            last_ping_sent=now,
            last_pong_received=now,
            connection_age_seconds=10.0,
        )
        dumped = status.model_dump()
        assert datetime.fromisoformat(dumped["last_activity"]) == now
        assert datetime.fromisoformat(dumped["last_ping_sent"]) == now
        assert datetime.fromisoformat(dumped["last_pong_received"]) == now

    def test_container_health_stats_serializer_none(self):
        from services.websocket_health_monitor import ContainerHealthStats

        stats = ContainerHealthStats(
            container_name="cnt",
            total_connections=0,
        )
        assert stats.serialize_datetime(None) is None

    def test_container_health_stats_serializer_with_datetime(self):
        """ContainerHealthStats serializes last_cleanup to valid ISO format."""
        from services.websocket_health_monitor import ContainerHealthStats

        cleanup_time = datetime(2025, 7, 10, 8, 0, 0, tzinfo=UTC)
        stats = ContainerHealthStats(
            container_name="cnt",
            total_connections=5,
            last_cleanup=cleanup_time,
        )
        dumped = stats.model_dump()
        parsed = datetime.fromisoformat(dumped["last_cleanup"])
        assert parsed == cleanup_time


# ---------------------------------------------------------------------------
# websocket_metrics - reset, global stats, all container metrics
# ---------------------------------------------------------------------------


class TestWebSocketMetrics:
    async def test_reset_container_metrics_not_found(self, metrics_collector):
        result = await metrics_collector.reset_container_metrics("nonexistent")
        assert result is False

    async def test_reset_container_metrics_found(self, metrics_collector):
        """After reset, getting metrics returns zero/empty values."""
        await metrics_collector.track_connection_opened("cnt", "c1")
        await metrics_collector.track_message_sent("cnt")
        result = await metrics_collector.reset_container_metrics("cnt")
        assert result is True

        # Verify metrics are actually zeroed after reset
        metrics = await metrics_collector.get_container_metrics("cnt")
        assert metrics.connections.active_connections == 0
        assert metrics.connections.total_connections_created == 0
        assert metrics.messages.total_messages_sent == 0

    async def test_get_global_stats(self, metrics_collector):
        """Global stats have exact values matching what we tracked."""
        await metrics_collector.track_connection_opened("cnt-a", "c1")
        await metrics_collector.track_message_sent("cnt-a")
        await metrics_collector.track_message_received("cnt-a")
        stats = await metrics_collector.get_global_stats()
        assert stats["total_containers_tracked"] == 1
        assert stats["total_active_connections"] == 1
        assert stats["total_messages_processed"] == 2
        assert stats["total_analyses_executed"] == 0
        assert stats["total_errors"] == 0

    async def test_get_all_container_metrics(self, metrics_collector):
        """All container metrics returns exact container names."""
        await metrics_collector.track_connection_opened("cnt1", "c1")
        await metrics_collector.track_connection_opened("cnt2", "c2")
        all_metrics = await metrics_collector.get_all_container_metrics()
        assert len(all_metrics) == 2
        container_names = {m.container_name for m in all_metrics}
        assert container_names == {"cnt1", "cnt2"}

    async def test_reset_singleton(self):
        """Singleton returns same object; reset breaks that."""
        from services.websocket_metrics import (
            WebSocketMetricsCollectorSingleton,
            get_websocket_metrics_collector,
            reset_websocket_metrics_collector,
        )

        inst1 = get_websocket_metrics_collector()
        inst2 = get_websocket_metrics_collector()
        # Verify second call returns same object
        assert inst1 is inst2

        reset_websocket_metrics_collector()
        assert WebSocketMetricsCollectorSingleton._instance is None

        # After reset, a new instance is created
        inst3 = get_websocket_metrics_collector()
        assert inst3 is not inst1

        # Clean up
        reset_websocket_metrics_collector()

    async def test_track_analysis_execution(self, metrics_collector):
        """Analysis metrics track times and success/failure counts."""
        await metrics_collector.track_analysis_execution("cnt", 150.0, success=True)
        await metrics_collector.track_analysis_execution("cnt", 200.0, success=False)

        metrics = await metrics_collector.get_container_metrics("cnt")
        assert metrics.analysis.total_analyses == 2
        assert metrics.analysis.successful_analyses == 1
        assert metrics.analysis.failed_analyses == 1
        assert metrics.analysis.min_execution_time_ms == 150.0
        assert metrics.analysis.max_execution_time_ms == 200.0
        assert metrics.analysis.average_execution_time_ms == 175.0

    async def test_track_error(self, metrics_collector):
        """Error metrics track counts by type."""
        await metrics_collector.track_error("cnt", "timeout", "request timed out")
        await metrics_collector.track_error("cnt", "timeout", "another timeout")
        await metrics_collector.track_error("cnt", "parse_error", "bad json")

        metrics = await metrics_collector.get_container_metrics("cnt")
        assert metrics.errors.total_errors == 3
        assert metrics.errors.errors_by_type["timeout"] == 2
        assert metrics.errors.errors_by_type["parse_error"] == 1
        assert metrics.errors.last_error_message == "bad json"


# ---------------------------------------------------------------------------
# path_resolution - validate_path_in_container_sync ValueError
# ---------------------------------------------------------------------------


class TestPathResolution:
    def test_validate_path_in_container_sync_value_error(self):
        """Test ValueError branch (e.g., different drives on Windows)."""
        from utils.path_resolution import validate_path_in_container_sync

        # commonpath raises ValueError for paths on different drives or empty strings
        # When ValueError is raised, the function should return False (path is NOT safe)
        result = validate_path_in_container_sync("", "")
        assert result is False

    def test_validate_path_in_container_sync_valid_subpath(self):
        """A path inside the container root should return True."""
        from utils.path_resolution import validate_path_in_container_sync

        assert (
            validate_path_in_container_sync("/workspace/app", "/workspace/app/src/main.py") is True
        )

    def test_validate_path_in_container_sync_traversal_attack(self):
        """A path escaping the container root should return False."""
        from utils.path_resolution import validate_path_in_container_sync

        assert (
            validate_path_in_container_sync("/workspace/app", "/workspace/app/../../etc/passwd")
            is False
        )

    def test_validate_path_in_container_sync_exact_root(self):
        """The container root itself should be valid."""
        from utils.path_resolution import validate_path_in_container_sync

        assert validate_path_in_container_sync("/workspace/app", "/workspace/app") is True

    def test_validate_path_in_container_sync_sibling_directory(self):
        """A sibling directory should NOT be valid."""
        from utils.path_resolution import validate_path_in_container_sync

        assert validate_path_in_container_sync("/workspace/app", "/workspace/other") is False


# ---------------------------------------------------------------------------
# main.py - exception handler edge cases
# ---------------------------------------------------------------------------


class TestMainExceptionHandlers:
    def test_coding_client_exception_handlers_registered(self):
        """Verify register_exception_handlers from coding-client wired up handlers on the app."""
        from fastapi.exceptions import RequestValidationError
        from starlette.exceptions import HTTPException as StarletteHTTPException

        from main import app

        handlers = app.exception_handlers
        # coding-client 1.3.1+ registers the HTTP handler against starlette's
        # base class so it catches both starlette- and fastapi-raised exceptions.
        assert StarletteHTTPException in handlers
        assert RequestValidationError in handlers
        assert Exception in handlers


# ---------------------------------------------------------------------------
# websocket_broadcast - serializer edge cases
# ---------------------------------------------------------------------------


class TestBroadcastSerializers:
    def test_broadcast_message_serializer_none(self):
        from services.websocket_broadcast import BroadcastEventType, BroadcastMessage

        msg = BroadcastMessage(
            message_id="m1",
            event_type=BroadcastEventType.CUSTOM,
            container_name="cnt",
            payload={},
        )
        assert msg.serialize_datetime(None) is None

    def test_broadcast_message_serializer_with_datetime(self):
        """BroadcastMessage timestamp is serialized to valid ISO format."""
        from services.websocket_broadcast import BroadcastEventType, BroadcastMessage

        ts = datetime(2025, 8, 1, 15, 45, 0, tzinfo=UTC)
        msg = BroadcastMessage(
            message_id="m1",
            event_type=BroadcastEventType.CUSTOM,
            container_name="cnt",
            payload={"key": "val"},
            timestamp=ts,
        )
        dumped = msg.model_dump()
        parsed = datetime.fromisoformat(dumped["timestamp"])
        assert parsed == ts

    def test_broadcast_subscription_serializer_none(self):
        from services.websocket_broadcast import BroadcastSubscription

        sub = BroadcastSubscription(
            subscription_id="s1",
            connection_id="c1",
            container_name="cnt",
        )
        assert sub.serialize_datetime(None) is None

    def test_broadcast_subscription_serializer_with_datetime(self):
        """BroadcastSubscription created_at is serialized to valid ISO format."""
        from services.websocket_broadcast import BroadcastSubscription

        ts = datetime(2025, 4, 10, 9, 0, 0, tzinfo=UTC)
        sub = BroadcastSubscription(
            subscription_id="s1",
            connection_id="c1",
            container_name="cnt",
            created_at=ts,
            last_message_at=ts,
        )
        dumped = sub.model_dump()
        parsed_created = datetime.fromisoformat(dumped["created_at"])
        parsed_last = datetime.fromisoformat(dumped["last_message_at"])
        assert parsed_created == ts
        assert parsed_last == ts
