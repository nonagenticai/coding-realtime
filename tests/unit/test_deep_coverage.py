"""Deep coverage tests targeting remaining uncovered lines.

Covers:
- routers/websocket_health.py: container connections endpoint (lines 355-416, 498)
- routers/websocket_rate_limits.py: error branches (lines 266-272, 335-341, 386-392)
- routers/realtime.py: container metrics, global metrics, watch/unwatch errors,
  processor missing parse_file_with_metrics (lines 956-957, 1066-1068, 1144-1146,
  1637-1706, 1785-1817)
- routers/health.py: detailed check exception paths (lines 67-68, 76-77, 84-85)
- services/metrics_service.py: async schema init (lines 83-84, 90-121)
- services/websocket_health_monitor.py: ping loop, send_ping, cleanup (lines 499,
  504-505, 518, 565-566, 589, 621-622, 704-706, 710-711)
- services/websocket_broadcast.py: unsubscribe fix verification, delivery failure
- services/websocket_connection_registry.py: lazy imports (lines 41-45)
- Bug fixes: send_message container_name race, broadcast unsubscribe dead expression
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.websockets import WebSocketState

from routers.realtime import (
    ConnectionManager,
    handle_analyze_file,
    handle_realtime_message,
    handle_unwatch_file,
    handle_watch_file,
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _ws(query_params=None, headers=None, state=WebSocketState.CONNECTED):
    ws = AsyncMock()
    ws.query_params = query_params or {}
    ws.headers = headers or {}
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.send_json = AsyncMock()
    ws.send_text = AsyncMock()
    ws.receive_text = AsyncMock()
    ws.client_state = state
    ws.client = MagicMock()
    ws.client.host = "127.0.0.1"
    return ws


# ===========================================================================
# routers/websocket_health.py — container connections with health_stats=None
# ===========================================================================


class TestWebSocketHealthConnections:
    """Cover lines 355-416 (get_container_connections with fallback health)."""

    @pytest.fixture
    def tenant_override(self):
        from coding_client.auth import require_tenant

        from main import app

        app.dependency_overrides[require_tenant] = lambda: "test-tenant"
        yield
        app.dependency_overrides.pop(require_tenant, None)

    async def test_container_connections_with_health_data(self, tenant_override):
        """Exercise the full get_container_connections path with health data."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_conn = MagicMock()
        mock_conn.connection_id = "c1_abc"
        mock_conn.container_name = "c1"
        mock_conn.connected_at = datetime(2025, 1, 1, tzinfo=UTC)
        mock_conn.last_activity = datetime(2025, 1, 1, tzinfo=UTC)

        mock_health = MagicMock()
        mock_health.health = "healthy"
        mock_health.average_latency_ms = 10.0
        mock_health.is_responsive = True

        mock_container_health = MagicMock()
        mock_container_health.container_name = "c1"
        mock_container_health.total_connections = 1
        mock_container_health.healthy_connections = 1
        mock_container_health.degraded_connections = 0
        mock_container_health.stale_connections = 0
        mock_container_health.average_latency_ms = 10.0
        mock_container_health.total_failed_pings = 0
        mock_container_health.last_cleanup = None

        mock_registry = AsyncMock()
        mock_registry.get_container_connections = AsyncMock(return_value=[mock_conn])

        mock_monitor = AsyncMock()
        mock_monitor.get_connection_health = AsyncMock(return_value=mock_health)
        mock_monitor.get_container_health = AsyncMock(return_value=mock_container_health)

        with (
            patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
            patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
            patch(
                "routers.websocket_health.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ws-health/container/c1/connections")

        assert resp.status_code == 200
        data = resp.json()
        assert data["total_connections"] == 1
        assert data["connections"][0]["connection_id"] == "c1_abc"

    async def test_container_connections_no_health_stats(self, tenant_override):
        """Exercise path where health_stats is None (lines 390-401)."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_conn = MagicMock()
        mock_conn.connection_id = "c1_abc"
        mock_conn.container_name = "c1"
        mock_conn.connected_at = datetime(2025, 1, 1, tzinfo=UTC)
        mock_conn.last_activity = datetime(2025, 1, 1, tzinfo=UTC)

        mock_health = MagicMock()
        mock_health.health = "healthy"
        mock_health.average_latency_ms = 10.0
        mock_health.is_responsive = True

        mock_registry = AsyncMock()
        mock_registry.get_container_connections = AsyncMock(return_value=[mock_conn])

        mock_monitor = AsyncMock()
        mock_monitor.get_connection_health = AsyncMock(return_value=mock_health)
        # Return None for container health to exercise the fallback path
        mock_monitor.get_container_health = AsyncMock(return_value=None)

        with (
            patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
            patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
            patch(
                "routers.websocket_health.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ws-health/container/c1/connections")

        assert resp.status_code == 200
        data = resp.json()
        # Verify fallback health summary has defaults
        assert data["health_summary"]["healthy_connections"] == 0
        assert data["health_summary"]["average_latency_ms"] is None

    async def test_container_connections_no_connections(self, tenant_override):
        """Exercise path where no connections exist (404)."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_registry = AsyncMock()
        mock_registry.get_container_connections = AsyncMock(return_value=[])

        with (
            patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
            patch(
                "routers.websocket_health.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ws-health/container/c1/connections")

        assert resp.status_code == 404

    async def test_container_connections_no_per_conn_health(self, tenant_override):
        """Exercise path where per-connection health is None (DISCONNECTED fallback)."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_conn = MagicMock()
        mock_conn.connection_id = "c1_abc"
        mock_conn.container_name = "c1"
        mock_conn.connected_at = datetime(2025, 1, 1, tzinfo=UTC)
        mock_conn.last_activity = datetime(2025, 1, 1, tzinfo=UTC)

        mock_registry = AsyncMock()
        mock_registry.get_container_connections = AsyncMock(return_value=[mock_conn])

        mock_monitor = AsyncMock()
        # Return None for per-connection health
        mock_monitor.get_connection_health = AsyncMock(return_value=None)
        mock_monitor.get_container_health = AsyncMock(return_value=None)

        with (
            patch("routers.websocket_health.get_websocket_registry", return_value=mock_registry),
            patch("routers.websocket_health.get_health_monitor", return_value=mock_monitor),
            patch(
                "routers.websocket_health.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ws-health/container/c1/connections")

        assert resp.status_code == 200
        data = resp.json()
        assert data["connections"][0]["health"] == "disconnected"
        assert data["connections"][0]["is_responsive"] is False


# ===========================================================================
# routers/websocket_rate_limits.py — error branches
# ===========================================================================


class TestRateLimitsErrorBranches:
    @pytest.fixture
    def tenant_override(self):
        from coding_client.auth import require_tenant

        from main import app

        app.dependency_overrides[require_tenant] = lambda: "test-tenant"
        yield
        app.dependency_overrides.pop(require_tenant, None)

    async def test_get_all_stats_error(self, tenant_override):
        """Exercise lines 266-272: get_all_stats exception handler."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_limiter = AsyncMock()
        mock_limiter.get_all_stats = AsyncMock(side_effect=RuntimeError("all stats fail"))

        with patch(
            "routers.websocket_rate_limits.get_websocket_rate_limiter",
            return_value=mock_limiter,
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/stats")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["total_containers"] == 0

    async def test_get_violations_error(self, tenant_override):
        """Exercise lines 335-341: get_violations exception handler."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_limiter = AsyncMock()
        mock_limiter.get_violations = AsyncMock(side_effect=RuntimeError("violations fail"))

        with patch(
            "routers.websocket_rate_limits.get_websocket_rate_limiter",
            return_value=mock_limiter,
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/violations")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["total_violations"] == 0

    async def test_get_global_stats_error(self, tenant_override):
        """Exercise lines 386-392: get_global_stats exception handler."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_limiter = AsyncMock()
        mock_limiter.get_global_stats = AsyncMock(side_effect=RuntimeError("global fail"))

        with patch(
            "routers.websocket_rate_limits.get_websocket_rate_limiter",
            return_value=mock_limiter,
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/global-stats")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["global_stats"] == {}


# ===========================================================================
# routers/realtime.py — container metrics and global metrics endpoints
# ===========================================================================


class TestRealtimeMetricsEndpoints:
    @pytest.fixture
    def tenant_override(self):
        from coding_client.auth import require_tenant

        from main import app

        app.dependency_overrides[require_tenant] = lambda: "test-tenant"
        yield
        app.dependency_overrides.pop(require_tenant, None)

    async def test_get_container_websocket_metrics_success(self, tenant_override):
        """Exercise lines 1637-1697: successful container metrics retrieval."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_metrics = MagicMock()
        mock_metrics.container_name = "c1"
        mock_metrics.timestamp = datetime(2025, 1, 1, tzinfo=UTC)
        mock_metrics.uptime_seconds = 3600.0
        mock_metrics.connections = MagicMock(
            active_connections=2,
            total_connections_created=5,
            total_connections_closed=3,
            average_connection_duration_seconds=600.0,
            oldest_connection_age_seconds=1200.0,
        )
        mock_metrics.messages = MagicMock(
            total_messages_sent=100,
            total_messages_received=80,
            messages_per_second=1.5,
            last_message_timestamp=None,
        )
        mock_metrics.analysis = MagicMock(
            total_analyses=50,
            successful_analyses=48,
            failed_analyses=2,
            average_execution_time_ms=15.0,
            min_execution_time_ms=5.0,
            max_execution_time_ms=50.0,
            total_execution_time_ms=750.0,
            success_rate=96.0,
        )
        mock_metrics.errors = MagicMock(
            total_errors=2,
            error_rate=0.04,
            errors_by_type={"parse": 2},
            last_error_timestamp=None,
            last_error_message=None,
        )

        mock_collector = AsyncMock()
        mock_collector.get_container_metrics = AsyncMock(return_value=mock_metrics)

        with (
            patch(
                "routers.realtime.metrics.get_websocket_metrics_collector",
                return_value=mock_collector,
            ),
            patch(
                "routers.realtime.metrics.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/realtime/metrics/c1")

        assert resp.status_code == 200
        data = resp.json()
        assert data["container_name"] == "c1"
        assert "metrics" in data
        assert data["metrics"]["connections"]["active_connections"] == 2

    async def test_get_container_websocket_metrics_error(self, tenant_override):
        """Exercise lines 1699-1706: container metrics exception handler."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_collector = AsyncMock()
        mock_collector.get_container_metrics = AsyncMock(
            side_effect=RuntimeError("metrics fail"),
        )

        with (
            patch(
                "routers.realtime.metrics.get_websocket_metrics_collector",
                return_value=mock_collector,
            ),
            patch(
                "routers.realtime.metrics.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/realtime/metrics/c1")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["metrics"] == {}

    async def test_get_container_metrics_validation_fails(self, tenant_override):
        """Exercise the container validation failure path (lines 1641-1643)."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_metrics = MagicMock()
        mock_metrics.container_name = "c1"
        mock_metrics.timestamp = datetime(2025, 1, 1, tzinfo=UTC)
        mock_metrics.uptime_seconds = 100.0
        mock_metrics.connections = MagicMock(
            active_connections=0,
            total_connections_created=0,
            total_connections_closed=0,
            average_connection_duration_seconds=0.0,
            oldest_connection_age_seconds=0.0,
        )
        mock_metrics.messages = MagicMock(
            total_messages_sent=0,
            total_messages_received=0,
            messages_per_second=0.0,
            last_message_timestamp=None,
        )
        mock_metrics.analysis = MagicMock(
            total_analyses=0,
            successful_analyses=0,
            failed_analyses=0,
            average_execution_time_ms=0.0,
            min_execution_time_ms=0.0,
            max_execution_time_ms=0.0,
            total_execution_time_ms=0.0,
            success_rate=0.0,
        )
        mock_metrics.errors = MagicMock(
            total_errors=0,
            error_rate=0.0,
            errors_by_type={},
            last_error_timestamp=None,
            last_error_message=None,
        )

        mock_collector = AsyncMock()
        mock_collector.get_container_metrics = AsyncMock(return_value=mock_metrics)

        with (
            patch(
                "routers.realtime.metrics.get_websocket_metrics_collector",
                return_value=mock_collector,
            ),
            patch(
                "routers.realtime.metrics.validate_and_get_container_for_tenant",
                new_callable=AsyncMock,
                side_effect=HTTPException(status_code=404, detail="not found"),
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/realtime/metrics/c1")

        # Ownership is enforced: a failed validation is a 404, never served anyway
        # (this used to pin the leak — "validation failure is logged but proceeds").
        assert resp.status_code == 404
        mock_collector.get_container_metrics.assert_not_awaited()

    async def test_get_global_websocket_metrics_success(self, tenant_override):
        """Exercise lines 1785-1813: successful global metrics retrieval."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        cm = MagicMock()
        cm.container_name = "c1"
        cm.connections = MagicMock(active_connections=2)
        cm.messages = MagicMock(messages_per_second=1.0)
        cm.analysis = MagicMock(total_analyses=10, success_rate=95.0)
        cm.errors = MagicMock(total_errors=1, error_rate=0.01)

        cm.messages = MagicMock(
            messages_per_second=1.0, total_messages_sent=3, total_messages_received=4
        )
        foreign = MagicMock()
        foreign.container_name = "other-tenant-box"

        mock_collector = AsyncMock()
        mock_collector.get_all_container_metrics = AsyncMock(return_value=[cm, foreign])

        with (
            patch(
                "routers.realtime.metrics.get_websocket_metrics_collector",
                return_value=mock_collector,
            ),
            patch(
                "routers.realtime.metrics.containers_owned_by_tenant",
                AsyncMock(return_value={"c1"}),
            ),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/realtime/metrics")

        assert resp.status_code == 200
        data = resp.json()
        assert data["global_stats"]["total_containers_tracked"] == 1
        assert data["global_stats"]["total_active_connections"] == 2
        assert data["global_stats"]["total_messages_processed"] == 7
        assert [m["container_name"] for m in data["container_metrics"]] == ["c1"]

    async def test_get_global_websocket_metrics_error(self, tenant_override):
        """Exercise lines 1815-1817: global metrics exception handler."""
        from httpx import ASGITransport, AsyncClient

        from main import app

        mock_collector = AsyncMock()
        mock_collector.get_all_container_metrics = AsyncMock(
            side_effect=RuntimeError("global fail"),
        )

        with patch(
            "routers.realtime.metrics.get_websocket_metrics_collector",
            return_value=mock_collector,
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/realtime/metrics")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["container_metrics"] == []


# ===========================================================================
# routers/realtime.py — analyze_file: processor without parse_file_with_metrics
# ===========================================================================


class TestAnalyzeFileEdgeCases:
    async def test_processor_without_parse_file_with_metrics(self):
        """A processor lacking parse_file_with_metrics reports an error.

        This used to return silently, leaving the client unable to tell a
        failed analysis from a lost request.
        """
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"
        cm.content_hashes = {}

        # Create processor WITHOUT parse_file_with_metrics attribute
        processor = MagicMock(spec=[])
        factory = AsyncMock()
        factory.get_processor_for_file.return_value = processor

        with (
            patch("routers.realtime.analyze.connection_manager", cm),
            patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
        ):
            await handle_analyze_file(
                "conn-1",
                {"file_path": "/workspace/file.py", "content": "x = 1"},
                None,
                None,
            )

        # An explicit error is sent — never a silent no-op.
        ws.send_text.assert_awaited()
        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "analysis_error"
        assert sent["success"] is False
        assert "does not support metrics" in sent["error"]

    async def test_analyze_file_with_workspace_relative_path(self):
        """Cover line 928: workspace_path + relative file_path joining."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        # Don't set connection_containers["conn-1"] — .get() returns None naturally
        cm.content_hashes = {}

        processor = AsyncMock()
        entity = SimpleNamespace(
            id="e1", name="f", type="function", line_number=1, language="python"
        )
        parse_result = SimpleNamespace(
            success=True,
            language="python",
            file_path="/workspace/src/main.py",
            entities=[entity],
            relationships=[],
            complexity_score=1.0,
            lines_of_code=5,
            errors=[],
        )
        processor.parse_file_with_metrics = AsyncMock(return_value=parse_result)
        factory = AsyncMock()
        factory.get_processor_for_file.return_value = processor

        cache = AsyncMock()
        cache.get_stats = AsyncMock(return_value={"hit_rate": 0})
        monitor = AsyncMock()

        with (
            patch("routers.realtime.analyze.connection_manager", cm),
            patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
            patch("routers.realtime.analyze.get_analysis_cache", return_value=cache),
            patch("routers.realtime.analyze.get_performance_monitor", return_value=monitor),
        ):
            await handle_analyze_file(
                "conn-1",
                {"file_path": "src/main.py", "content": "x = 1"},
                Path("/workspace"),
                None,  # no container
            )

        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "analysis_result"

    async def test_analyze_file_container_error_tracking(self):
        """Cover lines 984-986: error metric tracking when container is set."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"
        cm.content_hashes = {}

        processor = AsyncMock()
        processor.parse_file_with_metrics = AsyncMock(side_effect=ValueError("parse fail"))
        factory = AsyncMock()
        factory.get_processor_for_file.return_value = processor

        metrics = AsyncMock()

        with (
            patch("routers.realtime.analyze.connection_manager", cm),
            patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
            patch("routers.realtime.analyze.get_websocket_metrics_collector", return_value=metrics),
            patch(
                "routers.realtime._path_resolution.resolve_container_path",
                return_value="/workspace/file.py",
            ),
        ):
            await handle_analyze_file(
                "conn-1",
                {"file_path": "/workspace/file.py", "content": "x"},
                None,
                "c1",
            )

        # Verify error was tracked in metrics
        metrics.track_error.assert_awaited_once()
        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "analysis_error"
        assert "parse fail" in sent["error"]


# ===========================================================================
# routers/realtime.py — watch/unwatch file exception paths
# ===========================================================================


class TestWatchUnwatchExceptions:
    async def test_watch_file_value_error(self):
        """Cover lines 1066-1068: ValueError in watch_file."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"
        cm.file_watchers = {}

        # Patch watch_file to raise ValueError
        with (
            patch("routers.realtime.watch.connection_manager", cm),
            patch.object(cm, "watch_file", side_effect=ValueError("bad watch")),
        ):
            await handle_watch_file("conn-1", {"file_path": "/a.py"}, None, None)

        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "error"
        assert "bad watch" in sent["error"]

    async def test_unwatch_file_value_error(self):
        """Cover lines 1144-1146: ValueError in unwatch_file."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = {"/a.py"}
        cm.connection_containers["conn-1"] = "c1"
        cm.file_watchers = {"/a.py": {"conn-1"}}

        with (
            patch("routers.realtime.watch.connection_manager", cm),
            patch.object(cm, "unwatch_file", side_effect=ValueError("bad unwatch")),
        ):
            await handle_unwatch_file("conn-1", {"file_path": "/a.py"}, None, None)

        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "error"
        assert "bad unwatch" in sent["error"]

    async def test_unwatch_file_workspace_relative_path(self):
        """Cover line 1130: workspace_path + relative path in unwatch."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = {"/workspace/src/a.py"}
        # Don't set connection_containers["conn-1"] — .get() returns None naturally
        cm.file_watchers = {"/workspace/src/a.py": {"conn-1"}}

        with patch("routers.realtime.watch.connection_manager", cm):
            await handle_unwatch_file("conn-1", {"file_path": "src/a.py"}, Path("/workspace"), None)

        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "watch_stopped"
        assert "/workspace/src/a.py" in sent["file_path"]

    async def test_unwatch_file_container_path_resolution_fails(self):
        """Cover unwatch with container path resolution failure."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"

        with (
            patch("routers.realtime.watch.connection_manager", cm),
            patch(
                "routers.realtime._path_resolution.resolve_container_path",
                side_effect=RuntimeError("bad path"),
            ),
        ):
            await handle_unwatch_file("conn-1", {"file_path": "bad.py"}, None, "c1")

        sent = json.loads(ws.send_text.await_args.args[0])
        assert "Invalid file path" in sent["error"]


# ===========================================================================
# routers/health.py — exception paths in /detailed
# ===========================================================================


class TestHealthDetailedExceptions:
    async def test_detailed_redis_ping_exception(self):
        """Cover lines 67-68: Redis ping exception in detailed check."""
        import main
        from routers.health import detailed

        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(side_effect=ConnectionError("redis down"))

        mock_analysis = AsyncMock()
        mock_analysis.health_check = AsyncMock(return_value=True)
        mock_containers = AsyncMock()
        mock_containers.health_check = AsyncMock(return_value=True)

        original = main._redis
        main._redis = mock_redis
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

        assert result["checks"]["redis"] == "disconnected"

    async def test_detailed_analysis_client_exception(self):
        """Cover lines 76-77: coding-analysis health_check exception."""
        import main
        from routers.health import detailed

        original = main._redis
        main._redis = None
        try:
            with (
                patch(
                    "clients.coding_analysis.get_coding_analysis_client",
                    side_effect=RuntimeError("no analysis"),
                ),
                patch(
                    "clients.coding_containers.get_coding_containers_client",
                    return_value=AsyncMock(health_check=AsyncMock(return_value=True)),
                ),
            ):
                result = await detailed(tenant_id="test-tenant")
        finally:
            main._redis = original

        assert result["checks"]["coding_analysis"] is False

    async def test_detailed_containers_client_exception(self):
        """Cover lines 84-85: coding-containers health_check exception."""
        import main
        from routers.health import detailed

        original = main._redis
        main._redis = None
        try:
            with (
                patch(
                    "clients.coding_analysis.get_coding_analysis_client",
                    return_value=AsyncMock(health_check=AsyncMock(return_value=True)),
                ),
                patch(
                    "clients.coding_containers.get_coding_containers_client",
                    side_effect=RuntimeError("no containers"),
                ),
            ):
                result = await detailed(tenant_id="test-tenant")
        finally:
            main._redis = original

        assert result["checks"]["coding_containers"] is False


# ===========================================================================
# services/websocket_health_monitor.py — ping loop, send_ping edge cases
# ===========================================================================


class TestHealthMonitorPingEdgeCases:
    async def test_send_ping_disconnected_websocket(self, health_monitor, mock_websocket):
        """Cover lines 521-526: WebSocket not connected during ping."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)
        # Set websocket to disconnected state
        mock_websocket.client_state = WebSocketState.DISCONNECTED

        await health_monitor._send_ping("conn-1", mock_websocket)

        # Connection should be marked as disconnected
        status = await health_monitor.get_connection_health("conn-1")
        assert status.health.value == "disconnected"
        assert status.is_responsive is False

        await health_monitor.unregister_connection("conn-1")

    async def test_send_ping_timeout(self, health_monitor, mock_websocket):
        """Cover lines 556-575: Ping timeout handling."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)
        mock_websocket.send_json = AsyncMock(side_effect=TimeoutError("timed out"))

        await health_monitor._send_ping("conn-1", mock_websocket)

        status = await health_monitor.get_connection_health("conn-1")
        assert status.failed_pings == 1
        assert status.health.value == "degraded"

        await health_monitor.unregister_connection("conn-1")

    async def test_send_ping_multiple_timeouts_marks_stale(self, health_monitor, mock_websocket):
        """Multiple ping timeouts mark connection as stale."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)
        mock_websocket.send_json = AsyncMock(side_effect=TimeoutError("timed out"))

        # Send 3 pings (max_failed_pings default is 3)
        for _ in range(3):
            await health_monitor._send_ping("conn-1", mock_websocket)

        status = await health_monitor.get_connection_health("conn-1")
        assert status.failed_pings == 3
        assert status.health.value == "stale"
        assert status.is_responsive is False

        await health_monitor.unregister_connection("conn-1")

    async def test_send_ping_generic_exception(self, health_monitor, mock_websocket):
        """Cover lines 577-582: Generic exception during ping."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)
        mock_websocket.send_json = AsyncMock(side_effect=RuntimeError("unexpected"))

        await health_monitor._send_ping("conn-1", mock_websocket)

        status = await health_monitor.get_connection_health("conn-1")
        assert status.failed_pings == 1
        assert status.health.value == "degraded"

        await health_monitor.unregister_connection("conn-1")

    async def test_send_ping_connection_not_found(self, health_monitor, mock_websocket):
        """Cover line 518: send_ping for non-existent connection."""
        await health_monitor._send_ping("nonexistent", mock_websocket)
        # Should silently return without creating any health entry
        status = await health_monitor.get_connection_health("nonexistent")
        assert status is None

    async def test_handle_pong_recovers_degraded_connection(self, health_monitor, mock_websocket):
        """Cover lines 617-626: Pong recovers degraded connection to healthy."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        # Simulate a timeout to degrade
        mock_websocket.send_json = AsyncMock(side_effect=TimeoutError("timeout"))
        await health_monitor._send_ping("conn-1", mock_websocket)

        status = await health_monitor.get_connection_health("conn-1")
        assert status.health.value == "degraded"

        # Now receive pong
        result = await health_monitor.handle_pong("conn-1", "p1")
        assert result is True

        status = await health_monitor.get_connection_health("conn-1")
        assert status.health.value == "healthy"
        assert status.failed_pings == 0

        await health_monitor.unregister_connection("conn-1")

    async def test_handle_pong_not_found(self, health_monitor):
        """handle_pong for non-existent connection returns False."""
        result = await health_monitor.handle_pong("nonexistent", "p1")
        assert result is False

    async def test_cleanup_stale_connections_with_container(self, health_monitor, mock_websocket):
        """Cover cleanup_stale_connections with container_name."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        # Make connection stale by setting last_activity far in the past
        async with health_monitor._lock:
            health = health_monitor._connection_health["conn-1"]
            health.failed_pings = 5  # exceeds max_failed_pings

        cleaned = await health_monitor.cleanup_stale_connections(container_name="c1")
        assert cleaned == 1

    async def test_cleanup_stale_connections_global(self, health_monitor, mock_websocket):
        """Cover cleanup_stale_connections without container_name (global)."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        async with health_monitor._lock:
            health = health_monitor._connection_health["conn-1"]
            health.failed_pings = 5

        cleaned = await health_monitor.cleanup_stale_connections(container_name=None)
        assert cleaned == 1


# ===========================================================================
# services/websocket_health_monitor.py — cleanup_task_runner
# ===========================================================================


class TestCleanupTaskRunner:
    async def test_cleanup_task_runner_cancellation(self):
        """Cover lines 704-711: cleanup_task_runner cancel and exception paths."""
        from services.websocket_health_monitor import (
            WebSocketHealthMonitor,
            cleanup_task_runner,
        )

        monitor = WebSocketHealthMonitor(ping_interval_seconds=1)
        task = asyncio.create_task(cleanup_task_runner(monitor, cleanup_interval_seconds=0))

        # Let it run briefly then cancel
        await asyncio.sleep(0.05)
        task.cancel()

        # The cleanup_task_runner catches CancelledError internally and exits cleanly
        await task
        assert task.done()

        # Verify the monitor is still functional after cleanup runner exits
        ws = _ws()
        await monitor.register_connection("post-cleanup", "c1", ws)
        status = await monitor.get_connection_health("post-cleanup")
        assert status is not None
        assert status.health.value == "healthy"
        await monitor.unregister_connection("post-cleanup")

    async def test_broadcast_cleanup_task_runner_cancellation(self):
        """Cover broadcast cleanup_task_runner cancel path."""
        from services.websocket_broadcast import (
            WebSocketBroadcastService,
            cleanup_task_runner,
        )

        service = WebSocketBroadcastService()
        task = asyncio.create_task(cleanup_task_runner(service, cleanup_interval_seconds=0))

        await asyncio.sleep(0.05)
        task.cancel()

        await task
        assert task.done()

        # Verify subscriptions still work after cleanup runner exits
        ws = _ws()
        sub_id = await service.subscribe(
            connection_id="post-cleanup",
            container_name="cnt",
            websocket=ws,
        )
        assert sub_id is not None
        sub = await service.get_subscription(sub_id)
        assert sub is not None
        await service.unsubscribe(sub_id)

    async def test_registry_cleanup_task_runner_cancellation(self):
        """Cover connection registry cleanup_task_runner cancel path."""
        from services.websocket_connection_registry import (
            WebSocketConnectionRegistry,
            cleanup_task_runner,
        )

        registry = WebSocketConnectionRegistry(enable_rate_limiting=False)
        task = asyncio.create_task(cleanup_task_runner(registry, cleanup_interval_seconds=0))

        await asyncio.sleep(0.05)
        task.cancel()

        await task
        assert task.done()

        # Verify connections can still be registered after cleanup runner exits
        ws = _ws()
        conn_id = await registry.register_connection(ws, "cnt", connection_id="post-cleanup")
        assert conn_id == "post-cleanup"
        conn = await registry.get_connection(conn_id)
        assert conn is not None
        await registry.unregister_connection(conn_id)


# ===========================================================================
# services/websocket_broadcast.py — unsubscribe fix verification
# ===========================================================================


class TestBroadcastUnsubscribeFix:
    async def test_unsubscribe_removes_subscription(self, broadcast_service, mock_websocket):
        """Verify the unsubscribe fix (removed dead subs[subscription_id] expression)."""
        sub_id = await broadcast_service.subscribe(
            connection_id="c1",
            container_name="cnt",
            websocket=mock_websocket,
        )

        result = await broadcast_service.unsubscribe(sub_id)
        assert result is True

        # Verify subscription is actually gone
        sub = await broadcast_service.get_subscription(sub_id)
        assert sub is None

    async def test_unsubscribe_not_found(self, broadcast_service, mock_websocket):
        """Unsubscribe for non-existent subscription returns False and doesn't change state."""
        # Create a real subscription to verify count stability
        sub_id = await broadcast_service.subscribe(
            connection_id="c1",
            container_name="cnt",
            websocket=mock_websocket,
        )
        subs_before = await broadcast_service.get_container_subscriptions("cnt")
        count_before = len(subs_before)

        result = await broadcast_service.unsubscribe("nonexistent")
        assert result is False

        # Verify the existing subscription count did not change
        subs_after = await broadcast_service.get_container_subscriptions("cnt")
        assert len(subs_after) == count_before

        # Cleanup
        await broadcast_service.unsubscribe(sub_id)


# ===========================================================================
# ConnectionManager.send_message — bug fix: container_name race condition
# ===========================================================================


class TestSendMessageBugFix:
    async def test_send_message_error_uses_cached_container_name(self):
        """Verify the fix: send_message error handler uses already-fetched container_name."""
        cm = ConnectionManager()
        ws = _ws()
        ws.send_text.side_effect = ConnectionError("broken pipe")
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"

        metrics = AsyncMock()
        with patch(
            "routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics
        ):
            await cm.send_message("conn-1", {"type": "test"})

        # Verify error was tracked with the correct container name
        metrics.track_error.assert_awaited_once()
        call_args = metrics.track_error.await_args
        assert call_args.args[0] == "c1"  # first arg is container_name

        # Connection should be disconnected after error
        assert "conn-1" not in cm.active_connections


# ===========================================================================
# services/websocket_connection_registry.py — lazy import for broadcast service
# ===========================================================================


class TestRegistryLazyImport:
    def test_get_broadcast_service_cached(self):
        """Cover lines 38-45: lazy import of broadcast service."""
        import services.websocket_connection_registry as reg
        from services.websocket_broadcast import WebSocketBroadcastService

        original = reg._broadcast_service
        reg._broadcast_service = None
        try:
            result = reg.get_broadcast_service_cached()
            assert isinstance(result, WebSocketBroadcastService)

            # Verify singleton: calling again returns the same instance
            result2 = reg.get_broadcast_service_cached()
            assert result2 is result
        finally:
            reg._broadcast_service = original

    def test_get_rate_limiter_cached(self):
        """Cover lines 28-35: lazy import of rate limiter."""
        import services.websocket_connection_registry as reg
        from services.websocket_rate_limiter import WebSocketRateLimiter

        original = reg._rate_limiter
        reg._rate_limiter = None
        try:
            result = reg.get_rate_limiter()
            assert isinstance(result, WebSocketRateLimiter)

            # Verify singleton: calling again returns the same instance
            result2 = reg.get_rate_limiter()
            assert result2 is result
        finally:
            reg._rate_limiter = original


# ===========================================================================
# handle_realtime_message — workspace_path validation error
# ===========================================================================


class TestRealtimeMessageEdgeCases:
    async def test_handle_realtime_message_invalid_workspace_continues(self):
        """Cover lines 779-787: invalid workspace_path doesn't prevent handling."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"

        cache = AsyncMock()
        cache.get_stats = AsyncMock(return_value={"hit_rate": 0})

        with (
            patch("routers.realtime.status.connection_manager", cm),
            patch("routers.realtime.status.get_analysis_cache", return_value=cache),
            patch(
                "routers.realtime.handlers.validate_project_root",
                side_effect=ValueError("bad path"),
            ),
        ):
            await handle_realtime_message(
                "conn-1",
                {"type": "get_status"},
                "/invalid/\x00/path",
                "c1",
            )

        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "status_update"

    async def test_handle_realtime_message_analyze_file_type(self):
        """Cover the analyze_file routing in handle_realtime_message."""
        cm = ConnectionManager()
        ws = _ws()
        cm.active_connections["conn-1"] = ws
        cm.connection_files["conn-1"] = set()
        cm.connection_containers["conn-1"] = "c1"

        with (
            patch("routers.realtime.analyze.connection_manager", cm),
            patch(
                "routers.realtime.handlers.handle_analyze_file", new_callable=AsyncMock
            ) as mock_af,
        ):
            await handle_realtime_message(
                "conn-1",
                {"type": "analyze_file", "file_path": "/a.py", "content": "x"},
                None,
                "c1",
            )

        mock_af.assert_awaited_once()


# ===========================================================================
# services/websocket_broadcast.py — broadcast delivery failure
# ===========================================================================


class TestBroadcastDeliveryFailure:
    async def test_broadcast_with_failed_delivery(self, broadcast_service):
        """Cover exception path in broadcast delivery (line 409-414)."""
        from services.websocket_broadcast import BroadcastEventType

        ws = _ws()
        ws.send_json = AsyncMock(side_effect=RuntimeError("send failed"))

        await broadcast_service.subscribe(
            connection_id="c1",
            container_name="cnt",
            websocket=ws,
        )

        # Should not raise, just log
        msg = await broadcast_service.broadcast(
            container_name="cnt",
            event_type=BroadcastEventType.CUSTOM,
            payload={"key": "value"},
        )
        assert msg.event_type == BroadcastEventType.CUSTOM
        assert msg.container_name == "cnt"
        assert msg.payload == {"key": "value"}

        # Verify the message was stored in history despite delivery failure
        history = await broadcast_service.get_history("cnt")
        assert len(history) >= 1
        assert any(m.message_id == msg.message_id for m in history)

    async def test_broadcast_with_disconnected_subscriber(self, broadcast_service):
        """Subscriber with disconnected WebSocket is skipped."""
        from services.websocket_broadcast import BroadcastEventType

        ws = _ws(state=WebSocketState.DISCONNECTED)

        await broadcast_service.subscribe(
            connection_id="c1",
            container_name="cnt",
            websocket=ws,
        )

        msg = await broadcast_service.broadcast(
            container_name="cnt",
            event_type=BroadcastEventType.CUSTOM,
            payload={"key": "value"},
        )
        # Message should still be created, just not delivered
        assert msg is not None
        assert msg.container_name == "cnt"
        assert msg.event_type == BroadcastEventType.CUSTOM
        assert msg.payload == {"key": "value"}
        ws.send_json.assert_not_awaited()


# ===========================================================================
# services/websocket_health_monitor.py — ping history and latency calc
# ===========================================================================


class TestPingHistoryAndLatency:
    async def test_send_ping_stores_history(self, health_monitor, mock_websocket):
        """Verify ping history is stored and trimmed."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        # Send a successful ping
        mock_websocket.send_json = AsyncMock()
        await health_monitor._send_ping("conn-1", mock_websocket)

        async with health_monitor._lock:
            history = health_monitor._ping_history["conn-1"]
            assert len(history) == 1
            assert history[0].timed_out is False

        await health_monitor.unregister_connection("conn-1")

    async def test_handle_pong_calculates_latency(self, health_monitor, mock_websocket):
        """Verify pong handler calculates latency from ping history."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        # Send ping first
        mock_websocket.send_json = AsyncMock()
        await health_monitor._send_ping("conn-1", mock_websocket)

        # Handle pong
        await health_monitor.handle_pong("conn-1", "p1")

        status = await health_monitor.get_connection_health("conn-1")
        # Latency should be calculated (very small since test is immediate)
        assert status.average_latency_ms is not None
        assert isinstance(status.average_latency_ms, float)
        # Verify pong was actually counted
        assert status.total_pongs_received == 1

        await health_monitor.unregister_connection("conn-1")


# ===========================================================================
# services/websocket_health_monitor.py — detect_stale_connections
# ===========================================================================


class TestDetectStaleConnections:
    async def test_detect_stale_by_inactivity(self, health_monitor, mock_websocket):
        """Stale detection via inactivity threshold."""
        health_monitor.stale_threshold = 0  # immediate stale
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        stale = await health_monitor.detect_stale_connections()
        assert "conn-1" in stale

        await health_monitor.unregister_connection("conn-1")

    async def test_detect_stale_by_failed_pings(self, health_monitor, mock_websocket):
        """Stale detection via failed pings."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)

        async with health_monitor._lock:
            health_monitor._connection_health["conn-1"].failed_pings = 10

        stale = await health_monitor.detect_stale_connections(container_name="c1")
        assert "conn-1" in stale

        await health_monitor.unregister_connection("conn-1")

    async def test_detect_stale_no_connections(self, health_monitor, mock_websocket):
        """No stale connections when none registered."""
        stale = await health_monitor.detect_stale_connections()
        assert isinstance(stale, list)
        assert stale == []

        # Verify a freshly registered connection does not show as stale
        # (use a generous stale_threshold so it's definitely not stale)
        health_monitor.stale_threshold = 9999
        await health_monitor.register_connection("fresh-conn", "c1", mock_websocket)
        stale = await health_monitor.detect_stale_connections()
        assert "fresh-conn" not in stale
        await health_monitor.unregister_connection("fresh-conn")


# ===========================================================================
# services/websocket_health_monitor.py — get_all_container_health
# ===========================================================================


class TestAllContainerHealth:
    async def test_get_all_container_health(self, health_monitor, mock_websocket):
        """Cover get_all_container_health."""
        await health_monitor.register_connection("conn-1", "c1", mock_websocket)
        await health_monitor.register_connection("conn-2", "c2", mock_websocket)

        all_health = await health_monitor.get_all_container_health()
        assert "c1" in all_health
        assert "c2" in all_health

        await health_monitor.unregister_connection("conn-1")
        await health_monitor.unregister_connection("conn-2")
