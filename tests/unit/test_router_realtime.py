"""Tests for routers/realtime.py — WebSocket real-time analysis endpoints.

Covers: connect rejects, parse request edge cases, multi-file delegation,
metrics/status, timeout/service-unavailable, container client failure,
disconnect cleanup, and exception → structured error.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.websockets import WebSocketDisconnect, WebSocketState
from starlette.datastructures import Headers

from routers.realtime import (
    ConnectionManager,
    _build_analysis_response,
    _build_parse_result_data,
    handle_analyze_file,
    handle_get_status,
    handle_pong,
    handle_realtime_message,
    handle_unwatch_file,
    handle_watch_file,
    websocket_realtime_analysis,
)
from tests.conftest import ws_licence_headers

# Since coding-client 1.15.0 an uncredentialed WS handshake is REFUSED — a bare
# X-Tenant-Id proves nothing. This suite tests the router's BEHAVIOUR, not the
# auth boundary (tests/unit/test_ws_identity.py owns that), so every handshake
# here carries a real licence, exactly as coding-gateway does in production.
pytestmark = pytest.mark.usefixtures("ws_licence")


# ─── helpers ──────────────────────────────────────────────────────────
def _ws(query_params=None, headers=None, state=WebSocketState.CONNECTED):
    ws = AsyncMock()
    ws.query_params = query_params or {}
    # Starlette's real Headers, not a dict: it is case-insensitive, and the SDK
    # reads `authorization` in lower case while tests write `Authorization`. A
    # plain dict is NARROWER than the real object and would silently make every
    # credentialled handshake read as uncredentialed.
    ws.headers = Headers(headers or {})
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.send_json = AsyncMock()
    ws.send_text = AsyncMock()
    ws.receive_text = AsyncMock()
    ws.client_state = state
    ws.client = MagicMock()
    ws.client.host = "127.0.0.1"
    return ws


# ─── connect rejects ─────────────────────────────────────────────────


async def test_ws_rejects_missing_tenant():
    """A handshake with no credential at all is closed, never accepted.

    Before coding-client 1.15.0 this closed only because no header was sent — a
    caller that added `X-Tenant-Id: victim` and nothing else was SERVED. The
    forged-header half is pinned in test_ws_identity.py; this is the
    empty-handshake half.
    """
    ws = _ws(query_params={"container_name": "c1"})
    await websocket_realtime_analysis(ws, connection_id="conn-1")
    ws.close.assert_awaited_once()
    assert ws.close.await_args.kwargs["code"] == 1008
    assert "no verifiable credential" in ws.close.await_args.kwargs.get("reason", "")


async def test_ws_rejects_missing_container():
    """WebSocketContainerContext raises when container_name is absent."""
    from utils.websocket_utils import WebSocketErrorCode, WebSocketException

    ws = _ws(query_params={}, headers=ws_licence_headers("t1"))

    with (
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
            AsyncMock(
                side_effect=WebSocketException(
                    code=1008,
                    reason="Missing required query parameter: container_name",
                    error_code=WebSocketErrorCode.MISSING_CONTAINER_PARAM,
                )
            ),
        ),
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aexit__",
            AsyncMock(return_value=False),
        ),
    ):
        with pytest.raises(WebSocketException):
            await websocket_realtime_analysis(ws, connection_id="conn-1")


async def test_ws_rejects_invalid_tenant_for_container():
    """Close when container validation fails for tenant."""
    ws = _ws(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    with (
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
            AsyncMock(return_value="c1"),
        ),
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aexit__",
            AsyncMock(return_value=False),
        ),
        patch(
            "routers.realtime.websocket.validate_and_get_container_for_tenant",
            AsyncMock(side_effect=HTTPException(status_code=404, detail="not found")),
        ),
    ):
        await websocket_realtime_analysis(ws, connection_id="conn-1")
    ws.close.assert_awaited()


# ─── unsupported message type ────────────────────────────────────────


async def test_handle_realtime_message_unknown_type():
    """Unknown message type returns structured error."""
    AsyncMock()
    cm = ConnectionManager()
    cm.active_connections["conn-1"] = _ws()
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with patch("routers.realtime.handlers.connection_manager", cm):
        await handle_realtime_message("conn-1", {"type": "unknown_type"}, None, "c1")

    sent = cm.active_connections["conn-1"].send_text.await_args
    payload = json.loads(sent.args[0])
    assert payload["type"] == "error"
    assert "Unknown message type" in payload["error"]


# ─── analyze_file: missing file_path ─────────────────────────────────


async def test_analyze_file_missing_file_path():
    """Returns error when file_path is missing."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with patch("routers.realtime.analyze.connection_manager", cm):
        await handle_analyze_file("conn-1", {"content": "x"}, None, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"
    assert "Missing file_path or content" in sent["error"]


async def test_analyze_file_missing_content():
    """Returns error when content is missing."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with patch("routers.realtime.analyze.connection_manager", cm):
        await handle_analyze_file("conn-1", {"file_path": "/a.py"}, None, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"
    assert "Missing file_path or content" in sent["error"]


# ─── analyze_file: path traversal / resolution failure ────────────────


async def test_analyze_file_path_resolution_failure():
    """Container path resolution failure sends error."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with (
        patch("routers.realtime.analyze.connection_manager", cm),
        patch(
            "routers.realtime._path_resolution.resolve_container_path",
            side_effect=RuntimeError("path traversal detected"),
        ),
    ):
        await handle_analyze_file(
            "conn-1",
            {"file_path": "../../../etc/passwd", "content": "x"},
            None,
            "c1",
        )

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"
    assert "Invalid file path" in sent["error"]


# ─── analyze_file: unsupported language / no processor ────────────────


async def test_analyze_file_no_processor():
    """Returns error when no processor for the file."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.content_hashes = {}

    factory = AsyncMock()
    factory.get_processor_for_file.return_value = None

    with (
        patch("routers.realtime.analyze.connection_manager", cm),
        patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
    ):
        await handle_analyze_file(
            "conn-1",
            {"file_path": "/workspace/file.xyz", "content": "data"},
            None,
            None,
        )

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"
    assert "No processor available" in sent["error"]


# ─── analyze_file: parser failure ─────────────────────────────────────


async def test_analyze_file_parser_raises():
    """ValueError during analysis sends structured error."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.content_hashes = {}

    processor = AsyncMock()
    processor.parse_file_with_metrics = AsyncMock(side_effect=ValueError("bad parse"))
    factory = AsyncMock()
    factory.get_processor_for_file.return_value = processor

    metrics = AsyncMock()

    with (
        patch("routers.realtime.analyze.connection_manager", cm),
        patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
        patch("routers.realtime.analyze.get_websocket_metrics_collector", return_value=metrics),
    ):
        await handle_analyze_file(
            "conn-1",
            {"file_path": "/workspace/file.py", "content": "x = 1"},
            None,
            None,  # no container → skip resolve_container_path
        )

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"
    assert "bad parse" in sent["error"]


# ─── analyze_file: success path ───────────────────────────────────────


async def test_analyze_file_success():
    """Successful analysis sends result with metrics."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.content_hashes = {}

    entity = SimpleNamespace(id="e1", name="foo", type="function", line_number=1, language="python")
    rel = SimpleNamespace(source_id="e1", target_id="e2", type="calls")
    parse_result = SimpleNamespace(
        success=True,
        language="python",
        file_path="/workspace/file.py",
        entities=[entity],
        relationships=[rel],
        complexity_score=2.0,
        lines_of_code=5,
        errors=[],
    )
    processor = AsyncMock()
    processor.parse_file_with_metrics = AsyncMock(return_value=parse_result)
    factory = AsyncMock()
    factory.get_processor_for_file.return_value = processor

    cache = AsyncMock()
    cache.get_stats = AsyncMock(return_value={"hit_rate": 50.0})
    monitor = AsyncMock()
    metrics = AsyncMock()

    with (
        patch("routers.realtime.analyze.connection_manager", cm),
        patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
        patch("routers.realtime.analyze.get_analysis_cache", return_value=cache),
        patch("routers.realtime.analyze.get_performance_monitor", return_value=monitor),
        patch("routers.realtime.analyze.get_websocket_metrics_collector", return_value=metrics),
        patch(
            "routers.realtime._path_resolution.resolve_container_path",
            return_value="/workspace/file.py",
        ),
    ):
        await handle_analyze_file(
            "conn-1",
            {"file_path": "/workspace/file.py", "content": "x = 1"},
            None,
            "c1",
        )

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_result"
    assert sent["success"] is True
    assert sent["parse_result"]["language"] == "python"


# ─── multi-file delegation ───────────────────────────────────────────


async def test_handle_realtime_message_delegates_multi_file():
    """analyze_files message delegates to handle_multi_file_analysis."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with (
        patch("routers.realtime.handlers.connection_manager", cm),
        patch(
            "routers.realtime.handlers.handle_multi_file_analysis", new_callable=AsyncMock
        ) as mock_multi,
    ):
        await handle_realtime_message(
            "conn-1",
            {"type": "analyze_files", "file_paths": ["/a.py"]},
            None,
            "c1",
            "tenant-1",
        )
    mock_multi.assert_awaited_once()
    # Verify the callback is connection_manager.send_message
    assert mock_multi.await_args is not None
    assert mock_multi.await_args.args[2] == cm.send_message


# ─── get_status ───────────────────────────────────────────────────────


async def test_handle_get_status():
    """get_status sends status with watched files and cache stats."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = {"/a.py"}
    cm.connection_containers["conn-1"] = "c1"

    cache = AsyncMock()
    cache.get_stats = AsyncMock(return_value={"hit_rate": 75})

    with (
        patch("routers.realtime.status.connection_manager", cm),
        patch("routers.realtime.status.get_analysis_cache", return_value=cache),
    ):
        await handle_get_status("conn-1", {}, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "status_update"
    assert sent["connection_id"] == "conn-1"
    assert "/a.py" in sent["watched_files"]


async def test_handle_get_status_error():
    """get_status exception sends error."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with (
        patch("routers.realtime.status.connection_manager", cm),
        patch("routers.realtime.status.get_analysis_cache", side_effect=ValueError("cache error")),
    ):
        await handle_get_status("conn-1", {}, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "error"
    assert "cache error" in sent["error"]


# ─── watch / unwatch ──────────────────────────────────────────────────


async def test_handle_watch_file_success():
    """Watch file sends confirmation."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.file_watchers = {}

    with patch("routers.realtime.watch.connection_manager", cm):
        await handle_watch_file("conn-1", {"file_path": "/a.py"}, None, None)

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "watch_started"


async def test_handle_watch_file_missing_path():
    """Watch file with no path sends error."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with patch("routers.realtime.watch.connection_manager", cm):
        await handle_watch_file("conn-1", {}, None, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "error"
    assert "Missing file_path" in sent["error"]


async def test_handle_watch_file_container_path_resolution_fails():
    """Watch file with container path resolution failure."""
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
        await handle_watch_file("conn-1", {"file_path": "bad.py"}, None, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert "Invalid file path" in sent["error"]


async def test_handle_unwatch_file_success():
    """Unwatch file sends confirmation."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = {"/a.py"}
    cm.connection_containers["conn-1"] = "c1"
    cm.file_watchers = {"/a.py": {"conn-1"}}

    with patch("routers.realtime.watch.connection_manager", cm):
        await handle_unwatch_file("conn-1", {"file_path": "/a.py"}, None, None)

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "watch_stopped"


async def test_handle_unwatch_file_missing_path():
    """Unwatch file with no path sends error."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with patch("routers.realtime.watch.connection_manager", cm):
        await handle_unwatch_file("conn-1", {}, None, "c1")

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "error"
    assert "Missing file_path" in sent["error"]


async def test_handle_unwatch_file_container_path_resolution_fails():
    """Unwatch file with container path resolution failure."""
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


# ─── workspace_path relative resolution ──────────────────────────────


async def test_watch_file_workspace_relative_path():
    """When workspace_path given and path is relative, it's joined."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.file_watchers = {}

    with patch("routers.realtime.watch.connection_manager", cm):
        await handle_watch_file("conn-1", {"file_path": "src/main.py"}, Path("/workspace"), None)

    sent = json.loads(ws.send_text.await_args.args[0])
    assert "/workspace/src/main.py" in sent["file_path"]


# ─── disconnect cleanup ──────────────────────────────────────────────


async def test_connection_manager_disconnect_cleanup():
    """Disconnect cleans up all tracking structures."""
    cm = ConnectionManager()
    ws = _ws()
    metrics = AsyncMock()

    with patch("routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics):
        await cm.connect(ws, "conn-1", "c1")

    cm.watch_file("conn-1", "/a.py")
    assert "conn-1" in cm.active_connections
    assert "/a.py" in cm.file_watchers
    assert "c1" in cm.container_connections

    cm.disconnect("conn-1")

    assert "conn-1" not in cm.active_connections
    assert "conn-1" not in cm.connection_files
    assert "conn-1" not in cm.connection_containers
    assert "c1" not in cm.container_connections
    assert "/a.py" not in cm.file_watchers


# ─── pong handler ─────────────────────────────────────────────────────


async def test_handle_pong_success():
    """Pong with ping_id processes correctly."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    # The health monitor tracks the socket under the REGISTRY's id.
    cm.connection_monitor_ids["conn-1"] = "c1_registry-id"

    monitor = AsyncMock()
    monitor.handle_pong.return_value = True
    metrics = AsyncMock()

    with (
        patch("routers.realtime.status.connection_manager", cm),
        patch("routers.realtime.status.get_health_monitor", return_value=monitor),
        patch("routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics),
    ):
        await handle_pong("conn-1", {"ping_id": "p1"})

    # Reported under the monitor's id — this used to assert "conn-1", i.e. it
    # pinned the bug: the monitor never knew "conn-1", so every pong missed.
    monitor.handle_pong.assert_awaited_once_with("c1_registry-id", "p1")
    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "pong_acknowledged"


async def test_handle_pong_missing_ping_id():
    """Pong without ping_id is silently ignored."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    with patch("routers.realtime.status.connection_manager", cm):
        await handle_pong("conn-1", {})
    # No error, no message sent
    ws.send_text.assert_not_awaited()


async def test_handle_pong_error():
    """Pong handler suppresses exceptions."""
    monitor = AsyncMock()
    monitor.handle_pong.side_effect = RuntimeError("boom")

    with patch("routers.realtime.status.get_health_monitor", return_value=monitor):
        # Should not raise
        await handle_pong("conn-1", {"ping_id": "p1"})


# ─── connection_manager edge cases ────────────────────────────────────


async def test_connection_manager_send_message_not_connected():
    """send_message to unknown connection is a no-op."""
    cm = ConnectionManager()
    await cm.send_message("nonexistent", {"type": "test"})
    # No error raised


async def test_connection_manager_send_message_error_disconnects():
    """send_message that fails disconnects the connection."""
    cm = ConnectionManager()
    ws = _ws()
    ws.send_text.side_effect = ConnectionError("broken")
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    metrics = AsyncMock()
    with patch("routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics):
        await cm.send_message("conn-1", {"type": "test"})

    assert "conn-1" not in cm.active_connections


async def test_connection_manager_has_content_changed():
    """Content change detection works."""
    cm = ConnectionManager()
    assert cm.has_content_changed("/a.py", "hello") is True  # First time
    assert cm.has_content_changed("/a.py", "hello") is False  # Same
    assert cm.has_content_changed("/a.py", "world") is True  # Changed


async def test_connection_manager_broadcast_to_file_watchers():
    """Broadcast sends to all watchers of a file."""
    cm = ConnectionManager()
    ws1, ws2 = _ws(), _ws()
    cm.active_connections["c1"] = ws1
    cm.active_connections["c2"] = ws2
    cm.connection_files["c1"] = {"/a.py"}
    cm.connection_files["c2"] = {"/a.py"}
    cm.connection_containers["c1"] = "container"
    cm.connection_containers["c2"] = "container"
    cm.file_watchers = {"/a.py": {"c1", "c2"}}

    metrics = AsyncMock()
    with patch("routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics):
        await cm.broadcast_to_file_watchers("/a.py", {"type": "update"})

    assert ws1.send_text.await_count == 1
    assert ws2.send_text.await_count == 1


async def test_connection_manager_container_helpers():
    """Container helper methods return correct data."""
    cm = ConnectionManager()
    ws = _ws()
    metrics = AsyncMock()

    with patch("routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics):
        await cm.connect(ws, "conn-1", "c1")

    assert cm.get_container_name("conn-1") == "c1"
    assert cm.get_active_connections_for_container("c1") == ["conn-1"]
    assert cm.get_connection_count_for_container("c1") == 1
    assert cm.get_connection_metadata("conn-1") is not None
    assert "c1" in cm.get_all_container_names()
    assert cm.get_container_name("nonexistent") is None
    assert cm.get_connection_metadata("nonexistent") is None
    assert cm.get_active_connections_for_container("nonexistent") == []
    assert cm.get_connection_count_for_container("nonexistent") == 0


# ─── _build helpers ───────────────────────────────────────────────────


def test_build_parse_result_data():
    """Helper builds correct structure from parse result."""
    entity = SimpleNamespace(id="e1", name="f", type="function", line_number=1, language="python")
    rel = SimpleNamespace(source_id="e1", target_id="e2", type="calls")
    pr = SimpleNamespace(
        language="python",
        file_path="/a.py",
        lines_of_code=10,
        entities=[entity],
        relationships=[rel],
        complexity_score=3.0,
        success=True,
        errors=[],
    )
    data = _build_parse_result_data(pr)
    assert data["language"] == "python"
    assert len(data["entities"]) == 1
    assert len(data["relationships"]) == 1


async def test_build_analysis_response():
    """Helper builds response with diagnostics when errors present."""
    entity = SimpleNamespace(id="e1", name="f", type="function", line_number=1, language="python")
    pr = SimpleNamespace(
        language="python",
        file_path="/a.py",
        lines_of_code=5,
        entities=[entity],
        relationships=[],
        complexity_score=1.0,
        success=True,
        errors=["warning: unused var"],
    )
    resp = await _build_analysis_response("/a.py", pr, 10.0, {"hit_rate": 50.0}, "c1")
    assert resp["type"] == "analysis_result"
    assert resp["container_name"] == "c1"
    assert len(resp["diagnostics"]) == 1


# ─── GET /connections endpoint ────────────────────────────────────────


async def test_get_active_connections_success():
    from routers.realtime import get_active_connections

    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = {"/a.py"}
    cm.file_watchers = {"/a.py": {"conn-1"}}
    cm.connection_tenants["conn-1"] = "t1"

    cache = AsyncMock()
    cache.get_stats = AsyncMock(return_value={"hit_rate": 50.0})

    with (
        patch("routers.realtime.connections.connection_manager", cm),
        patch("routers.realtime.connections.get_analysis_cache", return_value=cache),
    ):
        result = await get_active_connections(tenant_id="t1")

    assert result.active_connections == 1
    assert result.total_watched_files == 1


async def test_get_active_connections_error():
    from routers.realtime import get_active_connections

    cm = ConnectionManager()

    with (
        patch("routers.realtime.connections.connection_manager", cm),
        patch("routers.realtime.connections.get_analysis_cache", side_effect=ValueError("boom")),
    ):
        result = await get_active_connections(tenant_id="t1")

    assert result.active_connections == 0
    assert result.error is not None


# ─── websocket message loop: disconnect and error ─────────────────────


async def test_ws_message_loop_disconnect():
    """WebSocket disconnect triggers cleanup."""
    ws = _ws(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))
    ws.receive_text.side_effect = WebSocketDisconnect()

    metrics = AsyncMock()

    with (
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
            AsyncMock(return_value="c1"),
        ),
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aexit__",
            AsyncMock(return_value=False),
        ),
        patch(
            "routers.realtime.websocket.validate_and_get_container_for_tenant",
            AsyncMock(return_value={"name": "c1"}),
        ),
        patch("routers.realtime.websocket.get_websocket_metrics_collector", return_value=metrics),
        patch("routers.realtime.websocket.connection_manager") as mock_cm,
    ):
        # connect() returns the (tenant, id) registry key; the route must use it.
        mock_cm.connect = AsyncMock(return_value="t1\x1fconn-1")
        mock_cm.disconnect = MagicMock()
        mock_cm.send_message = AsyncMock()

        await websocket_realtime_analysis(ws, connection_id="conn-1")

    # ...and pass ITS socket, so a superseded socket cannot evict a reconnect.
    mock_cm.disconnect.assert_called_once_with("t1\x1fconn-1", ws)


async def test_ws_message_loop_stops_when_superseded_mid_receive():
    """A socket superseded while awaiting a frame must not act on the shared key.

    A same-tenant reconnect takes the slot over while this loop is blocked in
    ``receive_text``. The frame that then arrives must not be handled: every
    manager call on the key would reach the NEW socket.
    """
    ws = _ws(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))
    ws.receive_text.return_value = '{"type": "get_status"}'

    with (
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
            AsyncMock(return_value="c1"),
        ),
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aexit__",
            AsyncMock(return_value=False),
        ),
        patch(
            "routers.realtime.websocket.validate_and_get_container_for_tenant",
            AsyncMock(return_value={"name": "c1"}),
        ),
        patch(
            "routers.realtime.websocket.get_websocket_metrics_collector", return_value=AsyncMock()
        ),
        patch("routers.realtime.websocket.check_message_rate_limit", AsyncMock()),
        patch("routers.realtime.websocket.handle_realtime_message", AsyncMock()) as handled,
        patch("routers.realtime.websocket.connection_manager") as mock_cm,
    ):
        mock_cm.connect = AsyncMock(return_value="t1\x1fconn-1")
        mock_cm.disconnect = MagicMock()
        mock_cm.send_message = AsyncMock()
        # Owns the slot when the loop starts; superseded by the time the frame lands.
        mock_cm.owns = MagicMock(side_effect=[True, False])

        await websocket_realtime_analysis(ws, connection_id="conn-1")

    ws.receive_text.assert_awaited_once()
    handled.assert_not_awaited()
    mock_cm.disconnect.assert_called_once_with("t1\x1fconn-1", ws)


async def test_ws_message_loop_json_error():
    """Invalid JSON triggers error handling and cleanup."""
    ws = _ws(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))
    ws.receive_text.side_effect = json.JSONDecodeError("bad", "", 0)

    metrics = AsyncMock()

    with (
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
            AsyncMock(return_value="c1"),
        ),
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aexit__",
            AsyncMock(return_value=False),
        ),
        patch(
            "routers.realtime.websocket.validate_and_get_container_for_tenant",
            AsyncMock(return_value={"name": "c1"}),
        ),
        patch("routers.realtime.websocket.get_websocket_metrics_collector", return_value=metrics),
        patch("routers.realtime.websocket.connection_manager") as mock_cm,
    ):
        # connect() returns the (tenant, id) registry key; the route must use it.
        mock_cm.connect = AsyncMock(return_value="t1\x1fconn-1")
        mock_cm.disconnect = MagicMock()
        mock_cm.send_message = AsyncMock()

        await websocket_realtime_analysis(ws, connection_id="conn-1")

    mock_cm.send_message.assert_awaited()
    # ...and pass ITS socket, so a superseded socket cannot evict a reconnect.
    mock_cm.disconnect.assert_called_once_with("t1\x1fconn-1", ws)


# ─── rate limiting in message loop ────────────────────────────────────


async def test_ws_rate_limit_sends_message_and_continues():
    """Rate limit exceeded sends message but doesn't disconnect."""
    from utils.websocket_utils import WebSocketErrorCode, WebSocketException

    ws = _ws(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))
    ws.receive_text.side_effect = [
        json.dumps({"type": "get_status"}),
        WebSocketDisconnect(),
    ]

    rate_exc = WebSocketException(
        code=1008,
        reason="rate exceeded",
        error_code=WebSocketErrorCode.MESSAGE_RATE_EXCEEDED,
        details={"current_rate": 20.0, "rate_limit": 10.0},
    )

    metrics = AsyncMock()

    with (
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
            AsyncMock(return_value="c1"),
        ),
        patch(
            "routers.realtime.websocket.WebSocketContainerContext.__aexit__",
            AsyncMock(return_value=False),
        ),
        patch(
            "routers.realtime.websocket.validate_and_get_container_for_tenant",
            AsyncMock(return_value={"name": "c1"}),
        ),
        patch("routers.realtime.websocket.get_websocket_metrics_collector", return_value=metrics),
        patch(
            "routers.realtime.websocket.check_message_rate_limit",
            AsyncMock(side_effect=rate_exc),
        ),
        patch(
            "routers.realtime.websocket.send_rate_limit_exceeded_message",
            new_callable=AsyncMock,
        ) as mock_send_rate,
    ):
        with patch("routers.realtime.websocket.connection_manager") as mock_cm:
            mock_cm.connect = AsyncMock()
            mock_cm.disconnect = MagicMock()
            mock_cm.send_message = AsyncMock()

            await websocket_realtime_analysis(ws, connection_id="conn-1")

    mock_send_rate.assert_awaited()


# ─── handle_realtime_message: pong routing ────────────────────────────


async def test_handle_realtime_message_routes_pong():
    """Pong message is routed to handle_pong."""
    monitor = AsyncMock()
    monitor.handle_pong.return_value = True

    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.connection_monitor_ids["conn-1"] = "c1_registry-id"

    with (
        patch("routers.realtime.status.connection_manager", cm),
        patch("routers.realtime.status.get_health_monitor", return_value=monitor),
        patch(
            "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
        ),
    ):
        await handle_realtime_message("conn-1", {"type": "pong", "ping_id": "p1"}, None, "c1")

    monitor.handle_pong.assert_awaited_once_with("c1_registry-id", "p1")


# ─── handle_realtime_message: watch routing ───────────────────────────


async def test_handle_realtime_message_routes_watch():
    """watch_file message is routed correctly."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"
    cm.file_watchers = {}

    with (
        patch("routers.realtime.watch.connection_manager", cm),
        patch("routers.realtime._path_resolution.resolve_container_path", return_value="/a.py"),
    ):
        await handle_realtime_message(
            "conn-1", {"type": "watch_file", "file_path": "/a.py"}, None, "c1"
        )

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "watch_started"


async def test_handle_realtime_message_routes_unwatch():
    """unwatch_file message is routed correctly."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = {"/a.py"}
    cm.connection_containers["conn-1"] = "c1"
    cm.file_watchers = {"/a.py": {"conn-1"}}

    with (
        patch("routers.realtime.watch.connection_manager", cm),
        patch("routers.realtime._path_resolution.resolve_container_path", return_value="/a.py"),
    ):
        await handle_realtime_message(
            "conn-1", {"type": "unwatch_file", "file_path": "/a.py"}, None, "c1"
        )

    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "watch_stopped"


# ─── GET /websocket-api documentation endpoint ───────────────────────


async def test_get_websocket_api_documentation():
    from routers.realtime import get_websocket_api_documentation

    result = await get_websocket_api_documentation(tenant_id="t1")
    assert result.endpoint == "/api/realtime/analysis"
    assert "analyze_file" in result.message_types


# ─── analyze_file: unchanged content is skipped ───────────────────────


async def test_analyze_file_unchanged_content_replays_cached_result():
    """Unchanged content REPLIES with the cached result, flagged ``cached``.

    Regression guard: this used to ``return`` without sending anything, so a
    dedup hit was indistinguishable from a lost request.
    """
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    content = "x = 1"
    # Seed via the manager API so the composite scope key is used.
    assert cm.has_content_changed("/a.py", content) is True
    cm.record_analysis("/a.py", {"type": "analysis_result", "file_path": "/a.py", "success": True})

    with patch("routers.realtime.analyze.connection_manager", cm):
        await handle_analyze_file(
            "conn-1",
            {"file_path": "/a.py", "content": content},
            None,
            None,
        )

    ws.send_text.assert_awaited()
    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_result"
    assert sent["cached"] is True
    assert sent["file_path"] == "/a.py"


async def test_analyze_file_unchanged_but_uncached_reanalyzes():
    """A hash hit with no cached response must NOT be a silent no-op.

    The first analysis can fail after the hash was recorded; without this
    fallthrough that file could never be analyzed again.
    """
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    content = "data"
    # Hash recorded, but no analysis ever stored (previous attempt failed).
    assert cm.has_content_changed("/workspace/file.xyz", content) is True

    factory = AsyncMock()
    factory.get_processor_for_file.return_value = None

    with (
        patch("routers.realtime.analyze.connection_manager", cm),
        patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
    ):
        await handle_analyze_file(
            "conn-1",
            {"file_path": "/workspace/file.xyz", "content": content},
            None,
            None,
        )

    ws.send_text.assert_awaited()
    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"


async def test_analyze_file_processor_without_metrics_reports_error():
    """A processor lacking ``parse_file_with_metrics`` must not answer silently."""
    cm = ConnectionManager()
    ws = _ws()
    cm.active_connections["conn-1"] = ws
    cm.connection_files["conn-1"] = set()
    cm.connection_containers["conn-1"] = "c1"

    processor = MagicMock(spec=[])  # no parse_file_with_metrics attribute
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

    ws.send_text.assert_awaited()
    sent = json.loads(ws.send_text.await_args.args[0])
    assert sent["type"] == "analysis_error"
    assert "does not support metrics" in sent["error"]


# ─── cross-tenant isolation of the analysis cache ─────────────────────


async def test_has_content_changed_is_tenant_scoped():
    """Two tenants analyzing the SAME path must not share a hash entry."""
    cm = ConnectionManager()
    same_path = "src/main.py"
    same_content = "print('hello')"

    assert cm.has_content_changed(same_path, same_content, tenant_id="tenant-a") is True
    # Tenant B is a first-time analysis for ITS scope, even though tenant A
    # just recorded the identical path+content.
    assert cm.has_content_changed(same_path, same_content, tenant_id="tenant-b") is True
    # And each tenant still dedups within its own scope.
    assert cm.has_content_changed(same_path, same_content, tenant_id="tenant-a") is False
    assert cm.has_content_changed(same_path, same_content, tenant_id="tenant-b") is False


async def test_has_content_changed_is_container_scoped():
    """Same tenant, two containers, same path — independent entries."""
    cm = ConnectionManager()
    assert cm.has_content_changed("src/main.py", "a", tenant_id="t1", container_name="c1") is True
    assert cm.has_content_changed("src/main.py", "a", tenant_id="t1", container_name="c2") is True
    assert cm.has_content_changed("src/main.py", "a", tenant_id="t1", container_name="c1") is False


async def test_last_analysis_is_tenant_scoped():
    """One tenant's stored analysis is never readable by another."""
    cm = ConnectionManager()
    cm.record_analysis("src/main.py", {"owner": "a"}, tenant_id="tenant-a")

    assert cm.get_last_analysis("src/main.py", tenant_id="tenant-a") == {"owner": "a"}
    assert cm.get_last_analysis("src/main.py", tenant_id="tenant-b") is None


async def test_two_tenants_same_path_each_get_their_own_reply():
    """End-to-end: tenant B is never starved by tenant A's identical analysis."""
    path = "src/main.py"
    content = "x = 1"
    cm = ConnectionManager()

    ws_a, ws_b = _ws(), _ws()
    for conn, ws in (("conn-a", ws_a), ("conn-b", ws_b)):
        cm.active_connections[conn] = ws
        cm.connection_files[conn] = set()
        cm.connection_containers[conn] = "c1"

    entity = SimpleNamespace(id="e1", name="foo", type="function", line_number=1, language="python")
    parse_result = SimpleNamespace(
        success=True,
        language="python",
        file_path=path,
        entities=[entity],
        relationships=[],
        complexity_score=1.0,
        lines_of_code=1,
        errors=[],
    )
    processor = AsyncMock()
    processor.parse_file_with_metrics = AsyncMock(return_value=parse_result)
    factory = AsyncMock()
    factory.get_processor_for_file.return_value = processor

    cache = AsyncMock()
    cache.get_stats = AsyncMock(return_value={"hit_rate": 50.0})

    with (
        patch("routers.realtime.analyze.connection_manager", cm),
        patch("routers.realtime.analyze.get_multi_language_factory", return_value=factory),
        patch("routers.realtime.analyze.get_websocket_metrics_collector", return_value=AsyncMock()),
        patch("routers.realtime.analyze.get_performance_monitor", return_value=AsyncMock()),
        patch("routers.realtime.analyze.get_analysis_cache", return_value=cache),
    ):
        await handle_analyze_file(
            "conn-a", {"file_path": path, "content": content}, None, None, "tenant-a"
        )
        await handle_analyze_file(
            "conn-b", {"file_path": path, "content": content}, None, None, "tenant-b"
        )

    # Both tenants got a real analysis_result — B was NOT swallowed as a
    # "unchanged" no-op just because A analyzed the same path first.
    for ws in (ws_a, ws_b):
        ws.send_text.assert_awaited()
        sent = json.loads(ws.send_text.await_args.args[0])
        assert sent["type"] == "analysis_result"
        assert sent.get("cached") is not True

    # Each tenant has its own independent cache entry.
    assert cm.get_last_analysis(path, tenant_id="tenant-a") is not None
    assert cm.get_last_analysis(path, tenant_id="tenant-b") is not None
    assert cm.get_last_analysis(path, tenant_id="tenant-c") is None


async def test_dispatcher_threads_tenant_id_into_analyze_file():
    """``handle_realtime_message`` must forward tenant_id to the analyze handler."""
    with patch("routers.realtime.handlers.handle_analyze_file", new=AsyncMock()) as spy:
        await handle_realtime_message(
            "conn-1",
            {"type": "analyze_file", "file_path": "/a.py", "content": "x"},
            None,
            "c1",
            "tenant-a",
        )
    spy.assert_awaited_once()
    assert spy.await_args is not None
    assert "tenant-a" in spy.await_args.args


# ─── analysis-state cache is bounded ──────────────────────────────────


async def test_content_hashes_are_lru_bounded():
    """The module-level maps must not grow without bound."""
    cm = ConnectionManager()
    cm.max_tracked_files = 5
    for i in range(50):
        cm.has_content_changed(f"/f{i}.py", "content", tenant_id="t1")
    assert len(cm.content_hashes) == 5
    # Oldest evicted, newest retained.
    assert cm.has_content_changed("/f49.py", "content", tenant_id="t1") is False
    assert cm.has_content_changed("/f0.py", "content", tenant_id="t1") is True


async def test_last_analysis_is_lru_bounded():
    cm = ConnectionManager()
    cm.max_tracked_files = 3
    for i in range(20):
        cm.record_analysis(f"/f{i}.py", {"n": i}, tenant_id="t1")
    assert len(cm.last_analysis) == 3
    assert cm.get_last_analysis("/f19.py", tenant_id="t1") == {"n": 19}
    assert cm.get_last_analysis("/f0.py", tenant_id="t1") is None


async def test_cache_helpers_accept_plain_dicts():
    """Tests/callers assign bare dicts to these attributes — must still work."""
    cm = ConnectionManager()
    cm.content_hashes = {}
    cm.last_analysis = {}
    assert cm.has_content_changed("/a.py", "x", tenant_id="t1") is True
    assert cm.has_content_changed("/a.py", "x", tenant_id="t1") is False
    cm.record_analysis("/a.py", {"ok": True}, tenant_id="t1")
    assert cm.get_last_analysis("/a.py", tenant_id="t1") == {"ok": True}
