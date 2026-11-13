from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.websockets import WebSocketDisconnect
from starlette.datastructures import Headers

from routers.broadcast_websocket import websocket_broadcast_subscribe
from services.websocket_broadcast import BroadcastEventType, BroadcastPriority
from tests.conftest import ws_licence_headers
from utils.websocket_utils import WebSocketException

# Since coding-client 1.15.0 an uncredentialed WS handshake is REFUSED — a bare
# X-Tenant-Id proves nothing. This suite tests the router's BEHAVIOUR, not the
# auth boundary (tests/unit/test_ws_identity.py owns that), so every handshake
# here carries a real licence, exactly as coding-gateway does in production.
pytestmark = pytest.mark.usefixtures("ws_licence")

# ---------------------------------------------------------------------------
# All optional parameters (event_types, tags, priority, replay_history,
# replay_limit) must be passed explicitly when calling the endpoint function
# directly, because FastAPI Query() defaults are not resolved outside the
# framework's dependency injection.
# ---------------------------------------------------------------------------

_DEFAULTS: dict[str, Any] = {
    "event_types": None,
    "tags": None,
    "priority": None,
    "replay_history": False,
    "replay_limit": 100,
}


def _websocket(query_params=None, headers=None):
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
    ws.receive_text = AsyncMock()
    return ws


def _patch_container_ok(container_name="c1"):
    """Patches that pass container extraction and tenant validation."""
    return (
        patch(
            "routers.broadcast_websocket.extract_container_from_websocket",
            AsyncMock(return_value=container_name),
        ),
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            AsyncMock(return_value={"name": container_name}),
        ),
    )


# ===================================================================
# 1. Tenant validation
# ===================================================================


async def test_broadcast_websocket_requires_tenant():
    """A handshake with no credential at all is closed 1008, never accepted.

    Before coding-client 1.15.0 this closed only because no header was sent —
    a caller that added `X-Tenant-Id: victim` and nothing else was SERVED
    (reproduced live on this route: it answered `subscription_established` for
    a container the caller held no licence for). The forged-header half is
    pinned separately in test_ws_identity.py; this is the empty-handshake half.
    """
    ws = _websocket(query_params={"container_name": "c1"})

    await websocket_broadcast_subscribe(ws, connection_id="conn-1", **_DEFAULTS)
    ws.close.assert_awaited_once()
    close_kwargs = ws.close.await_args.kwargs
    assert close_kwargs["code"] == 1008
    assert "no verifiable credential" in close_kwargs["reason"]
    ws.accept.assert_not_awaited()


# ===================================================================
# 2. Container extraction errors
# ===================================================================


async def test_broadcast_websocket_handles_websocket_exception():
    """When extract_container raises WebSocketException, close with its code and reason."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    with patch(
        "routers.broadcast_websocket.extract_container_from_websocket",
        AsyncMock(side_effect=WebSocketException(code=1003, reason="bad container")),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-1", **_DEFAULTS)
    ws.close.assert_awaited_once()
    assert ws.close.await_args.kwargs["code"] == 1003
    assert ws.close.await_args.kwargs["reason"] == "bad container"
    ws.accept.assert_not_awaited()


async def test_broadcast_websocket_handles_validation_errors():
    """When extract_container raises a generic exception, it should propagate."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    with patch(
        "routers.broadcast_websocket.extract_container_from_websocket",
        AsyncMock(side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await websocket_broadcast_subscribe(ws, connection_id="conn-1", **_DEFAULTS)


# ===================================================================
# 3. Container-tenant validation failure
# ===================================================================


async def test_broadcast_websocket_container_tenant_validation_failure():
    """HTTPException from validate_and_get_container_for_tenant closes with code 1008 and detail."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    with (
        patch(
            "routers.broadcast_websocket.extract_container_from_websocket",
            AsyncMock(return_value="c1"),
        ),
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            AsyncMock(side_effect=HTTPException(status_code=404, detail="Container not found")),
        ),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-1", **_DEFAULTS)
    ws.close.assert_awaited_once()
    assert ws.close.await_args.kwargs["code"] == 1008
    assert (
        ws.close.await_args.kwargs["reason"] == "Container validation failed: Container not found"
    )
    ws.accept.assert_not_awaited()


# ===================================================================
# 4. Invalid event_types filter at connection time
# ===================================================================


async def test_broadcast_websocket_invalid_event_type_filter():
    """Invalid event_types query param sends error JSON then closes with code 1008."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    extract_patch, validate_patch = _patch_container_ok()
    with extract_patch, validate_patch:
        await websocket_broadcast_subscribe(
            ws,
            connection_id="conn-1",
            event_types="not-real",
            tags=None,
            priority=None,
            replay_history=False,
            replay_limit=100,
        )

    ws.accept.assert_awaited_once()
    ws.send_json.assert_awaited_once()
    error_msg = ws.send_json.await_args_list[0].args[0]
    assert error_msg["type"] == "error"
    assert "Invalid event type" in error_msg["error"]

    ws.close.assert_awaited_once()
    assert ws.close.await_args.kwargs["code"] == 1008
    assert ws.close.await_args.kwargs["reason"] == "Invalid event type"


# ===================================================================
# 5. Invalid priority filter at connection time
# ===================================================================


async def test_broadcast_websocket_invalid_priority_filter():
    """Invalid priority query param sends error JSON then closes with code 1008."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    extract_patch, validate_patch = _patch_container_ok()
    with extract_patch, validate_patch:
        await websocket_broadcast_subscribe(
            ws,
            connection_id="conn-1",
            event_types=None,
            tags=None,
            priority="urgent",
            replay_history=False,
            replay_limit=100,
        )

    ws.accept.assert_awaited_once()
    ws.send_json.assert_awaited_once()
    error_msg = ws.send_json.await_args_list[0].args[0]
    assert error_msg["type"] == "error"
    assert "Invalid priority" in error_msg["error"]

    ws.close.assert_awaited_once()
    assert ws.close.await_args.kwargs["code"] == 1008
    assert ws.close.await_args.kwargs["reason"] == "Invalid priority"


# ===================================================================
# 6. Happy path: subscription, history replay, every message in the loop
# ===================================================================


async def test_broadcast_websocket_happy_path_validates_every_message():
    """
    Full happy-path flow:
    - subscription_established with correct fields
    - history_replayed from initial replay
    - message loop: subscribe, replay_history, bad replay, get_status, get_status(missing), unknown
    - cleanup via unsubscribe
    Validates EVERY send_json call in order.
    """
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    service = AsyncMock()
    service.subscribe.return_value = "sub-1"
    # First call: initial replay (replay_limit=5). Second call: in-loop replay_history message.
    service.replay_history.side_effect = [2, 1]

    subscription = SimpleNamespace(
        subscription_id="sub-1",
        container_name="c1",
        message_count=5,
        event_types={BroadcastEventType.TEST_COMPLETED},
        tags={"python"},
        priority_filter=BroadcastPriority.HIGH,
        last_message_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    # First get_status returns subscription, second returns None
    service.get_subscription.side_effect = [subscription, None]

    ws.receive_text.side_effect = [
        json.dumps({"type": "subscribe"}),
        json.dumps({"type": "replay_history", "event_types": ["test_completed"], "limit": 2}),
        json.dumps({"type": "replay_history", "event_types": ["bad"]}),
        json.dumps({"type": "get_status"}),
        json.dumps({"type": "get_status"}),
        json.dumps({"type": "funky_chicken"}),
        WebSocketDisconnect(),
    ]

    extract_patch, validate_patch = _patch_container_ok()
    with (
        extract_patch,
        validate_patch,
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        await websocket_broadcast_subscribe(
            ws,
            connection_id="conn-1",
            event_types="test_completed",
            tags="python,fast",
            priority=None,
            replay_history=True,
            replay_limit=5,
        )

    # --- Connection accepted ---
    ws.accept.assert_awaited_once()

    # --- subscribe called with correct parameters ---
    service.subscribe.assert_awaited_once()
    call_kwargs = service.subscribe.await_args.kwargs
    assert call_kwargs["connection_id"] == "conn-1"
    assert call_kwargs["container_name"] == "c1"
    assert call_kwargs["event_types"] == {BroadcastEventType.TEST_COMPLETED}
    assert call_kwargs["tags"] == {"python", "fast"}
    assert call_kwargs["priority_filter"] is None

    # --- Cleanup ---
    service.unsubscribe.assert_awaited_once_with("sub-1")

    # --- Validate EVERY send_json call ---
    sent = [call.args[0] for call in ws.send_json.await_args_list]
    assert len(sent) == 8

    # Message 0: subscription_established
    assert sent[0]["type"] == "subscription_established"
    assert sent[0]["subscription_id"] == "sub-1"
    assert sent[0]["container_name"] == "c1"
    assert sent[0]["event_types"] == ["test_completed"]
    assert set(sent[0]["tags"]) == {"python", "fast"}
    assert sent[0]["priority"] is None

    # Message 1: history_replayed (initial replay with 2 messages)
    assert sent[1] == {
        "type": "history_replayed",
        "messages_replayed": 2,
    }

    # Message 2: error response to "subscribe" (dynamic updates not supported)
    assert sent[2]["type"] == "error"
    assert "Dynamic subscription updates not yet supported" in sent[2]["error"]

    # Message 3: history_replayed from in-loop replay (1 message replayed)
    assert sent[3] == {
        "type": "history_replayed",
        "messages_replayed": 1,
    }

    # Message 4: error for replay with invalid event type "bad"
    assert sent[4] == {
        "type": "error",
        "error": "Invalid event type in replay request",
    }

    # Message 5: status_update with full subscription details
    assert sent[5]["type"] == "status_update"
    assert sent[5]["subscription_id"] == "sub-1"
    assert sent[5]["container_name"] == "c1"
    assert sent[5]["message_count"] == 5
    assert sent[5]["event_types"] == ["test_completed"]
    assert sent[5]["tags"] == ["python"]
    assert sent[5]["priority"] == "high"
    assert sent[5]["last_message_at"] == "2025-01-01T00:00:00+00:00"

    # Message 6: error for get_status when subscription is not found
    assert sent[6] == {
        "type": "error",
        "error": "Subscription not found",
    }

    # Message 7: error for unknown message type includes the type name
    assert sent[7] == {
        "type": "error",
        "error": "Unknown message type: funky_chicken",
    }

    # --- Verify replay_history call parameters ---
    replay_calls = service.replay_history.await_args_list
    assert len(replay_calls) == 2
    # Initial replay: only subscription_id and limit
    assert replay_calls[0].kwargs["subscription_id"] == "sub-1"
    assert replay_calls[0].kwargs["limit"] == 5
    # In-loop replay: subscription_id, event_types filter, limit
    assert replay_calls[1].kwargs["subscription_id"] == "sub-1"
    assert replay_calls[1].kwargs["event_types"] == {BroadcastEventType.TEST_COMPLETED}
    assert replay_calls[1].kwargs["limit"] == 2


# ===================================================================
# 7. Tenant from header instead of query param
# ===================================================================


async def test_broadcast_websocket_tenant_from_header():
    """Tenant can be provided via X-Tenant-Id header instead of query param."""
    ws = _websocket(
        query_params={"container_name": "c1"},
        headers=ws_licence_headers("t-header"),
    )

    service = AsyncMock()
    service.subscribe.return_value = "sub-99"
    ws.receive_text.side_effect = [WebSocketDisconnect()]

    extract_patch, validate_patch = _patch_container_ok()
    with (
        extract_patch,
        validate_patch,
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-h", **_DEFAULTS)
    ws.accept.assert_awaited_once()
    # subscription_established is the only message (no replay, immediate disconnect)
    sent = [call.args[0] for call in ws.send_json.await_args_list]
    assert len(sent) == 1
    assert sent[0]["type"] == "subscription_established"
    service.unsubscribe.assert_awaited_once_with("sub-99")


# ===================================================================
# 8. No replay when replay_history=False (default)
# ===================================================================


async def test_broadcast_websocket_no_replay_by_default():
    """When replay_history is False (default), no history_replayed message is sent."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    service = AsyncMock()
    service.subscribe.return_value = "sub-2"
    ws.receive_text.side_effect = [WebSocketDisconnect()]

    extract_patch, validate_patch = _patch_container_ok()
    with (
        extract_patch,
        validate_patch,
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-2", **_DEFAULTS)
    service.replay_history.assert_not_awaited()
    sent = [call.args[0] for call in ws.send_json.await_args_list]
    assert len(sent) == 1
    assert sent[0]["type"] == "subscription_established"


# ===================================================================
# 9. Replay with tags filter in the message loop
# ===================================================================


async def test_broadcast_websocket_replay_with_tags_filter():
    """In-loop replay_history message with tags passes the tags filter to the service."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    service = AsyncMock()
    service.subscribe.return_value = "sub-3"
    service.replay_history.return_value = 7

    ws.receive_text.side_effect = [
        json.dumps({"type": "replay_history", "tags": ["security", "critical"]}),
        WebSocketDisconnect(),
    ]

    extract_patch, validate_patch = _patch_container_ok()
    with (
        extract_patch,
        validate_patch,
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-3", **_DEFAULTS)
    replay_kwargs = service.replay_history.await_args.kwargs
    assert replay_kwargs["tags"] == {"security", "critical"}
    assert replay_kwargs["limit"] == 100  # default from message.get("limit", 100)

    sent = [call.args[0] for call in ws.send_json.await_args_list]
    # subscription_established + history_replayed
    assert len(sent) == 2
    assert sent[1] == {"type": "history_replayed", "messages_replayed": 7}


# ===================================================================
# 10. Generic exception in message loop sends error JSON
# ===================================================================


async def test_broadcast_websocket_loop_exception_sends_error():
    """A non-disconnect exception in the loop sends an error message to the client."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    service = AsyncMock()
    service.subscribe.return_value = "sub-err"

    ws.receive_text.side_effect = [
        ValueError("json decode failed"),
    ]

    extract_patch, validate_patch = _patch_container_ok()
    with (
        extract_patch,
        validate_patch,
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-err", **_DEFAULTS)
    sent = [call.args[0] for call in ws.send_json.await_args_list]
    # subscription_established + WebSocket error
    assert len(sent) == 2
    assert sent[1]["type"] == "error"
    assert "json decode failed" in sent[1]["error"]
    # Cleanup still happens
    service.unsubscribe.assert_awaited_once_with("sub-err")


# ===================================================================
# 11. Filters with no event_types or tags (subscribe to everything)
# ===================================================================


async def test_broadcast_websocket_no_filters_subscribes_to_all():
    """When no event_types/tags/priority given, subscribe is called with None for all filters."""
    ws = _websocket(query_params={"container_name": "c1"}, headers=ws_licence_headers("t1"))

    service = AsyncMock()
    service.subscribe.return_value = "sub-all"
    ws.receive_text.side_effect = [WebSocketDisconnect()]

    extract_patch, validate_patch = _patch_container_ok()
    with (
        extract_patch,
        validate_patch,
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        await websocket_broadcast_subscribe(ws, connection_id="conn-all", **_DEFAULTS)
    call_kwargs = service.subscribe.await_args.kwargs
    assert call_kwargs["event_types"] is None
    assert call_kwargs["tags"] is None
    assert call_kwargs["priority_filter"] is None

    sent = [call.args[0] for call in ws.send_json.await_args_list]
    assert sent[0]["event_types"] == []
    assert sent[0]["tags"] == []
    assert sent[0]["priority"] is None
