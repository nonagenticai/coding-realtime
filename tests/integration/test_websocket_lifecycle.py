"""Integration tests for WebSocket endpoints — exercises real ASGI app.

Tests the full broadcast WebSocket lifecycle: connection, subscription
establishment, message exchange, status queries, error handling, and
disconnection cleanup.  Uses Starlette's synchronous TestClient for
WebSocket testing (httpx.AsyncClient does not support WebSocket).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.websockets import WebSocketDisconnect

from tests.conftest import ws_licence_headers

# NOT marked `integration` — and it must not be. Everything here drives the real
# ASGI app through Starlette's TestClient with Redis, container validation and
# the broadcast service mocked; it needs no running service. The marker kept
# the whole file out of CI (`-m 'not integration'` in pyproject addopts), and
# while it was out, coding-client 1.15.0 made uncredentialed handshakes a 1008
# refusal: 11 of these 12 tests went red and nobody saw it for weeks.
#
# `ws_licence` makes the handshake credentialled exactly as coding-gateway's
# is in production (a licence introspected to TENANT), via the same helper the
# unit suites use. It is NOT autouse — see tests/conftest.py for why.
pytestmark = pytest.mark.usefixtures("ws_licence")

TENANT = "test-tenant"
HEADERS = ws_licence_headers(TENANT)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_container_validation():
    """Mock container extraction and validation to always succeed."""
    with (
        patch(
            "routers.broadcast_websocket.extract_container_from_websocket",
            new_callable=AsyncMock,
            return_value="test-container",
        ),
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new_callable=AsyncMock,
            return_value={"name": "test-container", "status": "running"},
        ) as validate,
    ):
        yield validate


@pytest.fixture
def mock_broadcast_service():
    """Mock the broadcast service for subscription tests."""
    mock_service = MagicMock()
    mock_service.subscribe = AsyncMock(return_value="sub-test-123")
    mock_service.unsubscribe = AsyncMock()
    mock_service.get_history = AsyncMock(return_value=[])
    mock_service.replay_history = AsyncMock(return_value=0)
    mock_service.get_subscription = AsyncMock(return_value=None)
    with patch(
        "routers.broadcast_websocket.get_broadcast_service",
        return_value=mock_service,
    ):
        yield mock_service


@pytest.fixture
def ws_client(mock_settings, mock_redis):
    """Starlette synchronous TestClient for WebSocket testing."""
    with patch("main._redis", mock_redis):
        from starlette.testclient import TestClient

        from main import app

        with TestClient(app) as client:
            yield client


# No `&tenant_id=`: the query parameter has never been read for identity (see
# test_ws_identity.test_no_router_reads_tenant_id_from_query_params); the
# tenant comes from the licence in HEADERS.
WS_URL = "/api/broadcast/subscribe?connection_id=test-conn-1&container_name=test-container"


# ===================================================================
# Connection establishment
# ===================================================================


def test_broadcast_ws_connects_successfully(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """WebSocket connection to /api/broadcast/subscribe succeeds and sends subscription_established."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        msg = ws.receive_json()
        assert msg["type"] == "subscription_established"


def test_broadcast_ws_subscription_id_returned(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """The subscription_established message includes the subscription_id from the service."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        msg = ws.receive_json()
        assert msg["subscription_id"] == "sub-test-123"


def test_broadcast_ws_container_name_in_established(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """subscription_established includes the correct container_name."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        msg = ws.receive_json()
        assert msg["container_name"] == "test-container"


def test_broadcast_ws_established_schema(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """subscription_established contains all expected fields with correct types."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        msg = ws.receive_json()
        assert set(msg.keys()) == {
            "type",
            "subscription_id",
            "container_name",
            "event_types",
            "tags",
            "priority",
        }
        assert isinstance(msg["event_types"], list)
        assert isinstance(msg["tags"], list)
        # No filter applied -> priority is None
        assert msg["priority"] is None


# ===================================================================
# Message exchange — unknown message type
# ===================================================================


def test_broadcast_ws_unknown_message_type_returns_error(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """Sending an unknown message type returns a typed error response."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        # Consume the subscription_established message
        ws.receive_json()

        ws.send_text(json.dumps({"type": "totally_bogus"}))
        err = ws.receive_json()
        assert err["type"] == "error"
        assert "totally_bogus" in err["error"]


# ===================================================================
# Message exchange — get_status
# ===================================================================


def test_broadcast_ws_get_status_when_no_subscription_found(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """get_status returns an error when the service cannot find the subscription."""
    # mock_broadcast_service.get_subscription returns None by default
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        ws.receive_json()  # subscription_established

        ws.send_text(json.dumps({"type": "get_status"}))
        resp = ws.receive_json()
        assert resp["type"] == "error"
        assert "not found" in resp["error"].lower()


def test_broadcast_ws_get_status_returns_details(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """get_status returns full status when subscription exists."""
    sub = MagicMock()
    sub.subscription_id = "sub-test-123"
    sub.container_name = "test-container"
    sub.message_count = 7
    sub.event_types = None
    sub.tags = None
    sub.priority_filter = None
    sub.last_message_at = None
    mock_broadcast_service.get_subscription = AsyncMock(return_value=sub)

    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        ws.receive_json()  # subscription_established

        ws.send_text(json.dumps({"type": "get_status"}))
        resp = ws.receive_json()
        assert resp["type"] == "status_update"
        assert resp["subscription_id"] == "sub-test-123"
        assert resp["container_name"] == "test-container"
        assert resp["message_count"] == 7
        assert isinstance(resp["event_types"], list)
        assert isinstance(resp["tags"], list)


# ===================================================================
# Message exchange — replay_history
# ===================================================================


def test_broadcast_ws_replay_history(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """replay_history request triggers replay and returns the replayed count."""
    mock_broadcast_service.replay_history = AsyncMock(return_value=3)

    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        ws.receive_json()  # subscription_established

        ws.send_text(json.dumps({"type": "replay_history", "limit": 50}))
        resp = ws.receive_json()
        assert resp["type"] == "history_replayed"
        assert resp["messages_replayed"] == 3


# ===================================================================
# Message exchange — subscribe (dynamic update not supported)
# ===================================================================


def test_broadcast_ws_subscribe_update_not_supported(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """Dynamic subscribe updates return an error because they are not yet supported."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        ws.receive_json()  # subscription_established

        ws.send_text(json.dumps({"type": "subscribe", "event_types": ["foo"]}))
        resp = ws.receive_json()
        assert resp["type"] == "error"
        assert "not yet supported" in resp["error"].lower()


# ===================================================================
# Disconnection cleanup
# ===================================================================


def test_broadcast_ws_unsubscribe_called_on_disconnect(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """When the client disconnects, unsubscribe is called with the subscription_id."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        ws.receive_json()  # subscription_established
    # After the context manager exits (disconnect), unsubscribe should have been called
    mock_broadcast_service.unsubscribe.assert_awaited_once_with("sub-test-123")


# ===================================================================
# Identity: the tenant is the licence's, and nothing else buys one
# ===================================================================


def test_broadcast_ws_validates_the_container_for_the_licence_tenant(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """Container ownership is checked against the tenant the LICENCE names."""
    with ws_client.websocket_connect(WS_URL, headers=HEADERS) as ws:
        ws.receive_json()
    mock_container_validation.assert_awaited_once_with("test-container", TENANT)


@pytest.mark.parametrize(
    ("url", "headers"),
    [
        pytest.param(WS_URL, {}, id="no-credential"),
        pytest.param(WS_URL + "&tenant_id=test-tenant", {}, id="query-tenant-only"),
        pytest.param(WS_URL, {"X-Tenant-Id": TENANT}, id="bare-header-only"),
    ],
)
def test_broadcast_ws_uncredentialed_handshake_is_refused(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
    url,
    headers,
):
    """No licence => closed 1008 before anything is served.

    Replaces `test_broadcast_ws_rejected_without_tenant`, which asserted
    `pytest.raises(Exception)` around a handshake with no tenant_id query param:
    ANY failure passed it, including the ones every other test in this file was
    hitting. Since coding-client 1.15.0 the rule is stricter than "a tenant must
    be named" — a named tenant (query OR bare header) without a licence is
    refused too, so both are pinned here, with the close code.
    """
    with pytest.raises(WebSocketDisconnect) as excinfo:
        with ws_client.websocket_connect(url, headers=headers) as ws:
            ws.receive_json()
    assert excinfo.value.code == 1008
    mock_broadcast_service.subscribe.assert_not_awaited()


# ===================================================================
# History replay on connect
# ===================================================================


def test_broadcast_ws_replay_on_connect(
    ws_client,
    mock_container_validation,
    mock_broadcast_service,
):
    """Setting replay_history=true replays history immediately after subscription."""
    mock_broadcast_service.replay_history = AsyncMock(return_value=5)

    url_with_replay = WS_URL + "&replay_history=true&replay_limit=25"
    with ws_client.websocket_connect(url_with_replay, headers=HEADERS) as ws:
        established = ws.receive_json()
        assert established["type"] == "subscription_established"

        replay = ws.receive_json()
        assert replay["type"] == "history_replayed"
        assert replay["messages_replayed"] == 5

    mock_broadcast_service.replay_history.assert_awaited_once_with(
        subscription_id="sub-test-123",
        limit=25,
    )
