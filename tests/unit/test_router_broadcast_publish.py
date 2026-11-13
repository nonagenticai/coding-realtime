"""Tests for the HTTP broadcast publish endpoint (POST /api/broadcast/publish).

This is the REST 'send' half of the broadcast pub/sub (D9): previously only the
WebSocket /subscribe route existed, so callers POSTing a broadcast hit a
WS-only path (404) and the event was silently dropped. These tests pin the
publish contract: tenant-scoped (BOLA), event/priority validated, container
resolvable from the body or from data.container_name, and `delivered` DERIVED
from the live subscriber count (a zero-subscriber publish is an observable
no-op, not a fake success).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from main import app


@pytest.fixture
def tenant_override():
    from coding_client.auth import require_tenant

    app.dependency_overrides[require_tenant] = lambda: "test-tenant"
    yield
    app.dependency_overrides.pop(require_tenant, None)


def _mock_service(subscriber_count: int):
    """A broadcast service whose broadcast() returns a message with a fixed id
    and whose count_reachable_subscribers() reports `subscriber_count` subs.

    Note the counting method is the CROSS-REPLICA one: the endpoint must not go
    back to the replica-local subscription map (see
    test_reachable_count_is_used_not_the_replica_local_one).
    """
    service = MagicMock()
    msg = MagicMock()
    msg.message_id = "msg-abc123"
    service.broadcast = AsyncMock(return_value=msg)
    service.count_reachable_subscribers = AsyncMock(return_value=subscriber_count)
    service.get_container_subscriptions = AsyncMock(return_value=[])
    return service


async def _post(body: dict):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        return await ac.post("/api/broadcast/publish", json=body)


async def test_publish_delivers_when_subscribers_present(tenant_override):
    service = _mock_service(subscriber_count=2)
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(return_value={"container_name": "c1"}),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        resp = await _post(
            {
                "event_type": "container_started",
                "data": {"container_name": "c1", "status": "running"},
                "priority": "high",
            }
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["message_id"] == "msg-abc123"
    assert data["container_name"] == "c1"
    assert data["event_type"] == "container_started"
    assert data["subscriber_count"] == 2
    assert data["delivered"] is True
    # broadcast() was called with the resolved container + payload.
    service.broadcast.assert_awaited_once()
    kwargs = service.broadcast.await_args.kwargs
    assert kwargs["container_name"] == "c1"
    assert kwargs["payload"] == {"container_name": "c1", "status": "running"}


async def test_publish_with_zero_subscribers_is_an_honest_no_op(tenant_override):
    """The whole point of D9: a publish with no subscribers must report
    delivered=False + subscriber_count=0 — NOT a blind success."""
    service = _mock_service(subscriber_count=0)
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(return_value={"container_name": "c1"}),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        resp = await _post({"event_type": "container_stopped", "data": {"container_name": "c1"}})

    assert resp.status_code == 200
    data = resp.json()
    assert data["subscriber_count"] == 0
    assert data["delivered"] is False


async def test_reachable_count_is_used_not_the_replica_local_one(tenant_override):
    """THE 2-replica case: the subscriber is attached to the OTHER replica, so
    this replica's local subscription map is empty — but the redis fan-out did
    deliver. The endpoint must report the cross-replica count.

    RED before the presence set: the endpoint called
    get_container_subscriptions() (local, empty here) and answered
    subscriber_count=0 / delivered=false for a message the client received.
    """
    service = _mock_service(subscriber_count=1)
    service.get_container_subscriptions = AsyncMock(return_value=[])  # nothing local
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(return_value={"container_name": "c1"}),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        resp = await _post({"event_type": "container_started", "data": {"container_name": "c1"}})

    assert resp.status_code == 200
    data = resp.json()
    assert data["subscriber_count"] == 1
    assert data["delivered"] is True
    # It counted against the message that was actually published, so the count
    # can honour that message's filters.
    service.count_reachable_subscribers.assert_awaited_once()
    assert service.count_reachable_subscribers.await_args[0][0] is service.broadcast.return_value
    service.get_container_subscriptions.assert_not_awaited()


async def test_container_resolved_from_top_level_field(tenant_override):
    service = _mock_service(subscriber_count=1)
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(return_value={"container_name": "top"}),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        resp = await _post({"event_type": "container_health", "container_name": "top", "data": {}})
    assert resp.status_code == 200
    assert resp.json()["container_name"] == "top"


async def test_missing_container_is_422(tenant_override):
    with patch("routers.broadcast_websocket.get_broadcast_service", return_value=_mock_service(0)):
        resp = await _post({"event_type": "container_started", "data": {}})
    assert resp.status_code == 422
    assert "container_name is required" in resp.json()["error"]["message"]


async def test_invalid_event_type_is_422(tenant_override):
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(return_value={}),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=_mock_service(1)),
    ):
        resp = await _post({"event_type": "not_a_real_event", "data": {"container_name": "c1"}})
    assert resp.status_code == 422
    assert "Invalid event_type" in resp.json()["error"]["message"]


async def test_invalid_priority_is_422(tenant_override):
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(return_value={}),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=_mock_service(1)),
    ):
        resp = await _post(
            {
                "event_type": "container_started",
                "priority": "URGENT",
                "data": {"container_name": "c1"},
            }
        )
    assert resp.status_code == 422
    assert "Invalid priority" in resp.json()["error"]["message"]


async def test_wrong_tenant_is_404_bola(tenant_override):
    """A container the caller does not own → 404 (validate raises), and we never
    reach broadcast()."""
    service = _mock_service(1)
    with (
        patch(
            "routers.broadcast_websocket.validate_and_get_container_for_tenant",
            new=AsyncMock(
                side_effect=HTTPException(status_code=404, detail="Container 'c1' not found")
            ),
        ),
        patch("routers.broadcast_websocket.get_broadcast_service", return_value=service),
    ):
        resp = await _post({"event_type": "container_started", "data": {"container_name": "c1"}})
    assert resp.status_code == 404
    service.broadcast.assert_not_awaited()
