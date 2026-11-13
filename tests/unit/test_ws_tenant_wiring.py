"""Handshake-level proof that the verified WS tenant reaches every connection store.

#54 made the connection list and the stale cleanup filter by the tenant each
connection recorded — but nothing drove a REAL handshake through the route, so
the three places that record that tenant could each be deleted with the suite
still green:

* ``connection_manager.connect(..., tenant_id=tenant_id)`` in the route;
* ``tenant_id=ws_tenant(self.websocket)`` in ``WebSocketContainerContext``;
* the ``_remember_ws_tenant`` call in ``extract_container_from_websocket``.

Deleting any one of them records ``None``, and a ``None`` tenant is fail-closed
everywhere: the caller's own connections silently vanish from their listing and
cleanup. These tests open a real ASGI WebSocket (Starlette ``TestClient``)
through the real route, the real ``WebSocketContainerContext`` and the real
``resolve_ws_tenant`` (licence introspection stubbed by ``ws_licence``), and
assert the tenant each store recorded.

They also pin coding.runtime GAP-3: ``connection_id`` is chosen by the CLIENT,
so two tenants can present the same one. The manager's registry key is
(tenant, connection_id); tenant B reusing tenant A's id must not replace A's
socket, A's tenant record, or disconnect A when B leaves.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from main import app
from routers.realtime._manager import (
    SUPERSEDED_CLOSE_CODE,
    SUPERSEDED_CLOSE_REASON,
    ConnectionManager,
)
from services.websocket_health_monitor import WebSocketHealthMonitor
from tests.conftest import ws_licence_headers

pytestmark = pytest.mark.usefixtures("ws_licence")

TENANT_A = "tenant-a-0001"
TENANT_B = "tenant-b-0002"


@contextmanager
def _live_stores():
    """Fresh, real connection stores wired into the route for one test.

    Container ownership is stubbed to "every container exists and is running" —
    ownership is not what these tests measure; the tenant that reaches the
    stores is.
    """
    cm = ConnectionManager()
    monitor = WebSocketHealthMonitor(ping_interval_seconds=3600)
    registry = MagicMock()
    ids = iter(f"reg-{n}" for n in range(100))
    registry.register_connection = AsyncMock(side_effect=lambda **_: next(ids))
    registry.unregister_connection = AsyncMock(return_value=True)
    owned = AsyncMock(return_value={"status": "running"})
    cache = MagicMock()
    cache.get_stats = AsyncMock(return_value={})
    with (
        patch("routers.realtime.status.get_analysis_cache", return_value=cache),
        patch("routers.realtime.websocket.connection_manager", cm),
        patch("routers.realtime.status.connection_manager", cm),
        patch("routers.realtime.websocket.validate_and_get_container_for_tenant", owned),
        patch("utils.websocket_utils.validate_and_get_container_for_tenant", owned),
        patch(
            "services.websocket_connection_registry.get_websocket_registry",
            return_value=registry,
        ),
        patch("services.websocket_health_monitor.get_health_monitor", return_value=monitor),
        patch("routers.realtime.status.get_health_monitor", return_value=monitor),
    ):
        yield cm, monitor


def _url(connection_id: str, container: str) -> str:
    return f"/api/realtime/analysis?connection_id={connection_id}&container_name={container}"


@contextmanager
def _open(client, connection_id: str, container: str, tenant: str):
    """Open a real WS through the route and consume its ``connection_established``."""
    with client.websocket_connect(
        _url(connection_id, container), headers=ws_licence_headers(tenant)
    ) as ws:
        hello = ws.receive_json()
        assert hello["type"] == "connection_established", hello
        assert hello["connection_id"] == connection_id  # the client's own id
        yield ws


def _receive_within(ws, seconds: float) -> dict:
    """Receive the next server message, failing (not hanging) after *seconds*.

    Starlette's TestClient has no receive timeout; this reads the session's
    own app->client stream (``_send_rx``) under ``anyio.fail_after``.
    """

    async def _receive():
        with anyio.fail_after(seconds):
            return await ws._send_rx.receive()

    return ws.portal.call(_receive)


def _status(ws) -> dict:
    """Round-trip one frame: proves the route reached its receive loop."""
    ws.send_json({"type": "get_status"})
    return ws.receive_json()


def test_handshake_records_the_verified_tenant_in_every_store():
    with _live_stores() as (cm, monitor), TestClient(app) as client:
        with _open(client, "sess-1", "box-a", TENANT_A) as ws:
            reply = _status(ws)
            assert reply["type"] == "status_update", reply

            # ConnectionManager: fails if the route drops tenant_id=tenant_id.
            assert list(cm.connection_tenants.values()) == [TENANT_A]
            assert cm.connections_for_tenant(TENANT_A) != []

            # Health monitor: fails if WebSocketContainerContext drops
            # tenant_id=ws_tenant(...) OR the _remember_ws_tenant call goes.
            assert list(monitor._connection_tenants.values()) == [TENANT_A]


def test_get_status_counts_only_the_callers_connections():
    """The WS ``get_status`` reply used to report EVERY tenant's connection count."""
    with _live_stores() as (cm, _), TestClient(app) as client:
        with (
            _open(client, "a-1", "box-a", TENANT_A) as a1,
            _open(client, "b-1", "box-b", TENANT_B) as b1,
            _open(client, "b-2", "box-b", TENANT_B) as b2,
        ):
            _status(b1)
            _status(b2)
            reply = _status(a1)
            assert reply["connection_id"] == "a-1"
            assert reply["total_connections"] == 1, reply


def test_same_connection_id_from_two_tenants_does_not_collide():
    """GAP-3: tenant B presenting tenant A's connection_id must not take A's slot."""
    with _live_stores() as (cm, _), TestClient(app) as client:
        with _open(client, "shared", "box-a", TENANT_A) as a:
            _status(a)
            with _open(client, "shared", "box-b", TENANT_B) as b:
                _status(b)
                # Both sockets are live, each under its own tenant.
                assert sorted(cm.connection_tenants.values()) == sorted([TENANT_A, TENANT_B])
                assert len(cm.connections_for_tenant(TENANT_A)) == 1
                assert len(cm.connections_for_tenant(TENANT_B)) == 1
                # A's frames still reach A (not B's socket), and carry A's
                # container and A's own client-visible id.
                reply = _status(a)
                assert reply["container_name"] == "box-a"
                assert reply["connection_id"] == "shared"

            # B leaving must not disconnect A.
            assert cm.connections_for_tenant(TENANT_A) != []
            assert _status(a)["container_name"] == "box-a"


async def test_same_tenant_reconnect_with_the_same_id_still_replaces():
    """Same tenant + same id keeps its pre-existing semantics: one slot, newest wins."""
    cm = ConnectionManager()
    first, second = AsyncMock(), AsyncMock()
    with patch(
        "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
    ):
        k1 = await cm.connect(first, "sess", "box-a", tenant_id=TENANT_A)
        k2 = await cm.connect(second, "sess", "box-a", tenant_id=TENANT_A)
    assert k1 == k2
    assert cm.connections_for_tenant(TENANT_A) == [k1]
    assert cm.active_connections[k1] is second
    assert cm.public_connection_id(k1) == "sess"
    # An unscoped (internal/legacy) key never aliases a tenant-scoped one.
    assert cm.connection_key("sess", None) != cm.connection_key("sess", TENANT_A)


async def test_old_socket_cleanup_after_same_tenant_reconnect_keeps_the_new_socket():
    """Manager level: the OLD socket's disconnect must not evict the NEW socket.

    Same tenant + same id reuse one slot. The old socket's route loop ends AFTER
    the new socket registered, and its ``finally`` calls
    ``disconnect(key, old_socket)``. Unconditionally deleting the slot there
    removed the new socket, its tenant record and its monitor id: the client
    vanished from its own listing and every later pong matched nothing.
    """
    cm = ConnectionManager()
    old, new = AsyncMock(), AsyncMock()
    with patch(
        "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
    ):
        k_old = await cm.connect(old, "sess", "box-a", tenant_id=TENANT_A, monitor_id="reg-old")
        k_new = await cm.connect(new, "sess", "box-a", tenant_id=TENANT_A, monitor_id="reg-new")
        assert k_old == k_new

        cm.disconnect(k_old, old)  # the superseded socket's cleanup

        assert cm.connections_for_tenant(TENANT_A) == [k_new]
        assert cm.active_connections[k_new] is new
        assert cm.monitor_connection_id(k_new) == "reg-new"
        assert cm.get_active_connections_for_container("box-a") == [k_new]

        # The new socket's own cleanup still removes the slot.
        cm.disconnect(k_new, new)
        assert cm.connections_for_tenant(TENANT_A) == []
        assert cm.monitor_connection_id(k_new) is None


async def test_replacing_a_socket_tears_down_its_watches_container_and_metrics():
    """With the disconnect guard, connect()'s teardown is the old socket's ONLY cleanup.

    The superseded socket's own ``disconnect(key, old)`` is a no-op, so if
    connect() did not tear the old slot down when replacing it, the old
    socket's file watches and its container-set entry would leak forever, and
    the connections metric would drift (opened 2, closed 1). Reconnecting onto
    a DIFFERENT container makes both leaks visible.
    """
    cm = ConnectionManager()
    metrics = AsyncMock()
    old, new = AsyncMock(), AsyncMock()
    with patch("routers.realtime._manager.get_websocket_metrics_collector", return_value=metrics):
        key = await cm.connect(old, "sess", "box-a", tenant_id=TENANT_A)
        cm.watch_file(key, "/w/f.py")
        assert await cm.connect(new, "sess", "box-b", tenant_id=TENANT_A) == key
        cm.disconnect(key, old)  # the superseded socket's own cleanup: a no-op
        await asyncio.sleep(0)  # let disconnect's metrics task run

        assert "/w/f.py" not in cm.file_watchers
        assert cm.get_active_connections_for_container("box-a") == []
        assert cm.get_active_connections_for_container("box-b") == [key]
        closed = [c.args for c in metrics.track_connection_closed.await_args_list]
        assert [(c[0], c[1]) for c in closed] == [("box-a", key)]
        # ...and the old client is told why it was dropped.
        old.close.assert_awaited_once_with(
            code=SUPERSEDED_CLOSE_CODE, reason=SUPERSEDED_CLOSE_REASON
        )
        new.close.assert_not_awaited()


async def test_reconnect_survives_the_superseded_socket_failing_to_close():
    """A dead transport on the OLD socket must not drop the RECONNECTING client.

    Starlette maps a transport OSError during close into
    ``WebSocketDisconnect(1006)``. connect() has already torn the old slot down
    by then, so letting it escape would leave the new client unregistered.
    """
    cm = ConnectionManager()
    old, new = AsyncMock(), AsyncMock()
    old.close.side_effect = WebSocketDisconnect(code=1006)
    with patch(
        "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
    ):
        key = await cm.connect(old, "sess", "box-a", tenant_id=TENANT_A)
        assert (
            await cm.connect(new, "sess", "box-a", tenant_id=TENANT_A, monitor_id="reg-new") == key
        )

    old.close.assert_awaited_once()
    assert cm.connections_for_tenant(TENANT_A) == [key]
    assert cm.active_connections[key] is new
    assert cm.monitor_connection_id(key) == "reg-new"
    new.send_text.assert_awaited()  # connection_established reached the new client


async def test_send_failure_after_the_slot_was_replaced_keeps_the_new_socket():
    """send_message's error path must only remove the socket it tried to send on.

    The slot is swapped to a new socket while ``await send_text`` is in flight,
    then that send fails. Disconnecting the KEY there would evict the new socket.
    """
    cm = ConnectionManager()
    old, new = AsyncMock(), AsyncMock()

    with patch(
        "routers.realtime._manager.get_websocket_metrics_collector", return_value=AsyncMock()
    ):
        key = await cm.connect(old, "sess", "box-a", tenant_id=TENANT_A)

        async def _swap_then_fail(_text):
            await cm.connect(new, "sess", "box-a", tenant_id=TENANT_A, monitor_id="reg-new")
            raise ConnectionError("peer gone")

        old.send_text.side_effect = _swap_then_fail
        await cm.send_message(key, {"type": "status_update"})

        assert cm.connections_for_tenant(TENANT_A) == [key]
        assert cm.active_connections[key] is new
        assert cm.monitor_connection_id(key) == "reg-new"


def test_real_reconnect_closes_the_superseded_socket_with_4001():
    """The superseded client receives close code 4001 "superseded"; the new one keeps working."""
    with _live_stores() as (cm, _), TestClient(app) as client:
        with _open(client, "sess", "box-a", TENANT_A) as old:
            _status(old)
            with _open(client, "sess", "box-a", TENANT_A) as new:
                # Bounded receive: without the close, a plain receive_json()
                # would block forever instead of failing.
                message = _receive_within(old, seconds=5)
                assert message["type"] == "websocket.close", message
                assert message["code"] == SUPERSEDED_CLOSE_CODE
                assert message["reason"] == SUPERSEDED_CLOSE_REASON

                reply = _status(new)
                assert reply["type"] == "status_update"
                assert reply["total_connections"] == 1
                assert len(cm.connections_for_tenant(TENANT_A)) == 1


def test_real_reconnect_old_socket_closing_leaves_the_new_one_registered_and_ponging():
    """Handshake level: reconnect, close the OLD socket, the NEW one still works."""
    with _live_stores() as (cm, monitor), TestClient(app) as client:
        with _open(client, "sess", "box-a", TENANT_A) as old:
            _status(old)
            [old_monitor_id] = list(monitor._connection_health)
            with _open(client, "sess", "box-a", TENANT_A) as new:
                _status(new)
                new_monitor_id = next(m for m in monitor._connection_health if m != old_monitor_id)
                old.close()
                # The old route's finally + WebSocketContainerContext exit run
                # on the server side; its exit unregisters the old monitor id
                # AFTER the manager disconnect, so wait for that.
                deadline = time.monotonic() + 5
                while old_monitor_id in monitor._connection_health:
                    assert time.monotonic() < deadline, "old socket cleanup never ran"
                    time.sleep(0.01)

                [key] = cm.connections_for_tenant(TENANT_A)
                assert cm.monitor_connection_id(key) == new_monitor_id
                assert _status(new)["total_connections"] == 1

                new.send_json({"type": "pong", "ping_id": "ping-1"})
                assert new.receive_json()["type"] == "pong_acknowledged"
                assert monitor._connection_health[new_monitor_id].total_pongs_received == 1


def test_a_pong_updates_the_health_monitor_entry_of_its_socket():
    """A client pong must reach the monitor under the id the MONITOR tracks.

    The monitor is fed by WebSocketContainerContext with the registry's id
    (here ``reg-0``). The pong handler used to report the ConnectionManager id,
    which the monitor has never heard of: every pong returned False, so
    last_pong_received stayed None, failed_pings never reset, and a live,
    answering client was eventually classed STALE and reaped.
    """
    with _live_stores() as (_, monitor), TestClient(app) as client:
        with _open(client, "sess-1", "box-a", TENANT_A) as ws:
            [monitor_id] = list(monitor._connection_health)
            status = monitor._connection_health[monitor_id]
            assert status.last_pong_received is None
            status.failed_pings = 2

            ws.send_json({"type": "pong", "ping_id": "ping-1"})
            ack = ws.receive_json()
            assert ack["type"] == "pong_acknowledged", ack

            assert status.last_pong_received is not None
            assert status.total_pongs_received == 1
            assert status.failed_pings == 0
