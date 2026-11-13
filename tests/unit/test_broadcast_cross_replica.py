"""Cross-replica broadcast fan-out tests (D2).

In-process delivery only reaches subscribers on the SAME replica. These tests
prove the redis pub/sub fan-out: a broadcast published on one replica reaches a
subscriber connected to ANOTHER replica, without double-delivering a replica's
own publishes, and degrading to local-only delivery when redis is absent.

The proof-test mirrors the spec's D2 acceptance ("scale to 2, subscribe on A,
publish via B, assert delivery on A") with two WebSocketBroadcastService
instances sharing an in-memory fake redis hub.

They also cover the follow-on requirement that made 2 replicas actually safe:
message HISTORY has to converge across replicas too. Live delivery alone is not
enough — ``replay_history`` is the documented reconnect catch-up, and with
no session affinity a reconnecting client can land on the replica that did NOT
publish a message. If only the publisher recorded history, that client would
silently replay a partial set (``history_replayed`` just reports a smaller
number; nothing errors).
"""

import asyncio
from unittest.mock import AsyncMock

from fastapi.websockets import WebSocketState

from services.websocket_broadcast import (
    BroadcastEventType,
    WebSocketBroadcastService,
)


class _FakePubSub:
    """Minimal async pub/sub matching redis.asyncio's pubsub() surface."""

    def __init__(self, hub: "_FakeRedis") -> None:
        self._hub = hub
        self._channels: set[str] = set()
        self._queue: asyncio.Queue = asyncio.Queue()

    async def subscribe(self, channel: str) -> None:
        self._channels.add(channel)
        self._hub._subs.append(self)

    async def listen(self):
        while True:
            yield await self._queue.get()

    async def aclose(self) -> None:
        if self in self._hub._subs:
            self._hub._subs.remove(self)


class _FakeRedis:
    """In-memory stand-in shared by two service instances (the 'same redis')."""

    def __init__(self) -> None:
        self._subs: list[_FakePubSub] = []

    async def publish(self, channel: str, data: str) -> None:
        for ps in list(self._subs):
            if channel in ps._channels:
                await ps._queue.put({"type": "message", "channel": channel, "data": data})

    def pubsub(self) -> _FakePubSub:
        return _FakePubSub(self)


def _ws() -> AsyncMock:
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    return ws


async def _wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = 50
    for _ in range(deadline):
        if predicate():
            return
        await asyncio.sleep(timeout / deadline)


async def test_cross_replica_fanout_delivers_to_other_replica():
    """Subscribe on replica A; publish via replica B; A's subscriber receives it."""
    hub = _FakeRedis()

    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    ws = _ws()
    await svc_a.subscribe("conn-a", "c1", ws)
    await svc_a.start_subscriber()
    # Let A's subscriber register on the hub before B publishes.
    await _wait_until(lambda: len(hub._subs) >= 1)

    svc_b = WebSocketBroadcastService()  # distinct instance_id — the "other replica"
    svc_b.attach_redis(hub)
    assert svc_b._instance_id != svc_a._instance_id
    # B has NO local subscriber for c1 — delivery can only happen via fan-out.
    await svc_b.broadcast("c1", BroadcastEventType.CONTAINER_STARTED, {"status": "running"})

    await _wait_until(lambda: ws.send_json.called)
    ws.send_json.assert_called_once()
    frame = ws.send_json.call_args[0][0]
    assert frame["type"] == "broadcast"
    assert frame["message"]["container_name"] == "c1"
    assert frame["message"]["event_type"] == "container_started"
    assert frame["message"]["payload"] == {"status": "running"}

    await svc_a.stop_subscriber()


async def test_own_publish_is_not_double_delivered():
    """A replica that both publishes and subscribes must deliver its own
    broadcast exactly once (local path), not again via its own redis echo."""
    hub = _FakeRedis()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    ws = _ws()
    await svc.subscribe("conn", "c1", ws)
    await svc.start_subscriber()
    await _wait_until(lambda: len(hub._subs) >= 1)

    await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"f": "x"})
    # Give the subscriber ample time to (wrongly) re-deliver the echo.
    await asyncio.sleep(0.1)
    ws.send_json.assert_called_once()

    await svc.stop_subscriber()


async def test_publish_without_redis_still_delivers_locally():
    """No redis attached → local delivery unaffected (pre-D2 behaviour)."""
    svc = WebSocketBroadcastService()  # no attach_redis
    ws = _ws()
    await svc.subscribe("conn", "c1", ws)
    await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"f": "x"})
    ws.send_json.assert_called_once()


async def test_start_subscriber_is_noop_without_redis():
    svc = WebSocketBroadcastService()
    await svc.start_subscriber()
    assert svc._subscriber_task is None
    # stop is safe even when never started
    await svc.stop_subscriber()


async def test_publish_failure_does_not_break_local_delivery():
    """A redis.publish that raises must not stop the local send."""
    hub = AsyncMock()
    hub.publish = AsyncMock(side_effect=RuntimeError("redis down"))
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    ws = _ws()
    await svc.subscribe("conn", "c1", ws)
    # Should not raise despite the publish failure.
    await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"f": "x"})
    ws.send_json.assert_called_once()


# ── History convergence across replicas ───────────────────────────────────
# Live delivery is not sufficient for 2 replicas; replay has to agree too.


async def test_remote_publish_enters_this_replicas_history():
    """A message published on B must appear in A's history.

    This is the RED case for scaling: before history was recorded on receipt,
    A's history stayed EMPTY while B's had the message, so replay depended on
    which replica the client happened to reach.
    """
    hub = _FakeRedis()
    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    await svc_a.start_subscriber()
    await _wait_until(lambda: len(hub._subs) >= 1)

    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)
    await svc_b.broadcast("c1", BroadcastEventType.BUILD_STARTED, {"n": 1})

    await _wait_until(lambda: len(svc_a._history.get("c1", [])) >= 1)

    history_a = await svc_a.get_history("c1")
    history_b = await svc_b.get_history("c1")
    assert len(history_a) == 1, "remote publish must be recorded in this replica's history"
    assert len(history_b) == 1
    # Same message on both replicas, not two different ones.
    assert history_a[0].message_id == history_b[0].message_id
    assert history_a[0].payload == {"n": 1}

    await svc_a.stop_subscriber()


async def test_replay_returns_remote_published_message_on_other_replica():
    """The end-to-end reconnect case: client connects to A and asks for replay of
    a message that B published. It must receive it."""
    hub = _FakeRedis()
    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    await svc_a.start_subscriber()
    await _wait_until(lambda: len(hub._subs) >= 1)

    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)
    await svc_b.broadcast("c1", BroadcastEventType.TEST_COMPLETED, {"passed": 7})
    await _wait_until(lambda: len(svc_a._history.get("c1", [])) >= 1)

    # Client now (re)connects to replica A and replays.
    ws = _ws()
    sub_id = await svc_a.subscribe("conn-reconnect", "c1", ws)
    replayed = await svc_a.replay_history(sub_id)

    assert replayed == 1, "replay on the non-publishing replica must not silently return 0"
    frame = ws.send_json.call_args[0][0]
    assert frame["type"] == "broadcast_replay"
    assert frame["message"]["event_type"] == "test_completed"
    assert frame["message"]["payload"] == {"passed": 7}

    await svc_a.stop_subscriber()


async def test_history_is_not_double_recorded_for_own_publish():
    """A replica that publishes AND subscribes records the message once, not
    twice (its own redis echo is skipped, and recording is id-idempotent)."""
    hub = _FakeRedis()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    await svc.start_subscriber()
    await _wait_until(lambda: len(hub._subs) >= 1)

    await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"f": "x"})
    await asyncio.sleep(0.1)  # ample time for a wrong second record

    history = await svc.get_history("c1")
    assert len(history) == 1
    stats = await svc.get_stats("c1")
    assert stats.messages_in_history == 1
    # total_broadcasts stays publish-scoped so a fan-out is not counted twice.
    assert stats.total_broadcasts == 1

    await svc.stop_subscriber()


async def test_remote_history_respects_disabled_history():
    """enable_history=False must not start accumulating remote messages."""
    hub = _FakeRedis()
    svc_a = WebSocketBroadcastService(enable_history=False)
    svc_a.attach_redis(hub)
    ws = _ws()
    await svc_a.subscribe("conn", "c1", ws)
    await svc_a.start_subscriber()
    await _wait_until(lambda: len(hub._subs) >= 1)

    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)
    await svc_b.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"f": "x"})

    # Delivery still happens; only history is suppressed.
    await _wait_until(lambda: ws.send_json.called)
    ws.send_json.assert_called_once()
    assert await svc_a.get_history("c1") == []

    await svc_a.stop_subscriber()
