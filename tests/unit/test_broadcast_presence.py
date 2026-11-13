"""Cross-replica subscriber PRESENCE tests.

The redis pub/sub fan-out (D2) already delivered a broadcast to subscribers on
every replica, but ``POST /api/broadcast/publish`` still derived
``subscriber_count`` / ``delivered`` from the publishing replica's in-process
subscription map. At 2 replicas a publish landing on A while the subscriber is
connected to B answered ``subscriber_count: 0, delivered: false`` for a message
the client HAD received — inverting the honesty property the endpoint exists to
provide, on roughly half of all publishes.

These tests pin the shared redis presence set that fixes it:
  * a subscriber on another replica is counted (the blocker itself);
  * the count is filter-aware, so "someone is attached" is never mistaken for
    "someone received it";
  * staleness is handled in BOTH directions — an expired entry from a replica
    that died without unsubscribing is not counted (and is pruned), while a
    live subscription is kept alive by the heartbeat;
  * a half-open websocket (no clean disconnect) stops being counted;
  * redis being absent or broken degrades to this replica's own count instead
    of reporting a false zero.
"""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest
from fastapi.websockets import WebSocketState

from services.websocket_broadcast import (
    BroadcastEventType,
    BroadcastMessage,
    BroadcastPriority,
    WebSocketBroadcastService,
)


class _FakeRedisHash:
    """In-memory stand-in for the redis hash commands presence uses.

    Shared by two service instances so they see the 'same redis', exactly like
    two replicas do. Mirrors ``decode_responses=True`` (str in, str out).
    """

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.expires: dict[str, int] = {}

    async def hset(self, key: str, field: str, value: str) -> int:
        bucket = self.hashes.setdefault(key, {})
        is_new = field not in bucket
        bucket[field] = value
        return 1 if is_new else 0

    async def hdel(self, key: str, *fields: str) -> int:
        bucket = self.hashes.get(key, {})
        removed = 0
        for field in fields:
            if bucket.pop(field, None) is not None:
                removed += 1
        return removed

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def expire(self, key: str, seconds: int) -> bool:
        self.expires[key] = seconds
        return True


def _ws(state: WebSocketState = WebSocketState.CONNECTED) -> AsyncMock:
    ws = AsyncMock()
    ws.client_state = state
    ws.send_json = AsyncMock()
    return ws


def _message(
    container: str = "c1",
    event_type: BroadcastEventType = BroadcastEventType.CONTAINER_STARTED,
    priority: BroadcastPriority = BroadcastPriority.NORMAL,
    tags: list[str] | None = None,
) -> BroadcastMessage:
    return BroadcastMessage(
        message_id="m1",
        event_type=event_type,
        container_name=container,
        priority=priority,
        payload={},
        tags=tags or [],
    )


# ── The blocker: presence must cross replicas ─────────────────────────────


async def test_subscriber_on_another_replica_is_counted():
    """RED before the presence set: replica B has no local subscription for c1,
    so it reported 0 subscribers / delivered=false for a message that replica
    A's subscriber genuinely received via the redis fan-out."""
    hub = _FakeRedisHash()

    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    await svc_a.subscribe("conn-a", "c1", _ws())

    svc_b = WebSocketBroadcastService()  # the OTHER replica: nothing local
    svc_b.attach_redis(hub)
    assert svc_b._instance_id != svc_a._instance_id
    assert await svc_b.count_local_matching_subscribers(_message()) == 0

    assert await svc_b.count_reachable_subscribers(_message()) == 1


async def test_both_replicas_agree_on_the_count():
    """The publisher's answer must not depend on which replica served it."""
    hub = _FakeRedisHash()
    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)

    await svc_a.subscribe("conn-a", "c1", _ws())
    await svc_b.subscribe("conn-b", "c1", _ws())

    message = _message()
    assert await svc_a.count_reachable_subscribers(message) == 2
    assert await svc_b.count_reachable_subscribers(message) == 2


async def test_unsubscribe_removes_presence_for_the_other_replica():
    hub = _FakeRedisHash()
    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)

    sub_id = await svc_a.subscribe("conn-a", "c1", _ws())
    assert await svc_b.count_reachable_subscribers(_message()) == 1

    await svc_a.unsubscribe(sub_id)
    assert await svc_b.count_reachable_subscribers(_message()) == 0


async def test_zero_subscribers_anywhere_is_still_an_honest_zero():
    """The presence set must not turn every publish into delivered=true."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    assert await svc.count_reachable_subscribers(_message()) == 0


# ── Filter awareness: "attached" is not "received" ────────────────────────


async def test_remote_subscriber_filtered_out_is_not_counted():
    """A subscriber on another replica that filtered this event type out did
    NOT receive the message, so it must not make delivered=true."""
    hub = _FakeRedisHash()
    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    await svc_a.subscribe("conn-a", "c1", _ws(), event_types={BroadcastEventType.TEST_COMPLETED})

    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)

    # Publishing a different event type: nobody receives it.
    assert (
        await svc_b.count_reachable_subscribers(
            _message(event_type=BroadcastEventType.BUILD_FAILED)
        )
        == 0
    )
    # Publishing the subscribed type: counted.
    assert (
        await svc_b.count_reachable_subscribers(
            _message(event_type=BroadcastEventType.TEST_COMPLETED)
        )
        == 1
    )


async def test_remote_priority_and_tag_filters_are_honoured():
    hub = _FakeRedisHash()
    svc_a = WebSocketBroadcastService()
    svc_a.attach_redis(hub)
    await svc_a.subscribe(
        "conn-a", "c1", _ws(), priority_filter=BroadcastPriority.HIGH, tags={"python"}
    )

    svc_b = WebSocketBroadcastService()
    svc_b.attach_redis(hub)

    # Too low a priority.
    assert (
        await svc_b.count_reachable_subscribers(
            _message(priority=BroadcastPriority.LOW, tags=["python"])
        )
        == 0
    )
    # Right priority, wrong tag.
    assert (
        await svc_b.count_reachable_subscribers(
            _message(priority=BroadcastPriority.CRITICAL, tags=["java"])
        )
        == 0
    )
    # Both satisfied.
    assert (
        await svc_b.count_reachable_subscribers(
            _message(priority=BroadcastPriority.CRITICAL, tags=["python"])
        )
        == 1
    )


async def test_unknown_remote_event_type_filter_does_not_become_match_all():
    """Version skew: an entry whose filters name an event type this replica does
    not know must be treated as 'matches nothing', never as an empty filter
    (which means 'all' and would over-report)."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    key = svc._presence_key("c1")
    hub.hashes[key] = {
        "sub-from-newer-replica": (
            '{"expires_at": %f, "event_types": ["some_future_event"], '
            '"tags": [], "priority": null, "instance": "other"}' % (time.time() + 60)
        )
    }
    assert await svc.count_reachable_subscribers(_message()) == 0


# ── Staleness: the lie in the other direction ─────────────────────────────


async def test_expired_presence_entry_is_not_counted_and_is_pruned():
    """A replica killed without clean unsubscribes (SIGKILL, node loss) leaves
    entries behind. Counting them forever would re-introduce the lie in the
    other direction: delivered=true with nobody listening."""
    hub = _FakeRedisHash()
    dead = WebSocketBroadcastService(presence_ttl_seconds=0)  # entry expires immediately
    dead.attach_redis(hub)
    await dead.subscribe("conn-dead", "c1", _ws())

    key = dead._presence_key("c1")
    assert len(hub.hashes[key]) == 1, "precondition: the entry was written"

    survivor = WebSocketBroadcastService()
    survivor.attach_redis(hub)
    await asyncio.sleep(0.01)

    assert await survivor.count_reachable_subscribers(_message()) == 0
    assert hub.hashes[key] == {}, "stale entry must be pruned, not left to accumulate"


async def test_heartbeat_keeps_a_live_subscription_counted():
    """The flip side: a long-lived websocket must not expire out of the set."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService(presence_ttl_seconds=1, presence_heartbeat_seconds=1)
    svc.attach_redis(hub)
    await svc.subscribe("conn", "c1", _ws())

    other = WebSocketBroadcastService()
    other.attach_redis(hub)

    # Simulate the heartbeat firing after the original stamp.
    await asyncio.sleep(0.02)
    refreshed = await svc.refresh_presence()
    assert refreshed == 1
    assert await other.count_reachable_subscribers(_message()) == 1


async def test_heartbeat_drops_a_half_open_socket():
    """A websocket that died without a clean disconnect never raises
    WebSocketDisconnect, so the router's cleanup never runs. The heartbeat only
    refreshes CONNECTED sockets, so such a subscriber ages out."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    ws = _ws()
    await svc.subscribe("conn", "c1", ws)

    other = WebSocketBroadcastService()
    other.attach_redis(hub)
    assert await other.count_reachable_subscribers(_message()) == 1

    # The peer vanished; starlette flips client_state without any exception.
    ws.client_state = WebSocketState.DISCONNECTED
    await svc.refresh_presence()

    assert await other.count_reachable_subscribers(_message()) == 0
    assert await svc.count_local_matching_subscribers(_message()) == 0


async def test_purge_local_presence_clears_this_replicas_entries():
    """Graceful shutdown must not leave the surviving replica over-reporting for
    a whole TTL window."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    await svc.subscribe("conn", "c1", _ws())

    other = WebSocketBroadcastService()
    other.attach_redis(hub)
    assert await other.count_reachable_subscribers(_message()) == 1

    await svc.purge_local_presence()
    assert await other.count_reachable_subscribers(_message()) == 0


async def test_malformed_presence_entry_is_pruned_not_counted():
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    key = svc._presence_key("c1")
    hub.hashes[key] = {"bad": "not-json", "also-bad": '"a string, not an object"'}

    assert await svc.count_reachable_subscribers(_message()) == 0
    assert hub.hashes[key] == {}


# ── Degradation: never report a false zero ────────────────────────────────


async def test_without_redis_the_local_count_is_used():
    """1 replica, no redis: local counting is exactly correct."""
    svc = WebSocketBroadcastService()  # no attach_redis
    await svc.subscribe("conn", "c1", _ws())
    assert await svc.count_reachable_subscribers(_message()) == 1


async def test_redis_failure_falls_back_to_local_count():
    """A broken redis must not turn a provably-delivered publish into
    delivered=false."""
    hub = AsyncMock()
    hub.hset = AsyncMock(side_effect=RuntimeError("redis down"))
    hub.expire = AsyncMock(side_effect=RuntimeError("redis down"))
    hub.hgetall = AsyncMock(side_effect=RuntimeError("redis down"))
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    await svc.subscribe("conn", "c1", _ws())  # registration fails, must not raise

    assert await svc.count_reachable_subscribers(_message()) == 1


async def test_local_subscriber_survives_a_missing_presence_entry():
    """max(shared, local), not 'trust redis': if our own registration was lost
    (redis blip) the locally-connected subscriber is still counted."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    await svc.subscribe("conn", "c1", _ws())
    hub.hashes[svc._presence_key("c1")] = {}  # entry lost

    assert await svc.count_reachable_subscribers(_message()) == 1


async def test_disconnected_local_socket_is_not_counted_locally():
    svc = WebSocketBroadcastService()
    ws = _ws(WebSocketState.DISCONNECTED)
    await svc.subscribe("conn", "c1", ws)
    assert await svc.count_reachable_subscribers(_message()) == 0


# ── Heartbeat task lifecycle ──────────────────────────────────────────────


async def test_heartbeat_task_starts_and_stops():
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService(presence_heartbeat_seconds=1)
    svc.attach_redis(hub)
    await svc.start_presence_heartbeat()
    assert svc._presence_task is not None
    # Idempotent.
    task = svc._presence_task
    await svc.start_presence_heartbeat()
    assert svc._presence_task is task
    await svc.stop_presence_heartbeat()
    assert svc._presence_task is None


async def test_heartbeat_is_noop_without_redis():
    svc = WebSocketBroadcastService()
    await svc.start_presence_heartbeat()
    assert svc._presence_task is None
    await svc.stop_presence_heartbeat()  # safe even when never started
    assert await svc.refresh_presence() == 0
    await svc.purge_local_presence()  # safe


async def test_heartbeat_loop_survives_a_failing_iteration():
    """A transient redis error must not kill the heartbeat task — otherwise
    presence silently expires and every publish starts reporting 0."""
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService(presence_heartbeat_seconds=0)
    svc.attach_redis(hub)
    calls: list[int] = []

    async def flaky() -> int:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return 0

    # `setattr` rather than a bare assignment: ty reads `svc.refresh_presence =`
    # as implicit shadowing of the bound method and errors, and the mypy-style
    # `# type: ignore[method-assign]` does not silence it.
    setattr(svc, "refresh_presence", flaky)  # noqa: B010
    await svc.start_presence_heartbeat()
    for _ in range(50):
        if len(calls) >= 3:
            break
        await asyncio.sleep(0.01)
    await svc.stop_presence_heartbeat()
    assert len(calls) >= 3, "loop must keep running after a failed iteration"


@pytest.mark.parametrize("state", [WebSocketState.CONNECTING, WebSocketState.DISCONNECTED])
async def test_only_connected_sockets_are_registered_by_the_heartbeat(state):
    hub = _FakeRedisHash()
    svc = WebSocketBroadcastService()
    svc.attach_redis(hub)
    ws = _ws(state)
    await svc.subscribe("conn", "c1", ws)
    assert await svc.refresh_presence() == 0
    assert hub.hashes[svc._presence_key("c1")] == {}
