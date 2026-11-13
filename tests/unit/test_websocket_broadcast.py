"""Tests for WebSocketBroadcastService."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.websockets import WebSocketState

from services.websocket_broadcast import (
    BroadcastEventType,
    BroadcastMessage,
    BroadcastPriority,
    BroadcastStats,
    WebSocketBroadcastService,
    cleanup_task_runner,
    get_broadcast_service,
)


@pytest.fixture
def service():
    return WebSocketBroadcastService()


@pytest.fixture
def connected_ws():
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    return ws


async def test_subscribe_returns_subscription_id(service, connected_ws):
    sub_id = await service.subscribe("conn-1", "container-a", connected_ws)
    assert isinstance(sub_id, str)
    assert "container-a" in sub_id


async def test_subscribe_with_event_types(service, connected_ws):
    events = {BroadcastEventType.FILE_CHANGED}
    sub_id = await service.subscribe("conn-1", "c1", connected_ws, event_types=events)
    sub = await service.get_subscription(sub_id)
    assert sub is not None
    assert BroadcastEventType.FILE_CHANGED in sub.event_types


async def test_subscribe_with_tags(service, connected_ws):
    sub_id = await service.subscribe("conn-1", "c1", connected_ws, tags={"python", "test"})
    sub = await service.get_subscription(sub_id)
    assert "python" in sub.tags


async def test_unsubscribe_removes_subscription(service, connected_ws):
    sub_id = await service.subscribe("conn-1", "c1", connected_ws)
    assert await service.unsubscribe(sub_id) is True
    assert await service.get_subscription(sub_id) is None


async def test_unsubscribe_unknown_returns_false(service):
    assert await service.unsubscribe("nonexistent") is False


async def test_broadcast_returns_message(service, connected_ws):
    await service.subscribe("conn-1", "c1", connected_ws)
    msg = await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"file": "test.py"})
    assert isinstance(msg, BroadcastMessage)
    assert msg.event_type == BroadcastEventType.FILE_CHANGED
    assert msg.container_name == "c1"


async def test_broadcast_delivers_to_subscriber(service, connected_ws):
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"file": "x.py"})
    connected_ws.send_json.assert_called_once()
    call_data = connected_ws.send_json.call_args[0][0]
    assert call_data["type"] == "broadcast"
    # Verify the actual message content is delivered, not just the wrapper
    msg = call_data["message"]
    assert msg["event_type"] == "file_changed"
    assert msg["container_name"] == "c1"
    assert msg["payload"] == {"file": "x.py"}
    assert "message_id" in msg
    assert "timestamp" in msg


async def test_broadcast_event_type_filtering(service, connected_ws):
    """Subscriber filtering on FILE_CHANGED only should not receive BUILD_STARTED."""
    await service.subscribe(
        "conn-1",
        "c1",
        connected_ws,
        event_types={BroadcastEventType.FILE_CHANGED},
    )
    await service.broadcast("c1", BroadcastEventType.BUILD_STARTED, {"id": "1"})
    connected_ws.send_json.assert_not_called()


async def test_broadcast_priority_filtering(service, connected_ws):
    """Subscriber with HIGH priority filter should not receive NORMAL priority messages."""
    await service.subscribe(
        "conn-1",
        "c1",
        connected_ws,
        priority_filter=BroadcastPriority.HIGH,
    )
    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.NORMAL
    )
    connected_ws.send_json.assert_not_called()

    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.CRITICAL
    )
    connected_ws.send_json.assert_called_once()


async def test_broadcast_tag_filtering(service, connected_ws):
    """Subscriber with tag filter should only receive matching tagged messages."""
    await service.subscribe("conn-1", "c1", connected_ws, tags={"python"})
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["java"])
    connected_ws.send_json.assert_not_called()

    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["python"])
    connected_ws.send_json.assert_called_once()


async def test_broadcast_only_to_correct_container(service, connected_ws):
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.subscribe("conn-2", "c2", ws2)

    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    connected_ws.send_json.assert_called_once()
    ws2.send_json.assert_not_called()


async def test_get_subscription_returns_none_for_unknown(service):
    assert await service.get_subscription("nope") is None


async def test_get_stats(service, connected_ws):
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    stats = await service.get_stats("c1")
    assert stats.total_broadcasts == 1
    assert stats.total_subscriptions == 1


async def test_history_stores_messages(service, connected_ws):
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    await service.broadcast("c1", BroadcastEventType.FILE_CREATED, {"n": 2})
    history = await service.get_history("c1")
    assert len(history) == 2


async def test_replay_history(service, connected_ws):
    sub_id = await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    await service.broadcast("c1", BroadcastEventType.FILE_CREATED, {"n": 2})
    connected_ws.send_json.reset_mock()
    replayed = await service.replay_history(sub_id)
    assert replayed == 2


# ---- Session RT-2 additions below ----


async def test_subscribe_stores_subscription_and_unique_ids(service, connected_ws):
    """Each subscribe call generates a unique subscription ID stored in registry."""
    id1 = await service.subscribe("conn-1", "c1", connected_ws)
    id2 = await service.subscribe("conn-2", "c1", connected_ws)
    assert id1 != id2
    sub1 = await service.get_subscription(id1)
    sub2 = await service.get_subscription(id2)
    assert sub1 is not None
    assert sub2 is not None
    assert sub1.connection_id == "conn-1"
    assert sub2.connection_id == "conn-2"


async def test_unsubscribe_cleans_up_connections(service, connected_ws):
    """After unsubscribe, the connection entry is also removed."""
    sub_id = await service.subscribe("conn-1", "c1", connected_ws)
    assert sub_id in service._connections
    await service.unsubscribe(sub_id)
    assert sub_id not in service._connections


async def test_broadcast_skips_disconnected_websocket(service):
    """Broadcast should skip WebSocket connections that are not CONNECTED."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.DISCONNECTED
    ws.send_json = AsyncMock()
    await service.subscribe("conn-1", "c1", ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    ws.send_json.assert_not_called()


async def test_broadcast_handles_send_failure(service):
    """Broadcast should not raise when send_json fails on a subscriber."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock(side_effect=RuntimeError("broken pipe"))
    await service.subscribe("conn-1", "c1", ws)
    # Should not raise
    msg = await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"data": 1})
    assert isinstance(msg, BroadcastMessage)


async def test_broadcast_stores_in_history(service, connected_ws):
    """Broadcast adds message to history when enable_history is True."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    assert len(service._history["c1"]) == 1


async def test_broadcast_no_history_when_disabled():
    """With enable_history=False, messages should not be stored."""
    svc = WebSocketBroadcastService(enable_history=False)
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    await svc.subscribe("conn-1", "c1", ws)
    await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    assert len(svc._history["c1"]) == 0


async def test_broadcast_event_type_filter_pass_and_reject(service):
    """Subscriber with event_types filter receives matching, rejects non-matching."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    await service.subscribe(
        "conn-1",
        "c1",
        ws,
        event_types={BroadcastEventType.TEST_COMPLETED, BroadcastEventType.TEST_FAILED},
    )
    # Should NOT deliver
    await service.broadcast("c1", BroadcastEventType.BUILD_STARTED, {})
    ws.send_json.assert_not_called()
    # Should deliver
    await service.broadcast("c1", BroadcastEventType.TEST_COMPLETED, {})
    assert ws.send_json.call_count == 1


async def test_broadcast_tag_filter_intersection(service):
    """Tag filtering uses intersection: message with at least one matching tag passes."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    await service.subscribe("conn-1", "c1", ws, tags={"python", "security"})
    # No intersection
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["java"])
    ws.send_json.assert_not_called()
    # Partial intersection (python matches)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["python", "ruby"])
    assert ws.send_json.call_count == 1


async def test_broadcast_priority_filter_levels(service):
    """Priority filter LOW<=NORMAL<=HIGH<=CRITICAL ordering."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    await service.subscribe("conn-1", "c1", ws, priority_filter=BroadcastPriority.NORMAL)
    # LOW should be rejected (below NORMAL)
    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.LOW
    )
    ws.send_json.assert_not_called()
    # NORMAL should pass
    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.NORMAL
    )
    assert ws.send_json.call_count == 1
    # HIGH should pass
    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.HIGH
    )
    assert ws.send_json.call_count == 2


async def test_register_and_execute_sync_event_hook(service, connected_ws):
    """Sync hook registered for an event type is called on broadcast."""
    captured = []
    hook = MagicMock(side_effect=lambda msg: captured.append(msg))
    service.register_event_hook(BroadcastEventType.BUILD_COMPLETED, hook)
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.BUILD_COMPLETED, {"id": "b1"})
    assert len(captured) == 1
    assert captured[0].event_type == BroadcastEventType.BUILD_COMPLETED


async def test_register_and_execute_async_event_hook(service, connected_ws):
    """Async hook registered for an event type is called on broadcast."""
    captured = []

    async def async_hook(msg):
        captured.append(msg)

    service.register_event_hook(BroadcastEventType.FILE_DELETED, async_hook)
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_DELETED, {"path": "/foo"})
    assert len(captured) == 1


async def test_event_hook_exception_does_not_propagate(service, connected_ws):
    """A hook that raises should not prevent broadcast from completing."""

    def bad_hook(msg):
        raise ValueError("hook failure")

    service.register_event_hook(BroadcastEventType.FILE_CHANGED, bad_hook)
    await service.subscribe("conn-1", "c1", connected_ws)
    msg = await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    assert isinstance(msg, BroadcastMessage)


async def test_get_history_returns_newest_first(service, connected_ws):
    """get_history should return messages sorted newest-first."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    await service.broadcast("c1", BroadcastEventType.FILE_CREATED, {"n": 2})
    await service.broadcast("c1", BroadcastEventType.FILE_DELETED, {"n": 3})
    history = await service.get_history("c1")
    assert len(history) == 3
    # Newest first: timestamps should be descending
    for i in range(len(history) - 1):
        assert history[i].timestamp >= history[i + 1].timestamp


async def test_get_history_filter_by_event_type(service, connected_ws):
    """get_history with event_types filter returns only matching messages."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    await service.broadcast("c1", BroadcastEventType.BUILD_STARTED, {})
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    history = await service.get_history("c1", event_types={BroadcastEventType.FILE_CHANGED})
    assert len(history) == 2
    assert all(m.event_type == BroadcastEventType.FILE_CHANGED for m in history)


async def test_get_history_filter_by_tags(service, connected_ws):
    """get_history with tags filter returns only messages with matching tags."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["python"])
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["java"])
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {}, tags=["python", "test"])
    history = await service.get_history("c1", tags={"python"})
    assert len(history) == 2


async def test_get_history_filter_by_since(service, connected_ws):
    """get_history with since filter returns only messages after the timestamp."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    cutoff = datetime.now(UTC)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 2})
    history = await service.get_history("c1", since=cutoff)
    assert len(history) == 1


async def test_get_history_with_limit(service, connected_ws):
    """get_history with limit returns at most limit messages."""
    await service.subscribe("conn-1", "c1", connected_ws)
    for i in range(5):
        await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": i})
    history = await service.get_history("c1", limit=2)
    assert len(history) == 2


async def test_replay_history_filters_by_event_type(service, connected_ws):
    """replay_history should filter by event_types when provided."""
    sub_id = await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    await service.broadcast("c1", BroadcastEventType.BUILD_STARTED, {"n": 2})
    connected_ws.send_json.reset_mock()
    replayed = await service.replay_history(sub_id, event_types={BroadcastEventType.FILE_CHANGED})
    assert replayed == 1


async def test_replay_history_missing_subscription(service):
    """replay_history with unknown subscription_id returns 0."""
    replayed = await service.replay_history("nonexistent-sub")
    assert replayed == 0


async def test_replay_history_disconnected_websocket(service):
    """replay_history with disconnected WebSocket returns 0."""
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    svc = WebSocketBroadcastService()
    sub_id = await svc.subscribe("conn-1", "c1", ws)
    await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    # Disconnect the websocket
    ws.client_state = WebSocketState.DISCONNECTED
    ws.send_json.reset_mock()
    replayed = await svc.replay_history(sub_id)
    assert replayed == 0


async def test_get_stats_tracks_event_types_and_priorities(service, connected_ws):
    """Stats should track broadcasts by event type and priority."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.HIGH
    )
    await service.broadcast(
        "c1", BroadcastEventType.BUILD_STARTED, {}, priority=BroadcastPriority.LOW
    )
    stats = await service.get_stats("c1")
    assert isinstance(stats, BroadcastStats)
    assert stats.total_broadcasts == 2
    assert BroadcastEventType.FILE_CHANGED.value in stats.broadcasts_by_event_type
    assert BroadcastEventType.BUILD_STARTED.value in stats.broadcasts_by_event_type
    assert BroadcastPriority.HIGH.value in stats.broadcasts_by_priority
    assert BroadcastPriority.LOW.value in stats.broadcasts_by_priority


async def test_get_stats_unknown_container_returns_default(service):
    """get_stats for an unknown container returns a default stats object."""
    stats = await service.get_stats("unknown-container")
    assert stats.total_broadcasts == 0
    assert stats.container_name == "unknown-container"


async def test_get_all_stats(service, connected_ws):
    """get_all_stats returns stats for all containers that have had activity."""
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    ws2.send_json = AsyncMock()
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.subscribe("conn-2", "c2", ws2)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    await service.broadcast("c2", BroadcastEventType.BUILD_STARTED, {})
    all_stats = await service.get_all_stats()
    assert "c1" in all_stats
    assert "c2" in all_stats


async def test_history_truncation_at_max():
    """History should be bounded by max_history_per_container (deque maxlen)."""
    svc = WebSocketBroadcastService(max_history_per_container=3)
    ws = AsyncMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_json = AsyncMock()
    await svc.subscribe("conn-1", "c1", ws)
    for i in range(5):
        await svc.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": i})
    history = await svc.get_history("c1")
    assert len(history) == 3


async def test_cleanup_expired_messages_removes_ttl_expired(service, connected_ws):
    """cleanup_expired_messages removes messages whose TTL has elapsed."""
    await service.subscribe("conn-1", "c1", connected_ws)
    # Broadcast with TTL of 0 seconds (already expired)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1}, ttl_seconds=0)
    # Broadcast without TTL (should remain)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 2})
    # Force the TTL message to be expired by adjusting its timestamp
    msg_list = list(service._history["c1"])
    msg_list[0].timestamp = datetime.now(UTC) - timedelta(seconds=10)
    service._history["c1"].clear()
    service._history["c1"].extend(msg_list)

    cleaned = await service.cleanup_expired_messages()
    assert cleaned == 1
    remaining = list(service._history["c1"])
    assert len(remaining) == 1
    assert remaining[0].payload == {"n": 2}


async def test_cleanup_expired_messages_keeps_no_ttl_messages(service, connected_ws):
    """Messages with no TTL should never be cleaned up."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 2})
    cleaned = await service.cleanup_expired_messages()
    assert cleaned == 0
    assert len(list(service._history["c1"])) == 2


async def test_get_container_subscriptions(service, connected_ws):
    """get_container_subscriptions returns all subs for a container."""
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.subscribe("conn-2", "c1", ws2)
    subs = await service.get_container_subscriptions("c1")
    assert len(subs) == 2


async def test_get_container_subscriptions_empty(service):
    """get_container_subscriptions for unknown container returns empty list."""
    subs = await service.get_container_subscriptions("nope")
    assert subs == []


async def test_singleton_get_broadcast_service():
    """get_broadcast_service returns the same instance on repeated calls."""
    import services.websocket_broadcast as mod

    # Reset global state for test isolation
    original = mod._broadcast_service
    mod._broadcast_service = None
    try:
        svc1 = get_broadcast_service()
        svc2 = get_broadcast_service()
        assert svc1 is svc2
    finally:
        mod._broadcast_service = original


async def test_cleanup_task_runner_cancellation():
    """cleanup_task_runner exits cleanly on CancelledError."""
    import asyncio

    svc = WebSocketBroadcastService()
    task = asyncio.create_task(cleanup_task_runner(svc, cleanup_interval_seconds=0))
    # Give it a moment to start, then cancel
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # If we get here without hanging, the test passes


async def test_broadcast_delivery_time_tracking(service, connected_ws):
    """Delivery time should be tracked in stats after broadcast."""
    await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    stats = await service.get_stats("c1")
    assert stats.average_delivery_time_ms >= 0.0


async def test_broadcast_updates_subscription_message_count(service, connected_ws):
    """Broadcast should increment the subscription's message_count."""
    sub_id = await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {})
    sub = await service.get_subscription(sub_id)
    assert sub.message_count == 2
    assert sub.last_message_at is not None


async def test_replay_history_sends_as_broadcast_replay_type(service, connected_ws):
    """Replayed messages should have type 'broadcast_replay'."""
    sub_id = await service.subscribe("conn-1", "c1", connected_ws)
    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"n": 1})
    connected_ws.send_json.reset_mock()
    await service.replay_history(sub_id)
    call_data = connected_ws.send_json.call_args[0][0]
    assert call_data["type"] == "broadcast_replay"


async def test_broadcast_with_correlation_id(service, connected_ws):
    """Broadcast message preserves correlation_id."""
    await service.subscribe("conn-1", "c1", connected_ws)
    msg = await service.broadcast(
        "c1",
        BroadcastEventType.FILE_CHANGED,
        {},
        correlation_id="corr-123",
    )
    assert msg.correlation_id == "corr-123"


async def test_broadcast_with_ttl(service, connected_ws):
    """Broadcast message preserves ttl_seconds."""
    await service.subscribe("conn-1", "c1", connected_ws)
    msg = await service.broadcast(
        "c1",
        BroadcastEventType.FILE_CHANGED,
        {},
        ttl_seconds=60,
    )
    assert msg.ttl_seconds == 60


async def test_subscribe_with_empty_filters_receives_all(service, connected_ws):
    """Subscriber with no event_types, no tags, no priority filter receives ALL messages."""
    await service.subscribe("conn-1", "c1", connected_ws)
    # Send messages with different event types, tags, and priorities
    await service.broadcast(
        "c1", BroadcastEventType.FILE_CHANGED, {}, priority=BroadcastPriority.LOW
    )
    await service.broadcast("c1", BroadcastEventType.BUILD_STARTED, {}, tags=["java"])
    await service.broadcast(
        "c1", BroadcastEventType.TEST_COMPLETED, {}, priority=BroadcastPriority.CRITICAL
    )
    # All 3 messages should have been delivered
    assert connected_ws.send_json.call_count == 3


async def test_multiple_subscribers_independent_delivery(service):
    """Two subscribers with different filters get different subsets of messages."""
    ws1 = AsyncMock()
    ws1.client_state = WebSocketState.CONNECTED
    ws1.send_json = AsyncMock()
    ws2 = AsyncMock()
    ws2.client_state = WebSocketState.CONNECTED
    ws2.send_json = AsyncMock()

    # ws1 only wants FILE_CHANGED events
    await service.subscribe(
        "conn-1",
        "c1",
        ws1,
        event_types={BroadcastEventType.FILE_CHANGED},
    )
    # ws2 only wants BUILD events
    await service.subscribe(
        "conn-2",
        "c1",
        ws2,
        event_types={BroadcastEventType.BUILD_STARTED, BroadcastEventType.BUILD_COMPLETED},
    )

    await service.broadcast("c1", BroadcastEventType.FILE_CHANGED, {"file": "a.py"})
    await service.broadcast("c1", BroadcastEventType.BUILD_STARTED, {"id": "b1"})
    await service.broadcast("c1", BroadcastEventType.TEST_COMPLETED, {"id": "t1"})

    # ws1 should only have received FILE_CHANGED
    assert ws1.send_json.call_count == 1
    assert ws1.send_json.call_args[0][0]["message"]["event_type"] == "file_changed"

    # ws2 should only have received BUILD_STARTED
    assert ws2.send_json.call_count == 1
    assert ws2.send_json.call_args[0][0]["message"]["event_type"] == "build_started"
