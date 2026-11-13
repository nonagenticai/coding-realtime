"""Additional tests for WebSocketRateLimiter — focused, single-behavior tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from services import websocket_rate_limiter as rate_module
from services.websocket_rate_limiter import (
    MessageBucket,
    RateLimitConfig,
    RateLimitViolation,
    WebSocketRateLimiter,
    get_websocket_rate_limiter,
)


@pytest.fixture(autouse=True)
def _reset_rate_limiter_singleton():
    rate_module._rate_limiter = None
    yield
    rate_module._rate_limiter = None


# ---------------------------------------------------------------------------
# Split from test_rate_limiter_limits_resets_and_global_stats
# ---------------------------------------------------------------------------


async def test_check_message_rate_allows_within_limit(rate_limiter):
    """First message on a fresh limiter (burst_size=20) must be allowed."""
    allowed = await rate_limiter.check_message_rate("c1")
    assert allowed is True


async def test_check_message_rate_denies_over_limit():
    """After exhausting the burst, the next message must be denied."""
    limiter = WebSocketRateLimiter(
        RateLimitConfig(messages_per_second=1.0, connections_per_container=5, burst_size=1)
    )
    # Consume the only burst token
    assert await limiter.check_message_rate("c1") is True
    # Second message exceeds the burst — must be denied
    assert await limiter.check_message_rate("c1") is False

    stats = await limiter.get_container_stats("c1")
    assert stats.total_violations == 1
    assert stats.total_messages == 1  # only the allowed message counted


async def test_check_connection_limit_allows_within_limit():
    """With connections_per_container=2, the first connection check must pass."""
    limiter = WebSocketRateLimiter(
        RateLimitConfig(messages_per_second=10.0, connections_per_container=2, burst_size=20)
    )
    assert await limiter.check_connection_limit("c1") is True


async def test_check_connection_limit_denies_over_limit():
    """After registering up to the limit, the next connection check must fail."""
    limiter = WebSocketRateLimiter(
        RateLimitConfig(messages_per_second=10.0, connections_per_container=2, burst_size=20)
    )
    await limiter.register_connection("c1")
    await limiter.register_connection("c1")

    # 2 connections registered, limit is 2 → next check must be denied
    assert await limiter.check_connection_limit("c1") is False

    stats = await limiter.get_container_stats("c1")
    assert stats.active_connections == 2
    assert stats.total_violations == 1


async def test_set_custom_limits():
    """set_container_message_limit and set_container_connection_limit persist in stats."""
    limiter = WebSocketRateLimiter()
    limiter.set_container_message_limit("c1", 42.0)
    limiter.set_container_connection_limit("c1", 7)

    stats = await limiter.get_container_stats("c1")
    assert stats.messages_limit == 42.0
    assert stats.connections_limit == 7


async def test_reset_container_stats():
    """reset_container_stats zeroes out violations and messages."""
    limiter = WebSocketRateLimiter(
        RateLimitConfig(messages_per_second=1.0, connections_per_container=1, burst_size=1)
    )
    # Generate one allowed message and one violation
    await limiter.check_message_rate("c1")
    await limiter.check_message_rate("c1")

    stats_before = await limiter.get_container_stats("c1")
    assert stats_before.total_messages == 1
    assert stats_before.total_violations == 1

    await limiter.reset_container_stats("c1")

    stats_after = await limiter.get_container_stats("c1")
    assert stats_after.total_messages == 0
    assert stats_after.total_violations == 0
    assert stats_after.last_violation is None


async def test_get_all_stats_returns_per_container():
    """get_all_stats keys by container name and returns correct stats objects."""
    limiter = WebSocketRateLimiter()
    await limiter.check_message_rate("alpha")
    await limiter.check_message_rate("beta")

    all_stats = await limiter.get_all_stats()
    assert set(all_stats.keys()) == {"alpha", "beta"}
    assert all_stats["alpha"].container_name == "alpha"
    assert all_stats["alpha"].total_messages == 1
    assert all_stats["beta"].container_name == "beta"
    assert all_stats["beta"].total_messages == 1


async def test_get_global_stats():
    """get_global_stats aggregates across containers correctly."""
    limiter = WebSocketRateLimiter(
        RateLimitConfig(messages_per_second=10.0, connections_per_container=5, burst_size=20)
    )
    await limiter.check_message_rate("c1")
    await limiter.check_message_rate("c2")
    await limiter.register_connection("c1")

    limiter.set_container_message_limit("c1", 3.0)

    global_stats = await limiter.get_global_stats()
    assert global_stats["total_containers"] == 2
    assert global_stats["total_active_connections"] == 1
    assert global_stats["total_messages_processed"] == 2
    assert global_stats["total_violations"] == 0
    assert global_stats["custom_limits"]["message_limits"]["c1"] == 3.0
    assert global_stats["config"]["messages_per_second"] == 10.0
    assert global_stats["config"]["burst_size"] == 20


async def test_unregister_removes_container():
    """Unregistering all connections removes the container from connection tracking."""
    limiter = WebSocketRateLimiter()
    await limiter.register_connection("c1")
    stats = await limiter.get_container_stats("c1")
    assert stats.active_connections == 1

    await limiter.unregister_connection("c1")
    stats = await limiter.get_container_stats("c1")
    assert stats.active_connections == 0

    # Unregistering again (already at 0) should not raise
    await limiter.unregister_connection("c1")
    stats = await limiter.get_container_stats("c1")
    assert stats.active_connections == 0


# ---------------------------------------------------------------------------
# Split from test_rate_limiter_validation_and_violation_trimming
# ---------------------------------------------------------------------------


async def test_set_limits_rejects_zero_rate():
    """set_container_message_limit(0) raises ValueError."""
    limiter = WebSocketRateLimiter()
    with pytest.raises(ValueError, match="must be positive"):
        limiter.set_container_message_limit("c1", 0)


async def test_set_limits_rejects_zero_connections():
    """set_container_connection_limit(0) raises ValueError."""
    limiter = WebSocketRateLimiter()
    with pytest.raises(ValueError, match="must be positive"):
        limiter.set_container_connection_limit("c1", 0)


async def test_violation_history_records_and_trims():
    """Violation history is capped at _max_violation_history, oldest entries dropped."""
    limiter = WebSocketRateLimiter()
    limiter._max_violation_history = 2

    violations_to_add = [
        RateLimitViolation(
            container_name=name,
            violation_type=vtype,
            timestamp=datetime.now(UTC),
            current_value=2.0,
            limit_value=1.0,
        )
        for name, vtype in [("a", "message_rate"), ("b", "message_rate"), ("c", "connection_limit")]
    ]

    for v in violations_to_add:
        limiter._record_violation(v)

    # Only the 2 most recent should survive (b and c)
    violations = await limiter.get_violations(limit=10)
    assert len(violations) == 2
    # get_violations returns most-recent first
    assert violations[0].container_name == "c"
    assert violations[0].violation_type == "connection_limit"
    assert violations[1].container_name == "b"
    assert violations[1].violation_type == "message_rate"


# ---------------------------------------------------------------------------
# Strengthened from test_message_bucket_current_rate
# ---------------------------------------------------------------------------


async def test_message_bucket_current_rate_within_bounds():
    """After consuming tokens, current_rate is between 0 and the configured rate."""
    bucket = MessageBucket(rate=5.0, burst_size=10)
    await bucket.consume(2.0)
    current_rate = await bucket.get_current_rate()

    assert current_rate >= 0.0
    assert current_rate <= 5.0


async def test_message_bucket_burst_behavior():
    """Consuming many tokens quickly exhausts the burst; rate reflects consumption."""
    bucket = MessageBucket(rate=2.0, burst_size=5)

    # Consume all 5 burst tokens one at a time
    results = [await bucket.consume(1.0) for _ in range(6)]

    # First 5 should succeed (burst_size=5, starts full)
    assert results[:5] == [True, True, True, True, True]
    # 6th should fail — burst exhausted, no time to refill
    assert results[5] is False

    current_rate = await bucket.get_current_rate()
    assert current_rate >= 0.0
    assert current_rate <= 2.0


# ---------------------------------------------------------------------------
# Singleton behavior (was buried at the end of the first monster test)
# ---------------------------------------------------------------------------


async def test_get_websocket_rate_limiter_returns_singleton():
    """get_websocket_rate_limiter returns the same instance on repeated calls."""
    first = get_websocket_rate_limiter()
    second = get_websocket_rate_limiter()
    assert first is second


async def test_tenant_filter_is_applied_before_the_limit():
    """Foreign violations must neither appear nor use up the caller's ``limit``.

    With more foreign violations than ``limit`` recorded AFTER the caller's own,
    filtering after slicing would take the last ``limit`` entries (all foreign),
    drop them, and hand the caller an empty list.
    """
    limiter = WebSocketRateLimiter()

    def _v(name: str, value: float) -> RateLimitViolation:
        return RateLimitViolation(
            container_name=name,
            violation_type="message_rate",
            timestamp=datetime.now(UTC),
            current_value=value,
            limit_value=1.0,
        )

    limiter._record_violation(_v("mine", 1.0))
    limiter._record_violation(_v("mine", 2.0))
    for i in range(10):
        limiter._record_violation(_v("foreign", float(i)))

    got = await limiter.get_violations(limit=3, container_names={"mine"})
    assert [(v.container_name, v.current_value) for v in got] == [("mine", 2.0), ("mine", 1.0)]

    # The limit still applies to the caller's own history.
    got = await limiter.get_violations(limit=1, container_names={"mine"})
    assert [(v.container_name, v.current_value) for v in got] == [("mine", 2.0)]
