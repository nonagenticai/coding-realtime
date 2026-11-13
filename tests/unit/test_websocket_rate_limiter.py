"""Tests for WebSocketRateLimiter."""

import pytest

from services.websocket_rate_limiter import (
    ContainerRateStats,
    MessageBucket,
    RateLimitConfig,
    WebSocketRateLimiter,
)


@pytest.fixture
def limiter():
    return WebSocketRateLimiter()


async def test_default_config(limiter):
    assert limiter.config.messages_per_second == 10.0
    assert limiter.config.connections_per_container == 10
    assert limiter.config.burst_size == 20


async def test_custom_config():
    cfg = RateLimitConfig(messages_per_second=5.0, connections_per_container=3, burst_size=10)
    limiter = WebSocketRateLimiter(config=cfg)
    assert limiter.config.messages_per_second == 5.0


async def test_check_message_rate_allowed(limiter):
    allowed = await limiter.check_message_rate("c1")
    assert allowed is True


async def test_record_message_increments_stats(limiter):
    await limiter.check_message_rate("c1")
    stats = await limiter.get_container_stats("c1")
    assert stats.total_messages == 1


async def test_check_connection_limit_allowed(limiter):
    assert await limiter.check_connection_limit("c1") is True


async def test_check_connection_limit_exceeded(limiter):
    for _ in range(10):
        await limiter.register_connection("c1")
    assert await limiter.check_connection_limit("c1") is False


async def test_register_unregister_connection(limiter):
    await limiter.register_connection("c1")
    await limiter.register_connection("c1")
    stats = await limiter.get_container_stats("c1")
    assert stats.active_connections == 2
    await limiter.unregister_connection("c1")
    stats = await limiter.get_container_stats("c1")
    assert stats.active_connections == 1


async def test_get_container_stats(limiter):
    stats = await limiter.get_container_stats("c1")
    assert isinstance(stats, ContainerRateStats)
    assert stats.container_name == "c1"


async def test_get_all_stats_empty(limiter):
    """A fresh limiter with no activity should return an empty dict, not None."""
    stats = await limiter.get_all_stats()
    assert stats == {}


async def test_get_violations_empty(limiter):
    """A fresh limiter should have no violations for any container."""
    violations = await limiter.get_violations("c1")
    assert violations == []
    # Also verify with no container filter
    all_violations = await limiter.get_violations()
    assert all_violations == []


async def test_rate_limiting_actually_blocks_messages():
    """Verify that once burst is exhausted, messages are actually denied."""
    cfg = RateLimitConfig(messages_per_second=1.0, connections_per_container=5, burst_size=2)
    limiter = WebSocketRateLimiter(config=cfg)
    # First 2 messages should pass (burst)
    assert await limiter.check_message_rate("c1") is True
    assert await limiter.check_message_rate("c1") is True
    # 3rd message should be denied (burst exhausted, no time elapsed for refill)
    assert await limiter.check_message_rate("c1") is False
    # Verify a violation was recorded
    stats = await limiter.get_container_stats("c1")
    assert stats.total_violations >= 1
    assert stats.is_rate_limited is True


async def test_message_bucket_consume():
    bucket = MessageBucket(rate=10.0, burst_size=2)
    assert await bucket.consume(1.0) is True
    assert await bucket.consume(1.0) is True
    assert await bucket.consume(1.0) is False
