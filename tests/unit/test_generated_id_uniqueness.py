"""Server-generated ids must not collide for connections made in the same instant.

Both ids were built from ``uuid7().hex[:8]``. The first 32 bits of a uuid7 are
the top of its 48-bit millisecond timestamp — ``unix_ts_ms >> 16`` — and carry
no random bits at all, so the suffix only changes every 65.536 s. Two
connections to one container (or two subscriptions from one connection id)
inside that window got the SAME id, and the second silently replaced the first
in a dict keyed by it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from services.websocket_broadcast import WebSocketBroadcastService
from services.websocket_connection_registry import WebSocketConnectionRegistry


async def test_two_registry_connections_in_the_same_instant_get_distinct_ids():
    registry = WebSocketConnectionRegistry(enable_rate_limiting=False)
    first = await registry.register_connection(AsyncMock(), "box-a")
    second = await registry.register_connection(AsyncMock(), "box-a")

    assert first != second
    live = {c.connection_id for c in await registry.get_container_connections("box-a")}
    assert live == {first, second}  # neither replaced the other


async def test_two_subscriptions_from_one_connection_id_get_distinct_ids():
    service = WebSocketBroadcastService(enable_history=False)
    first = await service.subscribe("tab", "box-a", AsyncMock())
    second = await service.subscribe("tab", "box-a", AsyncMock())

    assert first != second
    assert await service.get_subscription(first) is not None
    assert await service.get_subscription(second) is not None
