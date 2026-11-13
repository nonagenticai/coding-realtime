from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import main


async def test_lifespan_handles_redis_success_and_shutdown():
    redis_client = AsyncMock()
    redis_client.ping = AsyncMock(return_value=True)
    redis_client.aclose = AsyncMock()

    with patch("main.aioredis.from_url", return_value=redis_client):
        async with main.lifespan(main.app):
            assert main.get_redis() is redis_client

    redis_client.ping.assert_awaited_once()
    redis_client.aclose.assert_awaited_once()


async def test_lifespan_handles_redis_failure():
    """A client that cannot even be CONSTRUCTED (bad/missing REDIS_URL) means there
    is no redis at all, so the service runs without pub/sub."""
    with patch("main.aioredis.from_url", side_effect=RuntimeError("redis down")):
        async with main.lifespan(main.app):
            with pytest.raises(RuntimeError, match="Redis is not initialised"):
                main.get_redis()


async def test_lifespan_keeps_client_when_redis_is_merely_unreachable():
    """An UNREACHABLE redis at boot must not permanently disable cross-replica
    fan-out for this pod's life.

    redis-py reconnects lazily and the broadcast subscriber retries every 2s, so
    the client is kept and the pod self-heals when redis returns. Dropping it (the
    old behaviour) left a pod local-only forever while still passing readiness —
    silent cross-replica message loss at >1 replica.
    """
    redis_client = AsyncMock()
    redis_client.ping = AsyncMock(side_effect=ConnectionError("connection refused"))
    redis_client.aclose = AsyncMock()

    with patch("main.aioredis.from_url", return_value=redis_client):
        async with main.lifespan(main.app):
            # Client retained despite the failed startup probe.
            assert main.get_redis() is redis_client

    redis_client.ping.assert_awaited()
    redis_client.aclose.assert_awaited_once()


def test_exception_handlers_registered():
    """Verify that register_exception_handlers wired up handlers on the app."""
    from fastapi.exceptions import RequestValidationError
    from starlette.exceptions import HTTPException as StarletteHTTPException

    handlers = main.app.exception_handlers
    # coding-client 1.3.1+ registers the HTTP handler against starlette's base
    # class so it catches both starlette- and fastapi-raised exceptions.
    assert StarletteHTTPException in handlers, "HTTPException handler not registered"
    assert RequestValidationError in handlers, "RequestValidationError handler not registered"
    assert Exception in handlers, "Generic Exception handler not registered"
    # Verify they are callable handlers, not None
    assert callable(handlers[StarletteHTTPException])
    assert callable(handlers[RequestValidationError])
    assert callable(handlers[Exception])
