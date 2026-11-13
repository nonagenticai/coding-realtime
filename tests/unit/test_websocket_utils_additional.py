"""Integration-style tests for websocket_utils — full async-with lifecycle.

Complements test_websocket_utils.py (constructors) and test_websocket_utils_new.py
(unit-level behaviors) by testing the WebSocketContainerContext as an actual
async context manager and the rate-limit helpers end-to-end.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.websockets import WebSocketDisconnect, WebSocketState
from starlette.datastructures import Headers

from tests.conftest import ws_licence_headers
from utils import websocket_utils
from utils.websocket_utils import (
    WebSocketContainerContext,
    WebSocketErrorCode,
    WebSocketException,
    check_message_rate_limit,
    extract_container_from_websocket,
    send_rate_limit_exceeded_message,
)

# Since coding-client 1.15.0 an uncredentialed WS handshake is REFUSED — a bare
# X-Tenant-Id proves nothing. This suite tests the context-manager lifecycle,
# not the auth boundary (tests/unit/test_ws_identity.py owns that), so every
# handshake here carries a real licence.
pytestmark = pytest.mark.usefixtures("ws_licence")


def _install_service_module(monkeypatch, module_name: str, factory_name: str, value) -> None:
    """Swap a real service module for a stub, restoring the original on teardown.

    Must go through `monkeypatch.setitem`: a bare `sys.modules[name] = stub`
    leaks the stub into every later test in the session. It used to, and the
    stub (a bare `types.ModuleType` with no `__file__`) made
    `from services.websocket_rate_limiter import ContainerRateStats` fail with
    `ImportError: ... (unknown location)` in tests/integration.
    """
    module = types.ModuleType(module_name)
    setattr(module, factory_name, lambda: value)
    monkeypatch.setitem(sys.modules, module_name, module)


class TestContextFullLifecycle:
    """Full async-with lifecycle: enter -> use -> exit."""

    async def test_success_lifecycle_registers_and_cleans_up(self, monkeypatch):
        """Happy path: accept, extract, register, (block), unregister, close."""
        ws = AsyncMock()
        ws.client_state = WebSocketState.CONNECTED
        ws.close = AsyncMock()
        ws.accept = AsyncMock()

        monkeypatch.setattr(
            websocket_utils, "extract_container_from_websocket", AsyncMock(return_value="app")
        )
        evicted = []
        monkeypatch.setattr(websocket_utils, "evict_container_path", evicted.append)

        registry = AsyncMock()
        registry.register_connection.return_value = "conn-1"
        monitor = AsyncMock()
        _install_service_module(
            monkeypatch,
            "services.websocket_connection_registry",
            "get_websocket_registry",
            registry,
        )
        _install_service_module(
            monkeypatch, "services.websocket_health_monitor", "get_health_monitor", monitor
        )

        context = WebSocketContainerContext(
            ws, connection_id="external-id", metadata={"role": "test"}
        )
        async with context as container_name:
            assert container_name == "app"

        ws.accept.assert_awaited_once()
        registry.register_connection.assert_awaited_once()
        monitor.register_connection.assert_awaited_once()
        monitor.unregister_connection.assert_awaited_once_with("conn-1")
        registry.unregister_connection.assert_awaited_once_with("conn-1")
        ws.close.assert_awaited_once()
        assert evicted == ["app"]

    async def test_validation_failure_closes_without_entering_block(self, monkeypatch):
        """WebSocketException during setup closes ws, block body never runs."""
        ws = AsyncMock()
        ws.client_state = WebSocketState.CONNECTING
        ws.close = AsyncMock()
        ws.send_json = AsyncMock()

        monkeypatch.setattr(
            websocket_utils,
            "extract_container_from_websocket",
            AsyncMock(
                side_effect=WebSocketException(
                    code=1008,
                    reason="bad container",
                    error_code=WebSocketErrorCode.CONTAINER_NOT_FOUND,
                )
            ),
        )

        context = WebSocketContainerContext(ws)
        block_entered = False
        with pytest.raises(WebSocketException):
            async with context:
                block_entered = True  # pragma: no cover

        assert not block_entered
        ws.close.assert_awaited_once_with(code=1008, reason="bad container")
        ws.send_json.assert_not_called()

    async def test_exit_with_disconnect_propagates(self, monkeypatch):
        """WebSocketDisconnect in the block body propagates (not suppressed)."""
        ws = AsyncMock()
        ws.client_state = WebSocketState.CONNECTED
        ws.close = AsyncMock()

        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        context = WebSocketContainerContext(ws, auto_accept=False)
        context.container_name = "app"
        context.registered_connection_id = None

        result = await context.__aexit__(WebSocketDisconnect, WebSocketDisconnect(), None)
        assert result is False
        ws.close.assert_awaited_once()


class TestRateLimitEndToEnd:
    """Rate-limit helpers tested as a combined flow."""

    async def test_check_then_send_flow(self, monkeypatch):
        """check_message_rate_limit -> exceeded -> send_rate_limit_exceeded_message."""
        stats = types.SimpleNamespace(messages_per_second=4.0, messages_limit=3.0)
        limiter = AsyncMock()
        limiter.check_message_rate.return_value = False
        limiter.get_container_stats.return_value = stats
        _install_service_module(
            monkeypatch, "services.websocket_rate_limiter", "get_websocket_rate_limiter", limiter
        )

        # Step 1: check — should raise
        with pytest.raises(WebSocketException) as exc_info:
            await check_message_rate_limit("app")
        assert exc_info.value.error_code == WebSocketErrorCode.MESSAGE_RATE_EXCEEDED
        assert exc_info.value.details["current_rate"] == 4.0

        # Step 2: flip to allowed
        limiter.check_message_rate.return_value = True
        assert await check_message_rate_limit("app") is True

        # Step 3: send rate limit message to client
        ws = AsyncMock()
        ws.client_state = WebSocketState.CONNECTED
        await send_rate_limit_exceeded_message(ws, "app", current_value=5.0, limit_value=3.0)
        ws.send_json.assert_awaited_once()
        payload = ws.send_json.call_args[0][0]
        assert payload["error_code"] == "MESSAGE_RATE_EXCEEDED"

        # Step 4: disconnected ws skips send
        ws.client_state = WebSocketState.DISCONNECTED
        ws.send_json.reset_mock()
        await send_rate_limit_exceeded_message(ws, "app", violation_type="connection_limit")
        ws.send_json.assert_not_called()


class TestExtractContainerCombined:
    """Combined extract scenarios covering success, HTTP error, and generic error."""

    async def test_success_caches_path(self, monkeypatch):
        ws = AsyncMock()
        ws.query_params = {"container_name": "app", "tenant_id": "tenant-1"}
        ws.headers = Headers(ws_licence_headers("tenant-1"))

        async def _ok(*_args, **_kwargs):
            return {"status": "running", "codebase_path": "/workspace/app"}

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _ok)
        cached = {}
        monkeypatch.setattr(
            websocket_utils,
            "cache_container_path",
            lambda name, path: cached.setdefault(name, path),
        )

        assert await extract_container_from_websocket(ws) == "app"
        assert cached == {"app": "/workspace/app"}

    @pytest.mark.parametrize(
        "exc_factory,expected_code",
        [
            (
                lambda: HTTPException(status_code=404, detail="missing"),
                WebSocketErrorCode.CONTAINER_NOT_FOUND,
            ),
            (lambda: RuntimeError("down"), WebSocketErrorCode.SERVICE_UNAVAILABLE),
        ],
        ids=["http-error", "runtime-error"],
    )
    async def test_error_wrapping(self, monkeypatch, exc_factory, expected_code):
        ws = AsyncMock()
        ws.query_params = {"container_name": "app", "tenant_id": "t1"}
        ws.headers = Headers(ws_licence_headers("tenant-1"))

        async def _raise(*_args, **_kwargs):
            raise exc_factory()

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _raise)
        with pytest.raises(WebSocketException) as exc_info:
            await extract_container_from_websocket(ws)
        assert exc_info.value.error_code == expected_code
