"""Behavioral tests for websocket_utils — extract, context manager, rate limiting, path helpers.

Complements test_websocket_utils.py (which covers constructors and simple factories)
by exercising runtime behavior: container validation flows, context manager lifecycle,
rate limiter integration, and error-path factories with varying arguments.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock

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
    create_error_response,
    create_invalid_path_error,
    create_path_access_denied_error,
    create_path_not_found_error,
    create_path_outside_container_error,
    extract_container_from_websocket,
    send_rate_limit_exceeded_message,
)

# Since coding-client 1.15.0 an uncredentialed WS handshake is REFUSED — a bare
# X-Tenant-Id proves nothing. This suite tests container extraction BEHAVIOUR,
# not the auth boundary (tests/unit/test_ws_identity.py owns that), so every
# handshake here carries a real licence.
pytestmark = pytest.mark.usefixtures("ws_licence")

# ── helpers ──────────────────────────────────────────────────────────────


def _install_service_module(monkeypatch, module_name: str, factory_name: str, value) -> None:
    """Swap a real service module for a stub, restoring the original on teardown.

    Must go through `monkeypatch.setitem`: a bare `sys.modules[name] = stub`
    survives the test that installed it, and the stub (a bare `types.ModuleType`
    with no `__file__`) then makes any later
    `from services.<mod> import <Name>` fail with
    `ImportError: ... (unknown location)`. That is exactly what leaked out of
    `test_unexpected_error_allows` into tests/integration.
    """
    module = types.ModuleType(module_name)
    setattr(module, factory_name, lambda: value)
    monkeypatch.setitem(sys.modules, module_name, module)


def _remove_service_module(monkeypatch, module_name: str) -> None:
    """Hide a service module so the import inside the SUT raises ImportError.

    `monkeypatch.delitem` restores the real entry on teardown; a bare
    `sys.modules.pop` leaves the module absent for the rest of the session.
    """
    monkeypatch.delitem(sys.modules, module_name, raising=False)


def _make_ws(state=WebSocketState.CONNECTED, *, headers=None, **query_params):
    ws = AsyncMock()
    ws.client_state = state
    ws.query_params = query_params
    # Starlette's real Headers, not a dict: it is case-insensitive, and the SDK
    # reads `authorization` in lower case. A plain dict is NARROWER than the real
    # object and would make every credentialled handshake read as uncredentialed.
    ws.headers = Headers(ws_licence_headers("t-utils") if headers is None else headers)
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.send_json = AsyncMock()
    return ws


# ═══════════════════════════════════════════════════════════════════════════
# extract_container_from_websocket — status-gating
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractContainerStatusGating:
    """Container must be running/ready; other statuses raise CONTAINER_NOT_RUNNING."""

    @pytest.mark.parametrize("status", ["stopped", "creating", "error", "paused"])
    async def test_non_running_status_raises(self, monkeypatch, status):
        ws = _make_ws(container_name="app", tenant_id="t1")

        async def _status_reply(*_a, **_kw):
            return {"status": status, "codebase_path": "/ws/app"}

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _status_reply)

        with pytest.raises(WebSocketException) as exc_info:
            await extract_container_from_websocket(ws)
        assert exc_info.value.error_code == WebSocketErrorCode.CONTAINER_NOT_RUNNING
        assert exc_info.value.details["current_status"] == status

    @pytest.mark.parametrize("status", ["running", "ready"])
    async def test_running_ready_status_succeeds(self, monkeypatch, status):
        ws = _make_ws(container_name="app")

        async def _ok(*_a, **_kw):
            return {"status": status, "codebase_path": "/workspace/app"}

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _ok)
        cached = {}
        monkeypatch.setattr(
            websocket_utils,
            "cache_container_path",
            lambda name, path: cached.setdefault(name, path),
        )
        result = await extract_container_from_websocket(ws)
        assert result == "app"
        assert cached["app"] == "/workspace/app"


# ═══════════════════════════════════════════════════════════════════════════
# extract_container_from_websocket — object/dict/plain response shapes
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractContainerResponseShapes:
    """Container info can be a dict, an object with .status, or a plain value."""

    async def test_object_with_status_value_attr(self, monkeypatch):
        """Object whose .status has a .value attr (e.g. enum)."""
        ws = _make_ws(container_name="app")

        class _StatusEnum:
            value = "running"

        class _Info:
            status = _StatusEnum()
            codebase_path = "/workspace/app"

        async def _obj(*_a, **_kw):
            return _Info()

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _obj)
        cached = {}
        monkeypatch.setattr(
            websocket_utils,
            "cache_container_path",
            lambda name, path: cached.setdefault(name, path),
        )
        result = await extract_container_from_websocket(ws)
        assert result == "app"
        assert cached["app"] == "/workspace/app"

    async def test_object_with_plain_string_status(self, monkeypatch):
        """Object whose .status is a plain string, no codebase_path."""
        ws = _make_ws(container_name="app")

        class _Info:
            status = "running"
            codebase_path = None

        async def _obj(*_a, **_kw):
            return _Info()

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _obj)
        monkeypatch.setattr(websocket_utils, "cache_container_path", lambda *_a: None)
        assert await extract_container_from_websocket(ws) == "app"

    async def test_plain_return_value_skips_status_check(self, monkeypatch):
        """Non-dict, non-object return (e.g. string 'ok') skips status gating."""
        ws = _make_ws(container_name="app")

        async def _plain(*_a, **_kw):
            return "ok"

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _plain)
        monkeypatch.setattr(websocket_utils, "cache_container_path", lambda *_a: None)
        assert await extract_container_from_websocket(ws) == "app"


# ═══════════════════════════════════════════════════════════════════════════
# extract_container_from_websocket — exception wrapping
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractContainerExceptionWrapping:
    """HTTPException wraps to CONTAINER_NOT_FOUND; generic wraps to SERVICE_UNAVAILABLE."""

    @pytest.mark.parametrize("http_status", [403, 404, 500])
    async def test_http_exception_wraps_to_not_found(self, monkeypatch, http_status):
        ws = _make_ws(container_name="gone")

        async def _raise(*_a, **_kw):
            raise HTTPException(status_code=http_status, detail="problem")

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _raise)
        with pytest.raises(WebSocketException) as exc_info:
            await extract_container_from_websocket(ws)
        assert exc_info.value.error_code == WebSocketErrorCode.CONTAINER_NOT_FOUND
        assert "http_detail" in exc_info.value.details

    @pytest.mark.parametrize(
        "exc_class,exc_msg",
        [
            (RuntimeError, "connection refused"),
            (TimeoutError, "timed out"),
            (OSError, "network unreachable"),
        ],
    )
    async def test_generic_exception_wraps_to_unavailable(self, monkeypatch, exc_class, exc_msg):
        ws = _make_ws(container_name="boom")

        async def _raise(*_a, **_kw):
            raise exc_class(exc_msg)

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _raise)
        with pytest.raises(WebSocketException) as exc_info:
            await extract_container_from_websocket(ws)
        assert exc_info.value.error_code == WebSocketErrorCode.SERVICE_UNAVAILABLE
        assert exc_msg in exc_info.value.details["error"]


# ═══════════════════════════════════════════════════════════════════════════
# extract_container_from_websocket — tenant resolution
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractContainerTenant:
    """The tenant comes from the resolved identity, never from a raw input.

    Two spoof vectors are pinned here, and they closed in that order:

    * ``?tenant_id=`` — the ingress Lua filter strips a client ``x-tenant-id``
      but cannot strip a query string, so anyone denied the header could pass
      the same value as a query param. Ignored since 2026-07-29.
    * a bare ``X-Tenant-Id`` with **no credential at all** — served until
      coding-client 1.15.0, which is how this route handed out another tenant's
      stream to a caller holding no licence. Refused now.

    `expected_tenant is None` means the handshake must be REFUSED: the container
    is never looked up, so a regression cannot pass by returning ``None``.
    """

    @pytest.mark.parametrize(
        "query_params,headers,expected_tenant",
        [
            ({"container_name": "app", "tenant_id": "q-tenant"}, {}, None),
            (
                {"container_name": "app"},
                ws_licence_headers("h-tenant"),
                "h-tenant",
            ),
            (
                {"container_name": "app", "tenant_id": "spoof"},
                ws_licence_headers("h-tenant"),
                "h-tenant",
            ),
            ({"container_name": "app"}, {}, None),
            # The forgery itself: a header and nothing to back it.
            ({"container_name": "app"}, {"X-Tenant-Id": "victim"}, None),
        ],
        ids=[
            "query-param-ignored",
            "licenced-header",
            "query-cannot-override-licence",
            "no-tenant",
            "bare-forged-header-refused",
        ],
    )
    async def test_tenant_resolution(self, monkeypatch, query_params, headers, expected_tenant):
        ws = AsyncMock()
        ws.query_params = query_params
        ws.headers = Headers(headers)
        ws.client = MagicMock(host="10.42.7.13", port=51234)
        captured = []

        async def _capture(name, tenant_id=None):
            captured.append(tenant_id)
            return {"status": "running", "codebase_path": "/ws"}

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _capture)
        monkeypatch.setattr(websocket_utils, "cache_container_path", lambda *_a: None)

        if expected_tenant is None:
            with pytest.raises(WebSocketException) as excinfo:
                await extract_container_from_websocket(ws)
            assert excinfo.value.code == 1008
            # Never served: the container was not looked up for the claimed tenant.
            assert captured == []
            # And the close reason must not confirm the guess back to the caller.
            assert "victim" not in excinfo.value.reason
        else:
            await extract_container_from_websocket(ws)
            assert captured[0] == expected_tenant


# ═══════════════════════════════════════════════════════════════════════════
# extract_container — codebase_path caching
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractContainerCaching:
    """cache_container_path is called only when codebase_path is truthy."""

    async def test_no_codebase_path_skips_cache(self, monkeypatch):
        ws = _make_ws(container_name="app")

        async def _no_path(*_a, **_kw):
            return {"status": "running"}

        monkeypatch.setattr(websocket_utils, "validate_and_get_container_for_tenant", _no_path)
        cache_calls = []
        monkeypatch.setattr(
            websocket_utils, "cache_container_path", lambda *a: cache_calls.append(a)
        )
        await extract_container_from_websocket(ws)
        assert cache_calls == []


# ═══════════════════════════════════════════════════════════════════════════
# WebSocketContainerContext — __aenter__
# ═══════════════════════════════════════════════════════════════════════════


class TestContextAutoAccept:
    """auto_accept flag controls whether ws.accept() is called."""

    @pytest.mark.parametrize("auto_accept", [True, False])
    async def test_accept_called_based_on_flag(self, monkeypatch, auto_accept):
        ws = _make_ws()
        monkeypatch.setattr(
            websocket_utils, "extract_container_from_websocket", AsyncMock(return_value="c1")
        )
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        _remove_service_module(monkeypatch, "services.websocket_connection_registry")
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws, auto_accept=auto_accept)
        name = await ctx.__aenter__()
        assert name == "c1"
        if auto_accept:
            ws.accept.assert_awaited_once()
        else:
            ws.accept.assert_not_awaited()
        await ctx.__aexit__(None, None, None)


class TestContextServiceRegistration:
    """Registry/health monitor integration during context setup."""

    async def test_registry_import_error_uses_fallback_id(self, monkeypatch):
        ws = _make_ws()
        monkeypatch.setattr(
            websocket_utils, "extract_container_from_websocket", AsyncMock(return_value="c1")
        )
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        _remove_service_module(monkeypatch, "services.websocket_connection_registry")
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws, connection_id="ext-1")
        await ctx.__aenter__()
        assert ctx.registered_connection_id == "ext-1"
        await ctx.__aexit__(None, None, None)

    async def test_health_monitor_error_does_not_block(self, monkeypatch):
        ws = _make_ws()
        monkeypatch.setattr(
            websocket_utils, "extract_container_from_websocket", AsyncMock(return_value="c1")
        )
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)

        registry = AsyncMock()
        registry.register_connection.return_value = "conn-42"
        _install_service_module(
            monkeypatch,
            "services.websocket_connection_registry",
            "get_websocket_registry",
            registry,
        )
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws)
        name = await ctx.__aenter__()
        assert name == "c1"
        assert ctx.registered_connection_id == "conn-42"
        await ctx.__aexit__(None, None, None)


class TestContextErrorHandling:
    """Error handling during __aenter__ — WebSocketException & generic exceptions."""

    async def test_ws_exception_connecting_closes_without_json(self, monkeypatch):
        """CONNECTING state: close() with code/reason, no send_json."""
        ws = _make_ws(state=WebSocketState.CONNECTING, container_name="c1")
        monkeypatch.setattr(
            websocket_utils,
            "extract_container_from_websocket",
            AsyncMock(
                side_effect=WebSocketException(
                    code=1008,
                    reason="no container",
                    error_code=WebSocketErrorCode.MISSING_CONTAINER_PARAM,
                )
            ),
        )

        ctx = WebSocketContainerContext(ws, auto_accept=False)
        with pytest.raises(WebSocketException):
            await ctx.__aenter__()
        ws.close.assert_awaited_once_with(code=1008, reason="no container")
        ws.send_json.assert_not_called()

    async def test_ws_exception_connected_sends_json_then_closes(self, monkeypatch):
        """CONNECTED state: send error JSON, then close."""
        ws = _make_ws(state=WebSocketState.CONNECTED, container_name="c1")
        monkeypatch.setattr(
            websocket_utils,
            "extract_container_from_websocket",
            AsyncMock(
                side_effect=WebSocketException(
                    code=1011,
                    reason="not running",
                    error_code=WebSocketErrorCode.CONTAINER_NOT_RUNNING,
                    details={"container_name": "c1"},
                )
            ),
        )

        ctx = WebSocketContainerContext(ws, auto_accept=True)
        with pytest.raises(WebSocketException):
            await ctx.__aenter__()
        ws.send_json.assert_awaited_once()
        payload = ws.send_json.call_args[0][0]
        assert payload["error_code"] == "CONTAINER_NOT_RUNNING"
        ws.close.assert_awaited_once_with(code=1011, reason="not running")

    @pytest.mark.parametrize(
        "ws_state",
        [WebSocketState.CONNECTED, WebSocketState.CONNECTING],
        ids=["connected", "connecting"],
    )
    async def test_generic_exception_wraps_to_internal_error(self, monkeypatch, ws_state):
        """Non-WebSocketException wraps into INTERNAL_ERROR and closes."""
        ws = _make_ws(state=ws_state, container_name="c1")
        monkeypatch.setattr(
            websocket_utils,
            "extract_container_from_websocket",
            AsyncMock(side_effect=ValueError("unexpected")),
        )

        ctx = WebSocketContainerContext(ws, auto_accept=(ws_state == WebSocketState.CONNECTED))
        with pytest.raises(WebSocketException) as exc_info:
            await ctx.__aenter__()
        assert exc_info.value.error_code == WebSocketErrorCode.INTERNAL_ERROR
        assert "unexpected" in exc_info.value.reason
        ws.close.assert_awaited_once_with(code=1011, reason="Internal error")


# ═══════════════════════════════════════════════════════════════════════════
# WebSocketContainerContext — __aexit__
# ═══════════════════════════════════════════════════════════════════════════


class TestContextExit:
    """__aexit__ handles normal close, disconnect, and error paths."""

    async def test_normal_exit_closes_connected_ws(self, monkeypatch):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        _remove_service_module(monkeypatch, "services.websocket_connection_registry")
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws)
        ctx.container_name = "app"
        ctx.registered_connection_id = None

        result = await ctx.__aexit__(None, None, None)
        assert result is False
        ws.close.assert_awaited_once()

    async def test_disconnect_does_not_suppress_exception(self, monkeypatch):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        _remove_service_module(monkeypatch, "services.websocket_connection_registry")
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws)
        ctx.container_name = "app"
        ctx.registered_connection_id = None

        result = await ctx.__aexit__(WebSocketDisconnect, WebSocketDisconnect(), None)
        assert result is False

    async def test_runtime_error_closes_and_propagates(self, monkeypatch):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        _remove_service_module(monkeypatch, "services.websocket_connection_registry")
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws)
        ctx.container_name = "app"
        ctx.registered_connection_id = None

        result = await ctx.__aexit__(RuntimeError, RuntimeError("boom"), None)
        assert result is False
        ws.close.assert_awaited_once()

    async def test_full_cleanup_with_registered_connection(self, monkeypatch):
        """Unregisters from health monitor and registry, evicts cache, closes ws."""
        ws = _make_ws(state=WebSocketState.CONNECTED)
        evicted = []
        monkeypatch.setattr(websocket_utils, "evict_container_path", evicted.append)

        registry = AsyncMock()
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

        ctx = WebSocketContainerContext(ws)
        ctx.container_name = "app"
        ctx.registered_connection_id = "conn-99"

        await ctx.__aexit__(None, None, None)
        monitor.unregister_connection.assert_awaited_once_with("conn-99")
        registry.unregister_connection.assert_awaited_once_with("conn-99")
        assert evicted == ["app"]
        ws.close.assert_awaited_once()

    async def test_disconnected_ws_not_closed_again(self, monkeypatch):
        """Already-DISCONNECTED websocket is not closed."""
        ws = _make_ws(state=WebSocketState.DISCONNECTED)
        monkeypatch.setattr(websocket_utils, "evict_container_path", lambda _: None)
        _remove_service_module(monkeypatch, "services.websocket_connection_registry")
        _remove_service_module(monkeypatch, "services.websocket_health_monitor")

        ctx = WebSocketContainerContext(ws)
        ctx.container_name = "app"
        ctx.registered_connection_id = None

        await ctx.__aexit__(None, None, None)
        ws.close.assert_not_awaited()


# ═══════════════════════════════════════════════════════════════════════════
# check_message_rate_limit
# ═══════════════════════════════════════════════════════════════════════════


class TestCheckMessageRateLimit:
    """Rate limiter: allowed, exceeded, import error, unexpected error."""

    async def test_allowed(self, monkeypatch):
        limiter = AsyncMock()
        limiter.check_message_rate.return_value = True
        _install_service_module(
            monkeypatch, "services.websocket_rate_limiter", "get_websocket_rate_limiter", limiter
        )
        assert await check_message_rate_limit("app") is True

    async def test_exceeded_raises_with_stats(self, monkeypatch):
        stats = types.SimpleNamespace(messages_per_second=20.0, messages_limit=10.0)
        limiter = AsyncMock()
        limiter.check_message_rate.return_value = False
        limiter.get_container_stats.return_value = stats
        _install_service_module(
            monkeypatch, "services.websocket_rate_limiter", "get_websocket_rate_limiter", limiter
        )
        with pytest.raises(WebSocketException) as exc_info:
            await check_message_rate_limit("app")
        assert exc_info.value.error_code == WebSocketErrorCode.MESSAGE_RATE_EXCEEDED
        assert exc_info.value.details["current_rate"] == 20.0
        assert exc_info.value.details["rate_limit"] == 10.0

    async def test_import_error_allows(self, monkeypatch):
        _remove_service_module(monkeypatch, "services.websocket_rate_limiter")
        assert await check_message_rate_limit("app") is True

    async def test_unexpected_error_allows(self, monkeypatch):
        limiter = AsyncMock()
        limiter.check_message_rate.side_effect = RuntimeError("redis down")
        _install_service_module(
            monkeypatch, "services.websocket_rate_limiter", "get_websocket_rate_limiter", limiter
        )
        assert await check_message_rate_limit("app") is True


# ═══════════════════════════════════════════════════════════════════════════
# send_rate_limit_exceeded_message
# ═══════════════════════════════════════════════════════════════════════════


class TestSendRateLimitMessage:
    """Sending rate-limit messages over websocket."""

    @pytest.mark.parametrize(
        "violation_type,expected_code",
        [
            ("message_rate", "MESSAGE_RATE_EXCEEDED"),
            ("connection_limit", "CONNECTION_LIMIT_EXCEEDED"),
        ],
    )
    async def test_violation_type_maps_to_error_code(self, violation_type, expected_code):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        await send_rate_limit_exceeded_message(ws, "app", violation_type, 5.0, 3.0)
        ws.send_json.assert_awaited_once()
        payload = ws.send_json.call_args[0][0]
        assert payload["error_code"] == expected_code

    async def test_values_in_message_when_provided(self):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        await send_rate_limit_exceeded_message(ws, "app", "message_rate", 5.0, 3.0)
        payload = ws.send_json.call_args[0][0]
        assert "5.00" in payload["error"]
        assert "3.00" in payload["error"]

    async def test_no_values_omits_numeric_suffix(self):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        await send_rate_limit_exceeded_message(ws, "app")
        payload = ws.send_json.call_args[0][0]
        assert "/" not in payload["error"]

    async def test_disconnected_ws_skipped(self):
        ws = _make_ws(state=WebSocketState.DISCONNECTED)
        await send_rate_limit_exceeded_message(ws, "app", "message_rate")
        ws.send_json.assert_not_called()

    async def test_send_error_swallowed(self):
        ws = _make_ws(state=WebSocketState.CONNECTED)
        ws.send_json = AsyncMock(side_effect=RuntimeError("closed"))
        await send_rate_limit_exceeded_message(ws, "app", "message_rate", 5.0, 3.0)
        # No exception propagated


# ═══════════════════════════════════════════════════════════════════════════
# Path error factories — parametrized
# ═══════════════════════════════════════════════════════════════════════════


class TestPathErrorFactories:
    """Thorough parametrized testing of path error factories."""

    @pytest.mark.parametrize(
        "context,should_contain",
        [
            ("checked all dirs", "checked all dirs"),
            (None, None),
        ],
        ids=["with-context", "without-context"],
    )
    def test_path_not_found(self, context, should_contain):
        exc = create_path_not_found_error("c1", "/missing", context)
        assert exc.error_code == WebSocketErrorCode.PATH_NOT_FOUND
        assert exc.details["path"] == "/missing"
        if should_contain:
            assert should_contain in exc.reason
        else:
            assert "checked" not in exc.reason

    @pytest.mark.parametrize(
        "root,has_root_key",
        [("/app", True), (None, False)],
        ids=["with-root", "without-root"],
    )
    def test_path_outside_container(self, root, has_root_key):
        exc = create_path_outside_container_error("c1", "/etc/passwd", root)
        assert exc.error_code == WebSocketErrorCode.PATH_OUTSIDE_CONTAINER
        assert ("container_root" in exc.details) == has_root_key

    @pytest.mark.parametrize(
        "reason_arg,expected_in_reason",
        [("null byte", "null byte"), (None, "")],
        ids=["with-reason", "without-reason"],
    )
    def test_invalid_path(self, reason_arg, expected_in_reason):
        exc = create_invalid_path_error("c1", "\x00bad", reason_arg)
        assert exc.error_code == WebSocketErrorCode.INVALID_PATH_FORMAT
        if expected_in_reason:
            assert expected_in_reason in exc.reason
        assert exc.details["reason"] == (reason_arg or "")

    @pytest.mark.parametrize(
        "reason_arg",
        ["restricted", None],
        ids=["with-reason", "without-reason"],
    )
    def test_access_denied(self, reason_arg):
        exc = create_path_access_denied_error("c1", "/root/.ssh", reason_arg)
        assert exc.error_code == WebSocketErrorCode.PATH_ACCESS_DENIED
        assert exc.details["reason"] == (reason_arg or "")
        if reason_arg:
            assert reason_arg in exc.reason

    def test_create_error_response_structure(self):
        resp = create_error_response(WebSocketErrorCode.TIMEOUT_ERROR, "slow", {"timeout": 30})
        assert resp == {
            "type": "error",
            "error": "slow",
            "error_code": "TIMEOUT_ERROR",
            "details": {"timeout": 30},
        }
