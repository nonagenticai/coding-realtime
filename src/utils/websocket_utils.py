"""WebSocket utilities for container-scoped WebSocket connections.

Provides:
- WebSocketErrorCode / WebSocketException -- structured WS error handling
- extract_container_from_websocket -- validate container from query params
- WebSocketContainerContext -- async context manager for WS lifecycle
- check_message_rate_limit / send_rate_limit_exceeded_message
- Path-error helper factories

In the microservice, container validation calls the **coding-containers**
service over HTTP instead of the local DockerService.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from coding_client.auth.websocket import Outcome, resolve_websocket_identity
from fastapi import HTTPException, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState

from utils.container_validation import validate_and_get_container_for_tenant
from utils.path_resolution import cache_container_path, evict_container_path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------


class WebSocketErrorCode(StrEnum):
    """Standardised error codes for WebSocket container operations."""

    MISSING_CONTAINER_PARAM = "MISSING_CONTAINER_PARAM"
    CONTAINER_NOT_FOUND = "CONTAINER_NOT_FOUND"
    INVALID_CONTAINER_NAME = "INVALID_CONTAINER_NAME"

    CONTAINER_NOT_RUNNING = "CONTAINER_NOT_RUNNING"
    CONTAINER_NOT_READY = "CONTAINER_NOT_READY"
    CONTAINER_STARTUP_FAILED = "CONTAINER_STARTUP_FAILED"

    PATH_NOT_FOUND = "PATH_NOT_FOUND"
    PATH_OUTSIDE_CONTAINER = "PATH_OUTSIDE_CONTAINER"
    INVALID_PATH_FORMAT = "INVALID_PATH_FORMAT"
    PATH_ACCESS_DENIED = "PATH_ACCESS_DENIED"

    OPERATION_FAILED = "OPERATION_FAILED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"

    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    MESSAGE_RATE_EXCEEDED = "MESSAGE_RATE_EXCEEDED"
    CONNECTION_LIMIT_EXCEEDED = "CONNECTION_LIMIT_EXCEEDED"

    INTERNAL_ERROR = "INTERNAL_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"


class WebSocketException(Exception):
    """Exception for WebSocket-specific errors."""

    def __init__(
        self,
        code: int,
        reason: str,
        error_code: WebSocketErrorCode | None = None,
        details: dict[str, Any] | None = None,
    ):
        self.code = code
        self.reason = reason
        self.error_code = error_code or WebSocketErrorCode.INTERNAL_ERROR
        self.details = details or {}
        super().__init__(reason)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WsTenantDecision:
    """What a handshake proved about the tenant it is asking to act for.

    Frozen so a handler cannot widen a decision after it has been made.
    """

    #: The tenant the connection may act for, or ``None`` when none was
    #: established. ``None`` on a refusal — a refused connection never gets to
    #: see which tenant it guessed at.
    tenant_id: str | None = None
    #: True when a licence was introspected and vouched for ``tenant_id``.
    #: Since coding-client 1.15.0 this is True on **every** served connection:
    #: there is no longer a path that hands back a tenant nobody verified. It is
    #: kept rather than deleted because that makes it a pin — a served decision
    #: with ``verified is False`` means the uncredentialed residual came back.
    verified: bool = False
    #: True when serving this connection would be an authorisation failure.
    #: The handler MUST close with 1008.
    refused: bool = False
    #: Safe to put in a WS close reason: never contains the asserted tenant,
    #: because echoing it back confirms the guess to whoever sent it.
    reason: str | None = None


async def resolve_ws_tenant(websocket: WebSocket) -> WsTenantDecision:
    """Return the tenant decision for *websocket*.

    ``?tenant_id=`` is deliberately NOT read, and that must stay true.
    The ingress Lua filter
    (``gitops-production/clusters/strato/policies/envoy-filters/ext-authz-enforce.yaml``)
    unconditionally strips ``x-tenant-id`` and its siblings off every inbound
    request before any host or path branch, and re-injects the value only from
    what license-authz resolved.

    It strips **headers**; it cannot strip a query string. So a client that was
    denied ``X-Tenant-Id: victim`` could simply send ``?tenant_id=victim`` and
    have it honoured — a cross-tenant read on the WS path that the edge cannot
    see. The filter's own comment names this exact backdoor class as the thing
    it exists to close, but its ``WS_LICENSE_PATHS`` covered only one product
    host and never this service's own edge host.

    Why the header alone is no longer sufficient
    --------------------------------------------
    That edge argument is true **north-south** and false **pod-to-pod**. The Lua
    filter only ever sees traffic arriving through the ingress gateway; an
    east-west caller inside the mesh reaches this service directly and its
    ``X-Tenant-Id`` is never scrubbed by anything.

    ``AuthMiddleware`` cannot cover the gap either: it is a
    ``BaseHTTPMiddleware``, and Starlette never invokes ``dispatch`` on a
    ``websocket`` scope. So once the HTTP paths stopped trusting a bare header
    (``TRUST_TENANT_HEADER=false``), these WS handlers became the only place
    left in the fleet where any pod could assert any tenant and be believed.
    No config flip can close that — each handler has to resolve identity
    itself, which is what ``resolve_websocket_identity`` does here.

    The uncredentialed residual is CLOSED (coding-client >= 1.15.0)
    ---------------------------------------------------------------
    A browser **cannot** set an ``Authorization`` header on a WebSocket
    handshake — the WebSocket API has no way to pass one — so a
    browser-originated socket carries a credential only if a server-side proxy
    puts one there. While some proxies still sent none, refusing outright would
    not have hardened the browser path, it would have broken the live panels
    riding on it. So ``Outcome.NO_CREDENTIAL`` used to fall back to a raw
    ``X-Tenant-Id`` read and log a WARNING naming the peer.

    That fallback was never a safety property. A credential-less handshake
    carrying only a forged ``X-Tenant-Id`` was BELIEVED, and
    ``/api/broadcast/subscribe`` answered ``subscription_established`` for a
    container the caller held no licence for (reproduced from inside the pod).
    The WARNING was an *instrument* for finding the callers still to convert,
    and it has now reported: over ~10.5h of log retention on this service it
    fired **5 times, every one from peer=127.0.0.1** — a verification probe —
    and **0 times** on coding-analysis over ~18h. No real caller ever used it.
    Meanwhile both legitimate callers became credentialed: coding-gateway
    attaches its own licence on WS upgrades, and ``fr`` presents
    ``svc-fr-backend``'s licence on both of its WS proxies (driven live: 101
    with auth, 403 without).

    So the branch is gone. ``coding-client >= 1.15.0`` makes
    :attr:`WebSocketIdentity.should_close` True for ``NO_CREDENTIAL`` as well as
    ``REFUSED``, and this function acts on that one property rather than
    re-deciding from ``outcome``. That is deliberate: a caller that tested
    ``outcome is Outcome.REFUSED`` itself would have silently bypassed the fix.
    ``outcome`` is still read, but only to LABEL the log line — never to decide.
    """
    identity = await resolve_websocket_identity(websocket)

    if identity.should_close:
        # ⚠️ Decided by `should_close` alone. Re-deriving the decision here
        # (`outcome is Outcome.REFUSED`) is exactly the bypass that kept the
        # uncredentialed handshake alive through 1.14.0 — the SDK owns the
        # policy, this function owns the close. `outcome` below only labels the
        # log; it never gates.
        if identity.outcome is Outcome.NO_CREDENTIAL:
            client = getattr(websocket, "client", None)
            logger.warning(
                "Refused an uncredentialed WebSocket handshake (peer=%s, path=%s). "
                "The caller presented no verifiable licence, so any X-Tenant-Id it "
                "sent proves nothing. Send Authorization: Bearer <licence>.",
                f"{getattr(client, 'host', None)}:{getattr(client, 'port', None)}",
                getattr(getattr(websocket, "url", None), "path", None),
            )
        # Never echo the asserted tenant back: identity.reason is written to be
        # safe in a close frame precisely because the close frame is visible to
        # whoever sent the assertion.
        return WsTenantDecision(
            refused=True,
            reason=identity.reason or "this credential may not act for the tenant it asserted",
        )

    return WsTenantDecision(tenant_id=identity.tenant_id, verified=True)


def create_error_response(
    error_code: WebSocketErrorCode,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a structured error dict for sending over WebSocket."""
    return {
        "type": "error",
        "error": message,
        "error_code": error_code.value,
        "details": details or {},
    }


_WS_TENANT_SCOPE_KEY = "coding_realtime.tenant_id"


def _remember_ws_tenant(websocket: WebSocket, tenant_id: str | None) -> None:
    """Record the handshake's verified tenant on the connection scope."""
    scope = getattr(websocket, "scope", None)
    if isinstance(scope, dict):
        scope[_WS_TENANT_SCOPE_KEY] = tenant_id


def ws_tenant(websocket: WebSocket) -> str | None:
    """Return the tenant recorded by :func:`extract_container_from_websocket`, if any."""
    scope = getattr(websocket, "scope", None)
    value = scope.get(_WS_TENANT_SCOPE_KEY) if isinstance(scope, dict) else None
    return value if isinstance(value, str) and value else None


async def extract_container_from_websocket(websocket: WebSocket) -> str:
    """Extract and validate *container_name* from WebSocket query params.

    Validates the container exists (via the coding-containers HTTP service) and
    is in a running/ready state.

    Raises ``WebSocketException`` on any validation failure.
    """
    container_name = websocket.query_params.get("container_name")

    if not container_name:
        raise WebSocketException(
            code=1008,
            reason="Missing required query parameter: container_name",
            error_code=WebSocketErrorCode.MISSING_CONTAINER_PARAM,
            details={
                "parameter": "container_name",
                "location": "query_params",
                "message": "The 'container_name' query parameter is required for WebSocket connections",
            },
        )

    if not container_name.strip():
        raise WebSocketException(
            code=1008,
            reason="Container name cannot be empty",
            error_code=WebSocketErrorCode.INVALID_CONTAINER_NAME,
            details={"container_name": container_name},
        )

    try:
        # Identity-resolved — see resolve_ws_tenant for why ?tenant_id= is not
        # read and why a bare header is no longer a credential at all.
        decision = await resolve_ws_tenant(websocket)
        if decision.refused:
            raise WebSocketException(
                code=1008,
                reason=decision.reason or "tenant assertion refused",
                error_code=WebSocketErrorCode.VALIDATION_ERROR,
            )
        tenant_id = decision.tenant_id
        container_info = await validate_and_get_container_for_tenant(container_name, tenant_id)
        _remember_ws_tenant(websocket, tenant_id)

        # Check status
        container_status = None
        if isinstance(container_info, dict):
            container_status = container_info.get("status")
            codebase_path = container_info.get("codebase_path")
        elif hasattr(container_info, "status"):
            container_status = (
                container_info.status.value
                if hasattr(container_info.status, "value")
                else container_info.status
            )
            codebase_path = getattr(container_info, "codebase_path", None)
        else:
            codebase_path = None

        if container_status and container_status not in ("running", "ready"):
            raise WebSocketException(
                code=1011,
                reason=f"Container '{container_name}' is not running",
                error_code=WebSocketErrorCode.CONTAINER_NOT_RUNNING,
                details={
                    "container_name": container_name,
                    "current_status": container_status,
                    "required_status": "running or ready",
                },
            )

        # Cache the codebase path for later sync path resolution
        if codebase_path:
            cache_container_path(container_name, codebase_path)

        logger.debug("Container '%s' validated for WebSocket", container_name)
        return container_name

    except WebSocketException:
        raise
    except HTTPException as exc:
        raise WebSocketException(
            code=1008,
            reason=f"Container '{container_name}' not found",
            error_code=WebSocketErrorCode.CONTAINER_NOT_FOUND,
            details={"container_name": container_name, "http_detail": str(exc.detail)},
        ) from exc
    except Exception as exc:
        logger.error("Error validating container '%s': %s", container_name, exc, exc_info=True)
        raise WebSocketException(
            code=1011,
            reason=f"Failed to validate container: {exc}",
            error_code=WebSocketErrorCode.SERVICE_UNAVAILABLE,
            details={"container_name": container_name, "error": str(exc)},
        ) from exc


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class WebSocketContainerContext:
    """Async context manager for WebSocket connections with container scoping.

    Handles: accept -> validate container -> register connection ->
    (context block) -> unregister -> close.
    """

    def __init__(
        self,
        websocket: WebSocket,
        auto_accept: bool = True,
        connection_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.websocket = websocket
        self.auto_accept = auto_accept
        self.connection_id = connection_id
        self.metadata = metadata or {}
        self.container_name: str | None = None
        self.container_info: dict[str, Any] | None = None
        self.registered_connection_id: str | None = None

    async def __aenter__(self) -> str:
        try:
            if self.auto_accept:
                await self.websocket.accept()

            self.container_name = await extract_container_from_websocket(self.websocket)

            # Try to register with connection registry (service layer -- may not be wired yet)
            try:
                from services.websocket_connection_registry import get_websocket_registry

                registry = get_websocket_registry()
                self.registered_connection_id = await registry.register_connection(
                    websocket=self.websocket,
                    container_name=self.container_name,
                    connection_id=self.connection_id,
                    metadata=self.metadata,
                )
            except (ImportError, Exception) as exc:
                logger.debug("Connection registry not available: %s", exc)
                self.registered_connection_id = self.connection_id

            # Try to register with health monitor
            try:
                from services.websocket_health_monitor import get_health_monitor

                monitor = get_health_monitor()
                if self.registered_connection_id is not None:
                    await monitor.register_connection(
                        connection_id=self.registered_connection_id,
                        container_name=self.container_name,
                        websocket=self.websocket,
                        tenant_id=ws_tenant(self.websocket),
                    )
            except (ImportError, Exception) as exc:
                logger.debug("Health monitor not available: %s", exc)

            logger.info("WebSocket context established for container: %s", self.container_name)
            return self.container_name

        except WebSocketException as exc:
            error_resp = create_error_response(exc.error_code, exc.reason, exc.details)
            if self.websocket.client_state == WebSocketState.CONNECTING:
                await self.websocket.close(code=exc.code, reason=exc.reason)
            elif self.websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await self.websocket.send_json(error_resp)
                except Exception as send_exc:
                    logger.debug("Failed to send error response over WebSocket: %s", send_exc)
                await self.websocket.close(code=exc.code, reason=exc.reason)
            raise

        except Exception as exc:
            logger.error("Unexpected error in WebSocket setup: %s", exc, exc_info=True)
            try:
                if self.websocket.client_state == WebSocketState.CONNECTED:
                    await self.websocket.close(code=1011, reason="Internal error")
                elif self.websocket.client_state == WebSocketState.CONNECTING:
                    await self.websocket.close(code=1011, reason="Internal error")
            except Exception as close_exc:
                logger.debug("Failed to close WebSocket during error handling: %s", close_exc)
            raise WebSocketException(
                code=1011,
                reason=f"Internal error: {exc}",
                error_code=WebSocketErrorCode.INTERNAL_ERROR,
            ) from exc

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if exc_type == WebSocketDisconnect:
                logger.info("WebSocket disconnected for container: %s", self.container_name)
            else:
                logger.error(
                    "WebSocket error for container %s: %s",
                    self.container_name,
                    exc_val,
                )

        # Unregister from health monitor
        if self.registered_connection_id:
            try:
                from services.websocket_health_monitor import get_health_monitor

                monitor = get_health_monitor()
                await monitor.unregister_connection(self.registered_connection_id)
            except (ImportError, Exception) as exc:
                logger.debug("Health monitor cleanup skipped: %s", exc)

        # Unregister from connection registry
        if self.registered_connection_id:
            try:
                from services.websocket_connection_registry import get_websocket_registry

                registry = get_websocket_registry()
                await registry.unregister_connection(self.registered_connection_id)
            except (ImportError, Exception) as exc:
                logger.debug("Registry cleanup skipped: %s", exc)

        # Evict cached container path
        if self.container_name:
            evict_container_path(self.container_name)

        # Close WebSocket if still open
        try:
            if self.websocket.client_state == WebSocketState.CONNECTED:
                await self.websocket.close()
        except Exception as close_exc:
            logger.debug("Failed to close WebSocket in __aexit__: %s", close_exc)

        return False


# ---------------------------------------------------------------------------
# Rate limiting helpers
# ---------------------------------------------------------------------------


async def check_message_rate_limit(container_name: str) -> bool:
    """Check if the next message is allowed under the rate limit.

    Returns ``True`` if the message should be allowed.
    Raises ``WebSocketException`` if the rate limit is exceeded.
    """
    try:
        from services.websocket_rate_limiter import get_websocket_rate_limiter

        rate_limiter = get_websocket_rate_limiter()
        allowed = await rate_limiter.check_message_rate(container_name)

        if not allowed:
            stats = await rate_limiter.get_container_stats(container_name)
            raise WebSocketException(
                code=1008,
                reason=f"Message rate limit exceeded for container '{container_name}'",
                error_code=WebSocketErrorCode.MESSAGE_RATE_EXCEEDED,
                details={
                    "container_name": container_name,
                    "current_rate": stats.messages_per_second,
                    "rate_limit": stats.messages_limit,
                },
            )
        return True
    except WebSocketException:
        raise
    except ImportError:
        # Rate limiter service not wired yet; allow everything
        return True
    except Exception as exc:
        logger.error("Rate limit check error for '%s': %s", container_name, exc, exc_info=True)
        return True


async def send_rate_limit_exceeded_message(
    websocket: WebSocket,
    container_name: str,
    violation_type: str = "message_rate",
    current_value: float | None = None,
    limit_value: float | None = None,
) -> None:
    """Send a rate-limit-exceeded JSON message to the WebSocket client."""
    try:
        error_code = (
            WebSocketErrorCode.MESSAGE_RATE_EXCEEDED
            if violation_type == "message_rate"
            else WebSocketErrorCode.CONNECTION_LIMIT_EXCEEDED
        )

        message = f"Rate limit exceeded for container '{container_name}'"
        if current_value is not None and limit_value is not None:
            message += f": {current_value:.2f} / {limit_value:.2f}"

        resp = create_error_response(
            error_code=error_code,
            message=message,
            details={
                "container_name": container_name,
                "violation_type": violation_type,
                "current_value": current_value,
                "limit_value": limit_value,
            },
        )

        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_json(resp)
    except Exception as exc:
        logger.error("Error sending rate limit message: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Path error factories (for use in WS handlers)
# ---------------------------------------------------------------------------


def create_path_not_found_error(
    container_name: str,
    path: str,
    additional_context: str | None = None,
) -> WebSocketException:
    message = f"Path '{path}' not found in container '{container_name}'"
    if additional_context:
        message += f": {additional_context}"
    return WebSocketException(
        code=1008,
        reason=message,
        error_code=WebSocketErrorCode.PATH_NOT_FOUND,
        details={"container_name": container_name, "path": path},
    )


def create_path_outside_container_error(
    container_name: str,
    path: str,
    container_root: str | None = None,
) -> WebSocketException:
    details: dict[str, Any] = {"container_name": container_name, "path": path}
    if container_root:
        details["container_root"] = container_root
    return WebSocketException(
        code=1008,
        reason=f"Path '{path}' is outside container '{container_name}' boundary",
        error_code=WebSocketErrorCode.PATH_OUTSIDE_CONTAINER,
        details=details,
    )


def create_invalid_path_error(
    container_name: str,
    path: str,
    reason: str | None = None,
) -> WebSocketException:
    message = f"Invalid path format: '{path}'"
    if reason:
        message += f" - {reason}"
    return WebSocketException(
        code=1008,
        reason=message,
        error_code=WebSocketErrorCode.INVALID_PATH_FORMAT,
        details={"container_name": container_name, "path": path, "reason": reason or ""},
    )


def create_path_access_denied_error(
    container_name: str,
    path: str,
    reason: str | None = None,
) -> WebSocketException:
    message = f"Access denied to path '{path}' in container '{container_name}'"
    if reason:
        message += f": {reason}"
    return WebSocketException(
        code=1008,
        reason=message,
        error_code=WebSocketErrorCode.PATH_ACCESS_DENIED,
        details={"container_name": container_name, "path": path, "reason": reason or ""},
    )
