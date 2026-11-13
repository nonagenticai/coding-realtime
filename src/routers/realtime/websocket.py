"""WebSocket /analysis endpoint for realtime code analysis."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi import Query, WebSocket, WebSocketDisconnect
from fastapi.exceptions import HTTPException
from fastapi.routing import APIRouter

from routers.realtime._common import connection_manager, logger
from routers.realtime.handlers import handle_realtime_message
from services.websocket_metrics import get_websocket_metrics_collector
from utils.container_validation import validate_and_get_container_for_tenant
from utils.websocket_utils import (
    WebSocketContainerContext,
    WebSocketException,
    check_message_rate_limit,
    resolve_ws_tenant,
    send_rate_limit_exceeded_message,
)

router = APIRouter()


@router.websocket("/analysis", name="ws_realtime_analysis")
async def websocket_realtime_analysis(
    websocket: WebSocket,
    connection_id: str = Query(..., description="Unique connection identifier"),
    workspace_path: str | None = Query(None, description="Workspace root path"),
) -> None:
    """
    Real-time Code Analysis WebSocket - Live analysis during development

    Provides real-time code analysis updates during development, enabling instant
    feedback on code changes, syntax errors, complexity metrics, and suggestions.
    All connections are container-scoped for proper isolation.

    Query Parameters:
        connection_id: Unique identifier for the WebSocket connection
        container_name: Container name (extracted from query params and validated)
        workspace_path: Optional workspace root path for relative file resolution

    Supported incoming message types: analyze_file, analyze_files, watch_file,
    unwatch_file, get_status, pong.

    Outgoing message types include connection_established, analysis_result,
    analysis_error, watch_started/stopped, status_update, and error.
    """
    # Identity-resolved — see resolve_ws_tenant for why ?tenant_id= is not read
    # and why a bare X-Tenant-Id now buys nothing at all: since coding-client
    # 1.15.0 an uncredentialed handshake is refused, not served on its header.
    decision = await resolve_ws_tenant(websocket)
    if decision.refused:
        # decision.reason never names the asserted tenant — echoing it back
        # would confirm the guess to whoever sent it.
        await websocket.close(code=1008, reason=decision.reason)
        return
    tenant_id = decision.tenant_id
    if not tenant_id:
        await websocket.close(
            code=1008,
            # Defence in depth: a RESOLVED identity always carries a tenant, so
            # reaching here means the licence introspected to an empty one.
            # The reason deliberately does NOT tell the caller to send
            # X-Tenant-Id — since 1.15.0 a bare header is not a credential and
            # advertising it would send an unauthenticated caller down a path
            # that can only refuse it.
            reason="A verifiable licence is required on this handshake.",
        )
        return

    # Use WebSocketContainerContext for automatic container validation and connection management
    ctx = WebSocketContainerContext(websocket)
    async with ctx as container_name:
        # Validate container for tenant
        try:
            await validate_and_get_container_for_tenant(container_name, tenant_id)
        except HTTPException as e:
            await websocket.close(code=1008, reason=f"Container validation failed: {e.detail}")
            return

        # connect() returns the (tenant, connection_id) registry key. The raw
        # client id is NOT unique across tenants (GAP-3), so from here on every
        # manager/handler call uses the key; the client still sees its own id.
        # monitor_id: the id the health monitor registered this socket under
        # (the registry's, not the client's). Pongs must be reported under it.
        connection_id = await connection_manager.connect(
            websocket,
            connection_id,
            container_name,
            tenant_id=tenant_id,
            monitor_id=ctx.registered_connection_id,
        )
        try:
            # Loop only while this socket still owns its slot. A same-tenant
            # reconnect takes the slot over and closes this socket (4001
            # "superseded"); from then on every manager call on the shared key
            # would act on the NEW socket, so stop instead.
            while connection_manager.owns(connection_id, websocket):
                data = await websocket.receive_text()
                if not connection_manager.owns(connection_id, websocket):
                    break
                try:
                    message = json.loads(data)
                except json.JSONDecodeError:
                    # A single malformed frame must not tear down the whole
                    # session — report it and keep listening.
                    await connection_manager.send_message(
                        connection_id,
                        {
                            "type": "error",
                            "timestamp": datetime.now(UTC).isoformat(),
                            "container_name": container_name,
                            "error": "Invalid JSON in message; frame ignored",
                        },
                    )
                    continue

                # Check rate limit before processing message
                try:
                    await check_message_rate_limit(container_name)
                except WebSocketException as e:
                    # Send rate limit exceeded message
                    await send_rate_limit_exceeded_message(
                        websocket,
                        container_name,
                        violation_type="message_rate",
                        current_value=e.details.get("current_rate"),
                        limit_value=e.details.get("rate_limit"),
                    )
                    # Skip processing this message but don't disconnect
                    continue

                # Track message received in metrics
                metrics_collector = get_websocket_metrics_collector()
                await metrics_collector.track_message_received(container_name)

                await handle_realtime_message(
                    connection_id, message, workspace_path, container_name, tenant_id
                )
        except WebSocketDisconnect:
            logger.info(
                "WebSocket disconnected: %s (container: %s)",
                connection_id,
                container_name,
            )
        except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as e:
            logger.error(
                "WebSocket error for %s (container: %s): %s",
                connection_id,
                container_name,
                e,
            )
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": f"WebSocket error: {str(e)}",
                },
            )
        finally:
            # Pass THIS socket: after a same-tenant reconnect the slot holds the
            # newer socket, and this cleanup must not evict it.
            connection_manager.disconnect(connection_id, websocket)
