"""
WebSocket Broadcast Subscription Endpoint

Provides a WebSocket endpoint for subscribing to container-wide broadcasts, plus
an HTTP publish endpoint so services (e.g. coding-containers on container
lifecycle events) can inject broadcasts over REST — the sending half that the
WebSocket-only subscribe route never provided (D9).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from coding_client.auth import require_tenant
from fastapi import Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.routing import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from services.websocket_broadcast import (
    BroadcastEventType,
    BroadcastPriority,
    get_broadcast_service,
)
from utils.container_validation import validate_and_get_container_for_tenant
from utils.websocket_utils import (
    WebSocketException,
    extract_container_from_websocket,
    resolve_ws_tenant,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class BroadcastPublishRequest(BaseModel):
    """Body for ``POST /api/broadcast/publish``."""

    event_type: str = Field(..., description="Broadcast event type (a BroadcastEventType value)")
    data: dict[str, Any] = Field(default_factory=dict, description="Event payload")
    priority: str = Field("normal", description="Priority: low | normal | high | critical")
    container_name: str | None = Field(
        None,
        description="Target container; falls back to data['container_name'] when omitted",
    )
    tags: list[str] | None = Field(None, description="Optional tags for subscriber filtering")
    correlation_id: str | None = Field(None, description="Optional correlation id")
    ttl_seconds: int | None = Field(None, description="Optional history TTL in seconds")

    model_config = ConfigDict(extra="forbid")


class BroadcastPublishResponse(BaseModel):
    """Result of a publish — ``delivered`` is DERIVED from the live subscriber
    count so the caller can tell a real fan-out from a no-op (no subscribers),
    instead of a blind 200.

    ``subscriber_count`` is the number of subscribers **across all replicas**
    whose filters match this message, not the number attached to the replica
    that happened to serve the request.
    """

    message_id: str
    container_name: str
    event_type: str
    subscriber_count: int
    delivered: bool

    model_config = ConfigDict(extra="forbid")


@router.post(
    "/publish",
    name="broadcast_publish",
    operation_id="publish_broadcast",
    summary="Publish a broadcast to a container's subscribers",
    response_model=BroadcastPublishResponse,
)
async def publish_broadcast(
    body: BroadcastPublishRequest,
    tenant_id: str = Depends(require_tenant),
) -> BroadcastPublishResponse:
    """Inject a broadcast over HTTP (the REST publish half of the pub/sub pair).

    Tenant-scoped exactly like the WebSocket subscribe route: the caller must own
    ``container_name`` (BOLA protection via ``validate_and_get_container_for_tenant``).
    The container may be given top-level or inside ``data['container_name']`` (the
    shape coding-containers' lifecycle events already carry).
    """
    container_name = body.container_name or (
        body.data.get("container_name") if isinstance(body.data, dict) else None
    )
    if not container_name or not isinstance(container_name, str):
        raise HTTPException(
            status_code=422,
            detail="container_name is required (top-level or in data.container_name)",
        )

    try:
        event_type = BroadcastEventType(body.event_type)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid event_type: {body.event_type!r}"
        ) from exc
    try:
        priority = BroadcastPriority(body.priority)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid priority: {body.priority!r}") from exc

    # BOLA: the caller must own the container it publishes to.
    await validate_and_get_container_for_tenant(container_name, tenant_id)

    service = get_broadcast_service()
    message = await service.broadcast(
        container_name=container_name,
        event_type=event_type,
        payload=body.data,
        priority=priority,
        tags=body.tags,
        correlation_id=body.correlation_id,
        ttl_seconds=body.ttl_seconds,
    )

    # Derive delivery honestly from the live subscriber set (0 => it fanned out
    # to nobody; a real no-op the caller can now observe rather than a fake 200).
    #
    # The count is CROSS-REPLICA (shared redis presence set), not this replica's
    # local subscription map. With a local count, a publish routed to replica A
    # while the subscriber is connected to replica B reported
    # `subscriber_count: 0, delivered: false` even though the redis pub/sub
    # fan-out DID deliver the message — the exact inversion of the honesty
    # property this endpoint exists to provide, on ~half of all publishes at 2
    # replicas. It is also filter-aware: a subscriber that filtered this event
    # type out is not counted as having received it.
    subscriber_count = await service.count_reachable_subscribers(message)
    if subscriber_count == 0:
        logger.info(
            "Broadcast %s published to container %s with NO subscribers (no-op fan-out)",
            event_type.value,
            container_name,
        )

    return BroadcastPublishResponse(
        message_id=message.message_id,
        container_name=container_name,
        event_type=event_type.value,
        subscriber_count=subscriber_count,
        delivered=subscriber_count > 0,
    )


@router.websocket("/subscribe", name="ws_broadcast_subscribe")
async def websocket_broadcast_subscribe(
    websocket: WebSocket,
    connection_id: str = Query(..., description="Unique connection identifier"),
    event_types: str | None = Query(
        None, description="Comma-separated event types to subscribe to"
    ),
    tags: str | None = Query(None, description="Comma-separated tags to filter by"),
    priority: str | None = Query(None, description="Minimum priority level"),
    replay_history: bool = Query(False, description="Replay message history on connect"),
    replay_limit: int = Query(100, description="Maximum messages to replay"),
) -> None:
    """
    WebSocket endpoint for subscribing to container broadcasts.

    ## Connection Flow
    1. **Container Validation**: Validates container_name from query parameters
    2. **Subscription Creation**: Creates broadcast subscription with filters
    3. **History Replay**: Optional replay of message history
    4. **Live Updates**: Receives real-time broadcast messages
    5. **Graceful Cleanup**: Automatic cleanup on disconnect

    ## Query Parameters
    - `connection_id`: Unique identifier for the WebSocket connection (required)
    - `container_name`: Container name (required, in query params)
    - `event_types`: Comma-separated event types to subscribe to (optional, empty = all)
    - `tags`: Comma-separated tags to filter by (optional, empty = all)
    - `priority`: Minimum priority level (low, normal, high, critical) (optional)
    - `replay_history`: Whether to replay message history on connection (default: false)
    - `replay_limit`: Maximum number of messages to replay (default: 100)

    ## Message Types

    ### Incoming Messages

    #### **subscribe**
    Update subscription filters
    ```json
    {
        "type": "subscribe",
        "event_types": ["code_graph_update", "test_completed"],
        "tags": ["python", "critical"],
        "priority": "normal"
    }
    ```

    #### **unsubscribe**
    Remove specific event type subscriptions
    ```json
    {
        "type": "unsubscribe",
        "event_types": ["build_progress"]
    }
    ```

    #### **replay_history**
    Request replay of message history
    ```json
    {
        "type": "replay_history",
        "event_types": ["test_completed"],
        "limit": 50
    }
    ```

    #### **get_status**
    Get subscription status
    ```json
    {
        "type": "get_status"
    }
    ```

    ### Outgoing Messages

    #### **subscription_established**
    Subscription created successfully
    ```json
    {
        "type": "subscription_established",
        "subscription_id": "sub-123",
        "container_name": "my-container",
        "event_types": ["code_graph_update"],
        "tags": ["python"],
        "priority": "normal"
    }
    ```

    #### **broadcast**
    Broadcast message received
    ```json
    {
        "type": "broadcast",
        "message": {
            "message_id": "msg-123",
            "event_type": "code_graph_update",
            "priority": "normal",
            "timestamp": "2025-01-06T10:30:00Z",
            "payload": {
                "files_updated": 5,
                "entities_added": 12
            }
        }
    }
    ```

    #### **broadcast_replay**
    Replayed historical message
    ```json
    {
        "type": "broadcast_replay",
        "message": {
            "message_id": "msg-122",
            "event_type": "test_completed",
            "timestamp": "2025-01-06T10:25:00Z",
            "payload": {...}
        }
    }
    ```

    #### **subscription_updated**
    Subscription filters updated
    ```json
    {
        "type": "subscription_updated",
        "event_types": ["code_graph_update", "test_completed"],
        "tags": ["python"]
    }
    ```

    #### **status_update**
    Subscription status information
    ```json
    {
        "type": "status_update",
        "subscription_id": "sub-123",
        "container_name": "my-container",
        "message_count": 45,
        "event_types": ["code_graph_update"],
        "tags": ["python"]
    }
    ```

    ## Use Cases
    - **Real-time Updates**: Receive live notifications of container events
    - **Event Filtering**: Subscribe only to specific event types
    - **Priority Filtering**: Receive only high-priority messages
    - **History Replay**: Catch up on missed messages after reconnection
    - **Tag-based Routing**: Filter messages by custom tags
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

    # Extract and validate container from WebSocket
    try:
        container_name = await extract_container_from_websocket(websocket)
    except WebSocketException as e:
        logger.error("Container validation failed for connection %s: %s", connection_id, e.reason)
        await websocket.close(code=e.code, reason=e.reason)
        return

    # Validate container belongs to tenant
    try:
        await validate_and_get_container_for_tenant(container_name, tenant_id)
    except HTTPException as e:
        logger.error(
            "Container tenant validation failed for connection %s: %s",
            connection_id,
            e.detail,
        )
        await websocket.close(code=1008, reason=f"Container validation failed: {e.detail}")
        return

    # Accept connection
    await websocket.accept()

    # Parse filters
    event_types_set: set[BroadcastEventType] | None = None
    if event_types:
        try:
            event_types_set = {
                BroadcastEventType(et.strip()) for et in event_types.split(",") if et.strip()
            }
        except ValueError as e:
            await websocket.send_json(
                {
                    "type": "error",
                    "error": f"Invalid event type: {str(e)}",
                }
            )
            await websocket.close(code=1008, reason="Invalid event type")
            return

    tags_set: set[str] | None = None
    if tags:
        tags_set = {tag.strip() for tag in tags.split(",") if tag.strip()}

    priority_filter: BroadcastPriority | None = None
    if priority:
        try:
            priority_filter = BroadcastPriority(priority.lower())
        except ValueError as e:
            await websocket.send_json(
                {
                    "type": "error",
                    "error": f"Invalid priority: {str(e)}",
                }
            )
            await websocket.close(code=1008, reason="Invalid priority")
            return

    # Create subscription
    subscription_id = None
    service = None
    try:
        service = get_broadcast_service()
        subscription_id = await service.subscribe(
            connection_id=connection_id,
            container_name=container_name,
            websocket=websocket,
            event_types=event_types_set,
            tags=tags_set,
            priority_filter=priority_filter,
        )

        # Send confirmation
        await websocket.send_json(
            {
                "type": "subscription_established",
                "subscription_id": subscription_id,
                "container_name": container_name,
                "event_types": ([et.value for et in event_types_set] if event_types_set else []),
                "tags": list(tags_set) if tags_set else [],
                "priority": priority_filter.value if priority_filter else None,
            }
        )

        # Replay history if requested
        if replay_history:
            replayed = await service.replay_history(
                subscription_id=subscription_id,
                limit=replay_limit,
            )
            await websocket.send_json(
                {
                    "type": "history_replayed",
                    "messages_replayed": replayed,
                }
            )

        # Handle incoming messages
        try:
            while True:
                data = await websocket.receive_text()
                message = json.loads(data)

                message_type = message.get("type")

                if message_type == "subscribe":
                    # Update subscription filters (not implemented - would need to modify subscription)
                    await websocket.send_json(
                        {
                            "type": "error",
                            "error": "Dynamic subscription updates not yet supported. Please reconnect with new filters.",
                        }
                    )

                elif message_type == "replay_history":
                    # Replay history
                    requested_event_types = message.get("event_types")
                    requested_tags = message.get("tags")
                    requested_limit = message.get("limit", 100)

                    event_types_filter = None
                    if requested_event_types:
                        try:
                            event_types_filter = {
                                BroadcastEventType(et) for et in requested_event_types
                            }
                        except ValueError:
                            await websocket.send_json(
                                {
                                    "type": "error",
                                    "error": "Invalid event type in replay request",
                                }
                            )
                            continue

                    tags_filter = set(requested_tags) if requested_tags else None

                    replayed = await service.replay_history(
                        subscription_id=subscription_id,
                        event_types=event_types_filter,
                        tags=tags_filter,
                        limit=requested_limit,
                    )

                    await websocket.send_json(
                        {
                            "type": "history_replayed",
                            "messages_replayed": replayed,
                        }
                    )

                elif message_type == "get_status":
                    # Get subscription status
                    subscription = await service.get_subscription(subscription_id)
                    if subscription:
                        await websocket.send_json(
                            {
                                "type": "status_update",
                                "subscription_id": subscription.subscription_id,
                                "container_name": subscription.container_name,
                                "message_count": subscription.message_count,
                                "event_types": (
                                    [et.value for et in subscription.event_types]
                                    if subscription.event_types
                                    else []
                                ),
                                "tags": (list(subscription.tags) if subscription.tags else []),
                                "priority": (
                                    subscription.priority_filter.value
                                    if subscription.priority_filter
                                    else None
                                ),
                                "last_message_at": (
                                    subscription.last_message_at.isoformat()
                                    if subscription.last_message_at
                                    else None
                                ),
                            }
                        )
                    else:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "error": "Subscription not found",
                            }
                        )

                else:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "error": f"Unknown message type: {message_type}",
                        }
                    )

        except WebSocketDisconnect:
            logger.info(
                "Broadcast WebSocket disconnected: %s (container: %s)",
                connection_id,
                container_name,
            )
        except Exception as e:
            logger.error("Error in broadcast WebSocket %s: %s", connection_id, e)
            await websocket.send_json(
                {
                    "type": "error",
                    "error": f"WebSocket error: {str(e)}",
                }
            )

    finally:
        # Cleanup subscription
        if service and subscription_id:
            try:
                await service.unsubscribe(subscription_id)
                logger.info("Broadcast subscription cleaned up: %s", subscription_id)
            except Exception as e:
                logger.error("Error cleaning up subscription %s: %s", subscription_id, e)
