"""
WebSocket Broadcast Service

Provides container-wide broadcasting capabilities for WebSocket notifications.
Supports event subscriptions, message filtering, history tracking, and replay.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections import defaultdict, deque
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid7

from fastapi import WebSocket
from fastapi.websockets import WebSocketState
from pydantic import BaseModel, ConfigDict, Field, field_serializer

logger = logging.getLogger(__name__)

# ── Cross-replica subscriber presence ─────────────────────────────────────
# One redis HASH per container, field = subscription_id, value = a JSON entry
# carrying the subscription's FILTERS plus an absolute expiry. Entries are
# refreshed by a heartbeat and pruned on read, so a replica that dies without
# unsubscribing (SIGKILL, node loss) ages out instead of lying forever.
PRESENCE_KEY_PREFIX = "coding-realtime:presence:"
DEFAULT_PRESENCE_TTL_SECONDS = 30
DEFAULT_PRESENCE_HEARTBEAT_SECONDS = 10


class BroadcastEventType(StrEnum):
    """Types of broadcast events."""

    # Code graph events
    CODE_GRAPH_UPDATE = "code_graph_update"
    CODE_GRAPH_REBUILD = "code_graph_rebuild"
    CODE_GRAPH_SYNC = "code_graph_sync"

    # Test execution events
    TEST_STARTED = "test_started"
    TEST_COMPLETED = "test_completed"
    TEST_FAILED = "test_failed"
    TEST_SUITE_STARTED = "test_suite_started"
    TEST_SUITE_COMPLETED = "test_suite_completed"

    # Build events
    BUILD_STARTED = "build_started"
    BUILD_COMPLETED = "build_completed"
    BUILD_FAILED = "build_failed"
    BUILD_PROGRESS = "build_progress"

    # Analysis events
    ANALYSIS_STARTED = "analysis_started"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_FAILED = "analysis_failed"
    ANALYSIS_PROGRESS = "analysis_progress"

    # Container events
    CONTAINER_STARTED = "container_started"
    CONTAINER_STOPPED = "container_stopped"
    CONTAINER_HEALTH = "container_health"
    CONTAINER_RESOURCE_UPDATE = "container_resource_update"

    # File system events
    FILE_CHANGED = "file_changed"
    FILE_CREATED = "file_created"
    FILE_DELETED = "file_deleted"
    DIRECTORY_CHANGED = "directory_changed"

    # Dependency events
    DEPENDENCY_INSTALLED = "dependency_installed"
    DEPENDENCY_UPDATED = "dependency_updated"
    DEPENDENCY_REMOVED = "dependency_removed"

    # Quality events
    QUALITY_ISSUE_DETECTED = "quality_issue_detected"
    QUALITY_ISSUE_RESOLVED = "quality_issue_resolved"
    QUALITY_SCAN_COMPLETED = "quality_scan_completed"

    # Security events
    SECURITY_VULNERABILITY_DETECTED = "security_vulnerability_detected"
    SECURITY_SCAN_COMPLETED = "security_scan_completed"

    # Custom events
    CUSTOM = "custom"


class BroadcastPriority(StrEnum):
    """Priority levels for broadcast messages."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class BroadcastMessage(BaseModel):
    """Model for broadcast messages."""

    message_id: str = Field(..., description="Unique message identifier")
    event_type: BroadcastEventType = Field(..., description="Type of event")
    container_name: str = Field(..., description="Container this message belongs to")
    priority: BroadcastPriority = Field(
        default=BroadcastPriority.NORMAL, description="Message priority level"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Message creation timestamp",
    )
    payload: dict[str, Any] = Field(..., description="Message payload data")
    tags: list[str] = Field(default_factory=list, description="Optional tags for filtering")
    correlation_id: str | None = Field(None, description="Correlation ID to link related messages")
    ttl_seconds: int | None = Field(
        None, description="Time to live in seconds (for history cleanup)"
    )

    model_config = ConfigDict()

    @field_serializer("timestamp")
    def serialize_datetime(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return v.isoformat()


class BroadcastSubscription(BaseModel):
    """Model for broadcast subscriptions."""

    subscription_id: str = Field(..., description="Unique subscription identifier")
    connection_id: str = Field(..., description="WebSocket connection ID")
    container_name: str = Field(..., description="Container name")
    event_types: set[BroadcastEventType] = Field(
        default_factory=set, description="Event types to receive (empty = all)"
    )
    tags: set[str] = Field(default_factory=set, description="Tags to filter by (empty = all)")
    priority_filter: BroadcastPriority | None = Field(
        None, description="Minimum priority level to receive"
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Subscription creation timestamp",
    )
    last_message_at: datetime | None = Field(None, description="Timestamp of last received message")
    message_count: int = Field(default=0, description="Number of messages received")

    model_config = ConfigDict()

    @field_serializer("created_at", "last_message_at")
    def serialize_datetime(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return v.isoformat()


class BroadcastStats(BaseModel):
    """Statistics for broadcasts."""

    container_name: str = Field(..., description="Container name")
    total_broadcasts: int = Field(default=0, description="Total number of broadcasts sent")
    total_subscriptions: int = Field(
        default=0, description="Current number of active subscriptions"
    )
    messages_in_history: int = Field(default=0, description="Number of messages in history")
    broadcasts_by_event_type: dict[str, int] = Field(
        default_factory=dict, description="Broadcast count by event type"
    )
    broadcasts_by_priority: dict[str, int] = Field(
        default_factory=dict, description="Broadcast count by priority"
    )
    average_delivery_time_ms: float = Field(
        default=0.0, description="Average message delivery time in milliseconds"
    )


class WebSocketBroadcastService:
    """
    Service for broadcasting messages to WebSocket connections.

    Features:
    - Container-scoped broadcasts
    - Event type subscriptions
    - Tag-based filtering
    - Priority-based delivery
    - Message history and replay
    - Subscription management
    - Delivery statistics
    """

    def __init__(
        self,
        max_history_per_container: int = 1000,
        history_cleanup_interval_seconds: int = 300,
        enable_history: bool = True,
        presence_ttl_seconds: int = DEFAULT_PRESENCE_TTL_SECONDS,
        presence_heartbeat_seconds: int = DEFAULT_PRESENCE_HEARTBEAT_SECONDS,
    ):
        """
        Initialize the broadcast service.

        Args:
            max_history_per_container: Maximum messages to keep in history per container
            history_cleanup_interval_seconds: Interval for history cleanup
            enable_history: Whether to enable message history
            presence_ttl_seconds: How long a cross-replica presence entry stays
                valid without a heartbeat refresh
            presence_heartbeat_seconds: How often this replica refreshes its own
                presence entries (must be well below the TTL)
        """
        self.max_history_per_container = max_history_per_container
        self.history_cleanup_interval = history_cleanup_interval_seconds
        self.enable_history = enable_history
        self.presence_ttl_seconds = presence_ttl_seconds
        self.presence_heartbeat_seconds = presence_heartbeat_seconds

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

        # Subscriptions: container_name -> subscription_id -> BroadcastSubscription
        self._subscriptions: dict[str, dict[str, BroadcastSubscription]] = defaultdict(dict)

        # Connection mapping: subscription_id -> WebSocket
        self._connections: dict[str, WebSocket] = {}

        # Message history: container_name -> deque of BroadcastMessage
        self._history: dict[str, deque[BroadcastMessage]] = defaultdict(
            lambda: deque(maxlen=max_history_per_container)
        )

        # Statistics: container_name -> BroadcastStats
        self._stats: dict[str, BroadcastStats] = defaultdict(
            lambda: BroadcastStats(
                container_name="",
                broadcasts_by_event_type={},
                broadcasts_by_priority={},
            )
        )

        # Event hooks: event_type -> List of callback functions
        self._event_hooks: dict[BroadcastEventType, list[Callable]] = defaultdict(list)

        # Delivery time tracking
        self._delivery_times: dict[str, list[float]] = defaultdict(list)

        # ── Cross-replica fan-out (D2) ────────────────────────────────────
        # In-process subscriber maps only reach connections on THIS replica.
        # To fan a broadcast out to subscribers on OTHER replicas, each publish
        # is mirrored onto a redis pub/sub channel; every replica runs a
        # subscriber that delivers remote-origin messages to its own local
        # subscribers. `_instance_id` tags our own publishes so we don't double
        # deliver them. Redis is optional — without it, delivery is local-only
        # (correct at 1 replica; the pre-D2 behaviour).
        self._redis: Any = None
        self._instance_id: str = uuid7().hex
        self._broadcast_channel: str = "coding-realtime:broadcast"
        self._subscriber_task: asyncio.Task[None] | None = None

        # Cross-replica subscriber presence heartbeat (see PRESENCE_KEY_PREFIX).
        self._presence_task: asyncio.Task[None] | None = None

        logger.info(
            "WebSocket Broadcast Service initialized (history=%s, max_per_container=%d, instance=%s)",
            "enabled" if enable_history else "disabled",
            max_history_per_container,
            self._instance_id,
        )

    async def subscribe(
        self,
        connection_id: str,
        container_name: str,
        websocket: WebSocket,
        event_types: set[BroadcastEventType] | None = None,
        tags: set[str] | None = None,
        priority_filter: BroadcastPriority | None = None,
    ) -> str:
        """
        Subscribe a connection to broadcasts.

        Args:
            connection_id: WebSocket connection ID
            container_name: Container name
            websocket: WebSocket instance
            event_types: Event types to subscribe to (None = all)
            tags: Tags to filter by (None = all)
            priority_filter: Minimum priority level (None = all)

        Returns:
            str: Subscription ID
        """
        async with self._lock:
            # Full uuid7: hex[:8] is timestamp bits only (~65 s buckets), so two
            # subscriptions from one connection_id collided and overwrote.
            subscription_id = f"{container_name}_{connection_id}_{uuid7().hex}"

            subscription = BroadcastSubscription(
                subscription_id=subscription_id,
                connection_id=connection_id,
                container_name=container_name,
                event_types=event_types or set(),
                tags=tags or set(),
                priority_filter=priority_filter,
            )

            self._subscriptions[container_name][subscription_id] = subscription
            self._connections[subscription_id] = websocket

            # Update stats
            stats = self._stats[container_name]
            stats.container_name = container_name
            stats.total_subscriptions = len(self._subscriptions[container_name])

            logger.info(
                "Broadcast subscription created: %s for container %s (filters: events=%s, tags=%s, priority=%s)",
                subscription_id,
                container_name,
                len(event_types) if event_types else "all",
                len(tags) if tags else "all",
                priority_filter.value if priority_filter else "all",
            )

        # Publish presence OUTSIDE the lock (it is network I/O). Without this a
        # publish landing on another replica would report subscriber_count=0 /
        # delivered=false for a subscriber that did receive the message.
        await self._presence_register(subscription)

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe a connection from broadcasts.

        Args:
            subscription_id: Subscription ID

        Returns:
            bool: True if unsubscribed, False if not found
        """
        removed_from: str | None = None
        async with self._lock:
            # Find and remove subscription
            for container_name, subs in self._subscriptions.items():
                if subscription_id in subs:
                    del subs[subscription_id]

                    if subscription_id in self._connections:
                        del self._connections[subscription_id]

                    # Update stats
                    stats = self._stats[container_name]
                    stats.total_subscriptions = len(self._subscriptions[container_name])

                    # Clean up empty containers
                    if not subs:
                        del self._subscriptions[container_name]

                    logger.info(
                        "Broadcast subscription removed: %s for container %s",
                        subscription_id,
                        container_name,
                    )
                    removed_from = container_name
                    break

        if removed_from is None:
            logger.debug("Subscription %s not found for removal", subscription_id)
            return False

        # Drop the shared presence entry too, so other replicas stop counting a
        # subscriber that is gone (the TTL is the backstop for ungraceful exits).
        await self._presence_unregister(removed_from, subscription_id)
        return True

    async def broadcast(
        self,
        container_name: str,
        event_type: BroadcastEventType,
        payload: dict[str, Any],
        priority: BroadcastPriority = BroadcastPriority.NORMAL,
        tags: list[str] | None = None,
        correlation_id: str | None = None,
        ttl_seconds: int | None = None,
    ) -> BroadcastMessage:
        """
        Broadcast a message to all subscribers in a container.

        Args:
            container_name: Container to broadcast to
            event_type: Type of event
            payload: Message payload
            priority: Message priority
            tags: Optional tags for filtering
            correlation_id: Optional correlation ID
            ttl_seconds: Optional TTL for history

        Returns:
            BroadcastMessage: The broadcast message
        """
        start_time = asyncio.get_event_loop().time()

        # Create message
        message = BroadcastMessage(
            message_id=uuid7().hex,
            event_type=event_type,
            container_name=container_name,
            priority=priority,
            payload=payload,
            tags=tags or [],
            correlation_id=correlation_id,
            ttl_seconds=ttl_seconds,
        )

        # Add to history
        await self._record_history(message)

        # Get subscribers
        async with self._lock:
            subscriptions = list(self._subscriptions.get(container_name, {}).values())

            # Update stats
            stats = self._stats[container_name]
            stats.container_name = container_name
            stats.total_broadcasts += 1
            stats.broadcasts_by_event_type[event_type.value] = (
                stats.broadcasts_by_event_type.get(event_type.value, 0) + 1
            )
            stats.broadcasts_by_priority[priority.value] = (
                stats.broadcasts_by_priority.get(priority.value, 0) + 1
            )
            stats.messages_in_history = len(self._history[container_name])

        # Deliver to THIS replica's subscribers, then fan out to other replicas.
        delivered_count = await self._deliver_local(message)
        await self._publish_to_redis(message)

        # Track delivery time
        delivery_time = (asyncio.get_event_loop().time() - start_time) * 1000
        async with self._lock:
            self._delivery_times[container_name].append(delivery_time)
            if len(self._delivery_times[container_name]) > 100:
                self._delivery_times[container_name] = self._delivery_times[container_name][-100:]

            # Update average delivery time
            stats = self._stats[container_name]
            stats.average_delivery_time_ms = sum(self._delivery_times[container_name]) / len(
                self._delivery_times[container_name]
            )

        # Execute event hooks
        await self._execute_event_hooks(event_type, message)

        logger.debug(
            "Broadcast sent: container=%s, event=%s, priority=%s, delivered=%d/%d, time=%.2fms",
            container_name,
            event_type.value,
            priority.value,
            delivered_count,
            len(subscriptions),
            delivery_time,
        )

        return message

    async def _deliver_local(self, message: BroadcastMessage) -> int:
        """Deliver a message to THIS replica's matching subscribers.

        Shared by the local publish path (``broadcast``) and the cross-replica
        redis subscriber, so a message reaches the same subscribers whether it
        originated here or on another replica. Updates per-subscription delivery
        stats (they genuinely received a message) but NOT container-level publish
        stats — ``total_broadcasts`` counts what THIS replica published, so
        counting a remote message here would double-count it fleet-wide.
        History is recorded separately (see ``_record_history``) because replay
        must return the same set on every replica.
        """
        async with self._lock:
            subscriptions = list(self._subscriptions.get(message.container_name, {}).values())

        delivered_count = 0
        for subscription in subscriptions:
            if self._matches_subscription(message, subscription):
                websocket = self._connections.get(subscription.subscription_id)
                if websocket and websocket.client_state == WebSocketState.CONNECTED:
                    try:
                        await websocket.send_json(
                            {
                                "type": "broadcast",
                                "message": message.model_dump(mode="json"),
                            }
                        )
                        delivered_count += 1
                        subscription.last_message_at = datetime.now(UTC)
                        subscription.message_count += 1
                    except Exception as e:
                        logger.error(
                            "Failed to deliver broadcast to subscription %s: %s",
                            subscription.subscription_id,
                            e,
                        )
        return delivered_count

    async def _record_history(self, message: BroadcastMessage) -> None:
        """Record a broadcast in this replica's history.

        Called for BOTH locally published messages and messages received from
        another replica over redis, so every replica's history converges on the
        same set. This is what makes ``replay_history`` replica-independent.

        Without it history would be per-replica: at >1 replica a client whose
        connection lands on a replica that did not publish a message replays a
        partial set and is told ``history_replayed: <fewer>`` with no error —
        silent loss of the documented reconnect catch-up. It would also make the
        design depend on sticky sessions, which nothing configures.

        Idempotent on ``message_id`` so a redelivered message is never recorded
        twice.
        """
        if not self.enable_history:
            return
        async with self._lock:
            history = self._history[message.container_name]
            if any(existing.message_id == message.message_id for existing in history):
                return
            history.append(message)
            stats = self._stats[message.container_name]
            stats.container_name = message.container_name
            stats.messages_in_history = len(history)

    # ── Cross-replica fan-out (D2) ────────────────────────────────────────

    def attach_redis(self, redis_client: Any) -> None:
        """Wire a redis client for cross-replica pub/sub fan-out."""
        self._redis = redis_client

    async def _publish_to_redis(self, message: BroadcastMessage) -> None:
        """Mirror a broadcast onto the redis channel so other replicas deliver
        it to their local subscribers. Best-effort: a redis failure must not
        break local delivery (it is logged, not raised)."""
        if self._redis is None:
            return
        try:
            await self._redis.publish(
                self._broadcast_channel,
                json.dumps(
                    {"origin": self._instance_id, "message": message.model_dump(mode="json")}
                ),
            )
        except Exception as exc:
            logger.warning(
                "Cross-replica broadcast publish failed (local delivery unaffected): %s", exc
            )

    async def start_subscriber(self) -> None:
        """Start the background subscriber that delivers broadcasts published by
        OTHER replicas to this replica's local subscribers. No-op without redis
        or if already running."""
        if self._redis is None or self._subscriber_task is not None:
            return
        self._subscriber_task = asyncio.create_task(self._subscriber_loop())

    async def stop_subscriber(self) -> None:
        """Cancel the background subscriber (called on shutdown)."""
        if self._subscriber_task is not None:
            self._subscriber_task.cancel()
            try:
                await self._subscriber_task
            except asyncio.CancelledError:
                pass
            self._subscriber_task = None

    async def _subscriber_loop(self) -> None:
        """Subscribe to the broadcast channel and deliver remote-origin messages
        locally. Reconnects on transient redis errors; skips our own publishes
        (already delivered locally) via the instance-id origin tag."""
        while True:
            pubsub = None
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(self._broadcast_channel)
                logger.info(
                    "Cross-replica broadcast subscriber active on %s (instance %s)",
                    self._broadcast_channel,
                    self._instance_id,
                )
                async for raw in pubsub.listen():
                    if not isinstance(raw, dict) or raw.get("type") != "message":
                        continue
                    try:
                        envelope = json.loads(raw["data"])
                    except (ValueError, TypeError):
                        continue
                    if not isinstance(envelope, dict):
                        continue
                    # Skip our own publish — we already delivered it locally.
                    if envelope.get("origin") == self._instance_id:
                        continue
                    payload = envelope.get("message")
                    if not isinstance(payload, dict):
                        continue
                    try:
                        message = BroadcastMessage(**payload)
                    except Exception as exc:
                        logger.warning("Dropping malformed cross-replica broadcast: %s", exc)
                        continue
                    # Record BEFORE delivering: a client that reconnects to this
                    # replica must be able to replay a remotely-published message.
                    await self._record_history(message)
                    await self._deliver_local(message)
            except asyncio.CancelledError:
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass
                raise
            except Exception as exc:
                logger.warning("Broadcast subscriber loop error, reconnecting in 2s: %s", exc)
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass
                await asyncio.sleep(2)

    # ── Cross-replica subscriber presence ─────────────────────────────────
    # `_subscriptions` is per-process, so a publish that lands on replica A
    # while the subscriber is connected to replica B saw ZERO subscribers and
    # answered `delivered: false` even though redis pub/sub DID deliver the
    # message — inverting the very honesty property the publish endpoint
    # exists to provide, on roughly half of all publishes at 2 replicas.
    #
    # Presence is therefore mirrored into a shared redis HASH per container:
    #   key   coding-realtime:presence:<container>
    #   field <subscription_id>
    #   value {"expires_at": <epoch>, "event_types": [...], "tags": [...],
    #          "priority": <str|null>, "instance": <replica id>}
    #
    # The filters travel WITH the entry so the count answers the question the
    # caller actually asked ("would anyone have received THIS message?") rather
    # than the weaker "is anyone attached to this container at all?" — a
    # subscriber filtered to `test_completed` must not make a `build_failed`
    # publish report delivered=true.
    #
    # Staleness is handled in both directions:
    #   * entries carry an absolute expiry and are refreshed by a heartbeat, so
    #     a replica killed without clean unsubscribes ages out instead of
    #     inflating the count forever;
    #   * the heartbeat only refreshes subscriptions whose websocket is still
    #     CONNECTED, so a half-open socket that never raises WebSocketDisconnect
    #     also ages out;
    #   * expired fields are pruned on read, and the whole key carries a redis
    #     TTL so an abandoned container leaves nothing behind.

    def _presence_key(self, container_name: str) -> str:
        return f"{PRESENCE_KEY_PREFIX}{container_name}"

    def _presence_value(self, subscription: BroadcastSubscription, now: float) -> str:
        return json.dumps(
            {
                "expires_at": now + self.presence_ttl_seconds,
                "event_types": sorted(et.value for et in subscription.event_types),
                "tags": sorted(subscription.tags),
                "priority": (
                    subscription.priority_filter.value if subscription.priority_filter else None
                ),
                "instance": self._instance_id,
            }
        )

    async def _presence_register(self, subscription: BroadcastSubscription) -> None:
        """Publish one subscription into the shared presence set (best effort)."""
        if self._redis is None:
            return
        key = self._presence_key(subscription.container_name)
        try:
            await self._redis.hset(
                key,
                subscription.subscription_id,
                self._presence_value(subscription, time.time()),
            )
            await self._redis.expire(key, self.presence_ttl_seconds * 3)
        except Exception as exc:
            # Local delivery is unaffected; the count falls back to local-only.
            logger.warning("Presence register failed for %s: %s", subscription.subscription_id, exc)

    async def _presence_unregister(self, container_name: str, subscription_id: str) -> None:
        """Remove one subscription from the shared presence set (best effort)."""
        if self._redis is None:
            return
        try:
            await self._redis.hdel(self._presence_key(container_name), subscription_id)
        except Exception as exc:
            logger.warning("Presence unregister failed for %s: %s", subscription_id, exc)

    async def start_presence_heartbeat(self) -> None:
        """Start refreshing this replica's presence entries. No-op without redis
        or if already running."""
        if self._redis is None or self._presence_task is not None:
            return
        self._presence_task = asyncio.create_task(self._presence_heartbeat_loop())

    async def stop_presence_heartbeat(self) -> None:
        """Cancel the presence heartbeat (called on shutdown)."""
        if self._presence_task is not None:
            self._presence_task.cancel()
            try:
                await self._presence_task
            except asyncio.CancelledError:
                pass
            self._presence_task = None

    async def _presence_heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.presence_heartbeat_seconds)
                await self.refresh_presence()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Presence heartbeat iteration failed: %s", exc)

    async def refresh_presence(self) -> int:
        """Re-stamp every live local subscription's presence entry.

        A subscription whose websocket is no longer CONNECTED is NOT refreshed
        and is actively deleted: a half-open socket never raises
        ``WebSocketDisconnect``, so without this it would keep claiming a
        subscriber that can no longer be reached.

        Returns the number of entries refreshed (for tests/observability).
        """
        if self._redis is None:
            return 0

        async with self._lock:
            snapshot = {
                container: list(subs.values()) for container, subs in self._subscriptions.items()
            }
            connections = dict(self._connections)

        now = time.time()
        refreshed = 0
        for container_name, subscriptions in snapshot.items():
            key = self._presence_key(container_name)
            for subscription in subscriptions:
                websocket = connections.get(subscription.subscription_id)
                alive = websocket is not None and websocket.client_state == WebSocketState.CONNECTED
                try:
                    if alive:
                        await self._redis.hset(
                            key,
                            subscription.subscription_id,
                            self._presence_value(subscription, now),
                        )
                        refreshed += 1
                    else:
                        await self._redis.hdel(key, subscription.subscription_id)
                except Exception as exc:
                    logger.warning(
                        "Presence refresh failed for %s: %s", subscription.subscription_id, exc
                    )
            try:
                await self._redis.expire(key, self.presence_ttl_seconds * 3)
            except Exception as exc:
                logger.debug("Presence key expire refresh failed for %s: %s", key, exc)
        return refreshed

    async def purge_local_presence(self) -> None:
        """Remove all of THIS replica's presence entries.

        Called on graceful shutdown so a rolling restart does not leave the
        surviving replica over-reporting for a whole TTL window.
        """
        if self._redis is None:
            return
        async with self._lock:
            snapshot = {
                container: list(subs.keys()) for container, subs in self._subscriptions.items()
            }
        for container_name, subscription_ids in snapshot.items():
            for subscription_id in subscription_ids:
                await self._presence_unregister(container_name, subscription_id)

    def _presence_entry_matches(
        self,
        container_name: str,
        subscription_id: str,
        entry: dict[str, Any],
        message: (BroadcastMessage),
    ) -> bool:
        """Evaluate a stored presence entry's filters against a message.

        Rebuilds a ``BroadcastSubscription`` from the stored filters and reuses
        ``_matches_subscription`` so the count can never drift from what the
        delivery path actually does.
        """
        raw_event_types = entry.get("event_types") or []
        event_types: set[BroadcastEventType] = set()
        for value in raw_event_types:
            try:
                event_types.add(BroadcastEventType(value))
            except ValueError:
                # An event type this replica does not know (version skew). It
                # cannot equal our message's type, so dropping it is exact:
                # a filter that listed ONLY unknown types stays non-empty in
                # `raw_event_types`, and the empty parsed set below is treated
                # as "matches nothing" rather than the "no filter = all" case.
                continue
        if raw_event_types and not event_types:
            return False

        raw_priority = entry.get("priority")
        try:
            priority_filter = BroadcastPriority(raw_priority) if raw_priority else None
        except ValueError:
            priority_filter = None

        subscription = BroadcastSubscription(
            subscription_id=subscription_id,
            connection_id="",
            container_name=container_name,
            event_types=event_types,
            tags=set(entry.get("tags") or []),
            priority_filter=priority_filter,
        )
        return self._matches_subscription(message, subscription)

    async def count_local_matching_subscribers(self, message: BroadcastMessage) -> int:
        """Subscribers on THIS replica whose filters match ``message`` and whose
        websocket is still connected."""
        async with self._lock:
            subscriptions = list(self._subscriptions.get(message.container_name, {}).values())
            connections = dict(self._connections)
        count = 0
        for subscription in subscriptions:
            websocket = connections.get(subscription.subscription_id)
            if websocket is None or websocket.client_state != WebSocketState.CONNECTED:
                continue
            if self._matches_subscription(message, subscription):
                count += 1
        return count

    async def count_reachable_subscribers(self, message: BroadcastMessage) -> int:
        """Count subscribers on ANY replica that match this message's filters.

        This is what makes ``POST /api/broadcast/publish`` honest at >1 replica:
        the answer no longer depends on which replica the publisher happened to
        land on.

        The shared presence set already includes this replica's own entries, so
        the two counts are combined with ``max`` rather than summed. ``max`` (not
        "trust redis") matters because a redis blip can leave our own entries
        unregistered while local subscribers are provably connected and provably
        received the message — reporting 0 there would be a fresh lie in the
        opposite direction.

        Falls back to the local count when redis is absent or errors, which is
        exactly correct at 1 replica.
        """
        local_count = await self.count_local_matching_subscribers(message)
        if self._redis is None:
            return local_count

        key = self._presence_key(message.container_name)
        try:
            raw_entries = await self._redis.hgetall(key)
        except Exception as exc:
            logger.warning(
                "Presence lookup failed for %s; falling back to this replica's count: %s",
                message.container_name,
                exc,
            )
            return local_count

        if not isinstance(raw_entries, dict):
            return local_count

        now = time.time()
        expired: list[str] = []
        shared_count = 0
        for field, raw_value in raw_entries.items():
            subscription_id = field.decode() if isinstance(field, bytes) else str(field)
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode()
            try:
                entry = json.loads(raw_value)
            except (ValueError, TypeError):
                expired.append(subscription_id)
                continue
            if not isinstance(entry, dict):
                expired.append(subscription_id)
                continue
            expires_at = entry.get("expires_at")
            if not isinstance(expires_at, (int, float)) or expires_at <= now:
                # Stale: the owning replica stopped heartbeating (crash, node
                # loss, half-open socket). Counting it would re-introduce the
                # lie in the other direction.
                expired.append(subscription_id)
                continue
            if self._presence_entry_matches(
                message.container_name, subscription_id, entry, message
            ):
                shared_count += 1

        if expired:
            try:
                await self._redis.hdel(key, *expired)
            except Exception as exc:
                logger.debug("Pruning stale presence entries failed for %s: %s", key, exc)

        return max(shared_count, local_count)

    def _matches_subscription(
        self,
        message: BroadcastMessage,
        subscription: BroadcastSubscription,
    ) -> bool:
        """
        Check if a message matches a subscription's filters.

        Args:
            message: Broadcast message
            subscription: Subscription to check

        Returns:
            bool: True if message matches subscription filters
        """
        # Check event type filter
        if subscription.event_types and message.event_type not in subscription.event_types:
            return False

        # Check priority filter
        if subscription.priority_filter:
            priority_levels = {
                BroadcastPriority.LOW: 0,
                BroadcastPriority.NORMAL: 1,
                BroadcastPriority.HIGH: 2,
                BroadcastPriority.CRITICAL: 3,
            }
            if priority_levels[message.priority] < priority_levels[subscription.priority_filter]:
                return False

        # Check tags filter
        if subscription.tags:
            message_tags = set(message.tags)
            if not message_tags.intersection(subscription.tags):
                return False

        return True

    async def get_history(
        self,
        container_name: str,
        event_types: set[BroadcastEventType] | None = None,
        tags: set[str] | None = None,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[BroadcastMessage]:
        """
        Get message history for a container.

        Args:
            container_name: Container name
            event_types: Filter by event types
            tags: Filter by tags
            since: Only messages after this timestamp
            limit: Maximum number of messages

        Returns:
            List[BroadcastMessage]: Filtered message history
        """
        async with self._lock:
            messages = list(self._history.get(container_name, []))

        # Apply filters
        if event_types:
            messages = [m for m in messages if m.event_type in event_types]

        if tags:
            messages = [m for m in messages if set(m.tags).intersection(tags)]

        if since:
            messages = [m for m in messages if m.timestamp >= since]

        # Sort by timestamp (newest first)
        messages.sort(key=lambda m: m.timestamp, reverse=True)

        # Apply limit
        if limit:
            messages = messages[:limit]

        return messages

    async def replay_history(
        self,
        subscription_id: str,
        event_types: set[BroadcastEventType] | None = None,
        tags: set[str] | None = None,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> int:
        """
        Replay message history to a subscriber.

        Args:
            subscription_id: Subscription ID to replay to
            event_types: Filter by event types
            tags: Filter by tags
            since: Only messages after this timestamp
            limit: Maximum number of messages

        Returns:
            int: Number of messages replayed
        """
        # Get subscription
        async with self._lock:
            subscription = None
            for subs in self._subscriptions.values():
                if subscription_id in subs:
                    subscription = subs[subscription_id]
                    break

        if not subscription:
            logger.warning("Subscription %s not found for replay", subscription_id)
            return 0

        # Get history
        messages = await self.get_history(
            subscription.container_name,
            event_types=event_types,
            tags=tags,
            since=since,
            limit=limit,
        )

        # Deliver messages
        websocket = self._connections.get(subscription_id)
        if not websocket or websocket.client_state != WebSocketState.CONNECTED:
            logger.warning("WebSocket not connected for subscription %s", subscription_id)
            return 0

        replayed = 0
        for message in reversed(messages):  # Replay in chronological order
            if self._matches_subscription(message, subscription):
                try:
                    await websocket.send_json(
                        {
                            "type": "broadcast_replay",
                            "message": message.model_dump(mode="json"),
                        }
                    )
                    replayed += 1
                except Exception as e:
                    logger.error("Failed to replay message to %s: %s", subscription_id, e)
                    break

        logger.info(
            "Replayed %d messages to subscription %s (container: %s)",
            replayed,
            subscription_id,
            subscription.container_name,
        )

        return replayed

    async def get_subscription(self, subscription_id: str) -> BroadcastSubscription | None:
        """
        Get subscription details.

        Args:
            subscription_id: Subscription ID

        Returns:
            Optional[BroadcastSubscription]: Subscription if found
        """
        async with self._lock:
            for subs in self._subscriptions.values():
                if subscription_id in subs:
                    return subs[subscription_id]
        return None

    async def get_container_subscriptions(self, container_name: str) -> list[BroadcastSubscription]:
        """
        Get all subscriptions for a container.

        Args:
            container_name: Container name

        Returns:
            List[BroadcastSubscription]: List of subscriptions
        """
        async with self._lock:
            return list(self._subscriptions.get(container_name, {}).values())

    async def get_stats(self, container_name: str) -> BroadcastStats:
        """
        Get broadcast statistics for a container.

        Args:
            container_name: Container name

        Returns:
            BroadcastStats: Statistics
        """
        async with self._lock:
            stats = self._stats[container_name]
            if not stats.container_name:
                stats.container_name = container_name
            return stats

    async def get_all_stats(self) -> dict[str, BroadcastStats]:
        """
        Get broadcast statistics for all containers.

        Returns:
            Dict[str, BroadcastStats]: Stats keyed by container name
        """
        async with self._lock:
            return dict(self._stats)

    def register_event_hook(
        self,
        event_type: BroadcastEventType,
        callback: Callable[[BroadcastMessage], None],
    ) -> None:
        """
        Register a callback hook for an event type.

        Args:
            event_type: Event type to hook
            callback: Async callback function
        """
        self._event_hooks[event_type].append(callback)
        logger.info("Registered event hook for %s", event_type.value)

    async def _execute_event_hooks(
        self,
        event_type: BroadcastEventType,
        message: BroadcastMessage,
    ) -> None:
        """Execute all registered hooks for an event type."""
        if event_type in self._event_hooks:
            for callback in self._event_hooks[event_type]:
                try:
                    if inspect.iscoroutinefunction(callback):
                        await callback(message)
                    else:
                        callback(message)
                except Exception as e:
                    logger.error("Error executing event hook for %s: %s", event_type.value, e)

    async def cleanup_expired_messages(self) -> int:
        """
        Clean up expired messages from history based on TTL.

        Returns:
            int: Number of messages cleaned up
        """
        async with self._lock:
            cleaned = 0
            now = datetime.now(UTC)

            for container_name, history in self._history.items():
                # Filter out expired messages
                original_size = len(history)
                history_list = list(history)

                # Keep only non-expired messages
                filtered = [
                    msg
                    for msg in history_list
                    if msg.ttl_seconds is None
                    or ((now - msg.timestamp).total_seconds() < msg.ttl_seconds)
                ]

                # Update history
                history.clear()
                history.extend(filtered)

                cleaned += original_size - len(history)

                # Update stats
                stats = self._stats[container_name]
                stats.messages_in_history = len(history)

            if cleaned > 0:
                logger.info("Cleaned up %d expired messages from history", cleaned)

            return cleaned


# Global broadcast service instance
_broadcast_service: WebSocketBroadcastService | None = None


def get_broadcast_service(
    max_history_per_container: int = 1000,
    enable_history: bool = True,
) -> WebSocketBroadcastService:
    """
    Get the global broadcast service instance.

    Args:
        max_history_per_container: Maximum messages per container
        enable_history: Whether to enable history

    Returns:
        WebSocketBroadcastService: The global instance
    """
    global _broadcast_service
    if _broadcast_service is None:
        _broadcast_service = WebSocketBroadcastService(
            max_history_per_container=max_history_per_container,
            enable_history=enable_history,
        )
    return _broadcast_service


async def cleanup_task_runner(
    service: WebSocketBroadcastService,
    cleanup_interval_seconds: int = 300,
) -> None:
    """
    Background task to periodically clean up expired messages.

    Args:
        service: Broadcast service instance
        cleanup_interval_seconds: Cleanup interval
    """
    logger.info("Broadcast cleanup task started (interval=%ds)", cleanup_interval_seconds)

    while True:
        try:
            await asyncio.sleep(cleanup_interval_seconds)
            cleaned = await service.cleanup_expired_messages()
            if cleaned > 0:
                logger.info("Cleanup task removed %d expired messages", cleaned)
        except asyncio.CancelledError:
            logger.info("Broadcast cleanup task cancelled")
            break
        except Exception as e:
            logger.error("Error in cleanup task: %s", e, exc_info=True)
