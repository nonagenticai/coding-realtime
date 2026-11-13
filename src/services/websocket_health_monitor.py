"""
WebSocket Health Monitor Service

Provides comprehensive health monitoring for WebSocket connections including:
- Ping/pong heartbeat mechanism
- Connection health status tracking
- Stale connection detection per container
- Automatic connection cleanup
- Health metrics and statistics
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid7

from fastapi import WebSocket
from fastapi.websockets import WebSocketState
from pydantic import BaseModel, ConfigDict, Field, field_serializer

logger = logging.getLogger(__name__)


class ConnectionHealth(StrEnum):
    """Health status of a WebSocket connection."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    DISCONNECTED = "disconnected"


class PingStatus(BaseModel):
    """Status of a ping/pong operation."""

    sent_at: datetime = Field(..., description="When the ping was sent")
    received_at: datetime | None = Field(None, description="When the pong was received")
    latency_ms: float | None = Field(None, description="Round-trip latency in milliseconds")
    timed_out: bool = Field(False, description="Whether the ping timed out")

    model_config = ConfigDict()

    @field_serializer("sent_at", "received_at")
    def serialize_datetime(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return v.isoformat()


class ConnectionHealthStatus(BaseModel):
    """Health status information for a WebSocket connection."""

    connection_id: str = Field(..., description="Connection identifier")
    container_name: str = Field(..., description="Container this connection belongs to")
    health: ConnectionHealth = Field(..., description="Current health status")
    last_ping_sent: datetime | None = Field(None, description="Last ping sent timestamp")
    last_pong_received: datetime | None = Field(None, description="Last pong received timestamp")
    last_activity: datetime = Field(..., description="Last activity timestamp")
    average_latency_ms: float | None = Field(None, description="Average ping latency")
    failed_pings: int = Field(0, description="Number of consecutive failed pings")
    total_pings_sent: int = Field(0, description="Total pings sent")
    total_pongs_received: int = Field(0, description="Total pongs received")
    connection_age_seconds: float = Field(..., description="Age of connection in seconds")
    is_responsive: bool = Field(True, description="Whether connection is responsive")

    model_config = ConfigDict()

    @field_serializer("last_ping_sent", "last_pong_received", "last_activity")
    def serialize_datetime(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return v.isoformat()


class ContainerHealthStats(BaseModel):
    """Health statistics for connections to a container."""

    container_name: str = Field(..., description="Container name")
    total_connections: int = Field(..., description="Total active connections")
    healthy_connections: int = Field(0, description="Number of healthy connections")
    degraded_connections: int = Field(0, description="Number of degraded connections")
    stale_connections: int = Field(0, description="Number of stale connections")
    average_latency_ms: float | None = Field(
        None, description="Average latency across all connections"
    )
    total_failed_pings: int = Field(0, description="Total failed pings across all connections")
    last_cleanup: datetime | None = Field(None, description="Last cleanup operation timestamp")

    model_config = ConfigDict()

    @field_serializer("last_cleanup")
    def serialize_datetime(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return v.isoformat()


class WebSocketHealthMonitor:
    """
    Monitor and maintain health of WebSocket connections.

    Features:
    - Automatic ping/pong heartbeat
    - Connection health tracking
    - Stale connection detection
    - Per-container health statistics
    - Automatic cleanup of unhealthy connections
    """

    def __init__(
        self,
        ping_interval_seconds: int = 30,
        ping_timeout_seconds: int = 10,
        max_failed_pings: int = 3,
        stale_threshold_seconds: int = 300,
    ):
        """
        Initialize the health monitor.

        Args:
            ping_interval_seconds: Interval between ping messages
            ping_timeout_seconds: Timeout for pong responses
            max_failed_pings: Maximum consecutive failed pings before marking as stale
            stale_threshold_seconds: Time without activity before marking as stale
        """
        self.ping_interval = ping_interval_seconds
        self.ping_timeout = ping_timeout_seconds
        self.max_failed_pings = max_failed_pings
        self.stale_threshold = stale_threshold_seconds

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

        # Connection tracking
        self._connection_health: dict[str, ConnectionHealthStatus] = {}
        # connection_id -> tenant that opened it. Tenant-facing cleanup is scoped
        # by this so one tenant can never close another tenant's connections.
        self._connection_tenants: dict[str, str | None] = {}
        self._container_connections: dict[str, set[str]] = defaultdict(set)
        self._ping_tasks: dict[str, asyncio.Task] = {}
        self._ping_history: dict[str, list[PingStatus]] = defaultdict(list)

        # Statistics
        self._cleanup_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"total_cleanups": 0, "last_cleanup": None}
        )

        logger.info(
            "WebSocket Health Monitor initialized (ping_interval=%ds, timeout=%ds, max_failed=%d)",
            self.ping_interval,
            self.ping_timeout,
            self.max_failed_pings,
        )

    async def register_connection(
        self,
        connection_id: str,
        container_name: str,
        websocket: WebSocket,
        tenant_id: str | None = None,
    ) -> None:
        """
        Register a connection for health monitoring.

        Args:
            connection_id: Connection identifier
            container_name: Container name
            websocket: WebSocket instance
            tenant_id: Tenant verified on the handshake (scopes tenant-facing cleanup)
        """
        async with self._lock:
            now = datetime.now(UTC)

            # Create health status
            health_status = ConnectionHealthStatus(
                connection_id=connection_id,
                container_name=container_name,
                health=ConnectionHealth.HEALTHY,
                last_activity=now,
                connection_age_seconds=0,
            )

            self._connection_health[connection_id] = health_status
            self._connection_tenants[connection_id] = tenant_id
            self._container_connections[container_name].add(connection_id)

            # Start ping task
            ping_task = asyncio.create_task(self._ping_loop(connection_id, websocket))
            self._ping_tasks[connection_id] = ping_task

            logger.info(
                "WebSocket health monitoring started for connection %s (container: %s)",
                connection_id,
                container_name,
            )

    async def unregister_connection(self, connection_id: str) -> bool:
        """
        Unregister a connection from health monitoring.

        Args:
            connection_id: Connection identifier

        Returns:
            bool: True if connection was found and removed
        """
        async with self._lock:
            health_status = self._connection_health.get(connection_id)
            if not health_status:
                return False

            container_name = health_status.container_name

            # Cancel ping task
            if connection_id in self._ping_tasks:
                self._ping_tasks[connection_id].cancel()
                try:
                    await self._ping_tasks[connection_id]
                except asyncio.CancelledError:
                    pass
                del self._ping_tasks[connection_id]

            # Remove from tracking
            del self._connection_health[connection_id]
            self._connection_tenants.pop(connection_id, None)
            self._container_connections[container_name].discard(connection_id)
            if not self._container_connections[container_name]:
                del self._container_connections[container_name]

            # Clean up ping history
            if connection_id in self._ping_history:
                del self._ping_history[connection_id]

            logger.info(
                "WebSocket health monitoring stopped for connection %s (container: %s)",
                connection_id,
                container_name,
            )

            return True

    async def update_activity(self, connection_id: str) -> bool:
        """
        Update the last activity timestamp for a connection.

        Args:
            connection_id: Connection identifier

        Returns:
            bool: True if connection was found and updated
        """
        async with self._lock:
            health_status = self._connection_health.get(connection_id)
            if health_status:
                health_status.last_activity = datetime.now(UTC)
                return True
            return False

    def _owned_by(self, connection_id: str, tenant_id: str | None) -> bool:
        """Tenant filter. ``tenant_id=None`` is the internal (unscoped) form.

        A connection with no recorded tenant belongs to no tenant (fail closed).
        Caller must hold ``self._lock``.
        """
        if tenant_id is None:
            return True
        return bool(tenant_id) and self._connection_tenants.get(connection_id) == tenant_id

    async def get_connection_health(
        self, connection_id: str, tenant_id: str | None = None
    ) -> ConnectionHealthStatus | None:
        """
        Get health status for a specific connection.

        Args:
            connection_id: Connection identifier
            tenant_id: When given, a connection another tenant opened is
                reported as absent (``None``) — indistinguishable from unknown.

        Returns:
            Optional[ConnectionHealthStatus]: Health status if found
        """
        async with self._lock:
            if not self._owned_by(connection_id, tenant_id):
                return None
            return self._connection_health.get(connection_id)

    async def get_container_health(
        self, container_name: str, tenant_id: str | None = None
    ) -> ContainerHealthStats | None:
        """
        Get health statistics for all connections to a container.

        Args:
            container_name: Container name
            tenant_id: When given, ONLY connections this tenant opened count.

        Returns:
            Optional[ContainerHealthStats]: Health statistics if container has connections
        """
        async with self._lock:
            connection_ids = {
                c
                for c in self._container_connections.get(container_name, set())
                if self._owned_by(c, tenant_id)
            }
            if not connection_ids:
                return None

            connections = [
                self._connection_health[conn_id]
                for conn_id in connection_ids
                if conn_id in self._connection_health
            ]

            if not connections:
                return None

            # Calculate statistics
            healthy = sum(1 for c in connections if c.health == ConnectionHealth.HEALTHY)
            degraded = sum(1 for c in connections if c.health == ConnectionHealth.DEGRADED)
            stale = sum(1 for c in connections if c.health == ConnectionHealth.STALE)

            # Calculate average latency
            latencies = [
                c.average_latency_ms for c in connections if c.average_latency_ms is not None
            ]
            avg_latency = sum(latencies) / len(latencies) if latencies else None

            total_failed = sum(c.failed_pings for c in connections)

            cleanup_stats = self._cleanup_stats.get(container_name, {})
            last_cleanup = cleanup_stats.get("last_cleanup")

            return ContainerHealthStats(
                container_name=container_name,
                total_connections=len(connections),
                healthy_connections=healthy,
                degraded_connections=degraded,
                stale_connections=stale,
                average_latency_ms=avg_latency,
                total_failed_pings=total_failed,
                last_cleanup=last_cleanup,
            )

    async def get_all_container_health(
        self, tenant_id: str | None = None
    ) -> dict[str, ContainerHealthStats]:
        """
        Get health statistics for all containers.

        Args:
            tenant_id: When given, ONLY this tenant's connections are counted, so
                a container with none of them does not appear at all.

        Returns:
            Dict[str, ContainerHealthStats]: Health statistics keyed by container name
        """
        async with self._lock:
            container_names = list(self._container_connections.keys())

        stats = {}
        for container_name in container_names:
            health = await self.get_container_health(container_name, tenant_id=tenant_id)
            if health:
                stats[container_name] = health

        return stats

    async def detect_stale_connections(
        self,
        container_name: str | None = None,
        tenant_id: str | None = None,
    ) -> list[str]:
        """
        Detect stale connections (either for specific container or all).

        Args:
            container_name: Optional container name to check (if None, checks all)
            tenant_id: When given, ONLY connections opened by this tenant are
                considered. ``None`` is the internal (background task) form.

        Returns:
            List[str]: List of stale connection IDs
        """
        async with self._lock:
            now = datetime.now(UTC)
            stale_connections = []

            # Determine which connections to check
            if container_name:
                connection_ids = self._container_connections.get(container_name, set())
            else:
                connection_ids = set(self._connection_health.keys())
            if tenant_id is not None:
                connection_ids = {
                    c for c in connection_ids if self._connection_tenants.get(c) == tenant_id
                }

            for conn_id in connection_ids:
                health_status = self._connection_health.get(conn_id)
                if not health_status:
                    continue

                # Check if connection is stale
                time_since_activity = (now - health_status.last_activity).total_seconds()

                is_stale = (
                    health_status.failed_pings >= self.max_failed_pings
                    or time_since_activity > self.stale_threshold
                )

                if is_stale:
                    stale_connections.append(conn_id)
                    health_status.health = ConnectionHealth.STALE
                    health_status.is_responsive = False
                    logger.warning(
                        "Stale connection detected: %s (container: %s, failed_pings: %d, idle: %.1fs)",
                        conn_id,
                        health_status.container_name,
                        health_status.failed_pings,
                        time_since_activity,
                    )

            return stale_connections

    async def cleanup_stale_connections(
        self,
        container_name: str | None = None,
        auto_close: bool = True,
        tenant_id: str | None = None,
    ) -> int:
        """
        Clean up stale connections.

        Args:
            container_name: Optional container name to clean up (if None, cleans all)
            auto_close: Whether to automatically close WebSocket connections
            tenant_id: When given, ONLY this tenant's connections are cleaned

        Returns:
            int: Number of connections cleaned up
        """
        stale_connections = await self.detect_stale_connections(container_name, tenant_id=tenant_id)

        cleanup_count = 0
        for conn_id in stale_connections:
            # Unregister from health monitoring
            if await self.unregister_connection(conn_id):
                cleanup_count += 1

        # Update cleanup stats
        if container_name and cleanup_count > 0:
            async with self._lock:
                self._cleanup_stats[container_name]["total_cleanups"] += cleanup_count
                self._cleanup_stats[container_name]["last_cleanup"] = datetime.now(UTC)

        if cleanup_count > 0:
            logger.info(
                "Cleaned up %d stale connections%s",
                cleanup_count,
                f" for container {container_name}" if container_name else "",
            )

        return cleanup_count

    async def get_global_health_stats(self, tenant_id: str | None = None) -> dict[str, Any]:
        """
        Get global health statistics across all connections.

        Args:
            tenant_id: When given, every figure is computed over this tenant's
                connections only ("global" = across the caller's containers).
                The monitor configuration is service-wide and always returned.

        Returns:
            Dict[str, Any]: Global health statistics
        """
        async with self._lock:
            health = [h for c, h in self._connection_health.items() if self._owned_by(c, tenant_id)]
            total_connections = len(health)
            total_containers = len({h.container_name for h in health})

            # Count by health status
            healthy = sum(1 for h in health if h.health == ConnectionHealth.HEALTHY)
            degraded = sum(1 for h in health if h.health == ConnectionHealth.DEGRADED)
            stale = sum(1 for h in health if h.health == ConnectionHealth.STALE)

            # Calculate average latency
            latencies = [h.average_latency_ms for h in health if h.average_latency_ms is not None]
            avg_latency = sum(latencies) / len(latencies) if latencies else None

            # Total ping statistics
            total_pings_sent = sum(h.total_pings_sent for h in health)
            total_pongs_received = sum(h.total_pongs_received for h in health)
            total_failed_pings = sum(h.failed_pings for h in health)

            return {
                "total_connections": total_connections,
                "total_containers": total_containers,
                "healthy_connections": healthy,
                "degraded_connections": degraded,
                "stale_connections": stale,
                "average_latency_ms": avg_latency,
                "total_pings_sent": total_pings_sent,
                "total_pongs_received": total_pongs_received,
                "total_failed_pings": total_failed_pings,
                "ping_success_rate": (
                    (total_pongs_received / total_pings_sent * 100) if total_pings_sent > 0 else 100
                ),
                "ping_interval_seconds": self.ping_interval,
                "ping_timeout_seconds": self.ping_timeout,
                "max_failed_pings": self.max_failed_pings,
                "stale_threshold_seconds": self.stale_threshold,
            }

    async def _ping_loop(self, connection_id: str, websocket: WebSocket) -> None:
        """
        Background task for sending periodic pings.

        Args:
            connection_id: Connection identifier
            websocket: WebSocket instance
        """
        try:
            while True:
                await asyncio.sleep(self.ping_interval)

                # Check if connection still exists
                async with self._lock:
                    health_status = self._connection_health.get(connection_id)
                    if not health_status:
                        break

                # Send ping
                await self._send_ping(connection_id, websocket)

        except asyncio.CancelledError:
            logger.debug("Ping loop cancelled for connection %s", connection_id)
            raise
        except Exception as e:
            logger.error("Error in ping loop for connection %s: %s", connection_id, e)

    async def _send_ping(self, connection_id: str, websocket: WebSocket) -> None:
        """
        Send a ping and wait for pong.

        Args:
            connection_id: Connection identifier
            websocket: WebSocket instance
        """
        async with self._lock:
            health_status = self._connection_health.get(connection_id)
            if not health_status:
                return

        # Check if WebSocket is still connected
        if websocket.client_state != WebSocketState.CONNECTED:
            async with self._lock:
                if health_status:
                    health_status.health = ConnectionHealth.DISCONNECTED
                    health_status.is_responsive = False
            return

        now = datetime.now(UTC)
        ping_id = str(uuid7())

        # Create ping status
        ping_status = PingStatus(sent_at=now)

        try:
            # Send ping message
            await asyncio.wait_for(
                websocket.send_json(
                    {
                        "type": "ping",
                        "ping_id": ping_id,
                        "timestamp": now.isoformat(),
                        "container_name": health_status.container_name,
                    }
                ),
                timeout=self.ping_timeout,
            )

            async with self._lock:
                health_status.last_ping_sent = now
                health_status.total_pings_sent += 1

            # Wait for pong (this is simplified - in practice, you'd listen for pong in message handler)
            # For now, we assume the pong will be handled separately
            logger.debug("Ping sent for connection %s (ping_id: %s)", connection_id, ping_id)

        except TimeoutError:
            # Ping timed out
            ping_status.timed_out = True

            async with self._lock:
                health_status.failed_pings += 1

                # Update health status
                if health_status.failed_pings >= self.max_failed_pings:
                    health_status.health = ConnectionHealth.STALE
                    health_status.is_responsive = False
                elif health_status.failed_pings > 0:
                    health_status.health = ConnectionHealth.DEGRADED

                logger.warning(
                    "Ping timeout for connection %s (failed_pings: %d/%d)",
                    connection_id,
                    health_status.failed_pings,
                    self.max_failed_pings,
                )

        except Exception as e:
            logger.error("Error sending ping for connection %s: %s", connection_id, e)

            async with self._lock:
                health_status.failed_pings += 1
                health_status.health = ConnectionHealth.DEGRADED

        # Store ping status
        async with self._lock:
            self._ping_history[connection_id].append(ping_status)
            # Keep only last 10 pings
            if len(self._ping_history[connection_id]) > 10:
                self._ping_history[connection_id] = self._ping_history[connection_id][-10:]

    async def handle_pong(self, connection_id: str, ping_id: str) -> bool:
        """
        Handle a pong response.

        Args:
            connection_id: Connection identifier
            ping_id: Ping identifier from the pong message

        Returns:
            bool: True if pong was processed successfully
        """
        async with self._lock:
            health_status = self._connection_health.get(connection_id)
            if not health_status:
                return False

            now = datetime.now(UTC)

            # Update health status
            health_status.last_pong_received = now
            health_status.last_activity = now
            health_status.total_pongs_received += 1
            health_status.failed_pings = 0  # Reset failed pings
            health_status.is_responsive = True

            # Update health to healthy if it was degraded
            if health_status.health in [
                ConnectionHealth.DEGRADED,
                ConnectionHealth.STALE,
            ]:
                health_status.health = ConnectionHealth.HEALTHY
                logger.info(
                    "Connection %s recovered (container: %s)",
                    connection_id,
                    health_status.container_name,
                )

            # Calculate latency if we have ping history
            if connection_id in self._ping_history:
                for ping_status in reversed(self._ping_history[connection_id]):
                    if ping_status.received_at is None:
                        # This is the ping we're responding to
                        ping_status.received_at = now
                        ping_status.latency_ms = (now - ping_status.sent_at).total_seconds() * 1000

                        # Update average latency
                        latencies = [
                            p.latency_ms
                            for p in self._ping_history[connection_id]
                            if p.latency_ms is not None
                        ]
                        if latencies:
                            health_status.average_latency_ms = sum(latencies) / len(latencies)

                        logger.debug(
                            "Pong received for connection %s (latency: %.2fms)",
                            connection_id,
                            ping_status.latency_ms,
                        )
                        break

            return True


# Global health monitor instance
_health_monitor: WebSocketHealthMonitor | None = None


def get_health_monitor(
    ping_interval_seconds: int = 30,
    ping_timeout_seconds: int = 10,
    max_failed_pings: int = 3,
    stale_threshold_seconds: int = 300,
) -> WebSocketHealthMonitor:
    """
    Get the global WebSocket health monitor instance.

    Args:
        ping_interval_seconds: Interval between ping messages
        ping_timeout_seconds: Timeout for pong responses
        max_failed_pings: Maximum consecutive failed pings
        stale_threshold_seconds: Time without activity before marking as stale

    Returns:
        WebSocketHealthMonitor: The global health monitor instance
    """
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = WebSocketHealthMonitor(
            ping_interval_seconds=ping_interval_seconds,
            ping_timeout_seconds=ping_timeout_seconds,
            max_failed_pings=max_failed_pings,
            stale_threshold_seconds=stale_threshold_seconds,
        )
    return _health_monitor


async def cleanup_task_runner(
    health_monitor: WebSocketHealthMonitor,
    cleanup_interval_seconds: int = 60,
) -> None:
    """
    Background task to periodically clean up stale connections.

    Args:
        health_monitor: The health monitor to use for cleanup
        cleanup_interval_seconds: Interval between cleanup runs
    """
    logger.info("WebSocket health cleanup task started (interval=%ds)", cleanup_interval_seconds)

    while True:
        try:
            await asyncio.sleep(cleanup_interval_seconds)
            cleaned = await health_monitor.cleanup_stale_connections()
            if cleaned > 0:
                logger.info("Health cleanup task removed %d stale connections", cleaned)
        except asyncio.CancelledError:
            logger.info("WebSocket health cleanup task cancelled")
            break
        except Exception as e:
            logger.error("Error in health cleanup task: %s", e, exc_info=True)
