"""
WebSocket Connection Registry Service

Tracks and manages active WebSocket connections per container with connection limits,
health monitoring, and automatic cleanup.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import uuid7

from fastapi import WebSocket
from fastapi.websockets import WebSocketState
from pydantic import BaseModel, ConfigDict, Field, field_serializer

logger = logging.getLogger(__name__)

# Import rate limiter (lazy import to avoid circular dependencies)
_rate_limiter = None
_broadcast_service = None


def get_rate_limiter():
    """Get the rate limiter instance (lazy import)."""
    global _rate_limiter
    if _rate_limiter is None:
        from .websocket_rate_limiter import get_websocket_rate_limiter

        _rate_limiter = get_websocket_rate_limiter()
    return _rate_limiter


def get_broadcast_service_cached():
    """Get the broadcast service instance (lazy import)."""
    global _broadcast_service
    if _broadcast_service is None:
        from .websocket_broadcast import get_broadcast_service

        _broadcast_service = get_broadcast_service()
    return _broadcast_service


class ConnectionInfo(BaseModel):
    """Information about a WebSocket connection."""

    connection_id: str = Field(..., description="Unique connection identifier")
    container_name: str = Field(..., description="Container this connection belongs to")
    websocket: Any = Field(..., description="WebSocket instance (excluded from serialization)")
    connected_at: datetime = Field(..., description="Connection establishment timestamp")
    last_activity: datetime = Field(..., description="Last activity timestamp")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional connection metadata"
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    @field_serializer("connected_at", "last_activity")
    def serialize_datetime(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return v.isoformat()


class ContainerConnectionStats(BaseModel):
    """Statistics for connections to a container."""

    container_name: str = Field(..., description="Container name")
    active_connections: int = Field(..., description="Number of active connections")
    total_connections_created: int = Field(
        default=0, description="Total connections created (lifetime)"
    )
    connection_limit: int = Field(..., description="Maximum allowed connections")
    at_limit: bool = Field(..., description="Whether at connection limit")
    oldest_connection_age_seconds: float | None = Field(
        None, description="Age of oldest connection in seconds"
    )
    newest_connection_age_seconds: float | None = Field(
        None, description="Age of newest connection in seconds"
    )


class WebSocketConnectionRegistry:
    """
    Registry for tracking active WebSocket connections per container.

    Features:
    - Connection tracking by container
    - Connection count limits per container
    - Automatic cleanup on disconnect
    - Connection health monitoring
    - Activity tracking
    - Thread-safe operations
    """

    def __init__(
        self,
        default_max_connections_per_container: int = 10,
        connection_timeout_seconds: int = 3600,
        enable_rate_limiting: bool = True,
    ):
        """
        Initialize the connection registry.

        Args:
            default_max_connections_per_container: Default max connections per container
            connection_timeout_seconds: Timeout for inactive connections (seconds)
            enable_rate_limiting: Whether to enable rate limiting
        """
        self.default_max_connections = default_max_connections_per_container
        self.connection_timeout = connection_timeout_seconds
        self.enable_rate_limiting = enable_rate_limiting

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

        # Primary storage: connection_id -> ConnectionInfo
        self._connections: dict[str, ConnectionInfo] = {}

        # Index: container_name -> set of connection_ids
        self._container_connections: dict[str, set[str]] = defaultdict(set)

        # Container-specific connection limits
        self._container_limits: dict[str, int] = {}

        # Statistics tracking
        self._container_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"total_created": 0, "total_disconnected": 0}
        )

        logger.info(
            "WebSocket Connection Registry initialized (max_per_container=%d, timeout=%ds, rate_limiting=%s)",
            self.default_max_connections,
            self.connection_timeout,
            "enabled" if enable_rate_limiting else "disabled",
        )

    async def register_connection(
        self,
        websocket: WebSocket,
        container_name: str,
        connection_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Register a new WebSocket connection for a container.

        Args:
            websocket: The WebSocket instance
            container_name: Name of the container
            connection_id: Optional custom connection ID (generated if not provided)
            metadata: Optional metadata to store with the connection

        Returns:
            str: The connection ID

        Raises:
            ValueError: If connection limit is reached for the container
        """
        # Check rate limit first (before acquiring lock)
        if self.enable_rate_limiting:
            rate_limiter = get_rate_limiter()
            if not await rate_limiter.check_connection_limit(container_name):
                raise ValueError(
                    f"Rate limit exceeded for container '{container_name}': connection limit reached"
                )

        async with self._lock:
            # Generate connection ID if not provided
            if connection_id is None:
                # The FULL uuid7. hex[:8] is its top 32 bits — pure timestamp
                # (unix_ts_ms >> 16, i.e. ~65 s buckets) with ZERO random bits,
                # so two connections to one container inside the same bucket got
                # the same id and the second silently replaced the first.
                connection_id = f"{container_name}_{uuid7().hex}"

            # Check if connection already exists
            if connection_id in self._connections:
                logger.warning("Connection ID %s already exists, will be replaced", connection_id)
                await self._remove_connection_unsafe(connection_id)

            # Check connection limit
            current_count = len(self._container_connections[container_name])
            limit = self._get_container_limit(container_name)

            if current_count >= limit:
                logger.warning(
                    "Connection limit reached for container %s (%d/%d)",
                    container_name,
                    current_count,
                    limit,
                )
                raise ValueError(
                    f"Connection limit reached for container '{container_name}' ({current_count}/{limit})"
                )

            # Create connection info
            now = datetime.now(UTC)
            conn_info = ConnectionInfo(
                connection_id=connection_id,
                container_name=container_name,
                websocket=websocket,
                connected_at=now,
                last_activity=now,
                metadata=metadata or {},
            )

            # Store connection
            self._connections[connection_id] = conn_info
            self._container_connections[container_name].add(connection_id)

            # Update stats
            self._container_stats[container_name]["total_created"] += 1

            logger.info(
                "WebSocket connection registered: %s for container %s (%d/%d)",
                connection_id,
                container_name,
                len(self._container_connections[container_name]),
                limit,
            )

            # Register with rate limiter
            if self.enable_rate_limiting:
                rate_limiter = get_rate_limiter()
                await rate_limiter.register_connection(container_name)

            return connection_id

    async def unregister_connection(self, connection_id: str) -> bool:
        """
        Unregister a WebSocket connection.

        Args:
            connection_id: The connection ID to unregister

        Returns:
            bool: True if connection was found and removed, False otherwise
        """
        async with self._lock:
            return await self._remove_connection_unsafe(connection_id)

    async def _remove_connection_unsafe(self, connection_id: str) -> bool:
        """
        Remove a connection (internal, assumes lock is held).

        Args:
            connection_id: The connection ID to remove

        Returns:
            bool: True if connection was removed, False if not found
        """
        conn_info = self._connections.get(connection_id)
        if not conn_info:
            logger.debug("Connection %s not found for removal", connection_id)
            return False

        container_name = conn_info.container_name

        # Remove from primary storage
        del self._connections[connection_id]

        # Remove from container index
        self._container_connections[container_name].discard(connection_id)
        if not self._container_connections[container_name]:
            del self._container_connections[container_name]

        # Update stats
        self._container_stats[container_name]["total_disconnected"] += 1

        logger.info(
            "WebSocket connection unregistered: %s for container %s (remaining: %d)",
            connection_id,
            container_name,
            len(self._container_connections.get(container_name, set())),
        )

        # Unregister from rate limiter
        if self.enable_rate_limiting:
            rate_limiter = get_rate_limiter()
            await rate_limiter.unregister_connection(container_name)

        return True

    async def get_connection(self, connection_id: str) -> ConnectionInfo | None:
        """
        Get connection information by ID.

        Args:
            connection_id: The connection ID

        Returns:
            Optional[ConnectionInfo]: Connection info if found, None otherwise
        """
        async with self._lock:
            return self._connections.get(connection_id)

    async def get_container_connections(self, container_name: str) -> list[ConnectionInfo]:
        """
        Get all connections for a specific container.

        Args:
            container_name: Name of the container

        Returns:
            List[ConnectionInfo]: List of connection info objects
        """
        async with self._lock:
            connection_ids = self._container_connections.get(container_name, set())
            return [
                self._connections[conn_id]
                for conn_id in connection_ids
                if conn_id in self._connections
            ]

    async def get_container_connection_count(self, container_name: str) -> int:
        """
        Get the number of active connections for a container.

        Args:
            container_name: Name of the container

        Returns:
            int: Number of active connections
        """
        async with self._lock:
            return len(self._container_connections.get(container_name, set()))

    async def get_all_container_names(self) -> list[str]:
        """
        Get all container names with active connections.

        Returns:
            List[str]: List of container names
        """
        async with self._lock:
            return list(self._container_connections.keys())

    async def get_container_stats(self, container_name: str) -> ContainerConnectionStats:
        """
        Get connection statistics for a container.

        Args:
            container_name: Name of the container

        Returns:
            ContainerConnectionStats: Statistics for the container
        """
        async with self._lock:
            connections = [
                self._connections[conn_id]
                for conn_id in self._container_connections.get(container_name, set())
                if conn_id in self._connections
            ]

            active_count = len(connections)
            limit = self._get_container_limit(container_name)
            stats = self._container_stats[container_name]

            # Calculate connection ages
            now = datetime.now(UTC)
            oldest_age = None
            newest_age = None

            if connections:
                ages = [(now - conn.connected_at).total_seconds() for conn in connections]
                oldest_age = max(ages)
                newest_age = min(ages)

            return ContainerConnectionStats(
                container_name=container_name,
                active_connections=active_count,
                total_connections_created=stats["total_created"],
                connection_limit=limit,
                at_limit=(active_count >= limit),
                oldest_connection_age_seconds=oldest_age,
                newest_connection_age_seconds=newest_age,
            )

    async def get_all_stats(self) -> dict[str, ContainerConnectionStats]:
        """
        Get statistics for all containers with connections.

        Returns:
            Dict[str, ContainerConnectionStats]: Stats keyed by container name
        """
        async with self._lock:
            container_names = list(self._container_connections.keys())

        # Get stats for each container (this will acquire lock for each)
        stats = {}
        for container_name in container_names:
            stats[container_name] = await self.get_container_stats(container_name)

        return stats

    def set_container_limit(self, container_name: str, limit: int) -> None:
        """
        Set a custom connection limit for a specific container.

        Args:
            container_name: Name of the container
            limit: Maximum number of connections allowed
        """
        if limit <= 0:
            raise ValueError("Connection limit must be positive")

        self._container_limits[container_name] = limit
        logger.info("Set connection limit for container %s to %d", container_name, limit)

    def _get_container_limit(self, container_name: str) -> int:
        """
        Get the connection limit for a container.

        Args:
            container_name: Name of the container

        Returns:
            int: Connection limit
        """
        return self._container_limits.get(container_name, self.default_max_connections)

    async def update_activity(self, connection_id: str) -> bool:
        """
        Update the last activity timestamp for a connection.

        Args:
            connection_id: The connection ID

        Returns:
            bool: True if connection was found and updated, False otherwise
        """
        async with self._lock:
            conn_info = self._connections.get(connection_id)
            if conn_info:
                conn_info.last_activity = datetime.now(UTC)
                return True
            return False

    async def cleanup_stale_connections(self) -> int:
        """
        Clean up stale connections that have exceeded the timeout.

        Returns:
            int: Number of connections cleaned up
        """
        async with self._lock:
            now = datetime.now(UTC)
            stale_connections = []

            for conn_id, conn_info in self._connections.items():
                age = (now - conn_info.last_activity).total_seconds()
                if age > self.connection_timeout:
                    stale_connections.append(conn_id)

            # Remove stale connections
            cleanup_count = 0
            for conn_id in stale_connections:
                # Try to close the WebSocket gracefully
                conn_info = self._connections[conn_id]
                try:
                    if conn_info.websocket.client_state == WebSocketState.CONNECTED:
                        await conn_info.websocket.close(code=1000, reason="Connection timeout")
                except Exception as e:
                    logger.debug("Error closing stale connection %s: %s", conn_id, e)

                # Remove from registry
                if await self._remove_connection_unsafe(conn_id):
                    cleanup_count += 1

            if cleanup_count > 0:
                logger.info("Cleaned up %d stale WebSocket connections", cleanup_count)

            return cleanup_count

    async def cleanup_container_connections(self, container_name: str) -> int:
        """
        Clean up all connections for a specific container.

        Args:
            container_name: Name of the container

        Returns:
            int: Number of connections cleaned up
        """
        async with self._lock:
            connection_ids = list(self._container_connections.get(container_name, set()))

            cleanup_count = 0
            for conn_id in connection_ids:
                conn_info = self._connections.get(conn_id)
                if conn_info:
                    # Try to close WebSocket gracefully
                    try:
                        if conn_info.websocket.client_state == WebSocketState.CONNECTED:
                            await conn_info.websocket.close(code=1000, reason="Container cleanup")
                    except Exception as e:
                        logger.debug("Error closing connection %s: %s", conn_id, e)

                    # Remove from registry
                    if await self._remove_connection_unsafe(conn_id):
                        cleanup_count += 1

            if cleanup_count > 0:
                logger.info(
                    "Cleaned up %d connections for container %s",
                    cleanup_count,
                    container_name,
                )

            return cleanup_count

    async def get_global_stats(self) -> dict[str, Any]:
        """
        Get global registry statistics.

        Returns:
            Dict[str, Any]: Global statistics
        """
        async with self._lock:
            total_connections = len(self._connections)
            total_containers = len(self._container_connections)

            # Calculate total lifetime stats
            total_created = sum(stats["total_created"] for stats in self._container_stats.values())
            total_disconnected = sum(
                stats["total_disconnected"] for stats in self._container_stats.values()
            )

            # Connection age distribution
            now = datetime.now(UTC)
            ages = [
                (now - conn.connected_at).total_seconds() for conn in self._connections.values()
            ]

            return {
                "total_active_connections": total_connections,
                "total_containers_with_connections": total_containers,
                "total_connections_created_lifetime": total_created,
                "total_connections_disconnected_lifetime": total_disconnected,
                "average_connection_age_seconds": (sum(ages) / len(ages) if ages else 0),
                "oldest_connection_age_seconds": max(ages) if ages else 0,
                "newest_connection_age_seconds": min(ages) if ages else 0,
                "default_connection_limit": self.default_max_connections,
                "connection_timeout_seconds": self.connection_timeout,
            }

    async def broadcast_to_container(
        self,
        container_name: str,
        message: dict[str, Any],
    ) -> int:
        """
        Broadcast a message to all connections in a container.

        This is a convenience method that sends a message directly to all
        WebSocket connections for a container without using the broadcast
        service's subscription system.

        Args:
            container_name: Name of the container
            message: Message to broadcast

        Returns:
            int: Number of connections the message was sent to
        """
        async with self._lock:
            connection_ids = list(self._container_connections.get(container_name, set()))
            connections = [
                self._connections[conn_id]
                for conn_id in connection_ids
                if conn_id in self._connections
            ]

        sent_count = 0
        for conn_info in connections:
            try:
                if conn_info.websocket.client_state == WebSocketState.CONNECTED:
                    await conn_info.websocket.send_json(message)
                    sent_count += 1
            except Exception as e:
                logger.error(
                    "Failed to broadcast to connection %s in container %s: %s",
                    conn_info.connection_id,
                    container_name,
                    e,
                )

        logger.debug(
            "Broadcast to container %s: sent to %d/%d connections",
            container_name,
            sent_count,
            len(connections),
        )

        return sent_count

    async def broadcast_to_all(self, message: dict[str, Any]) -> dict[str, int]:
        """
        Broadcast a message to all connections across all containers.

        Args:
            message: Message to broadcast

        Returns:
            Dict[str, int]: Number of connections reached per container
        """
        async with self._lock:
            container_names = list(self._container_connections.keys())

        results = {}
        for container_name in container_names:
            sent_count = await self.broadcast_to_container(container_name, message)
            results[container_name] = sent_count

        return results


# Global registry instance
_registry: WebSocketConnectionRegistry | None = None


def get_websocket_registry(
    max_connections_per_container: int = 10,
    connection_timeout_seconds: int = 3600,
) -> WebSocketConnectionRegistry:
    """
    Get the global WebSocket connection registry instance.

    Args:
        max_connections_per_container: Default max connections per container
        connection_timeout_seconds: Timeout for inactive connections

    Returns:
        WebSocketConnectionRegistry: The global registry instance
    """
    global _registry
    if _registry is None:
        _registry = WebSocketConnectionRegistry(
            default_max_connections_per_container=max_connections_per_container,
            connection_timeout_seconds=connection_timeout_seconds,
        )
    return _registry


async def cleanup_task_runner(
    registry: WebSocketConnectionRegistry,
    cleanup_interval_seconds: int = 300,
) -> None:
    """
    Background task to periodically clean up stale connections.

    Args:
        registry: The connection registry to clean
        cleanup_interval_seconds: Interval between cleanup runs
    """
    logger.info("WebSocket cleanup task started (interval=%ds)", cleanup_interval_seconds)

    while True:
        try:
            await asyncio.sleep(cleanup_interval_seconds)
            cleaned = await registry.cleanup_stale_connections()
            if cleaned > 0:
                logger.info("Cleanup task removed %d stale connections", cleaned)
        except asyncio.CancelledError:
            logger.info("WebSocket cleanup task cancelled")
            break
        except Exception as e:
            logger.error("Error in cleanup task: %s", e, exc_info=True)
