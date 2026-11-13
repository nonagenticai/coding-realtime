"""
WebSocket Rate Limiter Service

Provides rate limiting for WebSocket operations per container with configurable limits,
tracking, and violation logging.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class RateLimitConfig(BaseModel):
    """Configuration for rate limiting."""

    messages_per_second: float = Field(
        default=10.0,
        description="Maximum messages per second per container",
        gt=0.0,
    )
    connections_per_container: int = Field(
        default=10,
        description="Maximum concurrent connections per container",
        gt=0,
    )
    burst_size: int = Field(
        default=20,
        description="Maximum burst size (allows short spikes above rate limit)",
        gt=0,
    )
    rate_limit_window_seconds: float = Field(
        default=1.0,
        description="Time window for rate limit calculation in seconds",
        gt=0.0,
    )


class RateLimitViolation(BaseModel):
    """Record of a rate limit violation."""

    container_name: str = Field(..., description="Container that violated the limit")
    violation_type: str = Field(..., description="Type of violation (messages, connections)")
    timestamp: datetime = Field(..., description="When the violation occurred")
    current_value: float = Field(..., description="Current value that exceeded limit")
    limit_value: float = Field(..., description="The limit that was exceeded")
    details: dict[str, Any] = Field(
        default_factory=dict, description="Additional violation details"
    )


class ContainerRateStats(BaseModel):
    """Rate statistics for a container."""

    container_name: str = Field(..., description="Container name")
    total_messages: int = Field(..., description="Total messages processed")
    messages_per_second: float = Field(..., description="Current message rate")
    active_connections: int = Field(..., description="Number of active connections")
    connections_limit: int = Field(..., description="Maximum allowed connections")
    messages_limit: float = Field(..., description="Maximum messages per second")
    is_rate_limited: bool = Field(..., description="Whether currently rate limited")
    total_violations: int = Field(default=0, description="Total violations recorded")
    last_violation: datetime | None = Field(None, description="Timestamp of last violation")


class MessageBucket:
    """Token bucket implementation for message rate limiting."""

    def __init__(self, rate: float, burst_size: int):
        """
        Initialize the message bucket.

        Args:
            rate: Messages per second allowed
            burst_size: Maximum burst size (tokens)
        """
        self.rate = rate  # messages per second
        self.burst_size = burst_size
        self.tokens = float(burst_size)  # Start with full burst capacity
        self.last_update = datetime.now(UTC)
        self._lock = asyncio.Lock()

    async def consume(self, tokens: float = 1.0) -> bool:
        """
        Try to consume tokens from the bucket.

        Args:
            tokens: Number of tokens to consume

        Returns:
            bool: True if tokens were consumed, False if rate limited
        """
        async with self._lock:
            now = datetime.now(UTC)
            elapsed = (now - self.last_update).total_seconds()

            # Refill tokens based on elapsed time
            self.tokens = min(self.burst_size, self.tokens + (elapsed * self.rate))
            self.last_update = now

            # Check if we have enough tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    async def get_current_rate(self) -> float:
        """
        Get the current effective rate.

        Returns:
            float: Current tokens available (proportional to rate)
        """
        async with self._lock:
            now = datetime.now(UTC)
            elapsed = (now - self.last_update).total_seconds()

            # Calculate current tokens without updating state
            current_tokens = min(self.burst_size, self.tokens + (elapsed * self.rate))

            # Return effective rate (tokens consumed per second)
            return max(0.0, self.rate - (current_tokens / max(elapsed, 0.1)))


class WebSocketRateLimiter:
    """
    Rate limiter for WebSocket operations per container.

    Features:
    - Per-container message rate limiting using token bucket algorithm
    - Connection count limits per container
    - Configurable limits and burst sizes
    - Violation tracking and logging
    - Statistics and monitoring
    - Thread-safe operations
    """

    def __init__(self, config: RateLimitConfig | None = None):
        """
        Initialize the rate limiter.

        Args:
            config: Rate limit configuration (uses defaults if not provided)
        """
        self.config = config or RateLimitConfig()

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

        # Per-container message buckets (token bucket algorithm)
        self._message_buckets: dict[str, MessageBucket] = {}

        # Per-container connection tracking
        self._container_connections: dict[str, int] = defaultdict(int)

        # Statistics tracking
        self._container_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "total_messages": 0,
                "total_violations": 0,
                "last_violation": None,
            }
        )

        # Violation history (limited to most recent violations)
        self._violation_history: list[RateLimitViolation] = []
        self._max_violation_history = 1000

        # Custom per-container limits
        self._custom_message_limits: dict[str, float] = {}
        self._custom_connection_limits: dict[str, int] = {}

        logger.info(
            "WebSocket Rate Limiter initialized: %.1f msg/s, %d connections/container, burst=%d",
            self.config.messages_per_second,
            self.config.connections_per_container,
            self.config.burst_size,
        )

    async def check_message_rate(self, container_name: str) -> bool:
        """
        Check if a message is allowed under the rate limit.

        Args:
            container_name: Name of the container

        Returns:
            bool: True if message is allowed, False if rate limited

        Raises:
            None: Logs rate limit violations
        """
        async with self._lock:
            # Get or create message bucket for container
            if container_name not in self._message_buckets:
                rate = self._custom_message_limits.get(
                    container_name, self.config.messages_per_second
                )
                self._message_buckets[container_name] = MessageBucket(
                    rate=rate, burst_size=self.config.burst_size
                )

            bucket = self._message_buckets[container_name]

        # Try to consume a token (outside the lock for better concurrency)
        allowed = await bucket.consume(1.0)

        if not allowed:
            # Rate limit exceeded
            async with self._lock:
                current_rate = await bucket.get_current_rate()
                limit = self._custom_message_limits.get(
                    container_name, self.config.messages_per_second
                )

                violation = RateLimitViolation(
                    container_name=container_name,
                    violation_type="message_rate",
                    timestamp=datetime.now(UTC),
                    current_value=current_rate,
                    limit_value=limit,
                    details={
                        "message": "Message rate limit exceeded",
                        "burst_size": self.config.burst_size,
                    },
                )

                # Record violation
                self._record_violation(violation)

                logger.warning(
                    "Rate limit exceeded for container '%s': %.2f msg/s (limit: %.2f msg/s)",
                    container_name,
                    current_rate,
                    limit,
                )

        else:
            # Update stats
            async with self._lock:
                self._container_stats[container_name]["total_messages"] += 1

        return allowed

    async def check_connection_limit(self, container_name: str) -> bool:
        """
        Check if a new connection is allowed under the connection limit.

        Args:
            container_name: Name of the container

        Returns:
            bool: True if connection is allowed, False if limit reached
        """
        async with self._lock:
            current_connections = self._container_connections[container_name]
            limit = self._custom_connection_limits.get(
                container_name, self.config.connections_per_container
            )

            if current_connections >= limit:
                # Connection limit exceeded
                violation = RateLimitViolation(
                    container_name=container_name,
                    violation_type="connection_limit",
                    timestamp=datetime.now(UTC),
                    current_value=float(current_connections),
                    limit_value=float(limit),
                    details={
                        "message": "Connection limit exceeded",
                        "active_connections": current_connections,
                    },
                )

                # Record violation
                self._record_violation(violation)

                logger.warning(
                    "Connection limit exceeded for container '%s': %d connections (limit: %d)",
                    container_name,
                    current_connections,
                    limit,
                )

                return False

            return True

    async def register_connection(self, container_name: str) -> None:
        """
        Register a new connection for a container.

        Args:
            container_name: Name of the container
        """
        async with self._lock:
            self._container_connections[container_name] += 1
            logger.debug(
                "Connection registered for container '%s': %d active connections",
                container_name,
                self._container_connections[container_name],
            )

    async def unregister_connection(self, container_name: str) -> None:
        """
        Unregister a connection for a container.

        Args:
            container_name: Name of the container
        """
        async with self._lock:
            if container_name in self._container_connections:
                self._container_connections[container_name] -= 1
                if self._container_connections[container_name] <= 0:
                    del self._container_connections[container_name]
                    logger.debug(
                        "All connections closed for container '%s'",
                        container_name,
                    )
                else:
                    logger.debug(
                        "Connection unregistered for container '%s': %d remaining",
                        container_name,
                        self._container_connections[container_name],
                    )

    def _record_violation(self, violation: RateLimitViolation) -> None:
        """
        Record a rate limit violation (internal, assumes lock is held).

        Args:
            violation: The violation to record
        """
        # Update container stats
        stats = self._container_stats[violation.container_name]
        stats["total_violations"] += 1
        stats["last_violation"] = violation.timestamp

        # Add to violation history
        self._violation_history.append(violation)

        # Trim history if too long
        if len(self._violation_history) > self._max_violation_history:
            self._violation_history = self._violation_history[-self._max_violation_history :]

    async def get_container_stats(self, container_name: str) -> ContainerRateStats:
        """
        Get rate statistics for a container.

        Args:
            container_name: Name of the container

        Returns:
            ContainerRateStats: Statistics for the container
        """
        async with self._lock:
            stats = self._container_stats[container_name]
            connections = self._container_connections.get(container_name, 0)

            message_limit = self._custom_message_limits.get(
                container_name, self.config.messages_per_second
            )
            connection_limit = self._custom_connection_limits.get(
                container_name, self.config.connections_per_container
            )

            # Get current message rate if bucket exists
            current_rate = 0.0
            if container_name in self._message_buckets:
                bucket = self._message_buckets[container_name]
                current_rate = await bucket.get_current_rate()

            # Check if currently rate limited
            is_rate_limited = current_rate >= message_limit * 0.9 or connections >= connection_limit

            return ContainerRateStats(
                container_name=container_name,
                total_messages=stats["total_messages"],
                messages_per_second=current_rate,
                active_connections=connections,
                connections_limit=connection_limit,
                messages_limit=message_limit,
                is_rate_limited=is_rate_limited,
                total_violations=stats["total_violations"],
                last_violation=stats["last_violation"],
            )

    async def get_all_stats(self) -> dict[str, ContainerRateStats]:
        """
        Get statistics for all containers.

        Returns:
            Dict[str, ContainerRateStats]: Stats keyed by container name
        """
        async with self._lock:
            container_names = self.tracked_container_names()

        stats = {}
        for container_name in container_names:
            stats[container_name] = await self.get_container_stats(container_name)

        return stats

    async def get_violations(
        self,
        container_name: str | None = None,
        limit: int = 100,
        container_names: set[str] | None = None,
    ) -> list[RateLimitViolation]:
        """
        Get recent rate limit violations.

        Args:
            container_name: Optional container name to filter violations
            limit: Maximum number of violations to return
            container_names: When given, ONLY violations for these containers
                (the caller tenant's) are considered — applied before ``limit``,
                so other tenants' violations neither appear nor use up the limit.

        Returns:
            List[RateLimitViolation]: List of violations (most recent first)
        """
        async with self._lock:
            history = self._violation_history
            if container_names is not None:
                history = [v for v in history if v.container_name in container_names]
            violations = history[-limit:]

            if container_name:
                violations = [v for v in violations if v.container_name == container_name]

            return list(reversed(violations))

    def set_container_message_limit(self, container_name: str, limit: float) -> None:
        """
        Set a custom message rate limit for a specific container.

        Args:
            container_name: Name of the container
            limit: Messages per second limit (must be positive)

        Raises:
            ValueError: If limit is not positive
        """
        if limit <= 0:
            raise ValueError("Message rate limit must be positive")

        self._custom_message_limits[container_name] = limit

        # Update existing bucket if it exists
        if container_name in self._message_buckets:
            self._message_buckets[container_name].rate = limit

        logger.info(
            "Set custom message rate limit for container '%s': %.2f msg/s",
            container_name,
            limit,
        )

    def set_container_connection_limit(self, container_name: str, limit: int) -> None:
        """
        Set a custom connection limit for a specific container.

        Args:
            container_name: Name of the container
            limit: Maximum number of connections (must be positive)

        Raises:
            ValueError: If limit is not positive
        """
        if limit <= 0:
            raise ValueError("Connection limit must be positive")

        self._custom_connection_limits[container_name] = limit
        logger.info(
            "Set custom connection limit for container '%s': %d connections",
            container_name,
            limit,
        )

    async def reset_container_stats(self, container_name: str) -> None:
        """
        Reset statistics for a container.

        Args:
            container_name: Name of the container
        """
        async with self._lock:
            if container_name in self._container_stats:
                self._container_stats[container_name] = {
                    "total_messages": 0,
                    "total_violations": 0,
                    "last_violation": None,
                }

            # Reset message bucket
            if container_name in self._message_buckets:
                rate = self._custom_message_limits.get(
                    container_name, self.config.messages_per_second
                )
                self._message_buckets[container_name] = MessageBucket(
                    rate=rate, burst_size=self.config.burst_size
                )

            logger.info("Reset stats for container '%s'", container_name)

    def tracked_container_names(self) -> set[str]:
        """Every container this limiter holds any state for."""
        return set(self._container_stats.keys()) | set(self._container_connections.keys())

    async def get_global_stats(self, container_names: set[str] | None = None) -> dict[str, Any]:
        """
        Get global rate limiter statistics.

        Args:
            container_names: When given, every per-container figure (totals,
                most recent violation, custom limits — which are KEYED BY
                container name) is restricted to these containers. ``config`` is
                the service-wide default and is always returned.

        Returns:
            Dict[str, Any]: Global statistics
        """

        def _in_scope(name: str) -> bool:
            return container_names is None or name in container_names

        async with self._lock:
            total_containers = len({n for n in self.tracked_container_names() if _in_scope(n)})
            total_connections = sum(
                v for n, v in self._container_connections.items() if _in_scope(n)
            )
            total_messages = sum(
                stats["total_messages"]
                for n, stats in self._container_stats.items()
                if _in_scope(n)
            )
            total_violations = sum(
                stats["total_violations"]
                for n, stats in self._container_stats.items()
                if _in_scope(n)
            )

            # Find most recent violation
            recent_violation = None
            scoped_history = [v for v in self._violation_history if _in_scope(v.container_name)]
            if scoped_history:
                recent_violation = scoped_history[-1].timestamp

            return {
                "total_containers": total_containers,
                "total_active_connections": total_connections,
                "total_messages_processed": total_messages,
                "total_violations": total_violations,
                "most_recent_violation": recent_violation,
                "config": {
                    "messages_per_second": self.config.messages_per_second,
                    "connections_per_container": self.config.connections_per_container,
                    "burst_size": self.config.burst_size,
                    "rate_limit_window_seconds": self.config.rate_limit_window_seconds,
                },
                "custom_limits": {
                    "message_limits": {
                        n: v for n, v in self._custom_message_limits.items() if _in_scope(n)
                    },
                    "connection_limits": {
                        n: v for n, v in self._custom_connection_limits.items() if _in_scope(n)
                    },
                },
            }


# Global rate limiter instance
_rate_limiter: WebSocketRateLimiter | None = None


def get_websocket_rate_limiter(
    config: RateLimitConfig | None = None,
) -> WebSocketRateLimiter:
    """
    Get the global WebSocket rate limiter instance.

    Args:
        config: Rate limit configuration (only used on first call)

    Returns:
        WebSocketRateLimiter: The global rate limiter instance
    """
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = WebSocketRateLimiter(config=config)
    return _rate_limiter
