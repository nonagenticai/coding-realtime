"""
WebSocket Metrics Collection Service

Tracks and collects metrics for WebSocket operations including:
- Active connections per container
- Message throughput per container
- Analysis execution times
- Error rates per container
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class MessageMetrics(BaseModel):
    """Metrics for message throughput."""

    total_messages_sent: int = Field(default=0, description="Total messages sent")
    total_messages_received: int = Field(default=0, description="Total messages received")
    messages_per_second: float = Field(default=0.0, description="Average messages per second")
    last_message_timestamp: datetime | None = Field(None, description="Timestamp of last message")


class AnalysisMetrics(BaseModel):
    """Metrics for analysis operations."""

    total_analyses: int = Field(default=0, description="Total analysis operations")
    successful_analyses: int = Field(default=0, description="Successful analyses")
    failed_analyses: int = Field(default=0, description="Failed analyses")
    average_execution_time_ms: float = Field(
        default=0.0, description="Average execution time in milliseconds"
    )
    min_execution_time_ms: float = Field(
        default=0.0, description="Minimum execution time in milliseconds"
    )
    max_execution_time_ms: float = Field(
        default=0.0, description="Maximum execution time in milliseconds"
    )
    total_execution_time_ms: float = Field(
        default=0.0, description="Total execution time in milliseconds"
    )
    success_rate: float = Field(default=0.0, description="Success rate percentage (0-100)")


class ErrorMetrics(BaseModel):
    """Metrics for errors."""

    total_errors: int = Field(default=0, description="Total errors")
    error_rate: float = Field(default=0.0, description="Error rate percentage (0-100)")
    errors_by_type: dict[str, int] = Field(default_factory=dict, description="Error counts by type")
    last_error_timestamp: datetime | None = Field(None, description="Timestamp of last error")
    last_error_message: str | None = Field(None, description="Last error message")


class ConnectionMetrics(BaseModel):
    """Metrics for connections."""

    active_connections: int = Field(default=0, description="Number of active connections")
    total_connections_created: int = Field(
        default=0, description="Total connections created (lifetime)"
    )
    total_connections_closed: int = Field(
        default=0, description="Total connections closed (lifetime)"
    )
    average_connection_duration_seconds: float = Field(
        default=0.0, description="Average connection duration in seconds"
    )
    oldest_connection_age_seconds: float | None = Field(
        None, description="Age of oldest connection in seconds"
    )


class ContainerMetrics(BaseModel):
    """Complete metrics for a container."""

    container_name: str = Field(..., description="Container name")
    timestamp: datetime = Field(..., description="Metrics timestamp")
    uptime_seconds: float = Field(..., description="Metrics collection uptime in seconds")
    connections: ConnectionMetrics = Field(..., description="Connection metrics")
    messages: MessageMetrics = Field(..., description="Message metrics")
    analysis: AnalysisMetrics = Field(..., description="Analysis metrics")
    errors: ErrorMetrics = Field(..., description="Error metrics")


class WebSocketMetricsCollector:
    """
    Collects and aggregates metrics for WebSocket operations.

    Features:
    - Connection tracking per container
    - Message throughput monitoring
    - Analysis execution time tracking
    - Error rate calculation
    - Container-scoped metrics
    """

    def __init__(self):
        """Initialize the metrics collector."""
        self._lock = asyncio.Lock()

        # Container-level metrics storage
        self._container_metrics: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "start_time": time.time(),
                "connections": {
                    "active": 0,
                    "total_created": 0,
                    "total_closed": 0,
                    "connection_times": [],  # (start_time, end_time) tuples
                },
                "messages": {
                    "sent": 0,
                    "received": 0,
                    "timestamps": [],  # Message timestamps for rate calculation
                },
                "analysis": {
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "execution_times": [],  # List of execution times
                },
                "errors": {
                    "total": 0,
                    "by_type": defaultdict(int),
                    "last_error": None,
                    "last_error_time": None,
                },
            }
        )

        logger.info("WebSocket metrics collector initialized")

    async def track_connection_opened(self, container_name: str, connection_id: str) -> None:
        """
        Track a new connection being opened.

        Args:
            container_name: Name of the container
            connection_id: Connection identifier
        """
        async with self._lock:
            metrics = self._container_metrics[container_name]
            metrics["connections"]["active"] += 1
            metrics["connections"]["total_created"] += 1

            logger.debug(
                "Connection opened: %s for container %s (active: %d)",
                connection_id,
                container_name,
                metrics["connections"]["active"],
            )

    async def track_connection_closed(
        self,
        container_name: str,
        connection_id: str,
        duration_seconds: float | None = None,
    ) -> None:
        """
        Track a connection being closed.

        Args:
            container_name: Name of the container
            connection_id: Connection identifier
            duration_seconds: Connection duration in seconds
        """
        async with self._lock:
            metrics = self._container_metrics[container_name]
            metrics["connections"]["active"] = max(0, metrics["connections"]["active"] - 1)
            metrics["connections"]["total_closed"] += 1

            if duration_seconds is not None:
                metrics["connections"]["connection_times"].append(duration_seconds)

            logger.debug(
                "Connection closed: %s for container %s (active: %d)",
                connection_id,
                container_name,
                metrics["connections"]["active"],
            )

    async def track_message_sent(self, container_name: str) -> None:
        """
        Track a message being sent.

        Args:
            container_name: Name of the container
        """
        async with self._lock:
            metrics = self._container_metrics[container_name]
            metrics["messages"]["sent"] += 1
            metrics["messages"]["timestamps"].append(time.time())

            # Keep only last 60 seconds of timestamps for rate calculation
            cutoff_time = time.time() - 60
            metrics["messages"]["timestamps"] = [
                ts for ts in metrics["messages"]["timestamps"] if ts > cutoff_time
            ]

    async def track_message_received(self, container_name: str) -> None:
        """
        Track a message being received.

        Args:
            container_name: Name of the container
        """
        async with self._lock:
            metrics = self._container_metrics[container_name]
            metrics["messages"]["received"] += 1
            metrics["messages"]["timestamps"].append(time.time())

            # Keep only last 60 seconds of timestamps for rate calculation
            cutoff_time = time.time() - 60
            metrics["messages"]["timestamps"] = [
                ts for ts in metrics["messages"]["timestamps"] if ts > cutoff_time
            ]

    async def track_analysis_execution(
        self,
        container_name: str,
        execution_time_ms: float,
        success: bool = True,
    ) -> None:
        """
        Track an analysis execution.

        Args:
            container_name: Name of the container
            execution_time_ms: Execution time in milliseconds
            success: Whether the analysis was successful
        """
        async with self._lock:
            metrics = self._container_metrics[container_name]
            metrics["analysis"]["total"] += 1

            if success:
                metrics["analysis"]["successful"] += 1
            else:
                metrics["analysis"]["failed"] += 1

            metrics["analysis"]["execution_times"].append(execution_time_ms)

            # Keep only last 1000 execution times to prevent memory growth
            if len(metrics["analysis"]["execution_times"]) > 1000:
                metrics["analysis"]["execution_times"] = metrics["analysis"]["execution_times"][
                    -1000:
                ]

            logger.debug(
                "Analysis tracked: container=%s, time=%.2fms, success=%s",
                container_name,
                execution_time_ms,
                success,
            )

    async def track_error(
        self,
        container_name: str,
        error_type: str,
        error_message: str | None = None,
    ) -> None:
        """
        Track an error occurrence.

        Args:
            container_name: Name of the container
            error_type: Type/category of the error
            error_message: Optional error message
        """
        async with self._lock:
            metrics = self._container_metrics[container_name]
            metrics["errors"]["total"] += 1
            metrics["errors"]["by_type"][error_type] += 1
            metrics["errors"]["last_error"] = error_message
            metrics["errors"]["last_error_time"] = datetime.now(UTC)

            logger.debug(
                "Error tracked: container=%s, type=%s, message=%s",
                container_name,
                error_type,
                error_message,
            )

    async def get_container_metrics(self, container_name: str) -> ContainerMetrics:
        """
        Get metrics for a specific container.

        Args:
            container_name: Name of the container

        Returns:
            ContainerMetrics: Metrics for the container
        """
        async with self._lock:
            if container_name not in self._container_metrics:
                # Return empty metrics for non-existent container
                return ContainerMetrics(
                    container_name=container_name,
                    timestamp=datetime.now(UTC),
                    uptime_seconds=0.0,
                    connections=ConnectionMetrics(),
                    messages=MessageMetrics(),
                    analysis=AnalysisMetrics(),
                    errors=ErrorMetrics(),
                )

            raw_metrics = self._container_metrics[container_name]
            uptime = time.time() - raw_metrics["start_time"]

            # Calculate connection metrics
            conn_data = raw_metrics["connections"]
            avg_duration = 0.0
            if conn_data["connection_times"]:
                avg_duration = sum(conn_data["connection_times"]) / len(
                    conn_data["connection_times"]
                )

            connection_metrics = ConnectionMetrics(
                active_connections=conn_data["active"],
                total_connections_created=conn_data["total_created"],
                total_connections_closed=conn_data["total_closed"],
                average_connection_duration_seconds=avg_duration,
                oldest_connection_age_seconds=(uptime if conn_data["active"] > 0 else None),
            )

            # Calculate message metrics
            msg_data = raw_metrics["messages"]
            recent_timestamps = [ts for ts in msg_data["timestamps"] if ts > time.time() - 60]
            messages_per_second = len(recent_timestamps) / 60.0 if recent_timestamps else 0.0

            last_msg_time = None
            if msg_data["timestamps"]:
                last_msg_time = datetime.fromtimestamp(max(msg_data["timestamps"]), tz=UTC)

            message_metrics = MessageMetrics(
                total_messages_sent=msg_data["sent"],
                total_messages_received=msg_data["received"],
                messages_per_second=messages_per_second,
                last_message_timestamp=last_msg_time,
            )

            # Calculate analysis metrics
            analysis_data = raw_metrics["analysis"]
            execution_times = analysis_data["execution_times"]

            avg_time = 0.0
            min_time = 0.0
            max_time = 0.0
            total_time = 0.0

            if execution_times:
                total_time = sum(execution_times)
                avg_time = total_time / len(execution_times)
                min_time = min(execution_times)
                max_time = max(execution_times)

            total_analyses = analysis_data["total"]
            success_rate = 0.0
            if total_analyses > 0:
                success_rate = (analysis_data["successful"] / total_analyses) * 100

            analysis_metrics = AnalysisMetrics(
                total_analyses=total_analyses,
                successful_analyses=analysis_data["successful"],
                failed_analyses=analysis_data["failed"],
                average_execution_time_ms=avg_time,
                min_execution_time_ms=min_time,
                max_execution_time_ms=max_time,
                total_execution_time_ms=total_time,
                success_rate=success_rate,
            )

            # Calculate error metrics
            error_data = raw_metrics["errors"]
            total_errors = error_data["total"]
            total_operations = msg_data["sent"] + msg_data["received"] + analysis_data["total"]

            error_rate = 0.0
            if total_operations > 0:
                error_rate = (total_errors / total_operations) * 100

            error_metrics = ErrorMetrics(
                total_errors=total_errors,
                error_rate=error_rate,
                errors_by_type=dict(error_data["by_type"]),
                last_error_timestamp=error_data["last_error_time"],
                last_error_message=error_data["last_error"],
            )

            return ContainerMetrics(
                container_name=container_name,
                timestamp=datetime.now(UTC),
                uptime_seconds=uptime,
                connections=connection_metrics,
                messages=message_metrics,
                analysis=analysis_metrics,
                errors=error_metrics,
            )

    async def get_all_container_metrics(self) -> list[ContainerMetrics]:
        """
        Get metrics for all containers.

        Returns:
            List[ContainerMetrics]: List of metrics for all containers
        """
        async with self._lock:
            container_names = list(self._container_metrics.keys())

        metrics_list = []
        for container_name in container_names:
            metrics = await self.get_container_metrics(container_name)
            metrics_list.append(metrics)

        return metrics_list

    async def reset_container_metrics(self, container_name: str) -> bool:
        """
        Reset metrics for a specific container.

        Args:
            container_name: Name of the container

        Returns:
            bool: True if metrics were reset, False if container not found
        """
        async with self._lock:
            if container_name in self._container_metrics:
                del self._container_metrics[container_name]
                logger.info("Reset metrics for container: %s", container_name)
                return True
            return False

    async def get_global_stats(self) -> dict[str, Any]:
        """
        Get global statistics across all containers.

        Returns:
            Dict[str, Any]: Global statistics
        """
        async with self._lock:
            total_containers = len(self._container_metrics)
            total_active_connections = 0
            total_messages = 0
            total_analyses = 0
            total_errors = 0

            for metrics in self._container_metrics.values():
                total_active_connections += metrics["connections"]["active"]
                total_messages += metrics["messages"]["sent"] + metrics["messages"]["received"]
                total_analyses += metrics["analysis"]["total"]
                total_errors += metrics["errors"]["total"]

            return {
                "total_containers_tracked": total_containers,
                "total_active_connections": total_active_connections,
                "total_messages_processed": total_messages,
                "total_analyses_executed": total_analyses,
                "total_errors": total_errors,
            }


class WebSocketMetricsCollectorSingleton:
    """Singleton wrapper for WebSocket metrics collector with class-level instance management."""

    _instance: WebSocketMetricsCollector | None = None
    _lock = __import__("threading").Lock()

    @classmethod
    def get_instance(cls) -> WebSocketMetricsCollector:
        """Get or create the singleton instance (thread-safe)."""
        if cls._instance is None:
            with cls._lock:
                # Double-checked locking
                if cls._instance is None:
                    cls._instance = WebSocketMetricsCollector()
                    logger.info("WebSocket metrics collector singleton created")
        assert cls._instance is not None
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (for testing)."""
        with cls._lock:
            cls._instance = None
            logger.debug("WebSocket metrics collector singleton reset")


def get_websocket_metrics_collector() -> WebSocketMetricsCollector:
    """
    Get the global WebSocket metrics collector instance.

    Returns:
        WebSocketMetricsCollector: The global metrics collector
    """
    return WebSocketMetricsCollectorSingleton.get_instance()


def reset_websocket_metrics_collector() -> None:
    """Reset the metrics collector (useful for testing)."""
    WebSocketMetricsCollectorSingleton.reset_instance()
