"""
WebSocket Health Check API Endpoints

Provides REST API endpoints for monitoring WebSocket connection health,
including per-container health statistics, global health metrics, and
active connection management.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from coding_client.auth import require_tenant
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, ConfigDict, Field

from models.common import BaseResponse, ResponseStatus
from services.websocket_connection_registry import (
    get_websocket_registry,
)
from services.websocket_health_monitor import (
    ConnectionHealth,
    get_health_monitor,
)
from utils.container_validation import validate_and_get_container_for_tenant

logger = logging.getLogger(__name__)
router = APIRouter()


def _not_found(resource: str, identifier: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={
            "status": "error",
            "message": f"{resource} '{identifier}' not found",
            "error_code": f"{resource.upper().replace(' ', '_')}_NOT_FOUND",
            "details": {},
        },
    )


class ConnectionHealthResponse(BaseModel):
    """Response model for connection health status."""

    connection_id: str = Field(..., description="Connection identifier")
    container_name: str = Field(..., description="Container name")
    health: ConnectionHealth = Field(..., description="Health status")
    last_ping_sent: str | None = Field(None, description="Last ping timestamp")
    last_pong_received: str | None = Field(None, description="Last pong timestamp")
    last_activity: str = Field(..., description="Last activity timestamp")
    average_latency_ms: float | None = Field(None, description="Average latency")
    failed_pings: int = Field(..., description="Failed ping count")
    total_pings_sent: int = Field(..., description="Total pings sent")
    total_pongs_received: int = Field(..., description="Total pongs received")
    connection_age_seconds: float = Field(..., description="Connection age in seconds")
    is_responsive: bool = Field(..., description="Whether connection is responsive")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection_id": "my-container_abc123",
                "container_name": "my-container",
                "health": "healthy",
                "last_ping_sent": "2025-01-06T10:30:00Z",
                "last_pong_received": "2025-01-06T10:30:00Z",
                "last_activity": "2025-01-06T10:30:00Z",
                "average_latency_ms": 15.5,
                "failed_pings": 0,
                "total_pings_sent": 120,
                "total_pongs_received": 120,
                "connection_age_seconds": 3600.0,
                "is_responsive": True,
            }
        }
    )


class ContainerHealthResponse(BaseModel):
    """Response model for container health statistics."""

    container_name: str = Field(..., description="Container name")
    total_connections: int = Field(..., description="Total active connections")
    healthy_connections: int = Field(..., description="Healthy connections count")
    degraded_connections: int = Field(..., description="Degraded connections count")
    stale_connections: int = Field(..., description="Stale connections count")
    average_latency_ms: float | None = Field(None, description="Average latency")
    total_failed_pings: int = Field(..., description="Total failed pings")
    last_cleanup: str | None = Field(None, description="Last cleanup timestamp")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "container_name": "my-container",
                "total_connections": 5,
                "healthy_connections": 4,
                "degraded_connections": 1,
                "stale_connections": 0,
                "average_latency_ms": 12.3,
                "total_failed_pings": 2,
                "last_cleanup": "2025-01-06T10:00:00Z",
            }
        }
    )


class ActiveConnectionInfo(BaseModel):
    """Information about an active WebSocket connection."""

    connection_id: str = Field(..., description="Connection identifier")
    container_name: str = Field(..., description="Container name")
    health: ConnectionHealth = Field(..., description="Health status")
    connected_at: str = Field(..., description="Connection timestamp")
    last_activity: str = Field(..., description="Last activity timestamp")
    connection_age_seconds: float = Field(..., description="Connection age")
    average_latency_ms: float | None = Field(None, description="Average latency")
    is_responsive: bool = Field(..., description="Whether connection is responsive")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection_id": "my-container_abc123",
                "container_name": "my-container",
                "health": "healthy",
                "connected_at": "2025-01-06T09:30:00Z",
                "last_activity": "2025-01-06T10:30:00Z",
                "connection_age_seconds": 3600.0,
                "average_latency_ms": 15.5,
                "is_responsive": True,
            }
        }
    )


class ContainerConnectionsResponse(BaseResponse):
    """Response model for container connections."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    container_name: str = Field(..., description="Container name")
    total_connections: int = Field(..., description="Total active connections")
    connections: list[ActiveConnectionInfo] = Field(..., description="Connection details")
    health_summary: ContainerHealthResponse = Field(..., description="Health summary")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Retrieved 5 active connections for container 'my-container'",
                "container_name": "my-container",
                "total_connections": 5,
                "connections": [
                    {
                        "connection_id": "my-container_abc123",
                        "container_name": "my-container",
                        "health": "healthy",
                        "connected_at": "2025-01-06T09:30:00Z",
                        "last_activity": "2025-01-06T10:30:00Z",
                        "connection_age_seconds": 3600.0,
                        "average_latency_ms": 15.5,
                        "is_responsive": True,
                    }
                ],
                "health_summary": {
                    "container_name": "my-container",
                    "total_connections": 5,
                    "healthy_connections": 4,
                    "degraded_connections": 1,
                    "stale_connections": 0,
                    "average_latency_ms": 12.3,
                    "total_failed_pings": 2,
                },
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


class GlobalHealthResponse(BaseResponse):
    """Response model for global health statistics."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    total_connections: int = Field(..., description="Total active connections")
    total_containers: int = Field(..., description="Total containers with connections")
    healthy_connections: int = Field(..., description="Healthy connections")
    degraded_connections: int = Field(..., description="Degraded connections")
    stale_connections: int = Field(..., description="Stale connections")
    average_latency_ms: float | None = Field(None, description="Average latency")
    total_pings_sent: int = Field(..., description="Total pings sent")
    total_pongs_received: int = Field(..., description="Total pongs received")
    total_failed_pings: int = Field(..., description="Total failed pings")
    ping_success_rate: float = Field(..., description="Ping success rate percentage")
    configuration: dict[str, Any] = Field(..., description="Health monitor configuration")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Global health statistics retrieved successfully",
                "total_connections": 15,
                "total_containers": 3,
                "healthy_connections": 12,
                "degraded_connections": 2,
                "stale_connections": 1,
                "average_latency_ms": 14.2,
                "total_pings_sent": 1500,
                "total_pongs_received": 1485,
                "total_failed_pings": 15,
                "ping_success_rate": 99.0,
                "configuration": {
                    "ping_interval_seconds": 30,
                    "ping_timeout_seconds": 10,
                    "max_failed_pings": 3,
                    "stale_threshold_seconds": 300,
                },
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


class CleanupResponse(BaseResponse):
    """Response model for cleanup operations."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    cleaned_connections: int = Field(..., description="Number of connections cleaned up")
    container_name: str | None = Field(None, description="Container name if specified")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Cleaned up 2 stale connections",
                "cleaned_connections": 2,
                "container_name": "my-container",
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


@router.get(
    "/connection/{connection_id}",
    summary="Get Connection Health",
    operation_id="get_connection_health",
    description="""
    Get health status for a specific WebSocket connection. A connection opened by
    another tenant is reported as not found (404).

    Returns detailed health information including:
    - Current health status (healthy, degraded, stale, disconnected)
    - Ping/pong statistics
    - Latency metrics
    - Activity timestamps
    - Responsiveness status
    """,
    response_model=ConnectionHealthResponse,
)
async def get_connection_health(
    connection_id: str = Path(..., description="Connection identifier"),
    tenant_id: str = Depends(require_tenant),
) -> ConnectionHealthResponse:
    """Get health status for a specific connection."""
    health_monitor = get_health_monitor()
    # Another tenant's connection is reported exactly like an unknown one (404).
    health_status = await health_monitor.get_connection_health(connection_id, tenant_id=tenant_id)

    if not health_status:
        raise _not_found("Connection", connection_id)

    return ConnectionHealthResponse(
        connection_id=health_status.connection_id,
        container_name=health_status.container_name,
        health=health_status.health,
        last_ping_sent=(
            health_status.last_ping_sent.isoformat() if health_status.last_ping_sent else None
        ),
        last_pong_received=(
            health_status.last_pong_received.isoformat()
            if health_status.last_pong_received
            else None
        ),
        last_activity=health_status.last_activity.isoformat(),
        average_latency_ms=health_status.average_latency_ms,
        failed_pings=health_status.failed_pings,
        total_pings_sent=health_status.total_pings_sent,
        total_pongs_received=health_status.total_pongs_received,
        connection_age_seconds=health_status.connection_age_seconds,
        is_responsive=health_status.is_responsive,
    )


@router.get(
    "/container/{container_name}",
    summary="Get Container Health",
    operation_id="get_ws_container_health",
    description="""
    Get health statistics for all WebSocket connections to a specific container.

    Returns aggregated health metrics including:
    - Total connection count
    - Breakdown by health status
    - Average latency
    - Failed ping statistics
    - Last cleanup timestamp
    """,
    response_model=ContainerHealthResponse,
)
async def get_container_health(
    container_name: str = Path(..., description="Container name"),
    tenant_id: str = Depends(require_tenant),
) -> ContainerHealthResponse:
    """Get health statistics for a container."""
    # Validate tenant has access to this container (returns 404 if not found or wrong tenant)
    await validate_and_get_container_for_tenant(container_name, tenant_id)

    health_monitor = get_health_monitor()
    health_stats = await health_monitor.get_container_health(container_name)

    if not health_stats:
        raise _not_found("Active connections for container", container_name)

    return ContainerHealthResponse(
        container_name=health_stats.container_name,
        total_connections=health_stats.total_connections,
        healthy_connections=health_stats.healthy_connections,
        degraded_connections=health_stats.degraded_connections,
        stale_connections=health_stats.stale_connections,
        average_latency_ms=health_stats.average_latency_ms,
        total_failed_pings=health_stats.total_failed_pings,
        last_cleanup=(health_stats.last_cleanup.isoformat() if health_stats.last_cleanup else None),
    )


@router.get(
    "/container/{container_name}/connections",
    summary="Get Active Connections for Container",
    operation_id="get_container_connections",
    description="""
    Get detailed information about all active WebSocket connections to a specific container.

    Returns:
    - List of all active connections with health status
    - Individual connection details
    - Aggregated health summary
    - Connection registry statistics

    This endpoint combines information from both the connection registry and health monitor
    to provide a complete view of container connections.
    """,
    response_model=ContainerConnectionsResponse,
)
async def get_container_connections(
    container_name: str = Path(..., description="Container name"),
    tenant_id: str = Depends(require_tenant),
) -> ContainerConnectionsResponse:
    """Get all active WebSocket connections for a container."""
    # Validate tenant has access to this container (returns 404 if not found or wrong tenant)
    await validate_and_get_container_for_tenant(container_name, tenant_id)

    registry = get_websocket_registry()
    health_monitor = get_health_monitor()

    # Get connections from registry
    connections = await registry.get_container_connections(container_name)

    if not connections:
        raise _not_found("Active connections for container", container_name)

    # Build connection info with health data
    active_connections = []
    for conn_info in connections:
        health_status = await health_monitor.get_connection_health(conn_info.connection_id)

        active_conn = ActiveConnectionInfo(
            connection_id=conn_info.connection_id,
            container_name=conn_info.container_name,
            health=(health_status.health if health_status else ConnectionHealth.DISCONNECTED),
            connected_at=conn_info.connected_at.isoformat(),
            last_activity=conn_info.last_activity.isoformat(),
            connection_age_seconds=(
                (
                    datetime.now(conn_info.last_activity.tzinfo) - conn_info.connected_at
                ).total_seconds()
            ),
            average_latency_ms=(health_status.average_latency_ms if health_status else None),
            is_responsive=health_status.is_responsive if health_status else False,
        )
        active_connections.append(active_conn)

    # Get health summary
    health_stats = await health_monitor.get_container_health(container_name)

    if not health_stats:
        # Create default health stats if not available
        health_summary = ContainerHealthResponse(
            container_name=container_name,
            total_connections=len(connections),
            healthy_connections=0,
            degraded_connections=0,
            stale_connections=0,
            average_latency_ms=None,
            total_failed_pings=0,
            last_cleanup=None,
        )
    else:
        health_summary = ContainerHealthResponse(
            container_name=health_stats.container_name,
            total_connections=health_stats.total_connections,
            healthy_connections=health_stats.healthy_connections,
            degraded_connections=health_stats.degraded_connections,
            stale_connections=health_stats.stale_connections,
            average_latency_ms=health_stats.average_latency_ms,
            total_failed_pings=health_stats.total_failed_pings,
            last_cleanup=(
                health_stats.last_cleanup.isoformat() if health_stats.last_cleanup else None
            ),
        )

    return ContainerConnectionsResponse(
        message=f"Retrieved {len(active_connections)} active connections for container '{container_name}'",
        container_name=container_name,
        total_connections=len(active_connections),
        connections=active_connections,
        health_summary=health_summary,
    )


@router.get(
    "/global",
    summary="Get Global Health Statistics",
    operation_id="get_global_health",
    description="""
    Get health statistics aggregated over the caller tenant's WebSocket connections
    (across all of the caller's containers — never another tenant's).

    Returns:
    - Total connection counts
    - Health status breakdown
    - Aggregated ping/pong statistics
    - Average latency metrics
    - Health monitor configuration
    """,
    response_model=GlobalHealthResponse,
)
async def get_global_health(
    tenant_id: str = Depends(require_tenant),
) -> GlobalHealthResponse:
    """Get health statistics aggregated over the CALLER's connections."""
    health_monitor = get_health_monitor()
    # Scoped: "global" means across the caller's containers. The unscoped form
    # disclosed the pod's total connection/container counts for every tenant.
    stats = await health_monitor.get_global_health_stats(tenant_id=tenant_id)

    return GlobalHealthResponse(
        message="Global health statistics retrieved successfully",
        total_connections=stats["total_connections"],
        total_containers=stats["total_containers"],
        healthy_connections=stats["healthy_connections"],
        degraded_connections=stats["degraded_connections"],
        stale_connections=stats["stale_connections"],
        average_latency_ms=stats["average_latency_ms"],
        total_pings_sent=stats["total_pings_sent"],
        total_pongs_received=stats["total_pongs_received"],
        total_failed_pings=stats["total_failed_pings"],
        ping_success_rate=stats["ping_success_rate"],
        configuration={
            "ping_interval_seconds": stats["ping_interval_seconds"],
            "ping_timeout_seconds": stats["ping_timeout_seconds"],
            "max_failed_pings": stats["max_failed_pings"],
            "stale_threshold_seconds": stats["stale_threshold_seconds"],
        },
    )


@router.post(
    "/cleanup",
    summary="Cleanup Stale Connections",
    operation_id="cleanup_stale_connections",
    description="""
    Manually trigger cleanup of stale WebSocket connections.

    Can cleanup connections for:
    - Specific container (if container_name provided)
    - All of the CALLER tenant's containers (if container_name is None) — never another tenant's

    Stale connections are those that:
    - Have exceeded the maximum failed pings threshold
    - Have been inactive beyond the stale threshold
    - Are no longer responsive to health checks
    """,
    response_model=CleanupResponse,
)
async def cleanup_stale_connections(
    container_name: str | None = Query(None, description="Container name (optional)"),
    tenant_id: str = Depends(require_tenant),
) -> CleanupResponse:
    """Manually trigger cleanup of stale connections — the CALLER's only.

    Scoped to ``tenant_id``: without a container every stale connection the
    caller's tenant opened is cleaned, never another tenant's. A named container
    must belong to the caller (404 otherwise, BOLA-safe).
    """
    if container_name:
        await validate_and_get_container_for_tenant(container_name, tenant_id)
    health_monitor = get_health_monitor()
    cleaned_count = await health_monitor.cleanup_stale_connections(
        container_name=container_name,
        auto_close=True,
        tenant_id=tenant_id,
    )

    message = f"Cleaned up {cleaned_count} stale connections"
    if container_name:
        message += f" for container '{container_name}'"

    return CleanupResponse(
        message=message,
        cleaned_connections=cleaned_count,
        container_name=container_name,
    )


@router.get(
    "/containers",
    summary="Get All Container Health Statistics",
    operation_id="list_container_health",
    description="""
    Get health statistics for the caller tenant's containers with active WebSocket
    connections. Other tenants' containers are never listed.

    Returns a dictionary mapping container names to their health statistics,
    including connection counts, health status breakdown, and performance metrics.
    """,
    response_model=dict[str, ContainerHealthResponse],
)
async def get_all_container_health(
    tenant_id: str = Depends(require_tenant),
) -> dict[str, ContainerHealthResponse]:
    """Get health statistics for the CALLER's containers only."""
    health_monitor = get_health_monitor()
    all_stats = await health_monitor.get_all_container_health(tenant_id=tenant_id)

    response = {}
    for container_name, health_stats in all_stats.items():
        response[container_name] = ContainerHealthResponse(
            container_name=health_stats.container_name,
            total_connections=health_stats.total_connections,
            healthy_connections=health_stats.healthy_connections,
            degraded_connections=health_stats.degraded_connections,
            stale_connections=health_stats.stale_connections,
            average_latency_ms=health_stats.average_latency_ms,
            total_failed_pings=health_stats.total_failed_pings,
            last_cleanup=(
                health_stats.last_cleanup.isoformat() if health_stats.last_cleanup else None
            ),
        )

    return response
