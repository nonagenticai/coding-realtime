"""GET /metrics and /metrics/{container_name} — WebSocket metrics endpoints."""

from __future__ import annotations

from coding_client.auth import require_tenant
from fastapi import Depends
from fastapi.routing import APIRouter

from models.common import ResponseStatus
from models.errors import Error404Response, Error500Response
from routers.realtime._manager import logger
from routers.realtime._metrics_models import ContainerMetricsResponse, GlobalMetricsResponse
from services.websocket_metrics import get_websocket_metrics_collector
from utils.container_validation import (
    containers_owned_by_tenant,
    validate_and_get_container_for_tenant,
)

router = APIRouter()


@router.get(
    "/metrics/{container_name}",
    summary="WebSocket Metrics",
    operation_id="get_container_ws_metrics",
    description="""
    Container WebSocket Metrics - Get detailed metrics for WebSocket operations

    ## Purpose
    Provides comprehensive metrics for WebSocket operations on a specific container,
    including connection statistics, message throughput, analysis performance, and error rates.

    ## Metrics Categories

    ### Connection Metrics
    - Active connections count
    - Total connections created/closed (lifetime)
    - Average connection duration
    - Oldest connection age

    ### Message Metrics
    - Total messages sent/received
    - Messages per second (last 60 seconds)
    - Last message timestamp

    ### Analysis Metrics
    - Total analyses executed
    - Success/failure counts
    - Execution time statistics (min, max, average)
    - Success rate percentage

    ### Error Metrics
    - Total errors
    - Error rate percentage
    - Errors by type breakdown
    - Last error details

    ## Use Cases
    - **Performance Monitoring**: Track WebSocket operation performance
    - **Capacity Planning**: Understand connection and throughput patterns
    - **Error Analysis**: Identify and diagnose error patterns
    - **SLA Monitoring**: Ensure service level objectives are met
    - **Debugging**: Investigate connection and analysis issues

    ## Response Information
    Returns real-time metrics collected since the container's first WebSocket connection.
    All metrics are container-scoped for proper isolation.

    ## Notes
    - Metrics are collected in real-time during WebSocket operations
    - Message rate calculated over rolling 60-second window
    - Execution times limited to last 1000 operations to prevent memory growth
    - Container must have active or recent WebSocket connections to return metrics
    """,
    response_model=ContainerMetricsResponse,
    responses={
        200: {
            "model": ContainerMetricsResponse,
            "description": "Successfully retrieved container metrics",
        },
        404: {
            "model": Error404Response,
            "description": "Container not found or has no metrics",
        },
        500: {"model": Error500Response, "description": "Internal server error"},
    },
)
async def get_container_websocket_metrics(
    container_name: str,
    tenant_id: str = Depends(require_tenant),
) -> ContainerMetricsResponse:
    """Get WebSocket operation metrics for a specific container."""
    # Ownership is enforced, not advisory: a foreign or unknown container is a
    # 404. This used to log the failure and "continue anyway", which served any
    # tenant's container metrics to anyone who named it. It sits outside the try
    # below because that broad except would turn the 404 into a 200.
    # (validate_and_get_container_for_tenant does not check running state, so a
    # stopped-but-owned container still gets its metrics.)
    await validate_and_get_container_for_tenant(container_name, tenant_id)
    try:
        metrics_collector = get_websocket_metrics_collector()
        container_metrics = await metrics_collector.get_container_metrics(container_name)

        metrics_dict = {
            "timestamp": container_metrics.timestamp.isoformat(),
            "uptime_seconds": container_metrics.uptime_seconds,
            "connections": {
                "active_connections": container_metrics.connections.active_connections,
                "total_connections_created": container_metrics.connections.total_connections_created,
                "total_connections_closed": container_metrics.connections.total_connections_closed,
                "average_connection_duration_seconds": container_metrics.connections.average_connection_duration_seconds,
                "oldest_connection_age_seconds": container_metrics.connections.oldest_connection_age_seconds,
            },
            "messages": {
                "total_messages_sent": container_metrics.messages.total_messages_sent,
                "total_messages_received": container_metrics.messages.total_messages_received,
                "messages_per_second": container_metrics.messages.messages_per_second,
                "last_message_timestamp": (
                    container_metrics.messages.last_message_timestamp.isoformat()
                    if container_metrics.messages.last_message_timestamp
                    else None
                ),
            },
            "analysis": {
                "total_analyses": container_metrics.analysis.total_analyses,
                "successful_analyses": container_metrics.analysis.successful_analyses,
                "failed_analyses": container_metrics.analysis.failed_analyses,
                "average_execution_time_ms": container_metrics.analysis.average_execution_time_ms,
                "min_execution_time_ms": container_metrics.analysis.min_execution_time_ms,
                "max_execution_time_ms": container_metrics.analysis.max_execution_time_ms,
                "total_execution_time_ms": container_metrics.analysis.total_execution_time_ms,
                "success_rate": container_metrics.analysis.success_rate,
            },
            "errors": {
                "total_errors": container_metrics.errors.total_errors,
                "error_rate": container_metrics.errors.error_rate,
                "errors_by_type": container_metrics.errors.errors_by_type,
                "last_error_timestamp": (
                    container_metrics.errors.last_error_timestamp.isoformat()
                    if container_metrics.errors.last_error_timestamp
                    else None
                ),
                "last_error_message": container_metrics.errors.last_error_message,
            },
        }

        return ContainerMetricsResponse(
            message=f"WebSocket metrics retrieved for container '{container_name}'",
            container_name=container_name,
            metrics=metrics_dict,
        )

    except Exception as e:
        logger.error(
            "Error retrieving WebSocket metrics for container %s: %s",
            container_name,
            e,
            exc_info=True,
        )
        return ContainerMetricsResponse(
            status=ResponseStatus.ERROR,
            message=f"Error retrieving metrics: {str(e)}",
            container_name=container_name,
            metrics={},
        )


@router.get(
    "/metrics",
    summary="Global WebSocket Metrics",
    operation_id="get_global_ws_metrics",
    description="""
    Global WebSocket Metrics - Get aggregated metrics across the caller's containers

    ## Purpose
    Provides WebSocket metrics aggregated over the caller tenant's containers.
    Other tenants' containers are never listed or counted.

    ## Response Information
    - Global statistics (total containers, connections, messages, etc.)
    - Per-container summary metrics
    - Aggregated error counts

    ## Use Cases
    - **Monitoring**: Overall WebSocket health of the caller's containers
    - **Capacity Planning**: The caller's resource usage
    - **Comparison**: Compare the caller's containers
    """,
    response_model=GlobalMetricsResponse,
    responses={
        200: {
            "model": GlobalMetricsResponse,
            "description": "Successfully retrieved global metrics",
        },
        500: {"model": Error500Response, "description": "Internal server error"},
    },
)
async def get_global_websocket_metrics(
    tenant_id: str = Depends(require_tenant),
) -> GlobalMetricsResponse:
    """Get WebSocket metrics aggregated over the CALLER's containers."""
    try:
        metrics_collector = get_websocket_metrics_collector()

        # The collector is a pod-wide singleton keyed by container name. Both the
        # per-container list and the totals are computed over the caller's
        # containers only — the unscoped form listed every tenant's containers
        # with their message rates, analysis and error counts.
        every_container = await metrics_collector.get_all_container_metrics()
        owned = await containers_owned_by_tenant(
            (m.container_name for m in every_container), tenant_id
        )
        all_container_metrics = [m for m in every_container if m.container_name in owned]
        global_stats = {
            "total_containers_tracked": len(all_container_metrics),
            "total_active_connections": sum(
                m.connections.active_connections for m in all_container_metrics
            ),
            "total_messages_processed": sum(
                m.messages.total_messages_sent + m.messages.total_messages_received
                for m in all_container_metrics
            ),
            "total_analyses_executed": sum(
                m.analysis.total_analyses for m in all_container_metrics
            ),
            "total_errors": sum(m.errors.total_errors for m in all_container_metrics),
        }

        container_list = []
        for cm in all_container_metrics:
            container_list.append(
                {
                    "container_name": cm.container_name,
                    "active_connections": cm.connections.active_connections,
                    "messages_per_second": cm.messages.messages_per_second,
                    "total_analyses": cm.analysis.total_analyses,
                    "success_rate": cm.analysis.success_rate,
                    "total_errors": cm.errors.total_errors,
                    "error_rate": cm.errors.error_rate,
                }
            )

        return GlobalMetricsResponse(
            message="Global WebSocket metrics retrieved successfully",
            global_stats=global_stats,
            container_metrics=container_list,
        )

    except Exception as e:
        logger.error("Error retrieving global WebSocket metrics: %s", e, exc_info=True)
        return GlobalMetricsResponse(
            status=ResponseStatus.ERROR,
            message=f"Error retrieving global metrics: {str(e)}",
            global_stats={},
            container_metrics=[],
        )
