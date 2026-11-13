"""
WebSocket Rate Limiting Monitoring API

Provides endpoints to monitor WebSocket rate limiting status, statistics,
and violations per container.
"""

from __future__ import annotations

import logging
from typing import Any

from coding_client.auth import require_tenant
from fastapi import Depends, Query
from fastapi.routing import APIRouter
from pydantic import ConfigDict, Field

from models.common import BaseResponse, ResponseStatus
from models.errors import Error404Response, Error500Response
from services.websocket_rate_limiter import (
    ContainerRateStats,
    RateLimitViolation,
    get_websocket_rate_limiter,
)
from utils.container_validation import (
    containers_owned_by_tenant,
    validate_and_get_container_for_tenant,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class RateLimitStatsResponse(BaseResponse):
    """Response model for container rate limit statistics."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    container_name: str = Field(..., description="Container name")
    stats: ContainerRateStats = Field(..., description="Rate limit statistics")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Rate limit statistics retrieved successfully",
                "container_name": "my-container",
                "stats": {
                    "container_name": "my-container",
                    "total_messages": 1500,
                    "messages_per_second": 8.5,
                    "active_connections": 3,
                    "connections_limit": 10,
                    "messages_limit": 10.0,
                    "is_rate_limited": False,
                    "total_violations": 2,
                    "last_violation": "2025-01-06T10:30:00Z",
                },
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


class AllRateLimitStatsResponse(BaseResponse):
    """Response model for all containers' rate limit statistics."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    total_containers: int = Field(..., description="Total number of containers")
    stats: dict[str, ContainerRateStats] = Field(
        ..., description="Rate limit statistics per container"
    )
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Rate limit statistics retrieved for all containers",
                "total_containers": 2,
                "stats": {
                    "container-1": {
                        "container_name": "container-1",
                        "total_messages": 1500,
                        "messages_per_second": 8.5,
                        "active_connections": 3,
                        "connections_limit": 10,
                        "messages_limit": 10.0,
                        "is_rate_limited": False,
                        "total_violations": 0,
                    },
                    "container-2": {
                        "container_name": "container-2",
                        "total_messages": 3200,
                        "messages_per_second": 9.8,
                        "active_connections": 5,
                        "connections_limit": 10,
                        "messages_limit": 10.0,
                        "is_rate_limited": False,
                        "total_violations": 5,
                        "last_violation": "2025-01-06T10:25:00Z",
                    },
                },
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


class RateLimitViolationsResponse(BaseResponse):
    """Response model for rate limit violations."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    total_violations: int = Field(..., description="Total number of violations returned")
    violations: list[RateLimitViolation] = Field(..., description="List of rate limit violations")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Rate limit violations retrieved successfully",
                "total_violations": 3,
                "violations": [
                    {
                        "container_name": "my-container",
                        "violation_type": "message_rate",
                        "timestamp": "2025-01-06T10:30:00Z",
                        "current_value": 15.2,
                        "limit_value": 10.0,
                        "details": {
                            "message": "Message rate limit exceeded",
                            "burst_size": 20,
                        },
                    }
                ],
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


class GlobalRateLimitStatsResponse(BaseResponse):
    """Response model for global rate limiter statistics."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    global_stats: dict[str, Any] = Field(..., description="Global rate limiter statistics")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Global rate limit statistics retrieved successfully",
                "global_stats": {
                    "total_containers": 5,
                    "total_active_connections": 15,
                    "total_messages_processed": 10500,
                    "total_violations": 8,
                    "most_recent_violation": "2025-01-06T10:25:00Z",
                    "config": {
                        "messages_per_second": 10.0,
                        "connections_per_container": 10,
                        "burst_size": 20,
                        "rate_limit_window_seconds": 1.0,
                    },
                },
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


@router.get(
    "/stats/{container_name}",
    summary="Get Rate Limit Stats for Container",
    operation_id="get_container_rate_limit_stats",
    description="""
    Get rate limiting statistics for a specific container.

    Returns detailed statistics including:
    - Total messages processed
    - Current message rate
    - Active connections
    - Rate limit status
    - Violation history
    """,
    response_model=RateLimitStatsResponse,
    responses={
        200: {
            "model": RateLimitStatsResponse,
            "description": "Rate limit statistics retrieved successfully",
        },
        404: {
            "model": Error404Response,
            "description": "Container not found",
        },
        500: {
            "model": Error500Response,
            "description": "Internal server error",
        },
    },
)
async def get_container_rate_limit_stats(
    container_name: str,
    tenant_id: str = Depends(require_tenant),
) -> RateLimitStatsResponse:
    """Get rate limiting statistics for a specific container."""
    # Validate tenant has access to this container (returns 404 if not found or wrong tenant)
    await validate_and_get_container_for_tenant(container_name, tenant_id)

    try:
        rate_limiter = get_websocket_rate_limiter()
        stats = await rate_limiter.get_container_stats(container_name)

        return RateLimitStatsResponse(
            message=f"Rate limit statistics retrieved for container '{container_name}'",
            container_name=container_name,
            stats=stats,
        )
    except Exception as e:
        logger.error(
            "Error getting rate limit stats for container '%s': %s",
            container_name,
            e,
            exc_info=True,
        )
        return RateLimitStatsResponse(
            status=ResponseStatus.ERROR,
            message=f"Error retrieving rate limit statistics: {str(e)}",
            container_name=container_name,
            stats=ContainerRateStats(
                container_name=container_name,
                total_messages=0,
                messages_per_second=0.0,
                active_connections=0,
                connections_limit=0,
                messages_limit=0.0,
                is_rate_limited=False,
            ),
        )


@router.get(
    "/stats",
    summary="Get Rate Limit Stats for All Containers",
    operation_id="list_rate_limit_stats",
    description="""
    Get rate limiting statistics for all containers with active WebSocket connections.

    Returns statistics for each container including message rates, connection counts,
    and violation history.
    """,
    response_model=AllRateLimitStatsResponse,
    responses={
        200: {
            "model": AllRateLimitStatsResponse,
            "description": "Rate limit statistics retrieved successfully",
        },
        500: {
            "model": Error500Response,
            "description": "Internal server error",
        },
    },
)
async def get_all_rate_limit_stats(
    tenant_id: str = Depends(require_tenant),
) -> AllRateLimitStatsResponse:
    """Get rate limiting statistics for the CALLER's containers only."""
    try:
        rate_limiter = get_websocket_rate_limiter()
        every_stat = await rate_limiter.get_all_stats()
        # The limiter is a pod-wide singleton keyed by container name; without
        # this every tenant's container names and traffic were returned.
        owned = await containers_owned_by_tenant(every_stat.keys(), tenant_id)
        all_stats = {name: st for name, st in every_stat.items() if name in owned}

        return AllRateLimitStatsResponse(
            message="Rate limit statistics retrieved for all containers",
            total_containers=len(all_stats),
            stats=all_stats,
        )
    except Exception as e:
        logger.error(
            "Error getting rate limit stats for all containers: %s",
            e,
            exc_info=True,
        )
        return AllRateLimitStatsResponse(
            status=ResponseStatus.ERROR,
            message=f"Error retrieving rate limit statistics: {str(e)}",
            total_containers=0,
            stats={},
        )


@router.get(
    "/violations",
    summary="Get Rate Limit Violations",
    operation_id="list_rate_limit_violations",
    description="""
    Get recent rate limit violations across all containers or for a specific container.

    Violations include:
    - Container name
    - Violation type (message rate, connection limit)
    - Timestamp
    - Current value that exceeded the limit
    - The limit that was exceeded
    """,
    response_model=RateLimitViolationsResponse,
    responses={
        200: {
            "model": RateLimitViolationsResponse,
            "description": "Rate limit violations retrieved successfully",
        },
        500: {
            "model": Error500Response,
            "description": "Internal server error",
        },
    },
)
async def get_rate_limit_violations(
    container_name: str | None = Query(
        None,
        description="Filter violations by container name (optional)",
    ),
    limit: int = Query(
        100,
        description="Maximum number of violations to return",
        ge=1,
        le=1000,
    ),
    tenant_id: str = Depends(require_tenant),
) -> RateLimitViolationsResponse:
    """Get recent rate limit violations — for the CALLER's containers only."""
    # A named container must be the caller's (404 otherwise, BOLA-safe). Outside
    # the try below: its broad except would turn the 404 into a 200.
    if container_name:
        await validate_and_get_container_for_tenant(container_name, tenant_id)
    try:
        rate_limiter = get_websocket_rate_limiter()
        owned = (
            {container_name}
            if container_name
            else await containers_owned_by_tenant(rate_limiter.tracked_container_names(), tenant_id)
        )
        violations = await rate_limiter.get_violations(
            container_name=container_name,
            limit=limit,
            container_names=owned,
        )

        message = "Rate limit violations retrieved successfully"
        if container_name:
            message = f"Rate limit violations retrieved for container '{container_name}'"

        return RateLimitViolationsResponse(
            message=message,
            total_violations=len(violations),
            violations=violations,
        )
    except Exception as e:
        logger.error(
            "Error getting rate limit violations: %s",
            e,
            exc_info=True,
        )
        return RateLimitViolationsResponse(
            status=ResponseStatus.ERROR,
            message=f"Error retrieving rate limit violations: {str(e)}",
            total_violations=0,
            violations=[],
        )


@router.get(
    "/global-stats",
    summary="Get Global Rate Limiter Statistics",
    operation_id="get_global_rate_limit_stats",
    description="""
    Get global rate limiter statistics across all containers.

    Returns aggregated statistics including:
    - Total containers tracked
    - Total active connections
    - Total messages processed
    - Total violations recorded
    - Rate limiter configuration
    """,
    response_model=GlobalRateLimitStatsResponse,
    responses={
        200: {
            "model": GlobalRateLimitStatsResponse,
            "description": "Global statistics retrieved successfully",
        },
        500: {
            "model": Error500Response,
            "description": "Internal server error",
        },
    },
)
async def get_global_rate_limit_stats(
    tenant_id: str = Depends(require_tenant),
) -> GlobalRateLimitStatsResponse:
    """Get rate limiter statistics aggregated over the CALLER's containers."""
    try:
        rate_limiter = get_websocket_rate_limiter()
        # Scoped, not left global: the totals are other tenants' traffic, and
        # custom_limits is keyed by container NAME, so the unscoped form listed
        # every tenant's throttled containers. `config` stays service-wide.
        owned = await containers_owned_by_tenant(rate_limiter.tracked_container_names(), tenant_id)
        global_stats = await rate_limiter.get_global_stats(container_names=owned)

        return GlobalRateLimitStatsResponse(
            message="Global rate limit statistics retrieved successfully",
            global_stats=global_stats,
        )
    except Exception as e:
        logger.error(
            "Error getting global rate limit stats: %s",
            e,
            exc_info=True,
        )
        return GlobalRateLimitStatsResponse(
            status=ResponseStatus.ERROR,
            message=f"Error retrieving global rate limit statistics: {str(e)}",
            global_stats={},
        )
