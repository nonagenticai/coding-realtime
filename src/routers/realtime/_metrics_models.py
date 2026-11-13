"""Response models for the realtime metrics endpoints."""

from __future__ import annotations

from typing import Any

from pydantic import ConfigDict, Field

from models.common import BaseResponse, ResponseStatus


class ContainerMetricsResponse(BaseResponse):
    """Response model for container metrics endpoint."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    container_name: str = Field(..., description="Container name")
    metrics: dict[str, Any] = Field(..., description="Container metrics data")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "container_name": "my-container",
                "metrics": {
                    "timestamp": "2025-01-06T10:30:00Z",
                    "uptime_seconds": 3600.0,
                    "connections": {
                        "active_connections": 5,
                        "total_connections_created": 10,
                        "total_connections_closed": 5,
                        "average_connection_duration_seconds": 720.0,
                    },
                    "messages": {
                        "total_messages_sent": 1000,
                        "total_messages_received": 500,
                        "messages_per_second": 2.5,
                    },
                    "analysis": {
                        "total_analyses": 200,
                        "successful_analyses": 195,
                        "failed_analyses": 5,
                        "average_execution_time_ms": 45.2,
                        "success_rate": 97.5,
                    },
                    "errors": {
                        "total_errors": 8,
                        "error_rate": 0.4,
                        "errors_by_type": {
                            "analysis_error": 5,
                            "send_error": 3,
                        },
                    },
                },
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )


class GlobalMetricsResponse(BaseResponse):
    """Response model for global metrics endpoint."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    global_stats: dict[str, Any] = Field(..., description="Global statistics")
    container_metrics: list[dict[str, Any]] = Field(
        default_factory=list, description="Metrics for all containers"
    )
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "global_stats": {
                    "total_containers_tracked": 3,
                    "total_active_connections": 10,
                    "total_messages_processed": 5000,
                    "total_analyses_executed": 1000,
                    "total_errors": 25,
                },
                "container_metrics": [
                    {
                        "container_name": "container-1",
                        "active_connections": 5,
                        "messages_per_second": 2.5,
                        "success_rate": 97.5,
                    }
                ],
                "timestamp": "2025-01-06T10:30:00Z",
            }
        }
    )
