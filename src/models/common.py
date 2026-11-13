"""Common API models shared across all coding-realtime endpoints.

Provides BaseResponse, ResponseStatus, SuccessResponse, PaginationParams,
HealthStatus, and ComponentHealth -- extracted from the monolith
common_api_models.py and adapted for the microservice.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Pattern from coding-client types.constraint_types
SAFE_STRING_PATTERN = r"^[^\x00-\x08\x0B\x0C\x0E-\x1F\x7F]*$"


class ResponseStatus(StrEnum):
    """Standard API response status values."""

    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class BaseResponse(BaseModel):
    """Base response model for all API responses."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Operation completed successfully",
                "timestamp": "2025-07-13T10:30:00Z",
                "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            }
        }
    )

    status: ResponseStatus = Field(..., description="Response status")
    message: str = Field(..., description="Human-readable message", pattern=SAFE_STRING_PATTERN)
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Response timestamp",
    )
    request_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        min_length=1,
        description="Unique request identifier",
    )


class SuccessResponse(BaseResponse):
    """Standard success response with optional data payload."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    data: dict[str, Any] | None = Field(None, description="Response data")


class PaginationParams(BaseModel):
    """Standard pagination parameters for list endpoints."""

    model_config = ConfigDict(json_schema_extra={"example": {"page": 1, "limit": 20}})

    page: int = Field(1, ge=1, description="Page number (1-based)")
    limit: int = Field(50, ge=1, le=1000, description="Items per page")


class PaginatedResponse(SuccessResponse):
    """Response for paginated list endpoints."""

    pagination: dict[str, Any] = Field(..., description="Pagination metadata")
    items: list[Any] = Field(..., description="Response items")

    @field_validator("pagination")
    @classmethod
    def validate_pagination(cls, v: dict) -> dict:
        required_keys = {"page", "limit", "total", "pages"}
        if not all(key in v for key in required_keys):
            raise ValueError(f"Pagination must contain: {required_keys}")
        return v


class HealthStatus(StrEnum):
    """System component health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ComponentHealth(BaseModel):
    """Health status for an individual system component."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Redis",
                "status": "healthy",
                "message": "Connection active",
                "last_check": "2025-07-13T10:30:00Z",
                "metrics": {"latency_ms": 1.2},
            }
        }
    )

    name: str = Field(..., min_length=1, pattern=SAFE_STRING_PATTERN, description="Component name")
    status: HealthStatus = Field(..., description="Health status")
    message: str | None = Field(None, pattern=SAFE_STRING_PATTERN, description="Status message")
    last_check: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Last health check",
    )
    metrics: dict[str, Any] | None = Field(None, description="Component metrics")
