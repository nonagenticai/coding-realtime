"""Error response models for the coding-realtime API.

Extracted from the monolith error_models.py and adapted to be self-contained
within the microservice.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from models.common import SAFE_STRING_PATTERN, ResponseStatus


class StandardErrorResponse(BaseModel):
    """Standard error response format for all API errors."""

    status: ResponseStatus = Field(
        default=ResponseStatus.ERROR,
        description="Response status (always 'error' for error responses)",
    )
    message: str = Field(
        ..., pattern=SAFE_STRING_PATTERN, description="Human-readable error message"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="ISO 8601 timestamp of when the error occurred",
    )
    request_id: str = Field(
        ...,
        pattern=SAFE_STRING_PATTERN,
        description="Unique request identifier for tracking",
    )
    path: str = Field(
        ...,
        pattern=SAFE_STRING_PATTERN,
        description="API endpoint path where the error occurred",
    )
    error_code: str | None = Field(
        None,
        pattern=SAFE_STRING_PATTERN,
        description="Machine-readable error code for programmatic handling",
    )
    details: dict[str, Any] | None = Field(None, description="Additional error context and details")

    @field_serializer("timestamp")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return timestamp.isoformat()

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "error",
                "message": "Resource not found",
                "timestamp": "2025-01-12T10:30:00.000Z",
                "request_id": "req_550e8400",
                "path": "/api/realtime/connection/abc",
                "error_code": "RESOURCE_NOT_FOUND",
                "details": {},
            }
        }
    )


class APIValidationErrorDetail(BaseModel):
    """Detailed information about a field validation error."""

    field: str = Field(..., pattern=SAFE_STRING_PATTERN, description="Field that failed validation")
    message: str = Field(..., pattern=SAFE_STRING_PATTERN, description="Validation error message")
    type: str = Field(..., pattern=SAFE_STRING_PATTERN, description="Type of validation error")
    input: Any = Field(None, description="The invalid input value")


class ValidationErrorResponse(StandardErrorResponse):
    """Error response for request validation failures (422)."""

    status: ResponseStatus = Field(default=ResponseStatus.ERROR, description="Response status")
    message: str = Field(
        default="Request validation failed",
        pattern=SAFE_STRING_PATTERN,
        description="Human-readable error message",
    )
    error_code: str = Field(
        default="VALIDATION_ERROR",
        pattern=SAFE_STRING_PATTERN,
        description="Machine-readable error code",
    )
    validation_errors: list[APIValidationErrorDetail] = Field(
        ..., description="List of validation errors"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "error",
                "message": "Request validation failed",
                "timestamp": "2025-01-12T10:30:00.000Z",
                "request_id": "req_550e8400",
                "path": "/api/realtime/analyze",
                "error_code": "VALIDATION_ERROR",
                "validation_errors": [
                    {
                        "field": "file_path",
                        "message": "Field required",
                        "type": "missing",
                        "input": None,
                    }
                ],
            }
        }
    )


# ── Pre-defined error models for common HTTP status codes ────────────


class Error400Response(StandardErrorResponse):
    """Bad Request (400)."""

    message: str = Field(default="Bad request", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="BAD_REQUEST", pattern=SAFE_STRING_PATTERN)


class Error401Response(StandardErrorResponse):
    """Unauthorized (401)."""

    message: str = Field(default="Authentication required", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="UNAUTHORIZED", pattern=SAFE_STRING_PATTERN)


class Error403Response(StandardErrorResponse):
    """Forbidden (403)."""

    message: str = Field(default="Insufficient permissions", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="FORBIDDEN", pattern=SAFE_STRING_PATTERN)


class Error404Response(StandardErrorResponse):
    """Not Found (404)."""

    message: str = Field(default="Resource not found", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="NOT_FOUND", pattern=SAFE_STRING_PATTERN)


class Error409Response(StandardErrorResponse):
    """Conflict (409)."""

    message: str = Field(default="Resource conflict", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="CONFLICT", pattern=SAFE_STRING_PATTERN)


class Error422Response(ValidationErrorResponse):
    """Unprocessable Entity (422) — alias for ValidationErrorResponse."""


class Error429Response(StandardErrorResponse):
    """Too Many Requests (429)."""

    message: str = Field(default="Too many requests", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="RATE_LIMIT_EXCEEDED", pattern=SAFE_STRING_PATTERN)


class Error500Response(StandardErrorResponse):
    """Internal Server Error (500)."""

    message: str = Field(default="Internal server error", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="INTERNAL_ERROR", pattern=SAFE_STRING_PATTERN)


class Error503Response(StandardErrorResponse):
    """Service Unavailable (503)."""

    message: str = Field(default="Service temporarily unavailable", pattern=SAFE_STRING_PATTERN)
    error_code: str = Field(default="SERVICE_UNAVAILABLE", pattern=SAFE_STRING_PATTERN)
