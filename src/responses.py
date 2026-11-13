"""Unified response helpers for consistent API responses.

Provides standardised success / error factory functions so every endpoint
returns the same shape.

Usage:
    from responses import success_response, not_found, bad_request

    return success_response(data={"items": items}, message="OK")
    raise not_found("Container", container_name)
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status


def success_response(data: Any, message: str = "Success") -> dict:
    """Create a standardised success response."""
    return {
        "status": "success",
        "message": message,
        "data": data,
    }


def error_response(
    status_code: int,
    message: str,
    error_code: str = "INTERNAL_ERROR",
    details: dict | None = None,
) -> HTTPException:
    """Create a standardised error HTTPException."""
    return HTTPException(
        status_code=status_code,
        detail={
            "status": "error",
            "message": message,
            "error_code": error_code,
            "details": details or {},
        },
    )


def not_found(resource: str, identifier: str) -> HTTPException:
    """404 Not Found."""
    return error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        message=f"{resource} '{identifier}' not found",
        error_code=f"{resource.upper().replace(' ', '_')}_NOT_FOUND",
    )


def bad_request(message: str, details: dict | None = None) -> HTTPException:
    """400 Bad Request."""
    return error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        message=message,
        error_code="BAD_REQUEST",
        details=details,
    )


def validation_error(message: str, field: str, details: dict | None = None) -> HTTPException:
    """422 Validation Error."""
    return error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        message=message,
        error_code="VALIDATION_ERROR",
        details={"field": field, **(details or {})},
    )


def internal_error(message: str, details: dict | None = None) -> HTTPException:
    """500 Internal Server Error."""
    return error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        message=message,
        error_code="INTERNAL_ERROR",
        details=details,
    )


def unauthorized(message: str = "Authentication required") -> HTTPException:
    """401 Unauthorized."""
    return error_response(
        status_code=status.HTTP_401_UNAUTHORIZED,
        message=message,
        error_code="UNAUTHORIZED",
    )


def forbidden(message: str = "Access denied") -> HTTPException:
    """403 Forbidden."""
    return error_response(
        status_code=status.HTTP_403_FORBIDDEN,
        message=message,
        error_code="FORBIDDEN",
    )


def conflict(resource: str, message: str) -> HTTPException:
    """409 Conflict."""
    return error_response(
        status_code=status.HTTP_409_CONFLICT,
        message=message,
        error_code=f"{resource.upper().replace(' ', '_')}_CONFLICT",
    )


def service_unavailable(message: str = "Service temporarily unavailable") -> HTTPException:
    """503 Service Unavailable."""
    return error_response(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        message=message,
        error_code="SERVICE_UNAVAILABLE",
    )


def timeout_error(message: str = "Request timed out") -> HTTPException:
    """504 Gateway Timeout."""
    return error_response(
        status_code=status.HTTP_504_GATEWAY_TIMEOUT,
        message=message,
        error_code="TIMEOUT",
    )


def rate_limited(message: str = "Rate limit exceeded") -> HTTPException:
    """429 Too Many Requests."""
    return error_response(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        message=message,
        error_code="RATE_LIMITED",
    )
