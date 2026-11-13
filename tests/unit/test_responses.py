"""Tests for response helper functions."""

from typing import Any, cast

from fastapi import HTTPException

from responses import (
    bad_request,
    conflict,
    error_response,
    forbidden,
    internal_error,
    not_found,
    rate_limited,
    service_unavailable,
    success_response,
    unauthorized,
    validation_error,
)


def _detail(exc: HTTPException) -> dict[str, Any]:
    """Cast HTTPException.detail to dict for test assertions."""
    return cast(dict[str, Any], exc.detail)


def test_success_response():
    resp = success_response(data={"items": [1, 2]}, message="OK")
    assert resp["status"] == "success"
    assert resp["message"] == "OK"
    assert resp["data"]["items"] == [1, 2]


def test_error_response():
    exc = error_response(400, "bad", error_code="BAD")
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 400
    assert _detail(exc)["error_code"] == "BAD"


def test_not_found():
    exc = not_found("Container", "abc")
    assert exc.status_code == 404
    assert "abc" in _detail(exc)["message"]


def test_bad_request():
    exc = bad_request("invalid input")
    assert exc.status_code == 400
    assert _detail(exc)["message"] == "invalid input"
    assert _detail(exc)["error_code"] == "BAD_REQUEST"


def test_validation_error():
    exc = validation_error("bad field", "name")
    assert exc.status_code == 422
    assert _detail(exc)["details"]["field"] == "name"
    assert _detail(exc)["error_code"] == "VALIDATION_ERROR"


def test_internal_error():
    exc = internal_error("oops")
    assert exc.status_code == 500
    assert _detail(exc)["message"] == "oops"
    assert _detail(exc)["error_code"] == "INTERNAL_ERROR"


def test_unauthorized():
    exc = unauthorized()
    assert exc.status_code == 401
    assert _detail(exc)["error_code"] == "UNAUTHORIZED"


def test_forbidden():
    exc = forbidden()
    assert exc.status_code == 403
    assert _detail(exc)["error_code"] == "FORBIDDEN"


def test_conflict():
    exc = conflict("Container", "already exists")
    assert exc.status_code == 409
    assert _detail(exc)["message"] == "already exists"
    # error_code is derived from the resource name: CONTAINER_CONFLICT
    assert _detail(exc)["error_code"] == "CONTAINER_CONFLICT"


def test_service_unavailable():
    exc = service_unavailable()
    assert exc.status_code == 503
    assert _detail(exc)["error_code"] == "SERVICE_UNAVAILABLE"


def test_rate_limited():
    exc = rate_limited()
    assert exc.status_code == 429
    assert _detail(exc)["error_code"] == "RATE_LIMITED"
