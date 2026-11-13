"""Tests for websocket_utils."""

from unittest.mock import AsyncMock

import pytest

from utils.websocket_utils import (
    WebSocketErrorCode,
    WebSocketException,
    create_error_response,
    create_invalid_path_error,
    create_path_access_denied_error,
    create_path_not_found_error,
    create_path_outside_container_error,
    extract_container_from_websocket,
)


def test_error_code_values():
    assert WebSocketErrorCode.MISSING_CONTAINER_PARAM == "MISSING_CONTAINER_PARAM"
    assert WebSocketErrorCode.RATE_LIMIT_EXCEEDED == "RATE_LIMIT_EXCEEDED"
    assert WebSocketErrorCode.INTERNAL_ERROR == "INTERNAL_ERROR"


def test_websocket_exception():
    exc = WebSocketException(
        code=1008,
        reason="test error",
        error_code=WebSocketErrorCode.CONTAINER_NOT_FOUND,
        details={"key": "val"},
    )
    assert exc.code == 1008
    assert exc.reason == "test error"
    assert exc.error_code == WebSocketErrorCode.CONTAINER_NOT_FOUND
    assert exc.details == {"key": "val"}


def test_websocket_exception_defaults():
    exc = WebSocketException(code=1011, reason="oops")
    assert exc.error_code == WebSocketErrorCode.INTERNAL_ERROR
    assert exc.details == {}


def test_create_error_response():
    resp = create_error_response(WebSocketErrorCode.CONTAINER_NOT_FOUND, "not found", {"id": "x"})
    assert resp["type"] == "error"
    assert resp["error"] == "not found"
    assert resp["error_code"] == "CONTAINER_NOT_FOUND"
    assert resp["details"] == {"id": "x"}
    # Verify all expected keys are present
    assert set(resp.keys()) == {"type", "error", "error_code", "details"}


def test_create_error_response_without_details():
    """When no details provided, details should be an empty dict, not missing."""
    resp = create_error_response(WebSocketErrorCode.INTERNAL_ERROR, "something broke")
    assert resp["type"] == "error"
    assert resp["error_code"] == "INTERNAL_ERROR"
    assert resp["details"] == {}


def test_create_path_not_found_error():
    exc = create_path_not_found_error("c1", "/bad/path")
    assert isinstance(exc, WebSocketException)
    assert exc.error_code == WebSocketErrorCode.PATH_NOT_FOUND
    assert "c1" in exc.reason


def test_create_path_outside_container_error():
    exc = create_path_outside_container_error("c1", "/etc/passwd", "/app")
    assert exc.error_code == WebSocketErrorCode.PATH_OUTSIDE_CONTAINER
    assert exc.details["container_root"] == "/app"


def test_create_invalid_path_error():
    exc = create_invalid_path_error("c1", "bad*path", "invalid chars")
    assert exc.error_code == WebSocketErrorCode.INVALID_PATH_FORMAT


def test_create_path_access_denied_error():
    exc = create_path_access_denied_error("c1", "/secret", "no perms")
    assert exc.error_code == WebSocketErrorCode.PATH_ACCESS_DENIED


async def test_extract_container_missing_param():
    ws = AsyncMock()
    ws.query_params = {}
    ws.headers = {}
    with pytest.raises(WebSocketException) as exc_info:
        await extract_container_from_websocket(ws)
    assert exc_info.value.error_code == WebSocketErrorCode.MISSING_CONTAINER_PARAM


async def test_extract_container_empty_name():
    ws = AsyncMock()
    ws.query_params = {"container_name": "  "}
    ws.headers = {}
    with pytest.raises(WebSocketException) as exc_info:
        await extract_container_from_websocket(ws)
    assert exc_info.value.error_code == WebSocketErrorCode.INVALID_CONTAINER_NAME
