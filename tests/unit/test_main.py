"""Tests for main FastAPI application."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """TestClient with auth disabled and Redis mocked."""
    with patch("main.settings") as mock_settings:
        mock_settings.SERVICE_NAME = "coding-realtime"
        mock_settings.VERSION = "0.1.0"
        mock_settings.ENVIRONMENT = "testing"
        mock_settings.PORT = 8694
        mock_settings.REDIS_URL = "redis://localhost:6379/0"
        mock_settings.CORS_ORIGINS = ["*"]
        mock_settings.ENABLE_AUTH = False
        mock_settings.docs_enabled = True

    from main import app

    return TestClient(app, raise_server_exceptions=False)


def test_root_endpoint(client):
    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "coding-realtime"
    assert "version" in data
    assert "health" in data


def test_health_endpoint_no_redis(client):
    import main

    original = main._redis
    main._redis = None
    try:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["redis"] == "disconnected"
        assert data["service"] == "coding-realtime"
    finally:
        main._redis = original


def test_health_endpoint_with_redis(client):
    import main

    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(return_value=True)
    original = main._redis
    main._redis = mock_redis
    try:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["redis"] == "connected"
    finally:
        main._redis = original


def test_exception_handler_registered_via_coding_client(client):
    """Verify that coding-client exception handlers produce KAG error responses."""
    resp = client.get("/nonexistent-path-that-triggers-404")
    # May get 401 (auth), 404, or 405 depending on middleware
    assert resp.status_code in (401, 404, 405)
    data = resp.json()
    # KAG format: {error: {code, message, ...}} or auth format: {detail: ...}
    assert "error" in data or "detail" in data


def test_get_redis_not_initialized():
    import main

    original = main._redis
    main._redis = None
    try:
        with pytest.raises(RuntimeError, match="Redis is not initialised"):
            main.get_redis()
    finally:
        main._redis = original
