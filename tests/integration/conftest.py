"""Integration test fixtures — real FastAPI app with mocked external services."""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

# Modules that snapshot the settings singleton at import time
# (`settings = get_settings()` at module scope). Patching only
# `config.settings._settings` reaches these ONLY if this fixture happens to
# trigger their very first import — see `mock_settings` for why that is not
# something we can rely on.
_SETTINGS_SNAPSHOT_TARGETS = ("main.settings", "routers.health.settings")


@pytest.fixture
def mock_redis():
    """A mock Redis that passes ping."""
    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)
    mock.aclose = AsyncMock()
    mock.publish = AsyncMock(return_value=1)
    mock.subscribe = AsyncMock()
    return mock


@pytest.fixture
def mock_settings():
    """Patch the settings singleton *and* every module-level snapshot of it.

    `src/main.py:27` and `src/routers/health.py:17` both do
    `settings = get_settings()` at module scope, binding a snapshot of the real
    `Settings` object the first time the module is imported. Patching only
    `config.settings._settings` therefore fixes those modules only when this
    fixture triggers their first import.

    That held for `pytest tests/integration` but NOT for the full suite CI runs:
    four `tests/unit/*` modules (`test_main_additional`, `test_router_*`) import
    `main` at module scope, so pytest's *collection* pass binds the snapshot to
    the real settings long before any fixture executes. The result was five
    tests that pass in isolation and fail in the full run — `/health` reporting
    `environment="production"` and `/` reporting `docs=None`. Patch the
    snapshots by name so the fixture is import-order independent.
    """
    s = MagicMock()
    s.SERVICE_NAME = "coding-realtime"
    s.VERSION = "1.0.0"
    s.ENVIRONMENT = "testing"
    s.PORT = 8694
    s.REDIS_URL = "redis://localhost:6379/0"
    s.CORS_ORIGINS = ["*"]
    s.ENABLE_AUTH = False
    s.ENABLE_DOCS = True
    s.docs_enabled = True
    s.LOG_LEVEL = "DEBUG"
    s.LOG_FORMAT = "text"
    s.CODING_ANALYSIS_URL = "http://localhost:8688"
    s.CODING_CONTAINERS_URL = "http://localhost:8691"
    s.WS_PING_INTERVAL = 30
    s.WS_PING_TIMEOUT = 10
    s.WS_MAX_FAILED_PINGS = 3
    s.WS_STALE_THRESHOLD = 300
    s.WS_MAX_CONNECTIONS_PER_CONTAINER = 10
    s.WS_MESSAGE_RATE_LIMIT = 10.0
    s.WS_RATE_LIMIT_BURST = 20

    with ExitStack() as stack:
        # Patch the singleton first: it makes any *first* import triggered by
        # the snapshot patches below see the mock too.
        stack.enter_context(patch("config.settings._settings", s))
        for target in _SETTINGS_SNAPSHOT_TARGETS:
            stack.enter_context(patch(target, s))
        yield s


@pytest.fixture
async def async_client(mock_settings, mock_redis):
    """httpx.AsyncClient wired to the real FastAPI ASGI app with mocked externals."""
    with patch("main._redis", mock_redis):
        from main import app

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            yield client
