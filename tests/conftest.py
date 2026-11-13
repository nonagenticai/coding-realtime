"""Shared test fixtures."""

import os

# ── Auth preconditions these suites need (coding-client >=1.7.0, >=1.9.0) ──
# These suites exercise ENDPOINT behaviour, not the auth boundary, and dispatch
# from 127.0.0.1 via TestClient. Two coding-client changes removed the implicit
# defaults they used to ride on, so the preconditions are now stated explicitly:
#
#   >=1.7.0 (#33) TRUSTED_CIDRS no longer defaults to
#           "127.0.0.1/32,::1/128,192.168.0.0/16,172.16.0.0/12,10.0.0.0/8".
#           That old default contained the k3s pod CIDR (10.42.0.0/16), so every
#           pod in the cluster satisfied require_tenant's trusted-network bypass.
#           Loopback ONLY here: the pod CIDR stays out, so no test can ever rely
#           on the bypass that default was changed to remove.
#   >=1.9.0 (#36) DEFAULT_TENANT_ID no longer falls back to the literal
#           "n8n-dev" (a stale pre-uuid7 label). Unset now resolves to None and
#           the bypass refuses to mint a caller rather than inventing a tenant.
#
# Without both, 57 endpoint tests 401 on a branch that used to be on by default.
# The value mirrors production exactly — clusters/strato/services/coding-realtime/
# configmap.yaml sets the same uuid7 — so these tests act as the tenant the
# service really acts as, and production is unaffected by the 1.9.0 change.
os.environ.setdefault("TRUSTED_CIDRS", "127.0.0.1/32,::1/128")
os.environ.setdefault("DEFAULT_TENANT_ID", "0195f48b-d000-7000-8000-000000000001")

from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import pytest  # noqa: E402

# ── Credentialled WebSocket handshakes (coding-client >= 1.15.0) ─────────────
# Through coding-client 1.14.0 a WS handshake with NO Authorization at all was
# served on whatever X-Tenant-Id it happened to carry, so a suite exercising WS
# *behaviour* could name a tenant with a bare header. 1.15.0 closes that: an
# uncredentialed handshake is refused, because a header nobody verified proves
# nothing. See src/utils/websocket_utils.resolve_ws_tenant.
#
# Those behaviour suites now have to present a licence, exactly as coding-gateway
# and fr do in production. `ws_licence_headers()` builds the handshake and the
# `ws_licence` fixture is what makes it introspect.
#
# The token ENCODES the tenant so each test keeps naming its own, and the licence
# comes back with that same tenant — the "agreeing header" shape, which needs no
# on-behalf allowlist. Nothing here widens the boundary: an Authorization header
# the helper did not build still introspects to `active: false` and is refused.

_WS_LICENCE_PREFIX = "licence-for::"
#: svc-coding-gateway — the subject this service really sees on WS upgrades.
WS_LICENCE_SUBJECT = "019ffa19-db80-700e-9840-8c17e23b35d5"


def ws_licence_headers(tenant_id: str, **extra: str) -> dict[str, str]:
    """Headers for a handshake `resolve_ws_tenant` will ACCEPT for *tenant_id*.

    Requires the `ws_licence` fixture to be active in the same test.
    """
    return {
        "Authorization": f"Bearer {_WS_LICENCE_PREFIX}{tenant_id}",
        "X-Tenant-Id": tenant_id,
        **extra,
    }


@pytest.fixture
def ws_licence(monkeypatch):
    """Make tokens minted by :func:`ws_licence_headers` introspect as live.

    Deliberately NOT autouse. A global "every Bearer is valid" stub would make
    the auth boundary itself untestable — tests/unit/test_ws_identity.py exists
    to prove uncredentialed and non-allowlisted handshakes are refused, and it
    would pass vacuously. Modules opt in with::

        pytestmark = pytest.mark.usefixtures("ws_licence")
    """
    from unittest.mock import patch

    from coding_client.auth.tenant import clear_license_caches

    def _post(url, json=None, **kwargs):  # noqa: A002 - httpx's own kwarg name
        token = (json or {}).get("token", "")
        resp = MagicMock()
        resp.status_code = 200
        if token.startswith(_WS_LICENCE_PREFIX):
            resp.json.return_value = {
                "active": True,
                "tenant_id": token[len(_WS_LICENCE_PREFIX) :],
                "subject_id": WS_LICENCE_SUBJECT,
            }
        else:
            resp.json.return_value = {"active": False, "reason": "token_invalid"}
        return resp

    client = AsyncMock()
    client.post = AsyncMock(side_effect=_post)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    # Without this, `_introspect_license` short-circuits to None before any HTTP
    # call and every handshake here would be uncredentialed — the stub would be
    # inert and the suites would fail for a reason that has nothing to do with
    # what they test.
    monkeypatch.setenv("LICENSE_INTROSPECT_URL", "http://test-licenses/license/introspect")

    clear_license_caches()
    with patch("coding_client.auth.tenant.httpx.AsyncClient", return_value=client):
        yield
    clear_license_caches()


@pytest.fixture
def mock_db_session():
    """Mock database session."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


@pytest.fixture
def mock_websocket():
    """Mock WebSocket connection."""
    ws = AsyncMock()
    ws.send_json = AsyncMock()
    ws.send_text = AsyncMock()
    ws.receive_json = AsyncMock()
    ws.receive_text = AsyncMock()
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.client = MagicMock()
    ws.client.host = "127.0.0.1"
    ws.query_params = {"container_name": "test-container"}
    ws.headers = {}
    # Default to CONNECTED state
    from fastapi.websockets import WebSocketState

    ws.client_state = WebSocketState.CONNECTED
    return ws


@pytest.fixture
def broadcast_service():
    """Fresh WebSocketBroadcastService instance."""
    from services.websocket_broadcast import WebSocketBroadcastService

    return WebSocketBroadcastService()


@pytest.fixture
def connection_registry():
    """Fresh WebSocketConnectionRegistry instance (rate limiting disabled)."""
    from services.websocket_connection_registry import WebSocketConnectionRegistry

    return WebSocketConnectionRegistry(enable_rate_limiting=False)


@pytest.fixture
def health_monitor():
    """Fresh WebSocketHealthMonitor instance."""
    from services.websocket_health_monitor import WebSocketHealthMonitor

    return WebSocketHealthMonitor()


@pytest.fixture
def rate_limiter():
    """Fresh WebSocketRateLimiter instance."""
    from services.websocket_rate_limiter import WebSocketRateLimiter

    return WebSocketRateLimiter()


@pytest.fixture
def metrics_collector():
    """Fresh WebSocketMetricsCollector instance."""
    from services.websocket_metrics import WebSocketMetricsCollector

    return WebSocketMetricsCollector()
