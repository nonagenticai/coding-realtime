"""The WS tenant must come from the authenticated context, never the query string.

Regression tests for the 2026-07-29 finding. The ingress Lua filter
(gitops-production .../envoy-filters/ext-authz-enforce.yaml) unconditionally
strips ``x-tenant-id`` off every inbound request and re-injects only what
license-authz resolved — which is what makes the header trustworthy. But it
strips *headers*; it cannot strip a query string. So while
``?tenant_id=`` was honoured, a client denied ``X-Tenant-Id: victim`` could
simply send ``?tenant_id=victim`` and read another tenant's stream.

These tests FAIL against the previous
``query_params.get("tenant_id") or headers.get("X-Tenant-Id")`` implementation.

``resolve_ws_tenant`` became **async** and now returns a ``WsTenantDecision``
(it resolves a verified identity rather than reading a header), and since
coding-client 1.15.0 an uncredentialed handshake is REFUSED rather than served
on its raw header. The pins here are unchanged in intent — the query string must
never grant a tenant — but the *floor* moved: an uncredentialed caller no longer
gets ``tenant_id=None``, it gets refused outright, and a credentialled one gets
the tenant its licence names. Asserting only "``?tenant_id=`` yields None" would
now pass vacuously, because everything uncredentialed yields None. So each case
asserts the refusal too. See ``test_ws_identity.py`` for the identity rules.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from starlette.datastructures import Headers

from tests.conftest import ws_licence_headers
from utils.websocket_utils import resolve_ws_tenant

pytestmark = pytest.mark.usefixtures("ws_licence")


def _ws(*, query: dict[str, str] | None = None, headers: dict[str, str] | None = None):
    ws = MagicMock()
    ws.query_params = query or {}
    ws.headers = Headers(headers or {})
    ws.url.path = "/api/realtime/analysis"
    ws.client = MagicMock(host="10.42.7.13", port=51234)
    return ws


async def test_query_param_alone_does_not_grant_a_tenant():
    """The spoof vector itself: ?tenant_id= must never become a tenant.

    It is now refused rather than merely ignored — with no credential there is
    nothing to resolve. The `refused` assertion is what keeps this test
    non-vacuous: `tenant_id is None` alone is true of every refusal.
    """
    decision = await resolve_ws_tenant(_ws(query={"tenant_id": "victim-tenant"}))
    assert decision.tenant_id is None
    assert decision.refused is True
    assert "victim-tenant" not in (decision.reason or "")


async def test_query_param_cannot_override_a_licence():
    """The query string is ignored even on a handshake that DOES resolve."""
    ws = _ws(
        query={"tenant_id": "victim-tenant"},
        headers=ws_licence_headers("real-tenant"),
    )
    decision = await resolve_ws_tenant(ws)
    assert decision.tenant_id == "real-tenant"
    assert decision.refused is False


async def test_a_bare_header_is_no_longer_honoured():
    """Was `test_header_is_still_honoured`, and its claim is now false.

    A header with no credential behind it was served through coding-client
    1.14.0 — that is the hole. It is kept as a test rather than deleted because
    the assertion is the fix: the same input must now be refused.
    """
    decision = await resolve_ws_tenant(_ws(headers={"X-Tenant-Id": "real-tenant"}))
    assert decision.refused is True
    assert decision.tenant_id is None


async def test_a_licenced_handshake_is_honoured():
    """The green half — without it, refusing everything would pass."""
    decision = await resolve_ws_tenant(_ws(headers=ws_licence_headers("real-tenant")))
    assert decision.tenant_id == "real-tenant"
    assert decision.verified is True
    assert decision.refused is False


async def test_no_tenant_anywhere_is_refused():
    """Callers close the socket with 1008; only the reason changed."""
    decision = await resolve_ws_tenant(_ws())
    assert decision.tenant_id is None
    assert decision.refused is True


def test_no_router_reads_tenant_id_from_query_params():
    """Guard against the pattern being reintroduced anywhere in the service.

    Three separate call sites had this bug; a fix that lands in two of them is
    not a fix. This fails if any src/ file starts reading the query param again.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[2] / "src"
    offenders = [
        str(p.relative_to(src))
        for p in src.rglob("*.py")
        if 'query_params.get("tenant_id")' in p.read_text()
    ]
    assert offenders == [], (
        f"tenant_id read from query params in: {offenders}. The edge filter "
        f"cannot strip query strings — use resolve_ws_tenant()."
    )
