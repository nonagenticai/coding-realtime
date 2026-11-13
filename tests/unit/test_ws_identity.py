"""The WS tenant must be a resolved identity, not a header anyone can set.

`AuthMiddleware` is a `BaseHTTPMiddleware`; Starlette never invokes `dispatch`
on a `websocket` scope. So no middleware and no config flip can protect a WS
route. Once the HTTP paths stopped trusting a bare `X-Tenant-Id`
(`TRUST_TENANT_HEADER=false`), these handlers became the only place left in the
fleet where any pod could assert any tenant and be believed.

Every test here FAILS if the guard in `resolve_ws_tenant` is removed and the
raw header read comes back.

The uncredentialed residual is CLOSED as of coding-client 1.15.0. It used to be
pinned here as a deliberate gap; that gap is what made a credential-less
handshake carrying only a forged `X-Tenant-Id` be believed, and this service
answered `subscription_established` on `/api/broadcast/subscribe` for a
container the caller held no licence for. `TestTheUncredentialedHandshakeIsClosed`
below is the same class, inverted: every case that used to prove the residual
was served now proves it is refused.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from coding_client.auth import _default_tenant as dt_mod
from coding_client.auth import _onbehalf as ob_mod
from coding_client.auth import _trusted_networks as tn_mod
from coding_client.auth.tenant import clear_license_caches
from fastapi.websockets import WebSocketDisconnect
from starlette.datastructures import Headers

from utils.websocket_utils import (
    WebSocketException,
    extract_container_from_websocket,
    resolve_ws_tenant,
)

# svc-coding-gateway — the subject already in this service's
# CODING_ONBEHALF_SUBJECTS, so the allowlisted case here is the production one.
GATEWAY_SUBJECT = "019ffa19-db80-700e-9840-8c17e23b35d5"
STRANGER_SUBJECT = "019ff971-7cbf-7505-8bad-1f86ab493f6e"
SERVICE_TENANT = "service-own-tenant-0001"
CUSTOMER_TENANT = "customer-tenant-0042"
VICTIM_TENANT = "victim-tenant-0099"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Production posture: no header trust, no trusted network, no default."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("SKIP_AUTH", raising=False)
    monkeypatch.delenv("CODING_ONBEHALF_SUBJECTS", raising=False)
    monkeypatch.setenv("TRUST_TENANT_HEADER", "false")
    monkeypatch.setenv("LICENSE_INTROSPECT_URL", "http://test-licenses/license/introspect")
    monkeypatch.setattr(tn_mod, "TRUSTED_NETWORKS", [])
    monkeypatch.setattr(dt_mod, "DEFAULT_TENANT_ID", None)
    ob_mod._warned_synthetic.clear()
    clear_license_caches()
    yield
    clear_license_caches()


def _ws(*, query: dict[str, str] | None = None, headers: dict[str, str] | None = None):
    """A stand-in exposing exactly what the resolver reads off a WebSocket.

    ``headers`` is Starlette's real :class:`Headers`, not a dict: it is
    case-insensitive, and the resolver reads ``X-Tenant-Id`` in one casing and
    ``authorization``/``x-tenant-id`` in another. A plain dict is *narrower than
    the real object* and would make half these assertions pass for the wrong
    reason.
    """
    ws = MagicMock()
    ws.query_params = query or {}
    ws.headers = Headers(headers or {})
    ws.url.path = "/api/realtime/analysis"
    ws.client.host = "10.42.7.13"
    ws.client.port = 51234
    return ws


def _introspection(payload: dict | None):
    """Patch the licence introspection call the SDK makes."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = payload
    client = AsyncMock()
    client.post = AsyncMock(return_value=resp)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return patch("coding_client.auth.tenant.httpx.AsyncClient", return_value=client)


def _licence(subject: str = GATEWAY_SUBJECT) -> dict:
    return {"active": True, "tenant_id": SERVICE_TENANT, "subject_id": subject}


class TestCredentialledCallersCannotForge:
    """The hole this closes: a credential no longer buys an arbitrary tenant."""

    async def test_an_allowlisted_proxy_resolves_to_the_customer_tenant(self, monkeypatch):
        """The gateway's production shape: own licence, customer in the header."""
        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        with _introspection(_licence()):
            decision = await resolve_ws_tenant(
                _ws(headers={"Authorization": "Bearer gw", "X-Tenant-Id": CUSTOMER_TENANT})
            )
        assert decision.refused is False
        assert decision.tenant_id == CUSTOMER_TENANT
        assert decision.verified is True

    async def test_a_non_allowlisted_credential_asserting_a_tenant_is_refused(self, monkeypatch):
        """A pod with a valid licence of its own may not name someone else's."""
        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        with _introspection(_licence(subject=STRANGER_SUBJECT)):
            decision = await resolve_ws_tenant(
                _ws(headers={"Authorization": "Bearer other", "X-Tenant-Id": VICTIM_TENANT})
            )
        assert decision.refused is True
        assert decision.tenant_id is None

    async def test_a_refusal_never_echoes_the_asserted_tenant(self, monkeypatch):
        """The close reason is visible to whoever sent the guess."""
        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        with _introspection(_licence(subject=STRANGER_SUBJECT)):
            decision = await resolve_ws_tenant(
                _ws(headers={"Authorization": "Bearer other", "X-Tenant-Id": VICTIM_TENANT})
            )
        assert VICTIM_TENANT not in (decision.reason or "")
        assert decision.reason

    async def test_an_empty_allowlist_refuses_a_differing_assertion(self):
        """Unset vouches for nobody — the module is inert-but-closed on install."""
        with _introspection(_licence()):
            decision = await resolve_ws_tenant(
                _ws(headers={"Authorization": "Bearer gw", "X-Tenant-Id": VICTIM_TENANT})
            )
        assert decision.refused is True

    async def test_an_agreeing_header_is_not_an_assertion(self):
        """coding-gateway re-injecting the tenant it read from this same token."""
        with _introspection(_licence()):
            decision = await resolve_ws_tenant(
                _ws(headers={"Authorization": "Bearer gw", "X-Tenant-Id": SERVICE_TENANT})
            )
        assert decision.refused is False
        assert decision.tenant_id == SERVICE_TENANT
        assert decision.verified is True

    async def test_no_assertion_resolves_to_the_licence_tenant(self):
        with _introspection(_licence()):
            decision = await resolve_ws_tenant(_ws(headers={"Authorization": "Bearer gw"}))
        assert decision.tenant_id == SERVICE_TENANT
        assert decision.verified is True


class TestTheUncredentialedHandshakeIsClosed:
    """Closed as of coding-client 1.15.0 — this class used to pin it open.

    The justification for keeping it open was real while it held: a browser
    cannot set `Authorization` on a WS handshake, so a browser-originated socket
    is credentialed only if a server-side proxy does it, and refusing before the
    proxies were converted would have broken the live panels instead of
    hardening them.

    The WARNING it emitted was the instrument for finding those proxies, and it
    has reported: over ~10.5h of retention on THIS service it fired 5 times,
    every one from peer=127.0.0.1 (a verification probe), and 0 times on
    coding-analysis over ~18h. No real caller ever used it. coding-gateway now
    attaches its own licence on WS upgrades and fr presents svc-fr-backend's on
    both of its WS proxies (driven live: 101 with auth, 403 without).
    """

    async def test_an_uncredentialed_caller_is_refused(self):
        """The forgery itself: a header, and nothing at all behind it."""
        decision = await resolve_ws_tenant(_ws(headers={"X-Tenant-Id": CUSTOMER_TENANT}))
        assert decision.refused is True
        assert decision.tenant_id is None

    async def test_the_refusal_never_echoes_the_asserted_tenant(self):
        """The close reason goes back to whoever sent the guess."""
        decision = await resolve_ws_tenant(_ws(headers={"X-Tenant-Id": CUSTOMER_TENANT}))
        assert decision.reason
        assert CUSTOMER_TENANT not in decision.reason

    async def test_the_refusal_warns_and_names_the_peer(self, caplog):
        """Refusing silently would hide a caller that legitimately broke.

        The WARNING moved rather than disappeared: it used to say "accepted",
        it now says "refused", and it still names the peer so an unconverted
        caller is identifiable from the log alone.
        """
        with caplog.at_level(logging.WARNING):
            await resolve_ws_tenant(_ws(headers={"X-Tenant-Id": CUSTOMER_TENANT}))
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "an uncredentialed refusal must be logged, not silent"
        rendered = " ".join(r.getMessage() for r in warnings)
        assert "Refused an uncredentialed WebSocket handshake" in rendered
        assert "10.42.7.13" in rendered
        assert "51234" in rendered

    async def test_a_credentialled_caller_does_not_trip_the_refusal_warning(self, caplog):
        """The warning must name only the callers still to convert."""
        with caplog.at_level(logging.WARNING), _introspection(_licence()):
            await resolve_ws_tenant(_ws(headers={"Authorization": "Bearer gw"}))
        rendered = " ".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert "Refused an uncredentialed WebSocket handshake" not in rendered

    async def test_no_credential_and_no_header_is_refused(self):
        decision = await resolve_ws_tenant(_ws())
        assert decision.refused is True
        assert decision.tenant_id is None

    async def test_an_inactive_licence_is_refused(self):
        """A revoked token is not a credential, and now buys nothing."""
        with _introspection({"active": False, "reason": "token_invalid"}):
            decision = await resolve_ws_tenant(
                _ws(headers={"Authorization": "Bearer revoked", "X-Tenant-Id": CUSTOMER_TENANT})
            )
        assert decision.refused is True
        assert decision.tenant_id is None

    async def test_every_served_decision_is_verified(self):
        """There is no longer a path that yields a tenant nobody verified.

        `verified` is kept on WsTenantDecision precisely so this can be
        asserted: a served decision with `verified is False` means the residual
        came back.
        """
        with _introspection(_licence()):
            decision = await resolve_ws_tenant(_ws(headers={"Authorization": "Bearer gw"}))
        assert decision.refused is False
        assert decision.verified is True
        assert decision.tenant_id == SERVICE_TENANT

    async def test_the_decision_is_taken_from_should_close_not_from_the_outcome(self):
        """A handler that re-derives `outcome is Outcome.REFUSED` bypasses the fix.

        This is the mutation that reopens the hole while every other test here
        still passes, so it is pinned directly: an identity whose `should_close`
        is True must be refused even when its outcome is NOT REFUSED.
        """
        from unittest.mock import patch as _patch

        from coding_client.auth.websocket import Outcome, WebSocketIdentity

        identity = WebSocketIdentity(
            outcome=Outcome.NO_CREDENTIAL, reason="no verifiable credential on the handshake"
        )
        assert identity.outcome is not Outcome.REFUSED
        assert identity.should_close is True
        with _patch(
            "utils.websocket_utils.resolve_websocket_identity",
            new=AsyncMock(return_value=identity),
        ):
            decision = await resolve_ws_tenant(_ws(headers={"X-Tenant-Id": CUSTOMER_TENANT}))
        assert decision.refused is True
        assert decision.tenant_id is None


class TestTheQueryStringBackdoorStaysClosed:
    """The 2026-07-29 finding. The edge filter cannot strip a query string."""

    async def test_query_param_alone_does_not_grant_a_tenant(self):
        decision = await resolve_ws_tenant(_ws(query={"tenant_id": VICTIM_TENANT}))
        assert decision.tenant_id is None
        # Non-vacuous: since 1.15.0 every uncredentialed handshake yields None,
        # so `tenant_id is None` alone no longer discriminates.
        assert decision.refused is True

    async def test_query_param_cannot_override_a_licence(self):
        """The query string is ignored even on a handshake that DOES resolve."""
        with _introspection(_licence()):
            decision = await resolve_ws_tenant(
                _ws(
                    query={"tenant_id": VICTIM_TENANT},
                    headers={"Authorization": "Bearer gw", "X-Tenant-Id": SERVICE_TENANT},
                )
            )
        assert decision.tenant_id == SERVICE_TENANT
        assert decision.refused is False

    def test_no_router_reads_tenant_id_from_query_params(self):
        """Guard against the pattern being reintroduced anywhere in the service."""
        from pathlib import Path

        src = Path(__file__).resolve().parents[2] / "src"
        py_files = list(src.rglob("*.py"))
        assert py_files, "positive control: the census must actually see source files"
        offenders = [
            str(p.relative_to(src))
            for p in py_files
            if 'query_params.get("tenant_id")' in p.read_text()
        ]
        assert offenders == [], (
            f"tenant_id read from query params in: {offenders}. The edge filter "
            f"cannot strip query strings — use resolve_ws_tenant()."
        )


class TestTheContainerPathRefusesToo:
    """extract_container_from_websocket resolves a tenant of its own.

    It is reachable independently of the two routers, so a fix that lands only
    in the routers is not a fix.
    """

    async def test_a_refused_assertion_aborts_container_validation(self, monkeypatch):
        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        ws = _ws(
            query={"container_name": "victim-box"},
            headers={"Authorization": "Bearer other", "X-Tenant-Id": VICTIM_TENANT},
        )
        with (
            _introspection(_licence(subject=STRANGER_SUBJECT)),
            patch(
                "utils.websocket_utils.validate_and_get_container_for_tenant",
                new=AsyncMock(return_value={"status": "running"}),
            ) as validate,
            pytest.raises(WebSocketException) as excinfo,
        ):
            await extract_container_from_websocket(ws)

        assert excinfo.value.code == 1008
        assert VICTIM_TENANT not in excinfo.value.reason
        validate.assert_not_awaited()

    async def test_an_allowlisted_assertion_validates_against_the_customer_tenant(
        self, monkeypatch
    ):
        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        ws = _ws(
            query={"container_name": "cust-box"},
            headers={"Authorization": "Bearer gw", "X-Tenant-Id": CUSTOMER_TENANT},
        )
        with (
            _introspection(_licence()),
            patch(
                "utils.websocket_utils.validate_and_get_container_for_tenant",
                new=AsyncMock(return_value={"status": "running"}),
            ) as validate,
        ):
            assert await extract_container_from_websocket(ws) == "cust-box"

        validate.assert_awaited_once_with("cust-box", CUSTOMER_TENANT)


class TestTheRoutersCloseOnRefusal:
    """Each router must act on the refusal, not merely receive it.

    These drive the real handler through the real resolver — no patched
    `resolve_ws_tenant` — so a router that reads the decision and serves anyway
    fails here. Coverage on `main` showed both refusal branches at zero: a guard
    nothing exercises is a guard that can be silently deleted.
    """

    def _refusing_ws(self):
        ws = AsyncMock()
        ws.query_params = {"container_name": "victim-box"}
        ws.headers = Headers({"Authorization": "Bearer other", "X-Tenant-Id": VICTIM_TENANT})
        ws.url.path = "/api/realtime/analysis"
        ws.client = MagicMock(host="10.42.7.13", port=51234)
        ws.close = AsyncMock()
        ws.accept = AsyncMock()
        return ws

    async def test_the_realtime_analysis_router_closes_1008(self, monkeypatch):
        from routers.realtime.websocket import websocket_realtime_analysis

        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        ws = self._refusing_ws()
        with (
            _introspection(_licence(subject=STRANGER_SUBJECT)),
            patch(
                "routers.realtime.websocket.WebSocketContainerContext.__aenter__",
                new=AsyncMock(return_value="victim-box"),
            ) as ctx,
        ):
            await websocket_realtime_analysis(ws, connection_id="conn-1", workspace_path=None)

        ws.close.assert_awaited_once()
        reason = ws.close.await_args.kwargs["reason"]
        assert ws.close.await_args.kwargs["code"] == 1008
        assert VICTIM_TENANT not in reason
        ctx.assert_not_awaited()
        # Must be the AUTHORISATION refusal, not the generic missing-tenant
        # close. A router that drops `decision.refused` still reaches 1008 by
        # accident, because a refusal carries no tenant — asserting only the
        # code would pass against exactly the bug this test exists to catch.
        assert "Tenant context required" not in reason
        assert "may not act for the tenant it asserted" in reason

    async def test_the_broadcast_subscribe_router_closes_1008(self, monkeypatch):
        from routers.broadcast_websocket import websocket_broadcast_subscribe

        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        ws = self._refusing_ws()
        ws.url.path = "/api/broadcast/subscribe"
        with (
            _introspection(_licence(subject=STRANGER_SUBJECT)),
            patch(
                "routers.broadcast_websocket.extract_container_from_websocket",
                new=AsyncMock(return_value="victim-box"),
            ) as extract,
        ):
            await websocket_broadcast_subscribe(
                ws,
                connection_id="conn-1",
                event_types=None,
                tags=None,
                priority=None,
                replay_history=False,
                replay_limit=100,
            )

        ws.close.assert_awaited_once()
        reason = ws.close.await_args.kwargs["reason"]
        assert ws.close.await_args.kwargs["code"] == 1008
        assert VICTIM_TENANT not in reason
        extract.assert_not_awaited()
        # See the realtime case above: the code alone does not discriminate.
        assert "Tenant context required" not in reason
        assert "may not act for the tenant it asserted" in reason

    async def test_an_allowlisted_proxy_is_not_closed_by_the_broadcast_router(self, monkeypatch):
        """The green half: the guard must not close the gateway's own sockets."""
        from routers.broadcast_websocket import websocket_broadcast_subscribe

        monkeypatch.setenv("CODING_ONBEHALF_SUBJECTS", GATEWAY_SUBJECT)
        ws = self._refusing_ws()
        ws.headers = Headers({"Authorization": "Bearer gw", "X-Tenant-Id": CUSTOMER_TENANT})
        with (
            _introspection(_licence()),
            patch(
                "routers.broadcast_websocket.extract_container_from_websocket",
                new=AsyncMock(return_value="cust-box"),
            ),
            patch(
                "routers.broadcast_websocket.validate_and_get_container_for_tenant",
                new=AsyncMock(return_value={"name": "cust-box"}),
            ) as validate,
        ):
            ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect())
            await websocket_broadcast_subscribe(
                ws,
                connection_id="conn-1",
                event_types=None,
                tags=None,
                priority=None,
                replay_history=False,
                replay_limit=100,
            )

        # Resolved to the CUSTOMER tenant, not the gateway's own.
        validate.assert_awaited_once_with("cust-box", CUSTOMER_TENANT)
        for call in ws.close.await_args_list:
            assert call.kwargs.get("code") != 1008
