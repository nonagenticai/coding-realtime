"""HTTP client adapter for the coding-containers microservice.

Provides async methods for container management operations such as
listing containers, reading files from containers, and checking
container health.

Why this module no longer speaks httpx
--------------------------------------
It now delegates to the SDK's :class:`~coding_client.clients.CodingWorkspacesClient`
(coding-client >= 1.19.0) instead of hand-building ``/api/workspaces/...`` URLs
and query parameters. That is not tidiness — it is the fix for a *class* of bug
this module demonstrated end to end:

``read_file`` sent ``params={"file_path": ...}`` to a route whose query parameter
is ``path``. FastAPI ignores an unknown query parameter, so ``path`` fell back to
its ``/workspace`` default, the route took its *directory* branch, and every file
read returned **200** with ``content: None`` — indistinguishable from a file that
is genuinely absent. No status code, no exception, no log line. The one instance
was fixed by hand; spelling the route and its parameter names once, in the SDK,
is what stops the next one.

Tenant scoping is carried in the ``X-Tenant-Id`` HEADER, per request. It used to
be sent as a ``?tenant_id=`` query parameter, which coding-containers does not
read: its routes resolve the tenant with ``require_tenant`` /
``get_current_user_with_tenant``, both header-based. So every call arrived
unauthenticated and coding-containers answered ``401``, which
``except httpx.HTTPError -> return None`` then turned into "container not
found". Live effect: ``POST /api/broadcast/publish`` resolves the container
through :func:`validate_and_get_container_for_tenant`, so **every** publish 404'd
— the REST broadcast path was completely dead end to end even for a container
that demonstrably existed. Mirrors ``CodingAnalysisClient``, except the tenant
here is per-request (the authenticated publisher's), not this service's own
identity.

What the SDK swap does NOT change: the fail-soft contract
---------------------------------------------------------
``CodingWorkspacesClient`` is an *raising* client — every response >= 400 becomes
a :class:`ServiceError`. This adapter is a *degrading* one, and its callers
(``validate_and_get_container_for_tenant``, the broadcast publish path, the
``/health/detailed`` probe) are written against that. So every SDK exception is
caught here and mapped back to the exact ``None`` / ``[]`` / ``False`` the httpx
version returned. Letting a ``ServiceError`` escape would turn today's honest
404 into a 500 on the hot publish path.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from coding_client.auth.outbound import optional_tenant_headers
from coding_client.auth.tenant import TENANT_ID_HEADER
from coding_client.clients import CodingWorkspacesClient, ServiceError

from config.settings import get_settings

logger = logging.getLogger(__name__)

#: Budget for ordinary calls. Matches the old ``httpx.Timeout(30.0, ...)``.
_REQUEST_TIMEOUT_SECONDS = 30.0

#: Connect bound from the old ``httpx.Timeout(30.0, connect=5.0)``. The SDK's
#: ``timeout: float`` collapses every phase to one number, so this is re-asserted
#: in ``_WorkspacesClient`` below rather than being silently widened to 30s.
_CONNECT_TIMEOUT_SECONDS = 5.0

#: Budget for ``/health``. The old code passed ``timeout=5.0`` as a PER-REQUEST
#: override; ``ServiceClient`` fixes its timeout at construction and offers no
#: per-request override, so the 5s budget is preserved by giving ``/health`` its
#: own client instance rather than by weakening the 30s one.
_HEALTH_TIMEOUT_SECONDS = 5.0

#: ``ServiceClient`` passes this to ``stop_after_attempt``, so 1 == a single
#: attempt == NO retry, which is what the httpx version did.
#:
#: Deliberate: the SDK default is 3 attempts with exponential backoff (min 0.5s),
#: which would turn an instant fail into a ~1.5s stall *per call* while
#: coding-containers is down. Every ``POST /api/broadcast/publish`` validates a
#: container through this client, so that stall lands on the broadcast hot path
#: and is multiplied by the fan-out. It also blows the 5s ``/health`` budget
#: outright (3 x 5s + backoff). Raise this to 3 only together with a decision
#: about publish-path latency under a downstream outage.
_MAX_ATTEMPTS = 1

#: Statuses that mean "coding-containers refused us", not "no such container".
#: Logged loudly and separately so an auth regression can never again masquerade
#: as a benign 404.
_AUTH_STATUSES = (401, 403)


def _service_token() -> str | None:
    """This service's own license, read per call so a rotation needs no restart.

    The ``or None`` is cosmetic, not a guard: ``optional_tenant_headers`` tests
    the token for truthiness, so a set-but-empty variable already yields no
    ``Authorization`` header. Verified by mutation — dropping it changes nothing
    observable. Stated here so nobody mistakes it for the thing that stops a bare
    ``Bearer `` going out; that is the SDK's job and it is tested there.
    """
    return os.getenv("CODING_SERVICE_TOKEN") or None


def _tenant_headers(tenant_id: str | None) -> dict[str, str] | None:
    """Build the identity headers for a request, or ``None`` when there are none.

    The tenant here is the authenticated PUBLISHER's, not this service's own, so
    coding-realtime is acting on behalf of a customer. Under
    ``TRUST_TENANT_HEADER=false`` a bare ``X-Tenant-Id`` proves nothing and the
    callee refuses it, so we also send this service's own license in
    ``Authorization``. coding-containers honours the asserted tenant only when
    ``svc-coding-realtime``'s subject is in its ``CODING_ONBEHALF_SUBJECTS``.

    ``Authorization`` is our OWN identity, never a forwarded copy of the
    caller's: publishes also originate from a WebSocket session with no inbound
    Bearer to borrow.

    A missing tenant is still passed through as ``None`` rather than substituted
    with a default: an unscoped call is an admin/internal lookup, and inventing a
    tenant would authorise a request against a tenant that may not own the
    container. When only the credential is available we still send it — proving
    who we are costs nothing and is not a claim about whose data we want.
    """
    headers = optional_tenant_headers(tenant_id, service_token=_service_token())
    return headers or None


def _status_of(exc: Exception) -> int | None:
    """The HTTP status behind a failure, or ``None`` for a transport failure.

    Two shapes reach this now. ``ServiceError`` is what the SDK raises for any
    response >= 400 and carries ``status_code`` directly. ``httpx.HTTPStatusError``
    is kept because it is still what a ``raise_for_status()`` anywhere below us
    would produce, and dropping the branch would silently downgrade a 401 to
    "unreachable" — the precise mislabelling this function exists to prevent.
    """
    if isinstance(exc, ServiceError):
        return exc.status_code
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code
    return None


def _log_http_error(operation: str, exc: Exception) -> None:
    """Log an HTTP failure, distinguishing auth refusal from unreachability."""
    status = _status_of(exc)
    if status in _AUTH_STATUSES:
        logger.error(
            "coding-containers REFUSED %s with HTTP %s — this is an authentication "
            "failure, not a missing container; the caller will see it as 'not found'. "
            "Check the %s header. (%s)",
            operation,
            status,
            TENANT_ID_HEADER,
            exc,
        )
    else:
        logger.warning("coding-containers unreachable for %s: %s", operation, exc)


class _WorkspacesClient(CodingWorkspacesClient):
    """``CodingWorkspacesClient`` with the short connect bound preserved.

    ``ServiceClient`` builds ``httpx.Timeout(self.timeout)``, which applies one
    number to every phase — so adopting the SDK unchanged would widen CONNECT
    from the old 5s to whatever read budget the caller asked for. A data call
    would then sit 30s before noticing a refused connection, on a service whose
    whole job is realtime. Only the connect bound is re-asserted; retries,
    ``ServiceError`` on >=400 and JSON parsing stay the base class's.
    """

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=_CONNECT_TIMEOUT_SECONDS),
            )
        return self._client


class CodingContainersClient:
    """Async HTTP client for the coding-containers service.

    A degrading facade over the SDK's ``CodingWorkspacesClient``. Public class
    name, method names and signatures are unchanged from the hand-rolled httpx
    version — this is an internals swap, and no call site in this repo needed an
    edit for it.
    """

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = (base_url or get_settings().CODING_CONTAINERS_URL).rstrip("/")
        self._workspaces: CodingWorkspacesClient | None = None
        self._health: httpx.AsyncClient | None = None

    def _build(self, timeout: float) -> CodingWorkspacesClient:
        return _WorkspacesClient(
            base_url=self._base_url,
            timeout=timeout,
            max_retries=_MAX_ATTEMPTS,
        )

    def _get_workspaces(self) -> CodingWorkspacesClient:
        """The 30s client used by every data call.

        Constructed lazily and reused, then dropped by :meth:`close` and rebuilt
        on the next call — which is the same lazy re-create the old
        ``_get_client`` implemented by hand with an ``is_closed`` check. Note
        that a *surviving* instance would also be safe: ``ServiceClient``'s own
        ``_ensure_client`` rebuilds its inner ``httpx.AsyncClient`` when closed.
        """
        if self._workspaces is None:
            self._workspaces = self._build(_REQUEST_TIMEOUT_SECONDS)
        return self._workspaces

    def _get_health_client(self) -> httpx.AsyncClient:
        """The 5s raw-httpx client used only by :meth:`health_check`.

        Raw httpx rather than the SDK, deliberately -- see :meth:`health_check`
        for why status fidelity matters there. Built lazily and REUSED, like the
        old ``_get_client``: a probe that rebuilt its client on every call would
        open a fresh connection each time, which is worse than what it replaced.
        Separate from the data client so a slow ``/health`` can never consume a
        real call's 30s budget, and vice versa.
        """
        if self._health is None or self._health.is_closed:
            self._health = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=httpx.Timeout(_HEALTH_TIMEOUT_SECONDS, connect=_CONNECT_TIMEOUT_SECONDS),
            )
        return self._health

    async def close(self) -> None:
        """Close the underlying HTTP clients."""
        if self._workspaces is not None:
            await self._workspaces.close()
            self._workspaces = None
        if self._health is not None:
            await self._health.aclose()
            self._health = None

    # ------------------------------------------------------------------
    # Container operations
    # ------------------------------------------------------------------

    async def get_container(
        self, container_name: str, tenant_id: str | None = None
    ) -> dict[str, Any] | None:
        """Get container details by name.

        Args:
            container_name: Name of the container.
            tenant_id: Tenant to scope the lookup to, sent as ``X-Tenant-Id``.

        Returns:
            Container info dict, or None if not found.
        """
        try:
            workspaces = self._get_workspaces()
            data = await workspaces.get_workspace(
                container_name, headers=_tenant_headers(tenant_id)
            )
        except ServiceError as exc:
            # 404 is a clean miss and stays silent, exactly as the explicit
            # ``if resp.status_code == 404`` branch was. Everything else is a
            # failure worth a line in the log before we degrade to the same None.
            if exc.status_code == 404:
                return None
            _log_http_error(f"container {container_name}", exc)
            return None
        except (httpx.HTTPError, RuntimeError) as exc:
            # RuntimeError is narrow and specific: it is the SDK constructor's
            # "no base_url and no CODING_CONTAINERS_URL" guard. Settings default
            # that variable to "", and the httpx version degraded that same
            # misconfiguration to None (httpx raised UnsupportedProtocol, an
            # HTTPError, which the old except already swallowed). Without this
            # arm a blank config would turn every publish into a 500.
            _log_http_error(f"container {container_name}", exc)
            return None
        return data

    async def list_containers(self, tenant_id: str | None = None) -> list[dict[str, Any]]:
        """List all containers, optionally filtered by tenant.

        Returns:
            List of container info dicts.
        """
        try:
            workspaces = self._get_workspaces()
            data = await workspaces.list_workspaces(headers=_tenant_headers(tenant_id))
        except (ServiceError, httpx.HTTPError, RuntimeError) as exc:
            # No 404 special case here, and there never was one: for a listing a
            # 404 is as useless as a 500, and both answer [].
            _log_http_error("list", exc)
            return []
        if data is None:
            # New arm, not a behaviour change: the SDK maps 204/empty-body to
            # None, where the old code would have raised JSONDecodeError out of
            # ``resp.json()`` — an exception that was NOT an httpx.HTTPError and
            # so escaped the handler entirely. [] is the contract this method
            # declares.
            return []
        return data.get("containers", data) if isinstance(data, dict) else data

    async def read_file(
        self,
        container_name: str,
        file_path: str,
        tenant_id: str | None = None,
    ) -> str | None:
        """Read a file from a container.

        The canonical route is ``GET /api/workspaces/{container_name}/files``
        and its query parameter is ``path``. Both the route and the parameter
        name are now the SDK's (``CodingWorkspacesClient.list_files`` forwards
        ``**query`` as ``params``), so ``path=`` below is the only place this
        service can still get it wrong — and it is pinned by a test that reads
        the key off the real outbound request.

        Args:
            container_name: Name of the container.
            file_path: Path inside the container.
            tenant_id: Optional tenant identifier.

        Returns:
            File content as string, or None on failure.
        """
        try:
            workspaces = self._get_workspaces()
            data = await workspaces.list_files(
                container_name,
                headers=_tenant_headers(tenant_id),
                path=file_path,
            )
        except ServiceError as exc:
            if exc.status_code == 404:
                return None
            _log_http_error(f"file read {container_name}/{file_path}", exc)
            return None
        except (httpx.HTTPError, RuntimeError) as exc:
            _log_http_error(f"file read {container_name}/{file_path}", exc)
            return None
        if not isinstance(data, dict):
            # 204 / empty / non-JSON. The SDK degrades those to None or to raw
            # text rather than raising, so an unusable payload is rejected
            # explicitly instead of reaching ``.get()`` and raising
            # AttributeError at a caller that is expecting ``str | None``.
            return None
        # A directory-shaped 200 carries ``content: None``. Returning that None
        # unchanged is correct and is pinned by a test: the caller's "no content"
        # must come from the response, never from an invented default.
        return data.get("content")

    async def health_check(self) -> bool:
        """Check if the coding-containers service is healthy.

        Deliberately NOT ``ServiceClient.health_check()``. That helper returns
        True for any response below 400, so a 3xx — an ingress redirecting
        ``/health`` to an auth page, say — would read as healthy. The old
        contract was ``status_code == 200`` and it stays that, because the one
        job of this probe is to not lie about liveness.

        Status is not observable through the SDK (it raises on >=400 and hands
        back a parsed body otherwise), so this single call stays on raw httpx.
        Nothing is lost by that: ``/health`` is a constant path with no query
        parameters, so it carries none of the URL/param-construction risk the
        SDK was adopted to remove. Uses the same 5s budget as before, on its own
        connection, so a hung coding-containers cannot consume a data call's.
        """
        try:
            resp = await self._get_health_client().get("/health")
            return resp.status_code == 200
        except httpx.HTTPError:
            return False


# ------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------

_client_instance: CodingContainersClient | None = None


def get_coding_containers_client() -> CodingContainersClient:
    """Return a singleton CodingContainersClient instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = CodingContainersClient()
    return _client_instance
