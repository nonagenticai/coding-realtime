"""Tests for HTTP client adapters (coding_analysis, coding_containers)."""

import logging
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from coding_client.auth.outbound import TenantMissingError
from coding_client.clients import CodingWorkspacesClient

from clients.coding_analysis import (
    CodingAnalysisClient,
    _empty_parse_result,
    get_coding_analysis_client,
)
from clients.coding_containers import (
    CodingContainersClient,
    get_coding_containers_client,
)

# ── CodingAnalysisClient ──


async def test_analysis_parse_file_success():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"success": True, "entities": []}
    mock_resp.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.parse_file("/app/main.py", "print('hi')", language="python")
    assert result["success"] is True
    mock_http.post.assert_called_once()


async def test_analysis_parse_file_http_error():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.parse_file("/app/main.py", "code")
    assert result["success"] is False
    assert len(result["errors"]) > 0


async def test_analysis_get_processor_info_found():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"language": "python", "parser_type": "ast"}
    mock_resp.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_processor_info("main.py")
    assert result is not None
    assert result["language"] == "python"


async def test_analysis_get_processor_info_not_found():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 404

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_processor_info("unknown.xyz")
    assert result is None


async def test_analysis_get_supported_languages():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"languages": ["python", "javascript"]}
    mock_resp.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_supported_languages()
    assert "python" in result


async def test_analysis_get_supported_extensions():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"extensions": [".py", ".js"]}
    mock_resp.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_supported_extensions()
    assert ".py" in result


async def test_analysis_health_check_healthy():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 200

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    assert await client.health_check() is True


async def test_analysis_health_check_unhealthy():
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=httpx.ConnectError("down"))
    mock_http.is_closed = False
    client._client = mock_http

    assert await client.health_check() is False


def test_empty_parse_result():
    result = _empty_parse_result("/app/test.py", "some error")
    assert result["success"] is False
    assert result["file_path"] == "/app/test.py"
    assert "some error" in result["errors"]


# ── CodingContainersClient ──
#
# Every test below drives the REAL SDK client (CodingWorkspacesClient ->
# ServiceClient) over an httpx.MockTransport. That is load-bearing, not a style
# choice. The adapter no longer calls ``.get()`` on an httpx client; it calls
# ``ServiceClient.get()``, which calls ``httpx.AsyncClient.request()``. The old
# tests set ``client._client = AsyncMock()`` and asserted on ``mock_http.get``.
# Against the migrated code those assertions become VACUOUS: an AsyncMock
# answers ``.request`` with a fresh AsyncMock, so the calls still "succeed",
# ``.get`` is never touched, and ``assert_called_once`` would be checking a call
# the production code never makes. Real httpx.Request objects are the only
# mock-proof assertion left, and they are strictly stronger — they pin the URL
# and query string that coding-containers would actually receive.

_ADAPTER_LOGGER = "clients.coding_containers"


def _adapter_records(caplog: pytest.LogCaptureFixture, level: int) -> list[logging.LogRecord]:
    """Records emitted by the adapter itself, at or above ``level``.

    Filtered by logger name on purpose: the SDK's ServiceClient logs its own
    ``service_error`` through structlog, and an assertion like
    ``errors[0].getMessage()`` would start reading the SDK's line instead of
    ours the moment structlog is routed to stdlib.
    """
    return [r for r in caplog.records if r.name == _ADAPTER_LOGGER and r.levelno >= level]


def _install_transport(
    client: CodingContainersClient,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    health: bool = False,
) -> None:
    """Point the SDK client the adapter really uses at a MockTransport.

    ONLY the transport is faked. base_url resolution, URL joining, query
    assembly, header assembly, the ``>=400 -> ServiceError`` mapping and the
    ``204/empty -> None`` mapping are all the real SDK code path, so no header
    and no URL the production code fails to build can be supplied by the double.

    ``base_url=sdk.base_url`` matters: ServiceClient sets base_url on the client
    and then requests a RELATIVE path, so a transport installed without it
    leaves httpx with a bare "/api/workspaces/c1" and raises
    ``ValueError: unknown url type``.
    """
    if health:
        # /health stays on RAW httpx so health_check can read status_code and
        # keep its `== 200` contract (ServiceClient.health_check would call a
        # 3xx healthy). So the seam here is the adapter's own client, not an
        # SDK-owned one.
        existing = client._get_health_client()
        client._health = httpx.AsyncClient(
            base_url=existing.base_url,
            timeout=existing.timeout,
            transport=httpx.MockTransport(handler),
        )
        return

    sdk = client._get_workspaces()
    # Mirrors ServiceClient._ensure_client, timeout included, so the only thing
    # that differs from production is where the bytes go.
    sdk._client = httpx.AsyncClient(
        base_url=sdk.base_url,
        timeout=httpx.Timeout(sdk.timeout),
        transport=httpx.MockTransport(handler),
    )


def _capturing(
    client: CodingContainersClient,
    status: int = 200,
    json_body: Any = None,
    *,
    health: bool = False,
) -> list[httpx.Request]:
    """Serve one canned response and record every request that produced it."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=json_body if json_body is not None else {})

    _install_transport(client, handler, health=health)
    return seen


def _failing(
    client: CodingContainersClient,
    exc_type: type[httpx.RequestError] = httpx.ConnectError,
    *,
    health: bool = False,
) -> list[httpx.Request]:
    """Make every request raise a transport error; record the attempts."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise exc_type("no route to host", request=request)

    _install_transport(client, handler, health=health)
    return seen


async def test_containers_get_container_found():
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing(client, json_body={"name": "c1", "status": "running"})
    try:
        result = await client.get_container("c1", tenant_id="t1")
    finally:
        await client.close()

    assert result is not None
    assert result["name"] == "c1"
    # The SDK owns the route now. Pinning it here is what turns "we adopted the
    # typed client" into a checked claim rather than a hope.
    assert seen[0].url.path == "/api/workspaces/c1"


async def test_containers_get_container_not_found_is_silent(caplog):
    """404 degrades to None AND stays silent, as the explicit branch did.

    The httpx version returned before ``raise_for_status()``, so a clean miss
    never reached ``_log_http_error``. Post-migration a 404 arrives as a
    ServiceError like every other failure, so "silent" has to be re-established
    deliberately — and pinned, or a 404 storm becomes a WARNING storm.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, status=404, json_body={"detail": "no such workspace"})
    try:
        with caplog.at_level(logging.DEBUG):
            result = await client.get_container("missing")
    finally:
        await client.close()

    assert result is None
    assert not _adapter_records(caplog, logging.WARNING)


async def test_containers_get_container_server_error_degrades_to_none(caplog):
    """A 500 must NOT escape as ServiceError. Callers are written against None.

    ``validate_and_get_container_for_tenant`` turns None into an HTTP 404. If a
    ServiceError escaped, a transient upstream 500 would surface as a 500 from
    every endpoint that validates a container first — including the broadcast
    publish path.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, status=500, json_body={"detail": "boom"})
    try:
        with caplog.at_level(logging.DEBUG):
            result = await client.get_container("c1", tenant_id="t1")
    finally:
        await client.close()

    assert result is None
    assert _adapter_records(caplog, logging.WARNING)


async def test_containers_list_containers():
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing(client, json_body={"containers": [{"name": "c1"}, {"name": "c2"}]})
    try:
        result = await client.list_containers(tenant_id="t1")
    finally:
        await client.close()

    assert len(result) == 2
    assert seen[0].url.path == "/api/workspaces/"


async def test_containers_read_file_success():
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing(client, json_body={"content": "print('hello')"})
    try:
        result = await client.read_file("c1", "/app/main.py")
    finally:
        await client.close()

    assert result == "print('hello')"
    assert seen[0].url.path == "/api/workspaces/c1/files"


async def test_containers_read_file_not_found():
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, status=404, json_body={"detail": "no such file"})
    try:
        result = await client.read_file("c1", "/missing.py")
    finally:
        await client.close()

    assert result is None


async def test_containers_health_check():
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing(client, health=True)
    try:
        assert await client.health_check() is True
    finally:
        await client.close()

    assert seen[0].url.path == "/health"


async def test_containers_close_closes_both_sdk_clients():
    """close() must release the data client AND the /health client.

    They are different KINDS: data goes through the SDK's CodingWorkspacesClient,
    while /health stays on a raw httpx.AsyncClient so health_check can read
    status_code. Two pooled clients either way, and closing only one leaks a
    connection pool for the life of the process — main.py's shutdown hook calls
    exactly this method.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    data_sdk = client._get_workspaces()
    await data_sdk._ensure_client()
    data_httpx = data_sdk._client
    health_httpx = client._get_health_client()
    assert data_httpx is not None and health_httpx is not None

    await client.close()

    assert data_httpx.is_closed
    assert health_httpx.is_closed
    assert client._workspaces is None
    assert client._health is None


# ── CodingAnalysisClient — tenant header ──
#
# These go through the real _get_client() and assert on a request built by real
# httpx, not on a mock's call kwargs — a regression that drops the header cannot
# pass them.


async def test_analysis_client_sends_tenant_header_on_every_request():
    """Every outbound request carries X-Tenant-Id.

    Without it this client only reached coding-analysis through that service's
    trusted-network fallback (source IP in an RFC1918 range), i.e. a network
    position rather than a credential.
    """
    client = CodingAnalysisClient(base_url="http://fake:8000", tenant_id="tenant-abc")
    http = await client._get_client()
    try:
        for method, path in (
            ("GET", "/api/quality/languages"),
            ("POST", "/api/analysis/analyze"),
            ("GET", "/health"),
        ):
            request = http.build_request(method, path)
            assert request.headers["x-tenant-id"] == "tenant-abc"
    finally:
        await client.close()


async def test_analysis_client_tenant_header_defaults_to_configured_tenant():
    """The configured tenant reaches the wire, compared to an EXPLICIT value.

    ⚠️ This assertion used to read
    ``request.headers["x-tenant-id"] == get_settings().DEFAULT_TENANT_ID``,
    which is a tautology: it compares the code to itself and stays green for
    *any* value, including the ``"n8n-dev"`` literal that used to be the
    settings default. It could not have caught the defect it was named for.
    The expected value is now written out, and it is the same uuid7 that
    ``clusters/strato/services/coding-realtime/configmap.yaml`` sets in
    production and that ``tests/conftest.py`` puts in the environment.
    """
    client = CodingAnalysisClient(base_url="http://fake:8000")
    http = await client._get_client()
    try:
        request = http.build_request("GET", "/api/quality/languages")
        assert request.headers["x-tenant-id"] == "0195f48b-d000-7000-8000-000000000001"
    finally:
        await client.close()


async def test_analysis_client_never_falls_back_to_the_n8n_dev_literal(monkeypatch):
    """With no tenant configured the client REFUSES; it does not guess.

    The regression this pins: ``settings.DEFAULT_TENANT_ID`` defaulted to
    ``"n8n-dev"``, the stale pre-uuid7 label of a tenant that really exists. An
    unset env var therefore did not fail — it sent a valid
    ``CODING_SERVICE_TOKEN`` while asserting a tenant nobody chose, which is a
    vouched cross-tenant call rather than a 401.

    ``DEFAULT_TENANT_ID`` is bound at import, so patch it where it is used.
    """
    monkeypatch.setattr("clients.coding_analysis.DEFAULT_TENANT_ID", None)
    client = CodingAnalysisClient(base_url="http://fake:8000")
    with pytest.raises(TenantMissingError) as excinfo:
        await client._get_client()
    # Not merely "some error": the message must name the call, so an operator
    # reading the log knows which integration is unconfigured.
    assert "coding-analysis" in str(excinfo.value)


async def test_analysis_client_refusal_survives_a_broad_except(monkeypatch):
    """``TenantMissingError`` is not a ValueError/RuntimeError/OSError.

    Three of this client's call sites wrap it in a narrow ``except`` that
    degrades to a plausible-looking fallback. If the refusal were a
    ``RuntimeError`` those handlers would swallow it and the misconfiguration
    would present as "coding-analysis is a bit slow today".
    """
    monkeypatch.setattr("clients.coding_analysis.DEFAULT_TENANT_ID", None)
    client = CodingAnalysisClient(base_url="http://fake:8000")
    for swallowed in (ValueError, RuntimeError, OSError, httpx.HTTPError):
        with pytest.raises(TenantMissingError):
            try:
                await client._get_client()
            except swallowed:  # pragma: no cover - must not be reached
                raise AssertionError(
                    f"TenantMissingError was absorbed by except {swallowed.__name__}"
                ) from None


# ── CodingAnalysisClient — additional gap coverage ──


async def test_analysis_get_client_creates_new_client():
    """Cover line 32: _get_client creates httpx.AsyncClient when _client is None."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    assert client._client is None
    with patch("clients.coding_analysis.httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.is_closed = False
        mock_cls.return_value = mock_instance
        result = await client._get_client()
        mock_cls.assert_called_once()
        assert result is mock_instance


async def test_analysis_get_client_recreates_when_closed():
    """Cover line 32: _get_client recreates client when existing one is closed."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    old_mock = AsyncMock()
    old_mock.is_closed = True
    client._client = old_mock
    with patch("clients.coding_analysis.httpx.AsyncClient") as mock_cls:
        new_mock = AsyncMock()
        new_mock.is_closed = False
        mock_cls.return_value = new_mock
        result = await client._get_client()
        mock_cls.assert_called_once()
        assert result is new_mock


async def test_analysis_close():
    """Cover lines 40-42: close() calls aclose and sets _client to None."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.is_closed = False
    mock_http.aclose = AsyncMock()
    client._client = mock_http

    await client.close()
    mock_http.aclose.assert_called_once()
    assert client._client is None


async def test_analysis_close_already_closed():
    """close() is a no-op when client is already closed."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.is_closed = True
    client._client = mock_http

    await client.close()
    mock_http.aclose.assert_not_called()


async def test_analysis_close_no_client():
    """close() is a no-op when _client is None."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    assert client._client is None
    await client.close()  # should not raise


async def test_analysis_parse_file_non_200():
    """Cover lines 78-84: parse_file handles HTTPStatusError (non-200 response)."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server Error",
        request=httpx.Request("POST", "http://fake:8000/api/analysis/analyze"),
        response=mock_resp,
    )

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.parse_file("/app/main.py", "code")
    assert result["success"] is False
    assert result["file_path"] == "/app/main.py"
    assert len(result["errors"]) > 0


async def test_analysis_parse_file_network_timeout():
    """parse_file handles network timeout (ReadTimeout)."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.post = AsyncMock(side_effect=httpx.ReadTimeout("read timed out"))
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.parse_file("/app/slow.py", "import time")
    assert result["success"] is False
    assert len(result["errors"]) > 0


async def test_analysis_parse_file_no_language():
    """parse_file omits language key when not provided."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"success": True, "entities": []}
    mock_resp.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_resp)
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.parse_file("/app/main.py", "print('hi')")
    assert result["success"] is True
    # Verify payload did not include language
    call_kwargs = mock_http.post.call_args
    payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
    assert "language" not in payload


async def test_analysis_get_processor_info_network_error():
    """Cover lines 111-117: get_processor_info returns None on network error."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_processor_info("main.py")
    assert result is None


async def test_analysis_get_supported_languages_network_error():
    """Cover lines 127-129: get_supported_languages returns [] on network error."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=httpx.ConnectError("down"))
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_supported_languages()
    assert result == []


async def test_analysis_get_supported_extensions_network_error():
    """Cover lines 139-141: get_supported_extensions returns [] on network error."""
    client = CodingAnalysisClient(base_url="http://fake:8000")
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=httpx.ConnectError("down"))
    mock_http.is_closed = False
    client._client = mock_http

    result = await client.get_supported_extensions()
    assert result == []


def test_get_coding_analysis_client_singleton():
    """Cover lines 185-187: singleton factory returns same instance."""
    import clients.coding_analysis as mod

    mod._client_instance = None
    with patch.object(mod, "get_settings") as mock_settings:
        mock_settings.return_value.CODING_ANALYSIS_URL = "http://fake:8000"
        c1 = get_coding_analysis_client()
        c2 = get_coding_analysis_client()
        assert c1 is c2
    # Clean up to avoid polluting other tests
    mod._client_instance = None


# ── CodingContainersClient — additional gap coverage ──


async def test_containers_builds_and_memoizes_the_sdk_workspaces_client():
    """The adapter delegates to the SDK, and does so with the intended budgets.

    Replaces a test that asserted ``httpx.AsyncClient`` was constructed — the
    adapter no longer constructs one, the SDK does. What is worth pinning now is
    that the two clients exist for their stated reasons: 30s for data, 5s for
    /health, and a single attempt on both.
    """
    client = CodingContainersClient(base_url="http://fake:8000/")
    assert client._workspaces is None
    assert client._health is None

    data_sdk = client._get_workspaces()
    assert isinstance(data_sdk, CodingWorkspacesClient)
    assert client._get_workspaces() is data_sdk, "must be memoized, not rebuilt per call"
    # Trailing slash stripped once by the adapter, again by ServiceClient — a
    # double slash here would produce //api/workspaces/ on the wire.
    assert data_sdk.base_url == "http://fake:8000"
    assert data_sdk.timeout == 30.0

    # /health is a RAW httpx client, not an SDK one: health_check must read
    # status_code to keep its `== 200` contract, which the SDK hides.
    health_httpx = client._get_health_client()
    assert isinstance(health_httpx, httpx.AsyncClient)
    assert client._get_health_client() is health_httpx, "must be memoized, not rebuilt per probe"
    assert health_httpx.timeout.read == 5.0

    # max_retries feeds stop_after_attempt(), so 1 == a single attempt == the
    # no-retry behaviour of the httpx version. See _MAX_ATTEMPTS. Only the SDK
    # client has retries at all; the raw /health client never retried either.
    assert data_sdk.max_retries == 1


async def test_containers_does_not_retry_a_transport_failure():
    """One attempt, as before — the SDK default of 3 is deliberately disabled.

    Every POST /api/broadcast/publish validates a container through this client.
    With the SDK default, a coding-containers outage would add ~1.5s of
    exponential backoff to EVERY publish before failing anyway. This test is the
    guard on that decision: if someone raises _MAX_ATTEMPTS they have to come
    here and say so.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    attempts = _failing(client)
    try:
        assert await client.get_container("c1") is None
    finally:
        await client.close()

    assert len(attempts) == 1


async def test_containers_get_container_network_error():
    """get_container returns None on network error."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _failing(client)
    try:
        result = await client.get_container("c1")
    finally:
        await client.close()

    assert result is None


async def test_containers_list_containers_network_error():
    """list_containers returns [] on network error."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _failing(client)
    try:
        result = await client.list_containers()
    finally:
        await client.close()

    assert result == []


async def test_containers_list_containers_http_error_returns_empty_list():
    """A 404/500 on the listing is [] too — there was never a 404 branch here."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, status=404, json_body={"detail": "nope"})
    try:
        assert await client.list_containers() == []
    finally:
        await client.close()


async def test_containers_list_containers_returns_list_directly():
    """list_containers handles response that is a plain list (not wrapped in dict)."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, json_body=[{"name": "c1"}, {"name": "c2"}])
    try:
        result = await client.list_containers()
    finally:
        await client.close()

    assert len(result) == 2


async def test_containers_list_containers_empty_body_is_an_empty_list():
    """A 204/empty body must be [], not None.

    The httpx version could not reach this: ``resp.json()`` raised
    JSONDecodeError, which is NOT an httpx.HTTPError and so escaped the handler
    entirely. The SDK maps an empty body to None instead, which would have
    silently changed this method's return type from list to None.
    """
    client = CodingContainersClient(base_url="http://fake:8000")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    _install_transport(client, handler)
    try:
        assert await client.list_containers() == []
    finally:
        await client.close()


async def test_containers_read_file_with_tenant_id():
    """read_file scopes by tenant via the X-Tenant-Id HEADER.

    This test previously asserted ``params["tenant_id"]``, pinning the broken
    contract: coding-containers never reads that query parameter (its routes use
    the header-based ``require_tenant`` / ``get_current_user_with_tenant``), so
    the call arrived unauthenticated and was answered 401.

    It then asserted ``params == {"file_path": ...}`` and its docstring called
    that one "real". It was not. The canonical route's parameter is ``path``;
    ``file_path`` was ignored, so every read returned the ``/workspace``
    directory listing and ``content: None``.

    Both halves are still pinned — but now off the REAL outbound request rather
    than off a mock's call kwargs. That upgrade is the point: the old form
    asserted on arguments the adapter passed to ``httpx.AsyncClient.get``, which
    the adapter no longer calls at all. Reading them back off the request that
    coding-containers would receive survives any future change of transport.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing(client, json_body={"content": "data"})
    try:
        result = await client.read_file("c1", "/app/main.py", tenant_id="t1")
    finally:
        await client.close()

    assert result == "data"
    request = seen[0]
    assert request.headers["x-tenant-id"] == "t1"
    assert request.url.params["path"] == "/app/main.py"


async def test_containers_read_file_query_key_matches_canonical_route():
    """The query key must be the one ``GET /api/workspaces/{name}/files`` reads.

    A wrong key here is invisible at runtime: FastAPI ignores unknown query
    parameters, the route's ``path`` falls back to ``/workspace``, and the
    caller gets 200 with ``content: None`` -- indistinguishable from a file that
    is genuinely absent. Only an assertion on the key itself catches it.

    Post-migration the key is handed to ``CodingWorkspacesClient.list_files`` as
    ``path=`` and forwarded as ``**query``, so a typo is still possible and this
    assertion still earns its place. It now reads the key off the encoded query
    string, which additionally catches an SDK change that stopped forwarding
    ``**query`` as params at all — a mock-based assertion could not see that.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing(client, json_body={"path": "/app/main.py", "type": "file", "content": "x"})
    try:
        await client.read_file("c1", "/app/main.py")
    finally:
        await client.close()

    params = seen[0].url.params
    assert set(params) == {"path"}, (
        f"coding-containers files_io.py declares `path: str = Query(...)`; "
        f"sending {sorted(params)} silently reads /workspace instead"
    )


async def test_containers_read_file_directory_response_is_not_content():
    """A directory answer carries no content, and read_file must not invent one.

    This is the exact shape the bug produced: HTTP 200, ``type: directory``,
    ``content: None``. Pinning it proves the None comes from the response body,
    not from an exception path — which is exactly what made the bug undetectable
    in production.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(
        client,
        json_body={
            "path": "/workspace",
            "type": "directory",
            "content": None,
            "entries": [{"name": "src", "type": "directory"}],
        },
    )
    try:
        assert await client.read_file("c1", "/app/main.py") is None
    finally:
        await client.close()


async def test_containers_read_file_unusable_body_is_none_not_a_crash():
    """A non-dict 200 body degrades to None instead of raising AttributeError.

    The SDK returns raw text for a non-JSON body and None for an empty one, so
    ``data.get("content")`` is no longer safe to call unguarded. The httpx
    version would have raised AttributeError here — an exception that is not an
    httpx.HTTPError and so escaped the handler and reached the caller as a 500.
    """
    client = CodingContainersClient(base_url="http://fake:8000")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not json at all")

    _install_transport(client, handler)
    try:
        assert await client.read_file("c1", "/app/main.py") is None
    finally:
        await client.close()


async def test_containers_read_file_network_error():
    """read_file returns None on network error."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _failing(client)
    try:
        result = await client.read_file("c1", "/app/main.py")
    finally:
        await client.close()

    assert result is None


async def test_containers_read_file_server_error_degrades_to_none():
    """A 500 on a read is None, not an escaping ServiceError."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, status=500, json_body={"detail": "boom"})
    try:
        assert await client.read_file("c1", "/app/main.py") is None
    finally:
        await client.close()


async def test_containers_health_check_failure():
    """health_check returns False on network error."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _failing(client, health=True)
    try:
        assert await client.health_check() is False
    finally:
        await client.close()


async def test_containers_health_check_unhealthy_status_is_false():
    """A 503 from /health is False, matching the old ``status_code == 200``."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, status=503, json_body={"status": "degraded"}, health=True)
    try:
        assert await client.health_check() is False
    finally:
        await client.close()


async def test_containers_health_check_uses_the_five_second_budget():
    """Both budgets survive the SDK adoption, on the clients that issue the bytes.

    ServiceClient binds its timeout at construction and exposes no per-request
    override, so the old per-request ``timeout=5.0`` on /health could only be
    preserved by giving it its own client. Asserted on the httpx client that
    actually issues each request, not on a wrapper attribute, so a regression
    that let /health fall back to the 30s data client is caught.

    The connect assertions are the other half: ``httpx.Timeout(30.0)`` applies
    one number to EVERY phase, so adopting the SDK unchanged would have widened
    connect from 5s to 30s on the data path. _WorkspacesClient re-asserts it.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    try:
        health_httpx = client._get_health_client()
        assert health_httpx.timeout.connect == 5.0
        assert health_httpx.timeout.read == 5.0

        # No MockTransport on the data path on purpose: this must inspect the
        # httpx client _WorkspacesClient._ensure_client builds for itself, or
        # the assertion would only read back a timeout the test supplied.
        data_httpx = await client._get_workspaces()._ensure_client()
        assert data_httpx.timeout.read == 30.0
        assert data_httpx.timeout.connect == 5.0, "connect must NOT widen to the read budget"
    finally:
        await client.close()


def test_get_coding_containers_client_singleton():
    """Cover lines 158-160: singleton factory returns same instance."""
    import clients.coding_containers as mod

    mod._client_instance = None
    with patch.object(mod, "get_settings") as mock_settings:
        mock_settings.return_value.CODING_CONTAINERS_URL = "http://fake:8000"
        c1 = get_coding_containers_client()
        c2 = get_coding_containers_client()
        assert c1 is c2
    mod._client_instance = None


async def test_containers_close_is_idempotent():
    """close() is a no-op when nothing was built, and safe to call twice.

    main.py's shutdown hook calls it unconditionally, including on a process
    that never issued a request.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    assert client._workspaces is None
    await client.close()  # nothing built — must not raise

    _capturing(client)
    await client.close()
    await client.close()  # already released — must not raise


async def test_containers_reuses_the_adapter_after_close():
    """A closed adapter still works — it rebuilds on the next call.

    The httpx version got this from an explicit ``is_closed`` check inside
    ``_get_client``. That check is gone, so the property has to be re-proved
    rather than assumed: ``get_coding_containers_client()`` hands out a
    process-wide singleton that outlives an aborted startup, and main.py's
    shutdown hook closes it.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing(client, json_body={"name": "c1"})
    assert await client.get_container("c1") is not None
    await client.close()

    seen = _capturing(client, json_body={"name": "c1"})
    try:
        assert await client.get_container("c1") is not None
    finally:
        await client.close()
    assert len(seen) == 1


# ── CodingContainersClient — tenant header (the dead broadcast path) ──
#
# These drive the REAL SDK client through an httpx.MockTransport, so the
# assertion is on the request coding-containers would actually receive. A
# regression that drops the header, or reverts to `?tenant_id=`, cannot pass
# them vacuously.


def _capturing_client(
    client: CodingContainersClient, status: int = 200, json_body: Any = None
) -> list[httpx.Request]:
    """Back-compat alias for :func:`_capturing`.

    Kept so the tenant-header and service-token tests below read exactly as they
    did before the SDK migration: what they assert has not changed, only where
    the transport is installed.
    """
    return _capturing(client, status=status, json_body=json_body)


async def test_containers_client_sends_tenant_as_a_header_not_a_query_param():
    """coding-containers resolves the tenant from X-Tenant-Id (require_tenant /
    get_current_user_with_tenant). It never reads ?tenant_id=.

    With the query-param form every call arrived unauthenticated and got 401,
    which `except httpx.HTTPError -> return None` reported as "container not
    found" — so `POST /api/broadcast/publish`, which resolves the container via
    validate_and_get_container_for_tenant, returned 404 for EVERY publish.
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing_client(client, json_body={"container_name": "c1"})
    try:
        await client.get_container("c1", tenant_id="tenant-abc")
    finally:
        await client.close()

    assert len(seen) == 1
    request = seen[0]
    assert request.headers["x-tenant-id"] == "tenant-abc"
    assert "tenant_id" not in request.url.params, (
        "the query param is ignored by coding-containers and only leaks the tenant into access logs"
    )


async def test_containers_client_sends_tenant_header_on_list_and_read_file():
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing_client(client, json_body={"containers": [], "content": ""})
    try:
        await client.list_containers(tenant_id="tenant-abc")
        await client.read_file("c1", "/app/main.py", tenant_id="tenant-abc")
    finally:
        await client.close()

    assert len(seen) == 2
    for request in seen:
        assert request.headers["x-tenant-id"] == "tenant-abc"
    # read_file still needs its genuine query parameter -- named `path`, which is
    # what coding-containers' files_io.py actually declares.
    assert seen[1].url.params["path"] == "/app/main.py"
    assert "file_path" not in seen[1].url.params


async def test_containers_client_sends_no_tenant_header_when_unscoped():
    """An unscoped (admin/internal) lookup must NOT invent a tenant — inventing
    one authorises the request against a tenant that may not own the container."""
    client = CodingContainersClient(base_url="http://fake:8000")
    seen = _capturing_client(client, json_body={})
    try:
        await client.get_container("c1")
    finally:
        await client.close()

    assert "x-tenant-id" not in seen[0].headers


async def test_containers_client_logs_a_401_as_an_auth_failure_not_unreachable(caplog):
    """Negative control: an auth refusal must be loud and correctly named.

    It still degrades to None (fail closed), but calling a 401 "unreachable" at
    WARNING is what let this sit undiagnosed — the caller sees 404 and nothing
    in the logs says "authentication".

    Post-migration the 401 reaches the adapter as ``ServiceError.status_code``
    rather than ``HTTPStatusError.response.status_code``, so this control is what
    proves ``_status_of`` did not quietly lose the auth branch and downgrade
    every refusal to "unreachable".
    """
    client = CodingContainersClient(base_url="http://fake:8000")
    _capturing_client(client, status=401, json_body={"detail": "no tenant"})
    try:
        with caplog.at_level(logging.DEBUG):
            result = await client.get_container("c1", tenant_id="tenant-abc")
    finally:
        await client.close()

    assert result is None
    errors = _adapter_records(caplog, logging.ERROR)
    assert errors, [r.getMessage() for r in caplog.records]
    msg = errors[0].getMessage()
    assert "401" in msg
    assert "X-Tenant-Id" in msg


async def test_containers_client_still_logs_a_transport_failure_as_unreachable(caplog):
    """The other half of the control: a genuine connect failure is a WARNING and
    is not mislabelled as an auth problem."""
    client = CodingContainersClient(base_url="http://fake:8000")
    _failing(client)
    try:
        with caplog.at_level(logging.DEBUG):
            result = await client.get_container("c1", tenant_id="tenant-abc")
    finally:
        await client.close()

    assert result is None
    assert not _adapter_records(caplog, logging.ERROR)
    warnings = _adapter_records(caplog, logging.WARNING)
    assert warnings
    assert "unreachable" in warnings[0].getMessage()


# ── on-behalf-of: the service license on outbound calls ──
#
# Under TRUST_TENANT_HEADER=false a bare X-Tenant-Id proves nothing, and the
# callee refuses an asserted tenant unless the caller also proves who it is.
# These pin the credential half; without them a silently-dropped Authorization
# looks exactly like today's passing tests.


class TestServiceTokenOnOutboundCalls:
    """CODING_SERVICE_TOKEN must reach the wire as `Authorization: Bearer`."""

    async def test_containers_client_sends_the_service_license(self, monkeypatch):
        monkeypatch.setenv("CODING_SERVICE_TOKEN", "svc-token-abc")
        client = CodingContainersClient(base_url="http://fake:8000")
        seen = _capturing_client(client, json_body={"name": "c1"})
        try:
            await client.get_container("c1", tenant_id="tenant-abc")
        finally:
            await client.close()

        assert seen[0].headers["authorization"] == "Bearer svc-token-abc"
        # The asserted tenant is the CUSTOMER's and must survive alongside it —
        # putting the credential in X-Tenant-Id would collapse every customer
        # into this service's own tenant.
        assert seen[0].headers["x-tenant-id"] == "tenant-abc"

    async def test_containers_client_sends_the_license_even_with_no_tenant(self, monkeypatch):
        """Proving who we are is not a claim about whose data we want."""
        monkeypatch.setenv("CODING_SERVICE_TOKEN", "svc-token-abc")
        client = CodingContainersClient(base_url="http://fake:8000")
        seen = _capturing_client(client, json_body={"containers": []})
        try:
            await client.list_containers()
        finally:
            await client.close()

        assert seen[0].headers["authorization"] == "Bearer svc-token-abc"
        assert "x-tenant-id" not in seen[0].headers

    async def test_containers_client_sends_no_authorization_when_unset(self, monkeypatch):
        """Absent is absent — never a fabricated or empty credential."""
        monkeypatch.delenv("CODING_SERVICE_TOKEN", raising=False)
        client = CodingContainersClient(base_url="http://fake:8000")
        seen = _capturing_client(client, json_body={"name": "c1"})
        try:
            await client.get_container("c1", tenant_id="tenant-abc")
        finally:
            await client.close()

        assert "authorization" not in seen[0].headers
        assert seen[0].headers["x-tenant-id"] == "tenant-abc"

    async def test_an_empty_service_token_is_not_sent_as_an_empty_bearer(self, monkeypatch):
        """`Bearer ` with no token is a malformed credential, not "no credential".

        The SDK strips and rejects it, but sending it would still burn an
        introspection round-trip on the fleet's hottest auth path.
        """
        monkeypatch.setenv("CODING_SERVICE_TOKEN", "")
        client = CodingContainersClient(base_url="http://fake:8000")
        seen = _capturing_client(client, json_body={"name": "c1"})
        try:
            await client.get_container("c1", tenant_id="tenant-abc")
        finally:
            await client.close()

        assert "authorization" not in seen[0].headers

    async def test_analysis_client_sends_the_service_license(self, monkeypatch):
        monkeypatch.setenv("CODING_SERVICE_TOKEN", "svc-token-abc")
        client = CodingAnalysisClient(base_url="http://fake:8000", tenant_id="t1")
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"languages": []})

        # Build through the real _get_client so the pinned headers are exercised,
        # then swap only the transport — patching the client wholesale would
        # bypass the code under test.
        real = await client._get_client()
        real._transport = httpx.MockTransport(handler)
        try:
            await client.get_supported_languages()
        finally:
            await client.close()

        assert seen[0].headers["authorization"] == "Bearer svc-token-abc"
        assert seen[0].headers["x-tenant-id"] == "t1"
