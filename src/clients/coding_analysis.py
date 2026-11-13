"""HTTP client adapter for the coding-analysis microservice.

Provides async methods that mirror the analysis service API, allowing
the realtime service to delegate code analysis operations over HTTP
instead of importing monolith code directly.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from coding_client.auth.outbound import tenant_headers
from coding_client.auth.tenant import DEFAULT_TENANT_ID

from config.settings import get_settings

logger = logging.getLogger(__name__)

# Reusable timeout configuration
_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


class CodingAnalysisClient:
    """Async HTTP client for the coding-analysis service.

    Every request carries ``X-Tenant-Id``. Without it this client was only
    reaching coding-analysis via that service's trusted-network fallback
    (source IP in an RFC1918 range), which is a network-position bypass, not a
    credential — and it grants the *same* DEFAULT_TENANT_ID context this header
    asks for explicitly. Sending the header keeps behaviour identical while
    letting coding-analysis stop trusting the pod CIDR. Mirrors
    ``coding-testing``'s ``_executor_headers``.

    The call sites (language-capability plumbing, cache stats, health) carry no
    per-request tenant, so this is the service's own identity.

    ⚠️ The tenant comes from ``coding_client.auth.tenant.DEFAULT_TENANT_ID``,
    which is ``None`` when the env var is unset. It used to come from a local
    ``settings.DEFAULT_TENANT_ID`` field that defaulted to ``"n8n-dev"``. That
    literal is not junk and it is not a real tenant_id either, which is what made
    it dangerous: it is the *stale label form* of a tenant that really exists
    (uuid7 since the 2026-04-29 Keycloak migration). Because this client also
    sends ``CODING_SERVICE_TOKEN``, an unset env var would not have 401'd — it
    would have made coding-realtime **vouch, with a valid credential, for a
    tenant nobody chose**. There is no safe literal to pick, so a missing tenant
    is an error: see :func:`coding_client.auth.outbound.tenant_headers`.
    """

    def __init__(self, base_url: str | None = None, tenant_id: str | None = None) -> None:
        settings = get_settings()
        self._base_url = (base_url or settings.CODING_ANALYSIS_URL).rstrip("/")
        self._tenant_id = tenant_id or DEFAULT_TENANT_ID
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            # Identity headers are built once and pinned on the client because
            # this client's tenant is fixed at construction (this service's own),
            # unlike CodingContainersClient's per-request publisher tenant.
            #
            # The service license rides in Authorization alongside it. Read here
            # rather than at import so a rotated secret is picked up when the
            # client is next rebuilt, and so tests can set it per case.
            #
            # ``tenant_headers``, not ``optional_tenant_headers``: everything
            # this client reaches (parse_file, processor info, cache stats) is
            # tenant-SCOPED, so a missing tenant is a cross-tenant
            # authorisation bug, not a lost log line. It raises
            # ``TenantMissingError`` rather than quietly omitting the header.
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=_DEFAULT_TIMEOUT,
                headers=tenant_headers(
                    self._tenant_id,
                    operation="coding-analysis",
                    service_token=os.getenv("CODING_SERVICE_TOKEN") or None,
                ),
            )
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Analysis operations
    # ------------------------------------------------------------------

    async def parse_file(
        self,
        file_path: str,
        content: str,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Parse a single file via the analysis service.

        Calls POST /api/analysis/analyze on coding-analysis.

        Args:
            file_path: Path to the source file.
            content: Source code content.
            language: Optional language hint.

        Returns:
            Parse result dictionary with entities, relationships, etc.
        """
        client = await self._get_client()
        payload: dict[str, Any] = {
            "file_path": file_path,
            "content": content,
        }
        if language:
            payload["language"] = language
        try:
            resp = await client.post("/api/analysis/analyze", json=payload)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "coding-analysis parse-file returned %s for %s: %s",
                exc.response.status_code,
                file_path,
                exc.response.text[:200],
            )
            return _empty_parse_result(file_path, str(exc))
        except httpx.HTTPError as exc:
            logger.warning(
                "coding-analysis unreachable for parse-file %s: %s",
                file_path,
                exc,
            )
            return _empty_parse_result(file_path, str(exc))

    async def get_processor_info(self, file_path: str) -> dict[str, Any] | None:
        """Check whether a processor exists for a given file extension.

        Calls GET /api/analysis/capabilities on coding-analysis.

        Returns:
            Dict with processor metadata, or None if unsupported.
        """
        client = await self._get_client()
        try:
            resp = await client.get(
                "/api/analysis/capabilities",
                params={"file_path": file_path},
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            logger.warning(
                "coding-analysis unreachable for processor-info %s: %s",
                file_path,
                exc,
            )
            return None

    async def get_supported_languages(self) -> list[str]:
        """Fetch the list of supported languages from the analysis service."""
        client = await self._get_client()
        try:
            resp = await client.get("/api/quality/languages")
            resp.raise_for_status()
            data = resp.json()
            return data.get("languages", [])
        except httpx.HTTPError as exc:
            logger.warning("coding-analysis unreachable for languages: %s", exc)
            return []

    async def get_supported_extensions(self) -> list[str]:
        """Fetch the list of supported file extensions from the analysis service."""
        client = await self._get_client()
        try:
            resp = await client.get("/api/quality/extensions")
            resp.raise_for_status()
            data = resp.json()
            return data.get("extensions", [])
        except httpx.HTTPError as exc:
            logger.warning("coding-analysis unreachable for extensions: %s", exc)
            return []

    async def health_check(self) -> bool:
        """Check if the coding-analysis service is healthy."""
        client = await self._get_client()
        try:
            resp = await client.get("/health", timeout=5.0)
            return resp.status_code == 200
        except httpx.HTTPError:
            return False


# ------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------

_client_instance: CodingAnalysisClient | None = None


def get_coding_analysis_client() -> CodingAnalysisClient:
    """Return a singleton CodingAnalysisClient instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = CodingAnalysisClient()
    return _client_instance


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _empty_parse_result(file_path: str, error: str) -> dict[str, Any]:
    """Return a minimal failed parse-result dict."""
    return {
        "success": False,
        "file_path": file_path,
        "language": "unknown",
        "parser_used": "none",
        "entities": [],
        "relationships": [],
        "lines_of_code": 0,
        "complexity_score": 0.0,
        "parse_time_ms": 0.0,
        "errors": [error],
        "warnings": [],
        "properties": {},
    }
