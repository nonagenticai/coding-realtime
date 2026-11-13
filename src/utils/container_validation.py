"""Container validation utilities for the coding-realtime microservice.

In the monolith these functions hit the local ContainerManager / DockerService.
In the microservice they call the **coding-containers** service over HTTP via
``CodingContainersClient`` from ``coding-client``.

Tenant isolation:
    - validate_and_get_container_for_tenant(): PREFERRED -- tenant-aware
    - validate_and_get_container(): Legacy (admin / internal only)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from typing import Any

from fastapi import HTTPException, status

from config.settings import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exception classes (kept identical to the monolith for compatibility)
# ---------------------------------------------------------------------------


class ContainerNotFoundException(Exception):
    """Container could not be found."""

    def __init__(self, container_name: str, message: str | None = None):
        self.container_name = container_name
        self.message = message or f"Container '{container_name}' not found"
        super().__init__(self.message)


class ContainerAccessDeniedException(Exception):
    """Access to a container was denied."""

    def __init__(self, container_name: str, message: str | None = None):
        self.container_name = container_name
        self.message = message or f"Access denied to container '{container_name}'"
        super().__init__(self.message)


class InvalidContainerStateException(Exception):
    """Container is in an invalid state for the requested operation."""

    def __init__(
        self,
        container_name: str,
        state: str | None = None,
        message: str | None = None,
    ):
        self.container_name = container_name
        self.state = state
        self.message = message or f"Container '{container_name}' is in an invalid state" + (
            f": {state}" if state else ""
        )
        super().__init__(self.message)


# ---------------------------------------------------------------------------
# HTTP error factory helpers
# ---------------------------------------------------------------------------


def handle_container_not_found(container_name: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={
            "error": "ContainerNotFound",
            "message": f"Container '{container_name}' not found",
            "container_name": container_name,
        },
    )


def handle_container_access_denied(container_name: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={
            "error": "ContainerAccessDenied",
            "message": (
                f"Access denied to container '{container_name}'. "
                "You do not have permission to access this container"
            ),
            "container_name": container_name,
        },
    )


def handle_invalid_container_state(container_name: str, state_value: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "error": "InvalidContainerState",
            "message": f"Container '{container_name}' is in an invalid state: {state_value}",
            "container_name": container_name,
            "current_state": state_value,
        },
    )


# ---------------------------------------------------------------------------
# Core validation helpers (call coding-containers over HTTP)
# ---------------------------------------------------------------------------

# Module-level client instance (lazy-init).
_containers_client = None


def _get_containers_client():
    """Lazy-initialise the CodingContainersClient singleton."""
    global _containers_client  # noqa: PLW0603
    if _containers_client is None:
        from clients.coding_containers import CodingContainersClient

        settings = get_settings()
        _containers_client = CodingContainersClient(base_url=settings.CODING_CONTAINERS_URL)
    return _containers_client


def _unwrap_container(container_name: str, result: Any) -> dict[str, Any]:
    """Return the container dict out of coding-containers' response.

    Accepts the ``{"status": ..., "container": {...}}`` envelope (the documented
    ``ContainerDetailResponse``) and, defensively, an already-flat container
    body. Anything else is treated as "not found" rather than passed on: handing
    a consumer a dict whose ``status`` means something else is exactly the
    failure this helper exists to end.
    """
    if not isinstance(result, dict):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Container '{container_name}' not found",
        )

    container = result.get("container")
    if isinstance(container, dict):
        return container

    # Already flat (no envelope) — accept, but say so: it means the
    # coding-containers response contract moved.
    if "container_name" in result:
        logger.warning(
            "coding-containers returned a FLAT container body for '%s' (no 'container' "
            "envelope key) — the response contract has changed; verify consumers.",
            container_name,
        )
        return result

    logger.error(
        "coding-containers response for '%s' carried no container payload (keys=%s)",
        container_name,
        sorted(result),
    )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Container '{container_name}' not found",
    )


async def validate_and_get_container_for_tenant(
    container_name: str,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Validate that a container exists and belongs to *tenant_id*.

    Calls the ``coding-containers`` service endpoint
    ``GET /api/workspaces/{container_name}`` with the tenant header.

    Returns **the container dict itself** — ``container_name``, ``status``,
    ``codebase_path``, … — on success. Raises ``HTTPException(404)`` for both
    "not found" and "wrong tenant" (BOLA protection).

    ⚠️ THE UNWRAP IS THE WHOLE POINT — do not "simplify" it away. coding-containers
    answers with the ``ContainerDetailResponse`` ENVELOPE::

        {"status": "success", "container": {..., "status": "running", ...}}

    This function used to return that envelope, while every consumer reads it as
    if it were the container. The damage was total and silent:

    * ``utils.websocket_utils.extract_container_from_websocket`` read
      ``.get("status")`` → ``"success"``, which is not in ``("running","ready")``
      ⇒ **every** ``/api/broadcast/subscribe`` handshake was rejected 403
      CONTAINER_NOT_RUNNING. No WebSocket subscriber could ever attach, which is
      why the broadcast fan-out sat at ``subscriber_count: 0`` forever while the
      publish side returned a clean 200.
    * ``utils.path_resolution._get_container_codebase_path`` read
      ``.get("codebase_path")`` → ``None`` ⇒ an unconditional HTTP 500
      "has no codebase path configured".

    Both read a key the envelope simply does not have, so both failed the same
    way for every container, forever — and neither failure looked like a bug in
    this function.
    """
    logger.debug("Validating container '%s' for tenant_id=%s", container_name, tenant_id)

    try:
        client = _get_containers_client()
        result = await client.get_container(container_name, tenant_id=tenant_id)

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Container '{container_name}' not found",
            )

        container = _unwrap_container(container_name, result)
        logger.debug("Container '%s' validated for tenant_id=%s", container_name, tenant_id)
        return container

    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "Container validation failed for '%s' (tenant=%s): %s",
            container_name,
            tenant_id,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Container '{container_name}' not found",
        ) from exc


async def containers_owned_by_tenant(
    container_names: Iterable[str],
    tenant_id: str | None,
) -> set[str]:
    """Return the subset of *container_names* that *tenant_id* owns.

    For the in-memory, container-keyed stores (rate limiter, metrics collector)
    that record no tenant of their own. Ownership is decided exactly as for a
    single container — :func:`validate_and_get_container_for_tenant`, i.e. the
    coding-containers lookup with the tenant — so listing endpoints and
    per-container endpoints can never disagree about who owns what.

    FAIL CLOSED: a container that fails validation for ANY reason (not found,
    foreign, coding-containers unreachable) is excluded, and a missing tenant
    owns nothing.
    """
    names = list(dict.fromkeys(n for n in container_names if n))
    if not names or not tenant_id:
        return set()
    results = await asyncio.gather(
        *(validate_and_get_container_for_tenant(n, tenant_id) for n in names),
        return_exceptions=True,
    )
    return {n for n, r in zip(names, results, strict=True) if not isinstance(r, BaseException)}


async def validate_and_get_container(container_name: str) -> dict[str, Any]:
    """Validate a container exists (no tenant filtering -- admin / internal use)."""
    return await validate_and_get_container_for_tenant(container_name, tenant_id=None)
