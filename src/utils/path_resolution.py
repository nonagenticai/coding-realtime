"""Path resolution utilities for container file operations.

In the monolith these called the local ContainerManager.  In the microservice
they call the **coding-containers** service via HTTP to obtain the container's
``codebase_path`` and then perform local path validation / resolution.

Security:
    All path resolution functions protect against path-traversal attacks using
    ``os.path.commonpath()`` to ensure resolved paths stay within container
    boundaries.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import HTTPException

from utils.container_validation import validate_and_get_container_for_tenant

logger = logging.getLogger(__name__)


async def _get_container_codebase_path(container_name: str, tenant_id: str | None = None) -> str:
    """Get the codebase_path for a container from the containers service."""
    container_info = await validate_and_get_container_for_tenant(container_name, tenant_id)

    # container_info is a dict from the HTTP response
    codebase_path = None
    if isinstance(container_info, dict):
        codebase_path = container_info.get("codebase_path")
    elif hasattr(container_info, "codebase_path"):
        codebase_path = container_info.codebase_path

    if not codebase_path:
        raise HTTPException(
            status_code=500,
            detail=f"Container '{container_name}' has no codebase path configured",
        )
    return codebase_path


def resolve_container_path(container_name: str, relative_path: str) -> str:
    """Resolve *relative_path* to an absolute path inside the container.

    **Note:** This is the synchronous variant kept for backward-compat with
    callers that already have the container info.  For the async version that
    fetches container info from coding-containers, use
    ``resolve_container_path_async``.

    The function validates that the resolved path does not escape the
    container's codebase directory.

    Since we cannot call HTTP from a sync function, this variant expects
    ``CONTAINER_PATHS`` to have been pre-populated (e.g. during WebSocket
    connect) or the caller can use the async version instead.
    """
    # Look up cached codebase path set during WS connection lifecycle
    container_root = _container_path_cache.get(container_name)
    if not container_root:
        raise HTTPException(
            status_code=404,
            detail=f"Container '{container_name}' not found (path not cached)",
        )

    normalized_relative = os.path.normpath(relative_path)
    absolute_path = os.path.normpath(os.path.join(container_root, normalized_relative))

    if not validate_path_in_container_sync(container_root, absolute_path):
        logger.warning(
            "Path traversal attempt detected: container=%s, relative_path=%s, resolved=%s",
            container_name,
            relative_path,
            absolute_path,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Invalid path: '{relative_path}' escapes container boundary",
        )

    return absolute_path


async def resolve_container_path_async(
    container_name: str,
    relative_path: str,
    tenant_id: str | None = None,
) -> str:
    """Async version that fetches container info from coding-containers."""
    container_root = await _get_container_codebase_path(container_name, tenant_id)

    normalized_relative = os.path.normpath(relative_path)
    absolute_path = os.path.normpath(os.path.join(container_root, normalized_relative))

    if not validate_path_in_container_sync(container_root, absolute_path):
        logger.warning(
            "Path traversal attempt: container=%s, path=%s, resolved=%s",
            container_name,
            relative_path,
            absolute_path,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Invalid path: '{relative_path}' escapes container boundary",
        )

    # Cache for subsequent sync calls
    _container_path_cache[container_name] = container_root

    return absolute_path


def validate_path_in_container(container_name: str, path: str) -> bool:
    """Validate that *path* is within the container's codebase boundary.

    Returns ``True`` if valid, ``False`` otherwise.
    Uses the cached codebase path for the container.
    """
    container_root = _container_path_cache.get(container_name)
    if not container_root:
        logger.warning("Cannot validate path: container '%s' not in cache", container_name)
        return False
    return validate_path_in_container_sync(container_root, path)


def validate_path_in_container_sync(container_root: str, path: str) -> bool:
    """Pure-function path containment check (no I/O)."""
    try:
        normalized_root = os.path.normpath(container_root)
        normalized_path = os.path.normpath(path)
        common = os.path.commonpath([normalized_root, normalized_path])
        return common == normalized_root
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Container path cache -- populated during WebSocket lifecycle
# ---------------------------------------------------------------------------

_container_path_cache: dict[str, str] = {}


def cache_container_path(container_name: str, codebase_path: str) -> None:
    """Store the codebase path so synchronous callers can resolve paths."""
    _container_path_cache[container_name] = codebase_path


def evict_container_path(container_name: str) -> None:
    """Remove a container from the path cache."""
    _container_path_cache.pop(container_name, None)


# ---------------------------------------------------------------------------
# Async file discovery helpers
# ---------------------------------------------------------------------------


async def discover_files_in_container(
    container_name: str,
    pattern: str = "**/*.py",
    exclude: list[str] | None = None,
    tenant_id: str | None = None,
) -> list[str]:
    """Discover files matching *pattern* inside a container's codebase.

    Returns relative paths (relative to the codebase root).
    """
    if exclude is None:
        exclude = [
            "**/__pycache__/**",
            "**/.git/**",
            "**/node_modules/**",
            "**/venv/**",
            "**/dist/**",
            "**/build/**",
            "**/.pytest_cache/**",
            "**/.mypy_cache/**",
        ]

    codebase_path = Path(await _get_container_codebase_path(container_name, tenant_id))
    if not codebase_path.exists():
        raise OSError(f"Container path does not exist: {codebase_path}")

    matched_files: list[str] = []
    for file_path in codebase_path.glob(pattern):
        if not file_path.is_file():
            continue
        try:
            relative_path = file_path.relative_to(codebase_path)
        except ValueError:
            continue

        excluded = False
        for exclude_pattern in exclude:
            relative_path_str = str(relative_path)
            if relative_path.match(exclude_pattern) or Path(relative_path_str).match(
                exclude_pattern.removeprefix("**/")
            ):
                excluded = True
                break
        if not excluded:
            matched_files.append(str(relative_path))

    return sorted(matched_files)
