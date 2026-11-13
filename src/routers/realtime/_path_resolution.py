"""Shared file-path resolution helper used by realtime message handlers.

The helper does NOT touch the connection_manager — handlers retain full
control over the response payload. This lets each handler-module test
patch its own ``connection_manager`` symbol without needing to also patch
this module.
"""

from __future__ import annotations

import logging
from pathlib import Path

from utils.path_resolution import resolve_container_path

logger = logging.getLogger("routers.realtime")


class PathResolutionError(Exception):
    """Raised when container path resolution fails. Wraps the original cause."""


def resolve_file_path_sync(
    *,
    raw_file_path: str,
    container_name: str | None,
    workspace_path: Path | None,
) -> str:
    """Resolve an inbound file path within the container/workspace context.

    Returns the resolved path on success. Raises :class:`PathResolutionError`
    if container-scoped resolution fails — the caller is responsible for
    converting that into a user-facing error message.
    """
    file_path = raw_file_path
    if container_name:
        try:
            resolved_path = resolve_container_path(container_name, file_path)
            logger.debug(
                "Resolved path for container %s: %s -> %s",
                container_name,
                file_path,
                resolved_path,
            )
            return resolved_path
        except Exception as e:
            logger.warning(
                "Path resolution failed for container %s, path %s: %s",
                container_name,
                file_path,
                e,
            )
            raise PathResolutionError(str(e)) from e
    if workspace_path and not Path(file_path).is_absolute():
        return str(Path(workspace_path) / file_path)
    return file_path
