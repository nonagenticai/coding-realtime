"""``watch_file`` / ``unwatch_file`` realtime handlers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from routers.realtime._manager import connection_manager, logger
from routers.realtime._path_resolution import PathResolutionError, resolve_file_path_sync


async def handle_watch_file(
    connection_id: str,
    message: dict[str, Any],
    workspace_path: Path | None = None,
    container_name: str | None = None,
):
    """Handle file watching request with container-scoped path resolution."""
    try:
        file_path = message.get("file_path")
        if not file_path:
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": "Missing file_path",
                },
            )
            return

        try:
            file_path = resolve_file_path_sync(
                raw_file_path=file_path,
                container_name=container_name,
                workspace_path=workspace_path,
            )
        except PathResolutionError as e:
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": f"Invalid file path: {str(e)}",
                },
            )
            return

        connection_manager.watch_file(connection_id, file_path)
        await connection_manager.send_message(
            connection_id,
            {
                "type": "watch_started",
                "timestamp": datetime.now(UTC).isoformat(),
                "file_path": file_path,
                "container_name": container_name,
                "message": f"Started watching {file_path}",
            },
        )
        logger.debug("Started watching %s for connection %s", file_path, connection_id)
    except (ValueError, OSError, AttributeError) as e:
        logger.error("Error in watch_file: %s", e)
        await connection_manager.send_message(
            connection_id,
            {
                "type": "error",
                "timestamp": datetime.now(UTC).isoformat(),
                "container_name": container_name,
                "error": str(e),
            },
        )


async def handle_unwatch_file(
    connection_id: str,
    message: dict[str, Any],
    workspace_path: Path | None = None,
    container_name: str | None = None,
):
    """Handle file unwatching request with container-scoped path resolution."""
    try:
        file_path = message.get("file_path")
        if not file_path:
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": "Missing file_path",
                },
            )
            return

        try:
            file_path = resolve_file_path_sync(
                raw_file_path=file_path,
                container_name=container_name,
                workspace_path=workspace_path,
            )
        except PathResolutionError as e:
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": f"Invalid file path: {str(e)}",
                },
            )
            return

        connection_manager.unwatch_file(connection_id, file_path)
        await connection_manager.send_message(
            connection_id,
            {
                "type": "watch_stopped",
                "timestamp": datetime.now(UTC).isoformat(),
                "file_path": file_path,
                "container_name": container_name,
                "message": f"Stopped watching {file_path}",
            },
        )
        logger.debug("Stopped watching %s for connection %s", file_path, connection_id)
    except (ValueError, OSError, AttributeError) as e:
        logger.error("Error in unwatch_file: %s", e)
        await connection_manager.send_message(
            connection_id,
            {
                "type": "error",
                "timestamp": datetime.now(UTC).isoformat(),
                "container_name": container_name,
                "error": str(e),
            },
        )
