"""Top-level message dispatcher for realtime WebSocket traffic.

Each individual handler now lives in its own resource-focused submodule
(``analyze``, ``watch``, ``status``). This module exposes the dispatcher
``handle_realtime_message`` plus convenient re-exports so external callers
keep a single import surface.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from routers.realtime._manager import connection_manager, logger
from routers.realtime.analyze import (
    _build_analysis_response,
    _build_parse_result_data,
    handle_analyze_file,
)
from routers.realtime.status import handle_get_status, handle_pong
from routers.realtime.watch import handle_unwatch_file, handle_watch_file
from routers.realtime_multi_file import handle_multi_file_analysis
from utils.parameter_validation import validate_project_root

__all__ = [
    "_build_analysis_response",
    "_build_parse_result_data",
    "handle_analyze_file",
    "handle_get_status",
    "handle_multi_file_analysis",
    "handle_pong",
    "handle_realtime_message",
    "handle_unwatch_file",
    "handle_watch_file",
]


async def handle_realtime_message(
    connection_id: str,
    message: dict[str, Any],
    workspace_path: str | None = None,
    container_name: str | None = None,
    tenant_id: str | None = None,
) -> None:
    """Handle incoming WebSocket messages with container scoping."""
    try:
        validated_workspace = validate_project_root(workspace_path, allow_none=True)
    except Exception as e:
        logger.error(
            "Invalid workspace_path for connection %s (container: %s): %s",
            connection_id,
            container_name,
            e,
            extra={"container_name": container_name, "connection_id": connection_id},
        )
        validated_workspace = None
    message_type = message.get("type")
    if message_type == "analyze_file":
        await handle_analyze_file(
            connection_id, message, validated_workspace, container_name, tenant_id
        )
    elif message_type == "analyze_files":  # multi-file analysis
        await handle_multi_file_analysis(
            connection_id,
            message,
            connection_manager.send_message,
            container_name,
            tenant_id=tenant_id,
        )
    elif message_type == "watch_file":
        await handle_watch_file(connection_id, message, validated_workspace, container_name)
    elif message_type == "unwatch_file":
        await handle_unwatch_file(connection_id, message, validated_workspace, container_name)
    elif message_type == "get_status":
        await handle_get_status(connection_id, message, container_name)
    elif message_type == "pong":
        await handle_pong(connection_id, message)
    else:
        await connection_manager.send_message(
            connection_id,
            {
                "type": "error",
                "timestamp": datetime.now(UTC).isoformat(),
                "error": f"Unknown message type: {message_type}",
                "container_name": container_name,
            },
        )
