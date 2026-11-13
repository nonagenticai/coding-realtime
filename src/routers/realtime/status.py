"""``get_status`` and ``pong`` realtime handlers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from routers.realtime._manager import connection_manager, logger
from services.analysis_cache import get_analysis_cache
from services.websocket_health_monitor import get_health_monitor


async def handle_get_status(
    connection_id: str, _: dict[str, Any], container_name: str | None = None
) -> None:
    """Handle status request."""
    try:
        watched_files = list(connection_manager.connection_files.get(connection_id, set()))
        cache = get_analysis_cache()
        cache_stats = await cache.get_stats()
        await connection_manager.send_message(
            connection_id,
            {
                "type": "status_update",
                "timestamp": datetime.now(UTC).isoformat(),
                "connection_id": connection_manager.public_connection_id(connection_id),
                "container_name": container_name,
                "watched_files": watched_files,
                "active_analyses": len(watched_files),
                "cache_stats": cache_stats,
                # The CALLER's connections only — this used to count every
                # tenant's sockets on the pod.
                "total_connections": len(
                    connection_manager.connections_for_tenant(
                        connection_manager.connection_tenants.get(connection_id) or ""
                    )
                ),
            },
        )
    except (ValueError, OSError, AttributeError) as e:
        logger.error("Error in get_status: %s", e)
        await connection_manager.send_message(
            connection_id,
            {
                "type": "error",
                "timestamp": datetime.now(UTC).isoformat(),
                "container_name": container_name,
                "error": str(e),
            },
        )


async def handle_pong(connection_id: str, message: dict[str, Any]) -> None:
    """Handle pong response from client."""
    try:
        ping_id = message.get("ping_id")
        if not ping_id:
            logger.warning("Received pong without ping_id from connection %s", connection_id)
            return

        # The monitor tracks this socket under the REGISTRY's id, not ours.
        monitor_id = connection_manager.monitor_connection_id(connection_id)
        success = False
        if monitor_id:
            health_monitor = get_health_monitor()
            success = await health_monitor.handle_pong(monitor_id, ping_id)

        if success:
            logger.debug("Pong processed for connection %s (ping_id: %s)", connection_id, ping_id)
        else:
            logger.warning("Failed to process pong for connection %s", connection_id)

        await connection_manager.send_message(
            connection_id,
            {
                "type": "pong_acknowledged",
                "timestamp": datetime.now(UTC).isoformat(),
                "ping_id": ping_id,
            },
        )

    except Exception as e:
        logger.error("Error handling pong for connection %s: %s", connection_id, e)
