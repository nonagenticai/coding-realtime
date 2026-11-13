"""GET /connections — list active realtime WebSocket connections."""

from __future__ import annotations

from coding_client.auth import require_tenant
from fastapi import Depends
from fastapi.routing import APIRouter

from models.errors import (
    Error400Response,
    Error404Response,
    Error500Response,
    ValidationErrorResponse,
)
from routers.realtime._common import (
    ActiveConnectionsResponse,
    ConnectionInfo,
    connection_manager,
    logger,
)
from services.analysis_cache import get_analysis_cache

router = APIRouter()


@router.get(
    "/connections",
    summary="Active Connections",
    operation_id="list_active_connections",
    description="\n    Real-time Analysis Connections - Monitor active WebSocket connections\n\n    ## Purpose\n    Provides information about currently active real-time analysis WebSocket connections,\n    including connection status, watched files, and performance metrics.\n\n    ## Response Information\n    - Active connection count\n    - Connection details and status\n    - Watched files per connection\n    - Performance metrics\n    - Connection health status\n\n    ## Use Cases\n    - **Monitoring**: Track active development sessions\n    - **Debugging**: Diagnose connection issues\n    - **Analytics**: Understand usage patterns\n    - **Resource Management**: Monitor system load\n    ",
    response_model=ActiveConnectionsResponse,
    responses={
        200: {
            "model": ActiveConnectionsResponse,
            "description": "Successfully retrieved active connections information",
            "content": {
                "application/json": {
                    "example": {
                        "active_connections": 3,
                        "total_watched_files": 15,
                        "connections": [
                            {
                                "connection_id": "dev-session-123",
                                "watched_files": [
                                    "/workspace/src/main.rs",
                                    "/workspace/src/lib.rs",
                                ],
                                "watched_files_count": 2,
                                "status": "active",
                            }
                        ],
                        "cache_stats": {
                            "hit_rate": 75.5,
                            "total_requests": 1000,
                            "cache_hits": 755,
                            "cache_misses": 245,
                        },
                        "timestamp": "2025-01-06T10:30:00Z",
                    }
                }
            },
        },
        400: {"model": Error400Response, "description": "Bad request"},
        404: {"model": Error404Response, "description": "Resource not found"},
        422: {"model": ValidationErrorResponse, "description": "Validation error"},
        500: {"model": Error500Response, "description": "Internal server error"},
    },
)
async def get_active_connections(
    tenant_id: str = Depends(require_tenant),
) -> ActiveConnectionsResponse:
    """Get information about active real-time analysis connections."""
    try:
        # Scoped to the CALLER's tenant: connection ids and watched file paths of
        # other tenants must never appear here, and neither may their counts.
        own_ids = connection_manager.connections_for_tenant(tenant_id)
        own_files: set[str] = set()
        connections_info = []
        for connection_id in own_ids:
            watched_files = list(connection_manager.connection_files.get(connection_id, set()))
            own_files.update(watched_files)
            connection_info = ConnectionInfo(
                # The client's own id, not the (tenant, id) registry key.
                connection_id=connection_manager.public_connection_id(connection_id),
                watched_files=watched_files,
                watched_files_count=len(watched_files),
                status="active",
            )
            connections_info.append(connection_info)
        cache = get_analysis_cache()
        cache_stats = await cache.get_stats()
        return ActiveConnectionsResponse(
            message="Active connections retrieved successfully",
            active_connections=len(own_ids),
            total_watched_files=len(own_files),
            connections=connections_info,
            cache_stats=cache_stats,
            error=None,
        )
    except (ValueError, OSError, AttributeError) as e:
        logger.error("Error getting active connections: %s", e)
        return ActiveConnectionsResponse(
            message=f"Error retrieving active connections: {str(e)}",
            active_connections=0,
            total_watched_files=0,
            connections=[],
            cache_stats=None,
            error=str(e),
        )
