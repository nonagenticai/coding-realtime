"""Realtime router package.

Assembles per-resource submodule routers into a single `router` exposed at the
package level so that ``from routers.realtime import router`` keeps working.

Public re-exports preserve the previous monolithic ``routers/realtime.py``
surface: ConnectionManager, the singleton ``connection_manager``, every
``handle_*`` coroutine, the WebSocket entry-point, response models, and the
HTTP endpoint functions. Tests that previously patched ``routers.realtime.X``
should now patch the submodule where ``X`` is actually resolved (e.g.
``routers.realtime.handlers.connection_manager``).
"""

from __future__ import annotations

from fastapi.routing import APIRouter

# Submodule routers
from routers.realtime import connections as _connections
from routers.realtime import docs as _docs
from routers.realtime import handlers as _handlers  # noqa: F401  (used by tests)
from routers.realtime import metrics as _metrics
from routers.realtime import websocket as _websocket
from routers.realtime._common import (
    ActiveConnectionsResponse,
    CacheStats,
    ConnectionInfo,
    ConnectionManager,
    RealtimeAnalysisRequest,
    RealtimeAnalysisResponse,
    WebSocketConnectionMessage,
    WebSocketErrorMessage,
    WebSocketMessage,
    WebSocketStatusMessage,
    WebSocketWatchMessage,
    connection_manager,
    logger,
)
from routers.realtime._metrics_models import ContainerMetricsResponse, GlobalMetricsResponse
from routers.realtime.connections import get_active_connections
from routers.realtime.docs import WebSocketAPIDocumentation, get_websocket_api_documentation
from routers.realtime.handlers import (
    _build_analysis_response,
    _build_parse_result_data,
    handle_analyze_file,
    handle_get_status,
    handle_pong,
    handle_realtime_message,
    handle_unwatch_file,
    handle_watch_file,
)
from routers.realtime.metrics import (
    get_container_websocket_metrics,
    get_global_websocket_metrics,
)
from routers.realtime.websocket import websocket_realtime_analysis

# Parent router that main.py mounts at /api/realtime.
# Submodule routers carry the route decorators with NO prefix; include_router
# preserves the original URL surface exactly.
router = APIRouter()
router.include_router(_websocket.router)
router.include_router(_connections.router)
router.include_router(_docs.router)
router.include_router(_metrics.router)


__all__ = [
    # router
    "router",
    # models
    "ActiveConnectionsResponse",
    "CacheStats",
    "ConnectionInfo",
    "ConnectionManager",
    "ContainerMetricsResponse",
    "GlobalMetricsResponse",
    "RealtimeAnalysisRequest",
    "RealtimeAnalysisResponse",
    "WebSocketAPIDocumentation",
    "WebSocketConnectionMessage",
    "WebSocketErrorMessage",
    "WebSocketMessage",
    "WebSocketStatusMessage",
    "WebSocketWatchMessage",
    # singletons / helpers
    "connection_manager",
    "logger",
    # handlers
    "_build_analysis_response",
    "_build_parse_result_data",
    "handle_analyze_file",
    "handle_get_status",
    "handle_pong",
    "handle_realtime_message",
    "handle_unwatch_file",
    "handle_watch_file",
    # endpoint functions
    "get_active_connections",
    "get_container_websocket_metrics",
    "get_global_websocket_metrics",
    "get_websocket_api_documentation",
    "websocket_realtime_analysis",
]
