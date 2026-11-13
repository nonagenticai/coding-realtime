"""Convenience re-exports of shared models + ConnectionManager singleton.

Submodules import from here so they don't depend on the package ``__init__``
(which would create a circular import during ``routers.realtime`` startup).
"""

from __future__ import annotations

from routers.realtime._manager import ConnectionManager, connection_manager, logger
from routers.realtime._models import (
    ActiveConnectionsResponse,
    CacheStats,
    ConnectionInfo,
    RealtimeAnalysisRequest,
    RealtimeAnalysisResponse,
    WebSocketConnectionMessage,
    WebSocketErrorMessage,
    WebSocketMessage,
    WebSocketStatusMessage,
    WebSocketWatchMessage,
)

__all__ = [
    "ActiveConnectionsResponse",
    "CacheStats",
    "ConnectionInfo",
    "ConnectionManager",
    "RealtimeAnalysisRequest",
    "RealtimeAnalysisResponse",
    "WebSocketConnectionMessage",
    "WebSocketErrorMessage",
    "WebSocketMessage",
    "WebSocketStatusMessage",
    "WebSocketWatchMessage",
    "connection_manager",
    "logger",
]
