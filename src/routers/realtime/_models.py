"""Pydantic models shared across the realtime router package."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from models.common import BaseResponse, ResponseStatus


class RealtimeAnalysisRequest(BaseModel):
    """Real-time analysis request model."""

    file_path: str = Field(..., description="Path to the file being analyzed")
    content: str = Field(..., description="Current file content")
    change_type: str = Field("update", description="Type of change (update, create, delete)")
    cursor_position: dict[str, int] | None = Field(
        None, description="Cursor position (line, column)"
    )
    selection_range: dict[str, Any] | None = Field(None, description="Selected text range")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "file_path": "/workspace/src/main.rs",
                "content": 'fn main() {\n    println!("Hello, world!");\n}',
                "change_type": "update",
                "cursor_position": {"line": 2, "column": 0},
                "selection_range": None,
            }
        }
    )


class RealtimeAnalysisResponse(BaseModel):
    """Real-time analysis response model."""

    type: str = Field(..., description="Message type")
    timestamp: str = Field(..., description="Response timestamp")
    file_path: str = Field(..., description="File path analyzed")
    success: bool = Field(..., description="Analysis success status")
    parse_result: dict[str, Any] | None = Field(None, description="Parse result")
    metrics: dict[str, Any] | None = Field(None, description="Performance metrics")
    diagnostics: list[dict[str, Any]] | None = Field(None, description="Analysis diagnostics")
    suggestions: list[dict[str, Any]] | None = Field(None, description="Code suggestions")
    error: str | None = Field(None, description="Error message if analysis failed")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "analysis_result",
                "timestamp": "2025-01-06T10:30:00Z",
                "file_path": "/workspace/src/main.rs",
                "success": True,
                "parse_result": {
                    "language": "rust",
                    "entities": [{"name": "main", "type": "function"}],
                    "complexity_score": 2.5,
                },
                "metrics": {"parse_time_ms": 45, "cache_hit": False},
            }
        }
    )


class ConnectionInfo(BaseModel):
    """Information about a single WebSocket connection."""

    connection_id: str = Field(..., description="Unique connection identifier")
    watched_files: list[str] = Field(
        default_factory=list[Any],
        description="List of files being watched by this connection",
    )
    watched_files_count: int = Field(..., description="Number of files being watched")
    status: str = Field(..., description="Connection status (active/inactive)")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection_id": "dev-session-123",
                "watched_files": ["/workspace/src/main.rs", "/workspace/src/lib.rs"],
                "watched_files_count": 2,
                "status": "active",
            }
        }
    )


class CacheStats(BaseModel):
    """Cache statistics for real-time analysis."""

    hit_rate: float = Field(..., description="Cache hit rate percentage (0-100)")
    total_requests: int = Field(..., description="Total number of cache requests")
    cache_hits: int = Field(..., description="Number of successful cache hits")
    cache_misses: int = Field(..., description="Number of cache misses")
    cache_size: int = Field(..., description="Current cache size in bytes")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "hit_rate": 75.5,
                "total_requests": 1000,
                "cache_hits": 755,
                "cache_misses": 245,
                "cache_size": 52428800,
            }
        }
    )


class ActiveConnectionsResponse(BaseResponse):
    """Response model for active connections endpoint."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    active_connections: int = Field(..., description="Total number of active WebSocket connections")
    total_watched_files: int = Field(
        ..., description="Total number of files being watched across all connections"
    )
    connections: list[ConnectionInfo] = Field(..., description="List of active connection details")
    cache_stats: dict[str, Any] | None = Field(None, description="Analysis cache statistics")
    error: str | None = Field(None, description="Error message if data collection failed")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
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
                    },
                    {
                        "connection_id": "dev-session-456",
                        "watched_files": ["/workspace/src/utils.py"],
                        "watched_files_count": 1,
                        "status": "active",
                    },
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
    )


class WebSocketMessage(BaseModel):
    """Base model for WebSocket messages."""

    type: str = Field(..., description="Message type identifier")
    timestamp: str = Field(..., description="ISO 8601 timestamp of when the message was sent")
    container_name: str | None = Field(
        None, description="Container name for container-scoped operations"
    )


class WebSocketErrorMessage(WebSocketMessage):
    """Error message sent via WebSocket."""

    type: str = Field(default="error", description="Message type (always 'error')")
    error: str = Field(..., description="Error description")
    file_path: str | None = Field(None, description="File path related to the error, if applicable")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "error",
                "timestamp": "2025-01-06T10:30:00Z",
                "error": "Failed to parse file: syntax error at line 5",
                "file_path": "/workspace/src/main.rs",
            }
        }
    )


class WebSocketStatusMessage(WebSocketMessage):
    """Status message sent via WebSocket."""

    type: str = Field(default="status_update", description="Message type (always 'status_update')")
    connection_id: str = Field(..., description="Connection identifier")
    watched_files: list[str] = Field(..., description="List of currently watched files")
    active_analyses: int = Field(..., description="Number of active analysis operations")
    cache_stats: dict[str, Any] | None = Field(None, description="Cache statistics")
    total_connections: int | None = Field(None, description="Total number of active connections")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "status_update",
                "timestamp": "2025-01-06T10:30:00Z",
                "connection_id": "dev-session-123",
                "watched_files": ["/workspace/src/main.rs"],
                "active_analyses": 1,
                "cache_stats": {"hit_rate": 75.5},
                "total_connections": 3,
            }
        }
    )


class WebSocketConnectionMessage(WebSocketMessage):
    """Connection established message sent via WebSocket."""

    type: str = Field(
        default="connection_established",
        description="Message type (always 'connection_established')",
    )
    connection_id: str = Field(..., description="Unique connection identifier")
    message: str = Field(..., description="Welcome message")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "connection_established",
                "timestamp": "2025-01-06T10:30:00Z",
                "connection_id": "dev-session-123",
                "message": "Real-time analysis WebSocket connected",
            }
        }
    )


class WebSocketWatchMessage(WebSocketMessage):
    """File watch status message sent via WebSocket."""

    type: str = Field(..., description="Message type (watch_started or watch_stopped)")
    file_path: str = Field(..., description="File path being watched/unwatched")
    message: str = Field(..., description="Status message")
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "watch_started",
                "timestamp": "2025-01-06T10:30:00Z",
                "file_path": "/workspace/src/main.rs",
                "message": "Started watching /workspace/src/main.rs",
            }
        }
    )
