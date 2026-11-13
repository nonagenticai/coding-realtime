"""GET /websocket-api — structured documentation for the realtime WebSocket API."""

from __future__ import annotations

from typing import Any

from coding_client.auth import require_tenant
from fastapi import Depends
from fastapi.routing import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from models.errors import Error404Response, Error500Response

router = APIRouter()


class WebSocketAPIDocumentation(BaseModel):
    """WebSocket API documentation response model."""

    endpoint: str = Field(..., description="WebSocket endpoint URL")
    description: str = Field(..., description="API description")
    connection_params: dict[str, str] = Field(..., description="Required connection parameters")
    message_types: dict[str, dict[str, Any]] = Field(
        ..., description="Supported message types with examples"
    )
    response_types: dict[str, dict[str, Any]] = Field(
        ..., description="Response message types with examples"
    )
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "endpoint": "/api/realtime/analysis",
                "description": "Real-time code analysis WebSocket API",
                "connection_params": {
                    "connection_id": "Unique connection identifier (required)",
                    "workspace_path": "Workspace root path (optional)",
                },
                "message_types": {
                    "analyze_file": {
                        "description": "Request file analysis",
                        "example": {
                            "type": "analyze_file",
                            "file_path": "/workspace/src/main.rs",
                            "content": "fn main() {}",
                        },
                    }
                },
                "response_types": {
                    "analysis_result": {
                        "description": "Analysis results",
                        "example": {
                            "type": "analysis_result",
                            "success": True,
                            "parse_result": {},
                        },
                    }
                },
            }
        }
    )


@router.get(
    "/websocket-api",
    summary="WebSocket API Documentation",
    operation_id="get_websocket_api_docs",
    description="\n    WebSocket API Reference - Get structured documentation for real-time analysis WebSocket API\n\n    ## Purpose\n    Provides programmatic access to WebSocket API documentation, including message\n    formats, response types, and connection parameters.\n\n    ## Response Information\n    - WebSocket endpoint details\n    - Connection parameter requirements\n    - All supported message types with examples\n    - All response types with examples\n    - Error message formats\n\n    ## Use Cases\n    - **Client Development**: Build WebSocket clients with proper message formats\n    - **API Integration**: Understand message structure for integration\n    - **Documentation**: Generate client-side documentation\n    - **Testing**: Create test messages based on examples\n    ",
    response_model=WebSocketAPIDocumentation,
    responses={
        200: {
            "model": WebSocketAPIDocumentation,
            "description": "WebSocket API documentation retrieved successfully",
        },
        404: {
            "model": Error404Response,
            "description": "WebSocket API documentation not found",
        },
        500: {"model": Error500Response, "description": "Internal server error"},
    },
)
async def get_websocket_api_documentation(
    tenant_id: str = Depends(require_tenant),
) -> WebSocketAPIDocumentation:
    """Get comprehensive WebSocket API documentation."""
    return WebSocketAPIDocumentation(
        endpoint="/api/realtime/analysis",
        description="Real-time code analysis WebSocket API for live development feedback",
        connection_params={
            "connection_id": "Unique connection identifier (required)",
            "workspace_path": "Workspace root path for relative file resolution (optional)",
        },
        message_types={
            "analyze_file": {
                "description": "Request analysis of a file with current content",
                "required_fields": ["type", "file_path", "content"],
                "optional_fields": [
                    "change_type",
                    "cursor_position",
                    "selection_range",
                ],
                "example": {
                    "type": "analyze_file",
                    "file_path": "/workspace/src/main.rs",
                    "content": 'fn main() {\n    println!("Hello, world!");\n}',
                    "change_type": "update",
                    "cursor_position": {"line": 2, "column": 0},
                },
            },
            "watch_file": {
                "description": "Start watching a file for changes",
                "required_fields": ["type", "file_path"],
                "example": {
                    "type": "watch_file",
                    "file_path": "/workspace/src/main.rs",
                },
            },
            "unwatch_file": {
                "description": "Stop watching a file",
                "required_fields": ["type", "file_path"],
                "example": {
                    "type": "unwatch_file",
                    "file_path": "/workspace/src/main.rs",
                },
            },
            "get_status": {
                "description": "Get current connection status and watched files",
                "required_fields": ["type"],
                "example": {"type": "get_status"},
            },
        },
        response_types={
            "connection_established": {
                "description": "WebSocket connection successfully established",
                "fields": ["type", "timestamp", "connection_id", "message"],
                "example": {
                    "type": "connection_established",
                    "timestamp": "2025-01-06T10:30:00Z",
                    "connection_id": "dev-session-123",
                    "message": "Real-time analysis WebSocket connected",
                },
            },
            "analysis_result": {
                "description": "File analysis completed successfully",
                "fields": [
                    "type",
                    "timestamp",
                    "file_path",
                    "success",
                    "parse_result",
                    "metrics",
                    "diagnostics",
                ],
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
                },
            },
            "analysis_error": {
                "description": "Analysis failed with error",
                "fields": ["type", "timestamp", "file_path", "success", "error"],
                "example": {
                    "type": "analysis_error",
                    "timestamp": "2025-01-06T10:30:00Z",
                    "file_path": "/workspace/src/main.rs",
                    "success": False,
                    "error": "Syntax error at line 5",
                },
            },
            "watch_started": {
                "description": "File watch initiated",
                "fields": ["type", "timestamp", "file_path", "message"],
                "example": {
                    "type": "watch_started",
                    "timestamp": "2025-01-06T10:30:00Z",
                    "file_path": "/workspace/src/main.rs",
                    "message": "Started watching /workspace/src/main.rs",
                },
            },
            "watch_stopped": {
                "description": "File watch terminated",
                "fields": ["type", "timestamp", "file_path", "message"],
                "example": {
                    "type": "watch_stopped",
                    "timestamp": "2025-01-06T10:30:00Z",
                    "file_path": "/workspace/src/main.rs",
                    "message": "Stopped watching /workspace/src/main.rs",
                },
            },
            "status_update": {
                "description": "Connection status information",
                "fields": [
                    "type",
                    "timestamp",
                    "connection_id",
                    "watched_files",
                    "active_analyses",
                    "cache_stats",
                ],
                "example": {
                    "type": "status_update",
                    "timestamp": "2025-01-06T10:30:00Z",
                    "connection_id": "dev-session-123",
                    "watched_files": ["/workspace/src/main.rs"],
                    "active_analyses": 1,
                    "cache_stats": {"hit_rate": 75.5},
                },
            },
            "error": {
                "description": "General error message",
                "fields": ["type", "timestamp", "error"],
                "example": {
                    "type": "error",
                    "timestamp": "2025-01-06T10:30:00Z",
                    "error": "Unknown message type: invalid_type",
                },
            },
        },
    )
