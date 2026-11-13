"""``analyze_file`` handler + parse-result/response builders."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from routers.realtime._manager import connection_manager, logger
from routers.realtime._path_resolution import PathResolutionError, resolve_file_path_sync
from services.analysis_cache import get_analysis_cache
from services.dependencies import get_multi_language_factory
from services.languages.base import LanguageProcessor
from services.performance_monitor import get_performance_monitor
from services.websocket_metrics import get_websocket_metrics_collector


def _build_parse_result_data(parse_result) -> dict[str, Any]:
    """Build parse result data structure."""
    return {
        "language": parse_result.language,
        "file_path": parse_result.file_path,
        "lines_of_code": parse_result.lines_of_code,
        "entities": [
            {
                "id": entity.id,
                "name": entity.name,
                "type": entity.type,
                "line_number": entity.line_number,
                "language": entity.language,
            }
            for entity in parse_result.entities
        ],
        "relationships": [
            {"source_id": rel.source_id, "target_id": rel.target_id, "type": rel.type}
            for rel in parse_result.relationships
        ],
        "complexity_score": parse_result.complexity_score,
        "success": parse_result.success,
        "errors": parse_result.errors,
    }


async def _build_analysis_response(
    file_path: str,
    parse_result,
    analysis_time: float,
    cache_stats: dict[str, Any],
    container_name: str | None = None,
) -> dict[str, Any]:
    """Build analysis response dictionary."""
    response = {
        "type": "analysis_result",
        "timestamp": datetime.now(UTC).isoformat(),
        "file_path": file_path,
        "success": parse_result.success,
        "container_name": container_name,
        "parse_result": _build_parse_result_data(parse_result),
        "metrics": {
            "analysis_time_ms": round(analysis_time, 2),
            "cache_hit_rate": cache_stats.get("hit_rate", 0),
            "total_entities": len(parse_result.entities),
            "total_relationships": len(parse_result.relationships),
        },
    }
    if parse_result.errors:
        response["diagnostics"] = [
            {"type": "error", "message": error, "line": 0, "column": 0}
            for error in parse_result.errors
        ]
    return response


async def handle_analyze_file(
    connection_id: str,
    message: dict[str, Any],
    workspace_path: Path | None = None,
    container_name: str | None = None,
    tenant_id: str | None = None,
):
    """Handle file analysis request with container-scoped path resolution.

    ``tenant_id`` scopes the unchanged-content cache. Without it the cache was
    keyed by file path alone on a module-level singleton, so two tenants
    analyzing e.g. ``src/main.py`` starved each other.
    """
    try:
        file_path = message.get("file_path")
        content = message.get("content")
        if not file_path or content is None:
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "analysis_error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": "Missing file_path or content",
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
                    "type": "analysis_error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "container_name": container_name,
                    "error": f"Invalid file path: {str(e)}",
                },
            )
            return

        file_path_obj = Path(file_path)
        if not connection_manager.has_content_changed(
            file_path, content, tenant_id=tenant_id, container_name=container_name
        ):
            # Unchanged content must still produce a REPLY. Returning nothing
            # made every dedup hit an indistinguishable silent no-op: the client
            # got no result and no error, and a request whose first analysis had
            # failed (so nothing was cached) could never be retried because the
            # hash was already recorded.
            cached = connection_manager.get_last_analysis(
                file_path, tenant_id=tenant_id, container_name=container_name
            )
            if cached is not None:
                logger.debug("Content unchanged for %s, replaying cached analysis", file_path)
                connection_manager.watch_file(connection_id, file_path)
                await connection_manager.send_message(
                    connection_id,
                    {
                        **cached,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "cached": True,
                    },
                )
                return
            logger.debug("Content unchanged for %s but no cached result — re-analyzing", file_path)
        connection_manager.watch_file(connection_id, file_path)
        start_time = time.time()
        factory = get_multi_language_factory()
        processor = await factory.get_processor_for_file(file_path_obj)
        if not processor:
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "analysis_error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "file_path": file_path,
                    "container_name": container_name,
                    "success": False,
                    "error": f"No processor available for file: {file_path}",
                },
            )
            return
        if hasattr(processor, "parse_file_with_metrics"):
            language_processor = cast(LanguageProcessor, processor)
            parse_result = await language_processor.parse_file_with_metrics(file_path_obj, content)
        else:
            # Same silent-no-op class as the unchanged-content path: the client
            # was told nothing at all. Report it.
            logger.warning("Processor for %s does not support parse_file_with_metrics", file_path)
            await connection_manager.send_message(
                connection_id,
                {
                    "type": "analysis_error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "file_path": file_path,
                    "container_name": container_name,
                    "success": False,
                    "error": f"Processor for {file_path} does not support metrics analysis",
                },
            )
            return
        analysis_time = (time.time() - start_time) * 1000

        if container_name:
            metrics_collector = get_websocket_metrics_collector()
            await metrics_collector.track_analysis_execution(
                container_name, analysis_time, success=parse_result.success
            )

        response = await _build_analysis_response(
            file_path,
            parse_result,
            analysis_time,
            await get_analysis_cache().get_stats(),
            container_name,
        )
        connection_manager.record_analysis(
            file_path, response, tenant_id=tenant_id, container_name=container_name
        )
        await connection_manager.send_message(connection_id, response)
        await get_performance_monitor().track_language_operation(
            parse_result.language, "realtime_analysis", analysis_time / 1000
        )
        logger.debug("Real-time analysis completed for %s in %.2fms", file_path, analysis_time)
    except (ValueError, OSError, AttributeError, TypeError) as e:
        logger.error("Error in analyze_file: %s", e)

        if container_name:
            metrics_collector = get_websocket_metrics_collector()
            await metrics_collector.track_error(container_name, "analysis_error", str(e))

        await connection_manager.send_message(
            connection_id,
            {
                "type": "analysis_error",
                "timestamp": datetime.now(UTC).isoformat(),
                "file_path": message.get("file_path", "unknown"),
                "container_name": container_name,
                "success": False,
                "error": str(e),
            },
        )
