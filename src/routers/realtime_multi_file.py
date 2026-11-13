"""
Multi-File Real-time Analysis Handler

Handles WebSocket-based multi-file analysis with parallel processing,
progress tracking, and container-scoped validation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid7

from services.dependencies import get_multi_language_factory
from services.languages.base import LanguageProcessor
from services.performance_monitor import get_performance_monitor
from utils.container_validation import validate_and_get_container_for_tenant
from utils.path_resolution import resolve_container_path, validate_path_in_container

logger = logging.getLogger(__name__)


class MultiFileAnalysisTracker:
    """Tracks progress and state of multi-file analysis operations."""

    def __init__(self, batch_id: str, total_files: int):
        """Initialize tracker for a batch analysis.

        Args:
            batch_id: Unique identifier for this batch
            total_files: Total number of files to analyze
        """
        self.batch_id = batch_id
        self.total_files = total_files
        self.completed_files = 0
        self.successful_files = 0
        self.failed_files = 0
        self.file_status: dict[str, dict[str, Any]] = {}
        self.file_results: list[dict[str, Any]] = []
        self.start_time = time.time()
        self.lock = asyncio.Lock()

    async def update_file_status(
        self,
        file_path: str,
        status: str,
        progress: float,
        message: str | None = None,
    ) -> None:
        """Update status for a single file.

        Args:
            file_path: Path to the file
            status: Current status (pending, analyzing, completed, failed)
            progress: Progress value (0.0 to 1.0)
            message: Optional status message
        """
        async with self.lock:
            self.file_status[file_path] = {
                "status": status,
                "progress": progress,
                "message": message,
            }

            if status in ["completed", "failed"]:
                self.completed_files += 1
                if status == "completed":
                    self.successful_files += 1
                else:
                    self.failed_files += 1

    async def add_file_result(self, result: dict[str, Any]) -> None:
        """Add a completed file result.

        Args:
            result: Analysis result for a file
        """
        async with self.lock:
            self.file_results.append(result)

    def get_overall_progress(self) -> float:
        """Calculate overall progress percentage.

        Returns:
            Progress value between 0.0 and 1.0
        """
        if self.total_files == 0:
            return 1.0
        return self.completed_files / self.total_files

    def get_progress_summary(self) -> dict[str, Any]:
        """Get complete progress summary.

        Returns:
            Dictionary with progress statistics
        """
        file_progress = [
            {
                "file_path": fp,
                "status": status["status"],
                "progress": status["progress"],
                "message": status.get("message"),
            }
            for fp, status in self.file_status.items()
        ]

        return {
            "batch_id": self.batch_id,
            "total_files": self.total_files,
            "completed_files": self.completed_files,
            "successful_files": self.successful_files,
            "failed_files": self.failed_files,
            "overall_progress": self.get_overall_progress(),
            "file_progress": file_progress,
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def get_elapsed_time(self) -> float:
        """Get elapsed time since batch started.

        Returns:
            Elapsed time in seconds
        """
        return time.time() - self.start_time


async def handle_multi_file_analysis(
    connection_id: str,
    message: dict[str, Any],
    send_message_callback,
    container_name: str | None = None,
    tenant_id: str | None = None,
) -> None:
    """Handle multi-file analysis request with parallel processing.

    Args:
        connection_id: WebSocket connection identifier
        message: Request message containing file_paths and options
        send_message_callback: Async callback to send messages to client
        container_name: Container name for scoped operations
    """
    try:
        # Extract request parameters
        file_paths = message.get("file_paths", [])
        analysis_type = message.get("analysis_type", "complexity")
        max_concurrent = message.get("max_concurrent", 3)
        options = message.get("options", {})

        # Validate request
        if not file_paths:
            await send_message_callback(
                connection_id,
                {
                    "type": "multi_file_error",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "error": "No file paths provided",
                    "container_name": container_name,
                },
            )
            return

        # Validate container if provided (tenant-aware)
        if container_name:
            container_info = await validate_and_get_container_for_tenant(container_name, tenant_id)
            if not container_info:
                await send_message_callback(
                    connection_id,
                    {
                        "type": "multi_file_error",
                        "timestamp": datetime.now(UTC).isoformat(),
                        "error": f"Container '{container_name}' not found or not running",
                        "container_name": container_name,
                    },
                )
                return

        # Create batch tracker
        batch_id = f"batch-{uuid7().hex[:8]}"
        tracker = MultiFileAnalysisTracker(batch_id, len(file_paths))

        logger.info(
            "Starting multi-file analysis: batch_id=%s, files=%d, container=%s",
            batch_id,
            len(file_paths),
            container_name,
        )

        # Send initial progress
        await send_message_callback(
            connection_id,
            {
                "type": "multi_file_progress",
                "data": tracker.get_progress_summary(),
                "container_name": container_name,
            },
        )

        # Initialize file status for all files
        for file_path in file_paths:
            await tracker.update_file_status(file_path, "pending", 0.0, "Queued for analysis")

        # Process files in parallel with concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_file(file_path: str) -> dict[str, Any]:
            """Process a single file with progress tracking."""
            async with semaphore:
                try:
                    # Update status to analyzing
                    await tracker.update_file_status(
                        file_path, "analyzing", 0.2, "Starting analysis"
                    )

                    # Send progress update
                    await send_message_callback(
                        connection_id,
                        {
                            "type": "multi_file_progress",
                            "data": tracker.get_progress_summary(),
                            "container_name": container_name,
                        },
                    )

                    # Analyze the file
                    result = await analyze_single_file_multi(
                        file_path,
                        analysis_type,
                        container_name,
                        options,
                        tracker,
                        send_message_callback,
                        connection_id,
                    )

                    # Update final status
                    if result["success"]:
                        await tracker.update_file_status(
                            file_path, "completed", 1.0, "Analysis complete"
                        )
                    else:
                        await tracker.update_file_status(
                            file_path,
                            "failed",
                            1.0,
                            result.get("error", "Unknown error"),
                        )

                    await tracker.add_file_result(result)

                    # Send progress update
                    await send_message_callback(
                        connection_id,
                        {
                            "type": "multi_file_progress",
                            "data": tracker.get_progress_summary(),
                            "container_name": container_name,
                        },
                    )

                    return result

                except Exception as e:
                    logger.error(
                        "Error analyzing file %s in batch %s: %s",
                        file_path,
                        batch_id,
                        e,
                        exc_info=True,
                    )
                    error_result = {
                        "file_path": file_path,
                        "success": False,
                        "error": str(e),
                        "execution_time": 0,
                    }
                    await tracker.update_file_status(file_path, "failed", 1.0, str(e))
                    await tracker.add_file_result(error_result)
                    return error_result

        # Execute all file processing tasks
        tasks = [process_file(fp) for fp in file_paths]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Calculate aggregated results
        aggregated_results = None
        if options.get("aggregate_results", True) and tracker.successful_files > 0:
            aggregated_results = calculate_aggregated_results(tracker.file_results)

        # Calculate performance metrics
        elapsed_time = tracker.get_elapsed_time()
        performance_metrics = {
            "total_time": elapsed_time,
            "average_time_per_file": (elapsed_time / len(file_paths) if file_paths else 0),
            "fastest_file_time": min(
                (r.get("execution_time", 0) for r in tracker.file_results if r.get("success")),
                default=0,
            ),
            "slowest_file_time": max(
                (r.get("execution_time", 0) for r in tracker.file_results if r.get("success")),
                default=0,
            ),
        }

        # Send final results
        final_result = {
            "type": "multi_file_result",
            "batch_id": batch_id,
            "file_results": tracker.file_results,
            "aggregated_results": aggregated_results,
            "performance_metrics": performance_metrics,
            "timestamp": datetime.now(UTC).isoformat(),
            "container_name": container_name,
        }

        await send_message_callback(connection_id, final_result)

        logger.info(
            "Multi-file analysis completed: batch_id=%s, successful=%d, failed=%d, time=%.2fs",
            batch_id,
            tracker.successful_files,
            tracker.failed_files,
            elapsed_time,
        )

    except Exception as e:
        logger.error(
            "Error in multi-file analysis handler: %s",
            e,
            exc_info=True,
        )
        await send_message_callback(
            connection_id,
            {
                "type": "multi_file_error",
                "timestamp": datetime.now(UTC).isoformat(),
                "error": f"Multi-file analysis failed: {str(e)}",
                "container_name": container_name,
            },
        )


async def analyze_single_file_multi(
    file_path: str,
    analysis_type: str,
    container_name: str | None,
    options: dict[str, Any],
    tracker: MultiFileAnalysisTracker,
    send_message_callback,
    connection_id: str,
) -> dict[str, Any]:
    """Analyze a single file with progress tracking.

    Args:
        file_path: Path to the file to analyze
        analysis_type: Type of analysis to perform
        container_name: Container name for scoped operations
        options: Analysis options
        tracker: Progress tracker instance
        send_message_callback: Callback to send progress updates
        connection_id: WebSocket connection ID

    Returns:
        Dictionary with analysis result
    """
    start_time = time.time()

    try:
        # Resolve file path within container context
        resolved_path = file_path
        if container_name:
            try:
                resolved_path = resolve_container_path(container_name, file_path)
                validate_path_in_container(container_name, resolved_path)
                logger.debug(
                    "Resolved path for container %s: %s -> %s",
                    container_name,
                    file_path,
                    resolved_path,
                )
            except Exception as e:
                logger.warning(
                    "Path resolution failed for container %s, path %s: %s",
                    container_name,
                    file_path,
                    e,
                )
                return {
                    "file_path": file_path,
                    "success": False,
                    "error": f"Invalid file path: {str(e)}",
                    "execution_time": 0,
                }

        file_path_obj = Path(resolved_path)

        # Read file content (in real scenario, read from container)
        if not file_path_obj.exists():
            return {
                "file_path": file_path,
                "success": False,
                "error": "File not found",
                "execution_time": time.time() - start_time,
            }

        # Update progress
        await tracker.update_file_status(file_path, "analyzing", 0.5, "Reading file")

        content = file_path_obj.read_text()

        # Get processor for the file
        factory = get_multi_language_factory()
        processor = await factory.get_processor_for_file(file_path_obj)

        if not processor:
            return {
                "file_path": file_path,
                "success": False,
                "error": f"No processor available for file: {file_path}",
                "execution_time": time.time() - start_time,
            }

        # Update progress
        await tracker.update_file_status(file_path, "analyzing", 0.7, "Parsing code")

        # Perform analysis
        if hasattr(processor, "parse_file_with_metrics"):
            language_processor = cast(LanguageProcessor, processor)
            parse_result = await language_processor.parse_file_with_metrics(file_path_obj, content)
        else:
            return {
                "file_path": file_path,
                "success": False,
                "error": "Processor does not support metrics parsing",
                "execution_time": time.time() - start_time,
            }

        execution_time = time.time() - start_time

        # Build result
        result = {
            "file_path": file_path,
            "success": parse_result.success,
            "language": parse_result.language,
            "entities_count": len(parse_result.entities),
            "relationships_count": len(parse_result.relationships),
            "complexity_score": parse_result.complexity_score,
            "lines_of_code": parse_result.lines_of_code,
            "execution_time": execution_time,
            "errors": parse_result.errors if parse_result.errors else [],
        }

        # Track performance
        await get_performance_monitor().track_language_operation(
            parse_result.language, "multi_file_analysis", execution_time
        )

        return result

    except Exception as e:
        logger.error("Error analyzing file %s: %s", file_path, e, exc_info=True)
        return {
            "file_path": file_path,
            "success": False,
            "error": str(e),
            "execution_time": time.time() - start_time,
        }


def calculate_aggregated_results(file_results: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate aggregated metrics across all successful file results.

    Args:
        file_results: List of individual file analysis results

    Returns:
        Dictionary with aggregated metrics
    """
    successful_results = [r for r in file_results if r.get("success")]

    if not successful_results:
        return {}

    total_entities = sum(r.get("entities_count", 0) for r in successful_results)
    total_relationships = sum(r.get("relationships_count", 0) for r in successful_results)
    total_complexity = sum(r.get("complexity_score", 0) for r in successful_results)
    total_loc = sum(r.get("lines_of_code", 0) for r in successful_results)

    languages = set(r.get("language") for r in successful_results if r.get("language"))

    avg_complexity = total_complexity / len(successful_results) if successful_results else 0
    avg_loc = total_loc / len(successful_results) if successful_results else 0

    return {
        "total_entities": total_entities,
        "total_relationships": total_relationships,
        "total_complexity": total_complexity,
        "total_lines_of_code": total_loc,
        "average_complexity": round(avg_complexity, 2),
        "average_lines_of_code": round(avg_loc, 2),
        "languages_analyzed": sorted(list(languages)),
        "files_analyzed": len(successful_results),
    }
