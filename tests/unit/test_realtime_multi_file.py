from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from routers import realtime_multi_file as multi_file


async def test_multi_file_tracker_summary_and_elapsed():
    tracker = multi_file.MultiFileAnalysisTracker("batch-1", 2)
    await tracker.update_file_status("a.py", "completed", 1.0, "done")
    await tracker.update_file_status("b.py", "failed", 1.0, "bad")
    await tracker.add_file_result({"file_path": "a.py", "success": True})

    summary = tracker.get_progress_summary()
    assert summary["completed_files"] == 2
    assert summary["successful_files"] == 1
    assert summary["failed_files"] == 1
    assert tracker.get_overall_progress() == 1.0
    assert tracker.get_elapsed_time() >= 0


async def test_analyze_single_file_multi_success_and_failures(tmp_path: Path, monkeypatch):
    file_path = tmp_path / "sample.py"
    file_path.write_text("print('hello')")
    tracker = multi_file.MultiFileAnalysisTracker("batch", 1)
    send_cb = AsyncMock()
    monitor = AsyncMock()

    parse_result = SimpleNamespace(
        success=True,
        language="python",
        entities=[1, 2],
        relationships=[1],
        complexity_score=3.5,
        lines_of_code=1,
        errors=[],
    )
    processor = AsyncMock()
    processor.parse_file_with_metrics.return_value = parse_result
    factory = AsyncMock()
    factory.get_processor_for_file.return_value = processor

    monkeypatch.setattr(multi_file, "get_multi_language_factory", lambda: factory)
    monkeypatch.setattr(multi_file, "get_performance_monitor", lambda: monitor)

    result = await multi_file.analyze_single_file_multi(
        str(file_path), "complexity", None, {}, tracker, send_cb, "conn-1"
    )
    assert result["success"] is True
    monitor.track_language_operation.assert_awaited_once()

    factory.get_processor_for_file.return_value = None
    missing_processor = await multi_file.analyze_single_file_multi(
        str(file_path), "complexity", None, {}, tracker, send_cb, "conn-1"
    )
    assert "No processor available" in missing_processor["error"]

    class _NoMetrics:
        pass

    factory.get_processor_for_file.return_value = _NoMetrics()
    no_metrics = await multi_file.analyze_single_file_multi(
        str(file_path), "complexity", None, {}, tracker, send_cb, "conn-1"
    )
    assert "does not support metrics parsing" in no_metrics["error"]

    not_found = await multi_file.analyze_single_file_multi(
        str(tmp_path / "missing.py"), "complexity", None, {}, tracker, send_cb, "conn-1"
    )
    assert not_found["error"] == "File not found"

    monkeypatch.setattr(
        multi_file,
        "resolve_container_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("bad path")),
    )
    invalid = await multi_file.analyze_single_file_multi(
        "bad.py", "complexity", "container", {}, tracker, send_cb, "conn-1"
    )
    assert "Invalid file path" in invalid["error"]


async def test_handle_multi_file_analysis_and_aggregation(tmp_path: Path, monkeypatch):
    first = tmp_path / "a.py"
    second = tmp_path / "b.py"
    first.write_text("print('a')")
    second.write_text("print('b')")

    send_cb = AsyncMock()
    factory = AsyncMock()
    parse_result = SimpleNamespace(
        success=True,
        language="python",
        entities=[1],
        relationships=[],
        complexity_score=1.0,
        lines_of_code=1,
        errors=[],
    )
    processor = AsyncMock()
    processor.parse_file_with_metrics.return_value = parse_result
    factory.get_processor_for_file.return_value = processor
    monitor = AsyncMock()

    monkeypatch.setattr(multi_file, "get_multi_language_factory", lambda: factory)
    monkeypatch.setattr(multi_file, "get_performance_monitor", lambda: monitor)
    monkeypatch.setattr(
        multi_file,
        "validate_and_get_container_for_tenant",
        AsyncMock(return_value={"name": "c1"}),
    )

    await multi_file.handle_multi_file_analysis(
        "conn-1",
        {"file_paths": [str(first), str(second)], "max_concurrent": 1},
        send_cb,
        container_name=None,
    )

    sent_payloads = [call.args[1] for call in send_cb.await_args_list]
    assert sent_payloads[0]["type"] == "multi_file_progress"
    assert sent_payloads[-1]["type"] == "multi_file_result"
    assert sent_payloads[-1]["aggregated_results"]["files_analyzed"] == 2

    send_cb.reset_mock()
    await multi_file.handle_multi_file_analysis("conn-1", {"file_paths": []}, send_cb)
    assert send_cb.await_args_list[-1].args[1]["type"] == "multi_file_error"

    send_cb.reset_mock()
    monkeypatch.setattr(
        multi_file,
        "validate_and_get_container_for_tenant",
        AsyncMock(return_value=None),
    )
    await multi_file.handle_multi_file_analysis(
        "conn-1",
        {"file_paths": [str(first)]},
        send_cb,
        container_name="missing",
        tenant_id="tenant-1",
    )
    assert "not found or not running" in send_cb.await_args_list[-1].args[1]["error"]


def test_calculate_aggregated_results_handles_empty_and_successful():
    assert multi_file.calculate_aggregated_results([]) == {}

    aggregated = multi_file.calculate_aggregated_results(
        [
            {
                "success": True,
                "entities_count": 2,
                "relationships_count": 1,
                "complexity_score": 4.0,
                "lines_of_code": 10,
                "language": "python",
            },
            {"success": False},
            {
                "success": True,
                "entities_count": 3,
                "relationships_count": 2,
                "complexity_score": 6.0,
                "lines_of_code": 20,
                "language": "go",
            },
        ]
    )
    assert aggregated["total_entities"] == 5
    assert aggregated["average_complexity"] == 5.0
    assert aggregated["languages_analyzed"] == ["go", "python"]
