from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException

from utils import path_resolution


@pytest.fixture(autouse=True)
def _clear_container_cache():
    path_resolution._container_path_cache.clear()
    yield
    path_resolution._container_path_cache.clear()


# ---------------------------------------------------------------------------
# Split from: test_get_container_codebase_path_variants
# ---------------------------------------------------------------------------


async def test_get_container_codebase_path_from_dict_response(monkeypatch):
    async def _dict_result(*_args, **_kwargs):
        return {"codebase_path": "/workspace/project"}

    monkeypatch.setattr(path_resolution, "validate_and_get_container_for_tenant", _dict_result)

    result = await path_resolution._get_container_codebase_path("c1", "t1")
    assert result == "/workspace/project"


async def test_get_container_codebase_path_from_object_response(monkeypatch):
    class _Obj:
        codebase_path = "/workspace/object"

    async def _obj_result(*_args, **_kwargs):
        return _Obj()

    monkeypatch.setattr(path_resolution, "validate_and_get_container_for_tenant", _obj_result)

    result = await path_resolution._get_container_codebase_path("c2")
    assert result == "/workspace/object"


async def test_get_container_codebase_path_returns_none_when_missing(monkeypatch):
    async def _missing(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(path_resolution, "validate_and_get_container_for_tenant", _missing)

    with pytest.raises(HTTPException, match="has no codebase path configured"):
        await path_resolution._get_container_codebase_path("c3")


# ---------------------------------------------------------------------------
# Split from: test_resolve_container_path_async_and_validate
# ---------------------------------------------------------------------------


async def test_resolve_container_path_joins_codebase_and_file(monkeypatch):
    async def _container_root(*_args, **_kwargs):
        return "/workspace/project"

    monkeypatch.setattr(path_resolution, "_get_container_codebase_path", _container_root)

    resolved = await path_resolution.resolve_container_path_async("c1", "src/main.py", "tenant")
    assert resolved == "/workspace/project/src/main.py"


async def test_validate_path_in_container_accepts_subpath(monkeypatch):
    async def _container_root(*_args, **_kwargs):
        return "/workspace/project"

    monkeypatch.setattr(path_resolution, "_get_container_codebase_path", _container_root)

    # resolve_container_path_async caches the root, which validate_path_in_container needs
    resolved = await path_resolution.resolve_container_path_async("c1", "src/main.py", "tenant")
    assert path_resolution.validate_path_in_container("c1", resolved) is True


async def test_validate_path_in_container_rejects_escape(monkeypatch):
    async def _container_root(*_args, **_kwargs):
        return "/workspace/project"

    monkeypatch.setattr(path_resolution, "_get_container_codebase_path", _container_root)

    with pytest.raises(HTTPException, match="escapes container boundary"):
        await path_resolution.resolve_container_path_async("c1", "../etc/passwd")


# ---------------------------------------------------------------------------
# Split from: test_discover_files_in_container_filters_results
# ---------------------------------------------------------------------------


async def test_discover_files_filters_pycache_and_hidden(tmp_path: Path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "src").mkdir()
    (project_root / "src" / "main.py").write_text("print('hi')")
    (project_root / "__pycache__").mkdir()
    (project_root / "__pycache__" / "ignore.py").write_text("x")

    async def _container_root(*_args, **_kwargs):
        return str(project_root)

    monkeypatch.setattr(path_resolution, "_get_container_codebase_path", _container_root)

    files = await path_resolution.discover_files_in_container("c1")
    assert files == ["src/main.py"]


async def test_discover_files_returns_empty_when_root_missing(tmp_path: Path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()

    async def _missing_root(*_args, **_kwargs):
        return str(project_root / "missing")

    monkeypatch.setattr(path_resolution, "_get_container_codebase_path", _missing_root)

    with pytest.raises(OSError, match="does not exist"):
        await path_resolution.discover_files_in_container(
            "c1",
            tenant_id="t1",
            pattern="**/*.py",
            exclude=[],
        )
