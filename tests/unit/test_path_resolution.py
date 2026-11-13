"""Tests for path_resolution utilities."""

import pytest

from utils.path_resolution import (
    cache_container_path,
    evict_container_path,
    resolve_container_path,
    validate_path_in_container,
    validate_path_in_container_sync,
)


def test_validate_path_in_container_sync_valid():
    assert validate_path_in_container_sync("/app", "/app/src/main.py") is True


def test_validate_path_in_container_sync_traversal():
    assert validate_path_in_container_sync("/app", "/app/../etc/passwd") is False


def test_validate_path_in_container_sync_absolute_outside():
    assert validate_path_in_container_sync("/app", "/etc/passwd") is False


def test_validate_path_in_container_sync_exact_root():
    assert validate_path_in_container_sync("/app", "/app") is True


def test_validate_path_in_container_sync_nested():
    assert validate_path_in_container_sync("/app/project", "/app/project/src/foo.py") is True


def test_cache_and_evict_container_path():
    cache_container_path("test-c", "/tmp/test-container")
    assert validate_path_in_container("test-c", "/tmp/test-container/src/main.py") is True
    evict_container_path("test-c")
    assert validate_path_in_container("test-c", "/tmp/test-container/src/main.py") is False


def test_resolve_container_path_uses_cache():
    cache_container_path("cached-c", "/home/user/project")
    result = resolve_container_path("cached-c", "src/main.py")
    assert "src/main.py" in result
    evict_container_path("cached-c")


def test_resolve_container_path_traversal_blocked():
    from fastapi import HTTPException

    cache_container_path("safe-c", "/home/user/project")
    with pytest.raises(HTTPException) as exc_info:
        resolve_container_path("safe-c", "../../etc/passwd")
    assert exc_info.value.status_code == 400
    evict_container_path("safe-c")
