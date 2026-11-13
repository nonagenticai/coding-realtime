from __future__ import annotations

from pathlib import Path

import pytest

from services.file_service import FileOperationError, FileService


def test_file_service_read_write_and_discovery(tmp_path: Path):
    service = FileService(base_path=tmp_path)

    written = service.write_text("nested/example.txt", "hello\nworld")

    assert written == tmp_path / "nested/example.txt"
    assert service.read_text("nested/example.txt") == "hello\nworld"
    assert service.read_bytes("nested/example.txt") == b"hello\nworld"
    assert list(service.read_lines("nested/example.txt")) == ["hello", "world"]
    assert service.exists("nested/example.txt") is True
    assert service.is_file("nested/example.txt") is True
    assert service.is_directory("nested") is True
    assert service.validate_path("nested/example.txt", must_exist=True) == written
    assert service.read_file("nested/example.txt") == "hello\nworld"
    assert service.file_exists("nested/example.txt") is True
    assert service.find_files(".", "*.txt") == [str(written)]


def test_file_service_error_paths(tmp_path: Path):
    service = FileService(base_path=tmp_path)
    (tmp_path / "folder").mkdir()

    with pytest.raises(FileOperationError, match="File not found"):
        service.read_text("missing.txt")

    with pytest.raises(FileOperationError, match="Not a directory"):
        service.list_files("missing-dir")

    with pytest.raises(FileOperationError, match="Path does not exist"):
        service.validate_path("missing.txt", must_exist=True)

    binary_path = tmp_path / "data.bin"
    binary_path.write_bytes(b"\xff")
    with pytest.raises(FileOperationError, match="Encoding error"):
        service.read_text(binary_path)


# ---- RT-3 additions ----


def test_read_text_success(tmp_path: Path):
    """read_text returns file content with correct encoding."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "hello.txt").write_text("abc123", encoding="utf-8")
    assert service.read_text("hello.txt") == "abc123"


def test_read_text_missing_file(tmp_path: Path):
    """read_text raises FileOperationError for nonexistent file."""
    service = FileService(base_path=tmp_path)
    with pytest.raises(FileOperationError, match="File not found"):
        service.read_text("nope.txt")


def test_read_text_decode_error(tmp_path: Path):
    """read_text raises FileOperationError on encoding mismatch."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "bad.bin").write_bytes(b"\x80\x81\x82\xff\xfe")
    with pytest.raises(FileOperationError, match="Encoding error"):
        service.read_text("bad.bin")


def test_read_text_permission_denied(tmp_path: Path):
    """read_text raises FileOperationError on PermissionError (line 51)."""
    service = FileService(base_path=tmp_path)
    p = tmp_path / "noperm.txt"
    p.write_text("secret")
    p.chmod(0o000)
    try:
        with pytest.raises(FileOperationError, match="Permission denied"):
            service.read_text("noperm.txt")
    finally:
        p.chmod(0o644)


def test_read_bytes_success(tmp_path: Path):
    """read_bytes returns raw bytes."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "data.bin").write_bytes(b"\x00\x01\x02")
    assert service.read_bytes("data.bin") == b"\x00\x01\x02"


def test_read_bytes_missing_file(tmp_path: Path):
    """read_bytes raises FileOperationError for nonexistent file (line 60-61)."""
    service = FileService(base_path=tmp_path)
    with pytest.raises(FileOperationError, match="File not found"):
        service.read_bytes("missing.bin")


def test_read_bytes_permission_denied(tmp_path: Path):
    """read_bytes raises FileOperationError on PermissionError (line 62-63)."""
    service = FileService(base_path=tmp_path)
    p = tmp_path / "noperm.bin"
    p.write_bytes(b"\xff")
    p.chmod(0o000)
    try:
        with pytest.raises(FileOperationError, match="Permission denied"):
            service.read_bytes("noperm.bin")
    finally:
        p.chmod(0o644)


def test_read_lines(tmp_path: Path):
    """read_lines yields stripped lines."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "multi.txt").write_text("line1\nline2\nline3\n")
    lines = list(service.read_lines("multi.txt"))
    assert lines == ["line1", "line2", "line3"]


def test_write_text_creates_parent_dirs(tmp_path: Path):
    """write_text with create_parents=True creates intermediate dirs."""
    service = FileService(base_path=tmp_path)
    result = service.write_text("a/b/c/deep.txt", "deep content")
    assert result.exists()
    assert result.read_text() == "deep content"


def test_write_text_no_create_parents(tmp_path: Path):
    """write_text with create_parents=False fails if parents missing."""
    service = FileService(base_path=tmp_path)
    with pytest.raises(FileNotFoundError):
        service.write_text("missing/dir/file.txt", "content", create_parents=False)


def test_exists_true_false(tmp_path: Path):
    """exists returns True for existing paths and False for missing."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "exists.txt").write_text("yes")
    assert service.exists("exists.txt") is True
    assert service.exists("nope.txt") is False


def test_is_file(tmp_path: Path):
    """is_file returns True for files, False for dirs and missing paths."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "f.txt").write_text("x")
    (tmp_path / "d").mkdir()
    assert service.is_file("f.txt") is True
    assert service.is_file("d") is False
    assert service.is_file("missing") is False


def test_is_directory(tmp_path: Path):
    """is_directory returns True for dirs, False for files and missing paths."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "mydir").mkdir()
    (tmp_path / "myfile.txt").write_text("x")
    assert service.is_directory("mydir") is True
    assert service.is_directory("myfile.txt") is False
    assert service.is_directory("missing") is False


def test_validate_path_must_exist_true(tmp_path: Path):
    """validate_path with must_exist=True raises for nonexistent path."""
    service = FileService(base_path=tmp_path)
    with pytest.raises(FileOperationError, match="Path does not exist"):
        service.validate_path("ghost.txt", must_exist=True)


def test_validate_path_must_exist_false(tmp_path: Path):
    """validate_path with must_exist=False returns resolved path even if missing."""
    service = FileService(base_path=tmp_path)
    result = service.validate_path("ghost.txt", must_exist=False)
    assert isinstance(result, Path)


def test_list_files_non_recursive(tmp_path: Path):
    """list_files without recursive only returns files in the given dir."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "top.txt").write_text("x")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "nested.txt").write_text("y")
    result = service.list_files(".", "*.txt", recursive=False)
    names = [p.name for p in result]
    assert "top.txt" in names
    assert "nested.txt" not in names


def test_list_files_recursive(tmp_path: Path):
    """list_files with recursive=True finds files in subdirs (line 126)."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "a.py").write_text("x")
    sub = tmp_path / "pkg"
    sub.mkdir()
    (sub / "b.py").write_text("y")
    result = service.list_files(".", "*.py", recursive=True)
    names = {p.name for p in result}
    assert names == {"a.py", "b.py"}


def test_legacy_read_file(tmp_path: Path):
    """read_file is a legacy wrapper for read_text."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "legacy.txt").write_text("old api")
    assert service.read_file("legacy.txt") == "old api"


def test_legacy_file_exists(tmp_path: Path):
    """file_exists is a legacy wrapper for is_file."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "there.txt").write_text("yep")
    assert service.file_exists("there.txt") is True
    assert service.file_exists("nope.txt") is False


def test_legacy_find_files(tmp_path: Path):
    """find_files returns string paths and handles missing directory gracefully."""
    service = FileService(base_path=tmp_path)
    (tmp_path / "one.py").write_text("1")
    sub = tmp_path / "inner"
    sub.mkdir()
    (sub / "two.py").write_text("2")

    found = service.find_files(str(tmp_path), "*.py")
    assert len(found) == 2
    assert all(isinstance(f, str) for f in found)

    # Missing directory returns empty list (line 145-146)
    assert service.find_files(str(tmp_path / "missing"), "*.py") == []


def test_resolve_path_absolute(tmp_path: Path):
    """_resolve_path returns absolute path as-is."""
    service = FileService(base_path=tmp_path)
    abs_path = tmp_path / "absolute.txt"
    resolved = service._resolve_path(str(abs_path))
    assert resolved == abs_path


def test_default_base_path():
    """FileService with no base_path defaults to cwd."""
    service = FileService()
    assert service.base_path == Path.cwd()
