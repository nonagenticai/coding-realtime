"""File service adapter for the coding-realtime service.

HTTP adapter version of the monolith's FileService. Provides the same
interface for basic file operations. Operations that can be done locally
(read, exists, etc.) are performed locally. Container-scoped operations
delegate to coding-containers over HTTP.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

logger = logging.getLogger(__name__)


class FileOperationError(Exception):
    """Raised when a file operation fails."""


class FileService:
    """Service for file system operations.

    HTTP adapter that mirrors the monolith FileService interface.
    Local file operations are performed directly; container-scoped
    file reads can delegate to coding-containers when needed.
    """

    def __init__(
        self,
        base_path: Path | None = None,
        default_encoding: str = "utf-8",
    ) -> None:
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.default_encoding = default_encoding

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def read_text(self, file_path: str | Path, encoding: str | None = None) -> str:
        """Read a text file."""
        encoding = encoding or self.default_encoding
        try:
            path = self._resolve_path(file_path)
            return path.read_text(encoding=encoding)
        except FileNotFoundError:
            raise FileOperationError(f"File not found: {file_path}")
        except PermissionError:
            raise FileOperationError(f"Permission denied: {file_path}")
        except UnicodeDecodeError as e:
            raise FileOperationError(f"Encoding error reading {file_path}: {e}")

    def read_bytes(self, file_path: str | Path) -> bytes:
        """Read a binary file."""
        try:
            path = self._resolve_path(file_path)
            return path.read_bytes()
        except FileNotFoundError:
            raise FileOperationError(f"File not found: {file_path}")
        except PermissionError:
            raise FileOperationError(f"Permission denied: {file_path}")

    def read_lines(self, file_path: str | Path, encoding: str | None = None) -> Iterator[str]:
        """Read a file line by line."""
        encoding = encoding or self.default_encoding
        path = self._resolve_path(file_path)
        with path.open("r", encoding=encoding) as f:
            for line in f:
                yield line.rstrip("\n")

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def write_text(
        self,
        file_path: str | Path,
        content: str,
        encoding: str | None = None,
        create_parents: bool = True,
    ) -> Path:
        """Write text to a file."""
        encoding = encoding or self.default_encoding
        path = self._resolve_path(file_path)
        if create_parents:
            path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding=encoding)
        return path

    # ------------------------------------------------------------------
    # Path / existence checks
    # ------------------------------------------------------------------

    def exists(self, path: str | Path) -> bool:
        """Check if a file or directory exists."""
        return self._resolve_path(path).exists()

    def is_file(self, path: str | Path) -> bool:
        """Check if path is a file."""
        resolved = self._resolve_path(path)
        return resolved.exists() and resolved.is_file()

    def is_directory(self, path: str | Path) -> bool:
        """Check if path is a directory."""
        resolved = self._resolve_path(path)
        return resolved.exists() and resolved.is_dir()

    def validate_path(self, path: str | Path, must_exist: bool = False) -> Path:
        """Validate a file path."""
        resolved = self._resolve_path(path)
        if must_exist and not resolved.exists():
            raise FileOperationError(f"Path does not exist: {path}")
        return resolved

    def list_files(
        self, dir_path: str | Path, pattern: str = "*", recursive: bool = False
    ) -> list[Path]:
        """List files in a directory."""
        path = self._resolve_path(dir_path)
        if not path.is_dir():
            raise FileOperationError(f"Not a directory: {dir_path}")
        if recursive:
            return [p for p in path.rglob(pattern) if p.is_file()]
        return [p for p in path.glob(pattern) if p.is_file()]

    # ------------------------------------------------------------------
    # Legacy compatibility
    # ------------------------------------------------------------------

    def read_file(self, path: str, encoding: str | None = None) -> str:
        """Legacy method for backward compatibility."""
        return self.read_text(path, encoding)

    def file_exists(self, path: str) -> bool:
        """Legacy method for backward compatibility."""
        return self.is_file(path)

    def find_files(self, directory: str, pattern: str) -> list[str]:
        """Legacy method for backward compatibility."""
        try:
            files = self.list_files(directory, pattern, recursive=True)
            return [str(f) for f in files]
        except FileOperationError:
            return []

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_path(self, path: str | Path) -> Path:
        """Resolve a path relative to the base path."""
        path_obj = Path(path)
        if path_obj.is_absolute():
            return path_obj
        return (self.base_path / path_obj).resolve()
