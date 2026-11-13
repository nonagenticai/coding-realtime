"""Multi-language processor factory adapter for the coding-realtime service.

HTTP adapter version of the monolith's MultiLanguageProcessorFactory.
Provides the same interface but delegates processor creation and file
analysis to the coding-analysis microservice over HTTP.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from services.languages.base import LanguageProcessor

logger = logging.getLogger(__name__)

# Known extensions for quick local checks (avoids HTTP round-trip)
_KNOWN_EXTENSIONS: dict[str, str] = {
    ".py": "python",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".java": "java",
    ".rs": "rust",
    ".go": "go",
    ".cs": "csharp",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".cxx": "cpp",
    ".c": "c",
    ".h": "c",
    ".hpp": "cpp",
}


class MultiLanguageProcessorFactory:
    """HTTP adapter that mimics the monolith MultiLanguageProcessorFactory.

    Instead of instantiating local language processors, this adapter
    returns lightweight LanguageProcessor instances that delegate
    parsing to the coding-analysis microservice.
    """

    def __init__(self) -> None:
        self._processor_cache: dict[str, LanguageProcessor] = {}

    async def get_processor(self, language: str) -> LanguageProcessor | None:
        """Get a processor for the specified language.

        Args:
            language: Programming language name (e.g. "python").

        Returns:
            LanguageProcessor adapter instance, or None if unknown.
        """
        language = language.lower()
        if language not in self._processor_cache:
            self._processor_cache[language] = LanguageProcessor(language=language)
        return self._processor_cache[language]

    async def get_processor_for_file(self, file_path: str | Path) -> LanguageProcessor | None:
        """Get a processor based on file extension.

        Args:
            file_path: Path to the file.

        Returns:
            LanguageProcessor adapter instance, or None if unsupported.
        """
        file_path = Path(file_path)
        ext = file_path.suffix.lower()
        language = _KNOWN_EXTENSIONS.get(ext)
        if language:
            return await self.get_processor(language)

        # Fall back to asking the analysis service
        from clients.coding_analysis import get_coding_analysis_client

        client = get_coding_analysis_client()
        info = await client.get_processor_info(str(file_path))
        if info and info.get("language"):
            return await self.get_processor(info["language"])

        logger.debug("No processor available for file extension: %s", ext)
        return None

    def get_supported_languages(self) -> list[str]:
        """Return locally-known supported languages.

        For a fully authoritative list, call the async version via the
        coding-analysis client.
        """
        return sorted(set(_KNOWN_EXTENSIONS.values()))

    def get_supported_extensions(self) -> list[str]:
        """Return locally-known supported file extensions."""
        return sorted(_KNOWN_EXTENSIONS.keys())

    def is_supported_file(self, file_path: str | Path) -> bool:
        """Check if a file has a known supported extension."""
        ext = Path(file_path).suffix.lower()
        return ext in _KNOWN_EXTENSIONS

    def get_language_info(self, language: str) -> dict[str, Any] | None:
        """Get basic language information (local only)."""
        language = language.lower()
        exts = [ext for ext, lang in _KNOWN_EXTENSIONS.items() if lang == language]
        if not exts:
            return None
        return {
            "name": language,
            "display_name": language.capitalize(),
            "extensions": exts,
            "parser_type": "remote",
            "features": {},
        }


# Backward compatibility alias
MultiLanguageFactory = MultiLanguageProcessorFactory
