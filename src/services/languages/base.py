"""Language processing base types for the coding-realtime service.

HTTP adapter version of the monolith's processing.languages.base module.
Provides the same LanguageProcessor, ParseResult, and ParserType interfaces
so that router code can use them without modification.

The LanguageProcessor here is *not* an ABC -- it is a concrete adapter that
delegates file parsing to the coding-analysis microservice over HTTP.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Enums
# ------------------------------------------------------------------


class ParserType(StrEnum):
    """Supported parser types (mirrors monolith enum)."""

    AST_NATIVE = "ast_native"
    TREE_SITTER = "tree_sitter"
    EXTERNAL_TOOL = "external_tool"
    LANGUAGE_SERVER = "language_server"
    REGEX_FALLBACK = "regex_fallback"


# ------------------------------------------------------------------
# Lightweight entity / relation models for parse results
# ------------------------------------------------------------------


class CodeEntity(BaseModel):
    """Minimal code entity returned in parse results."""

    model_config = ConfigDict(extra="allow")

    id: str = ""
    name: str = ""
    type: str = ""
    language: str = ""
    file_path: str = ""
    line_number: int = 0
    end_line_number: int | None = None
    column_start: int | None = None
    column_end: int | None = None
    visibility: str | None = None
    signature: str | None = None
    is_abstract: bool = False
    is_static: bool = False
    is_async: bool = False
    properties: dict[str, Any] = Field(default_factory=dict)


class EntityRelation(BaseModel):
    """Minimal entity relation returned in parse results."""

    model_config = ConfigDict(extra="allow")

    source_id: str = ""
    target_id: str = ""
    type: str = ""
    properties: dict[str, Any] = Field(default_factory=dict)


# ------------------------------------------------------------------
# ParseResult
# ------------------------------------------------------------------


class ParseResult(BaseModel):
    """Result of parsing a source code file.

    Mirrors the monolith ParseResult but uses lightweight entity models
    instead of importing CoreCodeEntity/EntityRelation from the monolith.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    success: bool = False
    file_path: str = ""
    language: str = ""
    parser_used: ParserType = ParserType.REGEX_FALLBACK

    # Parsed content
    ast: Any | None = Field(None, description="Abstract syntax tree (always None in adapter)")
    entities: list[CodeEntity] = Field(default_factory=list)
    relationships: list[EntityRelation] = Field(default_factory=list)

    # Metadata
    lines_of_code: int = 0
    complexity_score: float = 0.0
    parse_time_ms: float = 0.0

    # Error handling
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    # Language-specific properties
    properties: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> ParseResult:
        """Construct a ParseResult from a coding-analysis API response dict."""
        parser_used = ParserType.REGEX_FALLBACK
        raw_parser = data.get("parser_used", "")
        if raw_parser:
            try:
                parser_used = ParserType(raw_parser)
            except ValueError:
                logger.debug("Unknown parser type '%s', falling back to REGEX_FALLBACK", raw_parser)

        entities = [CodeEntity(**e) if isinstance(e, dict) else e for e in data.get("entities", [])]
        relationships = [
            EntityRelation(**r) if isinstance(r, dict) else r for r in data.get("relationships", [])
        ]

        return cls(
            success=data.get("success", False),
            file_path=data.get("file_path", ""),
            language=data.get("language", ""),
            parser_used=parser_used,
            entities=entities,
            relationships=relationships,
            lines_of_code=data.get("lines_of_code", 0),
            complexity_score=data.get("complexity_score", 0.0),
            parse_time_ms=data.get("parse_time_ms", 0.0),
            errors=data.get("errors", []),
            warnings=data.get("warnings", []),
            properties=data.get("properties", {}),
        )


# ------------------------------------------------------------------
# LanguageProcessor adapter
# ------------------------------------------------------------------


class LanguageProcessor:
    """HTTP adapter that mimics the monolith LanguageProcessor interface.

    Instead of doing local AST parsing, this adapter calls the
    coding-analysis microservice to parse files. It exposes the same
    public methods that the routers expect (parse_file_with_metrics,
    parse_file, etc.).
    """

    def __init__(self, language: str = "unknown") -> None:
        self.language = language
        self._initialized = False

    async def initialize(self) -> None:
        """No-op for the HTTP adapter."""
        self._initialized = True

    async def parse_file(self, file_path: Path, content: str) -> ParseResult:
        """Parse a file by delegating to coding-analysis over HTTP.

        Args:
            file_path: Path to the source file.
            content: Source code content.

        Returns:
            ParseResult with analysis data from the remote service.
        """
        from clients.coding_analysis import get_coding_analysis_client

        client = get_coding_analysis_client()
        data = await client.parse_file(
            file_path=str(file_path),
            content=content,
            language=self.language if self.language != "unknown" else None,
        )
        return ParseResult.from_api_response(data)

    async def parse_file_with_metrics(
        self,
        file_path: Path,
        content: str,
        analysis_id: str | None = None,
    ) -> ParseResult:
        """Parse a file with metrics -- same as parse_file for the adapter.

        The metrics collection happens server-side in coding-analysis.
        """
        return await self.parse_file(file_path, content)

    async def extract_entities(self, ast: Any, file_path: Path) -> list[CodeEntity]:
        """Not supported in adapter -- entities come from parse_file."""
        return []

    async def extract_relationships(
        self, ast: Any, entities: list[CodeEntity], file_path: Path
    ) -> list[EntityRelation]:
        """Not supported in adapter -- relationships come from parse_file."""
        return []

    async def calculate_complexity(self, ast: Any) -> float:
        """Not supported in adapter -- complexity comes from parse_file."""
        return 0.0

    async def validate_syntax(self, content: str) -> list[str]:
        """Not supported in adapter."""
        return []

    def supports_feature(self, feature: str) -> bool:
        """Feature checks not available via adapter."""
        return False
