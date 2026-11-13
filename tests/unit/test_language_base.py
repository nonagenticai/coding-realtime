"""Tests for services/languages/base.py types."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

from services.languages.base import (
    CodeEntity,
    EntityRelation,
    LanguageProcessor,
    ParseResult,
    ParserType,
)


def test_parser_type_values():
    assert ParserType.AST_NATIVE == "ast_native"
    assert ParserType.TREE_SITTER == "tree_sitter"
    assert ParserType.REGEX_FALLBACK == "regex_fallback"


def test_code_entity_defaults():
    entity = CodeEntity()
    assert entity.name == ""
    assert entity.type == ""
    assert entity.line_number == 0
    assert entity.is_async is False


def test_code_entity_with_values():
    entity = CodeEntity(
        id="e1",
        name="MyClass",
        type="class",
        language="python",
        file_path="main.py",
        line_number=10,
        visibility="public",
    )
    assert entity.name == "MyClass"
    assert entity.visibility == "public"


def test_entity_relation_defaults():
    rel = EntityRelation()
    assert rel.source_id == ""
    assert rel.target_id == ""
    assert rel.type == ""


def test_parse_result_defaults():
    result = ParseResult()
    assert result.success is False
    assert result.entities == []
    assert result.relationships == []
    assert result.parser_used == ParserType.REGEX_FALLBACK


def test_parse_result_from_api_response():
    data = {
        "success": True,
        "file_path": "/app/main.py",
        "language": "python",
        "parser_used": "ast_native",
        "entities": [{"id": "e1", "name": "foo", "type": "function", "line_number": 1}],
        "relationships": [{"source_id": "e1", "target_id": "e2", "type": "calls"}],
        "lines_of_code": 50,
        "complexity_score": 3.5,
        "parse_time_ms": 12.0,
        "errors": [],
        "warnings": ["unused import"],
        "properties": {"framework": "flask"},
    }
    result = ParseResult.from_api_response(data)
    assert result.success is True
    assert result.language == "python"
    assert result.parser_used == ParserType.AST_NATIVE
    assert len(result.entities) == 1
    assert result.entities[0].name == "foo"
    assert len(result.relationships) == 1
    assert result.lines_of_code == 50
    assert result.warnings == ["unused import"]


def test_parse_result_from_api_response_unknown_parser():
    data = {"parser_used": "some_unknown_parser", "success": False}
    result = ParseResult.from_api_response(data)
    assert result.parser_used == ParserType.REGEX_FALLBACK


def test_parse_result_from_api_response_empty():
    result = ParseResult.from_api_response({})
    assert result.success is False
    assert result.entities == []


async def test_language_processor_parse_file():
    proc = LanguageProcessor(language="python")

    mock_client = AsyncMock()
    mock_client.parse_file = AsyncMock(
        return_value={
            "success": True,
            "file_path": "/app/main.py",
            "language": "python",
            "parser_used": "ast_native",
            "entities": [],
            "relationships": [],
            "lines_of_code": 10,
        }
    )

    with patch("clients.coding_analysis.get_coding_analysis_client", return_value=mock_client):
        result = await proc.parse_file(Path("/app/main.py"), "print('hi')")
    assert result.success is True
    assert result.language == "python"


async def test_language_processor_parse_file_with_metrics():
    proc = LanguageProcessor(language="go")

    mock_client = AsyncMock()
    mock_client.parse_file = AsyncMock(
        return_value={
            "success": True,
            "language": "go",
        }
    )

    with patch("clients.coding_analysis.get_coding_analysis_client", return_value=mock_client):
        result = await proc.parse_file_with_metrics(Path("/app/main.go"), "package main")
    assert result.success is True


async def test_language_processor_extract_entities_empty():
    proc = LanguageProcessor()
    result = await proc.extract_entities(None, Path("/app/x.py"))
    assert result == []


async def test_language_processor_extract_relationships_empty():
    proc = LanguageProcessor()
    result = await proc.extract_relationships(None, [], Path("/app/x.py"))
    assert result == []


async def test_language_processor_calculate_complexity():
    proc = LanguageProcessor()
    assert await proc.calculate_complexity(None) == 0.0


def test_language_processor_supports_feature():
    proc = LanguageProcessor()
    assert proc.supports_feature("anything") is False


async def test_language_processor_initialize():
    proc = LanguageProcessor()
    assert proc._initialized is False
    await proc.initialize()
    assert proc._initialized is True
