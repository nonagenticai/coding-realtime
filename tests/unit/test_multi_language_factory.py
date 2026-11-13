"""Tests for MultiLanguageProcessorFactory."""

from unittest.mock import AsyncMock, patch

from services.languages.base import LanguageProcessor
from services.multi_language_factory import (
    MultiLanguageProcessorFactory,
)


async def test_get_processor_returns_language_processor():
    factory = MultiLanguageProcessorFactory()
    proc = await factory.get_processor("python")
    assert isinstance(proc, LanguageProcessor)
    assert proc.language == "python"


async def test_get_processor_caches():
    factory = MultiLanguageProcessorFactory()
    p1 = await factory.get_processor("python")
    p2 = await factory.get_processor("python")
    assert p1 is p2


async def test_get_processor_case_insensitive():
    factory = MultiLanguageProcessorFactory()
    p1 = await factory.get_processor("Python")
    p2 = await factory.get_processor("PYTHON")
    assert p1 is p2
    assert p1 is not None
    assert p1.language == "python"


async def test_get_processor_for_file_known_extension():
    factory = MultiLanguageProcessorFactory()
    proc = await factory.get_processor_for_file("main.py")
    assert proc is not None
    assert proc.language == "python"


async def test_get_processor_for_file_ts():
    factory = MultiLanguageProcessorFactory()
    proc = await factory.get_processor_for_file("app.tsx")
    assert proc is not None
    assert proc.language == "typescript"


async def test_get_processor_for_file_unknown_with_remote_fallback():
    factory = MultiLanguageProcessorFactory()
    mock_client = AsyncMock()
    mock_client.get_processor_info = AsyncMock(return_value={"language": "ruby"})

    with patch("clients.coding_analysis.get_coding_analysis_client", return_value=mock_client):
        proc = await factory.get_processor_for_file("app.rb")
    assert proc is not None
    assert proc.language == "ruby"


async def test_get_processor_for_file_unknown_no_remote():
    factory = MultiLanguageProcessorFactory()
    mock_client = AsyncMock()
    mock_client.get_processor_info = AsyncMock(return_value=None)

    with patch("clients.coding_analysis.get_coding_analysis_client", return_value=mock_client):
        proc = await factory.get_processor_for_file("data.xyz")
    assert proc is None


def test_get_supported_languages():
    factory = MultiLanguageProcessorFactory()
    langs = factory.get_supported_languages()
    assert "python" in langs
    assert "javascript" in langs
    assert langs == sorted(langs)


def test_get_supported_extensions():
    factory = MultiLanguageProcessorFactory()
    exts = factory.get_supported_extensions()
    assert ".py" in exts
    assert ".js" in exts


def test_is_supported_file():
    factory = MultiLanguageProcessorFactory()
    assert factory.is_supported_file("main.py") is True
    assert factory.is_supported_file("app.js") is True
    assert factory.is_supported_file("readme.md") is False


def test_get_language_info_known():
    factory = MultiLanguageProcessorFactory()
    info = factory.get_language_info("python")
    assert info is not None
    assert info["name"] == "python"
    assert ".py" in info["extensions"]
    assert info["parser_type"] == "remote"


def test_get_language_info_unknown():
    factory = MultiLanguageProcessorFactory()
    info = factory.get_language_info("brainfuck")
    assert info is None
