"""Language processing adapters for the coding-realtime service.

These are thin adapter types that match the monolith interface but delegate
actual parsing to the coding-analysis microservice over HTTP.
"""

from .base import LanguageProcessor, ParseResult, ParserType

__all__ = ["LanguageProcessor", "ParseResult", "ParserType"]
