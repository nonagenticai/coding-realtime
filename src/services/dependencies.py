"""Dependency injection for the coding-realtime service.

HTTP adapter version of the monolith's dependencies module.  Provides
the same public functions (get_multi_language_factory, get_file_service,
etc.) so that router imports work without modification.

All heavy analysis work is delegated to coding-analysis over HTTP via
the adapter classes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.file_service import FileService
    from services.multi_language_factory import MultiLanguageProcessorFactory

logger = logging.getLogger(__name__)


class DependencyContainer:
    """Container for managing singleton service instances."""

    _file_service: FileService | None = None
    _multi_language_factory: MultiLanguageProcessorFactory | None = None

    @classmethod
    def get_file_service(cls) -> FileService:
        """Get or create FileService singleton."""
        if cls._file_service is None:
            from services.file_service import FileService

            cls._file_service = FileService()
            logger.info("Initialized FileService adapter")
        return cls._file_service

    @classmethod
    def get_multi_language_factory(cls) -> MultiLanguageProcessorFactory:
        """Get or create MultiLanguageProcessorFactory singleton.

        Returns a factory adapter that delegates parsing to the
        coding-analysis microservice over HTTP.
        """
        if cls._multi_language_factory is None:
            from services.multi_language_factory import MultiLanguageProcessorFactory

            cls._multi_language_factory = MultiLanguageProcessorFactory()
            logger.info("Initialized MultiLanguageProcessorFactory adapter")
        return cls._multi_language_factory

    @classmethod
    def reset(cls) -> None:
        """Reset all singletons (for testing)."""
        cls._file_service = None
        cls._multi_language_factory = None


# ------------------------------------------------------------------
# Public API (same signatures as the monolith)
# ------------------------------------------------------------------


def get_file_service() -> FileService:
    """Get FileService singleton for dependency injection."""
    return DependencyContainer.get_file_service()


def get_multi_language_factory() -> MultiLanguageProcessorFactory:
    """Get MultiLanguageProcessorFactory singleton for dependency injection.

    Note: The monolith version is sync; the routers call this without
    ``await``. This adapter preserves that contract.
    """
    return DependencyContainer.get_multi_language_factory()


def reset_dependencies() -> None:
    """Reset all global dependencies (for testing)."""
    DependencyContainer.reset()
