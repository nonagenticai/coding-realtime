"""Tests for DependencyContainer."""

from services.dependencies import (
    DependencyContainer,
    get_file_service,
    get_multi_language_factory,
    reset_dependencies,
)


def test_get_file_service_returns_instance():
    DependencyContainer.reset()
    svc = get_file_service()
    from services.file_service import FileService

    assert isinstance(svc, FileService)


def test_get_multi_language_factory_returns_instance():
    DependencyContainer.reset()
    factory = get_multi_language_factory()
    from services.multi_language_factory import MultiLanguageProcessorFactory

    assert isinstance(factory, MultiLanguageProcessorFactory)


def test_reset_clears_singletons():
    DependencyContainer.reset()
    svc1 = get_file_service()
    DependencyContainer.reset()
    svc2 = get_file_service()
    assert svc1 is not svc2


# ---- RT-3 additions ----


def test_singleton_creation_both_services():
    """Both singletons are created and returned on first call (covers line 81 via get_multi_language_factory path)."""
    DependencyContainer.reset()
    fs = get_file_service()
    mlf = get_multi_language_factory()

    from services.file_service import FileService
    from services.multi_language_factory import MultiLanguageProcessorFactory

    assert isinstance(fs, FileService)
    assert isinstance(mlf, MultiLanguageProcessorFactory)

    # Second call returns same instances
    assert get_file_service() is fs
    assert get_multi_language_factory() is mlf


def test_reset_clears_both_and_recreated_differ():
    """After reset, newly created services are different objects."""
    DependencyContainer.reset()
    fs1 = get_file_service()
    mlf1 = get_multi_language_factory()

    reset_dependencies()

    fs2 = get_file_service()
    mlf2 = get_multi_language_factory()

    assert fs1 is not fs2
    assert mlf1 is not mlf2


def test_reset_dependencies_function():
    """reset_dependencies() is a public wrapper for DependencyContainer.reset()."""
    DependencyContainer.reset()
    get_file_service()
    reset_dependencies()
    assert DependencyContainer._file_service is None
    assert DependencyContainer._multi_language_factory is None
