"""Tests for Settings configuration."""

from config.settings import Settings, get_settings


def test_settings_defaults(monkeypatch):
    """The declared defaults, with nothing in the environment overriding them.

    `Settings` is a pydantic `BaseSettings`, so `Settings()` is NOT "the class
    defaults" — it is the defaults *after* the environment has had its say. A
    test named `..._defaults` has to supply the precondition its name claims,
    which means clearing the variables it asserts on. Without the delenv below
    this test silently asserts whatever the ambient environment happens to hold.

    That is not hypothetical. The shared archetype workflow
    (gitops-production/.github/workflows/python-service.yml) sets a job-wide
    `ENVIRONMENT: development` — added 2026-08-04 23:47 in gitops #2441, because
    rmt-secrets-lib 0.1.7 made an ABSENT ENVIRONMENT fail closed and "development"
    is the only value that satisfies rmt-secrets-lib, admin_app and core_app at
    once. This suite's conftest `setdefault`s TRUSTED_CIDRS and DEFAULT_TENANT_ID
    but not ENVIRONMENT, so from that commit onward CI ran with
    ENVIRONMENT=development while this line asserted "production" — and the next
    push to main (the coding-client 1.10.0 bump, #41) was the one that surfaced it.
    The archetype is right; the test was reading the ambient value and calling it
    a default.

    `case_sensitive: False` on the model means any casing of the name is picked
    up, so delete the exact key pydantic would read.
    """
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    s = Settings()
    assert s.SERVICE_NAME == "coding-realtime"
    assert s.PORT == 8694
    assert s.ENABLE_AUTH is True
    assert s.ENVIRONMENT == "production"
    # REDIS_URL has no default — gitops-production env supplies it.
    assert s.REDIS_URL == ""


def test_environment_is_read_from_the_environment(monkeypatch):
    """The other half: the default must still be overridable.

    Without this, `test_settings_defaults` could be made to pass by hard-coding
    the field to "production" and dropping the env lookup entirely — which would
    break every deployment that sets ENVIRONMENT. This pins that the delenv above
    is clearing a real, live override rather than papering over a dead one.
    """
    monkeypatch.setenv("ENVIRONMENT", "development")
    assert Settings().ENVIRONMENT == "development"


def test_docs_enabled_production():
    """In production with ENABLE_DOCS=False, docs should be disabled."""
    s = Settings(ENVIRONMENT="production", ENABLE_DOCS=False)
    assert s.docs_enabled is False


def test_docs_enabled_development():
    """In development, docs should always be enabled regardless of ENABLE_DOCS."""
    s = Settings(ENVIRONMENT="development", ENABLE_DOCS=False)
    assert s.docs_enabled is True


def test_docs_enabled_explicit():
    """Explicitly enabling docs in production should work."""
    s = Settings(ENVIRONMENT="production", ENABLE_DOCS=True)
    assert s.docs_enabled is True


def test_docs_disabled_testing_environment():
    """In non-development environments like testing, docs follow the ENABLE_DOCS flag."""
    s = Settings(ENVIRONMENT="testing", ENABLE_DOCS=False)
    assert s.docs_enabled is False

    s2 = Settings(ENVIRONMENT="testing", ENABLE_DOCS=True)
    assert s2.docs_enabled is True


def test_get_settings_returns_singleton():
    """get_settings should return a Settings instance and return the same one each time."""
    s1 = get_settings()
    s2 = get_settings()
    assert isinstance(s1, Settings)
    assert s1 is s2
