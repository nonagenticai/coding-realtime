"""Service configuration via Pydantic Settings.

All settings are loaded from environment variables with sensible defaults.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """coding-realtime service configuration."""

    model_config = {"env_prefix": "", "case_sensitive": False}

    # ── Service identity ──────────────────────────────────────────────
    SERVICE_NAME: str = "coding-realtime"
    VERSION: str = "1.0.0"
    ENVIRONMENT: str = "production"
    PORT: int = 8694

    # ── Database (optional – some realtime features may need persistence) ──
    DATABASE_URL: str | None = None
    PGBOUNCER_URL: str | None = None

    # ── Redis (required for pub/sub broadcast and rate-limit state) ──
    # Real value comes from the coding-realtime Deployment env block in
    # gitops-production. No default — fail loud if unset.
    REDIS_URL: str = ""

    # ── Authentication & authorisation ────────────────────────────────
    ENABLE_AUTH: bool = True
    # NB there is deliberately no ``DEFAULT_TENANT_ID`` field here. It used to
    # exist with a ``"n8n-dev"`` default, which is the *stale pre-uuid7 label*
    # of a tenant that really exists — so an unset env var did not fail, it
    # silently attributed this service's calls to an unaddressable silo, with a
    # valid ``CODING_SERVICE_TOKEN`` behind them. The tenant is now read from
    # ``coding_client.auth.tenant.DEFAULT_TENANT_ID``, which is ``None`` when
    # unset, and the one call site that needs it refuses rather than guesses.
    # See ``clients/coding_analysis.py``.

    # ── API docs ──────────────────────────────────────────────────────
    ENABLE_DOCS: bool = False

    # ── Logging ───────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # "json" or "text"

    # ── CORS ──────────────────────────────────────────────────────────
    CORS_ORIGINS: list[str] = Field(default_factory=lambda: ["*"])

    # ── OpenTelemetry (OTLP) ──────────────────────────────────────────
    OTLP_TRACES_ENDPOINT: str | None = None
    OTLP_INSECURE: bool = True
    OTEL_SERVICE_NAME: str = "coding-realtime"

    # ── Inter-service URLs ────────────────────────────────────────────
    # Real values come from the coding-realtime Deployment env block in
    # gitops-production. No defaults — fail loud at the call-site if unset.
    CODING_ANALYSIS_URL: str = ""
    CODING_CONTAINERS_URL: str = ""

    # ── WebSocket-specific ────────────────────────────────────────────
    WS_PING_INTERVAL: int = 30  # seconds between pings
    WS_PING_TIMEOUT: int = 10  # seconds to wait for pong
    WS_MAX_FAILED_PINGS: int = 3
    WS_STALE_THRESHOLD: int = 300  # seconds before connection is stale
    WS_MAX_CONNECTIONS_PER_CONTAINER: int = 10
    WS_MESSAGE_RATE_LIMIT: float = 10.0  # messages per second
    WS_RATE_LIMIT_BURST: int = 20

    # ── Realtime analysis state ───────────────────────────────────────
    # LRU cap on the per-(tenant, container, file) content-hash and
    # last-analysis maps held by the ConnectionManager singleton. They are
    # shared across a container's connections, so they cannot be pruned on
    # disconnect; the cap is what keeps them bounded on a long-lived pod.
    REALTIME_MAX_TRACKED_FILES: int = 2000

    @property
    def docs_enabled(self) -> bool:
        """Docs are on in dev or when explicitly enabled."""
        return self.ENVIRONMENT == "development" or self.ENABLE_DOCS


def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return _settings


_settings = Settings()
