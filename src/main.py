"""FastAPI application entry point for coding-realtime.

Wires up routers, middleware, Redis, and exception handling.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from enum import Enum

import redis.asyncio as aioredis
from coding_client.api.exception_handlers import register_exception_handlers
from coding_client.api.logging_setup import configure_logging
from coding_client.api.openapi_config import configure_openapi
from coding_client.api.request_logging import HealthCheckFilter, RequestLoggingMiddleware
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import get_settings

configure_logging()

logger = logging.getLogger(__name__)
settings = get_settings()
logging.getLogger("uvicorn.access").addFilter(HealthCheckFilter())

# ---------------------------------------------------------------------------
# Redis connection (module-level, initialised in lifespan)
# ---------------------------------------------------------------------------
_redis: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    """Return the shared Redis connection (available after startup)."""
    if _redis is None:
        raise RuntimeError("Redis is not initialised -- called before lifespan startup?")
    return _redis


async def _redis_ping(client: aioredis.Redis) -> bool:
    """Wrapper around redis ping that satisfies the type checker.

    ``redis.asyncio.Redis.ping()`` is typed as ``Awaitable[bool] | bool``
    but is always awaitable when using the async client.
    """
    result = client.ping()
    if hasattr(result, "__await__"):
        return bool(await result)
    return bool(result)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application startup / shutdown events."""
    global _redis  # noqa: PLW0603

    logger.info(
        "Starting %s v%s (%s) on port %s",
        settings.SERVICE_NAME,
        settings.VERSION,
        settings.ENVIRONMENT,
        settings.PORT,
    )

    # ── Redis ─────────────────────────────────────────────────────────
    # Client construction and reachability are handled SEPARATELY on purpose.
    # A construction failure (missing/invalid REDIS_URL) means there is no redis
    # to talk to at all -> pub/sub is genuinely unavailable. But an unreachable
    # redis at boot must NOT permanently disable cross-replica fan-out for the
    # life of the pod: redis-py reconnects lazily on the next command and the
    # broadcast subscriber retries every 2s, so keeping the client lets the pod
    # self-heal when redis returns. (Previously a boot-time outage left this pod
    # local-only forever while still passing its readiness probe, because
    # /health returns 200 with status "degraded" — silent loss at >1 replica.)
    try:
        _redis = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
        )
    except Exception as exc:
        logger.warning("Redis client could not be created (no pub/sub): %s", exc)
        _redis = None

    if _redis is not None:
        try:
            await _redis_ping(_redis)
            logger.info("Redis connected: %s", settings.REDIS_URL)
        except Exception as exc:
            logger.warning(
                "Redis unreachable at startup; cross-replica fan-out will self-heal "
                "when redis returns: %s",
                exc,
            )

    # ── Cross-replica broadcast fan-out (D2) ──────────────────────────
    # Wire the broadcast service to redis and start its subscriber so a
    # broadcast published on any replica reaches subscribers on every replica.
    # Degrades to local-only delivery when redis is unavailable.
    try:
        from services.websocket_broadcast import get_broadcast_service

        broadcast_service = get_broadcast_service()
        if _redis is not None:
            broadcast_service.attach_redis(_redis)
            await broadcast_service.start_subscriber()
            # Cross-replica subscriber PRESENCE. Without the heartbeat, presence
            # entries would expire under a long-lived websocket and the publish
            # endpoint would start under-reporting delivery.
            await broadcast_service.start_presence_heartbeat()
    except Exception as exc:
        logger.warning("Broadcast cross-replica fan-out not started: %s", exc)

    yield

    # Stop the broadcast subscriber before closing redis, and drop this pod's
    # presence entries so the surviving replica does not over-report for a whole
    # TTL window after a graceful restart.
    try:
        from services.websocket_broadcast import get_broadcast_service

        broadcast_service = get_broadcast_service()
        await broadcast_service.stop_presence_heartbeat()
        await broadcast_service.purge_local_presence()
        await broadcast_service.stop_subscriber()
    except Exception as exc:
        logger.debug("Broadcast subscriber stop skipped: %s", exc)

    # ── Shutdown ──────────────────────────────────────────────────────
    # Close HTTP client singletons
    try:
        from clients.coding_analysis import get_coding_analysis_client

        await get_coding_analysis_client().close()
        logger.info("CodingAnalysisClient closed")
    except Exception as exc:
        logger.debug("CodingAnalysisClient cleanup skipped: %s", exc)

    try:
        from clients.coding_containers import get_coding_containers_client

        await get_coding_containers_client().close()
        logger.info("CodingContainersClient closed")
    except Exception as exc:
        logger.debug("CodingContainersClient cleanup skipped: %s", exc)

    if _redis is not None:
        await _redis.aclose()
        logger.info("Redis connection closed")
    logger.info("Shutting down %s", settings.SERVICE_NAME)


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title=settings.SERVICE_NAME,
    description=(
        "WebSocket gateway for real-time code analysis.\n\n"
        "Manages persistent connections between IDE clients and the analysis backend, "
        "providing live parse results, container-scoped broadcast pub/sub, per-connection "
        "health monitoring, and rate limiting. Delegates parsing to **coding-analysis** "
        "and container validation to **coding-containers**.\n\n"
        "## Authentication\n\n"
        "Every tenant-scoped route resolves the caller's tenant with `coding-client`'s "
        "`require_tenant`. Production runs with `TRUST_TENANT_HEADER=false`, so:\n\n"
        "1. **`Authorization: Bearer <license-token>`** is the identity. It is "
        "introspected via the licenses service (`LICENSE_INTROSPECT_URL`) and the "
        "licence's `tenant_id` is the caller's tenant. coding-gateway forwards the "
        "client's Authorization header.\n"
        "2. **`X-Tenant-Id` is NOT a credential.** A bare header (no licence) is "
        "ignored and the request is a 401. Alongside a licence, a header naming a "
        "*different* tenant is an on-behalf-of assertion: honoured only when the "
        "licence's subject is listed in `CODING_ONBEHALF_SUBJECTS` (e.g. "
        "coding-gateway acting for a customer), otherwise **403**. A header that "
        "agrees with the licence changes nothing. (With `TRUST_TENANT_HEADER=true`, a "
        "non-production setting, the header is trusted as presented -- never rely on "
        "that.)\n"
        "3. **Trusted-network bypass** -- grants `DEFAULT_TENANT_ID` by source IP, only "
        "when an operator sets `TRUSTED_CIDRS`. It is empty by default and in "
        "production, so no source address is trusted on network position alone.\n\n"
        "**WebSocket handshakes** (`/api/realtime/analysis`, `/api/broadcast/subscribe`) "
        "bypass the HTTP middleware, so each handler resolves identity itself with the "
        "same rules: a verifiable licence is REQUIRED on the handshake (an "
        "uncredentialed handshake is closed 1008 even if it carries `X-Tenant-Id`), "
        "`?tenant_id=` is never read, and the on-behalf allowlist applies.\n\n"
        "Listings and aggregates (`/api/realtime/connections`, `/api/realtime/metrics`, "
        "`/api/ws-health/*`, `/api/stats`, `/api/violations`, `/api/global-stats`) are "
        "scoped to the caller's tenant; a container or connection belonging to another "
        "tenant is reported as not found (404).\n\n"
        "Health/liveness/readiness probes are public.\n\n"
        "## Endpoint Groups\n\n"
        "- **Health** (`/health`, `/api/health/*`) -- liveness, readiness, detailed checks\n"
        "- **Real-time Analysis** (`/api/realtime/*`) -- WebSocket analysis + REST metrics\n"
        "- **Broadcast** (`/api/broadcast/*`) -- container-scoped pub/sub WebSocket\n"
        "- **WebSocket Health** (`/api/ws-health/*`) -- connection/container health stats\n"
        "- **Rate Limits** (`/api/stats/*`, `/api/violations`, `/api/global-stats`) -- rate-limit monitoring"
    ),
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# ── CORS ──────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,  # type: ignore[invalid-argument-type]
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials="*" not in settings.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Request logging middleware ────────────────────────────────────────
app.add_middleware(RequestLoggingMiddleware)  # type: ignore[invalid-argument-type]

# ── Auth middleware (from coding-client) ──────────────────────────────
if settings.ENABLE_AUTH:
    try:
        from auth.middleware import AuthMiddleware
        from auth.tenant import TenantMiddleware

        app.add_middleware(TenantMiddleware)  # type: ignore[invalid-argument-type]
        app.add_middleware(AuthMiddleware)  # type: ignore[invalid-argument-type]
        logger.info("Auth + Tenant middleware enabled")
    except ImportError:
        logger.warning(
            "coding-client auth not available -- running WITHOUT authentication middleware"
        )

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
# Each router module is built by the router-extraction agents; imports are
# guarded so the app can still start even if a router module is not yet present.

_router_specs: list[tuple[str, str, list[str | Enum]]] = [
    # (module_path, prefix, tags)
    ("routers.health", "/api/health", ["Health"]),
    ("routers.websocket_health", "/api/ws-health", ["WebSocket Health"]),
    ("routers.websocket_rate_limits", "/api", ["WebSocket Rate Limits"]),
    ("routers.realtime", "/api/realtime", ["Real-time Analysis"]),
    ("routers.broadcast_websocket", "/api/broadcast", ["Broadcast WebSocket"]),
]

for _module, _prefix, _tags in _router_specs:
    try:
        import importlib

        mod = importlib.import_module(_module)
        app.include_router(mod.router, prefix=_prefix, tags=_tags)
        logger.info("Router loaded: %s -> %s", _module, _prefix)
    except (ImportError, AttributeError) as exc:
        logger.warning("Router not yet available: %s (%s)", _module, exc)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@app.get("/health", tags=["Health"], operation_id="health_check")
async def health():
    """Liveness / readiness probe."""
    redis_ok = False
    if _redis is not None:
        try:
            await _redis_ping(_redis)
            redis_ok = True
        except Exception as exc:
            logger.debug("Redis ping failed in /health: %s", exc)

    return {
        "status": "ok" if redis_ok else "degraded",
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT,
        "redis": "connected" if redis_ok else "disconnected",
        "timestamp": datetime.now(UTC).isoformat(),
    }


@app.get("/", tags=["Root"], operation_id="get_root")
async def root():
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "docs": "/docs" if settings.docs_enabled else None,
        "health": "/health",
    }


# ---------------------------------------------------------------------------
# Exception handlers (from coding-client KAG API standard)
# ---------------------------------------------------------------------------
register_exception_handlers(app, debug=bool(getattr(settings, "DEBUG", False)))

# ---------------------------------------------------------------------------
# OpenAPI security configuration
# ---------------------------------------------------------------------------
configure_openapi(
    app,
    protected_prefixes={"/api/realtime", "/api/broadcast", "/api/ws-health", "/api/stats"},
    public_endpoints={"/", "/health", "/docs", "/redoc", "/openapi.json"},
)
