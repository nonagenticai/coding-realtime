"""Health probe endpoints for Kubernetes liveness, readiness, and detailed checks."""

from __future__ import annotations

import logging
import time
from typing import Any

from coding_client.auth import require_tenant
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from config.settings import get_settings
from main import _redis_ping

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter()

_start_time = time.time()


@router.get("/live", summary="Liveness Check", operation_id="health_live")
async def liveness() -> dict[str, str]:
    """Kubernetes liveness probe — always returns 200 if the process is alive."""
    return {"status": "alive"}


@router.get("/ready", summary="Readiness Check", operation_id="health_ready")
async def readiness() -> JSONResponse:
    """Kubernetes readiness probe — checks Redis connectivity."""
    from main import _redis

    redis_ok = False
    if _redis is not None:
        try:
            await _redis_ping(_redis)
            redis_ok = True
        except Exception as exc:
            logger.debug("Redis ping failed in /ready: %s", exc)

    return JSONResponse(
        status_code=200 if redis_ok else 503,
        content={
            "status": "ready" if redis_ok else "not_ready",
            "redis": "connected" if redis_ok else "disconnected",
        },
    )


@router.get("/detailed", summary="Detailed Health Check", operation_id="health_detailed")
async def detailed(
    tenant_id: str = Depends(require_tenant),
) -> dict[str, Any]:
    """Detailed health check — checks Redis, coding-analysis, coding-containers."""
    from main import _redis

    uptime = time.time() - _start_time
    checks: dict[str, Any] = {}

    # Redis
    redis_ok = False
    if _redis is not None:
        try:
            await _redis_ping(_redis)
            redis_ok = True
        except Exception as exc:
            logger.debug("Redis ping failed in /detailed: %s", exc)
    checks["redis"] = "connected" if redis_ok else "disconnected"

    # coding-analysis
    try:
        from clients.coding_analysis import get_coding_analysis_client

        checks["coding_analysis"] = await get_coding_analysis_client().health_check()
    except Exception:
        checks["coding_analysis"] = False

    # coding-containers
    try:
        from clients.coding_containers import get_coding_containers_client

        checks["coding_containers"] = await get_coding_containers_client().health_check()
    except Exception:
        checks["coding_containers"] = False

    overall = (
        redis_ok
        and checks.get("coding_analysis") is True
        and checks.get("coding_containers") is True
    )
    return {
        "status": "healthy" if overall else "degraded",
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "uptime_seconds": round(uptime, 2),
        "checks": checks,
    }
