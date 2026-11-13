# coding-realtime

[![CI](https://github.com/nonagenticai/coding-realtime/actions/workflows/ci.yml/badge.svg)](https://github.com/nonagenticai/coding-realtime/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/nonagenticai/coding-realtime)](https://github.com/nonagenticai/coding-realtime/releases)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

WebSocket gateway for the code-graph-helper platform. Provides persistent, bidirectional connections between IDE clients and the analysis backend so that code changes produce instant feedback -- parse results, complexity scores, diagnostics -- without polling.

The service does not perform analysis itself. It delegates parsing to **coding-analysis** over HTTP and validates containers through **coding-containers**. Its value is the real-time delivery layer: connection lifecycle management, per-container broadcast pub/sub, rate limiting, health monitoring, and metrics collection.

## Architecture

```
IDE / Browser
    |  WebSocket
    v
coding-realtime (port 8694)
    |                       |
    | HTTP                  | HTTP
    v                       v
coding-analysis (8688)   coding-containers (8691)
    (parse files)           (container CRUD, file reads)
    |
    v
  Redis (pub/sub, rate-limit state)
```

## Authentication

Auth is handled by the `coding-client` library (`AuthMiddleware` + `TenantMiddleware`).

**No Keycloak, no JWT validation in this service.** Tenant identity is resolved in priority order:

First match wins -- a later mechanism only runs if every earlier one missed.

1. **`X-Tenant-Id` header, trusted as presented.** Not verified by this service. In
   production coding-gateway resolves the tenant itself and injects this header, so in
   practice **this is the only step that ever fires** and steps 2-3 are unreachable for
   every current caller.
2. **`Authorization: Bearer <license-token>`** -- introspected against
   `licenses:8544/license/introspect` (2s timeout, successful
   lookups cached 5 min). Reached **only** when no tenant header is present, and only
   functional when `LICENSE_INTROSPECT_URL` is set on the Deployment. With it unset,
   `_introspect_license` logs `"LICENSE_INTROSPECT_URL not set; license introspection
   skipped"` and returns no tenant -- the whole step is a no-op.
3. **Istio-forwarded user headers** -- `X-User-Id`, `X-User-Name`, `X-User-Email`,
   `X-User-Roles`, `X-Organization-Id`
4. **Trusted-network bypass** -- source IP in `TRUSTED_CIDRS` gets `DEFAULT_TENANT_ID`.
   ⚠️ The library default is `127.0.0.1/32,::1/128,192.168.0.0/16,172.16.0.0/12,10.0.0.0/8`.
   `10.0.0.0/8` contains the k3s pod CIDR `10.42.0.0/16`, so unless the Deployment sets
   `TRUSTED_CIDRS: ""` this grants a tenant to **any pod in the cluster**, credentials or
   not. This is a network-position bypass, not a "loopback" convenience.
5. **Dev/test API key** -- `Authorization: Bearer <DEV_API_KEY|TEST_API_KEY>` when
   `ENVIRONMENT=development|testing`

Public endpoints (no auth): `/`, `/health`, `/health/ready`, `/health/live`, `/metrics`, `/openapi.json`, `/docs`, `/redoc`.

### WebSocket endpoints use none of the above

`AuthMiddleware` and `TenantMiddleware` subclass Starlette's `BaseHTTPMiddleware`, which
short-circuits on any non-HTTP scope -- so **neither runs on a WebSocket handshake**.
`/api/realtime/analysis` and `/api/broadcast/subscribe` are authenticated by hand in
`resolve_ws_tenant`, which reads the `X-Tenant-Id` header and nothing else: no Bearer
introspection and no trusted-network bypass. The WS path is therefore *stricter* than the
REST path. The `?tenant_id=` query parameter was removed in `df581a5` (#34) and is gone.

## Project Structure

```
src/
  main.py                          # FastAPI app, lifespan, CORS, middleware, exception handlers
  config/
    settings.py                    # Pydantic Settings (all env vars)
  auth/
    middleware.py                  # Re-exports AuthMiddleware from coding-client
    tenant.py                     # Re-exports TenantMiddleware, require_tenant from coding-client
  routers/
    health.py                     # /api/health/* -- liveness, readiness, detailed
    realtime.py                   # /api/realtime/* -- analysis WebSocket + REST status/metrics
    realtime_multi_file.py        # Multi-file analysis handler (used by realtime.py)
    broadcast_websocket.py        # /api/broadcast/* -- broadcast subscription WebSocket
    websocket_health.py           # /api/ws-health/* -- connection/container health stats
    websocket_rate_limits.py      # /api/* -- rate-limit stats/violations/global-stats
  services/
    websocket_broadcast.py        # Container-scoped pub/sub with event filtering, priority, history
    websocket_connection_registry.py  # Connection tracking, limits per container, stale cleanup
    websocket_health_monitor.py   # Ping/pong heartbeat, latency, stale detection
    websocket_rate_limiter.py     # Token-bucket rate limiting per container
    websocket_metrics.py          # Connection, message, analysis, error metrics per container
    analysis_cache.py             # In-memory content-hash dedup cache
    performance_monitor.py        # Per-language operation duration tracking
    performance_optimizer.py      # Performance optimization logic
    metrics_service.py            # SQLite-backed test/quality recording
    file_service.py               # File operations
    multi_language_factory.py     # Language processor factory
    dependencies.py               # Service dependency injection
    languages/
      base.py                     # Base language processor interface
  clients/
    coding_analysis.py            # HTTP client for coding-analysis
    coding_containers.py          # HTTP client for coding-containers
  utils/
    websocket_utils.py            # WebSocketContainerContext, rate-limit helpers
    container_validation.py       # Tenant-aware container existence checks
    path_resolution.py            # Path traversal protection for container-scoped access
    parameter_validation.py       # Parameter validation helpers
  models/
    common.py                     # BaseResponse, ResponseStatus
    errors.py                     # Error response models
  responses.py                    # Additional response models
tests/
pyproject.toml
Dockerfile
docker-compose.yml
```

## API Reference

All REST routes require a valid tenant context (via `X-Tenant-Id` header or auth middleware) unless noted.

### Health (`/api/health` + `/health`)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Liveness probe. Returns `ok` or `degraded` with Redis status. |
| GET | `/api/health/live` | No | Kubernetes liveness -- always 200 if process is up. |
| GET | `/api/health/ready` | No | Kubernetes readiness -- 200 if Redis connected, 503 otherwise. |
| GET | `/api/health/detailed` | Yes | Checks Redis, coding-analysis, and coding-containers health. |

### Real-time Analysis (`/api/realtime`)

#### WebSocket

| Path | Description |
|------|-------------|
| `WS /api/realtime/analysis?connection_id=...&container_name=...` | Main analysis WebSocket. |

**Query params:** `connection_id` (required), `container_name` (required), `workspace_path` (optional).

**Client -> Server messages:**

| type | Description | Key fields |
|------|-------------|------------|
| `analyze_file` | Request single-file analysis | `file_path`, `content`, `change_type`, `cursor_position` |
| `multi_file_analysis` | Batch analysis with parallel processing | `file_paths`, `analysis_type`, `max_concurrent`, `options` |
| `watch_file` | Start watching a file path | `file_path` |
| `unwatch_file` | Stop watching a file path | `file_path` |
| `get_status` | Request connection status | -- |
| `pong` | Respond to health ping | `ping_id` |

**Server -> Client messages:**

| type | Description |
|------|-------------|
| `connection_established` | Sent on connect with `connection_id` |
| `analysis_result` | Parse result with entities, complexity, metrics |
| `analysis_error` | Analysis failure with error message |
| `multi_file_progress` | Progress updates during batch analysis |
| `multi_file_result` | Final batch results with aggregation |
| `watch_started` / `watch_stopped` | File watch confirmation |
| `status_update` | Current watched files, cache stats |
| `ping` | Health heartbeat (expect `pong` back) |
| `error` | General error |

#### REST

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/realtime/connections` | List active WebSocket connections, watched files, cache stats. |
| GET | `/api/realtime/websocket-api` | Structured WebSocket API documentation (message types, examples). |
| GET | `/api/realtime/metrics/{container_name}` | Detailed metrics for a container (connections, messages, analysis, errors). |
| GET | `/api/realtime/metrics` | Aggregated metrics across all containers. |

### Broadcast (`/api/broadcast`)

| Path | Description |
|------|-------------|
| `WS /api/broadcast/subscribe?connection_id=...&container_name=...` | Subscribe to container broadcast events. |

**Query params:** `connection_id` (required), `container_name` (required), `tenant_id` (required), `event_types` (optional, comma-separated), `tags` (optional, comma-separated), `priority` (optional: low/normal/high/critical), `replay_history` (optional bool), `replay_limit` (optional int).

**Client -> Server messages:** `replay_history`, `get_status`.

**Server -> Client messages:** `subscription_established`, `broadcast`, `broadcast_replay`, `history_replayed`, `status_update`, `error`.

### WebSocket Health (`/api/ws-health`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/ws-health/connection/{connection_id}` | Health status for a single connection (latency, pings, responsiveness). |
| GET | `/api/ws-health/container/{container_name}` | Aggregated health for all connections to a container. |
| GET | `/api/ws-health/container/{container_name}/connections` | List all connections for a container with health details. |
| GET | `/api/ws-health/global` | Global health stats across all connections and containers. |
| POST | `/api/ws-health/cleanup` | Trigger manual cleanup of stale connections. Optional `container_name` query param. |
| GET | `/api/ws-health/containers` | Health stats for every container with active connections. |

### Rate Limits (`/api`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/stats/{container_name}` | Rate-limit stats for a container (message rate, connection count, violations). |
| GET | `/api/stats` | Rate-limit stats for all containers. |
| GET | `/api/violations` | Recent rate-limit violations. Optional `container_name` and `limit` query params. |
| GET | `/api/global-stats` | Global rate-limiter stats with configuration. |

## Configuration

All settings are loaded from environment variables (see `src/config/settings.py`).

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_NAME` | `coding-realtime` | Service identifier |
| `VERSION` | `1.0.0` | Service version |
| `ENVIRONMENT` | `production` | `production` or `development` |
| `PORT` | `8694` | HTTP/WS listen port |
| `REDIS_URL` | `redis://code-graph-redis:6379/0` | Redis for pub/sub and rate-limit state |
| `DATABASE_URL` | -- | Optional PostgreSQL URL |
| `PGBOUNCER_URL` | -- | Optional PgBouncer URL |
| `ENABLE_AUTH` | `true` | Enable auth + tenant middleware |
| `DEFAULT_TENANT_ID` | `n8n-dev` | Fallback tenant |
| `ENABLE_DOCS` | `false` | Enable `/docs` and `/redoc` (auto-enabled in development) |
| `LOG_LEVEL` | `INFO` | Logging level |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `CORS_ORIGINS` | `["*"]` | Allowed CORS origins (Istio handles CORS in production) |
| `CODING_ANALYSIS_URL` | `http://coding-analysis:8688` | coding-analysis base URL |
| `CODING_CONTAINERS_URL` | `http://coding-containers:8691` | coding-containers base URL |
| `WS_PING_INTERVAL` | `30` | Seconds between WebSocket pings |
| `WS_PING_TIMEOUT` | `10` | Seconds to wait for pong |
| `WS_MAX_FAILED_PINGS` | `3` | Failed pings before marking stale |
| `WS_STALE_THRESHOLD` | `300` | Seconds idle before marking stale |
| `WS_MAX_CONNECTIONS_PER_CONTAINER` | `10` | Connection limit per container |
| `WS_MESSAGE_RATE_LIMIT` | `10.0` | Messages per second per container |
| `WS_RATE_LIMIT_BURST` | `20` | Burst allowance above rate limit |
| `OTLP_TRACES_ENDPOINT` | -- | OpenTelemetry OTLP endpoint |
| `OTLP_INSECURE` | `true` | Use insecure OTLP transport |
| `OTEL_SERVICE_NAME` | `coding-realtime` | OTLP service name |

## Service Dependencies

| Service | Purpose | Required |
|---------|---------|----------|
| **Redis** | Pub/sub for broadcasts, rate-limit state | Degraded without it (service still starts) |
| **coding-analysis** (8688) | File parsing, complexity scoring, entity extraction | Yes -- analysis requests fail without it |
| **coding-containers** (8691) | Container validation, file reads, tenant scoping | Yes -- all container-scoped operations fail without it |
| **coding-client** (library) | Auth middleware, tenant extraction | Yes (imported as Python package) |

## Development

```bash
# Install dependencies
uv sync

# Run locally (auth disabled, docs enabled)
ENVIRONMENT=development ENABLE_AUTH=false uvicorn main:app --host 0.0.0.0 --port 8694 --reload

# Lint and format
ruff check src/ tests/
ruff format src/ tests/

# Type check
ty check src/

# Run tests
pytest
```

## CI/CD

GitHub Actions workflow: `ruff + ty → build amd64 → smoke test → push`

Image pushed to `ghcr.io/nonagenticai/coding-realtime:sha-<commit>` and tagged `:latest` on success. Flux auto-deploys to K3s.

## Docker

The Dockerfile uses a two-stage build with Python 3.14 and uv. Production image runs as non-root `appuser` on port 8694.

```bash
docker compose up -d
```

## Security

Report vulnerabilities privately through the repository's Security tab — see
[`SECURITY.md`](SECURITY.md). Do not open a public issue for a security problem.

## Contributing

Issues and pull requests are welcome. See
[`CONTRIBUTING.md`](https://github.com/nonagenticai/.github/blob/main/CONTRIBUTING.md) for the
organisation-wide rules; setup, test and lint commands are in this README. Commits carry DCO
sign-off.

## License

Apache-2.0 — see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
