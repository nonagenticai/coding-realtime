# Operation ID Plan: coding-realtime

## Files to Edit
- `src/main.py` (2 app-level routes)
- `src/routers/health.py` (3 routes)
- `src/routers/websocket_health.py` (6 routes)
- `src/routers/websocket_rate_limits.py` (4 routes)
- `src/routers/realtime.py` (5 routes: 1 WebSocket + 4 GET)
- `src/routers/broadcast_websocket.py` (1 WebSocket route)

## Mapping Table

### main.py (app-level, no prefix)

| Method | Effective Path | Function | operation_id |
|--------|---------------|----------|--------------|
| GET | `/health` | `health` | `health_check` |
| GET | `/` | `root` | `get_root` |

### health.py (prefix `/api/health`)

| Method | Effective Path | Function | operation_id |
|--------|---------------|----------|--------------|
| GET | `/api/health/live` | `liveness` | `health_live` |
| GET | `/api/health/ready` | `readiness` | `health_ready` |
| GET | `/api/health/detailed` | `detailed` | `health_detailed` |

### websocket_health.py (prefix `/api/ws-health`)

| Method | Effective Path | Function | operation_id |
|--------|---------------|----------|--------------|
| GET | `/api/ws-health/connection/{connection_id}` | `get_connection_health` | `get_connection_health` |
| GET | `/api/ws-health/container/{container_name}` | `get_container_health` | `get_ws_container_health` |
| GET | `/api/ws-health/container/{container_name}/connections` | `get_container_connections` | `get_container_connections` |
| GET | `/api/ws-health/global` | `get_global_health` | `get_global_health` |
| POST | `/api/ws-health/cleanup` | `cleanup_stale_connections` | `cleanup_stale_connections` |
| GET | `/api/ws-health/containers` | `get_all_container_health` | `list_container_health` |

> `get_ws_container_health` (not the originally-planned `get_container_health`)
> is the shipped id — renamed in `src/routers/websocket_health.py` by commit
> `c9ac601` on 2026-04-10 and never back-ported into this doc. The `ws_`
> qualifier disambiguates it from the sibling `list_container_health` and from
> the same-named route in coding-containers. Because `operationId` is part of
> the published OpenAPI contract and drives method names in any generated
> client, renaming it back would be a breaking change for consumers of the
> schema. This doc follows the route, not the other way round.
>
> (The hand-written `coding-client` SDK is unaffected either way: its
> `CodingRealtimeClient.get_container_health` is keyed on the URL path
> `/api/ws-health/container/{name}`, which has not changed.)

### websocket_rate_limits.py (prefix `/api`)

| Method | Effective Path | Function | operation_id |
|--------|---------------|----------|--------------|
| GET | `/api/stats/{container_name}` | `get_container_rate_limit_stats` | `get_container_rate_limit_stats` |
| GET | `/api/stats` | `get_all_rate_limit_stats` | `list_rate_limit_stats` |
| GET | `/api/violations` | `get_rate_limit_violations` | `list_rate_limit_violations` |
| GET | `/api/global-stats` | `get_global_rate_limit_stats` | `get_global_rate_limit_stats` |

### realtime.py (prefix `/api/realtime`)

| Method | Effective Path | Function | operation_id |
|--------|---------------|----------|--------------|
| WS | `/api/realtime/analysis` | `websocket_realtime_analysis` | `ws_realtime_analysis` |
| GET | `/api/realtime/connections` | `get_active_connections` | `list_active_connections` |
| GET | `/api/realtime/websocket-api` | `get_websocket_api_documentation` | `get_websocket_api_docs` |
| GET | `/api/realtime/metrics/{container_name}` | `get_container_websocket_metrics` | `get_container_ws_metrics` |
| GET | `/api/realtime/metrics` | `get_global_websocket_metrics` | `get_global_ws_metrics` |

### broadcast_websocket.py (prefix `/api/broadcast`)

| Method | Effective Path | Function | operation_id |
|--------|---------------|----------|--------------|
| WS | `/api/broadcast/subscribe` | `websocket_broadcast_subscribe` | `ws_broadcast_subscribe` |

## Uniqueness Check

All 21 operation_ids are unique:
`health_check`, `get_root`, `health_live`, `health_ready`, `health_detailed`,
`get_connection_health`, `get_ws_container_health`, `get_container_connections`,
`get_global_health`, `cleanup_stale_connections`, `list_container_health`,
`get_container_rate_limit_stats`, `list_rate_limit_stats`,
`list_rate_limit_violations`, `get_global_rate_limit_stats`,
`ws_realtime_analysis`, `list_active_connections`, `get_websocket_api_docs`,
`get_container_ws_metrics`, `get_global_ws_metrics`, `ws_broadcast_subscribe`

## Execution Steps
```bash
cd /Users/mini1/Movies/code_graph_helper/coding-realtime && git pull --rebase
```
1. Add `operation_id=` to every `@router` / `@app` decorator in the files listed above
2. For WebSocket routes, add `operation_id=` to `@router.websocket()`
3. Validate uniqueness (no duplicates across all files)
4. `pre-commit run --all-files`
5. `pytest`
6. `git commit` and `push`
