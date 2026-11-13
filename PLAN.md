# coding-realtime — Extraction Plan

## Purpose
WebSocket and real-time communication service. Owns WebSocket connections, broadcast, metrics collection, and health monitoring.

## Port: 8694

---

## Endpoints

| Route Prefix | Source Router | Source Endpoint File |
|---|---|---|
| `/api/ws-health` | `websocket_health` | `src/app/api/endpoints/websocket_health.py` |
| `/api` (ws rate limits) | `websocket_rate_limits` | `src/app/api/endpoints/websocket_rate_limits.py` |
| `/api/realtime` | `realtime` | `src/app/api/endpoints/realtime.py` |
| `/api/broadcast` | `broadcast_websocket` | `src/app/api/endpoints/broadcast_websocket.py` |

## Endpoint Sub-modules

| Sub-package | Files | Notes |
|---|---|---|
| `realtime_handlers/` | varies | WebSocket handler implementations |
| `realtime_multi_file/` | varies | Multi-file real-time operations |

## Services to Extract

| Monolith Service | Target Location |
|---|---|
| `services/websocket_broadcast_service.py` | `src/services/websocket_broadcast.py` |
| `services/websocket_connection_registry.py` | `src/services/websocket_connection_registry.py` |
| `services/websocket_health_monitor.py` | `src/services/websocket_health_monitor.py` |
| `services/metrics_service.py` | `src/services/metrics_service.py` |
| `services/performance_monitor.py` | `src/services/performance_monitor.py` |
| `core/performance_optimizer.py` | `src/services/performance_optimizer.py` |

## Duplication Resolution
- **Performance monitoring**: This service owns both `performance_monitor.py` AND `core/performance_optimizer.py`. Other services call this service's metrics API instead of doing local monitoring.

## Dependencies
- coding-client (shared models, auth)
- websockets (WebSocket protocol)
- redis (pub/sub for broadcast)
