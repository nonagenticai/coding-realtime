"""ConnectionManager — tracks WebSocket connections and per-container state."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect, WebSocketState

from config.settings import get_settings
from services.websocket_metrics import get_websocket_metrics_collector

logger = logging.getLogger("routers.realtime")

# Field separator for composite cache keys. \x1f (ASCII unit separator) cannot
# occur in a tenant id, container name or POSIX path, so the key is unambiguous
# (unlike ":" or "/", which would let "a:b"+"c" collide with "a"+"b:c").
_KEY_SEP = "\x1f"

# Application close code (4000-4999 is reserved for applications by RFC 6455
# section 7.4.2) sent to a socket whose slot a same-tenant reconnect took over.
SUPERSEDED_CLOSE_CODE = 4001
SUPERSEDED_CLOSE_REASON = "superseded"


def analysis_scope_key(
    file_path: str,
    *,
    tenant_id: str | None = None,
    container_name: str | None = None,
) -> str:
    """Build the cache key for per-file analysis state.

    Keyed by **(tenant, container, path)** — never by path alone. A path such as
    ``src/main.py`` is near-certain to be shared between tenants, so a
    path-only key on a module-level singleton makes one tenant's analysis
    request depend on another tenant's: the second tenant's content hash
    "matches" and its request is treated as a no-op. Container names are already
    tenant-validated upstream, but the tenant component is kept as
    defence-in-depth so isolation does not silently rest on container-name
    uniqueness.

    ``None`` components are encoded distinctly so an unscoped legacy call can
    never alias a scoped one.
    """
    return _KEY_SEP.join(
        (
            tenant_id if tenant_id is not None else "\x00",
            container_name if container_name is not None else "\x00",
            file_path,
        )
    )


def _lru_put(store: dict[str, Any], key: str, value: Any, max_entries: int) -> None:
    """Insert/refresh *key* (most-recently-used) and evict past the cap.

    Without a cap these maps are unbounded: they are only ever written, never
    pruned on disconnect (entries are shared by every connection of a
    container), so a long-lived pod accumulates one entry per distinct
    (tenant, container, path) forever.

    Recency is carried by plain ``dict`` insertion order (guaranteed since 3.7)
    and only ever uses ``pop``/``__setitem__``/``iter`` — so tests (and any
    caller) may assign a bare ``dict`` to these attributes. Deliberately does
    NOT require ``OrderedDict.move_to_end``.
    """
    store.pop(key, None)
    store[key] = value
    while max_entries > 0 and len(store) > max_entries:
        del store[next(iter(store))]


def _lru_touch(store: dict[str, Any], key: str) -> None:
    """Mark *key* most-recently-used without changing its value.

    Same duck-typed contract as :func:`_lru_put`: pop + re-insert, never
    ``move_to_end``.
    """
    if key in store:
        store[key] = store.pop(key)


class ConnectionManager:
    """Manages WebSocket connections for real-time analysis."""

    def __init__(self) -> None:
        self.active_connections: dict[str, WebSocket] = {}
        self.connection_containers: dict[str, str] = {}  # connection_id -> container_name
        self.connection_start_times: dict[str, float] = {}  # connection_id -> start timestamp
        # connection_id -> tenant that opened it (the handshake's verified tenant).
        # Read by every tenant-facing listing so one tenant never sees another's
        # connection ids or watched paths.
        self.connection_tenants: dict[str, str | None] = {}
        # registry key -> the connection_id the CLIENT chose. Every map in this
        # class is keyed by connection_key(), never by the raw client id; this is
        # how the client's own id is echoed back to it (and only to it).
        self.connection_public_ids: dict[str, str] = {}
        # registry key -> the id WebSocketHealthMonitor tracks this socket under.
        # The monitor is fed by WebSocketContainerContext with the REGISTRY's id
        # (``{container}_{uuid7}``), which is neither the client's id nor our key,
        # so a pong reported under either of those matched nothing, ever.
        self.connection_monitor_ids: dict[str, str] = {}
        self.file_watchers: dict[str, set[str]] = {}
        self.connection_files: dict[str, set[str]] = {}
        # Both maps are keyed by analysis_scope_key(), NOT by raw file path, and
        # are LRU-bounded (see _lru_put).
        self.max_tracked_files = get_settings().REALTIME_MAX_TRACKED_FILES
        self.last_analysis: dict[str, dict[str, Any]] = {}
        self.content_hashes: dict[str, str] = {}

        # Container-aware connection tracking
        self.container_connections: dict[
            str, set[str]
        ] = {}  # container_name -> set of connection_ids
        self.connection_metadata: dict[
            str, dict[str, Any]
        ] = {}  # connection_id -> metadata (timestamps, etc.)

    @staticmethod
    def connection_key(connection_id: str, tenant_id: str | None) -> str:
        """Registry key for a connection: **(tenant, client connection_id)**.

        ``connection_id`` is chosen by the CLIENT (a query parameter), so two
        tenants can present the same one. Keyed by the raw id, tenant B
        reusing tenant A's id replaced A's socket and A's tenant record — and
        B's disconnect then tore down A's slot. Namespacing by the handshake's
        verified tenant makes that impossible while keeping same-tenant
        reconnect semantics exactly as before (same tenant + same id = same
        slot, the newer socket replaces the older).

        ``None`` (internal/legacy callers with no tenant) keeps the raw id. The
        WS route never passes ``None``: it closes a handshake without a verified
        tenant before it gets here.
        """
        if tenant_id is None:
            return connection_id
        return _KEY_SEP.join((tenant_id, connection_id))

    def monitor_connection_id(self, key: str) -> str | None:
        """The health monitor's id for registry *key*, if the socket was registered there."""
        return self.connection_monitor_ids.get(key)

    def public_connection_id(self, key: str) -> str:
        """The client-chosen id behind registry *key* (what the client may see)."""
        return self.connection_public_ids.get(key, key)

    async def connect(
        self,
        websocket: WebSocket,
        connection_id: str,
        container_name: str,
        tenant_id: str | None = None,
        monitor_id: str | None = None,
    ) -> str:
        """Register (and, if still needed, accept) a WebSocket connection.

        Returns the registry key (see :meth:`connection_key`) under which the
        connection is tracked. Callers MUST use that key — not the raw client
        ``connection_id`` — for every later call on this manager.
        """
        # WebSocketContainerContext has usually accepted already. A second
        # accept is not idempotent in Starlette: it raises RuntimeError
        # ("Expected ASGI message 'websocket.send' or 'websocket.close', but got
        # 'websocket.accept'"), which killed EVERY real /api/realtime/analysis
        # handshake before it registered. Only accept a socket that is not open.
        if getattr(websocket, "application_state", None) != WebSocketState.CONNECTED:
            await websocket.accept()
        public_id = connection_id
        connection_id = self.connection_key(public_id, tenant_id)
        previous = self.active_connections.get(connection_id)
        if previous is not None and previous is not websocket:
            # Same tenant + same client id = same slot; the newer socket wins.
            # Tear the OLD socket's slot down now (its container set, its file
            # watches, its metrics close), so that when the old socket's route
            # loop later ends and calls disconnect(key, old_socket), the slot
            # already belongs to the new socket and that call is a no-op.
            self.disconnect(connection_id)
            # Then CLOSE the old socket. Left open, its route loop kept running
            # on the shared key: its requests were answered on the NEW socket
            # and its watch_file calls landed in the new slot. The close makes
            # the old client see why it was dropped, and the route loop exits
            # because it no longer owns the slot (see owns()).
            try:
                await previous.close(code=SUPERSEDED_CLOSE_CODE, reason=SUPERSEDED_CLOSE_REASON)
            except (RuntimeError, ConnectionError, OSError, WebSocketDisconnect) as exc:
                # Already closed/closing: nothing left to tell that client.
                # Starlette turns a transport OSError on send into
                # WebSocketDisconnect(1006); letting it escape would drop the
                # RECONNECTING client after the old slot was already torn down.
                logger.debug("Superseded socket for %s already closed: %s", connection_id, exc)
        self.connection_public_ids[connection_id] = public_id
        if monitor_id:
            self.connection_monitor_ids[connection_id] = monitor_id
        else:
            self.connection_monitor_ids.pop(connection_id, None)
        self.active_connections[connection_id] = websocket
        self.connection_tenants[connection_id] = tenant_id
        self.connection_files[connection_id] = set[Any]()
        self.connection_containers[connection_id] = container_name
        self.connection_start_times[connection_id] = time.time()

        if container_name not in self.container_connections:
            self.container_connections[container_name] = set()
        self.container_connections[container_name].add(connection_id)

        self.connection_metadata[connection_id] = {
            "container_name": container_name,
            "tenant_id": tenant_id,
            "connected_at": datetime.now(UTC).isoformat(),
            "connection_start_time": time.time(),
        }

        metrics_collector = get_websocket_metrics_collector()
        await metrics_collector.track_connection_opened(container_name, connection_id)

        logger.info(
            "Real-time analysis connection established: %s (container: %s)",
            connection_id,
            container_name,
        )
        await self.send_message(
            connection_id,
            {
                "type": "connection_established",
                "timestamp": datetime.now(UTC).isoformat(),
                "connection_id": public_id,
                "container_name": container_name,
                "message": "Real-time analysis WebSocket connected",
            },
        )
        return connection_id

    def owns(self, connection_id: str, websocket: WebSocket) -> bool:
        """True while slot *connection_id* still holds *websocket*.

        False once a same-tenant reconnect has taken the slot over: the socket
        is superseded and must stop acting on the shared key.
        """
        return self.active_connections.get(connection_id) is websocket

    def connections_for_tenant(self, tenant_id: str) -> list[str]:
        """Return the active connection ids opened by *tenant_id* — and only those.

        A connection with no recorded tenant belongs to nobody who can ask, so it
        is never returned (fail closed).
        """
        return [
            cid
            for cid in self.active_connections
            if tenant_id and self.connection_tenants.get(cid) == tenant_id
        ]

    def disconnect(self, connection_id: str, websocket: WebSocket | None = None) -> None:
        """Remove the connection in slot *connection_id*.

        Pass *websocket* (the socket whose lifetime is ending) from every caller
        that owns one. A same-tenant reconnect reuses the slot, so the OLD
        socket's cleanup runs AFTER the new socket registered; without the
        identity check it deleted the NEW socket's slot and monitor id, the new
        client vanished from every listing and its pongs matched nothing. When
        the slot no longer holds *websocket*, this is a no-op: the slot was
        already torn down for that socket when it was replaced (see connect()).

        ``websocket=None`` is an unconditional removal, for callers that own the
        slot rather than a socket.
        """
        if websocket is not None and self.active_connections.get(connection_id) is not websocket:
            logger.debug(
                "Ignoring disconnect of a superseded socket for %s: slot was reused",
                connection_id,
            )
            return
        duration = None
        if connection_id in self.connection_start_times:
            duration = time.time() - self.connection_start_times[connection_id]
            del self.connection_start_times[connection_id]

        container_name = self.connection_containers.get(connection_id)

        if connection_id in self.connection_files:
            for file_path in self.connection_files[connection_id]:
                if file_path in self.file_watchers:
                    self.file_watchers[file_path].discard(connection_id)
                    if not self.file_watchers[file_path]:
                        del self.file_watchers[file_path]
            del self.connection_files[connection_id]
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
        if connection_id in self.connection_containers:
            del self.connection_containers[connection_id]
        self.connection_tenants.pop(connection_id, None)
        self.connection_public_ids.pop(connection_id, None)
        self.connection_monitor_ids.pop(connection_id, None)

        if container_name and container_name in self.container_connections:
            self.container_connections[container_name].discard(connection_id)
            if not self.container_connections[container_name]:
                del self.container_connections[container_name]

        if connection_id in self.connection_metadata:
            del self.connection_metadata[connection_id]

        if container_name:
            metrics_collector = get_websocket_metrics_collector()
            import asyncio

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None:
                asyncio.create_task(
                    metrics_collector.track_connection_closed(
                        container_name, connection_id, duration
                    )
                )

            logger.info(
                "Real-time analysis connection closed: %s (container: %s)",
                connection_id,
                container_name,
            )
        else:
            logger.info("Real-time analysis connection closed: %s", connection_id)

    async def send_message(self, connection_id: str, message: dict[str, Any]) -> None:
        """Send a message to a specific connection with container context."""
        if connection_id in self.active_connections:
            container_name = self.connection_containers.get(connection_id)
            if container_name and "container_name" not in message:
                message["container_name"] = container_name

            websocket = self.active_connections[connection_id]
            try:
                await websocket.send_text(json.dumps(message))

                if container_name:
                    metrics_collector = get_websocket_metrics_collector()
                    await metrics_collector.track_message_sent(container_name)
            except (ConnectionError, OSError) as e:
                logger.error(
                    "Failed to send message to %s (container: %s): %s",
                    connection_id,
                    container_name or "unknown",
                    e,
                )

                if container_name:
                    metrics_collector = get_websocket_metrics_collector()
                    await metrics_collector.track_error(container_name, "send_error", str(e))

                self.disconnect(connection_id, websocket)

    async def broadcast_to_file_watchers(self, file_path: str, message: dict[str, Any]) -> None:
        """Broadcast a message to all connections watching a file.

        .. warning::
           ``file_watchers`` is still keyed by raw path, so this would fan a
           message out to watchers of the SAME path in a DIFFERENT tenant. It is
           currently unreachable (no production caller) — scoping it needs the
           watch key split from the user-visible path reported by
           ``get_status``/``connections``, which is a separate change. Do not
           wire a caller to it before that is done.
        """
        if file_path in self.file_watchers:
            for connection_id in self.file_watchers[file_path].copy():
                await self.send_message(connection_id, message)

    def get_container_name(self, connection_id: str) -> str | None:
        """Get the container name for a connection."""
        return self.connection_containers.get(connection_id)

    def get_active_connections_for_container(self, container_name: str) -> list[str]:
        """Get all active connection IDs for a specific container."""
        return list(self.container_connections.get(container_name, set()))

    def get_connection_count_for_container(self, container_name: str) -> int:
        """Get the number of active connections for a specific container."""
        return len(self.container_connections.get(container_name, set()))

    def get_connection_metadata(self, connection_id: str) -> dict[str, Any] | None:
        """Get metadata for a specific connection."""
        return self.connection_metadata.get(connection_id)

    def get_all_container_names(self) -> list[str]:
        """Get all container names with active connections."""
        return list(self.container_connections.keys())

    def watch_file(self, connection_id: str, file_path: str) -> None:
        """Add a file to the watch list for a connection."""
        if connection_id not in self.connection_files:
            self.connection_files[connection_id] = set[Any]()
        self.connection_files[connection_id].add(file_path)
        if file_path not in self.file_watchers:
            self.file_watchers[file_path] = set[Any]()
        self.file_watchers[file_path].add(connection_id)
        container_name = self.get_container_name(connection_id)
        logger.debug(
            "Connection %s (container: %s) watching file: %s",
            connection_id,
            container_name,
            file_path,
        )

    def unwatch_file(self, connection_id: str, file_path: str) -> None:
        """Remove a file from the watch list for a connection."""
        if connection_id in self.connection_files:
            self.connection_files[connection_id].discard(file_path)
        if file_path in self.file_watchers:
            self.file_watchers[file_path].discard(connection_id)
            if not self.file_watchers[file_path]:
                del self.file_watchers[file_path]
        container_name = self.get_container_name(connection_id)
        logger.debug(
            "Connection %s (container: %s) stopped watching file: %s",
            connection_id,
            container_name,
            file_path,
        )

    def has_content_changed(
        self,
        file_path: str,
        content: str,
        *,
        tenant_id: str | None = None,
        container_name: str | None = None,
    ) -> bool:
        """Check if file content changed since this scope's last analysis.

        The scope is (tenant, container, path) — see :func:`analysis_scope_key`.
        Keying by path alone made this module-level singleton a cross-tenant
        data dependency.
        """
        content_hash = hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()
        key = analysis_scope_key(file_path, tenant_id=tenant_id, container_name=container_name)
        previous = self.content_hashes.get(key)
        if previous == content_hash:
            _lru_touch(self.content_hashes, key)
            return False
        _lru_put(self.content_hashes, key, content_hash, self.max_tracked_files)
        return True

    def record_analysis(
        self,
        file_path: str,
        response: dict[str, Any],
        *,
        tenant_id: str | None = None,
        container_name: str | None = None,
    ) -> None:
        """Remember the last analysis response for a (tenant, container, path)."""
        key = analysis_scope_key(file_path, tenant_id=tenant_id, container_name=container_name)
        _lru_put(self.last_analysis, key, response, self.max_tracked_files)

    def get_last_analysis(
        self,
        file_path: str,
        *,
        tenant_id: str | None = None,
        container_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the last analysis response for a (tenant, container, path)."""
        key = analysis_scope_key(file_path, tenant_id=tenant_id, container_name=container_name)
        response = self.last_analysis.get(key)
        if response is not None:
            _lru_touch(self.last_analysis, key)
        return response


# Module-level singleton used by submodules and the public router.
connection_manager = ConnectionManager()
