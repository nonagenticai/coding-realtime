"""Tenant middleware re-export for coding-realtime."""

from coding_client.auth import TenantMiddleware, require_tenant

__all__ = ["TenantMiddleware", "require_tenant"]
