"""Tests for container_validation utilities."""

from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from utils.container_validation import (
    ContainerAccessDeniedException,
    ContainerNotFoundException,
    InvalidContainerStateException,
    containers_owned_by_tenant,
    handle_container_access_denied,
    handle_container_not_found,
    handle_invalid_container_state,
    validate_and_get_container_for_tenant,
)


def test_container_not_found_exception():
    exc = ContainerNotFoundException("my-container")
    assert exc.container_name == "my-container"
    assert "my-container" in exc.message


def test_container_not_found_custom_message():
    exc = ContainerNotFoundException("c1", message="Custom msg")
    assert exc.message == "Custom msg"


def test_container_access_denied_exception():
    exc = ContainerAccessDeniedException("my-container")
    assert exc.container_name == "my-container"
    assert "Access denied" in exc.message


def test_invalid_container_state_exception():
    exc = InvalidContainerStateException("c1", state="stopped")
    assert exc.container_name == "c1"
    assert exc.state == "stopped"
    assert "stopped" in exc.message


def test_handle_container_not_found():
    exc = handle_container_not_found("c1")
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 404
    assert cast(dict[str, Any], exc.detail)["error"] == "ContainerNotFound"


def test_handle_container_access_denied():
    exc = handle_container_access_denied("c1")
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 403
    assert cast(dict[str, Any], exc.detail)["error"] == "ContainerAccessDenied"


def test_handle_invalid_container_state():
    exc = handle_invalid_container_state("c1", "stopped")
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 409
    assert cast(dict[str, Any], exc.detail)["current_state"] == "stopped"


# The REAL body of coding-containers' `GET /api/containers/{name}`
# (`ContainerDetailResponse`): a `status: "success"` envelope wrapping the
# container. The old test faked a FLAT `{"name": ..., "status": "running"}`,
# which no version of coding-containers has ever returned — so it passed while
# production rejected every WebSocket subscription and 500'd every path
# resolution. Every case below asserts against this shape.
_ENVELOPE: dict[str, Any] = {
    "status": "success",
    "message": "Container details retrieved",
    "container": {
        "container_name": "c1",
        "status": "running",
        "codebase_path": "/workspace",
        "tenant_id": "t1",
    },
}


async def test_validate_container_for_tenant_success():
    mock_client = AsyncMock()
    mock_client.get_container = AsyncMock(return_value=_ENVELOPE)

    with patch("utils.container_validation._get_containers_client", return_value=mock_client):
        result = await validate_and_get_container_for_tenant("c1", tenant_id="t1")

    # The CONTAINER, not the envelope.
    assert result["container_name"] == "c1"
    assert result["codebase_path"] == "/workspace"
    mock_client.get_container.assert_called_once_with("c1", tenant_id="t1")


async def test_validate_returns_container_status_not_envelope_status():
    """The regression: `status` must be the container's, never the envelope's.

    `"success"` is not in `("running", "ready")`, so returning the envelope made
    `extract_container_from_websocket` reject EVERY broadcast subscription with
    403 CONTAINER_NOT_RUNNING.
    """
    mock_client = AsyncMock()
    mock_client.get_container = AsyncMock(return_value=_ENVELOPE)

    with patch("utils.container_validation._get_containers_client", return_value=mock_client):
        result = await validate_and_get_container_for_tenant("c1", tenant_id="t1")

    assert result["status"] == "running"
    assert result["status"] != _ENVELOPE["status"]


async def test_validate_rejects_a_body_with_no_container_payload():
    """False-positive control: unwrapping must not invent a container.

    A response that carries neither an envelope nor a flat container body is
    "not found" — passing it through is how a dict whose `status` means
    something else reached consumers in the first place.
    """
    mock_client = AsyncMock()
    mock_client.get_container = AsyncMock(return_value={"status": "success"})

    with patch("utils.container_validation._get_containers_client", return_value=mock_client):
        with pytest.raises(HTTPException) as excinfo:
            await validate_and_get_container_for_tenant("c1", tenant_id="t1")
    assert excinfo.value.status_code == 404


async def test_validate_accepts_a_flat_body_defensively():
    """A flat body (contract drift) is accepted, keyed on `container_name`."""
    flat = {"container_name": "c1", "status": "running", "codebase_path": "/workspace"}
    mock_client = AsyncMock()
    mock_client.get_container = AsyncMock(return_value=flat)

    with patch("utils.container_validation._get_containers_client", return_value=mock_client):
        result = await validate_and_get_container_for_tenant("c1", tenant_id="t1")
    assert result == flat


@pytest.mark.parametrize("tenant_id", ["", None])
async def test_containers_owned_by_tenant_fails_closed_without_a_tenant(tenant_id):
    """A missing tenant owns NOTHING and never reaches the lookup.

    ``validate_and_get_container_for_tenant(name, tenant_id=None)`` is the
    admin/unscoped lookup: it succeeds for any existing container. If the guard
    went, an empty/None tenant would be handed every container on the pod.
    """
    lookup = AsyncMock(return_value={"name": "box-a", "status": "running"})
    with patch("utils.container_validation.validate_and_get_container_for_tenant", lookup):
        owned = await containers_owned_by_tenant(["box-a", "box-b"], tenant_id)
    assert owned == set()
    lookup.assert_not_awaited()


async def test_containers_owned_by_tenant_with_a_tenant_scopes_the_lookup():
    """Positive control for the test above: a real tenant does reach the lookup."""
    lookup = AsyncMock(return_value={"name": "box-a", "status": "running"})
    with patch("utils.container_validation.validate_and_get_container_for_tenant", lookup):
        owned = await containers_owned_by_tenant(["box-a"], "tenant-a")
    assert owned == {"box-a"}
    lookup.assert_awaited_once_with("box-a", "tenant-a")
