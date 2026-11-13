"""Tests for common models."""

import pytest

from models.common import (
    BaseResponse,
    ComponentHealth,
    HealthStatus,
    PaginationParams,
    ResponseStatus,
    SuccessResponse,
)


def test_response_status_values():
    assert ResponseStatus.SUCCESS == "success"
    assert ResponseStatus.ERROR == "error"
    assert ResponseStatus.PENDING == "pending"
    assert ResponseStatus.RUNNING == "running"


def test_base_response_creation():
    resp = BaseResponse(status=ResponseStatus.SUCCESS, message="OK")
    assert resp.status == ResponseStatus.SUCCESS
    assert resp.message == "OK"
    assert resp.timestamp is not None
    # request_id is a full UUID string
    assert len(resp.request_id) == 36
    assert resp.request_id.count("-") == 4


def test_success_response_defaults():
    resp = SuccessResponse(message="Done")
    assert resp.status == ResponseStatus.SUCCESS
    assert resp.data is None


def test_success_response_with_data():
    resp = SuccessResponse(message="Done", data={"key": "value"})
    assert resp.data == {"key": "value"}


def test_pagination_params_defaults():
    params = PaginationParams()
    assert params.page == 1
    assert params.limit == 50


def test_pagination_params_validation():
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        PaginationParams(page=0)
    # Verify the error is specifically about the 'page' field being below minimum
    errors = exc_info.value.errors()
    assert len(errors) >= 1
    assert errors[0]["loc"] == ("page",)
    assert errors[0]["type"] == "greater_than_equal"


def test_pagination_params_limit_validation():
    from pydantic import ValidationError

    # limit above maximum (1000)
    with pytest.raises(ValidationError) as exc_info:
        PaginationParams(limit=1001)
    errors = exc_info.value.errors()
    assert errors[0]["loc"] == ("limit",)


def test_health_status_values():
    assert HealthStatus.HEALTHY == "healthy"
    assert HealthStatus.DEGRADED == "degraded"
    assert HealthStatus.UNHEALTHY == "unhealthy"
    assert HealthStatus.UNKNOWN == "unknown"


def test_component_health():
    comp = ComponentHealth(name="Redis", status=HealthStatus.HEALTHY)
    assert comp.name == "Redis"
    assert comp.status == HealthStatus.HEALTHY
    assert comp.last_check is not None
