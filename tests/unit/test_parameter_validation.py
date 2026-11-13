"""Tests for parameter_validation utilities."""

from pathlib import Path

import pytest
from fastapi import HTTPException

from utils.parameter_validation import validate_path_parameter, validate_project_root


def test_validate_path_valid():
    result = validate_path_parameter("/some/path")
    assert isinstance(result, Path)
    assert str(result) == "/some/path"


def test_validate_path_none_allowed():
    assert validate_path_parameter(None, allow_none=True) is None


def test_validate_path_none_not_allowed():
    with pytest.raises(HTTPException) as exc_info:
        validate_path_parameter(None, allow_none=False)
    assert exc_info.value.status_code == 400


def test_validate_path_null_like_values():
    for value in ["null", "none", "nil", "undefined", "na", "n/a"]:
        with pytest.raises(HTTPException) as exc_info:
            validate_path_parameter(value)
        assert exc_info.value.status_code == 400


def test_validate_path_empty_string():
    assert validate_path_parameter("", allow_none=True) is None


def test_validate_path_empty_not_allowed():
    with pytest.raises(HTTPException) as exc_info:
        validate_path_parameter("", allow_none=False)
    assert exc_info.value.status_code == 400


def test_validate_project_root_valid():
    result = validate_project_root("/some/project")
    assert isinstance(result, Path)


def test_validate_project_root_none():
    assert validate_project_root(None, allow_none=True) is None


def test_validate_project_root_none_not_allowed():
    with pytest.raises(HTTPException) as exc_info:
        validate_project_root(None, allow_none=False)
    assert exc_info.value.status_code == 400
