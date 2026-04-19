from pathlib import Path

import pytest

from dsctl.errors import UserInputError
from dsctl.services.enums import (
    list_enum_names_result,
    list_enum_result,
    supported_enum_choices,
)


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def test_list_enum_result_returns_generated_enum_members() -> None:
    result = list_enum_result("priority")
    data = _mapping(result.data)

    assert result.resolved == {
        "enum": {
            "requested": "priority",
            "name": "priority",
            "ds_version": "3.4.1",
        }
    }
    assert data == {
        "name": "priority",
        "module": "common.enums.priority",
        "class_name": "Priority",
        "ds_version": "3.4.1",
        "value_type": "string",
        "member_count": 5,
        "members": [
            {
                "name": "HIGHEST",
                "value": "HIGHEST",
                "attributes": {"code": 0, "descp": "highest"},
            },
            {
                "name": "HIGH",
                "value": "HIGH",
                "attributes": {"code": 1, "descp": "high"},
            },
            {
                "name": "MEDIUM",
                "value": "MEDIUM",
                "attributes": {"code": 2, "descp": "medium"},
            },
            {
                "name": "LOW",
                "value": "LOW",
                "attributes": {"code": 3, "descp": "low"},
            },
            {
                "name": "LOWEST",
                "value": "LOWEST",
                "attributes": {"code": 4, "descp": "lowest"},
            },
        ],
    }


def test_list_enum_names_result_returns_supported_enum_names() -> None:
    result = list_enum_names_result()
    data = result.data

    assert isinstance(data, list)

    assert result.resolved == {
        "enum": {
            "ds_version": "3.4.1",
            "count": len(data),
        }
    }
    assert {"name": "priority", "list_command": "dsctl enum list priority"} in data


def test_list_enum_result_resolves_class_name_alias() -> None:
    result = list_enum_result("ReleaseState")
    data = _mapping(result.data)

    assert result.resolved == {
        "enum": {
            "requested": "ReleaseState",
            "name": "release-state",
            "ds_version": "3.4.1",
        }
    }
    assert data["module"] == "common.enums.release_state"
    assert data["member_count"] == 2


def test_list_enum_result_uses_selected_version_with_compatible_contract(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text("DS_VERSION=3.3.2\n", encoding="utf-8")

    result = list_enum_result("priority", env_file=str(env_file))
    data = _mapping(result.data)

    assert _mapping(result.resolved["enum"])["ds_version"] == "3.3.2"
    assert data["ds_version"] == "3.3.2"
    assert data["name"] == "priority"


def test_list_enum_result_rejects_unknown_enum() -> None:
    with pytest.raises(UserInputError, match="Unsupported enum"):
        list_enum_result("missing-enum")


def test_supported_enum_choices_include_expected_names() -> None:
    choices = supported_enum_choices()

    assert "priority" in choices
    assert "resource-type" in choices
    assert "task-execution-status" in choices
