from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeProjectAdapter,
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ApiTransportError
from dsctl.services import runtime as runtime_service
from dsctl.services import task_type as task_type_service


def _install_task_type_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeTaskTypeAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            task_type_adapter=adapter,
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_list_task_types_result_returns_remote_payload_and_cli_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value="SHELL",
                is_collection_value=True,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="REMOTE_SHELL",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="CUSTOM_PLUGIN",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="SUB_WORKFLOW",
                is_collection_value=True,
                task_category_value="Logic",
            ),
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    result = task_type_service.list_task_types_result()
    data = _mapping(result.data)
    task_types = _sequence(data["taskTypes"])
    coverage = _mapping(data["cliCoverage"])

    assert result.resolved == {"source": "favourite/taskTypes"}
    assert data["count"] == 4
    assert list(task_types) == [
        {
            "taskType": "SHELL",
            "isCollection": True,
            "taskCategory": "Universal",
        },
        {
            "taskType": "REMOTE_SHELL",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "CUSTOM_PLUGIN",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "SUB_WORKFLOW",
            "isCollection": True,
            "taskCategory": "Logic",
        },
    ]
    assert data["taskTypesByCategory"] == {
        "Universal": ["SHELL", "REMOTE_SHELL", "CUSTOM_PLUGIN"],
        "Logic": ["SUB_WORKFLOW"],
    }
    assert "REMOTESHELL" in _sequence(coverage["taskTemplateTypes"])
    assert "SPARK" in _sequence(coverage["genericTaskTemplateTypes"])
    assert "CUSTOM_PLUGIN" in _sequence(coverage["untemplatedTaskTypes"])
    assert "REMOTE_SHELL" not in _sequence(coverage["untemplatedTaskTypes"])


def test_list_task_types_result_rejects_missing_required_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value=None,
                task_category_value="Universal",
            )
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    with pytest.raises(ApiTransportError, match="missing required field 'taskType'"):
        task_type_service.list_task_types_result()


def test_task_type_summary_result_describes_local_authoring_contract() -> None:
    result = task_type_service.task_type_summary_result("sql")
    data = _mapping(result.data)

    assert result.resolved == {"task_type": "SQL"}
    assert data["task_type"] == "SQL"
    assert data["template_command"] == "dsctl template task SQL"
    assert data["raw_template_command"] == "dsctl template task SQL --raw"
    assert "task_params.sql" in _sequence(data["required_paths"])


def test_task_type_schema_result_describes_fields_and_state_rules() -> None:
    result = task_type_service.task_type_schema_result("SQL")
    data = _mapping(result.data)
    fields = _sequence(data["fields"])
    state_rules = _sequence(data["state_rules"])
    schema = _mapping(data["schema"])

    assert result.resolved == {"task_type": "SQL"}
    assert any(_mapping(field)["path"] == "task_params.sqlType" for field in fields)
    assert _mapping(state_rules[1])["when"] == "task_params.sqlType == 1"
    assert _mapping(schema["x-dsctl"])["raw_template_command"] == (
        "dsctl template task SQL --raw"
    )


def test_task_type_schema_result_exposes_field_discovery_commands() -> None:
    sql_result = task_type_service.task_type_schema_result("SQL")
    shell_result = task_type_service.task_type_schema_result("SHELL")
    conditions_result = task_type_service.task_type_schema_result("CONDITIONS")

    sql_data = _mapping(sql_result.data)
    shell_data = _mapping(shell_result.data)
    conditions_data = _mapping(conditions_result.data)

    sql_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(sql_data["fields"])
    }
    shell_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(shell_data["fields"])
    }
    conditions_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(conditions_data["fields"])
    }

    assert sql_fields["task_params.groupId"]["choice_source"] == (
        "dsctl alert-group list"
    )
    assert "dsctl alert-group create --name NAME --instance-id ID" in _sequence(
        sql_fields["task_params.groupId"]["related_commands"]
    )

    resource_field = shell_fields["task_params.resourceList[].resourceName"]
    assert resource_field["choice_source"] == "dsctl resource list --dir DIR"
    assert "dsctl resource upload --file FILE" in _sequence(
        resource_field["related_commands"]
    )

    project_field = conditions_fields[
        "task_params.dependence.dependTaskList[].dependItemList[].projectCode"
    ]
    assert project_field["choice_source"] == "dsctl project list"
    assert conditions_fields[
        "task_params.dependence.dependTaskList[].dependItemList[].cycle"
    ]["choices"] == ["hour", "day", "week", "month"]
    assert "today" in _sequence(
        conditions_fields[
            "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
        ]["choices"]
    )

    shell_choice_sources = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(shell_data["choice_sources"])
    }
    assert shell_choice_sources["task_params.resourceList[].resourceName"] == {
        "path": "task_params.resourceList[].resourceName",
        "command": "dsctl resource list --dir DIR",
        "value": "fullName",
        "description": (
            "Run `dsctl resource list --dir DIR` and use `fullName` as "
            "task_params.resourceList[].resourceName; upload the file first "
            "when it is missing."
        ),
        "related_commands": [
            "dsctl resource list",
            "dsctl resource upload --file FILE",
            "dsctl resource view RESOURCE",
        ],
    }
