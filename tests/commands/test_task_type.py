import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProjectAdapter,
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_task_type_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            task_type_adapter=FakeTaskTypeAdapter(
                task_types=[
                    FakeTaskType(
                        task_type_value="SHELL",
                        is_collection_value=True,
                        task_category_value="Universal",
                    ),
                    FakeTaskType(
                        task_type_value="CUSTOM_PLUGIN",
                        is_collection_value=False,
                        task_category_value="Universal",
                    ),
                ]
            ),
        ),
    )


def test_task_type_list_command_returns_remote_discovery_payload() -> None:
    result = runner.invoke(app, ["task-type", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.list"
    assert payload["resolved"] == {"source": "favourite/taskTypes"}
    assert payload["data"]["count"] == 2
    assert payload["data"]["taskTypes"][0] == {
        "taskType": "SHELL",
        "isCollection": True,
        "taskCategory": "Universal",
    }
    assert payload["data"]["taskTypesByCategory"] == {
        "Universal": ["SHELL", "CUSTOM_PLUGIN"]
    }
    assert "SPARK" in payload["data"]["cliCoverage"]["genericTaskTemplateTypes"]
    assert payload["data"]["cliCoverage"]["untemplatedTaskTypes"] == ["CUSTOM_PLUGIN"]


def test_task_type_help_distinguishes_live_catalog_from_template_catalog() -> None:
    group_result = runner.invoke(app, ["task-type", "--help"])
    list_result = runner.invoke(app, ["task-type", "list", "--help"])

    assert group_result.exit_code == 0
    assert list_result.exit_code == 0
    assert "local task authoring contracts" in group_result.stdout
    assert "schema" in group_result.stdout
    assert "CLI authoring" in list_result.stdout
    assert "coverage" in list_result.stdout


def test_task_type_get_command_returns_local_authoring_summary() -> None:
    result = runner.invoke(app, ["task-type", "get", "sql"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.get"
    assert payload["resolved"] == {"task_type": "SQL"}
    assert payload["data"]["task_type"] == "SQL"
    assert payload["data"]["schema_command"] == "dsctl task-type schema SQL"
    assert payload["data"]["raw_template_command"] == "dsctl template task SQL --raw"
    assert "task_params.sql" in payload["data"]["required_paths"]


def test_task_type_schema_command_returns_field_contract() -> None:
    result = runner.invoke(app, ["task-type", "schema", "SQL"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.schema"
    field_paths = [field["path"] for field in payload["data"]["fields"]]
    assert "task_params.sqlType" in field_paths
    assert payload["data"]["state_rules"][1]["when"] == "task_params.sqlType == 1"
    assert payload["data"]["schema"]["x-dsctl"]["template_command"] == (
        "dsctl template task SQL"
    )


def test_task_type_schema_command_returns_choice_sources_for_fields() -> None:
    result = runner.invoke(app, ["task-type", "schema", "SHELL"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    resource_field = next(
        field
        for field in payload["data"]["fields"]
        if field["path"] == "task_params.resourceList[].resourceName"
    )
    assert resource_field["choice_source"] == "dsctl resource list --dir DIR"
    assert resource_field["related_commands"] == [
        "dsctl resource list",
        "dsctl resource upload --file FILE",
        "dsctl resource view RESOURCE",
    ]
