import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectParameter,
    FakeProjectParameterAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_project_parameter_adapter() -> FakeProjectParameterAdapter:
    return FakeProjectParameterAdapter(
        project_parameters=[
            FakeProjectParameter(
                code=101,
                project_code_value=7,
                param_name_value="warehouse_db",
                param_value_value="jdbc:mysql://warehouse",
                param_data_type_value="VARCHAR",
            ),
            FakeProjectParameter(
                code=102,
                project_code_value=7,
                param_name_value="parallelism",
                param_value_value="4",
                param_data_type_value="INT",
            ),
        ]
    )


@pytest.fixture(autouse=True)
def patch_project_parameter_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            project_parameter_adapter=fake_project_parameter_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod"),
        ),
    )


def test_project_parameter_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["project-parameter", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-parameter.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["pageSize"] == 1
    assert payload["resolved"]["project"]["code"] == 7
    assert payload["resolved"]["project"]["source"] == "context"


def test_project_parameter_list_help_points_to_project_and_data_type_discovery() -> (
    None
):
    result = runner.invoke(app, ["project-parameter", "list", "--help"])

    assert result.exit_code == 0
    assert "project list" in result.stdout
    assert "enum list" in result.stdout
    assert "data-type" in result.stdout


def test_project_parameter_get_command_reports_not_found_suggestion() -> None:
    result = runner.invoke(app, ["project-parameter", "get", "missing"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-parameter.get"
    assert payload["error"]["type"] == "not_found"
    assert payload["error"]["suggestion"] == (
        "Retry with `project-parameter list` in the selected project to inspect "
        "available values, or pass the numeric code if known."
    )


def test_project_parameter_get_help_points_to_selected_project_list() -> None:
    result = runner.invoke(app, ["project-parameter", "get", "--help"])

    assert result.exit_code == 0
    assert "project-parameter" in result.stdout
    assert "list" in result.stdout
    assert "selected project" in result.stdout


def test_project_parameter_create_command_returns_created_parameter() -> None:
    result = runner.invoke(
        app,
        [
            "project-parameter",
            "create",
            "--name",
            "retry_limit",
            "--value",
            "3",
            "--data-type",
            "INT",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-parameter.create"
    assert payload["data"]["paramName"] == "retry_limit"
    assert payload["data"]["paramValue"] == "3"
    assert payload["data"]["paramDataType"] == "INT"


def test_project_parameter_update_command_allows_empty_value() -> None:
    result = runner.invoke(
        app,
        ["project-parameter", "update", "warehouse_db", "--value", ""],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-parameter.update"
    assert payload["data"]["paramName"] == "warehouse_db"
    assert payload["data"]["paramValue"] == ""


def test_project_parameter_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["project-parameter", "delete", "warehouse_db"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-parameter.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."
