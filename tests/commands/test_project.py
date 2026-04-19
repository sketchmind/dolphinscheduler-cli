import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import FakeProject, FakeProjectAdapter, fake_service_runtime
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod", description="daily jobs"),
            FakeProject(code=9, name="streaming", description="stream jobs"),
        ]
    )


@pytest.fixture(autouse=True)
def patch_project_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            profile=make_profile(),
        ),
    )


def test_project_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["project", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "project.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["pageSize"] == 1
    assert payload["data"]["totalList"][0]["name"] == "etl-prod"


def test_project_list_command_materializes_one_page_with_all() -> None:
    result = runner.invoke(app, ["project", "list", "--page-size", "1", "--all"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.list"
    assert payload["resolved"]["all"] is True
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalPage"] == 1
    assert payload["data"]["pageSize"] == 2
    assert payload["data"]["currentPage"] == 1
    assert [item["name"] for item in payload["data"]["totalList"]] == [
        "etl-prod",
        "streaming",
    ]


def test_project_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["project", "get", "etl-prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.get"
    assert payload["resolved"]["project"]["code"] == 7
    assert payload["data"]["name"] == "etl-prod"


def test_project_get_help_points_to_list_for_selector() -> None:
    result = runner.invoke(app, ["project", "get", "--help"])

    assert result.exit_code == 0
    assert "project" in result.stdout
    assert "list" in result.stdout


def test_project_get_command_reports_not_found_suggestion() -> None:
    result = runner.invoke(app, ["project", "get", "missing"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.get"
    assert payload["error"]["type"] == "not_found"
    assert payload["error"]["suggestion"] == (
        "Retry with `project list` to inspect available values, or pass the "
        "numeric code if known."
    )


def test_project_get_command_reports_ambiguous_resolution_suggestion(
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    fake_project_adapter.projects.append(FakeProject(code=11, name="etl-prod"))

    result = runner.invoke(app, ["project", "get", "etl-prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.get"
    assert payload["error"]["type"] == "resolution_error"
    assert payload["error"]["suggestion"] == (
        "Retry with one explicit numeric code from the matching results: 7, 11."
    )


def test_project_create_command_returns_created_project() -> None:
    result = runner.invoke(
        app,
        ["project", "create", "--name", "demo", "--description", "test project"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.create"
    assert payload["data"]["name"] == "demo"
    assert payload["data"]["description"] == "test project"


def test_project_update_command_updates_description() -> None:
    result = runner.invoke(
        app,
        ["project", "update", "etl-prod", "--description", "updated"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.update"
    assert payload["data"]["code"] == 7
    assert payload["data"]["description"] == "updated"


def test_project_update_command_rejects_conflicting_description_flags() -> None:
    result = runner.invoke(
        app,
        [
            "project",
            "update",
            "etl-prod",
            "--description",
            "updated",
            "--clear-description",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["action"] == "project.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Use either --description VALUE or --clear-description, not both."
    )


def test_project_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["project", "delete", "etl-prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["action"] == "project.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_project_delete_command_reports_deleted_project() -> None:
    result = runner.invoke(app, ["project", "delete", "etl-prod", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project.delete"
    assert payload["data"]["deleted"] is True
    assert payload["data"]["project"]["code"] == 7
