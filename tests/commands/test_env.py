import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeEnvironment,
    FakeEnvironmentAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_environment_adapter() -> FakeEnvironmentAdapter:
    return FakeEnvironmentAdapter(
        environments=[
            FakeEnvironment(code=7, name="prod", description="prod env"),
            FakeEnvironment(code=9, name="test", description="test env"),
        ]
    )


@pytest.fixture(autouse=True)
def patch_env_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_environment_adapter: FakeEnvironmentAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            environment_adapter=fake_environment_adapter,
            profile=make_profile(),
        ),
    )


def test_env_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["env", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "env.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["pageSize"] == 1
    assert payload["data"]["totalList"][0]["name"] == "prod"


def test_env_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["env", "get", "prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.get"
    assert payload["resolved"]["environment"]["code"] == 7
    assert payload["data"]["name"] == "prod"


def test_env_create_command_returns_created_environment() -> None:
    result = runner.invoke(
        app,
        [
            "env",
            "create",
            "--name",
            "qa",
            "--config",
            '{"JAVA_HOME":"/opt/java"}',
            "--worker-group",
            "default",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.create"
    assert payload["data"]["name"] == "qa"
    assert payload["data"]["workerGroups"] == ["default"]


def test_env_update_command_can_clear_description_and_worker_groups() -> None:
    result = runner.invoke(
        app,
        [
            "env",
            "update",
            "prod",
            "--config",
            '{"JAVA_HOME":"/opt/java-21"}',
            "--clear-description",
            "--clear-worker-groups",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.update"
    assert payload["data"]["description"] is None
    assert payload["data"]["workerGroups"] == []


def test_env_update_command_requires_one_change_suggestion() -> None:
    result = runner.invoke(app, ["env", "update", "prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --name, --config, "
        "--description, --clear-description, --worker-group, or "
        "--clear-worker-groups."
    )


def test_env_update_command_reports_upstream_input_suggestion(
    fake_environment_adapter: FakeEnvironmentAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_update(
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: list[str],
    ) -> FakeEnvironment:
        del code, name, config, description, worker_groups
        raise ApiResultError(
            result_code=130015,
            result_message="workerGroups invalid",
        )

    monkeypatch.setattr(fake_environment_adapter, "update", reject_update)

    result = runner.invoke(
        app,
        [
            "env",
            "update",
            "prod",
            "--config",
            '{"JAVA_HOME":"/opt/java-21"}',
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Verify --name, --config, and --worker-group values, then retry."
    )


def test_env_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["env", "delete", "prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_env_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["env", "delete", "prod", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "env.delete"
    assert payload["data"]["deleted"] is True
