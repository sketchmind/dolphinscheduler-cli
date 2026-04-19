import json
from pathlib import Path

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
    result = runner.invoke(app, ["environment", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "environment.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["pageSize"] == 1
    assert payload["data"]["totalList"][0]["name"] == "prod"


def test_env_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["environment", "get", "prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.get"
    assert payload["resolved"]["environment"]["code"] == 7
    assert payload["data"]["name"] == "prod"


def test_env_selector_help_points_to_list() -> None:
    result = runner.invoke(app, ["environment", "get", "--help"])

    assert result.exit_code == 0
    assert "Use list to" in result.stdout
    assert "discover values" in result.stdout


def test_env_create_help_points_to_config_template() -> None:
    result = runner.invoke(app, ["environment", "create", "--help"])

    assert result.exit_code == 0
    assert "--config-file" in result.stdout
    assert "dsctl template environment" in result.stdout


def test_env_create_command_returns_created_environment() -> None:
    result = runner.invoke(
        app,
        [
            "environment",
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
    assert payload["action"] == "environment.create"
    assert payload["data"]["name"] == "qa"
    assert payload["data"]["workerGroups"] == ["default"]


def test_env_create_command_accepts_config_file() -> None:
    with runner.isolated_filesystem():
        Path("env.sh").write_text(
            "export JAVA_HOME=/opt/java\nexport PATH=$JAVA_HOME/bin:$PATH\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app,
            [
                "environment",
                "create",
                "--name",
                "qa",
                "--config-file",
                "env.sh",
            ],
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.create"
    assert payload["data"]["config"] == (
        "export JAVA_HOME=/opt/java\nexport PATH=$JAVA_HOME/bin:$PATH"
    )


def test_env_create_command_requires_config_source() -> None:
    result = runner.invoke(app, ["environment", "create", "--name", "qa"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass --config TEXT or --config-file PATH. Run `dsctl template environment` "
        "for an example shell/export config."
    )


def test_env_create_command_rejects_multiple_config_sources() -> None:
    with runner.isolated_filesystem():
        Path("env.sh").write_text("export JAVA_HOME=/opt/java\n", encoding="utf-8")

        result = runner.invoke(
            app,
            [
                "environment",
                "create",
                "--name",
                "qa",
                "--config",
                "export JAVA_HOME=/other",
                "--config-file",
                "env.sh",
            ],
        )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.create"
    assert payload["error"]["type"] == "user_input_error"
    assert "mutually exclusive" in payload["error"]["message"]


def test_env_update_command_can_clear_description_and_worker_groups() -> None:
    result = runner.invoke(
        app,
        [
            "environment",
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
    assert payload["action"] == "environment.update"
    assert payload["data"]["description"] is None
    assert payload["data"]["workerGroups"] == []


def test_env_update_command_accepts_config_file() -> None:
    with runner.isolated_filesystem():
        Path("env.sh").write_text(
            "export JAVA_HOME=/opt/java-21\nexport PATH=$JAVA_HOME/bin:$PATH\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app,
            [
                "environment",
                "update",
                "prod",
                "--config-file",
                "env.sh",
            ],
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.update"
    assert payload["data"]["config"] == (
        "export JAVA_HOME=/opt/java-21\nexport PATH=$JAVA_HOME/bin:$PATH"
    )


def test_env_update_command_requires_one_change_suggestion() -> None:
    result = runner.invoke(app, ["environment", "update", "prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.update"
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
            "environment",
            "update",
            "prod",
            "--config",
            '{"JAVA_HOME":"/opt/java-21"}',
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Verify --name, --config, and --worker-group values, then retry."
    )


def test_env_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["environment", "delete", "prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_env_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["environment", "delete", "prod", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "environment.delete"
    assert payload["data"]["deleted"] is True
