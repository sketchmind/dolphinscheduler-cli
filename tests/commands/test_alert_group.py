import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeAlertGroup,
    FakeAlertGroupAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


def _alert_groups() -> list[FakeAlertGroup]:
    return [
        FakeAlertGroup(
            id=21,
            group_name_value="ops",
            alert_instance_ids_value="7,8",
            description="ops alerts",
            create_user_id_value=1,
        ),
        FakeAlertGroup(
            id=22,
            group_name_value="etl",
            alert_instance_ids_value="9",
            description="etl alerts",
            create_user_id_value=1,
        ),
    ]


@pytest.fixture
def fake_alert_group_adapter() -> FakeAlertGroupAdapter:
    return FakeAlertGroupAdapter(alert_groups=_alert_groups())


@pytest.fixture(autouse=True)
def patch_alert_group_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_alert_group_adapter: FakeAlertGroupAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            alert_group_adapter=fake_alert_group_adapter,
            profile=make_profile(),
        ),
    )


def test_alert_group_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["alert-group", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "alert-group.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["groupName"] == "ops"


def test_alert_group_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["alert-group", "get", "ops"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.get"
    assert payload["resolved"]["alertGroup"]["id"] == 21
    assert payload["data"]["alertInstanceIds"] == "7,8"


def test_alert_group_get_help_points_to_list_for_selector() -> None:
    result = runner.invoke(app, ["alert-group", "get", "--help"])

    assert result.exit_code == 0
    assert "alert-group" in result.stdout
    assert "list" in result.stdout


def test_alert_group_create_command_returns_created_group() -> None:
    result = runner.invoke(
        app,
        [
            "alert-group",
            "create",
            "--name",
            "platform",
            "--instance-id",
            "7",
            "--instance-id",
            "8",
            "--description",
            "platform alerts",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.create"
    assert payload["data"]["groupName"] == "platform"
    assert payload["data"]["alertInstanceIds"] == "7,8"


def test_alert_group_create_help_points_to_alert_plugin_list() -> None:
    result = runner.invoke(app, ["alert-group", "create", "--help"])

    assert result.exit_code == 0
    assert "alert-plugin list" in result.stdout


def test_alert_group_update_command_returns_updated_group() -> None:
    result = runner.invoke(
        app,
        [
            "alert-group",
            "update",
            "ops",
            "--instance-id",
            "8",
            "--instance-id",
            "9",
            "--clear-description",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.update"
    assert payload["data"]["groupName"] == "ops"
    assert payload["data"]["alertInstanceIds"] == "8,9"
    assert payload["data"]["description"] is None


def test_alert_group_update_command_requires_one_change_suggestion() -> None:
    result = runner.invoke(app, ["alert-group", "update", "ops"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --name, --description, "
        "--clear-description, --instance-id, or --clear-instance-ids."
    )


def test_alert_group_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["alert-group", "delete", "ops"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_alert_group_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["alert-group", "delete", "ops", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.delete"
    assert payload["data"]["deleted"] is True


def test_alert_group_delete_command_rejects_default_group(
    fake_alert_group_adapter: FakeAlertGroupAdapter,
) -> None:
    fake_alert_group_adapter.alert_groups.append(
        FakeAlertGroup(
            id=1,
            group_name_value="default",
            alert_instance_ids_value="",
            description="default alerts",
            create_user_id_value=1,
        )
    )

    result = runner.invoke(app, ["alert-group", "delete", "default", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-group.delete"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Choose a non-default alert group; DolphinScheduler does not allow "
        "deleting the default group."
    )
