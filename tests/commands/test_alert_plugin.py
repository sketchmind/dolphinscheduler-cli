import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeAlertPlugin,
    FakeAlertPluginAdapter,
    FakePluginDefine,
    FakeProjectAdapter,
    FakeUiPluginAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()

ALERT_PLUGIN_PARAMS = json.dumps(
    [
        {
            "field": "url",
            "name": "url",
            "type": "input",
            "value": "https://hooks.example.test/ops",
        }
    ],
    ensure_ascii=False,
)


@pytest.fixture
def fake_ui_plugin_adapter() -> FakeUiPluginAdapter:
    return FakeUiPluginAdapter(
        plugin_defines=[
            FakePluginDefine(
                id=3,
                plugin_name_value="Slack",
                plugin_type_value="ALERT",
                plugin_params_value=ALERT_PLUGIN_PARAMS,
            )
        ]
    )


@pytest.fixture
def fake_alert_plugin_adapter() -> FakeAlertPluginAdapter:
    return FakeAlertPluginAdapter(
        alert_plugins=[
            FakeAlertPlugin(
                id=11,
                plugin_define_id_value=3,
                instance_name_value="slack-ops",
                plugin_instance_params_value=ALERT_PLUGIN_PARAMS,
                instance_type_value="ALERT",
                warning_type_value="ALL",
                alert_plugin_name_value="Slack",
            )
        ],
        plugin_names_by_id={3: "Slack"},
    )


@pytest.fixture(autouse=True)
def patch_alert_plugin_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            ui_plugin_adapter=fake_ui_plugin_adapter,
            alert_plugin_adapter=fake_alert_plugin_adapter,
            profile=make_profile(),
        ),
    )


def test_alert_plugin_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["alert-plugin", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.list"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["instanceName"] == "slack-ops"


def test_alert_plugin_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["alert-plugin", "get", "slack-ops"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.get"
    assert payload["resolved"]["alertPlugin"]["id"] == 11
    assert payload["data"]["alertPluginName"] == "Slack"


def test_alert_plugin_schema_command_returns_plugin_definition() -> None:
    result = runner.invoke(app, ["alert-plugin", "schema", "Slack"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.schema"
    assert payload["data"]["pluginName"] == "Slack"


def test_alert_plugin_create_command_reads_params_from_file(
    tmp_path: Path,
) -> None:
    params_file = tmp_path / "params.json"
    params_file.write_text(ALERT_PLUGIN_PARAMS, encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "alert-plugin",
            "create",
            "--name",
            "slack-nightly",
            "--plugin",
            "Slack",
            "--file",
            str(params_file),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.create"
    assert payload["data"]["instanceName"] == "slack-nightly"


def test_alert_plugin_update_command_returns_updated_payload() -> None:
    result = runner.invoke(
        app,
        [
            "alert-plugin",
            "update",
            "slack-ops",
            "--name",
            "slack-ops-renamed",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.update"
    assert payload["data"]["instanceName"] == "slack-ops-renamed"


def test_alert_plugin_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["alert-plugin", "delete", "slack-ops"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.delete"
    assert payload["error"]["type"] == "user_input_error"


def test_alert_plugin_test_command_returns_test_confirmation() -> None:
    result = runner.invoke(app, ["alert-plugin", "test", "slack-ops"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.test"
    assert payload["data"] == {"tested": True}


def test_alert_plugin_test_command_requires_live_alert_server(
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    fake_alert_plugin_adapter.test_send_errors_by_plugin_define_id[3] = ApiResultError(
        result_code=110017,
        result_message="alert server not exist",
    )

    result = runner.invoke(app, ["alert-plugin", "test", "slack-ops"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "alert-plugin.test"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Create or start at least one live alert server before retrying the "
        "alert-plugin test."
    )
