import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.services.template import parameter_syntax_index_data, task_template_metadata
from dsctl.upstream import upstream_default_task_types

runner = CliRunner()


def test_schema_command_returns_machine_readable_cli_surface() -> None:
    result = runner.invoke(app, ["schema"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["data"]["schema_version"] == 1
    assert payload["data"]["cli"] == {"name": "dsctl", "version": "0.1.0"}
    command_names = [item["name"] for item in payload["data"]["commands"]]
    assert command_names[:18] == [
        "version",
        "context",
        "doctor",
        "schema",
        "capabilities",
        "use",
        "enum",
        "lint",
        "env",
        "cluster",
        "datasource",
        "namespace",
        "resource",
        "queue",
        "worker-group",
        "task-group",
        "alert-plugin",
        "alert-group",
    ]
    assert "task-type" in command_names
    expected_supported_types = list(upstream_default_task_types())
    expected_typed_types = list(supported_typed_task_types())
    expected_generic_types = [
        task_type
        for task_type in expected_supported_types
        if task_type not in expected_typed_types
    ]
    assert payload["data"]["capabilities"]["templates"]["task"] == {
        "supported_types": expected_supported_types,
        "typed_types": expected_typed_types,
        "generic_types": expected_generic_types,
        "templates_by_type": task_template_metadata(),
    }
    assert payload["data"]["capabilities"]["templates"]["parameters"] == (
        parameter_syntax_index_data()
    )
    assert payload["data"]["capabilities"]["self_description"] == {
        "schema": True,
        "template": True,
        "capabilities": True,
        "command_invocation_source": "schema",
        "capabilities_scope": "feature_discovery",
    }
    assert payload["data"]["errors"] == {
        "fields": ["type", "message", "details", "source", "suggestion"],
        "source": {
            "field": "error.source",
            "kind": "remote",
            "system": "dolphinscheduler",
            "layers": {
                "result": {
                    "fields": [
                        "kind",
                        "system",
                        "layer",
                        "result_code",
                        "result_message",
                    ]
                },
                "http": {
                    "fields": [
                        "kind",
                        "system",
                        "layer",
                        "status_code",
                    ]
                },
            },
        },
    }
    assert payload["data"]["output"] == {
        "formats": ["json", "table", "tsv"],
        "default_format": "json",
        "format_option": "--output-format",
        "columns_option": "--columns",
        "success_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
        ],
        "error_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
            "error",
        ],
        "ok_values": {
            "success": True,
            "error": False,
        },
        "warning_details_aligned": True,
        "data_shape_metadata": True,
    }
    assert payload["data"]["capabilities"]["monitor"] == {
        "health": True,
        "database": True,
        "server_types": ["master", "worker", "alert-server"],
    }


def test_schema_command_honors_env_file_ds_version() -> None:
    with runner.isolated_filesystem():
        Path("cluster.env").write_text("DS_VERSION=3.3.2\n", encoding="utf-8")

        result = runner.invoke(app, ["--env-file", "cluster.env", "schema"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["capabilities"]["ds"]["selected_version"] == "3.3.2"
    assert payload["data"]["capabilities"]["ds"]["current_version"] == "3.3.2"
    assert payload["data"]["capabilities"]["ds"]["tested"] is False


def test_schema_command_returns_group_scope() -> None:
    result = runner.invoke(app, ["schema", "--group", "task-instance"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["resolved"] == {
        "schema": {
            "view": "group",
            "group": "task-instance",
        }
    }
    assert "capabilities" not in payload["data"]
    commands = payload["data"]["commands"]
    assert len(commands) == 1
    assert commands[0]["name"] == "task-instance"


def test_schema_command_returns_command_scope() -> None:
    result = runner.invoke(app, ["schema", "--command", "task-instance.list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["resolved"] == {
        "schema": {
            "view": "command",
            "command": "task-instance.list",
        }
    }
    commands = payload["data"]["commands"]
    assert len(commands) == 1
    assert commands[0]["name"] == "task-instance"
    assert [item["action"] for item in commands[0]["commands"]] == [
        "task-instance.list"
    ]


def test_schema_command_rejects_conflicting_scope_options() -> None:
    result = runner.invoke(
        app,
        ["schema", "--group", "workflow", "--command", "workflow.run"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["error"]["type"] == "user_input_error"
    assert "mutually exclusive" in payload["error"]["message"]
