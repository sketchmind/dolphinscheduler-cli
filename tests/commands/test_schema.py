import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.template import (
    cluster_config_template_capability_data,
    parameter_syntax_index_data,
    task_template_metadata,
)
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
        "environment",
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
    assert payload["data"]["capabilities"]["templates"]["workflow"] == {
        "with_schedule_option": True,
        "raw_template_command": "dsctl template workflow --raw",
    }
    assert payload["data"]["capabilities"]["templates"]["workflow_patch"] == {
        "raw_template_command": "dsctl template workflow-patch --raw",
        "target_command": "dsctl workflow edit WORKFLOW --patch FILE",
    }
    assert payload["data"]["capabilities"]["templates"]["workflow_instance_patch"] == {
        "raw_template_command": "dsctl template workflow-instance-patch --raw",
        "target_command": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --patch FILE"
        ),
    }
    assert payload["data"]["capabilities"]["templates"]["task"] == {
        "supported_types": expected_supported_types,
        "typed_types": expected_typed_types,
        "generic_types": expected_generic_types,
        "templates_by_type": task_template_metadata(),
        "index_command": "dsctl template task",
        "summary_command_pattern": "dsctl task-type get TYPE",
        "schema_command_pattern": "dsctl task-type schema TYPE",
        "raw_template_command_pattern": "dsctl template task TYPE --raw",
    }
    assert payload["data"]["capabilities"]["templates"]["datasource"] == (
        datasource_template_index_data()
    )
    assert payload["data"]["capabilities"]["templates"]["parameters"] == (
        parameter_syntax_index_data()
    )
    assert payload["data"]["capabilities"]["templates"]["environment"] == {
        "command": "dsctl template environment",
        "source_options": ["--config TEXT", "--config-file PATH"],
        "target_commands": [
            "dsctl environment create --name NAME --config-file env.sh",
            "dsctl environment update ENVIRONMENT --config-file env.sh",
        ],
    }
    assert payload["data"]["capabilities"]["templates"]["cluster"] == (
        cluster_config_template_capability_data()
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
        "json_column_projection": True,
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


def test_schema_command_can_list_group_and_command_values() -> None:
    groups_result = runner.invoke(app, ["schema", "--list-groups"])

    assert groups_result.exit_code == 0
    groups_payload = json.loads(groups_result.stdout)
    assert groups_payload["resolved"]["schema"]["view"] == "groups"
    assert groups_payload["data"][0]["schema_command"] == "dsctl schema --group use"

    commands_result = runner.invoke(app, ["schema", "--list-commands"])

    assert commands_result.exit_code == 0
    commands_payload = json.loads(commands_result.stdout)
    assert commands_payload["resolved"]["schema"]["view"] == "commands"
    assert commands_payload["data"][0]["group"] is None
    assert any(
        item["action"] == "datasource.create"
        and item["schema_command"] == "dsctl schema --command datasource.create"
        for item in commands_payload["data"]
    )


def test_schema_command_list_values_render_as_table_rows() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "schema", "--list-groups"],
    )

    assert result.exit_code == 0
    assert "name" in result.stdout
    assert "schema_command" in result.stdout
    assert "dsctl schema --group use" in result.stdout


def test_schema_command_datasource_create_uses_payload_reference() -> None:
    result = runner.invoke(app, ["schema", "--command", "datasource.create"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    commands = payload["data"]["commands"]
    datasource_create = commands[0]["commands"][0]
    assert "payload_schema" not in datasource_create
    assert datasource_create["payload"]["template_command"] == (
        "dsctl template datasource --type MYSQL"
    )
    assert datasource_create["payload"]["template_command_pattern"] == (
        "dsctl template datasource --type TYPE"
    )
    assert datasource_create["payload"]["template_json_path"] == "data.json"
    assert datasource_create["payload"]["template_discovery_command"] == (
        "dsctl template datasource"
    )


def test_schema_command_datasource_create_table_output_is_compact() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "schema", "--command", "datasource.create"],
    )

    assert result.exit_code == 0
    assert max(len(line) for line in result.stdout.splitlines()) < 240
    assert "dsctl template datasource --type MYSQL" in result.stdout
    assert "template_discovery_command" in result.stdout
    assert "additional_fields_by_type" not in result.stdout


def test_schema_command_long_choices_render_as_discovery_hint() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "schema", "--command", "template.datasource"],
    )

    assert result.exit_code == 0
    assert max(len(line) for line in result.stdout.splitlines()) < 240
    assert "choices=29 values; use discovery_command" in result.stdout
    assert "dsctl template datasource" in result.stdout
    assert "ALIYUN_SERVERLESS_SPARK" not in result.stdout


def test_schema_command_table_output_supports_contract_columns() -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            "table",
            "--columns",
            "flag,description,discovery_command",
            "schema",
            "--command",
            "environment.create",
        ],
    )

    assert result.exit_code == 0
    assert "--config" in result.stdout
    assert "dsctl template environment" in result.stdout
    assert "Unknown display column" not in result.stdout


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
