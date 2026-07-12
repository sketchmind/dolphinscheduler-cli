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
    result = runner.invoke(app, ["schema", "--full"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["data"]["schema_version"] == 2
    assert payload["data"]["view"] == "full"
    assert payload["data"]["cli"] == {"name": "dsctl", "version": "0.2.0"}
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
        "export_command": "dsctl workflow export WORKFLOW",
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
        "file_source_command": ("dsctl workflow-instance export WORKFLOW_INSTANCE"),
        "file_target_command": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --file FILE"
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
        "compact_option": "--compact",
        "compact_json": True,
        "json_encoding": "utf-8",
        "default_json_layout": "pretty",
        "error_channel": "stderr",
        "row_diagnostics_channel": "stderr",
        "success_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
        ],
        "optional_success_fields": ["next_actions"],
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
        "next_actions": {
            "field": "next_actions",
            "presence": "successful_applicable_json_responses_only",
            "max_items": 3,
            "ordered": True,
            "item_fields": ["action", "command", "mutates"],
            "command_kind": "complete_shell_invocation",
            "authorization": "advisory",
            "row_output": False,
            "preserves_env_file": True,
        },
    }
    assert payload["data"]["capabilities"]["monitor"] == {
        "health": True,
        "database": True,
        "server_types": ["master", "worker", "alert-server"],
    }


def test_schema_command_honors_env_file_ds_version(isolated_cwd: Path) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=3.3.2\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["--env-file", "cluster.env", "schema"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"] == {
        "selected_version": "3.3.2",
        "contract_version": "3.4.1",
        "support_level": "experimental",
        "tested": False,
    }


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
    assert payload["data"]["group"]["name"] == "task-instance"
    assert any(
        item["action"] == "task-instance.list" for item in payload["data"]["actions"]
    )


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
    command = payload["data"]["command"]
    assert command["name"] == "list"
    assert command["action"] == "task-instance.list"


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
    datasource_create = payload["data"]["command"]
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


def test_schema_command_expanded_scope_keeps_derived_table_contract_rows() -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            "table",
            "schema",
            "--command",
            "datasource.create",
            "--full",
        ],
    )

    assert result.exit_code == 0
    assert "description" in result.stdout.splitlines()[0]
    assert "--file" in result.stdout
    assert "template_discovery_command" in result.stdout


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


def test_schema_command_default_table_prioritizes_compact_invocation_fields() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "schema", "--command", "workflow.backfill"],
    )

    assert result.exit_code == 0
    assert max(len(line) for line in result.stdout.splitlines()) < 240
    header = result.stdout.splitlines()[0]
    assert "invocation" in header
    assert "description" not in header
    assert "dsctl workflow backfill [WORKFLOW] [OPTIONS]" in result.stdout
    assert "at_least_one_of" in result.stdout
    assert "--date | --start+--end" in result.stdout


def test_schema_command_table_exposes_runtime_value_resolution() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "schema", "--command", "workflow.run"],
    )

    assert result.exit_code == 0
    assert "resolve=flag>pref>medium" in result.stdout
    assert "resolve=flag>pref>none" in result.stdout


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


def test_schema_command_table_output_exposes_numeric_minimum() -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            "table",
            "schema",
            "--command",
            "workflow-instance.watch",
        ],
    )

    assert result.exit_code == 0
    assert "minimum=1" in result.stdout
    assert "minimum=0" in result.stdout


def test_schema_command_rejects_conflicting_scope_options() -> None:
    result = runner.invoke(
        app,
        ["schema", "--group", "workflow", "--command", "workflow.run"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "schema"
    assert payload["error"]["type"] == "user_input_error"
    assert "mutually exclusive" in payload["error"]["message"]


def test_schema_command_rejects_full_list_view() -> None:
    result = runner.invoke(app, ["schema", "--list-commands", "--full"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "schema"
    assert payload["error"]["type"] == "user_input_error"
    assert "--full cannot be combined" in payload["error"]["message"]
