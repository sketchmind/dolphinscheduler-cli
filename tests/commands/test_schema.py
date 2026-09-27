import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.template import (
    cluster_config_template_capability_data,
    parameter_syntax_index_data,
    supported_task_template_types,
    task_template_metadata,
)

runner = CliRunner()


def test_schema_command_returns_machine_readable_cli_surface() -> None:
    result = runner.invoke(app, ["schema", "--full"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["data"]["schema_version"] == 3
    assert payload["data"]["view"] == "full"
    assert payload["data"]["cli"] == {"name": "dsctl", "version": "0.4.0"}
    command_names = [item["name"] for item in payload["data"]["commands"]]
    assert command_names[:18] == [
        "version",
        "doctor",
        "schema",
        "capabilities",
        "context",
        "config",
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
    expected_supported_types = list(supported_task_template_types())
    expected_typed_types = list(supported_typed_task_types())
    expected_generic_types = [
        task_type
        for task_type in expected_supported_types
        if task_type not in expected_typed_types
    ]
    assert payload["data"]["capabilities"]["templates"]["workflow"] == {
        "with_schedule_option": True,
        "raw_template_command": "dsctl template workflow --raw",
        "export_command_pattern": "dsctl workflow export WORKFLOW",
    }
    assert payload["data"]["capabilities"]["templates"]["workflow_patch"] == {
        "raw_template_command": "dsctl template workflow-patch --raw",
        "target_command_pattern": "dsctl workflow edit WORKFLOW --patch FILE",
    }
    assert payload["data"]["capabilities"]["templates"]["workflow_instance_patch"] == {
        "raw_template_command": "dsctl template workflow-instance-patch --raw",
        "target_command_pattern": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT "
            "--patch FILE"
        ),
        "file_source_command_pattern": (
            "dsctl workflow-instance export WORKFLOW_INSTANCE --project PROJECT"
        ),
        "file_target_command_pattern": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT "
            "--file FILE"
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
        "source_options": ["--config CONFIG", "--config-file CONFIG_FILE"],
        "target_command_patterns": [
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
        "surface_inventory_scope": "installed_cli_surface",
        "action_availability_command_pattern": ("dsctl capabilities --action ACTION"),
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
        "formats": ["json", "json-compact", "table", "tsv"],
        "default_format": "json",
        "format_option": "--format",
        "columns_option": "--columns",
        "compact_json": True,
        "compact_list_encoding": "columns_rows",
        "compact_list_contract": {
            "data_shape_flag": "compact_rows",
            "fields": ["columns", "rows"],
            "column_selection": "top_level_fields",
            "scope_paths": "decoded_logical_collections",
        },
        "json_encoding": "utf-8",
        "default_json_layout": "pretty",
        "error_channel": "stderr",
        "row_diagnostics_channel": "stderr",
        "success_fields": [
            "ok",
            "action",
            "resolved",
            "data",
        ],
        "optional_success_fields": ["warnings", "next_actions", "action_index"],
        "error_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "error",
        ],
        "ok_values": {
            "success": True,
            "error": False,
        },
        "warnings": {"type": "array", "items": "object", "presence": "nonempty"},
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
        "action_index": {
            "field": "action_index",
            "presence": "successful_applicable_json_responses_only",
            "max_indexed_targets": 100,
            "index_fields": [
                "scope",
                "target",
                "authorization",
                "eligibility",
                "groups",
                "schema_command_pattern",
                "group_command",
                "target_count",
                "indexed_target_count",
                "truncated",
            ],
            "target_fields": ["resource", "field"],
            "group_fields": [
                "targets",
                "read",
                "read_needs_input",
                "mutate",
                "mutate_needs_input",
            ],
            "all_targets_semantics": "all_returned_rows",
            "authorization": "not_evaluated",
            "eligibility": "row_facts_only",
            "row_output": False,
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
        "contract_version": "3.3.2",
        "support_level": "experimental",
        "tested": False,
    }


def test_schema_command_returns_group_scope() -> None:
    result = runner.invoke(app, ["schema", "--group", "task-instance"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "schema": {
            "view": "group",
            "group": "task-instance",
        },
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
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "schema": {
            "view": "command",
            "command": "task-instance.list",
        },
    }
    command = payload["data"]["command"]
    assert command["name"] == "list"
    assert command["action"] == "task-instance.list"


def test_schema_command_exposes_exact_version_action_capability(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=1.3.9\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "--env-file",
            "cluster.env",
            "schema",
            "--command",
            "task-type.list",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["capability"] == {
        "action": "task-type.list",
        "availability": "unsupported",
        "verification": "static",
        "constraint": (
            "This DolphinScheduler release predates live favourite task-type "
            "discovery introduced in 3.1.0."
        ),
    }


def test_322_clear_schema_matches_action_capability(isolated_cwd: Path) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=3.2.2\n",
        encoding="utf-8",
    )

    capabilities_result = runner.invoke(
        app,
        [
            "--env-file",
            "cluster.env",
            "capabilities",
            "--action",
            "project-worker-group.clear",
        ],
    )
    schema_result = runner.invoke(
        app,
        [
            "--env-file",
            "cluster.env",
            "schema",
            "--command",
            "project-worker-group.clear",
        ],
    )

    assert capabilities_result.exit_code == 0
    assert schema_result.exit_code == 0
    capabilities_payload = json.loads(capabilities_result.stdout)
    schema_payload = json.loads(schema_result.stdout)
    capability = capabilities_payload["data"]["capability"]
    assert schema_payload["data"]["command"]["action"] == ("project-worker-group.clear")
    assert schema_payload["data"]["capability"] == capability
    assert capability["availability"] == "limited"
    assert capability["verification"] == "static"
    assert "1402003" in capability["constraint"]


def test_schema_command_can_list_group_and_command_values() -> None:
    groups_result = runner.invoke(app, ["schema", "--list-groups"])

    assert groups_result.exit_code == 0
    groups_payload = json.loads(groups_result.stdout)
    assert groups_payload["resolved"]["schema"]["view"] == "groups"
    assert groups_payload["data"][0]["schema_command"] == "dsctl schema --group context"

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
        ["--format", "table", "schema", "--list-groups"],
    )

    assert result.exit_code == 0
    assert "name" in result.stdout
    assert "schema_command" in result.stdout
    assert "dsctl schema --group context" in result.stdout


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
        ["--format", "table", "schema", "--command", "datasource.create"],
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
            "--format",
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
        ["--format", "table", "schema", "--command", "template.datasource"],
    )

    assert result.exit_code == 0
    assert max(len(line) for line in result.stdout.splitlines()) < 240
    assert "choices=29 values; use discovery_command" in result.stdout
    assert "dsctl template datasource" in result.stdout
    assert "ALIYUN_SERVERLESS_SPARK" not in result.stdout


def test_schema_command_default_table_prioritizes_compact_invocation_fields() -> None:
    result = runner.invoke(
        app,
        ["--format", "table", "schema", "--command", "workflow.backfill"],
    )

    assert result.exit_code == 0
    assert max(len(line) for line in result.stdout.splitlines()) < 240
    header = result.stdout.splitlines()[0]
    assert "invocation" in header
    assert "description" not in header
    assert "dsctl workflow backfill WORKFLOW [OPTIONS]" in result.stdout
    assert "at_least_one_of" in result.stdout
    assert "--date | --start+--end" in result.stdout


@pytest.mark.parametrize(
    ("ds_version", "expected_shape"),
    [
        ("1.3.9", "comma-range"),
        ("3.0.6", "comma-range"),
        ("3.1.0", "json"),
        ("3.4.3", "json"),
    ],
)
def test_workflow_backfill_schema_projects_exact_date_selection(
    isolated_cwd: Path,
    ds_version: str,
    expected_shape: str,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        f"DS_VERSION={ds_version}\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "--env-file",
            "cluster.env",
            "schema",
            "--command",
            "workflow.backfill",
        ],
    )

    assert result.exit_code == 0
    command = json.loads(result.stdout)["data"]["command"]
    option_names = {option["name"] for option in command["options"]}
    if expected_shape == "json":
        assert "date" in option_names
        assert command["constraints"][0] == {
            "kind": "at_least_one_of",
            "alternatives": [["--date"], ["--start", "--end"]],
        }
        assert "unavailable_options" not in command
    else:
        assert "date" not in option_names
        assert command["constraints"] == [
            {"kind": "requires_all", "fields": ["--start", "--end"]}
        ]
        unavailable = [
            {
                "flag": "--date",
                "availability": "upstream_absent",
                "introduced_in": "3.1.0",
                "instruction": "use --start and --end",
            }
        ]
        if ds_version == "1.3.9":
            unavailable.append(
                {
                    "flag": "--environment-code",
                    "availability": "upstream_absent",
                    "introduced_in": "2.0.0",
                    "instruction": "omit",
                }
            )
        assert command["unavailable_options"] == unavailable


def test_unresolved_workflow_backfill_schema_keeps_union_with_unknown_boundary(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_API_URL=http://ds.example.test/api\nDS_API_TOKEN=secret\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        app,
        [
            "--env-file",
            "cluster.env",
            "schema",
            "--command",
            "workflow.backfill",
        ],
    )

    assert result.exit_code == 0
    command = json.loads(result.stdout)["data"]["command"]
    date_option = next(
        option for option in command["options"] if option["name"] == "date"
    )
    assert "Available on DS 3.1.0 and newer." in date_option["description"]
    assert command["version_specific_constraints"] == "unknown"
    assert "constraints" not in command
    assert "unavailable_options" not in command


def test_schema_command_table_exposes_runtime_value_resolution() -> None:
    result = runner.invoke(
        app,
        ["--format", "table", "schema", "--command", "workflow.run"],
    )

    assert result.exit_code == 0
    assert "resolve=flag>pref>medium" in result.stdout
    assert "resolve=flag>pref>none" in result.stdout


def test_schema_command_table_output_supports_contract_columns() -> None:
    result = runner.invoke(
        app,
        [
            "--format",
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
            "--format",
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
