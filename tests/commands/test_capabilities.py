import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.template import parameter_syntax_index_data
from tests.support import normalize_cli_help

runner = CliRunner()

EXPECTED_VERSION_METADATA = [
    {
        "server_version": "3.3.2",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": False,
    },
    {
        "server_version": "3.4.0",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": False,
    },
    {
        "server_version": "3.4.1",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": True,
    },
]
EXPECTED_DS_CAPABILITIES = {
    "current_version": "3.4.1",
    "selected_version": "3.4.1",
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "tested": True,
    "supported_versions": ["3.3.2", "3.4.0", "3.4.1"],
    "versions": EXPECTED_VERSION_METADATA,
}


def test_capabilities_command_returns_surface_discovery() -> None:
    result = runner.invoke(app, ["capabilities"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["data"]["ds"] == EXPECTED_DS_CAPABILITIES
    assert payload["data"]["resources"]["top_level"] == [
        "version",
        "context",
        "doctor",
        "schema",
        "capabilities",
    ]
    assert payload["data"]["resources"]["groups"]["use"]["commands"] == [
        "project",
        "workflow",
    ]
    assert payload["data"]["resources"]["groups"]["enum"]["commands"] == [
        "names",
        "list",
    ]
    assert payload["data"]["resources"]["groups"]["task-type"]["commands"] == ["list"]
    assert payload["data"]["resources"]["groups"]["template"]["commands"] == [
        "workflow",
        "params",
        "environment",
        "cluster",
        "datasource",
        "task",
    ]
    assert payload["data"]["resources"]["groups"]["monitor"]["commands"] == [
        "health",
        "server",
        "database",
    ]
    assert payload["data"]["resources"]["groups"]["datasource"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "test",
    ]
    assert payload["data"]["errors"] == {
        "structured": True,
        "suggestion": True,
        "source": True,
        "source_kind": "remote",
        "source_system": "dolphinscheduler",
        "source_layers": ["result", "http"],
    }
    assert payload["data"]["output"] == {
        "standard_envelope": True,
        "formats": ["json", "table", "tsv"],
        "default_format": "json",
        "data_shape_metadata": True,
        "display_columns": True,
        "json_column_projection": True,
        "resolved_metadata": True,
        "warnings": True,
        "warning_details_alignment": True,
        "structured_errors": True,
    }
    assert payload["data"]["self_description"] == {
        "schema": True,
        "template": True,
        "capabilities": True,
        "command_invocation_source": "schema",
        "capabilities_scope": "feature_discovery",
    }
    assert payload["data"]["authoring"]["parameter_syntax"] == (
        parameter_syntax_index_data()
    )
    assert payload["data"]["authoring"]["environment_config_template"] is True
    assert payload["data"]["authoring"]["cluster_config_template"] is True
    assert payload["data"]["authoring"]["datasource_payload_templates"] is True
    assert (
        payload["data"]["authoring"]["datasource_template_types"]
        == (datasource_template_index_data()["supported_types"])
    )
    assert payload["data"]["enums"]["discovery"] is True
    assert "priority" in payload["data"]["enums"]["names"]


def test_capabilities_help_points_to_section_discovery() -> None:
    result = runner.invoke(app, ["capabilities", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "dsctl schema --command" in help_text
    assert "capabilities" in help_text
    assert "selection" in help_text
    assert "runtime" in help_text


def test_capabilities_command_honors_env_file_ds_version() -> None:
    with runner.isolated_filesystem():
        Path("cluster.env").write_text("DS_VERSION=3.3.2\n", encoding="utf-8")

        result = runner.invoke(app, ["--env-file", "cluster.env", "capabilities"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"]["current_version"] == "3.3.2"
    assert payload["data"]["ds"]["selected_version"] == "3.3.2"
    assert payload["data"]["ds"]["contract_version"] == "3.4.1"
    assert payload["data"]["ds"]["tested"] is False
    assert "priority" in payload["data"]["enums"]["names"]


def test_capabilities_command_returns_summary() -> None:
    result = runner.invoke(app, ["capabilities", "--summary"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["resolved"] == {"capabilities": {"view": "summary"}}
    assert "resources" in payload["data"]
    assert "runtime" in payload["data"]
    assert "authoring" in payload["data"]
    assert "parameter_syntax" not in payload["data"]["authoring"]


def test_capabilities_command_returns_section() -> None:
    result = runner.invoke(app, ["capabilities", "--section", "runtime"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["resolved"] == {
        "capabilities": {
            "view": "section",
            "section": "runtime",
        }
    }
    assert set(payload["data"]) == {"cli", "ds", "self_description", "runtime"}
    assert payload["data"]["runtime"]["task-instance"]["commands"] == [
        "list",
        "get",
        "watch",
        "sub-workflow",
        "log",
        "force-success",
        "savepoint",
        "stop",
    ]


def test_capabilities_command_rejects_conflicting_scope_options() -> None:
    result = runner.invoke(
        app,
        ["capabilities", "--summary", "--section", "runtime"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["error"]["type"] == "user_input_error"
    assert "mutually exclusive" in payload["error"]["message"]
