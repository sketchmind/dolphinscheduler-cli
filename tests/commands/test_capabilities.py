import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.template import parameter_syntax_index_data
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    get_version_support,
    supported_version_metadata,
)
from tests.support import normalize_cli_help

runner = CliRunner()

EXPECTED_VERSION_METADATA = list(supported_version_metadata())
EXPECTED_DS_CAPABILITIES = {
    "current_version": "3.4.1",
    "selected_version": "3.4.1",
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "tested": True,
    "supported_version_count": len(SUPPORTED_VERSIONS),
    "supported_versions": list(SUPPORTED_VERSIONS),
    "versions": EXPECTED_VERSION_METADATA,
    "catalog": {
        "selected_version": "3.4.1",
        "action_count": 181,
        "availability_counts": {
            "supported": 180,
            "limited": 1,
            "unsupported": 0,
        },
        "verification_counts": {
            "static": 29,
            "contract_tested": 145,
            "live_smoke": 7,
            "live_full": 0,
        },
    },
}


@pytest.mark.parametrize(
    ("version", "availability"),
    [
        ("3.2.0", "supported"),
        ("3.2.1", "supported"),
        ("3.2.2", "supported"),
        ("3.3.1", "limited"),
        ("3.3.2", "limited"),
        ("3.4.0", "limited"),
        ("3.4.1", "limited"),
        ("3.4.2", "supported"),
        ("3.4.3", "supported"),
    ],
)
def test_execute_task_discovery_distinguishes_wire_from_runtime_support(
    monkeypatch: pytest.MonkeyPatch, version: str, availability: str
) -> None:
    monkeypatch.setenv("DS_VERSION", version)
    action = "workflow-instance.execute-task"
    result = runner.invoke(app, ["capabilities", "--action", action])
    assert result.exit_code == 0
    capability = json.loads(result.stdout)["data"]["capability"]
    assert capability["availability"] == availability
    if availability == "limited":
        assert "EXECUTE_TASK" in capability["constraint"]

    result = runner.invoke(app, ["schema", "--command", action])
    assert result.exit_code == 0
    schema = json.loads(result.stdout)["data"]
    assert schema["capability"] == capability


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
@pytest.mark.parametrize(
    "action",
    ["project.list", "project.get", "workflow.list", "workflow.get"],
)
def test_every_exact_profile_discovers_definition_reads_as_executable(
    isolated_cwd: Path,
    ds_version: str,
    action: str,
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
            "--format",
            "json-compact",
            "capabilities",
            "--action",
            action,
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"]["selected_version"] == ds_version
    assert payload["data"]["capability"]["availability"] == "supported"
    assert payload["data"]["capability"]["verification"] == "live_smoke"


def test_capabilities_command_returns_full_surface_discovery() -> None:
    result = runner.invoke(app, ["capabilities", "--full"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["resolved"]["capabilities"] == {"view": "full"}
    assert payload["data"]["ds"] == EXPECTED_DS_CAPABILITIES
    action_catalog = payload["data"]["action_catalog"]
    assert len(action_catalog) == 181
    assert any(
        item
        == {
            "action": "workflow.create",
            "availability": "supported",
            "verification": "contract_tested",
        }
        for item in action_catalog
    )
    assert payload["data"]["resources"]["top_level"] == [
        "version",
        "doctor",
        "schema",
        "capabilities",
    ]
    assert payload["data"]["resources"]["groups"]["context"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert payload["data"]["resources"]["groups"]["config"]["commands"] == [
        "get",
        "set",
        "unset",
    ]
    assert payload["data"]["resources"]["groups"]["enum"]["commands"] == [
        "names",
        "list",
    ]
    assert payload["data"]["resources"]["groups"]["task-type"]["commands"] == [
        "list",
        "get",
        "schema",
    ]
    assert payload["data"]["resources"]["groups"]["template"]["commands"] == [
        "workflow",
        "workflow-patch",
        "workflow-instance-patch",
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
        "formats": ["json", "json-compact", "table", "tsv"],
        "default_format": "json",
        "compact_json": True,
        "compact_list_encoding": "columns_rows",
        "json_encoding": "utf-8",
        "default_json_layout": "pretty",
        "error_channel": "stderr",
        "row_diagnostics_channel": "stderr",
        "data_shape_metadata": True,
        "display_columns": True,
        "json_column_projection": True,
        "resolved_metadata": True,
        "warnings": True,
        "structured_warnings": True,
        "structured_errors": True,
        "structured_next_actions": True,
        "structured_action_index": True,
        "max_action_index_targets": 100,
    }
    assert payload["data"]["self_description"] == {
        "schema": True,
        "template": True,
        "capabilities": True,
        "command_invocation_source": "schema",
        "capabilities_scope": "feature_discovery",
        "surface_inventory_scope": "installed_cli_surface",
        "action_availability_command_pattern": ("dsctl capabilities --action ACTION"),
    }
    assert payload["data"]["surface"] == {
        "inventory_scope": "installed_cli_surface",
        "selected_version_availability_source": "action_catalog",
        "action_availability_command_pattern": "dsctl capabilities --action ACTION",
    }
    authoring = payload["data"]["authoring"]
    inventory = authoring["installed_cli_inventory"]
    availability = authoring["selected_version_availability"]
    assert inventory["parameter_syntax"] == parameter_syntax_index_data()
    assert availability["environment_config_template"] is True
    assert availability["cluster_config_template"] is True
    assert availability["datasource_payload_templates"] is True
    assert (
        inventory["datasource_template_types"]
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
    assert "--full" in help_text


def test_capabilities_command_honors_env_file_ds_version(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=3.3.2\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["--env-file", "cluster.env", "capabilities"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"]["current_version"] == "3.3.2"
    assert payload["data"]["ds"]["selected_version"] == "3.3.2"
    assert payload["data"]["ds"]["contract_version"] == "3.3.2"
    assert payload["data"]["ds"]["support_level"] == "experimental"
    assert payload["data"]["ds"]["tested"] is False
    assert payload["data"]["enums"]["discovery"] is True
    assert "priority" in payload["data"]["enums"]["names"]


def test_capabilities_ds_metadata_includes_bounded_exact_catalog_summary(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=1.3.9\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "--format", "json-compact", "capabilities"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    catalog = payload["data"]["ds"]["catalog"]
    assert catalog == get_version_support("1.3.9").catalog.summary_metadata()
    assert catalog["availability_counts"]["unsupported"] > 0
    assert catalog["availability_counts"]["supported"] < catalog["action_count"]
    assert len(json.dumps(catalog, separators=(",", ":"))) < 1_024


def test_capabilities_command_returns_one_exact_action_without_full_inventory(
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
            "--format",
            "json-compact",
            "capabilities",
            "--action",
            "task-type.list",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["resolved"]["capabilities"] == {
        "view": "action",
        "action": "task-type.list",
    }
    assert payload["data"]["capability"]["availability"] == "unsupported"
    assert set(payload["data"]) == {"cli", "ds", "capability", "links"}
    assert len(result.stdout.encode("utf-8")) < 2 * 1024


def test_capabilities_exposes_322_clear_limit_without_narrowing_neighbors(
    isolated_cwd: Path,
) -> None:
    env_file = isolated_cwd / "cluster.env"

    def capability(ds_version: str, action: str) -> dict[str, object]:
        env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "--env-file",
                "cluster.env",
                "capabilities",
                "--action",
                action,
            ],
        )
        assert result.exit_code == 0
        action_capability = json.loads(result.stdout)["data"]["capability"]
        assert isinstance(action_capability, dict)
        return action_capability

    clear_322 = capability("3.2.2", "project-worker-group.clear")
    assert clear_322["availability"] == "limited"
    assert clear_322["verification"] == "static"
    assert "1402003" in str(clear_322["constraint"])

    for action in ("project-worker-group.list", "project-worker-group.set"):
        supported = capability("3.2.2", action)
        assert supported["availability"] == "supported"
        assert "constraint" not in supported

    for ds_version in ("3.3.1", "3.4.3"):
        supported = capability(ds_version, "project-worker-group.clear")
        assert supported["availability"] == "supported"
        assert "constraint" not in supported


def test_capabilities_command_unknown_action_returns_bounded_candidates() -> None:
    result = runner.invoke(
        app,
        ["--format", "json-compact", "capabilities", "--action", "workflow.creat"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["details"]["candidates"][0]["action"] == ("workflow.create")
    assert len(result.stderr.encode("utf-8")) < 2 * 1024


def test_capabilities_command_returns_summary() -> None:
    result = runner.invoke(app, ["capabilities", "--summary"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["resolved"]["capabilities"] == {"view": "summary"}
    assert "resources" in payload["data"]
    assert "runtime" in payload["data"]
    assert "authoring" in payload["data"]
    inventory = payload["data"]["authoring"]["installed_cli_inventory"]
    assert "parameter_syntax" not in inventory


def test_capabilities_command_defaults_to_bounded_summary() -> None:
    default_result = runner.invoke(app, ["--format", "json-compact", "capabilities"])
    explicit_result = runner.invoke(
        app,
        ["--format", "json-compact", "capabilities", "--summary"],
    )

    assert default_result.exit_code == 0
    assert explicit_result.exit_code == 0
    assert json.loads(default_result.stdout) == json.loads(explicit_result.stdout)
    payload = json.loads(default_result.stdout)
    # The summary reports scope without repeating every exact version profile.
    ds = payload["data"]["ds"]
    assert ds["supported_version_count"] == len(SUPPORTED_VERSIONS)
    assert "versions" not in ds
    assert "supported_versions" not in ds
    assert payload["resolved"]["capabilities"] == {"view": "summary"}
    inventory = payload["data"]["authoring"]["installed_cli_inventory"]
    assert "parameter_syntax" not in inventory
    assert "task_templates" not in inventory


def test_capabilities_command_returns_section() -> None:
    result = runner.invoke(app, ["capabilities", "--section", "runtime"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "capabilities"
    assert payload["resolved"]["capabilities"] == {
        "view": "section",
        "section": "runtime",
    }
    assert set(payload["data"]) == {
        "cli",
        "ds",
        "surface",
        "self_description",
        "runtime",
    }
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
    payload = json.loads(result.stderr)
    assert payload["action"] == "capabilities"
    assert payload["error"]["type"] == "user_input_error"
    assert "mutually exclusive" in payload["error"]["message"]


def test_capabilities_command_rejects_action_with_expanded_view() -> None:
    result = runner.invoke(
        app,
        ["capabilities", "--action", "workflow.get", "--full"],
    )

    assert result.exit_code == 1
    assert "mutually exclusive" in json.loads(result.stderr)["error"]["message"]


def test_capabilities_command_rejects_full_with_section() -> None:
    result = runner.invoke(
        app,
        ["capabilities", "--full", "--section", "runtime"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "capabilities"
    assert payload["error"]["type"] == "user_input_error"
    assert "mutually exclusive" in payload["error"]["message"]
