import json
from pathlib import Path

import pytest
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

from dsctl.app import _normalize_root_options, app
from tests.support import normalize_cli_help

runner = CliRunner()


def test_version_command_reports_cli_and_ds_versions() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "version"
    assert payload["data"] == {
        "cli": "0.3.0",
        "ds": "3.4.1",
        "selected_ds_version": "3.4.1",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "supported_ds_versions": ["3.3.2", "3.4.0", "3.4.1"],
    }


def test_version_command_can_render_tsv_columns() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "tsv", "--columns", "cli,ds,family", "version"],
    )

    assert result.exit_code == 0
    assert result.stdout == ("cli\tds\tfamily\n0.3.0\t3.4.1\tworkflow-3.3-plus\n")


def test_version_command_can_project_json_columns() -> None:
    result = runner.invoke(app, ["--columns", "cli,ds", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["data"] == {"cli": "0.3.0", "ds": "3.4.1"}


def test_version_command_accepts_compact_json_output() -> None:
    result = runner.invoke(app, ["--compact", "version"])

    assert result.exit_code == 0
    assert result.stderr == ""
    assert result.stdout.count("\n") == 1
    assert json.loads(result.stdout)["action"] == "version"


def test_compact_rejects_non_json_output() -> None:
    result = runner.invoke(
        app,
        ["--compact", "--output-format", "table", "version"],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "--compact can only be used" in result.stderr


def test_root_help_describes_global_output_option_placement() -> None:
    result = runner.invoke(app, ["--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "--compact" in help_text
    assert "Global option" in help_text
    assert "before or after the command path" in help_text


def test_root_help_routes_agents_to_narrow_discovery_and_navigation() -> None:
    result = runner.invoke(app, ["--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "schema --command ACTION" in help_text
    assert "leaf `--help`" in help_text
    assert "inspect one relevant group" in help_text
    assert "do not preload unrelated groups" in help_text
    assert "downstream lifecycle actions" in help_text
    assert "next_actions" in help_text
    assert "matches the current goal and is authorized" in help_text
    assert "run that command unchanged" in help_text


def test_workflow_create_help_separates_bounded_lint_from_full_dry_run() -> None:
    result = runner.invoke(app, ["workflow", "create", "--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "lint workflow FILE" in help_text
    assert "full DS request" in help_text
    assert "bounded DAG validation" in help_text


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["version", "--compact"], ["--compact", "version"]),
        (
            ["workflow", "list", "--output-format", "table", "--compact"],
            ["--output-format", "table", "--compact", "workflow", "list"],
        ),
        (
            ["version", "--output-format=tsv"],
            ["--output-format=tsv", "version"],
        ),
        (
            ["workflow", "get", "--", "--compact"],
            ["workflow", "get", "--", "--compact"],
        ),
        (
            ["workflow", "--compact", "list", "--all"],
            ["--compact", "workflow", "list", "--all"],
        ),
        (
            ["workflow", "list", "--all", "--compact"],
            ["--compact", "workflow", "list", "--all"],
        ),
    ],
)
def test_normalize_root_options_accepts_either_placement(
    args: list[str],
    expected: list[str],
) -> None:
    root_command = get_command(app)
    assert isinstance(root_command, TyperGroup)
    assert _normalize_root_options(root_command, args) == expected


@pytest.mark.parametrize(
    "args",
    [
        ["workflow", "list", "--search", "--compact"],
        ["workflow", "get", "--project", "--compact", "daily-sync"],
        ["schema", "--command", "--compact"],
        ["workflow", "list", "--search=--compact"],
    ],
)
def test_normalize_root_options_preserves_leaf_option_values(args: list[str]) -> None:
    root_command = get_command(app)
    assert isinstance(root_command, TyperGroup)
    assert _normalize_root_options(root_command, args) == args


def test_global_render_options_can_follow_leaf_command() -> None:
    result = runner.invoke(
        app,
        ["version", "--columns", "cli,ds", "--compact"],
    )

    assert result.exit_code == 0
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["data"] == {"cli": "0.3.0", "ds": "3.4.1"}


@pytest.mark.parametrize("ds_version", ["3.3.2", "3.4.0"])
def test_version_command_marks_untested_versions_experimental(
    ds_version: str,
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        f"DS_VERSION={ds_version}\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["--env-file", "cluster.env", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"] == ds_version
    assert payload["data"]["selected_ds_version"] == ds_version
    assert payload["data"]["contract_version"] == "3.4.1"
    assert payload["data"]["family"] == "workflow-3.3-plus"
    assert payload["data"]["support_level"] == "experimental"


def test_context_command_reads_env_file_and_project_context(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_API_URL=http://example.test/dolphinscheduler\nDS_API_TOKEN=secret-token\n",
        encoding="utf-8",
    )
    (isolated_cwd / ".dsctl-context.yaml").write_text(
        (
            "project: etl-prod\n"
            "workflow: daily-etl\n"
            "set_at: '2026-07-13T10:00:00+00:00'\n"
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "context"],
        env={"XDG_CONFIG_HOME": str(isolated_cwd / "xdg")},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "context"
    assert payload["data"]["api_url"] == "http://example.test/dolphinscheduler"
    assert payload["data"]["ds_version"] == "3.4.1"
    assert payload["data"]["project"] == "etl-prod"
    assert payload["data"]["workflow"] == "daily-etl"
    assert payload["data"]["set_at"] == "2026-07-13T10:00:00+00:00"
    assert payload["resolved"] == {
        "context": {"scope": "project"},
        "remote_validation": "not_performed",
    }
    assert "default_project" not in payload["data"]
