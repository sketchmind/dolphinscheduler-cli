import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app
from tests.support import normalize_cli_help

runner = CliRunner()


def test_use_project_command_sets_context_and_clears_workflow(
    isolated_cwd: Path,
) -> None:
    env = {"XDG_CONFIG_HOME": str(isolated_cwd / ".config")}

    initial_project = runner.invoke(
        app,
        ["use", "project", "analytics"],
        env=env,
    )
    first = runner.invoke(app, ["use", "workflow", "daily-sync"], env=env)
    second = runner.invoke(app, ["use", "project", "etl-prod"], env=env)

    assert initial_project.exit_code == 0
    assert first.exit_code == 0
    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    assert payload["action"] == "use.project"
    assert payload["data"]["project"] == "etl-prod"
    assert payload["data"]["workflow"] is None


def test_use_workflow_command_requires_project_context(isolated_cwd: Path) -> None:
    env = {"XDG_CONFIG_HOME": str(isolated_cwd / ".config")}

    result = runner.invoke(app, ["use", "workflow", "daily-sync"], env=env)

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl use project NAME --scope project` before setting workflow context."
    )


def test_use_project_command_repairs_legacy_workflow_only_context(
    isolated_cwd: Path,
) -> None:
    env = {"XDG_CONFIG_HOME": str(isolated_cwd / ".config")}
    (isolated_cwd / ".dsctl-context.yaml").write_text(
        "workflow: orphan-workflow\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["use", "project", "etl-prod"], env=env)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["project"] == "etl-prod"
    assert payload["data"]["workflow"] is None


def test_use_clear_command_clears_project_scope(isolated_cwd: Path) -> None:
    env = {"XDG_CONFIG_HOME": str(isolated_cwd / ".config")}

    runner.invoke(app, ["use", "project", "etl-prod"], env=env)
    result = runner.invoke(app, ["use", "--clear"], env=env)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "use.clear"
    assert payload["data"]["project"] is None
    assert payload["data"]["workflow"] is None


def test_use_help_points_to_context_target_discovery() -> None:
    project_result = runner.invoke(app, ["use", "project", "--help"])
    workflow_result = runner.invoke(app, ["use", "workflow", "--help"])

    assert project_result.exit_code == 0
    assert "project" in project_result.stdout
    assert "list" in project_result.stdout
    assert workflow_result.exit_code == 0
    assert "workflow" in workflow_result.stdout
    assert "list" in workflow_result.stdout
    assert (
        "requires an effective project"
        in normalize_cli_help(workflow_result.stdout).lower()
    )
