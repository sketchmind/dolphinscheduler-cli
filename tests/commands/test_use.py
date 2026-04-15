import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app

runner = CliRunner()


def test_use_project_command_sets_context_and_clears_workflow() -> None:
    with runner.isolated_filesystem():
        env = {"XDG_CONFIG_HOME": str((Path.cwd() / ".config").resolve())}

        first = runner.invoke(app, ["use", "workflow", "daily-sync"], env=env)
        second = runner.invoke(app, ["use", "project", "etl-prod"], env=env)

        assert first.exit_code == 0
        assert second.exit_code == 0
        payload = json.loads(second.stdout)
        assert payload["action"] == "use.project"
        assert payload["data"]["project"] == "etl-prod"
        assert payload["data"]["workflow"] is None


def test_use_clear_command_clears_project_scope() -> None:
    with runner.isolated_filesystem():
        env = {"XDG_CONFIG_HOME": str((Path.cwd() / ".config").resolve())}

        runner.invoke(app, ["use", "project", "etl-prod"], env=env)
        result = runner.invoke(app, ["use", "--clear"], env=env)

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["action"] == "use.clear"
        assert payload["data"]["project"] is None
        assert payload["data"]["workflow"] is None
