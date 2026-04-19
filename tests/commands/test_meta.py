import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import _misplaced_root_option, app

runner = CliRunner()


def test_version_command_reports_cli_and_ds_versions() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "version"
    assert payload["data"] == {
        "cli": "0.1.0",
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
    assert result.stdout == ("cli\tds\tfamily\n0.1.0\t3.4.1\tworkflow-3.3-plus\n")


def test_version_command_can_project_json_columns() -> None:
    result = runner.invoke(app, ["--columns", "cli,ds", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["data"] == {"cli": "0.1.0", "ds": "3.4.1"}


def test_misplaced_root_option_detection() -> None:
    assert (
        _misplaced_root_option(["worker-group", "list", "--output-format", "table"])
        == "--output-format"
    )
    assert _misplaced_root_option(["--output-format", "table", "version"]) is None


def test_version_command_honors_env_file_ds_version() -> None:
    with runner.isolated_filesystem():
        Path("cluster.env").write_text("DS_VERSION=3.3.2\n", encoding="utf-8")

        result = runner.invoke(app, ["--env-file", "cluster.env", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"] == "3.3.2"
    assert payload["data"]["selected_ds_version"] == "3.3.2"
    assert payload["data"]["contract_version"] == "3.4.1"
    assert payload["data"]["family"] == "workflow-3.3-plus"


def test_context_command_reads_env_file_and_project_context() -> None:
    with runner.isolated_filesystem():
        Path("cluster.env").write_text(
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=secret-token\n",
            encoding="utf-8",
        )
        Path(".dsctl-context.yaml").write_text(
            "project: etl-prod\nworkflow: daily-etl\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app,
            ["--env-file", "cluster.env", "context"],
            env={"XDG_CONFIG_HOME": str(Path("xdg").resolve())},
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "context"
    assert payload["data"]["api_url"] == "http://example.test/dolphinscheduler"
    assert payload["data"]["ds_version"] == "3.4.1"
    assert payload["data"]["project"] == "etl-prod"
    assert payload["data"]["workflow"] == "daily-etl"
    assert "default_project" not in payload["data"]
