import json
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import _misplaced_root_option, app, main

runner = CliRunner()


def test_version_command_reports_cli_and_ds_versions() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "version"
    assert payload["data"] == {
        "cli": "0.2.0",
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
    assert result.stdout == ("cli\tds\tfamily\n0.2.0\t3.4.1\tworkflow-3.3-plus\n")


def test_version_command_can_project_json_columns() -> None:
    result = runner.invoke(app, ["--columns", "cli,ds", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["data"] == {"cli": "0.2.0", "ds": "3.4.1"}


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

    assert result.exit_code == 0
    assert "--compact" in result.stdout
    assert "Global option" in result.stdout
    assert "COMMAND" in result.stdout


def test_misplaced_root_option_detection() -> None:
    assert (
        _misplaced_root_option(["worker-group", "list", "--output-format", "table"])
        == "--output-format"
    )
    assert _misplaced_root_option(["--output-format", "table", "version"]) is None
    assert _misplaced_root_option(["version", "--compact"]) == "--compact"
    assert _misplaced_root_option(["--compact", "version"]) is None


def test_console_main_reports_misplaced_compact_on_stderr(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["dsctl", "version", "--compact"])

    with pytest.raises(SystemExit) as exc_info:
        main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert captured.out == ""
    assert captured.err == (
        "--compact is a global dsctl option. Put it before the command group, "
        "for example: dsctl --compact <command> ...\n"
    )


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
        "project: etl-prod\nworkflow: daily-etl\n",
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
    assert "default_project" not in payload["data"]
