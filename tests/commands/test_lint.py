import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app

runner = CliRunner()


def test_lint_workflow_command_returns_local_summary(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: []
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["lint", "workflow", str(spec_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "lint.workflow"
    assert payload["data"]["valid"] is True
    assert payload["data"]["summary"]["taskCount"] == 1
    assert payload["data"]["diagnostics"][-1] == {
        "code": "workflow_project_selection_external",
        "status": "warning",
        "message": (
            "workflow.project is not set in the file; workflow create will need "
            "--project or stored project context."
        ),
        "field": "workflow.project",
        "suggestion": (
            "Pass --project when creating the workflow or set project context "
            "before retrying."
        ),
    }
    assert payload["warnings"] == [
        "workflow.project is not set in the file; workflow create will need "
        "--project or stored project context."
    ]


def test_lint_workflow_command_reports_invalid_schedule_contract(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: []
schedule:
  cron: "0 0 2 * * ?"
  timezone: Asia/Shanghai
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: false
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["lint", "workflow", str(spec_path)])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "lint.workflow"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Set workflow.release_state=ONLINE or remove the schedule block "
        "before retrying."
    )
