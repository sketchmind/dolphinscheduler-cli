import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.output import CommandResult

runner = CliRunner()


def test_lint_workflow_command_passes_global_env_file_to_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / "ds.env"
    profile_path.write_text("DS_VERSION=3.4.1\n", encoding="utf-8")
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text("workflow: {name: daily-etl}\ntasks: []\n", encoding="utf-8")
    captured: dict[str, str | None] = {}

    def fake_lint_workflow_result(
        *,
        file: str,
        env_file: str | None = None,
    ) -> CommandResult:
        captured["file"] = file
        captured["env_file"] = env_file
        return CommandResult(data={"valid": True})

    monkeypatch.setattr(
        "dsctl.commands.lint.lint_workflow_result",
        fake_lint_workflow_result,
    )

    result = runner.invoke(
        app,
        [
            "--env-file",
            str(profile_path),
            "lint",
            "workflow",
            str(spec_path),
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "file": str(spec_path.resolve()),
        "env_file": str(profile_path),
    }


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
        "severity": "warning",
        "code": "workflow_project_selection_external",
        "message": (
            "workflow.project is not set in the file; workflow create will need "
            "--project or stored project context."
        ),
        "path": "workflow.project",
    }
    assert [item["message"] for item in payload.get("warnings", [])] == [
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
    payload = json.loads(result.stderr)
    assert payload["action"] == "lint.workflow"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Set workflow.release_state=ONLINE or remove the schedule block "
        "before retrying."
    )
    assert payload["data"]["valid"] is False
    assert any(
        item["code"] == "workflow_schedule_contract_invalid"
        for item in payload["data"]["diagnostics"]
    )


def test_lint_workflow_patch_command_reports_deferred_baseline_checks(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match: {name: existing}
        set: {description: changed}
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["lint", "workflow-patch", str(patch_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "lint.workflow-patch"
    assert payload["data"]["valid"] is True
    assert payload["data"]["baseline"]["requiredForCompleteValidation"] is True
    assert payload["data"]["summary"]["updateCount"] == 1


def test_lint_workflow_instance_patch_command_returns_all_unsupported_fields(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      name: renamed
      release_state: ONLINE
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["lint", "workflow-instance-patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "lint.workflow-instance-patch"
    assert payload["data"]["valid"] is False
    unsupported_paths = {
        item["path"]
        for item in payload["data"]["diagnostics"]
        if item["code"] == "workflow_instance_patch_field_unsupported"
    }
    assert unsupported_paths == {
        "patch.workflow.set.name",
        "patch.workflow.set.release_state",
    }
