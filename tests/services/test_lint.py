from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from dsctl.errors import UserInputError
from dsctl.services.lint import lint_workflow_result


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (bytes, bytearray, str))
    return value


def test_lint_workflow_result_returns_local_summary(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  project: analytics
  release_state: ONLINE
tasks:
  - name: extract
    type: shell
    command: echo extract
    depends_on: []
  - name: load
    type: SHELL
    command: echo load
    depends_on:
      - extract
schedule:
  cron: "0 0 2 * * ?"
  timezone: Asia/Shanghai
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: false
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    expected_checks = [
        {
            "code": "workflow_spec_model_valid",
            "status": "pass",
            "message": "Workflow YAML matches the stable workflow spec.",
        },
        {
            "code": "workflow_schedule_contract_valid",
            "status": "pass",
            "message": (
                "Workflow schedule block is locally compatible with workflow create."
            ),
        },
        {
            "code": "workflow_compiles_for_create",
            "status": "pass",
            "message": ("Workflow DAG compiles to the DS workflow-create payload."),
        },
    ]
    assert result.resolved == {"kind": "workflow", "file": str(spec_path)}
    assert result.warnings == []
    assert result.warning_details == []
    assert result.data == {
        "kind": "workflow",
        "valid": True,
        "summary": {
            "name": "daily-etl",
            "project": "analytics",
            "releaseState": "ONLINE",
            "executionType": "PARALLEL",
            "taskCount": 2,
            "edgeCount": 1,
            "taskTypeCounts": {"SHELL": 2},
            "rootTasks": ["extract"],
            "leafTasks": ["load"],
            "hasSchedule": True,
        },
        "compilation": {
            "taskDefinitionCount": 2,
            "taskRelationCount": 2,
            "globalParamCount": 0,
        },
        "checks": expected_checks,
        "diagnostics": [
            {
                "code": check["code"],
                "status": check["status"],
                "message": check["message"],
                "field": None,
                "suggestion": None,
            }
            for check in expected_checks
        ],
    }


def test_lint_workflow_result_warns_when_project_is_external(
    tmp_path: Path,
) -> None:
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

    result = lint_workflow_result(file=spec_path)

    assert result.warnings == [
        "workflow.project is not set in the file; workflow create will need "
        "--project or stored project context."
    ]
    assert list(result.warning_details) == [
        {
            "code": "workflow_project_selection_external",
            "message": (
                "workflow.project is not set in the file; workflow create will "
                "need --project or stored project context."
            ),
            "field": "workflow.project",
            "suggestion": (
                "Pass --project when creating the workflow or set project "
                "context before retrying."
            ),
            "accepted_sources": [
                "--project",
                "context.project",
            ],
        }
    ]
    data = _mapping(result.data)
    diagnostics = _sequence(data["diagnostics"])
    assert dict(_mapping(diagnostics[-1])) == {
        "code": "workflow_project_selection_external",
        "status": "warning",
        "message": result.warnings[0],
        "field": "workflow.project",
        "suggestion": (
            "Pass --project when creating the workflow or set project context "
            "before retrying."
        ),
    }


def test_lint_workflow_result_warns_on_risky_time_parameter_format(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  project: analytics
  global_params:
    week_key: "$[yyyyww]"
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: []
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert result.warnings == [
        "workflow.global_params.week_key contains $[yyyyww]: combining "
        "calendar-year tokens such as yyyy with week tokens such as ww can be "
        "wrong near year boundaries."
    ]
    assert list(result.warning_details) == [
        {
            "code": "parameter_time_format_calendar_year_with_week",
            "message": result.warnings[0],
            "field": "workflow.global_params.week_key",
            "expression": "$[yyyyww]",
            "pattern": "yyyyww",
            "suggestion": (
                "Use DS year_week(...) when week-of-year output is intended, or "
                "choose yyyy versus YYYY deliberately before applying the workflow."
            ),
        }
    ]
    data = _mapping(result.data)
    diagnostics = _sequence(data["diagnostics"])
    assert dict(_mapping(diagnostics[-1])) == {
        "code": "parameter_time_format_calendar_year_with_week",
        "status": "warning",
        "message": result.warnings[0],
        "field": "workflow.global_params.week_key",
        "suggestion": (
            "Use DS year_week(...) when week-of-year output is intended, or "
            "choose yyyy versus YYYY deliberately before applying the workflow."
        ),
    }


def test_lint_workflow_result_rejects_schedule_on_offline_workflow(
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

    with pytest.raises(
        UserInputError,
        match=r"require workflow\.release_state=ONLINE",
    ):
        lint_workflow_result(file=spec_path)


def test_lint_workflow_result_rejects_unknown_dependencies(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
tasks:
  - name: load
    type: SHELL
    command: echo load
    depends_on:
      - missing
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="depends on unknown task 'missing'"):
        lint_workflow_result(file=spec_path)
