from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from dsctl.errors import ConfigError
from dsctl.output import CommandResult, error_payload, success_payload
from dsctl.output_formats import (
    OutputFormat,
    RenderOptions,
    render_command,
    render_raw_command,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_render_command_reports_incomplete_page_on_stderr(
    output_format: OutputFormat,
) -> None:
    payload = success_payload(
        "project.list",
        CommandResult(
            data={
                "totalList": [
                    {"code": 1, "name": "one"},
                    {"code": 2, "name": "two"},
                ],
                "total": 10,
                "totalPage": 5,
                "pageNo": 1,
                "pageSize": 2,
                "currentPage": 1,
            }
        ),
    )

    rendered = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format=output_format),
    )

    assert rendered.exit_code == 0
    assert rendered.stdout
    assert rendered.stderr == "page: 1/5; showing 2 of 10 rows\n"


def test_render_command_omits_page_diagnostic_for_complete_result() -> None:
    payload = success_payload(
        "project.list",
        CommandResult(
            data={
                "totalList": [{"code": 1, "name": "one"}],
                "total": 1,
                "totalPage": 1,
                "pageNo": 1,
                "pageSize": 100,
                "currentPage": 1,
            }
        ),
    )

    rendered = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format="table"),
    )

    assert rendered.stderr == ""


def test_render_command_omits_page_diagnostic_for_empty_result() -> None:
    payload = success_payload(
        "project.list",
        CommandResult(
            data={
                "totalList": [],
                "total": 0,
                "totalPage": 0,
                "pageNo": 1,
                "pageSize": 100,
                "currentPage": 1,
            }
        ),
    )

    rendered = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format="table"),
    )

    assert rendered.stderr == ""


def test_render_command_does_not_append_resolved_context_to_row_output() -> None:
    payload = success_payload(
        "workflow.list",
        CommandResult(
            data=[{"code": 1, "name": "daily-etl", "version": 1}],
            resolved={
                "project": {
                    "code": 7,
                    "name": "etl-prod",
                    "source": "context",
                },
                "workflow": {
                    "code": 9,
                    "name": "daily-etl",
                    "source": "context",
                },
            },
        ),
    )

    rendered = render_command(
        payload,
        action="workflow.list",
        options=RenderOptions(output_format="table"),
    )

    assert rendered.stderr == ""


def test_render_command_reports_actual_row_count_on_a_later_page() -> None:
    payload = success_payload(
        "project.list",
        CommandResult(
            data={
                "totalList": [{"code": 5, "name": "last"}],
                "total": 5,
                "totalPage": 3,
                "pageNo": 3,
                "pageSize": 2,
                "currentPage": 3,
            }
        ),
    )

    rendered = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format="tsv"),
    )

    assert rendered.stderr == "page: 3/3; showing 1 of 5 rows\n"


def test_render_command_keeps_json_diagnostics_inside_envelope() -> None:
    message = "dry run: no request was sent"
    payload = success_payload(
        "project.create",
        CommandResult(
            data={"dry_run": True},
            warnings=[message],
            warning_details=[
                {
                    "code": "dry_run_no_request_sent",
                    "message": message,
                    "request_sent": False,
                }
            ],
        ),
    )

    rendered = render_command(
        payload,
        action="project.create",
        options=RenderOptions(compact=True),
    )

    assert rendered.exit_code == 0
    assert rendered.stderr == ""
    assert json.loads(rendered.stdout)["warning_details"][0]["code"] == (
        "dry_run_no_request_sent"
    )


def test_render_command_routes_json_error_to_stderr() -> None:
    payload = error_payload("context", ConfigError("missing profile"))

    rendered = render_command(
        payload,
        action="context",
        options=RenderOptions(compact=True),
    )

    assert rendered.stdout == ""
    assert rendered.exit_code == 1
    assert rendered.stderr.count("\n") == 1
    assert json.loads(rendered.stderr)["error"]["type"] == "config_error"


def test_render_raw_command_preserves_artifact_and_reports_warning() -> None:
    message = "generic task template: inspect task type schema before applying"
    payload = success_payload(
        "template.task",
        CommandResult(
            data={"yaml": "type: CUSTOM"},
            warnings=[message],
            warning_details=[
                {
                    "code": "generic_task_template",
                    "message": message,
                }
            ],
        ),
    )

    rendered = render_raw_command("type: CUSTOM", payload=payload)

    assert rendered.stdout == "type: CUSTOM"
    assert rendered.stderr == f"warning[generic_task_template]: {message}\n"
    assert rendered.exit_code == 0


def test_compact_projection_has_a_bounded_machine_output_budget() -> None:
    rows: list[JsonObject] = [
        {
            "id": index,
            "name": f"每日同步-{index}",
            "state": "SUCCESS",
            "taskParams": "x" * 1_000,
        }
        for index in range(10)
    ]
    payload = success_payload(
        "task-instance.list",
        CommandResult(
            data={
                "totalList": rows,
                "total": 10,
                "totalPage": 1,
                "pageNo": 1,
                "pageSize": 10,
                "currentPage": 1,
            }
        ),
    )

    full = render_command(
        payload,
        action="task-instance.list",
        options=RenderOptions(),
    )
    projected = render_command(
        payload,
        action="task-instance.list",
        options=RenderOptions(
            compact=True,
            columns=("id", "name", "state"),
        ),
    )

    assert projected.stdout.count("\n") == 1
    assert "taskParams" not in projected.stdout
    assert "每日同步" in projected.stdout
    assert len(projected.stdout.encode("utf-8")) < 1_500
    assert len(projected.stdout) < len(full.stdout) * 0.3
