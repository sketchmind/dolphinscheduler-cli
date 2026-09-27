from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from rich.cells import cell_len

from dsctl.errors import ConfigError
from dsctl.output import CommandResult, dry_run_result, error_payload, result_payload
from dsctl.output_formats import (
    OutputFormat,
    RenderOptions,
    render_command,
    render_raw_command,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


def test_dry_run_defaults_to_effects_and_expands_the_same_prepared_requests() -> None:
    result = dry_run_result(
        method="PUT",
        path="/definition/7",
        form_data={"tasks": "large native payload"},
        resolved={"workflow": {"code": 7}},
        extra_data={"diff": {"removed_tasks": ["old"]}},
    )
    payload = result_payload("workflow.edit", result)
    default = render_command(payload, action="workflow.edit", options=RenderOptions())
    data = json.loads(default.stdout)["data"]
    assert data["diff"] == {"removed_tasks": ["old"]}
    assert data["execution_order"] == [{"method": "PUT", "path": "/definition/7"}]
    assert "requests" not in data
    assert "request" not in data
    assert "--columns requests" in data["request_details"]
    expanded = render_command(
        payload, action="workflow.edit", options=RenderOptions(columns=("requests",))
    )
    assert json.loads(expanded.stdout)["data"] == {
        "requests": [
            {
                "method": "PUT",
                "path": "/definition/7",
                "form": {"tasks": "large native payload"},
            }
        ]
    }
    assert isinstance(result.data, dict)
    assert "requests" in result.data
    compact = render_command(
        payload,
        action="workflow.edit",
        options=RenderOptions(output_format="json-compact"),
    )
    assert json.loads(compact.stdout) == json.loads(default.stdout)


def test_no_change_preview_has_no_mutation_stages() -> None:
    payload = result_payload(
        "workflow.edit",
        dry_run_result(
            method="PUT",
            path="/definition/7",
            requests=[],
            extra_data={"no_change": True},
        ),
    )
    rendered = render_command(payload, action="workflow.edit", options=RenderOptions())
    assert json.loads(rendered.stdout)["data"]["execution_order"] == []
    expanded = render_command(
        payload,
        action="workflow.edit",
        options=RenderOptions(columns=("requests",)),
    )
    assert json.loads(expanded.stdout)["data"] == {"requests": []}


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_text_aggregate_reports_changing_non_atomic_coverage(
    output_format: OutputFormat,
) -> None:
    payload = result_payload(
        "project.list",
        CommandResult(
            data={
                "totalList": [{"code": 1, "name": "one"}],
                "total": 1,
                "totalPage": 1,
                "pageNo": 1,
                "coverage": {
                    "scope": "initial_page_range",
                    "requested_start_page": 3,
                    "requested_page_size": 20,
                    "initial_total_pages": 4,
                    "pages_read": 2,
                    "rows_read": 1,
                    "initial_total": 2,
                    "totals_changed": True,
                    "scope_complete": True,
                    "atomic_snapshot": False,
                },
            }
        ),
    )
    rendered = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format=output_format),
    )
    assert "scope=initial_page_range" in rendered.stderr
    assert "1 rows / 2 pages" in rendered.stderr
    assert "totals changed during reading" in rendered.stderr
    assert "non-atomic observation" in rendered.stderr
    data = payload["data"]
    assert isinstance(data, dict)
    coverage = data["coverage"]
    assert isinstance(coverage, dict)
    assert coverage["initial_total"] == 2
    assert "coverage" not in rendered.stdout


@pytest.mark.parametrize(
    ("action", "row_field"),
    [
        ("template.task", "rows"),
        ("template.workflow", "lines"),
        ("template.workflow-patch", "lines"),
        ("template.workflow-instance-patch", "lines"),
    ],
)
def test_yaml_documents_derive_line_views_without_storing_a_copy(
    action: str, row_field: str
) -> None:
    payload = result_payload(
        action, CommandResult(data={"yaml": "# example\nname: daily\n"})
    )
    original = json.loads(json.dumps(payload))
    default = render_command(payload, action=action, options=RenderOptions())
    assert json.loads(default.stdout)["data"] == original["data"]
    table = render_command(
        payload, action=action, options=RenderOptions(output_format="tsv")
    )
    assert table.stdout == "line_no\tline\n1\t# example\n2\tname: daily\n"
    selected = render_command(
        payload, action=action, options=RenderOptions(columns=("line",))
    )
    assert json.loads(selected.stdout)["data"] == {
        row_field: [{"line": "# example"}, {"line": "name: daily"}]
    }
    assert payload == original


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_text_errors_retain_partial_mutation_and_confirmation_facts(
    output_format: OutputFormat,
) -> None:
    payload = error_payload(
        "workflow.create",
        ConfigError(
            "Schedule activation failed",
            source={
                "kind": "remote",
                "system": "dolphinscheduler",
                "layer": "result",
                "result_code": 30002,
            },
            details={
                "mutation_applied": True,
                "completed_stages": ["workflow_created"],
                "known_resources": {"workflow_code": 123},
                "confirmation": {"token": "reviewed-risk", "affected_tasks": 4},
            },
        ),
    )
    rendered = render_command(
        payload,
        action="workflow.create",
        options=RenderOptions(output_format=output_format),
    )
    assert rendered.exit_code == 1
    for fact in (
        "mutation_applied",
        "workflow_created",
        "123",
        "reviewed-risk",
        "affected_tasks",
        "Source",
        "dolphinscheduler",
        "30002",
    ):
        assert fact in rendered.stderr


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_text_rows_cannot_inject_control_characters(
    output_format: OutputFormat,
) -> None:
    payload = result_payload(
        "project.list",
        CommandResult(data={"totalList": [{"name": "a\tb\nc\rd\x1b[2J\x08"}]}),
    )
    rendered = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format=output_format, columns=("name",)),
    )
    assert "\x1b" not in rendered.stdout
    assert "\x08" not in rendered.stdout
    assert "\r" not in rendered.stdout
    assert "\\x1b[2J\\x08" in rendered.stdout
    assert rendered.stdout.count("\n") == (3 if output_format == "table" else 2)


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_render_command_reports_incomplete_page_on_stderr(
    output_format: OutputFormat,
) -> None:
    payload = result_payload(
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
    payload = result_payload(
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
    payload = result_payload(
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


@pytest.mark.parametrize("action", ["task.list", "task.get"])
def test_render_command_projects_legacy_task_identity_columns(action: str) -> None:
    task = {"id": "native-task-1", "name": "extract"}
    payload = result_payload(
        action,
        CommandResult(data=[task] if action == "task.list" else task),
    )

    rendered = render_command(
        payload,
        action=action,
        options=RenderOptions(output_format="tsv"),
    )

    assert rendered.stdout == "id\tname\nnative-task-1\textract\n"


def test_render_command_does_not_append_resolved_context_to_row_output() -> None:
    payload = result_payload(
        "workflow.list",
        CommandResult(
            data={
                "totalList": [
                    {
                        "code": 1,
                        "name": "daily-etl",
                        "version": 1,
                        "releaseState": "ONLINE",
                        "scheduleReleaseState": None,
                        "scheduleId": None,
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "pageNo": 1,
                "pageSize": 100,
                "currentPage": 1,
            },
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
    assert "daily-etl" in rendered.stdout
    assert "etl-prod" not in rendered.stdout


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_render_command_does_not_append_next_actions_to_row_output(
    output_format: OutputFormat,
) -> None:
    payload = result_payload(
        "workflow.run",
        CommandResult(data={"workflowInstanceIds": [242]}),
    )

    rendered = render_command(
        payload,
        action="workflow.run",
        options=RenderOptions(output_format=output_format),
    )

    assert "next_actions" not in rendered.stdout
    assert "workflow-instance watch" not in rendered.stdout
    assert rendered.stderr == ""


def test_json_column_projection_preserves_next_actions_outside_data() -> None:
    payload = result_payload(
        "workflow.run",
        CommandResult(
            data={"workflowInstanceIds": [242], "unbounded": "x" * 1_000},
            resolved={"project": {"code": 7}},
        ),
    )

    rendered = render_command(
        payload,
        action="workflow.run",
        options=RenderOptions(
            output_format="json-compact", columns=("workflowInstanceIds",)
        ),
    )
    output = json.loads(rendered.stdout)

    assert output["data"] == {"workflowInstanceIds": [242]}
    assert output["next_actions"][0]["action"] == "workflow-instance.watch"


def test_json_column_projection_preserves_action_index_outside_data() -> None:
    payload = result_payload(
        "workflow-instance.list",
        CommandResult(
            data={"totalList": [{"id": 263, "name": "daily-1", "state": "SUCCESS"}]}
        ),
    )

    rendered = render_command(
        payload,
        action="workflow-instance.list",
        options=RenderOptions(output_format="json-compact", columns=("id", "state")),
    )
    output = json.loads(rendered.stdout)

    assert output["data"] == {
        "totalList": {"columns": ["id", "state"], "rows": [[263, "SUCCESS"]]}
    }
    assert output["action_index"]["groups"][0]["read"][0] == ("workflow-instance.get")


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_row_output_does_not_append_action_index(output_format: OutputFormat) -> None:
    payload = result_payload(
        "workflow-instance.list",
        CommandResult(data={"totalList": [{"id": 263, "state": "SUCCESS"}]}),
    )

    rendered = render_command(
        payload,
        action="workflow-instance.list",
        options=RenderOptions(output_format=output_format),
    )

    assert "action_index" not in rendered.stdout
    assert "workflow-instance.get" not in rendered.stdout
    assert rendered.stderr == ""


def test_render_command_reports_actual_row_count_on_a_later_page() -> None:
    payload = result_payload(
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
    message = "dry run: no mutation was sent; lookup and verification reads may occur"
    payload = result_payload(
        "project.create",
        CommandResult(
            data={"dry_run": True},
            warnings=[message],
            warning_details=[
                {
                    "code": "dry_run_no_mutation_sent",
                    "message": message,
                    "mutation_sent": False,
                }
            ],
        ),
    )

    rendered = render_command(
        payload,
        action="project.create",
        options=RenderOptions(output_format="json-compact"),
    )

    assert rendered.exit_code == 0
    assert rendered.stderr == ""
    assert json.loads(rendered.stdout)["warnings"][0]["code"] == (
        "dry_run_no_mutation_sent"
    )


def test_render_command_routes_json_error_to_stderr() -> None:
    payload = error_payload("context", ConfigError("missing profile"))

    rendered = render_command(
        payload,
        action="context",
        options=RenderOptions(output_format="json-compact"),
    )

    assert rendered.stdout == ""
    assert rendered.exit_code == 1
    assert rendered.stderr.count("\n") == 1
    assert json.loads(rendered.stderr)["error"]["type"] == "config_error"


def test_render_raw_command_preserves_artifact_and_reports_warning() -> None:
    message = "generic task template: inspect task type schema before applying"
    payload = result_payload(
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


def test_compact_projection_preserves_selected_rows_and_envelope() -> None:
    rows: list[JsonObject] = [
        {
            "id": index,
            "name": f"每日同步-{index}",
            "state": "SUCCESS",
            "taskParams": "x" * 1_000,
        }
        for index in range(10)
    ]
    payload = result_payload(
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
            output_format="json-compact",
            columns=("id", "name", "state"),
        ),
    )

    assert projected.stdout.count("\n") == len(rows) + 2
    assert "taskParams" not in projected.stdout
    assert "每日同步" in projected.stdout
    original = json.loads(full.stdout)
    selected = json.loads(projected.stdout)
    assert selected["data"]["totalList"] == {
        "columns": ["id", "name", "state"],
        "rows": [
            [row[key] for key in ("id", "name", "state")]
            for row in original["data"]["totalList"]
        ],
    }
    assert {key: value for key, value in selected.items() if key != "data"} == {
        key: value for key, value in original.items() if key != "data"
    }


@pytest.mark.parametrize("output_format", ["json", "json-compact"])
def test_projection_preserves_explicit_targets_for_a_truncated_index(
    output_format: OutputFormat,
) -> None:
    payload = result_payload(
        "workflow-instance.list",
        CommandResult(
            data={
                "totalList": [
                    {"id": target, "name": f"run-{target}", "state": "SUCCESS"}
                    for target in range(1, 102)
                ]
            },
        ),
    )
    rendered = render_command(
        payload,
        action="workflow-instance.list",
        options=RenderOptions(output_format=output_format, columns=("name",)),
    )
    selected = json.loads(rendered.stdout)
    expected_rows = [{"name": f"run-{target}"} for target in range(1, 102)]
    if output_format == "json-compact":
        assert selected["data"]["totalList"] == {
            "columns": ["name"],
            "rows": [[row["name"]] for row in expected_rows],
        }
    else:
        assert selected["data"]["totalList"] == expected_rows
    assert selected["action_index"] == payload["action_index"]
    assert selected["action_index"]["truncated"] is True
    assert selected["action_index"]["groups"][0]["targets"] == list(range(1, 101))


def test_dotted_columns_preserve_json_types_metadata_and_source() -> None:
    result = CommandResult(
        data={
            "totalList": [
                {"id": 7, "name": "工作流", "detail": {"state": None, "count": 3}},
                {"id": 8, "name": "second", "detail": {"count": 0}},
            ],
            "total": 2,
            "coverage": {"scope_complete": True},
        },
        resolved={"project": {"code": 9}},
    )
    payload = result_payload("project.list", result)
    columns = ("id", "detail.state", "detail.count")
    rendered = render_command(
        payload, action="project.list", options=RenderOptions(columns=columns)
    )
    projected = json.loads(rendered.stdout)
    assert projected["data"]["totalList"] == [
        {"id": 7, "detail": {"state": None, "count": 3}},
        {"id": 8, "detail": {"count": 0}},
    ]
    assert projected["data"]["total"] == 2
    assert projected["data"]["coverage"] == {"scope_complete": True}
    assert projected["resolved"] == payload["resolved"]
    assert isinstance(result.data, dict)
    rows = result.data["totalList"]
    assert isinstance(rows, list)
    assert rows[0] == {"id": 7, "name": "工作流", "detail": {"state": None, "count": 3}}
    tabular = render_command(
        payload,
        action="project.list",
        options=RenderOptions(output_format="tsv", columns=columns),
    )
    assert tabular.stdout == "id\tdetail.state\tdetail.count\n7\t\t3\n8\t\t0\n"


def test_dotted_columns_prefer_literal_keys() -> None:
    payload = result_payload(
        "version", CommandResult(data={"build.id": 7, "build": {"id": 8}})
    )
    rendered = render_command(
        payload, action="version", options=RenderOptions(columns=("build.id",))
    )
    assert json.loads(rendered.stdout)["data"] == {"build.id": 7}


def test_overlapping_parent_and_child_columns_leave_input_unchanged() -> None:
    payload = result_payload(
        "version",
        CommandResult(data={"build": {"id": 8, "flags": [True, None]}}),
    )
    original = json.dumps(payload, sort_keys=True)
    rendered = render_command(
        payload,
        action="version",
        options=RenderOptions(columns=("build", "build.id")),
    )
    assert json.loads(rendered.stdout)["data"] == {
        "build": {"id": 8, "flags": [True, None]}
    }
    assert json.dumps(payload, sort_keys=True) == original


def test_table_uses_display_width_and_expands_nested_details() -> None:
    rows = result_payload(
        "project.list",
        CommandResult(data=[{"name": "中文", "id": 1}, {"name": "hello", "id": 2}]),
    )
    rendered = render_command(
        rows,
        action="project.list",
        options=RenderOptions(output_format="table", columns=("name", "id")),
    )
    positions = {
        cell_len(line.split("|")[0])
        for line in rendered.stdout.splitlines()
        if "|" in line
    }
    assert len(positions) == 1
    details = result_payload(
        "version",
        CommandResult(
            data={
                "runtime": {"version": "3.4.1"},
                "checks": [{"name": "API", "status": "UP"}],
            }
        ),
    )
    rendered = render_command(
        details, action="version", options=RenderOptions(output_format="table")
    )
    assert "runtime\nfield" in rendered.stdout
    assert "checks\nname" in rendered.stdout
    assert '{"' not in rendered.stdout
