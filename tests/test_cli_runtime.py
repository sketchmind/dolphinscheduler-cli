from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

import pytest
import typer

from dsctl.action_policy import preflight_selected_action
from dsctl.cli_runtime import AppState, emit_raw_result, emit_result, set_app_state
from dsctl.errors import ConfigError, MutationOutcomeUnknownError, UserInputError
from dsctl.output import CommandResult, dry_run_result, require_json_object
from dsctl.output_formats import OutputFormat, RenderOptions

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.support.json_types import JsonObject


def test_emit_result_formats_dsctl_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def builder() -> CommandResult:
        message = "Missing required setting: DS_API_URL"
        raise ConfigError(message)

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("context", builder)

    assert exc_info.value.exit_code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "ok": False,
        "action": "context",
        "resolved": {},
        "data": {},
        "error": {
            "type": "config_error",
            "message": "Missing required setting: DS_API_URL",
        },
    }


def test_emit_result_does_not_swallow_unexpected_exceptions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def builder() -> CommandResult:
        message = "boom"
        raise ValueError(message)

    with pytest.raises(ValueError, match="boom"):
        emit_result("context", builder)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_emit_result_preserves_completed_write_when_column_rendering_fails(
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = 0

    def builder() -> CommandResult:
        nonlocal calls
        calls += 1
        return CommandResult(
            data={
                "code": 101,
                "name": "daily-sync",
                "releaseState": "OFFLINE",
                "receipt": {"id": "offline-1"},
            },
            resolved={"workflow": {"code": 101}},
        )

    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(
                output_format="json-compact",
                columns=("workflow", "state"),
            ),
        )
    )

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("workflow.offline", builder)

    assert exc_info.value.exit_code == 1
    assert calls == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["data"] == {
        "code": 101,
        "name": "daily-sync",
        "releaseState": "OFFLINE",
        "receipt": {"id": "offline-1"},
    }
    assert payload["resolved"] == {"workflow": {"code": 101}}
    details = payload["error"]["details"]
    assert details["phase"] == "output_render"
    assert details["result_available"] is True
    assert details["operation_returned_success"] is True
    assert "mutation_applied" not in details
    assert "Do not repeat the command" in payload["error"]["suggestion"]
    assert "retry" not in payload["error"]["suggestion"].lower()


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_text_render_error_preserves_completed_data_and_resolved_target(
    capsys: pytest.CaptureFixture[str],
    output_format: OutputFormat,
) -> None:
    calls = 0

    def builder() -> CommandResult:
        nonlocal calls
        calls += 1
        return CommandResult(
            data={"code": 17, "receipt": {"id": "offline-2"}},
            resolved={"workflow": {"code": 17, "name": "daily-sync"}},
        )

    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(
                output_format=output_format,
                columns=("nonexistent_display_field",),
            ),
        )
    )

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("workflow.offline", builder)

    assert exc_info.value.exit_code == 1
    assert calls == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Error [user_input_error]" in captured.err
    assert "output_render" in captured.err
    assert "offline-2" in captured.err
    assert "Resolved" in captured.err
    assert "daily-sync" in captured.err
    assert "Do not repeat the command" in captured.err


@pytest.mark.parametrize(
    ("action", "result"),
    [
        (
            "version",
            CommandResult(data={"cli": "0.4.0", "ds": "3.4.1"}),
        ),
        (
            "workflow.edit",
            dry_run_result(
                method="PUT",
                path="projects/7/workflows/11",
                extra_data={"workflow": {"code": 11}},
            ),
        ),
    ],
)
def test_post_result_render_error_does_not_claim_a_write(
    capsys: pytest.CaptureFixture[str],
    action: str,
    result: CommandResult,
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(columns=("unknown_column",)),
        )
    )

    with pytest.raises(typer.Exit):
        emit_result(action, lambda: result)

    payload = json.loads(capsys.readouterr().err)
    details = payload["error"]["details"]
    assert details["phase"] == "output_render"
    assert details["result_available"] is True
    assert details["operation_returned_success"] is True
    assert "mutation_applied" not in details
    if action == "workflow.edit":
        assert payload["data"]["dry_run"] is True
        assert payload["warnings"][0]["mutation_sent"] is False


def test_emit_raw_result_preserves_completed_result_when_selector_fails(
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = 0

    def builder() -> CommandResult:
        nonlocal calls
        calls += 1
        return CommandResult(
            data={"text": "workflow: daily-sync\n", "receipt": {"id": 17}},
            resolved={"workflow": {"code": 17}},
        )

    def failed_selector(_result: CommandResult) -> str:
        message = "Raw artifact selector failed"
        raise UserInputError(message)

    with pytest.raises(typer.Exit) as exc_info:
        emit_raw_result("workflow.export", builder, failed_selector)

    assert exc_info.value.exit_code == 1
    assert calls == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["data"]["receipt"] == {"id": 17}
    assert payload["resolved"] == {"workflow": {"code": 17}}
    assert payload["error"]["details"] == {
        "phase": "output_render",
        "result_available": True,
        "operation_returned_success": True,
    }


def test_emit_result_rejects_unsupported_exact_version_before_builder(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")
    set_app_state(AppState(env_file=None, action_preflight=preflight_selected_action))
    called = False

    def builder() -> CommandResult:
        nonlocal called
        called = True
        return CommandResult(data={"created": True})

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("audit.list", builder)

    captured = capsys.readouterr()
    assert exc_info.value.exit_code == 1
    assert called is False
    assert captured.out == ""
    assert json.loads(captured.err)["error"] == {
        "type": "unsupported_feature",
        "message": ("audit.list is unsupported on DolphinScheduler 1.3.9."),
        "details": {
            "action": "audit.list",
            "selected_version": "1.3.9",
            "availability": "unsupported",
            "constraint": (
                "This DolphinScheduler release predates audit-log management "
                "introduced in 3.0.0."
            ),
        },
        "suggestion": (
            "Run `dsctl capabilities --action audit.list` to inspect the "
            "constraint. This operation requires a server version that "
            "supports it; the selected profile must match that server."
        ),
    }


def test_emit_result_gates_remote_task_type_discovery_before_builder(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.0.6")
    set_app_state(AppState(env_file=None, action_preflight=preflight_selected_action))
    called = False

    def builder() -> CommandResult:
        nonlocal called
        called = True
        return CommandResult(data={"taskTypes": []})

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("task-type.list", builder)

    assert exc_info.value.exit_code == 1
    assert called is False
    assert json.loads(capsys.readouterr().err)["error"]["type"] == (
        "unsupported_feature"
    )


def test_emit_raw_result_allows_342_workflow_export(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.2")
    set_app_state(AppState(env_file=None, action_preflight=preflight_selected_action))

    emit_raw_result(
        "workflow.export",
        lambda: CommandResult(data={"text": "workflow: daily-sync\n"}),
        lambda result: str(
            require_json_object(result.data, label="raw export data")["text"]
        ),
    )

    captured = capsys.readouterr()
    assert captured.out == "workflow: daily-sync\n"
    assert captured.err == ""


def test_emit_result_allows_supported_exact_version_action(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.2")
    set_app_state(AppState(env_file=None, action_preflight=preflight_selected_action))
    called = False

    def builder() -> CommandResult:
        nonlocal called
        called = True
        return CommandResult(data={"totalList": [], "total": 0})

    emit_result("project.list", builder)

    assert called is True
    assert json.loads(capsys.readouterr().out)["action"] == "project.list"


def test_compact_list_rejects_dotted_columns_before_service_or_discovery(
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []

    def preflight(_action: str, _env_file: Path | None) -> frozenset[str]:
        calls.append("preflight")
        return frozenset()

    def builder() -> CommandResult:
        calls.append("service")
        return CommandResult(data={"totalList": []})

    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(
                output_format="json-compact", columns=("taskParams.key",)
            ),
            action_preflight=preflight,
        )
    )
    with pytest.raises(typer.Exit):
        emit_result("task-instance.list", builder)
    captured = capsys.readouterr()
    assert calls == []
    assert captured.out == ""
    error = json.loads(captured.err)["error"]
    assert error["type"] == "user_input_error"
    assert "--format json" in error["suggestion"]


def test_emit_result_leaves_unknown_version_diagnostics_to_doctor_builder(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "9.9.9")
    set_app_state(AppState(env_file=None, action_preflight=preflight_selected_action))
    called = False

    def builder() -> CommandResult:
        nonlocal called
        called = True
        return CommandResult(data={"status": "error"})

    emit_result("doctor", builder)

    assert called is True
    assert json.loads(capsys.readouterr().out)["data"] == {"status": "error"}


def test_emit_raw_result_rejects_unsupported_action_before_builder(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")
    set_app_state(AppState(env_file=None, action_preflight=preflight_selected_action))
    called = False

    def builder() -> CommandResult:
        nonlocal called
        called = True
        return CommandResult(data={"text": "artifact"})

    with pytest.raises(typer.Exit):
        emit_raw_result("audit.list", builder, lambda result: str(result.data))

    assert called is False
    assert json.loads(capsys.readouterr().err)["error"]["type"] == (
        "unsupported_feature"
    )


def test_emit_raw_result_writes_errors_only_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def builder() -> CommandResult:
        message = "Missing required setting: DS_API_URL"
        raise ConfigError(message)

    with pytest.raises(typer.Exit) as exc_info:
        emit_raw_result("workflow.export", builder, lambda result: str(result.data))

    captured = capsys.readouterr()
    assert exc_info.value.exit_code == 1
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["type"] == "config_error"


def test_emit_raw_result_keeps_existing_operation_failure_without_selecting_body(
    capsys: pytest.CaptureFixture[str],
) -> None:
    selector_calls = 0

    def selector(_result: CommandResult) -> str:
        nonlocal selector_calls
        selector_calls += 1
        return "must-not-render"

    failure = ConfigError("Export preparation failed")
    with pytest.raises(typer.Exit) as exc_info:
        emit_raw_result(
            "workflow.export",
            lambda: CommandResult(
                data={"diagnostics": ["definition unavailable"]},
                failure=failure,
            ),
            selector,
        )

    assert exc_info.value.exit_code == 1
    assert selector_calls == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["data"] == {"diagnostics": ["definition unavailable"]}
    assert payload["error"] == {
        "type": "config_error",
        "message": "Export preparation failed",
    }


def test_emit_raw_result_validates_render_options_before_builder(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="table", columns=("*", "name")),
        )
    )
    called = False

    def builder() -> CommandResult:
        nonlocal called
        called = True
        return CommandResult(data={"text": "artifact"})

    try:
        with pytest.raises(typer.Exit) as exc_info:
            emit_raw_result(
                "task-instance.log",
                builder,
                lambda result: str(result.data),
            )

        captured = capsys.readouterr()
        assert exc_info.value.exit_code == 1
        assert called is False
        assert captured.out == ""
        assert "--columns" in captured.err
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_raw_result_preserves_exact_body_with_compact_json_enabled(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="json-compact"),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(data={"text": "line-1\nline-2"})

    try:
        emit_raw_result(
            "task-instance.log",
            builder,
            lambda result: "line-1\nline-2",
        )
        captured = capsys.readouterr()
        assert captured.out == "line-1\nline-2"
        assert captured.err == ""
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_can_render_table_rows(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(
                output_format="table",
                columns=("code", "name"),
            ),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={
                "totalList": [{"code": 101, "name": "etl-prod", "description": "demo"}],
                "total": 1,
            }
        )

    try:
        emit_result("project.list", builder)
        assert capsys.readouterr().out == (
            "code | name\n-----+---------\n101  | etl-prod\n"
        )
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_uses_datasource_list_defaults_without_owner_user_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="table"),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={
                "totalList": [
                    {
                        "id": 7,
                        "name": "warehouse",
                        "type": "MYSQL",
                        "userName": "admin",
                        "createTime": "2026-04-19 10:00:00",
                    }
                ],
                "total": 1,
            }
        )

    try:
        emit_result("datasource.list", builder)
        assert capsys.readouterr().out == (
            "id | name      | type  | createTime\n"
            "---+-----------+-------+--------------------\n"
            "7  | warehouse | MYSQL | 2026-04-19 10:00:00\n"
        )
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_can_render_empty_table_with_default_columns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="table"),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(data={"totalList": [], "total": 0})

    try:
        emit_result("cluster.list", builder)
        assert capsys.readouterr().out == (
            "code | name | config\n-----+------+-------\n"
        )
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_reports_page_metadata_to_stderr_for_table_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="table"),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={
                "totalList": [
                    {"code": 101, "name": "etl-prod", "description": "demo"},
                    {"code": 102, "name": "stock-etl", "description": "demo"},
                ],
                "total": 10,
                "totalPage": 5,
                "pageNo": 1,
                "pageSize": 2,
                "currentPage": 1,
            }
        )

    try:
        emit_result("project.list", builder)
        captured = capsys.readouterr()
        assert "etl-prod" in captured.out
        assert "stock-etl" in captured.out
        assert captured.err == "page: 1/5; showing 2 of 10 rows\n"
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_reports_structured_warnings_to_stderr_for_row_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(
                output_format="tsv",
                columns=("id", "name"),
            ),
        )
    )

    def builder() -> CommandResult:
        message = (
            "dry run: no mutation was sent; lookup and verification reads may occur"
        )
        return CommandResult(
            data=[{"id": 7, "name": "extract"}],
            warnings=[message],
            warning_details=[
                {
                    "code": "dry_run_no_mutation_sent",
                    "message": message,
                    "mutation_sent": False,
                }
            ],
        )

    try:
        emit_result("task-instance.list", builder)
        captured = capsys.readouterr()
        assert captured.out.startswith("id\tname\n")
        assert captured.err == (
            "warning[dry_run_no_mutation_sent]: dry run: no mutation was sent; "
            "lookup and verification reads may occur\n"
        )
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_renders_compact_utf8_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="json-compact"),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(data={"name": "无人值守测试"})

    try:
        emit_result("project.get", builder)
        captured = capsys.readouterr()
        assert captured.err == ""
        assert captured.out.count("\n") == 1
        assert "无人值守测试" in captured.out
        assert "\\u" not in captured.out
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_preserves_explicit_env_file_in_next_actions(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "test cluster.env"
    env_file.write_text("DS_VERSION=3.4.1\n", encoding="utf-8")
    set_app_state(
        AppState(
            env_file=env_file,
            render_options=RenderOptions(output_format="json-compact"),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={"workflowInstanceIds": [242]},
            resolved={"project": {"code": 7}},
        )

    try:
        emit_result("workflow.run", builder)
        payload = json.loads(capsys.readouterr().out)
        command = payload["next_actions"][0]["command"]
        assert shlex.split(command) == [
            "dsctl",
            "--env-file",
            str(env_file),
            "--format",
            "json-compact",
            "--columns",
            "id,name,state,startTime,endTime,duration",
            "workflow-instance",
            "watch",
            "242",
            "--project",
            "7",
        ]
    finally:
        set_app_state(AppState(env_file=None))


@pytest.mark.parametrize("context_name", [None, "production"])
def test_unknown_stop_outcome_preserves_target_in_reconciliation_actions(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    context_name: str | None,
) -> None:
    env_file = tmp_path / "test cluster.env"
    env_file.write_text("DS_VERSION=3.4.3\n", encoding="utf-8")

    def annotate_selection(payload: JsonObject, selected: Path | None) -> JsonObject:
        assert selected == env_file
        selection = {"context": context_name, "env_file": str(env_file)}
        resolved = require_json_object(
            payload["resolved"],
            label="test error resolved",
        )
        return {
            **payload,
            "resolved": {**resolved, "selection": selection},
        }

    set_app_state(
        AppState(
            env_file=env_file,
            error_postprocess=annotate_selection,
            render_options=RenderOptions(output_format="json-compact"),
        )
    )

    def builder() -> CommandResult:
        message = "Workflow-instance stop outcome is unknown"
        raise MutationOutcomeUnknownError(
            message,
            details={
                "known_resources": {
                    "project": {"code": 7, "name": "etl-prod"},
                    "workflowInstance": {"id": 901},
                }
            },
        )

    try:
        with pytest.raises(typer.Exit) as exc_info:
            emit_result("workflow-instance.stop", builder)
        assert exc_info.value.exit_code == 1
        payload = json.loads(capsys.readouterr().err)
        assert [item["action"] for item in payload["next_actions"]] == [
            "workflow-instance.get",
            "workflow-instance.watch",
        ]
        commands = [shlex.split(item["command"]) for item in payload["next_actions"]]
        expected_prefix = (
            ["dsctl", "--env-file", str(env_file)]
            if context_name is None
            else ["dsctl", "--context", context_name]
        )
        assert all(
            command[: len(expected_prefix)] == expected_prefix for command in commands
        )
        assert all(command[-2:] == ["--project", "7"] for command in commands)
        if context_name is not None:
            assert all("--env-file" not in command for command in commands)
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_columns_wildcard_renders_all_row_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="tsv", columns=("*",)),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={
                "totalList": [
                    {"id": 7, "name": "extract", "state": "SUCCESS"},
                    {"id": 8, "name": "load", "host": "worker-1"},
                ],
                "total": 2,
            }
        )

    try:
        emit_result("task-instance.list", builder)
        assert capsys.readouterr().out == (
            "id\tname\tstate\thost\n7\textract\tSUCCESS\t\n8\tload\t\tworker-1\n"
        )
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_projects_json_columns_for_page_rows(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(columns=("id", "name")),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={
                "totalList": [
                    {"id": 7, "name": "extract", "state": "SUCCESS"},
                    {"id": 8, "name": "load", "host": "worker-1"},
                ],
                "total": 2,
            }
        )

    try:
        emit_result("task-instance.list", builder)
        payload = json.loads(capsys.readouterr().out)
        assert payload["data"] == {
            "total": 2,
            "totalList": [
                {"id": 7, "name": "extract"},
                {"id": 8, "name": "load"},
            ],
        }
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_projects_json_columns_for_object_data(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(columns=("cli", "ds")),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(
            data={
                "cli": "0.4.0",
                "ds": "3.4.1",
                "family": "workflow-3.3-plus",
            }
        )

    try:
        emit_result("version", builder)
        payload = json.loads(capsys.readouterr().out)
        assert payload["data"] == {"cli": "0.4.0", "ds": "3.4.1"}
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_rejects_mixed_columns_wildcard(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="table", columns=("*", "id")),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(data={"totalList": [{"id": 7}]})

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("task-instance.list", builder)

    assert exc_info.value.exit_code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    output = captured.err
    assert "Error [user_input_error]" in output
    assert "user_input_error" in output
    assert "--columns '*' cannot be combined with explicit columns" in output
