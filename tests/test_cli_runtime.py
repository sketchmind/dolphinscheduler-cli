from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

import pytest
import typer

from dsctl.cli_runtime import AppState, emit_raw_result, emit_result, set_app_state
from dsctl.errors import ConfigError
from dsctl.output import CommandResult
from dsctl.output_formats import RenderOptions

if TYPE_CHECKING:
    from pathlib import Path


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
        "warnings": [],
        "warning_details": [],
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


def test_emit_raw_result_validates_render_options_before_builder(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(output_format="table", compact=True),
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
        assert "--compact can only be used" in captured.err
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_raw_result_preserves_exact_body_with_compact_json_enabled(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(compact=True),
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
        message = "dry run: no request was sent"
        return CommandResult(
            data=[{"id": 7, "name": "extract"}],
            warnings=[message],
            warning_details=[
                {
                    "code": "dry_run_no_request_sent",
                    "message": message,
                    "request_sent": False,
                }
            ],
        )

    try:
        emit_result("task-instance.list", builder)
        captured = capsys.readouterr()
        assert captured.out.startswith("id\tname\n")
        assert captured.err == (
            "warning[dry_run_no_request_sent]: dry run: no request was sent\n"
        )
    finally:
        set_app_state(AppState(env_file=None))


def test_emit_result_renders_compact_utf8_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    set_app_state(
        AppState(
            env_file=None,
            render_options=RenderOptions(compact=True),
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
    set_app_state(
        AppState(
            env_file=env_file,
            render_options=RenderOptions(compact=True),
        )
    )

    def builder() -> CommandResult:
        return CommandResult(data={"workflowInstanceIds": [242]})

    try:
        emit_result("workflow.run", builder)
        payload = json.loads(capsys.readouterr().out)
        command = payload["next_actions"][0]["command"]
        assert shlex.split(command) == [
            "dsctl",
            "--env-file",
            str(env_file),
            "--compact",
            "--columns",
            "id,name,state,startTime,endTime,duration",
            "workflow-instance",
            "watch",
            "242",
        ]
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
                "cli": "0.3.0",
                "ds": "3.4.1",
                "family": "workflow-3.3-plus",
            }
        )

    try:
        emit_result("version", builder)
        payload = json.loads(capsys.readouterr().out)
        assert payload["data"] == {"cli": "0.3.0", "ds": "3.4.1"}
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
    assert "error.type" in output
    assert "user_input_error" in output
    assert "--columns '*' cannot be combined with explicit columns" in output
