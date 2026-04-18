import json

import pytest
import typer

from dsctl.cli_runtime import AppState, emit_result, set_app_state
from dsctl.errors import ConfigError
from dsctl.output import CommandResult
from dsctl.output_formats import RenderOptions


def test_emit_result_formats_dsctl_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def builder() -> CommandResult:
        message = "Missing required setting: DS_API_URL"
        raise ConfigError(message)

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("context", builder)

    assert exc_info.value.exit_code == 1
    assert json.loads(capsys.readouterr().out) == {
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

    assert capsys.readouterr().out == ""


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
    output = capsys.readouterr().out
    assert "error.type" in output
    assert "user_input_error" in output
    assert "--columns '*' cannot be combined with explicit columns" in output
