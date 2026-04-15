import pytest
import typer

from dsctl.cli_runtime import emit_result
from dsctl.errors import ConfigError
from dsctl.output import CommandResult


def test_emit_result_formats_dsctl_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    printed: list[object] = []
    monkeypatch.setattr("dsctl.cli_runtime.print_json", printed.append)

    def builder() -> CommandResult:
        message = "Missing required setting: DS_API_URL"
        raise ConfigError(message)

    with pytest.raises(typer.Exit) as exc_info:
        emit_result("context", builder)

    assert exc_info.value.exit_code == 1
    assert printed == [
        {
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
    ]


def test_emit_result_does_not_swallow_unexpected_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    printed: list[object] = []
    monkeypatch.setattr("dsctl.cli_runtime.print_json", printed.append)

    def builder() -> CommandResult:
        message = "boom"
        raise ValueError(message)

    with pytest.raises(ValueError, match="boom"):
        emit_result("context", builder)

    assert printed == []
