import ast
import inspect
import json
import textwrap
from collections.abc import Callable

import pytest
import typer
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.cli_runtime import emit_raw_result, emit_result
from dsctl.cli_surface import (
    COMMAND_GROUPS,
    RESOURCE_COMMAND_TREE,
    TOP_LEVEL_COMMANDS,
    SurfaceCommand,
)
from dsctl.commands import audit as audit_commands
from dsctl.commands import task_type as task_type_commands


def test_app_registration_matches_shared_cli_surface() -> None:
    assert [command.name for command in app.registered_commands] == list(
        TOP_LEVEL_COMMANDS
    )
    assert [group.name for group in app.registered_groups] == list(COMMAND_GROUPS)
    for group in app.registered_groups:
        assert group.name is not None
        assert (
            _typer_surface(_require_typer(group.typer_instance))
            == (RESOURCE_COMMAND_TREE[group.name])
        )


def test_registered_command_callbacks_use_shared_json_emitter() -> None:
    for path, callback in _registered_command_callbacks(app):
        assert callback.__globals__.get("emit_result") is emit_result, path

        callback_ast = ast.parse(textwrap.dedent(inspect.getsource(callback)))
        call_names = {
            node.func.id
            for node in ast.walk(callback_ast)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        attribute_calls = {
            f"{node.func.value.id}.{node.func.attr}"
            for node in ast.walk(callback_ast)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
        }

        assert "emit_result" in call_names or "emit_raw_result" in call_names, path
        if "emit_raw_result" in call_names:
            assert callback.__globals__.get("emit_raw_result") is emit_raw_result, path
        assert "print_json" not in call_names, path
        assert "result_payload" not in call_names, path
        assert "error_payload" not in call_names, path
        assert "typer.echo" not in attribute_calls, path


def test_app_injects_exact_version_policy_before_remote_command_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.0.6")

    result = CliRunner().invoke(app, ["task-type", "list"])

    assert result.exit_code == 1
    assert json.loads(result.stderr)["error"]["type"] == "unsupported_feature"


@pytest.mark.parametrize(
    ("arguments", "module", "builder_name"),
    [
        (["task-type", "list"], task_type_commands, "list_task_types_result"),
        (
            ["audit", "list"],
            audit_commands,
            "list_audit_logs_result",
        ),
    ],
)
def test_legacy_profile_rejects_unsupported_actions_before_builder(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    module: object,
    builder_name: str,
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")
    called = False

    def fail_builder(*args: object, **kwargs: object) -> None:
        del args, kwargs
        nonlocal called
        called = True
        message = "unsupported action builder must not run"
        raise AssertionError(message)

    monkeypatch.setattr(module, builder_name, fail_builder)

    result = CliRunner().invoke(app, arguments)

    assert result.exit_code == 1
    assert called is False
    error = json.loads(result.stderr)["error"]
    assert error["type"] == "unsupported_feature"
    assert error["details"]["selected_version"] == "1.3.9"


def _typer_surface(app: typer.Typer) -> tuple[SurfaceCommand, ...]:
    command_nodes = tuple(
        SurfaceCommand(name=command.name)
        for command in app.registered_commands
        if command.name is not None
    )
    group_nodes = tuple(
        SurfaceCommand(
            name=group.name,
            commands=_typer_surface(_require_typer(group.typer_instance)),
        )
        for group in app.registered_groups
        if group.name is not None
    )
    return (*command_nodes, *group_nodes)


def _registered_command_callbacks(
    app: typer.Typer,
    *,
    prefix: str = "",
) -> list[tuple[str, Callable[..., object]]]:
    callbacks: list[tuple[str, Callable[..., object]]] = []
    for command in app.registered_commands:
        if command.name is None or command.callback is None:
            continue
        callbacks.append((prefix + command.name, command.callback))
    for group in app.registered_groups:
        if group.name is None or group.typer_instance is None:
            continue
        callbacks.extend(
            _registered_command_callbacks(
                group.typer_instance,
                prefix=f"{prefix}{group.name} ",
            )
        )
    return callbacks


def _require_typer(app: typer.Typer | None) -> typer.Typer:
    assert app is not None
    return app
