import ast
import inspect
import textwrap
from collections.abc import Callable

import typer

from dsctl.app import app
from dsctl.cli_runtime import emit_result
from dsctl.cli_surface import (
    COMMAND_GROUPS,
    RESOURCE_COMMAND_TREE,
    TOP_LEVEL_COMMANDS,
    SurfaceCommand,
)


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

        assert "emit_result" in call_names, path
        assert "print_json" not in call_names, path
        assert "success_payload" not in call_names, path
        assert "error_payload" not in call_names, path
        assert "typer.echo" not in attribute_calls, path


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
