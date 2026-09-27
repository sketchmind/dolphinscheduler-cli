from pathlib import Path

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.context import (
    create_context_result,
    delete_context_result,
    get_saved_context_result,
    list_contexts_result,
    update_context_result,
)
from dsctl.services.meta import get_context_result

context_app = typer.Typer(invoke_without_command=True)


def register_context_commands(app: typer.Typer) -> None:
    """Register effective inspection and saved context management."""
    app.add_typer(context_app, name="context")


@context_app.callback()
@bind_command("context")
def context_command(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is not None:
        return
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("context", lambda: get_context_result(env_file=env_file))


@context_app.command("list")
@bind_command("context.list")
def list_command() -> None:
    emit_result("context.list", list_contexts_result)


@context_app.command("get")
@bind_command("context.get")
def get_command(name: str) -> None:
    emit_result("context.get", lambda: get_saved_context_result(name))


@context_app.command("create")
@bind_command("context.create")
def create_command(name: str, *, file: Path, project: str | None) -> None:
    emit_result(
        "context.create",
        lambda: create_context_result(name, file=file, project=project),
    )


@context_app.command("update")
@bind_command("context.update")
def update_command(
    name: str, *, file: Path | None, project: str | None, clear_project: bool
) -> None:
    emit_result(
        "context.update",
        lambda: update_context_result(
            name, file=file, project=project, clear_project=clear_project
        ),
    )


@context_app.command("delete")
@bind_command("context.delete")
def delete_command(name: str) -> None:
    emit_result("context.delete", lambda: delete_context_result(name))
