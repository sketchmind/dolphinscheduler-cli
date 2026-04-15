from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result
from dsctl.context import ContextScope
from dsctl.services.use import clear_context_result, set_context_value_result

use_app = typer.Typer(
    help="Set or clear persisted CLI context.",
    invoke_without_command=True,
    no_args_is_help=True,
)


def register_use_commands(app: typer.Typer) -> None:
    """Register the `use` command group."""
    app.add_typer(use_app, name="use")


@use_app.callback()
def use_callback(
    ctx: typer.Context,
    *,
    clear: Annotated[
        bool,
        typer.Option(
            "--clear",
            help="Clear the selected context scope or target value.",
        ),
    ] = False,
    scope: Annotated[
        ContextScope,
        typer.Option(
            "--scope",
            help="Select which persisted context layer to update.",
        ),
    ] = "project",
) -> None:
    """Clear all context in one scope when no subcommand is given."""
    if ctx.invoked_subcommand is not None:
        return
    if not clear:
        message = "use requires a target subcommand or --clear"
        raise typer.BadParameter(message)
    emit_result(
        "use.clear",
        lambda: clear_context_result(scope=scope),
    )


@use_app.command("project")
def use_project_command(
    name: Annotated[
        str | None,
        typer.Argument(
            help="Project name to persist for later commands.",
        ),
    ] = None,
    *,
    clear: Annotated[
        bool,
        typer.Option(
            "--clear",
            help=(
                "Clear the stored project context and any workflow context beneath it."
            ),
        ),
    ] = False,
    scope: Annotated[
        ContextScope,
        typer.Option(
            "--scope",
            help="Select which persisted context layer to update.",
        ),
    ] = "project",
) -> None:
    """Set or clear the project context."""
    if clear:
        emit_result(
            "use.project",
            lambda: clear_context_result(target="project", scope=scope),
        )
        return
    if name is None:
        message = "project name is required unless --clear is used"
        raise typer.BadParameter(message)
    emit_result(
        "use.project",
        lambda: set_context_value_result("project", name, scope=scope),
    )


@use_app.command("workflow")
def use_workflow_command(
    name: Annotated[
        str | None,
        typer.Argument(
            help="Workflow name to persist for later commands.",
        ),
    ] = None,
    *,
    clear: Annotated[
        bool,
        typer.Option(
            "--clear",
            help="Clear the stored workflow context.",
        ),
    ] = False,
    scope: Annotated[
        ContextScope,
        typer.Option(
            "--scope",
            help="Select which persisted context layer to update.",
        ),
    ] = "project",
) -> None:
    """Set or clear the workflow context."""
    if clear:
        emit_result(
            "use.workflow",
            lambda: clear_context_result(target="workflow", scope=scope),
        )
        return
    if name is None:
        message = "workflow name is required unless --clear is used"
        raise typer.BadParameter(message)
    emit_result(
        "use.workflow",
        lambda: set_context_value_result("workflow", name, scope=scope),
    )
