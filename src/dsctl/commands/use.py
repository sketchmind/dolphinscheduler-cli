from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result
from dsctl.context import ContextScope
from dsctl.services.use import clear_context_result, set_context_value_result

use_app = typer.Typer(
    help="Set or clear local CLI context without remote validation.",
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
            help="Clear the entire selected context scope.",
        ),
    ] = False,
    scope: Annotated[
        ContextScope | None,
        typer.Option(
            "--scope",
            help=(
                "Select which persisted context layer `use --clear` updates. "
                "Defaults to project."
            ),
        ),
    ] = None,
) -> None:
    """Clear all context in one scope when no subcommand is given."""
    if ctx.invoked_subcommand is not None:
        if clear:
            message = (
                "--clear before a use subcommand is ambiguous; use "
                "`dsctl use TARGET --clear` without NAME"
            )
            raise typer.BadParameter(message)
        if scope is not None:
            message = "Place --scope after the use subcommand"
            raise typer.BadParameter(message)
        return
    if not clear:
        message = "use requires a target subcommand or --clear"
        raise typer.BadParameter(message)
    emit_result(
        "use.clear",
        lambda: clear_context_result(scope=scope or "project"),
    )


@use_app.command("project")
def use_project_command(
    name: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Project name to persist for later commands. Run `dsctl "
                "project list` to discover values."
            ),
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
    """Set or clear local project context; no remote validation."""
    if clear:
        if name is not None:
            message = "NAME cannot be combined with --clear"
            raise typer.BadParameter(message)
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
            help=(
                "Workflow name to persist for later commands. Run `dsctl "
                "workflow list` in the selected project to discover values. "
                "Requires a project binding: pass --project; otherwise project "
                "scope uses the effective project, while user scope uses its own "
                "stored project."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name to bind with the workflow in the selected context "
                "scope. Run `dsctl project list` to discover values."
            ),
        ),
    ] = None,
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
    """Set or clear local workflow context; no remote validation."""
    if clear:
        if name is not None:
            message = "NAME cannot be combined with --clear"
            raise typer.BadParameter(message)
        if project is not None:
            message = "--project cannot be combined with --clear"
            raise typer.BadParameter(message)
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
        lambda: set_context_value_result(
            "workflow",
            name,
            project=project,
            scope=scope,
        ),
    )
