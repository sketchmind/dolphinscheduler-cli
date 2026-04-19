from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.project_worker_group import (
    clear_project_worker_groups_result,
    list_project_worker_groups_result,
    set_project_worker_groups_result,
)

project_worker_group_app = typer.Typer(
    help="Manage DolphinScheduler project worker-group assignments.",
    no_args_is_help=True,
)


def register_project_worker_group_commands(app: typer.Typer) -> None:
    """Register the `project-worker-group` command group."""
    app.add_typer(project_worker_group_app, name="project-worker-group")


@project_worker_group_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
) -> None:
    """List the worker groups currently reported for one selected project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-worker-group.list",
        lambda: list_project_worker_groups_result(
            project=project,
            env_file=env_file,
        ),
    )


@project_worker_group_app.command("set")
def set_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
    worker_groups: Annotated[
        list[str] | None,
        typer.Option(
            "--worker-group",
            help=(
                "Worker group to keep assigned to this project. Repeat as "
                "needed; run `dsctl worker-group list` to discover values."
            ),
        ),
    ] = None,
) -> None:
    """Replace the explicit worker-group assignment set for one selected project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-worker-group.set",
        lambda: set_project_worker_groups_result(
            project=project,
            worker_groups=[] if worker_groups is None else worker_groups,
            env_file=env_file,
        ),
    )


@project_worker_group_app.command("clear")
def clear_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm removal of all explicit project worker-group assignments.",
        ),
    ] = False,
) -> None:
    """Clear the explicit worker-group assignment set for one selected project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-worker-group.clear",
        lambda: clear_project_worker_groups_result(
            project=project,
            force=force,
            env_file=env_file,
        ),
    )
