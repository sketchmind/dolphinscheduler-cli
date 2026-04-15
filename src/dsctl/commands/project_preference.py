from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.project_preference import (
    disable_project_preference_result,
    enable_project_preference_result,
    get_project_preference_result,
    update_project_preference_result,
)

project_preference_app = typer.Typer(
    help=(
        "Manage DolphinScheduler project preferences as a project-level "
        "default-value source."
    ),
    no_args_is_help=True,
)


def register_project_preference_commands(app: typer.Typer) -> None:
    """Register the `project-preference` command group."""
    app.add_typer(project_preference_app, name="project-preference")


@project_preference_app.command("get")
def get_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Get the singleton project preference default source for one selected project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-preference.get",
        lambda: get_project_preference_result(
            project=project,
            env_file=env_file,
        ),
    )


@project_preference_app.command("update")
def update_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    preferences_json: Annotated[
        str | None,
        typer.Option(
            "--preferences-json",
            help="Inline JSON object used as the DS project preference payload.",
        ),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option(
            "--file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help="Path to one JSON object file for the DS project preference payload.",
            readable=True,
            resolve_path=True,
        ),
    ] = None,
) -> None:
    """Create or update the selected project-level default-value source."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-preference.update",
        lambda: update_project_preference_result(
            project=project,
            preferences_json=preferences_json,
            file=file,
            env_file=env_file,
        ),
    )


@project_preference_app.command("enable")
def enable_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Enable the selected project preference default-value source."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-preference.enable",
        lambda: enable_project_preference_result(
            project=project,
            env_file=env_file,
        ),
    )


@project_preference_app.command("disable")
def disable_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Disable the selected project preference default-value source."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-preference.disable",
        lambda: disable_project_preference_result(
            project=project,
            env_file=env_file,
        ),
    )
