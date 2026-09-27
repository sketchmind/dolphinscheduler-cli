from pathlib import Path

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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
@bind_command("project-preference.get")
def get_command(
    ctx: typer.Context,
    *,
    project: str | None,
) -> None:
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
@bind_command("project-preference.update")
def update_command(
    ctx: typer.Context,
    *,
    project: str | None,
    preferences_json: str | None,
    file: Path | None,
) -> None:
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
@bind_command("project-preference.enable")
def enable_command(
    ctx: typer.Context,
    *,
    project: str | None,
) -> None:
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
@bind_command("project-preference.disable")
def disable_command(
    ctx: typer.Context,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-preference.disable",
        lambda: disable_project_preference_result(
            project=project,
            env_file=env_file,
        ),
    )
