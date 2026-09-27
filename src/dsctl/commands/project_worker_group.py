import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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
@bind_command("project-worker-group.list")
def list_command(
    ctx: typer.Context,
    *,
    project: str | None,
) -> None:
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
@bind_command("project-worker-group.set")
def set_command(
    ctx: typer.Context,
    *,
    project: str | None,
    worker_groups: list[str] | None,
) -> None:
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
@bind_command("project-worker-group.clear")
def clear_command(
    ctx: typer.Context,
    *,
    project: str | None,
    force: bool,
) -> None:
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
