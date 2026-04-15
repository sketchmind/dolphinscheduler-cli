from __future__ import annotations

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.task_type import list_task_types_result

task_type_app = typer.Typer(
    help="Discover DolphinScheduler task types for the current runtime.",
    no_args_is_help=True,
)


def register_task_type_commands(app: typer.Typer) -> None:
    """Register the `task-type` command group."""
    app.add_typer(task_type_app, name="task-type")


@task_type_app.command("list")
def list_command(ctx: typer.Context) -> None:
    """List DS task types plus the current user's favourite flags."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-type.list",
        lambda: list_task_types_result(env_file=env_file),
    )


__all__ = ["register_task_type_commands"]
