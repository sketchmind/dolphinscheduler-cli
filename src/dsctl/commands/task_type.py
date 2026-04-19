from __future__ import annotations

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.task_type import list_task_types_result

TASK_TYPE_LIST_HELP = (
    "List live DS task types, categories, favourite flags, and CLI authoring coverage."
)

task_type_app = typer.Typer(
    help="List live DS task-type catalog for the configured cluster and current user.",
    no_args_is_help=True,
)


def register_task_type_commands(app: typer.Typer) -> None:
    """Register the `task-type` command group."""
    app.add_typer(task_type_app, name="task-type")


@task_type_app.command("list", help=TASK_TYPE_LIST_HELP)
def list_command(ctx: typer.Context) -> None:
    """List the live DS task-type catalog."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-type.list",
        lambda: list_task_types_result(env_file=env_file),
    )


__all__ = ["register_task_type_commands"]
