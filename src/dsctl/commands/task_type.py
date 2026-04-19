from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.task_type import (
    list_task_types_result,
    task_type_schema_result,
    task_type_summary_result,
)

TASK_TYPE_LIST_HELP = (
    "List live DS task types, categories, favourite flags, and CLI authoring coverage."
)
TASK_TYPE_GET_HELP = "Summarize the local authoring contract for one task type."
TASK_TYPE_SCHEMA_HELP = (
    "Print the full local authoring schema for one task type, including fields, "
    "state rules, choices, and compile mapping."
)

task_type_app = typer.Typer(
    help="Discover DS task types and local task authoring contracts.",
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


@task_type_app.command("get", help=TASK_TYPE_GET_HELP)
def get_command(
    task_type: Annotated[
        str,
        typer.Argument(
            help=(
                "Task type to inspect. Discover values with `dsctl template task` "
                "or the live catalog with `dsctl task-type list`."
            ),
        ),
    ],
) -> None:
    """Summarize one local task authoring contract."""
    emit_result(
        "task-type.get",
        lambda: task_type_summary_result(task_type),
    )


@task_type_app.command("schema", help=TASK_TYPE_SCHEMA_HELP)
def schema_command(
    task_type: Annotated[
        str,
        typer.Argument(
            help=(
                "Task type whose local authoring schema should be printed. "
                "Discover values with `dsctl template task`."
            ),
        ),
    ],
) -> None:
    """Print the full local task authoring schema."""
    emit_result(
        "task-type.schema",
        lambda: task_type_schema_result(task_type),
    )


__all__ = ["register_task_type_commands"]
