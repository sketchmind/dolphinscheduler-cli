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
    "Print a bounded field contract for one task type; select detailed views "
    "explicitly."
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
    *,
    field: Annotated[
        str | None,
        typer.Option(
            "--field",
            help=(
                "Return one exact authoring field and its related state rules. "
                "Discover paths with the default bounded field view; quote paths "
                "containing []."
            ),
        ),
    ] = None,
    json_schema: Annotated[
        bool,
        typer.Option(
            "--json-schema",
            help="Return the nested JSON Schema without repeated authoring metadata.",
        ),
    ] = False,
    compile_mappings: Annotated[
        bool,
        typer.Option(
            "--compile-mappings",
            help="Return authoring-path to DS REST payload mappings.",
        ),
    ] = False,
    full: Annotated[
        bool,
        typer.Option(
            "--full",
            help="Return the former expanded authoring contract for compatibility.",
        ),
    ] = False,
) -> None:
    """Print one progressive local task authoring view."""
    emit_result(
        "task-type.schema",
        lambda: task_type_schema_result(
            task_type,
            field=field,
            json_schema=json_schema,
            compile_mappings=compile_mappings,
            full=full,
        ),
    )


__all__ = ["register_task_type_commands"]
