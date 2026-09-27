import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.task_type import (
    list_task_types_result,
    task_type_schema_result,
    task_type_summary_result,
)

task_type_app = typer.Typer(
    help="Discover DS task types and local task authoring contracts.",
    no_args_is_help=True,
)


def register_task_type_commands(app: typer.Typer) -> None:
    """Register the `task-type` command group."""
    app.add_typer(task_type_app, name="task-type")


@task_type_app.command("list")
@bind_command("task-type.list")
def list_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-type.list",
        lambda: list_task_types_result(env_file=env_file),
    )


@task_type_app.command("get")
@bind_command("task-type.get")
def get_command(
    ctx: typer.Context,
    task_type: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-type.get",
        lambda: task_type_summary_result(task_type, env_file=env_file),
    )


@task_type_app.command("schema")
@bind_command("task-type.schema")
def schema_command(
    ctx: typer.Context,
    task_type: str,
    *,
    field: str | None,
    json_schema: bool,
    compile_mappings: bool,
    full: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-type.schema",
        lambda: task_type_schema_result(
            task_type,
            field=field,
            json_schema=json_schema,
            compile_mappings=compile_mappings,
            full=full,
            env_file=env_file,
        ),
    )


__all__ = ["register_task_type_commands"]
