import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.task import get_task_result, list_tasks_result, update_task_result

task_app = typer.Typer(
    help="Manage DolphinScheduler task definitions inside workflows.",
    no_args_is_help=True,
)


def register_task_commands(app: typer.Typer) -> None:
    """Register the `task` command group."""
    app.add_typer(task_app, name="task")


@task_app.command("list")
@bind_command("task.list")
def list_command(
    ctx: typer.Context,
    *,
    project: str | None,
    workflow: str,
    search: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task.list",
        lambda: list_tasks_result(
            project=project,
            workflow=workflow,
            search=search,
            env_file=env_file,
        ),
    )


@task_app.command("get")
@bind_command("task.get")
def get_command(
    ctx: typer.Context,
    task: str,
    *,
    project: str | None,
    workflow: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task.get",
        lambda: get_task_result(
            task,
            project=project,
            workflow=workflow,
            env_file=env_file,
        ),
    )


@task_app.command("update")
@bind_command("task.update")
def update_command(
    ctx: typer.Context,
    task: str,
    *,
    project: str | None,
    workflow: str,
    set_values: list[str] | None,
    dry_run: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task.update",
        lambda: update_task_result(
            task,
            project=project,
            workflow=workflow,
            set_values=[] if set_values is None else set_values,
            dry_run=dry_run,
            env_file=env_file,
        ),
    )
