from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.task import get_task_result, list_tasks_result, update_task_result

task_app = typer.Typer(
    help="Manage DolphinScheduler task definitions inside workflows.",
    no_args_is_help=True,
)

PROJECT_HELP = (
    "Project name or code. Run `dsctl project list` to discover values; falls "
    "back to stored project context."
)
TASK_HELP = "Task name or numeric code. Use `dsctl task list` to discover values."
WORKFLOW_HELP = (
    "Workflow name or code. Run `dsctl workflow list` in the selected project "
    "to discover values; falls back to workflow context."
)


def register_task_commands(app: typer.Typer) -> None:
    """Register the `task` command group."""
    app.add_typer(task_app, name="task")


@task_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_HELP,
        ),
    ] = None,
    workflow: Annotated[
        str | None,
        typer.Option(
            "--workflow",
            help=WORKFLOW_HELP,
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help=(
                "Filter tasks by name substring after fetching the workflow task list."
            ),
        ),
    ] = None,
) -> None:
    """List tasks inside one workflow."""
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
def get_command(
    ctx: typer.Context,
    task: Annotated[
        str,
        typer.Argument(help=TASK_HELP),
    ],
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_HELP,
        ),
    ] = None,
    workflow: Annotated[
        str | None,
        typer.Option(
            "--workflow",
            help=WORKFLOW_HELP,
        ),
    ] = None,
) -> None:
    """Get one task definition by name or code."""
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
def update_command(
    ctx: typer.Context,
    task: Annotated[
        str,
        typer.Argument(help=TASK_HELP),
    ],
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_HELP,
        ),
    ] = None,
    workflow: Annotated[
        str | None,
        typer.Option(
            "--workflow",
            help=WORKFLOW_HELP,
        ),
    ] = None,
    set_values: Annotated[
        list[str] | None,
        typer.Option(
            "--set",
            help=(
                "Inline KEY=VALUE update for this single task. Repeat as "
                "needed. Common keys: command, retry.times, timeout, "
                "depends_on. Run `dsctl schema --command task.update` for all "
                "supported keys."
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Compile the native task update request without sending it.",
        ),
    ] = False,
) -> None:
    """Update one task; use workflow edit for DAG changes.

    Use workflow-instance edit for repairs.
    """
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
