from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.task_instance import (
    DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
    force_success_task_instance_result,
    get_sub_workflow_instance_result,
    get_task_instance_log_result,
    get_task_instance_result,
    list_task_instances_result,
    savepoint_task_instance_result,
    stop_task_instance_result,
    watch_task_instance_result,
)

task_instance_app = typer.Typer(
    help="Inspect and control DolphinScheduler task instances.",
    no_args_is_help=True,
)

TASK_INSTANCE_HELP = "Task instance id. Run `dsctl task-instance list` to discover ids."
WORKFLOW_INSTANCE_OPTION_HELP = (
    "Workflow instance id used to resolve the owning project. Run `dsctl "
    "workflow-instance list` to discover ids."
)
WORKFLOW_INSTANCE_FILTER_HELP = (
    "Workflow instance id used to narrow the project-scoped task-instance query. "
    "Run `dsctl workflow-instance list` to discover ids."
)
PROJECT_FILTER_HELP = (
    "Project name or code for the project-scoped query. Run `dsctl project list` "
    "to discover values; required via flag or context when --workflow-instance "
    "is omitted."
)
TASK_STATE_HELP = (
    "Filter by DS task execution status name. Run `dsctl enum list "
    "task-execution-status` to discover values."
)
TASK_CODE_HELP = (
    "Filter by task definition code. Run `dsctl task list` to discover values."
)
TASK_EXECUTE_TYPE_HELP = (
    "Filter by DS task execute type: BATCH or STREAM. Run `dsctl enum list "
    "task-execute-type` to discover values."
)


def register_task_instance_commands(app: typer.Typer) -> None:
    """Register the `task-instance` command group."""
    app.add_typer(task_instance_app, name="task-instance")


@task_instance_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    workflow_instance: Annotated[
        int | None,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_FILTER_HELP,
        ),
    ] = None,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_FILTER_HELP,
        ),
    ] = None,
    workflow: Annotated[
        str | None,
        typer.Option(
            "--workflow",
            help=(
                "Reserved compatibility option. DS 3.4.1 task-instance list "
                "does not reliably filter by workflow definition."
            ),
            hidden=True,
        ),
    ] = None,
    workflow_instance_name: Annotated[
        str | None,
        typer.Option(
            "--workflow-instance-name",
            help="Filter by workflow instance name.",
        ),
    ] = None,
    page_no: Annotated[
        int,
        typer.Option("--page-no", help="Remote page number."),
    ] = 1,
    page_size: Annotated[
        int,
        typer.Option("--page-size", help="Remote page size."),
    ] = 100,
    all_pages: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Fetch all remaining pages up to the safety limit.",
        ),
    ] = False,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help=(
                "Free-text upstream searchVal filter. Use --task for an exact "
                "task instance name filter."
            ),
        ),
    ] = None,
    task: Annotated[
        str | None,
        typer.Option(
            "--task",
            help="Filter by exact task instance name.",
        ),
    ] = None,
    task_code: Annotated[
        int | None,
        typer.Option(
            "--task-code",
            help=TASK_CODE_HELP,
        ),
    ] = None,
    executor: Annotated[
        str | None,
        typer.Option(
            "--executor",
            help="Filter by executor user name.",
        ),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option(
            "--state",
            help=TASK_STATE_HELP,
        ),
    ] = None,
    host: Annotated[
        str | None,
        typer.Option(
            "--host",
            help="Filter by worker host.",
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Task start-time lower bound, e.g. '2026-04-11 10:00:00'.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Task start-time upper bound, e.g. '2026-04-11 11:00:00'.",
        ),
    ] = None,
    execute_type: Annotated[
        str | None,
        typer.Option(
            "--execute-type",
            help=TASK_EXECUTE_TYPE_HELP,
        ),
    ] = None,
) -> None:
    """List task instances with project-scoped runtime filters."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.list",
        lambda: list_task_instances_result(
            workflow_instance=workflow_instance,
            project=project,
            workflow=workflow,
            workflow_instance_name=workflow_instance_name,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            search=search,
            task=task,
            task_code=task_code,
            executor=executor,
            state=state,
            host=host,
            start=start,
            end=end,
            execute_type=execute_type,
            env_file=env_file,
        ),
    )


@task_instance_app.command("get")
def get_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_OPTION_HELP,
        ),
    ],
) -> None:
    """Get one task instance by id within one workflow instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.get",
        lambda: get_task_instance_result(
            task_instance,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("watch")
def watch_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_OPTION_HELP,
        ),
    ],
    interval_seconds: Annotated[
        int,
        typer.Option(
            "--interval-seconds",
            help="Polling interval in seconds.",
        ),
    ] = DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    timeout_seconds: Annotated[
        int,
        typer.Option(
            "--timeout-seconds",
            help="Maximum seconds to wait. Use 0 to wait indefinitely.",
        ),
    ] = DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
) -> None:
    """Poll one task instance until it reaches a finished state."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.watch",
        lambda: watch_task_instance_result(
            task_instance,
            workflow_instance=workflow_instance,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            env_file=env_file,
        ),
    )


@task_instance_app.command("sub-workflow")
def sub_workflow_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_OPTION_HELP,
        ),
    ],
) -> None:
    """Return the child workflow instance for one SUB_WORKFLOW task instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.sub-workflow",
        lambda: get_sub_workflow_instance_result(
            task_instance,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("log")
def log_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    tail: Annotated[
        int,
        typer.Option(
            "--tail",
            help="Return the last N log lines by chunking the upstream logger API.",
        ),
    ] = 200,
) -> None:
    """Fetch the tail of one task-instance log."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.log",
        lambda: get_task_instance_log_result(
            task_instance,
            tail=tail,
            env_file=env_file,
        ),
    )


@task_instance_app.command("force-success")
def force_success_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_OPTION_HELP,
        ),
    ],
) -> None:
    """Force one failed task instance into FORCED_SUCCESS."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.force-success",
        lambda: force_success_task_instance_result(
            task_instance,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("savepoint")
def savepoint_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_OPTION_HELP,
        ),
    ],
) -> None:
    """Request one savepoint for a running task instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.savepoint",
        lambda: savepoint_task_instance_result(
            task_instance,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("stop")
def stop_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help=TASK_INSTANCE_HELP),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help=WORKFLOW_INSTANCE_OPTION_HELP,
        ),
    ],
) -> None:
    """Request stop for one task instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.stop",
        lambda: stop_task_instance_result(
            task_instance,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )
