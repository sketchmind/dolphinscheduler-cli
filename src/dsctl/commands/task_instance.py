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


def register_task_instance_commands(app: typer.Typer) -> None:
    """Register the `task-instance` command group."""
    app.add_typer(task_instance_app, name="task-instance")


@task_instance_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to scope the task-instance query.",
        ),
    ],
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
            help="Filter task instances by upstream searchVal.",
        ),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option(
            "--state",
            help="Filter by DS task execution status name.",
        ),
    ] = None,
) -> None:
    """List task instances inside one workflow instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.list",
        lambda: list_task_instances_result(
            workflow_instance=workflow_instance,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            search=search,
            state=state,
            env_file=env_file,
        ),
    )


@task_instance_app.command("get")
def get_command(
    ctx: typer.Context,
    task_instance: Annotated[
        int,
        typer.Argument(help="Task instance id."),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to resolve the owning project.",
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
        typer.Argument(help="Task instance id."),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to resolve the owning project.",
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
        typer.Argument(help="Task instance id."),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to scope the task-instance relation.",
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
        typer.Argument(help="Task instance id."),
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
        typer.Argument(help="Task instance id."),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to resolve the owning project.",
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
        typer.Argument(help="Task instance id."),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to resolve the owning project.",
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
        typer.Argument(help="Task instance id."),
    ],
    *,
    workflow_instance: Annotated[
        int,
        typer.Option(
            "--workflow-instance",
            help="Workflow instance id used to resolve the owning project.",
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
