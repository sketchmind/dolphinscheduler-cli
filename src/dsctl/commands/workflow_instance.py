from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.workflow_instance import (
    DEFAULT_WATCH_INTERVAL_SECONDS,
    DEFAULT_WATCH_TIMEOUT_SECONDS,
    digest_workflow_instance_result,
    edit_workflow_instance_result,
    execute_task_in_workflow_instance_result,
    get_parent_workflow_instance_result,
    get_workflow_instance_result,
    list_workflow_instances_result,
    recover_failed_workflow_instance_result,
    rerun_workflow_instance_result,
    stop_workflow_instance_result,
    watch_workflow_instance_result,
)

workflow_instance_app = typer.Typer(
    help="Inspect DolphinScheduler workflow instances.",
    no_args_is_help=True,
)

PROJECT_FILTER_HELP = (
    "Project name or code for project-scoped filters. Run `dsctl project list` "
    "to discover values."
)
WORKFLOW_FILTER_HELP = (
    "Workflow name or code filter. With --project, resolved inside that project; "
    "run `dsctl workflow list` to discover values."
)
WORKFLOW_INSTANCE_HELP = (
    "Workflow instance id. Run `dsctl workflow-instance list` to discover ids."
)
SUB_WORKFLOW_INSTANCE_HELP = (
    "Sub-workflow instance id. Run `dsctl workflow-instance list` to discover ids."
)
FINISHED_WORKFLOW_INSTANCE_HELP = (
    "Finished workflow instance id. Run `dsctl workflow-instance list` to discover ids."
)
WORKFLOW_STATE_HELP = (
    "Filter by DS workflow execution status name. Run `dsctl enum list "
    "workflow-execution-status` to discover values."
)
INSTANCE_TASK_HELP = (
    "Task name or task code within the workflow instance. Run `dsctl "
    "task-instance list --workflow-instance WORKFLOW_INSTANCE` to discover values."
)


def register_workflow_instance_commands(app: typer.Typer) -> None:
    """Register the `workflow-instance` command group."""
    app.add_typer(workflow_instance_app, name="workflow-instance")


@workflow_instance_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
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
            help=WORKFLOW_FILTER_HELP,
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter workflow instances by upstream searchVal; requires --project.",
        ),
    ] = None,
    executor: Annotated[
        str | None,
        typer.Option(
            "--executor",
            help="Filter by executor user name; requires --project.",
        ),
    ] = None,
    host: Annotated[
        str | None,
        typer.Option(
            "--host",
            help="Filter by workflow instance host.",
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Filter by start time lower bound, e.g. '2026-04-11 10:00:00'.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Filter by start time upper bound, e.g. '2026-04-11 11:00:00'.",
        ),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option(
            "--state",
            help=WORKFLOW_STATE_HELP,
        ),
    ] = None,
) -> None:
    """List workflow instances using explicit runtime filters."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.list",
        lambda: list_workflow_instances_result(
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            project=project,
            workflow=workflow,
            search=search,
            executor=executor,
            host=host,
            start=start,
            end=end,
            state=state,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("get")
def get_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
) -> None:
    """Get one workflow instance by id."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.get",
        lambda: get_workflow_instance_result(
            workflow_instance,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("parent")
def parent_command(
    ctx: typer.Context,
    sub_workflow_instance: Annotated[
        int,
        typer.Argument(help=SUB_WORKFLOW_INSTANCE_HELP),
    ],
) -> None:
    """Return the parent workflow instance for one sub-workflow instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.parent",
        lambda: get_parent_workflow_instance_result(
            sub_workflow_instance,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("digest")
def digest_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
) -> None:
    """Return one compact workflow-instance runtime digest."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.digest",
        lambda: digest_workflow_instance_result(
            workflow_instance,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("edit")
def edit_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=FINISHED_WORKFLOW_INSTANCE_HELP),
    ],
    *,
    patch: Annotated[
        Path,
        typer.Option(
            "--patch",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Path to one workflow-instance patch YAML file. Start from "
                "`dsctl template workflow-instance-patch --raw`; `tasks.create[]` "
                "uses full task fragments from `dsctl template task`; "
                "`tasks.update[].set` uses partial task fields discovered with "
                "`dsctl task-type schema TYPE`."
            ),
            readable=True,
            resolve_path=True,
        ),
    ],
    sync_definition: Annotated[
        bool,
        typer.Option(
            "--sync-definition/--no-sync-definition",
            help="Also write the edited DAG back to the current workflow definition.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help=(
                "Compile the merged workflow-instance edit payload without sending it."
            ),
        ),
    ] = False,
) -> None:
    """Edit one finished workflow instance from a YAML patch file."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.edit",
        lambda: edit_workflow_instance_result(
            workflow_instance,
            patch=patch,
            sync_definition=sync_definition,
            dry_run=dry_run,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("watch")
def watch_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
    interval_seconds: Annotated[
        int,
        typer.Option(
            "--interval-seconds",
            help="Polling interval in seconds.",
        ),
    ] = DEFAULT_WATCH_INTERVAL_SECONDS,
    timeout_seconds: Annotated[
        int,
        typer.Option(
            "--timeout-seconds",
            help="Maximum seconds to wait. Use 0 to wait indefinitely.",
        ),
    ] = DEFAULT_WATCH_TIMEOUT_SECONDS,
) -> None:
    """Poll one workflow instance until it reaches a final state."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.watch",
        lambda: watch_workflow_instance_result(
            workflow_instance,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("stop")
def stop_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
) -> None:
    """Request stop for one workflow instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.stop",
        lambda: stop_workflow_instance_result(
            workflow_instance,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("rerun")
def rerun_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
) -> None:
    """Request rerun for one finished workflow instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.rerun",
        lambda: rerun_workflow_instance_result(
            workflow_instance,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("recover-failed")
def recover_failed_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
) -> None:
    """Recover one failed workflow instance from failed tasks."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.recover-failed",
        lambda: recover_failed_workflow_instance_result(
            workflow_instance,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("execute-task")
def execute_task_command(
    ctx: typer.Context,
    workflow_instance: Annotated[
        int,
        typer.Argument(help=WORKFLOW_INSTANCE_HELP),
    ],
    task: Annotated[
        str,
        typer.Option(
            "--task",
            help=INSTANCE_TASK_HELP,
        ),
    ],
    scope: Annotated[
        str,
        typer.Option(
            "--scope",
            help="Task execution scope: self, pre, or post.",
        ),
    ] = "self",
) -> None:
    """Execute one task inside one finished workflow instance."""
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.execute-task",
        lambda: execute_task_in_workflow_instance_result(
            workflow_instance,
            task=task,
            scope=scope,
            env_file=env_file,
        ),
    )
