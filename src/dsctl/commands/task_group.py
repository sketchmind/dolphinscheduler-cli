from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.output import CommandResult
from dsctl.services.task_group import (
    UNSET,
    DescriptionUpdate,
    close_task_group_result,
    create_task_group_result,
    force_start_task_group_queue_result,
    get_task_group_result,
    list_task_group_queues_result,
    list_task_groups_result,
    set_task_group_queue_priority_result,
    start_task_group_result,
    update_task_group_result,
)

task_group_app = typer.Typer(
    help="Manage DolphinScheduler task groups.",
    no_args_is_help=True,
)
task_group_queue_app = typer.Typer(
    help="Manage DolphinScheduler task-group queues.",
    no_args_is_help=True,
)

TASK_GROUP_HELP = (
    "Task-group name or numeric id. Run `dsctl task-group list` to discover values."
)


def register_task_group_commands(app: typer.Typer) -> None:
    """Register the `task-group` command group."""
    task_group_app.add_typer(task_group_queue_app, name="queue")
    app.add_typer(task_group_app, name="task-group")


@task_group_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Use only for project-scoped listing; "
                "run `dsctl project list` to discover values."
            ),
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter task groups by task-group name.",
        ),
    ] = None,
    status: Annotated[
        str | None,
        typer.Option(
            "--status",
            help="Filter task groups by status: open, closed, 1, or 0.",
        ),
    ] = None,
    page_no: Annotated[
        int,
        typer.Option("--page-no", min=1, help="Page number to fetch."),
    ] = 1,
    page_size: Annotated[
        int,
        typer.Option("--page-size", min=1, help="Page size to request."),
    ] = 100,
    all_pages: Annotated[
        bool,
        typer.Option("--all", help="Fetch all remaining pages up to the safety limit."),
    ] = False,
) -> None:
    """List task groups."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.list",
        lambda: list_task_groups_result(
            project=project,
            search=search,
            status=status,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@task_group_app.command("get")
def get_command(
    ctx: typer.Context,
    task_group: Annotated[
        str,
        typer.Argument(help=TASK_GROUP_HELP),
    ],
) -> None:
    """Get one task group by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.get",
        lambda: get_task_group_result(task_group, env_file=env_file),
    )


@task_group_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Falls back to stored project context; "
                "run `dsctl project list` to discover values."
            ),
        ),
    ] = None,
    name: Annotated[
        str,
        typer.Option("--name", help="Task-group name."),
    ],
    group_size: Annotated[
        int,
        typer.Option("--group-size", min=1, help="Task-group capacity."),
    ],
    description: Annotated[
        str | None,
        typer.Option("--description", help="Optional task-group description."),
    ] = None,
) -> None:
    """Create one task group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.create",
        lambda: create_task_group_result(
            project=project,
            name=name,
            group_size=group_size,
            description=description,
            env_file=env_file,
        ),
    )


@task_group_app.command("update")
def update_command(
    ctx: typer.Context,
    task_group: Annotated[
        str,
        typer.Argument(help=TASK_GROUP_HELP),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option("--name", help="Updated task-group name."),
    ] = None,
    group_size: Annotated[
        int | None,
        typer.Option("--group-size", min=1, help="Updated task-group capacity."),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option("--description", help="Updated task-group description."),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option("--clear-description", help="Clear the stored description."),
    ] = False,
) -> None:
    """Update one task group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        if clear_description:
            description_update = ""
        elif description is None:
            description_update = UNSET
        else:
            description_update = description
        return update_task_group_result(
            task_group,
            name=name,
            group_size=group_size,
            description=description_update,
            env_file=env_file,
        )

    emit_result("task-group.update", build_result)


@task_group_app.command("close")
def close_command(
    ctx: typer.Context,
    task_group: Annotated[
        str,
        typer.Argument(help=TASK_GROUP_HELP),
    ],
) -> None:
    """Close one task group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.close",
        lambda: close_task_group_result(task_group, env_file=env_file),
    )


@task_group_app.command("start")
def start_command(
    ctx: typer.Context,
    task_group: Annotated[
        str,
        typer.Argument(help=TASK_GROUP_HELP),
    ],
) -> None:
    """Start one task group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.start",
        lambda: start_task_group_result(task_group, env_file=env_file),
    )


@task_group_queue_app.command("list")
def list_queue_command(
    ctx: typer.Context,
    task_group: Annotated[
        str,
        typer.Argument(help=TASK_GROUP_HELP),
    ],
    *,
    task_instance: Annotated[
        str | None,
        typer.Option("--task-instance", help="Filter by task-instance name."),
    ] = None,
    workflow_instance: Annotated[
        str | None,
        typer.Option(
            "--workflow-instance",
            help="Filter by workflow-instance name.",
        ),
    ] = None,
    status: Annotated[
        str | None,
        typer.Option(
            "--status",
            help=(
                "Filter by queue status: WAIT_QUEUE, ACQUIRE_SUCCESS, RELEASE, "
                "-1, 1, or 2."
            ),
        ),
    ] = None,
    page_no: Annotated[
        int,
        typer.Option("--page-no", min=1, help="Page number to fetch."),
    ] = 1,
    page_size: Annotated[
        int,
        typer.Option("--page-size", min=1, help="Page size to request."),
    ] = 100,
    all_pages: Annotated[
        bool,
        typer.Option("--all", help="Fetch all remaining pages up to the safety limit."),
    ] = False,
) -> None:
    """List queue rows for one task group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.queue.list",
        lambda: list_task_group_queues_result(
            task_group,
            task_instance=task_instance,
            workflow_instance=workflow_instance,
            status=status,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@task_group_queue_app.command("force-start")
def force_start_queue_command(
    ctx: typer.Context,
    queue_id: Annotated[
        int,
        typer.Argument(
            help=(
                "Numeric task-group queue id. Run "
                "`dsctl task-group queue list TASK_GROUP` to discover ids."
            ),
        ),
    ],
) -> None:
    """Force-start one waiting task-group queue row."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.queue.force-start",
        lambda: force_start_task_group_queue_result(queue_id, env_file=env_file),
    )


@task_group_queue_app.command("set-priority")
def set_priority_queue_command(
    ctx: typer.Context,
    queue_id: Annotated[
        int,
        typer.Argument(
            help=(
                "Numeric task-group queue id. Run "
                "`dsctl task-group queue list TASK_GROUP` to discover ids."
            ),
        ),
    ],
    *,
    priority: Annotated[
        int,
        typer.Option("--priority", min=0, help="Updated queue priority."),
    ],
) -> None:
    """Set one task-group queue priority."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.queue.set-priority",
        lambda: set_task_group_queue_priority_result(
            queue_id,
            priority=priority,
            env_file=env_file,
        ),
    )
