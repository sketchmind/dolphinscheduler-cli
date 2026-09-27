import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.errors import UserInputError
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


def register_task_group_commands(app: typer.Typer) -> None:
    """Register the `task-group` command group."""
    task_group_app.add_typer(task_group_queue_app, name="queue")
    app.add_typer(task_group_app, name="task-group")


@task_group_app.command("list")
@bind_command("task-group.list")
def list_command(
    ctx: typer.Context,
    *,
    project: str | None,
    search: str | None,
    status: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
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
@bind_command("task-group.get")
def get_command(
    ctx: typer.Context,
    task_group: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.get",
        lambda: get_task_group_result(task_group, env_file=env_file),
    )


@task_group_app.command("create")
@bind_command("task-group.create")
def create_command(
    ctx: typer.Context,
    *,
    project: str | None,
    name: str,
    group_size: int,
    description: str | None,
) -> None:
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
@bind_command("task-group.update")
def update_command(
    ctx: typer.Context,
    task_group: str,
    *,
    name: str | None,
    group_size: int | None,
    description: str | None,
    clear_description: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        if description is not None and clear_description:
            message = "--description and --clear-description cannot be used together"
            raise UserInputError(
                message,
                suggestion=(
                    "Use either --description VALUE or --clear-description, not both."
                ),
            )
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
@bind_command("task-group.close")
def close_command(
    ctx: typer.Context,
    task_group: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.close",
        lambda: close_task_group_result(task_group, env_file=env_file),
    )


@task_group_app.command("start")
@bind_command("task-group.start")
def start_command(
    ctx: typer.Context,
    task_group: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.start",
        lambda: start_task_group_result(task_group, env_file=env_file),
    )


@task_group_queue_app.command("list")
@bind_command("task-group.queue.list")
def list_queue_command(
    ctx: typer.Context,
    task_group: str,
    *,
    task_instance: str | None,
    workflow_instance: str | None,
    status: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
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
@bind_command("task-group.queue.force-start")
def force_start_queue_command(
    ctx: typer.Context,
    queue_id: int,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "task-group.queue.force-start",
        lambda: force_start_task_group_queue_result(queue_id, env_file=env_file),
    )


@task_group_queue_app.command("set-priority")
@bind_command("task-group.queue.set-priority")
def set_priority_queue_command(
    ctx: typer.Context,
    queue_id: int,
    *,
    priority: int,
) -> None:
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
