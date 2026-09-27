import typer

from dsctl.cli_runtime import emit_raw_result, emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.output import CommandResult
from dsctl.services.task_instance import (
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
@bind_command("task-instance.list")
def list_command(
    ctx: typer.Context,
    *,
    workflow_instance: int | None,
    project: str | None,
    workflow: str | None,
    workflow_instance_name: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
    search: str | None,
    task: str | None,
    task_code: int | None,
    executor: str | None,
    state: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    execute_type: str | None,
) -> None:
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
@bind_command("task-instance.get")
def get_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    project: str | None,
    workflow_instance: int | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.get",
        lambda: get_task_instance_result(
            task_instance,
            project=project,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("watch")
@bind_command("task-instance.watch")
def watch_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    project: str | None,
    workflow_instance: int | None,
    interval_seconds: int,
    timeout_seconds: int,
    exit_status: bool,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.watch",
        lambda: watch_task_instance_result(
            task_instance,
            project=project,
            workflow_instance=workflow_instance,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            exit_status=exit_status,
            env_file=env_file,
        ),
    )


@task_instance_app.command("sub-workflow")
@bind_command("task-instance.sub-workflow")
def sub_workflow_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    project: str | None,
    workflow_instance: int,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.sub-workflow",
        lambda: get_sub_workflow_instance_result(
            task_instance,
            project=project,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("log")
@bind_command("task-instance.log")
def log_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    tail: int | None,
    start_line: int | None,
    limit: int | None,
    raw: bool,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    if raw:
        emit_raw_result(
            "task-instance.log",
            lambda: get_task_instance_log_result(
                task_instance,
                tail=tail,
                start_line=start_line,
                limit=limit,
                env_file=env_file,
            ),
            _task_log_text,
        )
        return
    emit_result(
        "task-instance.log",
        lambda: get_task_instance_log_result(
            task_instance,
            tail=tail,
            start_line=start_line,
            limit=limit,
            env_file=env_file,
        ),
    )


def _task_log_text(result: CommandResult) -> str:
    data = result.data
    if not isinstance(data, dict):
        message = "task-instance log result data must be an object"
        raise TypeError(message)
    text = data.get("text")
    if not isinstance(text, str):
        message = "task-instance log result data is missing text"
        raise TypeError(message)
    return text


@task_instance_app.command("force-success")
@bind_command("task-instance.force-success")
def force_success_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    project: str | None,
    workflow_instance: int,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.force-success",
        lambda: force_success_task_instance_result(
            task_instance,
            project=project,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("savepoint")
@bind_command("task-instance.savepoint")
def savepoint_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    project: str | None,
    workflow_instance: int | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.savepoint",
        lambda: savepoint_task_instance_result(
            task_instance,
            project=project,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )


@task_instance_app.command("stop")
@bind_command("task-instance.stop")
def stop_command(
    ctx: typer.Context,
    task_instance: int,
    *,
    project: str | None,
    workflow_instance: int | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "task-instance.stop",
        lambda: stop_task_instance_result(
            task_instance,
            project=project,
            workflow_instance=workflow_instance,
            env_file=env_file,
        ),
    )
