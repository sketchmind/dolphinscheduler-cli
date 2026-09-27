from pathlib import Path

import typer

from dsctl.cli_runtime import emit_raw_result, emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.output import CommandResult
from dsctl.services.workflow_instance import (
    digest_workflow_instance_result,
    edit_workflow_instance_result,
    execute_task_in_workflow_instance_result,
    export_workflow_instance_yaml_result,
    get_parent_workflow_instance_result,
    get_workflow_instance_result,
    list_workflow_instances_by_trigger_result,
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


def register_workflow_instance_commands(app: typer.Typer) -> None:
    """Register the `workflow-instance` command group."""
    app.add_typer(workflow_instance_app, name="workflow-instance")


@workflow_instance_app.command("list")
@bind_command("workflow-instance.list")
def list_command(
    ctx: typer.Context,
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    project: str | None,
    workflow: str | None,
    search: str | None,
    executor: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    state: str | None,
    trigger_code: int | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    if trigger_code is not None:
        emit_result(
            "workflow-instance.list",
            lambda: list_workflow_instances_by_trigger_result(
                trigger_code,
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
        return
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
@bind_command("workflow-instance.get")
def get_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.get",
        lambda: get_workflow_instance_result(
            workflow_instance,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("export")
@bind_command("workflow-instance.export")
def export_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_raw_result(
        "workflow-instance.export",
        lambda: export_workflow_instance_yaml_result(
            workflow_instance,
            project=project,
            env_file=env_file,
        ),
        _workflow_instance_yaml,
    )


@workflow_instance_app.command("parent")
@bind_command("workflow-instance.parent")
def parent_command(
    ctx: typer.Context,
    sub_workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.parent",
        lambda: get_parent_workflow_instance_result(
            sub_workflow_instance,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("digest")
@bind_command("workflow-instance.digest")
def digest_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.digest",
        lambda: digest_workflow_instance_result(
            workflow_instance,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("edit")
@bind_command("workflow-instance.edit")
def edit_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
    patch: Path | None,
    file: Path | None,
    sync_definition: bool,
    dry_run: bool,
    confirm_risk: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.edit",
        lambda: edit_workflow_instance_result(
            workflow_instance,
            project=project,
            patch=patch,
            file=file,
            sync_definition=sync_definition,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("watch")
@bind_command("workflow-instance.watch")
def watch_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
    interval_seconds: int,
    timeout_seconds: int,
    exit_status: bool,
    after_run_times: int | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.watch",
        lambda: watch_workflow_instance_result(
            workflow_instance,
            project=project,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            after_run_times=after_run_times,
            exit_status=exit_status,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("stop")
@bind_command("workflow-instance.stop")
def stop_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.stop",
        lambda: stop_workflow_instance_result(
            workflow_instance,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("rerun")
@bind_command("workflow-instance.rerun")
def rerun_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.rerun",
        lambda: rerun_workflow_instance_result(
            workflow_instance,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("recover-failed")
@bind_command("workflow-instance.recover-failed")
def recover_failed_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.recover-failed",
        lambda: recover_failed_workflow_instance_result(
            workflow_instance,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_instance_app.command("execute-task")
@bind_command("workflow-instance.execute-task")
def execute_task_command(
    ctx: typer.Context,
    workflow_instance: int,
    *,
    project: str | None,
    task: str,
    scope: str,
) -> None:
    state_obj = get_app_state(ctx)
    env_file = None if state_obj.env_file is None else str(state_obj.env_file)
    emit_result(
        "workflow-instance.execute-task",
        lambda: execute_task_in_workflow_instance_result(
            workflow_instance,
            project=project,
            task=task,
            scope=scope,
            env_file=env_file,
        ),
    )


def _workflow_instance_yaml(result: CommandResult) -> str:
    data = result.data
    if not isinstance(data, dict):
        message = "workflow-instance result data must be an object"
        raise TypeError(message)
    yaml_text = data.get("yaml")
    if not isinstance(yaml_text, str):
        message = "workflow-instance result data is missing yaml"
        raise TypeError(message)
    return yaml_text
