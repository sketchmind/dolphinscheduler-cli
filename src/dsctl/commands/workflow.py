from pathlib import Path

import typer

from dsctl.cli_runtime import emit_raw_result, emit_result, get_app_state
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.commands._contract_adapter import bind_command
from dsctl.output import CommandResult
from dsctl.services.workflow import (
    backfill_workflow_result,
    create_workflow_result,
    delete_workflow_result,
    describe_workflow_result,
    digest_workflow_result,
    edit_workflow_result,
    export_workflow_yaml_result,
    get_workflow_result,
    list_workflows_result,
    offline_workflow_result,
    online_workflow_result,
    run_workflow_result,
    run_workflow_task_result,
)
from dsctl.services.workflow.execution import validate_backfill_workflow_inputs
from dsctl.services.workflow_lineage import (
    get_workflow_lineage_result,
    list_workflow_dependent_tasks_result,
    list_workflow_lineage_result,
)

workflow_app = typer.Typer(
    help="Manage DolphinScheduler workflows.",
    no_args_is_help=True,
)
workflow_lineage_app = typer.Typer(
    help="Inspect DolphinScheduler workflow lineage.",
    no_args_is_help=True,
)
workflow_app.add_typer(workflow_lineage_app, name="lineage")

_WORKFLOW_CREATE = COMMAND_CATALOG.command("workflow.create")


def register_workflow_commands(app: typer.Typer) -> None:
    """Register the `workflow` command group."""
    app.add_typer(workflow_app, name="workflow")


@workflow_app.command("list")
@bind_command("workflow.list")
def list_command(
    ctx: typer.Context,
    *,
    project: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.list",
        lambda: list_workflows_result(
            project=project,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@workflow_app.command("get")
@bind_command("workflow.get")
def get_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.get",
        lambda: get_workflow_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_app.command("export")
@bind_command("workflow.export")
def export_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_raw_result(
        "workflow.export",
        lambda: export_workflow_yaml_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
        _workflow_yaml,
    )


@workflow_app.command("describe")
@bind_command("workflow.describe")
def describe_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.describe",
        lambda: describe_workflow_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_app.command("digest")
@bind_command("workflow.digest")
def digest_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.digest",
        lambda: digest_workflow_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_lineage_app.command("list")
@bind_command("workflow.lineage.list")
def lineage_list_command(
    ctx: typer.Context,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.lineage.list",
        lambda: list_workflow_lineage_result(
            project=project,
            env_file=env_file,
        ),
    )


@workflow_lineage_app.command("get")
@bind_command("workflow.lineage.get")
def lineage_get_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.lineage.get",
        lambda: get_workflow_lineage_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_lineage_app.command("dependent-tasks")
@bind_command("workflow.lineage.dependent-tasks")
def lineage_dependent_tasks_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
    task: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.lineage.dependent-tasks",
        lambda: list_workflow_dependent_tasks_result(
            workflow,
            task=task,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_app.command(_WORKFLOW_CREATE.name)
@bind_command("workflow.create")
def create_command(
    ctx: typer.Context,
    *,
    file: Path,
    project: str | None,
    dry_run: bool,
    confirm_risk: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        _WORKFLOW_CREATE.action,
        lambda: create_workflow_result(
            file=file,
            project=project,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@workflow_app.command("edit")
@bind_command("workflow.edit")
def edit_command(
    ctx: typer.Context,
    workflow: str,
    *,
    patch: Path | None,
    file: Path | None,
    project: str | None,
    dry_run: bool,
    confirm_risk: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.edit",
        lambda: edit_workflow_result(
            workflow,
            patch=patch,
            file=file,
            project=project,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@workflow_app.command("online")
@bind_command("workflow.online")
def online_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.online",
        lambda: online_workflow_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_app.command("offline")
@bind_command("workflow.offline")
def offline_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.offline",
        lambda: offline_workflow_result(
            workflow,
            project=project,
            env_file=env_file,
        ),
    )


@workflow_app.command("run")
@bind_command("workflow.run")
def run_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str] | None,
    dry_run: bool,
    execution_dry_run: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.run",
        lambda: run_workflow_result(
            workflow,
            project=project,
            worker_group=worker_group,
            tenant=tenant,
            failure_strategy=failure_strategy,
            priority=priority,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            environment_code=environment_code,
            params=params,
            dry_run=dry_run,
            execution_dry_run=execution_dry_run,
            env_file=env_file,
        ),
    )


@workflow_app.command("run-task")
@bind_command("workflow.run-task")
def run_task_command(
    ctx: typer.Context,
    workflow: str,
    *,
    task: str,
    project: str | None,
    scope: str,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str] | None,
    dry_run: bool,
    execution_dry_run: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.run-task",
        lambda: run_workflow_task_result(
            workflow,
            task=task,
            project=project,
            scope=scope,
            worker_group=worker_group,
            tenant=tenant,
            failure_strategy=failure_strategy,
            priority=priority,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            environment_code=environment_code,
            params=params,
            dry_run=dry_run,
            execution_dry_run=execution_dry_run,
            env_file=env_file,
        ),
    )


@workflow_app.command("backfill")
@bind_command("workflow.backfill")
def backfill_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
    start: str | None,
    end: str | None,
    dates: list[str] | None,
    task: str | None,
    scope: str,
    run_mode: str | None,
    expected_parallelism_number: int | None,
    complement_dependent_mode: str | None,
    all_level_dependent: bool,
    execution_order: str | None,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str] | None,
    dry_run: bool,
    execution_dry_run: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.backfill",
        lambda: backfill_workflow_result(
            workflow,
            project=project,
            start=start,
            end=end,
            dates=dates,
            task=task,
            scope=scope,
            run_mode=run_mode,
            expected_parallelism_number=expected_parallelism_number,
            complement_dependent_mode=complement_dependent_mode,
            all_level_dependent=all_level_dependent,
            execution_order=execution_order,
            worker_group=worker_group,
            tenant=tenant,
            failure_strategy=failure_strategy,
            priority=priority,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            environment_code=environment_code,
            params=params,
            dry_run=dry_run,
            execution_dry_run=execution_dry_run,
            env_file=env_file,
        ),
        local_validate=lambda: validate_backfill_workflow_inputs(
            start=start,
            end=end,
            dates=[] if dates is None else dates,
            scope=scope,
            run_mode=run_mode,
            expected_parallelism_number=expected_parallelism_number,
            complement_dependent_mode=complement_dependent_mode,
            all_level_dependent=all_level_dependent,
            execution_order=execution_order,
            params=[] if params is None else params,
        ),
    )


@workflow_app.command("delete")
@bind_command("workflow.delete")
def delete_command(
    ctx: typer.Context,
    workflow: str,
    *,
    project: str | None,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.delete",
        lambda: delete_workflow_result(
            workflow,
            project=project,
            force=force,
            env_file=env_file,
        ),
    )


def _workflow_yaml(result: CommandResult) -> str:
    data = result.data
    if not isinstance(data, dict):
        message = "workflow result data must be an object"
        raise TypeError(message)
    yaml_text = data.get("yaml")
    if not isinstance(yaml_text, str):
        message = "workflow result data is missing yaml"
        raise TypeError(message)
    return yaml_text
