from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.workflow import (
    backfill_workflow_result,
    create_workflow_result,
    delete_workflow_result,
    describe_workflow_result,
    digest_workflow_result,
    edit_workflow_result,
    get_workflow_result,
    list_workflows_result,
    offline_workflow_result,
    online_workflow_result,
    run_workflow_result,
    run_workflow_task_result,
)
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


def register_workflow_commands(app: typer.Typer) -> None:
    """Register the `workflow` command group."""
    app.add_typer(workflow_app, name="workflow")


@workflow_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter workflows by name substring after fetching the project list.",
        ),
    ] = None,
) -> None:
    """List workflows inside one project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.list",
        lambda: list_workflows_result(
            project=project,
            search=search,
            env_file=env_file,
        ),
    )


@workflow_app.command("get")
def get_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            help="Output format: json or yaml.",
            case_sensitive=False,
        ),
    ] = "json",
) -> None:
    """Get one workflow by name or code."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.get",
        lambda: get_workflow_result(
            workflow,
            project=project,
            output_format=output_format.lower(),
            env_file=env_file,
        ),
    )


@workflow_app.command("describe")
def describe_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Describe one workflow with tasks and relations."""
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
def digest_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Return one compact workflow graph summary."""
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
def lineage_list_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Return the project-wide workflow lineage graph."""
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
def lineage_get_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context "
                "when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Return the lineage graph anchored on one workflow."""
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
def lineage_dependent_tasks_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context "
                "when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    task: Annotated[
        str | None,
        typer.Option(
            "--task",
            help="Task name or numeric code inside the selected workflow.",
        ),
    ] = None,
) -> None:
    """Return workflows/tasks that depend on one workflow or task."""
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


@workflow_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    file: Annotated[
        Path,
        typer.Option(
            "--file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Path to one workflow YAML specification file. Start from "
                "`dsctl template workflow` when authoring a new file."
            ),
            readable=True,
            resolve_path=True,
        ),
    ],
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Override workflow.project from the YAML file.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Compile the workflow payload without sending the create request.",
        ),
    ] = False,
    confirm_risk: Annotated[
        str | None,
        typer.Option(
            "--confirm-risk",
            help=(
                "Explicit confirmation token returned by a previous high-risk "
                "schedule validation failure."
            ),
        ),
    ] = None,
) -> None:
    """Create one workflow definition from a YAML file."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.create",
        lambda: create_workflow_result(
            file=file,
            project=project,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@workflow_app.command("edit")
def edit_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context "
                "when omitted."
            ),
        ),
    ] = None,
    *,
    patch: Annotated[
        Path,
        typer.Option(
            "--patch",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Path to one workflow patch YAML file. Use --dry-run to inspect "
                "the compiled diff before apply."
            ),
            readable=True,
            resolve_path=True,
        ),
    ],
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Compile the merged workflow update payload without sending it.",
        ),
    ] = False,
) -> None:
    """Edit one workflow definition from a YAML patch file."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "workflow.edit",
        lambda: edit_workflow_result(
            workflow,
            patch=patch,
            project=project,
            dry_run=dry_run,
            env_file=env_file,
        ),
    )


@workflow_app.command("online")
def online_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Bring one workflow definition online."""
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
def offline_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
) -> None:
    """Bring one workflow definition offline."""
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
def run_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    worker_group: Annotated[
        str | None,
        typer.Option(
            "--worker-group",
            help=(
                "Override the worker group used to start the workflow instance. "
                "Omit to allow enabled project preference before the DS "
                "fallback `default` worker group."
            ),
        ),
    ] = None,
    tenant: Annotated[
        str | None,
        typer.Option(
            "--tenant",
            help=(
                "Override the tenant code used to start the workflow instance. "
                "Omit to allow enabled project preference before the DS "
                "fallback `default` tenant."
            ),
        ),
    ] = None,
    failure_strategy: Annotated[
        str | None,
        typer.Option(
            "--failure-strategy",
            help="Failure strategy: continue or end. Defaults to DS UI continue.",
        ),
    ] = None,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help=(
                "Workflow instance priority: highest, high, medium, low, or "
                "lowest. Omit to allow enabled project preference before medium."
            ),
        ),
    ] = None,
    warning_type: Annotated[
        str | None,
        typer.Option(
            "--warning-type",
            help=(
                "Warning type: none, success, failure, or all. Omit to allow "
                "enabled project preference before none."
            ),
        ),
    ] = None,
    warning_group_id: Annotated[
        int | None,
        typer.Option(
            "--warning-group-id",
            help="Warning group id. Omit to allow enabled project preference.",
        ),
    ] = None,
    environment_code: Annotated[
        int | None,
        typer.Option(
            "--environment-code",
            help="Environment code. Omit to allow enabled project preference.",
        ),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option(
            "--param",
            help=(
                "Workflow start parameter in KEY=VALUE form. Repeat for multiple "
                "parameters."
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Resolve and compile the start request without sending it.",
        ),
    ] = False,
    execution_dry_run: Annotated[
        bool,
        typer.Option(
            "--execution-dry-run",
            help=(
                "Set DolphinScheduler dryRun=1; DS creates dry-run instances and "
                "skips task plugin trigger execution."
            ),
        ),
    ] = False,
) -> None:
    """Trigger one workflow definition and return created workflow instance ids."""
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
def run_task_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    task: Annotated[
        str,
        typer.Option(
            "--task",
            help="Task name or task code within the workflow definition.",
        ),
    ],
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    scope: Annotated[
        str,
        typer.Option(
            "--scope",
            help="Task execution scope: self, pre, or post.",
        ),
    ] = "self",
    worker_group: Annotated[
        str | None,
        typer.Option(
            "--worker-group",
            help=(
                "Override the worker group used to start the workflow instance. "
                "Omit to allow enabled project preference before the DS "
                "fallback `default` worker group."
            ),
        ),
    ] = None,
    tenant: Annotated[
        str | None,
        typer.Option(
            "--tenant",
            help=(
                "Override the tenant code used to start the workflow instance. "
                "Omit to allow enabled project preference before the DS "
                "fallback `default` tenant."
            ),
        ),
    ] = None,
    failure_strategy: Annotated[
        str | None,
        typer.Option(
            "--failure-strategy",
            help="Failure strategy: continue or end. Defaults to DS UI continue.",
        ),
    ] = None,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help=(
                "Workflow instance priority: highest, high, medium, low, or "
                "lowest. Omit to allow enabled project preference before medium."
            ),
        ),
    ] = None,
    warning_type: Annotated[
        str | None,
        typer.Option(
            "--warning-type",
            help=(
                "Warning type: none, success, failure, or all. Omit to allow "
                "enabled project preference before none."
            ),
        ),
    ] = None,
    warning_group_id: Annotated[
        int | None,
        typer.Option(
            "--warning-group-id",
            help="Warning group id. Omit to allow enabled project preference.",
        ),
    ] = None,
    environment_code: Annotated[
        int | None,
        typer.Option(
            "--environment-code",
            help="Environment code. Omit to allow enabled project preference.",
        ),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option(
            "--param",
            help=(
                "Workflow start parameter in KEY=VALUE form. Repeat for multiple "
                "parameters."
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Resolve and compile the start request without sending it.",
        ),
    ] = False,
    execution_dry_run: Annotated[
        bool,
        typer.Option(
            "--execution-dry-run",
            help=(
                "Set DolphinScheduler dryRun=1; DS creates dry-run instances and "
                "skips task plugin trigger execution."
            ),
        ),
    ] = False,
) -> None:
    """Start one workflow definition from a selected task."""
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
def backfill_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Complement start datetime, for example '2026-04-01 00:00:00'.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Complement end datetime, for example '2026-04-10 00:00:00'.",
        ),
    ] = None,
    dates: Annotated[
        list[str] | None,
        typer.Option(
            "--date",
            help=(
                "Explicit complement schedule datetime. Repeat for multiple "
                "dates instead of using --start/--end."
            ),
        ),
    ] = None,
    task: Annotated[
        str | None,
        typer.Option(
            "--task",
            help="Optional task name or task code to backfill from.",
        ),
    ] = None,
    scope: Annotated[
        str,
        typer.Option(
            "--scope",
            help="Task execution scope when --task is set: self, pre, or post.",
        ),
    ] = "self",
    run_mode: Annotated[
        str | None,
        typer.Option(
            "--run-mode",
            help="Complement run mode: serial or parallel.",
        ),
    ] = None,
    expected_parallelism_number: Annotated[
        int,
        typer.Option(
            "--expected-parallelism-number",
            help="Expected parallelism number when --run-mode parallel is used.",
        ),
    ] = 2,
    complement_dependent_mode: Annotated[
        str | None,
        typer.Option(
            "--complement-dependent-mode",
            help="Complement dependent mode: off or all.",
        ),
    ] = None,
    all_level_dependent: Annotated[
        bool,
        typer.Option(
            "--all-level-dependent",
            help="Enable all-level dependent complement when dependent mode is all.",
        ),
    ] = False,
    execution_order: Annotated[
        str | None,
        typer.Option(
            "--execution-order",
            help="Complement execution order: desc or asc.",
        ),
    ] = None,
    worker_group: Annotated[
        str | None,
        typer.Option(
            "--worker-group",
            help=(
                "Override the worker group used to start the workflow instance. "
                "Omit to allow enabled project preference before the DS "
                "fallback `default` worker group."
            ),
        ),
    ] = None,
    tenant: Annotated[
        str | None,
        typer.Option(
            "--tenant",
            help=(
                "Override the tenant code used to start the workflow instance. "
                "Omit to allow enabled project preference before the DS "
                "fallback `default` tenant."
            ),
        ),
    ] = None,
    failure_strategy: Annotated[
        str | None,
        typer.Option(
            "--failure-strategy",
            help="Failure strategy: continue or end. Defaults to DS UI continue.",
        ),
    ] = None,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help=(
                "Workflow instance priority: highest, high, medium, low, or "
                "lowest. Omit to allow enabled project preference before medium."
            ),
        ),
    ] = None,
    warning_type: Annotated[
        str | None,
        typer.Option(
            "--warning-type",
            help=(
                "Warning type: none, success, failure, or all. Omit to allow "
                "enabled project preference before none."
            ),
        ),
    ] = None,
    warning_group_id: Annotated[
        int | None,
        typer.Option(
            "--warning-group-id",
            help="Warning group id. Omit to allow enabled project preference.",
        ),
    ] = None,
    environment_code: Annotated[
        int | None,
        typer.Option(
            "--environment-code",
            help="Environment code. Omit to allow enabled project preference.",
        ),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option(
            "--param",
            help=(
                "Workflow start parameter in KEY=VALUE form. Repeat for multiple "
                "parameters."
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Resolve and compile the backfill request without sending it.",
        ),
    ] = False,
    execution_dry_run: Annotated[
        bool,
        typer.Option(
            "--execution-dry-run",
            help=(
                "Set DolphinScheduler dryRun=1; DS creates dry-run instances and "
                "skips task plugin trigger execution."
            ),
        ),
    ] = False,
) -> None:
    """Backfill one workflow definition and return created workflow instance ids."""
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
    )


@workflow_app.command("delete")
def delete_command(
    ctx: typer.Context,
    workflow: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Workflow name or numeric code. Falls back to workflow context"
                " when omitted."
            ),
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help="Project name or code. Falls back to stored project context.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm workflow deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one workflow definition."""
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
