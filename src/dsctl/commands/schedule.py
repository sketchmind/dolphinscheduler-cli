from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.schedule import (
    create_schedule_result,
    delete_schedule_result,
    explain_schedule_result,
    get_schedule_result,
    list_schedules_result,
    offline_schedule_result,
    online_schedule_result,
    preview_schedule_result,
    update_schedule_result,
)

schedule_app = typer.Typer(
    help="Manage DolphinScheduler schedules.",
    no_args_is_help=True,
)

PROJECT_HELP = (
    "Project name or code. Run `dsctl project list` to discover values; falls "
    "back to stored project context."
)
SCHEDULE_ID_HELP = "Schedule id. Use `dsctl schedule list` to discover values."
WORKFLOW_HELP = (
    "Workflow name or code. Run `dsctl workflow list` in the selected project "
    "to discover values; falls back to workflow context."
)


def register_schedule_commands(app: typer.Typer) -> None:
    """Register the `schedule` command group."""
    app.add_typer(schedule_app, name="schedule")


@schedule_app.command("list")
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
            help=(
                "Exact workflow name or code to narrow the project schedule "
                "list. Run `dsctl workflow list` in the selected project to "
                "discover values."
            ),
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help=(
                "Filter schedules by workflow name substring within the"
                " selected project."
            ),
        ),
    ] = None,
    page_no: Annotated[
        int,
        typer.Option(
            "--page-no",
            min=1,
            help="Page number to fetch when not using --all.",
        ),
    ] = 1,
    page_size: Annotated[
        int,
        typer.Option(
            "--page-size",
            min=1,
            help="Page size to request from the upstream API.",
        ),
    ] = 100,
    all_pages: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Fetch all remaining pages up to the safety limit.",
        ),
    ] = False,
) -> None:
    """List schedules inside one project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.list",
        lambda: list_schedules_result(
            project=project,
            workflow=workflow,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@schedule_app.command("get")
def get_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int,
        typer.Argument(help=SCHEDULE_ID_HELP),
    ],
) -> None:
    """Get one schedule by id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.get",
        lambda: get_schedule_result(schedule_id, env_file=env_file),
    )


@schedule_app.command("preview")
def preview_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int | None,
        typer.Argument(
            help=(
                "Existing schedule id to preview. Use `dsctl schedule list` to "
                "discover values."
            )
        ),
    ] = None,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_HELP,
        ),
    ] = None,
    cron: Annotated[
        str | None,
        typer.Option(
            "--cron",
            help=(
                "Quartz cron expression for an ad hoc preview "
                "(6 or 7 fields, seconds first)."
            ),
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Schedule start time in DS datetime string format.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Schedule end time in DS datetime string format.",
        ),
    ] = None,
    timezone: Annotated[
        str | None,
        typer.Option(
            "--timezone",
            help="Timezone id, for example Asia/Shanghai.",
        ),
    ] = None,
) -> None:
    """Preview the next fire times for a schedule."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.preview",
        lambda: preview_schedule_result(
            schedule_id=schedule_id,
            project=project,
            cron=cron,
            start=start,
            end=end,
            timezone=timezone,
            env_file=env_file,
        ),
    )


@schedule_app.command("explain")
def explain_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int | None,
        typer.Argument(
            help=(
                "Existing schedule id to explain as an update. Use `dsctl "
                "schedule list` to discover values."
            )
        ),
    ] = None,
    *,
    workflow: Annotated[
        str | None,
        typer.Option(
            "--workflow",
            help=(
                "Workflow name or code. Run `dsctl workflow list` in the "
                "selected project to discover values; falls back to workflow "
                "context for create explain."
            ),
        ),
    ] = None,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_HELP,
        ),
    ] = None,
    cron: Annotated[
        str | None,
        typer.Option(
            "--cron",
            help="Quartz cron expression (6 or 7 fields, seconds first).",
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Schedule start time in DS datetime string format.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Schedule end time in DS datetime string format.",
        ),
    ] = None,
    timezone: Annotated[
        str | None,
        typer.Option(
            "--timezone",
            help="Timezone id, for example Asia/Shanghai.",
        ),
    ] = None,
    failure_strategy: Annotated[
        str | None,
        typer.Option(
            "--failure-strategy",
            help="Failure strategy: CONTINUE or END.",
        ),
    ] = None,
    warning_type: Annotated[
        str | None,
        typer.Option(
            "--warning-type",
            help="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
        ),
    ] = None,
    warning_group_id: Annotated[
        int | None,
        typer.Option(
            "--warning-group-id",
            min=0,
            help=(
                "Warning group id for create explain or updated value for "
                "update explain. Create explain can also inherit enabled "
                "project preference when omitted; run `dsctl alert-group list` "
                "to discover ids."
            ),
        ),
    ] = None,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help="Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, or LOWEST.",
        ),
    ] = None,
    worker_group: Annotated[
        str | None,
        typer.Option(
            "--worker-group",
            help=(
                "Worker group for create explain or updated value for update "
                "explain. Create explain can also inherit enabled project "
                "preference when omitted; run `dsctl worker-group list` to "
                "discover values."
            ),
        ),
    ] = None,
    tenant_code: Annotated[
        str | None,
        typer.Option(
            "--tenant-code",
            help=(
                "Tenant code for create explain. Create explain can also "
                "inherit enabled project preference when omitted; run `dsctl "
                "tenant list` to discover values."
            ),
        ),
    ] = None,
    environment_code: Annotated[
        int | None,
        typer.Option(
            "--environment-code",
            min=0,
            help=(
                "Environment code for create explain or updated value for "
                "update explain. Create explain can also inherit enabled "
                "project preference when omitted; run `dsctl environment list` "
                "to discover values."
            ),
        ),
    ] = None,
) -> None:
    """Explain one schedule create or update mutation."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.explain",
        lambda: explain_schedule_result(
            schedule_id=schedule_id,
            workflow=workflow,
            project=project,
            cron=cron,
            start=start,
            end=end,
            timezone=timezone,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            priority=priority,
            worker_group=worker_group,
            tenant_code=tenant_code,
            environment_code=environment_code,
            env_file=env_file,
        ),
    )


@schedule_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    workflow: Annotated[
        str | None,
        typer.Option(
            "--workflow",
            help=WORKFLOW_HELP,
        ),
    ] = None,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=PROJECT_HELP,
        ),
    ] = None,
    cron: Annotated[
        str,
        typer.Option(
            "--cron",
            help="Quartz cron expression (6 or 7 fields, seconds first).",
        ),
    ],
    start: Annotated[
        str,
        typer.Option(
            "--start",
            help="Schedule start time in DS datetime string format.",
        ),
    ],
    end: Annotated[
        str,
        typer.Option(
            "--end",
            help="Schedule end time in DS datetime string format.",
        ),
    ],
    timezone: Annotated[
        str,
        typer.Option(
            "--timezone",
            help="Timezone id, for example Asia/Shanghai.",
        ),
    ],
    failure_strategy: Annotated[
        str | None,
        typer.Option(
            "--failure-strategy",
            help="Failure strategy: CONTINUE or END.",
        ),
    ] = None,
    warning_type: Annotated[
        str | None,
        typer.Option(
            "--warning-type",
            help="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
        ),
    ] = None,
    warning_group_id: Annotated[
        int | None,
        typer.Option(
            "--warning-group-id",
            min=0,
            help=(
                "Warning group id. Omit to keep the CLI fallback chain, "
                "including enabled project preference; run `dsctl alert-group "
                "list` to discover ids."
            ),
        ),
    ] = None,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help="Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, or LOWEST.",
        ),
    ] = None,
    worker_group: Annotated[
        str | None,
        typer.Option(
            "--worker-group",
            help=(
                "Worker group. Omit to allow enabled project preference; run "
                "`dsctl worker-group list` to discover values."
            ),
        ),
    ] = None,
    tenant_code: Annotated[
        str | None,
        typer.Option(
            "--tenant-code",
            help=(
                "Tenant code. Omit to allow enabled project preference; run "
                "`dsctl tenant list` to discover values."
            ),
        ),
    ] = None,
    environment_code: Annotated[
        int | None,
        typer.Option(
            "--environment-code",
            min=0,
            help=(
                "Environment code. Omit to keep the CLI fallback chain, "
                "including enabled project preference; run `dsctl environment "
                "list` to discover values."
            ),
        ),
    ] = None,
    confirm_risk: Annotated[
        str | None,
        typer.Option(
            "--confirm-risk",
            help="Confirm one high-risk schedule mutation token returned earlier.",
        ),
    ] = None,
) -> None:
    """Create one schedule."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.create",
        lambda: create_schedule_result(
            workflow=workflow,
            project=project,
            cron=cron,
            start=start,
            end=end,
            timezone=timezone,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            priority=priority,
            worker_group=worker_group,
            tenant_code=tenant_code,
            environment_code=environment_code,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@schedule_app.command("update")
def update_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int,
        typer.Argument(help=SCHEDULE_ID_HELP),
    ],
    *,
    cron: Annotated[
        str | None,
        typer.Option(
            "--cron",
            help=(
                "Updated Quartz cron expression (6 or 7 fields, seconds first). "
                "Omit to keep the current value."
            ),
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Updated schedule start time. Omit to keep the current value.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Updated schedule end time. Omit to keep the current value.",
        ),
    ] = None,
    timezone: Annotated[
        str | None,
        typer.Option(
            "--timezone",
            help="Updated timezone id. Omit to keep the current value.",
        ),
    ] = None,
    failure_strategy: Annotated[
        str | None,
        typer.Option(
            "--failure-strategy",
            help="Failure strategy: CONTINUE or END.",
        ),
    ] = None,
    warning_type: Annotated[
        str | None,
        typer.Option(
            "--warning-type",
            help="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
        ),
    ] = None,
    warning_group_id: Annotated[
        int | None,
        typer.Option(
            "--warning-group-id",
            min=0,
            help=(
                "Updated warning group id. Run `dsctl alert-group list` to "
                "discover ids; omit to keep the current value."
            ),
        ),
    ] = None,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help="Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, or LOWEST.",
        ),
    ] = None,
    worker_group: Annotated[
        str | None,
        typer.Option(
            "--worker-group",
            help=(
                "Updated worker group. Run `dsctl worker-group list` to "
                "discover values; omit to keep the current value."
            ),
        ),
    ] = None,
    environment_code: Annotated[
        int | None,
        typer.Option(
            "--environment-code",
            min=0,
            help=(
                "Updated environment code. Run `dsctl environment list` to "
                "discover values; omit to keep the current value."
            ),
        ),
    ] = None,
    confirm_risk: Annotated[
        str | None,
        typer.Option(
            "--confirm-risk",
            help="Confirm one high-risk schedule mutation token returned earlier.",
        ),
    ] = None,
) -> None:
    """Update one schedule."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.update",
        lambda: update_schedule_result(
            schedule_id,
            cron=cron,
            start=start,
            end=end,
            timezone=timezone,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            priority=priority,
            worker_group=worker_group,
            environment_code=environment_code,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@schedule_app.command("delete")
def delete_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int,
        typer.Argument(help=SCHEDULE_ID_HELP),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm schedule deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one schedule."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.delete",
        lambda: delete_schedule_result(
            schedule_id,
            force=force,
            env_file=env_file,
        ),
    )


@schedule_app.command("online")
def online_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int,
        typer.Argument(help=SCHEDULE_ID_HELP),
    ],
) -> None:
    """Bring one schedule online."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.online",
        lambda: online_schedule_result(schedule_id, env_file=env_file),
    )


@schedule_app.command("offline")
def offline_command(
    ctx: typer.Context,
    schedule_id: Annotated[
        int,
        typer.Argument(help=SCHEDULE_ID_HELP),
    ],
) -> None:
    """Bring one schedule offline."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.offline",
        lambda: offline_schedule_result(schedule_id, env_file=env_file),
    )
