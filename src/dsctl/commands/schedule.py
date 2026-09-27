import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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


def register_schedule_commands(app: typer.Typer) -> None:
    """Register the `schedule` command group."""
    app.add_typer(schedule_app, name="schedule")


@schedule_app.command("list")
@bind_command("schedule.list")
def list_command(
    ctx: typer.Context,
    *,
    project: str | None,
    workflow: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
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
@bind_command("schedule.get")
def get_command(
    ctx: typer.Context,
    schedule_id: int,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.get",
        lambda: get_schedule_result(
            schedule_id,
            project=project,
            env_file=env_file,
        ),
    )


@schedule_app.command("preview")
@bind_command("schedule.preview")
def preview_command(
    ctx: typer.Context,
    schedule_id: int | None,
    *,
    project: str | None,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
) -> None:
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
@bind_command("schedule.explain")
def explain_command(
    ctx: typer.Context,
    schedule_id: int | None,
    *,
    workflow: str | None,
    project: str | None,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    missed_fire_policy: str | None,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    priority: str | None,
    worker_group: str | None,
    tenant_code: str | None,
    environment_code: int | None,
) -> None:
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
            missed_fire_policy=missed_fire_policy,
            env_file=env_file,
        ),
    )


@schedule_app.command("create")
@bind_command("schedule.create")
def create_command(
    ctx: typer.Context,
    *,
    workflow: str,
    project: str | None,
    cron: str,
    start: str,
    end: str,
    timezone: str | None,
    missed_fire_policy: str | None,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    priority: str | None,
    worker_group: str | None,
    tenant_code: str | None,
    environment_code: int | None,
    confirm_risk: str | None,
) -> None:
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
            missed_fire_policy=missed_fire_policy,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@schedule_app.command("update")
@bind_command("schedule.update")
def update_command(
    ctx: typer.Context,
    schedule_id: int,
    *,
    project: str | None,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    missed_fire_policy: str | None,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    priority: str | None,
    worker_group: str | None,
    environment_code: int | None,
    confirm_risk: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.update",
        lambda: update_schedule_result(
            schedule_id,
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
            environment_code=environment_code,
            missed_fire_policy=missed_fire_policy,
            confirm_risk=confirm_risk,
            env_file=env_file,
        ),
    )


@schedule_app.command("delete")
@bind_command("schedule.delete")
def delete_command(
    ctx: typer.Context,
    schedule_id: int,
    *,
    project: str | None,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.delete",
        lambda: delete_schedule_result(
            schedule_id,
            project=project,
            force=force,
            env_file=env_file,
        ),
    )


@schedule_app.command("online")
@bind_command("schedule.online")
def online_command(
    ctx: typer.Context,
    schedule_id: int,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.online",
        lambda: online_schedule_result(
            schedule_id,
            project=project,
            env_file=env_file,
        ),
    )


@schedule_app.command("offline")
@bind_command("schedule.offline")
def offline_command(
    ctx: typer.Context,
    schedule_id: int,
    *,
    project: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schedule.offline",
        lambda: offline_schedule_result(
            schedule_id,
            project=project,
            env_file=env_file,
        ),
    )
