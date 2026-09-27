from pathlib import Path

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.commands._secret_input import read_password
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.user import (
    UNSET,
    PhoneUpdate,
    QueueUpdate,
    create_user_result,
    delete_user_result,
    get_user_result,
    grant_user_datasources_result,
    grant_user_namespaces_result,
    grant_user_project_result,
    list_users_result,
    revoke_user_datasources_result,
    revoke_user_namespaces_result,
    revoke_user_project_result,
    update_user_result,
)

user_app = typer.Typer(
    help="Manage DolphinScheduler users.",
    no_args_is_help=True,
)
user_grant_app = typer.Typer(
    help="Grant DolphinScheduler user permissions.",
    no_args_is_help=True,
)
user_revoke_app = typer.Typer(
    help="Revoke DolphinScheduler user permissions.",
    no_args_is_help=True,
)


def register_user_commands(app: typer.Typer) -> None:
    """Register the `user` command group."""
    user_app.add_typer(user_grant_app, name="grant")
    user_app.add_typer(user_revoke_app, name="revoke")
    app.add_typer(user_app, name="user")


@user_app.command("list")
@bind_command("user.list")
def list_command(
    ctx: typer.Context,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.list",
        lambda: list_users_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@user_app.command("get")
@bind_command("user.get")
def get_command(
    ctx: typer.Context,
    user: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.get",
        lambda: get_user_result(user, env_file=env_file),
    )


@user_app.command("create")
@bind_command("user.create")
def create_command(
    ctx: typer.Context,
    *,
    user_name: str,
    password: str | None,
    password_file: Path | None,
    email: str,
    tenant: str,
    state_value: int,
    phone: str | None,
    queue: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.create",
        lambda: create_user_result(
            user_name=user_name,
            password=read_password(password, password_file, required=True),
            email=email,
            tenant=tenant,
            state=state_value,
            phone=phone,
            queue=queue,
            env_file=env_file,
        ),
    )


@user_app.command("update")
@bind_command("user.update")
def update_command(
    ctx: typer.Context,
    user: str,
    *,
    user_name: str | None,
    password: str | None,
    password_file: Path | None,
    email: str | None,
    tenant: str | None,
    state_value: int | None,
    phone: str | None,
    clear_phone: bool,
    queue: str | None,
    clear_queue: bool,
    time_zone: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        phone_update: PhoneUpdate
        queue_update: QueueUpdate

        if phone is not None and clear_phone:
            message = "--phone and --clear-phone cannot be used together"
            raise UserInputError(message)
        if queue is not None and clear_queue:
            message = "--queue and --clear-queue cannot be used together"
            raise UserInputError(message)

        if clear_phone:
            phone_update = None
        elif phone is not None:
            phone_update = phone
        else:
            phone_update = UNSET

        if clear_queue:
            queue_update = None
        elif queue is not None:
            queue_update = queue
        else:
            queue_update = UNSET

        return update_user_result(
            user,
            user_name=user_name,
            password=read_password(password, password_file, required=False),
            email=email,
            tenant=tenant,
            state=state_value,
            phone=phone_update,
            queue=queue_update,
            time_zone=time_zone,
            env_file=env_file,
        )

    emit_result("user.update", build_result)


@user_app.command("delete")
@bind_command("user.delete")
def delete_command(
    ctx: typer.Context,
    user: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.delete",
        lambda: delete_user_result(
            user,
            force=force,
            env_file=env_file,
        ),
    )


@user_grant_app.command("project")
@bind_command("user.grant.project")
def grant_project_command(
    ctx: typer.Context,
    user: str,
    project: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.grant.project",
        lambda: grant_user_project_result(
            user,
            project,
            env_file=env_file,
        ),
    )


@user_grant_app.command("datasource")
@bind_command("user.grant.datasource")
def grant_datasource_command(
    ctx: typer.Context,
    user: str,
    *,
    datasource: list[str],
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.grant.datasource",
        lambda: grant_user_datasources_result(
            user,
            datasource,
            env_file=env_file,
        ),
    )


@user_grant_app.command("namespace")
@bind_command("user.grant.namespace")
def grant_namespace_command(
    ctx: typer.Context,
    user: str,
    *,
    namespace: list[str],
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.grant.namespace",
        lambda: grant_user_namespaces_result(
            user,
            namespace,
            env_file=env_file,
        ),
    )


@user_revoke_app.command("project")
@bind_command("user.revoke.project")
def revoke_project_command(
    ctx: typer.Context,
    user: str,
    project: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.revoke.project",
        lambda: revoke_user_project_result(
            user,
            project,
            env_file=env_file,
        ),
    )


@user_revoke_app.command("datasource")
@bind_command("user.revoke.datasource")
def revoke_datasource_command(
    ctx: typer.Context,
    user: str,
    *,
    datasource: list[str],
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.revoke.datasource",
        lambda: revoke_user_datasources_result(
            user,
            datasource,
            env_file=env_file,
        ),
    )


@user_revoke_app.command("namespace")
@bind_command("user.revoke.namespace")
def revoke_namespace_command(
    ctx: typer.Context,
    user: str,
    *,
    namespace: list[str],
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.revoke.namespace",
        lambda: revoke_user_namespaces_result(
            user,
            namespace,
            env_file=env_file,
        ),
    )
