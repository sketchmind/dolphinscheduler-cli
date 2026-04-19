from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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

USER_HELP = "User name or numeric id. Run `dsctl user list` to discover values."


def register_user_commands(app: typer.Typer) -> None:
    """Register the `user` command group."""
    user_app.add_typer(user_grant_app, name="grant")
    user_app.add_typer(user_revoke_app, name="revoke")
    app.add_typer(user_app, name="user")


@user_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter users by user name using the upstream search value.",
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
    """List users with optional filtering and pagination controls."""
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
def get_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
) -> None:
    """Get one user by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.get",
        lambda: get_user_result(user, env_file=env_file),
    )


@user_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    user_name: Annotated[
        str,
        typer.Option(
            "--user-name",
            help="User name.",
        ),
    ],
    password: Annotated[
        str,
        typer.Option(
            "--password",
            help="Plain-text user password.",
        ),
    ],
    email: Annotated[
        str,
        typer.Option(
            "--email",
            help="User email.",
        ),
    ],
    tenant: Annotated[
        str,
        typer.Option(
            "--tenant",
            help=(
                "Tenant code or numeric id. Run `dsctl tenant list` to discover values."
            ),
        ),
    ],
    state_value: Annotated[
        int,
        typer.Option(
            "--state",
            min=0,
            max=1,
            help="User state. Use 1 for enabled and 0 for disabled.",
        ),
    ],
    phone: Annotated[
        str | None,
        typer.Option(
            "--phone",
            help="Optional user phone.",
        ),
    ] = None,
    queue: Annotated[
        str | None,
        typer.Option(
            "--queue",
            help=(
                "Optional queue-name override stored on the user. Run "
                "`dsctl queue list` to discover queue names."
            ),
        ),
    ] = None,
) -> None:
    """Create one user."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "user.create",
        lambda: create_user_result(
            user_name=user_name,
            password=password,
            email=email,
            tenant=tenant,
            state=state_value,
            phone=phone,
            queue=queue,
            env_file=env_file,
        ),
    )


@user_app.command("update")
def update_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    *,
    user_name: Annotated[
        str | None,
        typer.Option(
            "--user-name",
            help="Updated user name.",
        ),
    ] = None,
    password: Annotated[
        str | None,
        typer.Option(
            "--password",
            help="Updated plain-text user password.",
        ),
    ] = None,
    email: Annotated[
        str | None,
        typer.Option(
            "--email",
            help="Updated user email.",
        ),
    ] = None,
    tenant: Annotated[
        str | None,
        typer.Option(
            "--tenant",
            help=(
                "Updated tenant code or numeric id. Run `dsctl tenant list` to "
                "discover values."
            ),
        ),
    ] = None,
    state_value: Annotated[
        int | None,
        typer.Option(
            "--state",
            min=0,
            max=1,
            help="Updated user state. Use 1 for enabled and 0 for disabled.",
        ),
    ] = None,
    phone: Annotated[
        str | None,
        typer.Option(
            "--phone",
            help="Updated user phone.",
        ),
    ] = None,
    clear_phone: Annotated[
        bool,
        typer.Option(
            "--clear-phone",
            help="Clear the stored user phone.",
        ),
    ] = False,
    queue: Annotated[
        str | None,
        typer.Option(
            "--queue",
            help=(
                "Updated queue-name override stored on the user. Run "
                "`dsctl queue list` to discover queue names."
            ),
        ),
    ] = None,
    clear_queue: Annotated[
        bool,
        typer.Option(
            "--clear-queue",
            help="Clear the stored queue-name override.",
        ),
    ] = False,
    time_zone: Annotated[
        str | None,
        typer.Option(
            "--time-zone",
            help="Updated IANA time zone.",
        ),
    ] = None,
) -> None:
    """Update one user."""
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
            password=password,
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
def delete_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm user deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one user."""
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
def grant_project_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    project: Annotated[
        str,
        typer.Argument(
            help=(
                "Project name or numeric code. Run `dsctl project list` "
                "to discover values."
            )
        ),
    ],
) -> None:
    """Grant one project to one user with write permission."""
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
def grant_datasource_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    *,
    datasource: Annotated[
        list[str],
        typer.Option(
            "--datasource",
            help=(
                "Datasource name or numeric id. Repeat to grant multiple "
                "datasources; run `dsctl datasource list` to discover values."
            ),
        ),
    ],
) -> None:
    """Grant one or more datasources to one user."""
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
def grant_namespace_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    *,
    namespace: Annotated[
        list[str],
        typer.Option(
            "--namespace",
            help=(
                "Namespace name or numeric id. Repeat to grant multiple "
                "namespaces; run `dsctl namespace list` to discover values."
            ),
        ),
    ],
) -> None:
    """Grant one or more namespaces to one user."""
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
def revoke_project_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    project: Annotated[
        str,
        typer.Argument(
            help=(
                "Project name or numeric code. Run `dsctl project list` "
                "to discover values."
            )
        ),
    ],
) -> None:
    """Revoke one project from one user."""
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
def revoke_datasource_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    *,
    datasource: Annotated[
        list[str],
        typer.Option(
            "--datasource",
            help=(
                "Datasource name or numeric id. Repeat to revoke multiple "
                "datasources; run `dsctl datasource list` to discover values."
            ),
        ),
    ],
) -> None:
    """Revoke one or more datasources from one user."""
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
def revoke_namespace_command(
    ctx: typer.Context,
    user: Annotated[
        str,
        typer.Argument(help=USER_HELP),
    ],
    *,
    namespace: Annotated[
        list[str],
        typer.Option(
            "--namespace",
            help=(
                "Namespace name or numeric id. Repeat to revoke multiple "
                "namespaces; run `dsctl namespace list` to discover values."
            ),
        ),
    ],
) -> None:
    """Revoke one or more namespaces from one user."""
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
