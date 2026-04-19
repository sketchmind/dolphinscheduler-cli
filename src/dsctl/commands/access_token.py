from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.access_token import (
    create_access_token_result,
    delete_access_token_result,
    generate_access_token_result,
    get_access_token_result,
    list_access_tokens_result,
    update_access_token_result,
)

access_token_app = typer.Typer(
    help="Manage DolphinScheduler access tokens.",
    no_args_is_help=True,
)

ACCESS_ID_HELP = "Access-token id. Run `dsctl access-token list` to discover values."


def register_access_token_commands(app: typer.Typer) -> None:
    """Register the `access-token` command group."""
    app.add_typer(access_token_app, name="access-token")


@access_token_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter access tokens using the upstream search value.",
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
    """List access tokens with optional filtering and pagination controls."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.list",
        lambda: list_access_tokens_result(
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@access_token_app.command("get")
def get_command(
    ctx: typer.Context,
    access_token: Annotated[
        int,
        typer.Argument(help=ACCESS_ID_HELP),
    ],
) -> None:
    """Get one access token by numeric id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.get",
        lambda: get_access_token_result(access_token, env_file=env_file),
    )


@access_token_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    user: Annotated[
        str,
        typer.Option(
            "--user",
            help="User name or numeric id. Run `dsctl user list` to discover values.",
        ),
    ],
    expire_time: Annotated[
        str,
        typer.Option(
            "--expire-time",
            help="Token expiration time, for example '2027-01-01 00:00:00'.",
        ),
    ],
    token: Annotated[
        str | None,
        typer.Option(
            "--token",
            help="Optional token string. Omit to let DS generate one.",
        ),
    ] = None,
) -> None:
    """Create one access token."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.create",
        lambda: create_access_token_result(
            user=user,
            expire_time=expire_time,
            token=token,
            env_file=env_file,
        ),
    )


@access_token_app.command("update")
def update_command(
    ctx: typer.Context,
    access_token: Annotated[
        int,
        typer.Argument(help=ACCESS_ID_HELP),
    ],
    *,
    user: Annotated[
        str | None,
        typer.Option(
            "--user",
            help=(
                "Updated user name or numeric id. Run `dsctl user list` to "
                "discover values."
            ),
        ),
    ] = None,
    expire_time: Annotated[
        str | None,
        typer.Option(
            "--expire-time",
            help=("Updated token expiration time, for example '2027-01-01 00:00:00'."),
        ),
    ] = None,
    token: Annotated[
        str | None,
        typer.Option(
            "--token",
            help="Updated token string.",
        ),
    ] = None,
    regenerate_token: Annotated[
        bool,
        typer.Option(
            "--regenerate-token",
            help="Ask DS to generate a fresh token string.",
        ),
    ] = False,
) -> None:
    """Update one access token by numeric id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.update",
        lambda: update_access_token_result(
            access_token,
            user=user,
            expire_time=expire_time,
            token=token,
            regenerate_token=regenerate_token,
            env_file=env_file,
        ),
    )


@access_token_app.command("delete")
def delete_command(
    ctx: typer.Context,
    access_token: Annotated[
        int,
        typer.Argument(help=ACCESS_ID_HELP),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm access-token deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one access token by numeric id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.delete",
        lambda: delete_access_token_result(
            access_token,
            force=force,
            env_file=env_file,
        ),
    )


@access_token_app.command("generate")
def generate_command(
    ctx: typer.Context,
    *,
    user: Annotated[
        str,
        typer.Option(
            "--user",
            help="User name or numeric id. Run `dsctl user list` to discover values.",
        ),
    ],
    expire_time: Annotated[
        str,
        typer.Option(
            "--expire-time",
            help="Token expiration time, for example '2027-01-01 00:00:00'.",
        ),
    ],
) -> None:
    """Generate one token string without persisting it."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.generate",
        lambda: generate_access_token_result(
            user=user,
            expire_time=expire_time,
            env_file=env_file,
        ),
    )
