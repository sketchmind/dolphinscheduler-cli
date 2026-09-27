import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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


def register_access_token_commands(app: typer.Typer) -> None:
    """Register the `access-token` command group."""
    app.add_typer(access_token_app, name="access-token")


@access_token_app.command("list")
@bind_command("access-token.list")
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
@bind_command("access-token.get")
def get_command(
    ctx: typer.Context,
    access_token: int,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "access-token.get",
        lambda: get_access_token_result(access_token, env_file=env_file),
    )


@access_token_app.command("create")
@bind_command("access-token.create")
def create_command(
    ctx: typer.Context,
    *,
    user: str,
    expire_time: str,
    token: str | None,
) -> None:
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
@bind_command("access-token.update")
def update_command(
    ctx: typer.Context,
    access_token: int,
    *,
    user: str | None,
    expire_time: str | None,
    token: str | None,
    regenerate_token: bool,
) -> None:
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
@bind_command("access-token.delete")
def delete_command(
    ctx: typer.Context,
    access_token: int,
    *,
    force: bool,
) -> None:
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
@bind_command("access-token.generate")
def generate_command(
    ctx: typer.Context,
    *,
    user: str,
    expire_time: str,
) -> None:
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
