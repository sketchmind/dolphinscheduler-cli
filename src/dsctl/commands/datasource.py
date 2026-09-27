from pathlib import Path

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.datasource import (
    connection_test_datasource_result,
    create_datasource_result,
    delete_datasource_result,
    get_datasource_result,
    list_datasources_result,
    update_datasource_result,
)

datasource_app = typer.Typer(
    help=(
        "Manage DolphinScheduler datasources. Create/update use DS-native JSON "
        "payload files."
    ),
    no_args_is_help=True,
)


def register_datasource_commands(app: typer.Typer) -> None:
    """Register the `datasource` command group."""
    app.add_typer(datasource_app, name="datasource")


@datasource_app.command("list")
@bind_command("datasource.list")
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
        "datasource.list",
        lambda: list_datasources_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@datasource_app.command("get")
@bind_command("datasource.get")
def get_command(
    ctx: typer.Context,
    datasource: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.get",
        lambda: get_datasource_result(datasource, env_file=env_file),
    )


@datasource_app.command("create")
@bind_command("datasource.create")
def create_command(
    ctx: typer.Context,
    *,
    file: Path,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.create",
        lambda: create_datasource_result(file=file, env_file=env_file),
    )


@datasource_app.command("update")
@bind_command("datasource.update")
def update_command(
    ctx: typer.Context,
    datasource: str,
    *,
    file: Path,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.update",
        lambda: update_datasource_result(
            datasource,
            file=file,
            env_file=env_file,
        ),
    )


@datasource_app.command("delete")
@bind_command("datasource.delete")
def delete_command(
    ctx: typer.Context,
    datasource: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.delete",
        lambda: delete_datasource_result(
            datasource,
            force=force,
            env_file=env_file,
        ),
    )


@datasource_app.command("test")
@bind_command("datasource.test")
def test_command(
    ctx: typer.Context,
    datasource: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.test",
        lambda: connection_test_datasource_result(
            datasource,
            env_file=env_file,
        ),
    )
