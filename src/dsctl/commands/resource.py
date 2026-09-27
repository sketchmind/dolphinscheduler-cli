from pathlib import Path

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.resource import (
    create_resource_result,
    delete_resource_result,
    download_resource_result,
    list_resources_result,
    mkdir_resource_result,
    upload_resource_result,
    view_resource_result,
)

resource_app = typer.Typer(
    help="Manage DolphinScheduler file resources.",
    no_args_is_help=True,
)


def register_resource_commands(app: typer.Typer) -> None:
    """Register the `resource` command group."""
    app.add_typer(resource_app, name="resource")


@resource_app.command("list")
@bind_command("resource.list")
def list_command(
    ctx: typer.Context,
    *,
    directory: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.list",
        lambda: list_resources_result(
            env_file=env_file,
            directory=directory,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@resource_app.command("view")
@bind_command("resource.view")
def view_command(
    ctx: typer.Context,
    resource: str,
    *,
    skip_line_num: int,
    limit: int,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.view",
        lambda: view_resource_result(
            resource,
            skip_line_num=skip_line_num,
            limit=limit,
            env_file=env_file,
        ),
    )


@resource_app.command("upload")
@bind_command("resource.upload")
def upload_command(
    ctx: typer.Context,
    *,
    file: Path,
    directory: str | None,
    name: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.upload",
        lambda: upload_resource_result(
            file=file,
            directory=directory,
            name=name,
            env_file=env_file,
        ),
    )


@resource_app.command("create")
@bind_command("resource.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    content: str,
    directory: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.create",
        lambda: create_resource_result(
            name=name,
            content=content,
            directory=directory,
            env_file=env_file,
        ),
    )


@resource_app.command("mkdir")
@bind_command("resource.mkdir")
def mkdir_command(
    ctx: typer.Context,
    name: str,
    *,
    directory: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.mkdir",
        lambda: mkdir_resource_result(
            name=name,
            directory=directory,
            env_file=env_file,
        ),
    )


@resource_app.command("download")
@bind_command("resource.download")
def download_command(
    ctx: typer.Context,
    resource: str,
    *,
    output: Path | None,
    overwrite: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.download",
        lambda: download_resource_result(
            resource,
            output=output,
            overwrite=overwrite,
            env_file=env_file,
        ),
    )


@resource_app.command("delete")
@bind_command("resource.delete")
def delete_command(
    ctx: typer.Context,
    resource: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "resource.delete",
        lambda: delete_resource_result(
            resource,
            force=force,
            env_file=env_file,
        ),
    )
