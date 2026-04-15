from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.pagination import DEFAULT_PAGE_SIZE
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
def list_command(
    ctx: typer.Context,
    *,
    directory: Annotated[
        str | None,
        typer.Option(
            "--dir",
            help="DS directory fullName path. Defaults to the upstream base directory.",
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter resource names by the upstream search value.",
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
    ] = DEFAULT_PAGE_SIZE,
    all_pages: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Fetch all remaining pages up to the safety limit.",
        ),
    ] = False,
) -> None:
    """List resources inside one DS directory."""
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
def view_command(
    ctx: typer.Context,
    resource: Annotated[
        str,
        typer.Argument(help="DS resource fullName path."),
    ],
    *,
    skip_line_num: Annotated[
        int,
        typer.Option(
            "--skip-line-num",
            min=0,
            help="Number of lines to skip before returning content.",
        ),
    ] = 0,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            min=1,
            help="Maximum number of lines to fetch.",
        ),
    ] = 100,
) -> None:
    """View one text content window for one resource file."""
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
def upload_command(
    ctx: typer.Context,
    *,
    file: Annotated[
        Path,
        typer.Option(
            "--file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help="Local file path to upload.",
            readable=True,
            resolve_path=True,
        ),
    ],
    directory: Annotated[
        str | None,
        typer.Option(
            "--dir",
            help=(
                "Destination DS directory fullName path. Defaults to the "
                "upstream base directory."
            ),
        ),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Override the remote leaf file name. Defaults to the local file name.",
        ),
    ] = None,
) -> None:
    """Upload one local file into one DS directory."""
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
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Remote leaf file name, including the file extension.",
        ),
    ],
    content: Annotated[
        str,
        typer.Option(
            "--content",
            help="Inline text content to write into the remote resource file.",
        ),
    ],
    directory: Annotated[
        str | None,
        typer.Option(
            "--dir",
            help=(
                "Destination DS directory fullName path. Defaults to the "
                "upstream base directory."
            ),
        ),
    ] = None,
) -> None:
    """Create one text resource from inline content."""
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
def mkdir_command(
    ctx: typer.Context,
    name: Annotated[
        str,
        typer.Argument(help="Leaf directory name to create."),
    ],
    *,
    directory: Annotated[
        str | None,
        typer.Option(
            "--dir",
            help=(
                "Parent DS directory fullName path. Defaults to the upstream "
                "base directory."
            ),
        ),
    ] = None,
) -> None:
    """Create one directory inside one DS resource directory."""
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
def download_command(
    ctx: typer.Context,
    resource: Annotated[
        str,
        typer.Argument(help="DS resource fullName path."),
    ],
    *,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            file_okay=True,
            dir_okay=True,
            help=(
                "Local output file path or existing directory. Defaults to the "
                "current working directory plus the remote leaf name."
            ),
            resolve_path=True,
        ),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite",
            help="Replace an existing local output file.",
        ),
    ] = False,
) -> None:
    """Download one remote resource to one local file path."""
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
def delete_command(
    ctx: typer.Context,
    resource: Annotated[
        str,
        typer.Argument(help="DS resource fullName path."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm resource deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one resource."""
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
