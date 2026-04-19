from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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

DATASOURCE_HELP = (
    "Datasource name or numeric id. Run `dsctl datasource list` to discover values."
)


def register_datasource_commands(app: typer.Typer) -> None:
    """Register the `datasource` command group."""
    app.add_typer(datasource_app, name="datasource")


@datasource_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter datasources by name using the upstream search value.",
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
    """List datasource identities and summary fields."""
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
def get_command(
    ctx: typer.Context,
    datasource: Annotated[
        str,
        typer.Argument(help=DATASOURCE_HELP),
    ],
) -> None:
    """Get one datasource by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.get",
        lambda: get_datasource_result(datasource, env_file=env_file),
    )


@datasource_app.command("create")
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
                "Path to one DS-native datasource JSON payload file. Start "
                "with `dsctl template datasource`, then "
                "`dsctl template datasource --type TYPE` and pass the saved "
                "data.json path here."
            ),
            readable=True,
            resolve_path=True,
        ),
    ],
) -> None:
    """Create one datasource from a JSON payload file."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.create",
        lambda: create_datasource_result(file=file, env_file=env_file),
    )


@datasource_app.command("update")
def update_command(
    ctx: typer.Context,
    datasource: Annotated[
        str,
        typer.Argument(help=DATASOURCE_HELP),
    ],
    *,
    file: Annotated[
        Path,
        typer.Option(
            "--file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Path to one DS-native datasource JSON payload file. Start from "
                "`dsctl datasource get DATASOURCE` or "
                "`dsctl template datasource --type TYPE`, then pass the saved "
                "JSON path here. Masked password ****** preserves the existing "
                "password."
            ),
            readable=True,
            resolve_path=True,
        ),
    ],
) -> None:
    """Update one datasource from a JSON payload file."""
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
def delete_command(
    ctx: typer.Context,
    datasource: Annotated[
        str,
        typer.Argument(help=DATASOURCE_HELP),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm datasource deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one datasource by name or id."""
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
def test_command(
    ctx: typer.Context,
    datasource: Annotated[
        str,
        typer.Argument(help=DATASOURCE_HELP),
    ],
) -> None:
    """Run one datasource connection test after create or update."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "datasource.test",
        lambda: connection_test_datasource_result(
            datasource,
            env_file=env_file,
        ),
    )
