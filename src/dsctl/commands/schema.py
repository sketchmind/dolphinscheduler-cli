from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.schema import get_schema_result


def register_schema_commands(app: typer.Typer) -> None:
    """Register the top-level `schema` command."""
    app.command("schema")(schema_command)


def schema_command(
    ctx: typer.Context,
    group: Annotated[
        str | None,
        typer.Option(
            "--group",
            help="Return schema for one command group.",
        ),
    ] = None,
    command: Annotated[
        str | None,
        typer.Option(
            "--command",
            help="Return schema for one stable command action.",
        ),
    ] = None,
) -> None:
    """Print the stable machine-readable CLI schema."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schema",
        lambda: get_schema_result(
            env_file=env_file,
            group=group,
            command_action=command,
        ),
    )
