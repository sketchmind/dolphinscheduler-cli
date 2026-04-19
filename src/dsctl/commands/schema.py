from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.schema import get_schema_result


def register_schema_commands(app: typer.Typer) -> None:
    """Register the top-level `schema` command."""
    app.command("schema")(schema_command)


def schema_command(
    ctx: typer.Context,
    *,
    group: Annotated[
        str | None,
        typer.Option(
            "--group",
            help=(
                "Return schema for one command group. Discover values with "
                "`dsctl schema --list-groups`."
            ),
        ),
    ] = None,
    command: Annotated[
        str | None,
        typer.Option(
            "--command",
            help=(
                "Return schema for one stable command action. Discover values "
                "with `dsctl schema --list-commands`."
            ),
        ),
    ] = None,
    list_groups: Annotated[
        bool,
        typer.Option(
            "--list-groups",
            help="List valid values for --group.",
        ),
    ] = False,
    list_commands: Annotated[
        bool,
        typer.Option(
            "--list-commands",
            help="List valid values for --command.",
        ),
    ] = False,
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
            list_groups=list_groups,
            list_commands=list_commands,
        ),
    )
