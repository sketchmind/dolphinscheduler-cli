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
                "Return one group's action index. Discover groups with "
                "`dsctl schema` or `dsctl schema --list-groups`."
            ),
        ),
    ] = None,
    command: Annotated[
        str | None,
        typer.Option(
            "--command",
            help=(
                "Return one complete action-local contract. Discover actions "
                "with `dsctl schema` or `dsctl schema --group GROUP`."
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
            help="List valid action names for --command.",
        ),
    ] = False,
    full: Annotated[
        bool,
        typer.Option(
            "--full",
            help=(
                "Return the expanded schema representation. May be combined "
                "with --group or --command."
            ),
        ),
    ] = False,
) -> None:
    """Discover the CLI schema; no options returns the bounded action index."""
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
            full=full,
        ),
    )
