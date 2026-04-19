from __future__ import annotations

from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.enums import list_enum_names_result, list_enum_result

enum_app = typer.Typer(
    help="Discover generated DolphinScheduler enums. Start with `dsctl enum names`.",
    no_args_is_help=True,
)


def register_enum_commands(app: typer.Typer) -> None:
    """Register the `enum` command group."""
    app.add_typer(enum_app, name="enum")


@enum_app.command("names")
def names_command(ctx: typer.Context) -> None:
    """List supported generated enum discovery names."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("enum.names", lambda: list_enum_names_result(env_file=env_file))


@enum_app.command("list")
def list_command(
    ctx: typer.Context,
    enum_name: Annotated[
        str,
        typer.Argument(
            help=(
                "Stable enum discovery name. Run `dsctl enum names` to list "
                "supported values."
            ),
            show_choices=False,
        ),
    ],
) -> None:
    """List the members of one supported generated enum."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("enum.list", lambda: list_enum_result(enum_name, env_file=env_file))


__all__ = ["register_enum_commands"]
