from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import AppState
from dsctl.commands.registry import register_all_commands

app = typer.Typer(
    add_completion=False,
    help="Generated-first REST-only DolphinScheduler CLI.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)


@app.callback()
def main_callback(
    ctx: typer.Context,
    env_file: Annotated[
        Path | None,
        typer.Option(
            "--env-file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Load DS_* settings from an env file before reading the process"
                " environment."
            ),
            readable=True,
            resolve_path=True,
        ),
    ] = None,
) -> None:
    """Initialize shared command state."""
    ctx.obj = AppState(env_file=env_file)


def main() -> None:
    """Run the Typer application."""
    app()


register_all_commands(app)
