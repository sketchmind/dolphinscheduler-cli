from pathlib import Path
from typing import Annotated, cast

import typer

from dsctl.cli_runtime import AppState, set_app_state
from dsctl.commands.registry import register_all_commands
from dsctl.errors import UserInputError
from dsctl.output_formats import (
    OUTPUT_FORMAT_CHOICES,
    OutputFormat,
    RenderOptions,
    parse_columns,
)

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
    output_format: Annotated[
        str,
        typer.Option(
            "--output-format",
            help=(
                "Render the standard envelope as json, or render row-oriented "
                "views as table/tsv."
            ),
        ),
    ] = "json",
    columns: Annotated[
        str | None,
        typer.Option(
            "--columns",
            help=(
                "Comma-separated display columns for --output-format table or "
                "--output-format tsv."
            ),
        ),
    ] = None,
) -> None:
    """Initialize shared command state."""
    normalized_format = output_format.lower()
    if normalized_format not in OUTPUT_FORMAT_CHOICES:
        message = f"Unsupported output format: {output_format}"
        raise typer.BadParameter(message)
    format_choice = cast("OutputFormat", normalized_format)
    try:
        parsed_columns = parse_columns(columns)
    except UserInputError as exc:
        raise typer.BadParameter(exc.message) from exc
    state = AppState(
        env_file=env_file,
        render_options=RenderOptions(
            output_format=format_choice,
            columns=parsed_columns,
        ),
    )
    ctx.obj = state
    set_app_state(state)


def main() -> None:
    """Run the Typer application."""
    app()


register_all_commands(app)
