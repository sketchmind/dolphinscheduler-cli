import sys
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

_ROOT_OPTIONS_WITH_VALUES = frozenset({"--env-file", "--output-format", "--columns"})
_ROOT_OPTION_EXAMPLES = {
    "--env-file": "dsctl --env-file cluster.env <command> ...",
    "--output-format": "dsctl --output-format table <command> ...",
    "--columns": "dsctl --columns id,name,state <command> ...",
}

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
                "Render the standard envelope as json, or render row/object "
                "views as table/tsv."
            ),
        ),
    ] = "json",
    columns: Annotated[
        str | None,
        typer.Option(
            "--columns",
            help=(
                "Comma-separated row/object fields to render or project. In json "
                "mode this narrows the standard envelope data payload."
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
    misplaced = _misplaced_root_option(sys.argv[1:])
    if misplaced is not None:
        _show_misplaced_root_option_error(misplaced)
        raise SystemExit(2)
    app()


register_all_commands(app)


def _misplaced_root_option(args: list[str]) -> str | None:
    """Return a root-only option that appears after the command path."""
    seen_command = False
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--":
            return None

        option = _root_option_name(arg)
        if option is not None:
            if seen_command:
                return option
            index += 1 if "=" in arg else 2
            continue

        if arg.startswith("-"):
            index += 1
            continue

        seen_command = True
        index += 1
    return None


def _root_option_name(token: str) -> str | None:
    for option in _ROOT_OPTIONS_WITH_VALUES:
        if token == option or token.startswith(f"{option}="):
            return option
    return None


def _show_misplaced_root_option_error(option: str) -> None:
    example = _ROOT_OPTION_EXAMPLES[option]
    typer.echo(
        (
            f"{option} is a global dsctl option. Put it before the command "
            f"group, for example: {example}"
        ),
        err=True,
    )
