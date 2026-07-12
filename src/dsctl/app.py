import sys
from pathlib import Path
from typing import Annotated, cast

import typer

from dsctl.cli_runtime import AppState, set_app_state
from dsctl.command_contract import COMMAND_CATALOG, CommandBindingError
from dsctl.commands._contract_adapter import typer_option
from dsctl.commands.registry import register_all_commands
from dsctl.errors import UserInputError
from dsctl.output_formats import (
    OutputFormat,
    RenderOptions,
    parse_columns,
)

_ROOT_OPTION_ARITY = {
    option.flag: option.arity for option in COMMAND_CATALOG.global_options
}
_ROOT_OPTION_EXAMPLES = {
    option.flag: option.example for option in COMMAND_CATALOG.global_options
}
_ENV_FILE = COMMAND_CATALOG.global_option("env-file").input
_OUTPUT_FORMAT = COMMAND_CATALOG.global_option("output-format").input
_COLUMNS = COMMAND_CATALOG.global_option("columns").input
_COMPACT = COMMAND_CATALOG.global_option("compact").input

_ROOT_HELP = (
    "Generated-first REST-only DolphinScheduler CLI.\n\n"
    "Agent path: inspect only the command you will execute next, using its "
    "leaf `--help` or `dsctl schema --command ACTION`. If the action is "
    "unknown, inspect one relevant group; do not preload unrelated groups or "
    "downstream lifecycle actions.\n\n"
    "Successful JSON may include complete, output-bounded `next_actions` "
    "commands. When one matches the current goal and is authorized, run that "
    "command unchanged; never parallelize `mutates=true` with reads that "
    "depend on that mutation."
)

app = typer.Typer(
    add_completion=False,
    help=_ROOT_HELP,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)


@app.callback()
def main_callback(
    ctx: typer.Context,
    env_file: Annotated[
        Path | None,
        typer_option(_ENV_FILE),
    ] = None,
    output_format: Annotated[
        str,
        typer_option(_OUTPUT_FORMAT),
    ] = "json",
    columns: Annotated[
        str | None,
        typer_option(_COLUMNS),
    ] = None,
    *,
    compact: Annotated[
        bool,
        typer_option(_COMPACT),
    ] = False,
) -> None:
    """Initialize shared command state."""
    format_choice = _parse_output_format(output_format)
    try:
        parsed_columns = parse_columns(columns)
    except UserInputError as exc:
        raise typer.BadParameter(exc.message) from exc
    state = AppState(
        env_file=env_file,
        render_options=RenderOptions(
            output_format=format_choice,
            columns=parsed_columns,
            compact=compact,
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


def _parse_output_format(value: str) -> OutputFormat:
    """Parse one Typer string option into the stable output-format literal."""
    try:
        normalized = COMMAND_CATALOG.validate_global_values({"output-format": value})[
            "output-format"
        ]
    except CommandBindingError as exc:
        message = f"Unsupported output format: {value}"
        raise typer.BadParameter(message) from exc
    return cast("OutputFormat", normalized)


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
            arity = _ROOT_OPTION_ARITY[option]
            index += 1 if "=" in arg else 1 + arity
            continue

        if arg.startswith("-"):
            index += 1
            continue

        seen_command = True
        index += 1
    return None


def _root_option_name(token: str) -> str | None:
    for option in _ROOT_OPTION_ARITY:
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
