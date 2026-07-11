import sys
from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import AppState, set_app_state
from dsctl.commands.registry import register_all_commands
from dsctl.errors import UserInputError
from dsctl.output_formats import (
    OutputFormat,
    RenderOptions,
    parse_columns,
)

_ROOT_OPTION_ARITY = {
    "--env-file": 1,
    "--output-format": 1,
    "--columns": 1,
    "--compact": 0,
}
_ROOT_OPTION_EXAMPLES = {
    "--env-file": "dsctl --env-file cluster.env <command> ...",
    "--output-format": "dsctl --output-format table <command> ...",
    "--columns": "dsctl --columns id,name,state <command> ...",
    "--compact": "dsctl --compact <command> ...",
}

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
        typer.Option(
            "--env-file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Global option; place before COMMAND. Load DS_* settings from an "
                "env file before reading the process environment."
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
                "Global option; place before COMMAND. Render the standard envelope "
                "as json, or render row/object views as table/tsv."
            ),
        ),
    ] = "json",
    columns: Annotated[
        str | None,
        typer.Option(
            "--columns",
            help=(
                "Global option; place before COMMAND. Comma-separated row/object "
                "fields to render or project. In json mode this narrows the "
                "standard envelope data payload."
            ),
        ),
    ] = None,
    *,
    compact: Annotated[
        bool,
        typer.Option(
            "--compact",
            help=(
                "Global option; place before COMMAND. Emit JSON without indentation; "
                "valid only with --output-format json."
            ),
        ),
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
    normalized = value.lower()
    if normalized == "json":
        return "json"
    if normalized == "table":
        return "table"
    if normalized == "tsv":
        return "tsv"
    message = f"Unsupported output format: {value}"
    raise typer.BadParameter(message)


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
