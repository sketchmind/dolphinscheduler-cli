from __future__ import annotations

from pathlib import (
    Path,  # noqa: TC003 - Typer resolves callback annotations at runtime.
)
from typing import TYPE_CHECKING, Annotated, cast

import typer
from typer.core import TyperCommand, TyperGroup, TyperOption

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

if TYPE_CHECKING:
    from click import Context as ClickContext

_ROOT_OPTION_ARITY = {
    option.flag: option.arity for option in COMMAND_CATALOG.global_options
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


class _GlobalOptionGroup(TyperGroup):
    """Let root rendering/config options appear anywhere before `--`."""

    def parse_args(self, ctx: ClickContext, args: list[str]) -> list[str]:
        """Move catalogued root options ahead of the command before Click parses."""
        return super().parse_args(ctx, _normalize_root_options(self, args))


app = typer.Typer(
    add_completion=False,
    cls=_GlobalOptionGroup,
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


def _normalize_root_options(
    root: TyperGroup,
    args: list[str],
) -> list[str]:
    """Move unambiguous root options while preserving command option values."""
    root_args: list[str] = []
    command_args: list[str] = []
    current_command: TyperCommand | TyperGroup = root
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--":
            command_args.extend(args[index:])
            break

        local_arity = _command_option_arity(current_command, arg)
        root_option = _root_option_name(arg)
        if local_arity is not None and (
            current_command is not root or root_option is None
        ):
            option_end = index + (1 if "=" in arg else 1 + local_arity)
            command_args.extend(args[index:option_end])
            index = option_end
            continue

        if root_option is not None:
            arity = _ROOT_OPTION_ARITY[root_option]
            option_end = index + (1 if "=" in arg else 1 + arity)
            root_args.extend(args[index:option_end])
            index = option_end
            continue

        command_args.append(arg)
        if isinstance(current_command, TyperGroup) and not arg.startswith("-"):
            child = current_command.commands.get(arg)
            if child is None:
                return args
            current_command = cast("TyperCommand | TyperGroup", child)
        index += 1
    return [*root_args, *command_args]


def _root_option_name(token: str) -> str | None:
    for option in _ROOT_OPTION_ARITY:
        if token == option or token.startswith(f"{option}="):
            return option
    return None


def _command_option_arity(
    command: TyperCommand | TyperGroup,
    token: str,
) -> int | None:
    """Return how many following tokens one command-local option consumes."""
    option_name = token.split("=", 1)[0]
    for parameter in command.params:
        if not isinstance(parameter, TyperOption):
            continue
        option_names = (*parameter.opts, *parameter.secondary_opts)
        if option_name not in option_names:
            continue
        if parameter.is_flag or parameter.count:
            return 0
        return 0 if "=" in token else parameter.nargs
    return None
