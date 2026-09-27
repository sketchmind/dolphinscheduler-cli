from __future__ import annotations

import json
import sys
from collections.abc import Sequence  # noqa: TC003 - method override annotation only.
from pathlib import (
    Path,  # noqa: TC003 - Typer resolves callback annotations at runtime.
)
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

import typer
from typer import _click
from typer.completion import (
    get_completion_inspect_parameters,
    install_callback,
    show_callback,
)
from typer.core import TyperCommand, TyperGroup, TyperOption
from typing_extensions import override

from dsctl import __version__
from dsctl.action_policy import preflight_selected_action
from dsctl.cli_runtime import AppState, set_app_state
from dsctl.command_contract import COMMAND_CATALOG, CommandBindingError
from dsctl.commands._contract_adapter import (
    SemanticMetavarGroup,
    apply_semantic_metavars,
    bind_global_options,
    configure_semantic_help,
)
from dsctl.commands.registry import register_all_commands
from dsctl.errors import UserInputError
from dsctl.output import error_payload
from dsctl.output_formats import (
    OUTPUT_FORMAT_CHOICES,
    OutputFormat,
    RenderOptions,
    parse_columns,
)
from dsctl.services.version_resolution import (
    annotate_target_error,
    annotate_target_result,
    invocation_scope,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject

_ROOT_OPTION_ARITY = {
    option.flag: option.arity for option in COMMAND_CATALOG.global_options
}
_ROOT_HELP = "Manage Apache DolphinScheduler through its REST API."
_ROOT_EPILOG = (
    "Examples:\n"
    "  dsctl doctor\n"
    "  dsctl workflow list --project etl-prod\n"
    "  dsctl workflow-instance watch 901 --project etl-prod\n\n"
    "Global options may appear before or after the command path. "
    "--context and --env-file select mutually exclusive sources.\n\n"
    "JSON preserves types and metadata; table/tsv show data fields. "
    "--columns selects fields. json-compact encodes declared lists as columns/rows "
    "with top-level column selection; other results retain their JSON structure. "
    "Raw artifact output is unchanged by display options."
)
CompletionShell = Literal["bash", "zsh", "fish", "powershell", "pwsh"]

# Register Typer's shell protocol handlers without probing the parent process.
get_completion_inspect_parameters()


def _version_callback(value: bool) -> bool:  # noqa: FBT001
    if value:
        typer.echo(__version__)
        raise typer.Exit()
    return value


class _GlobalOptionGroup(SemanticMetavarGroup):
    """Let root rendering/config options appear anywhere before `--`."""

    def __init__(self, **attrs: Any) -> None:  # noqa: ANN401
        """Apply one metavar policy after Typer materializes the full tree."""
        super().__init__(**attrs)
        apply_semantic_metavars(self)

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:  # noqa: ANN401
        """Move catalogued root options ahead of the command before Click parses."""
        return super().parse_args(
            ctx,
            _normalize_root_options(self, args),
        )

    @override
    def main(
        self,
        args: Sequence[str] | None = None,
        prog_name: str | None = None,
        complete_var: str | None = None,
        standalone_mode: bool = True,
        windows_expand_args: bool = True,
        **extra: Any,
    ) -> Any:
        """Render parser failures through the selected public error format."""
        invocation_args = list(sys.argv[1:] if args is None else args)
        try:
            result = super().main(
                args=invocation_args,
                prog_name=prog_name,
                complete_var=complete_var,
                standalone_mode=False,
                windows_expand_args=windows_expand_args,
                **extra,
            )
        except _click.exceptions.NoArgsIsHelpError as exc:
            if message := exc.format_message():
                typer.echo(message, err=True)
            return _usage_exit(exc.exit_code, standalone_mode=standalone_mode)
        except _click.exceptions.UsageError as exc:
            _render_usage_error(exc, invocation_args)
            return _usage_exit(exc.exit_code, standalone_mode=standalone_mode)
        except _click.exceptions.Exit as exc:
            return _usage_exit(exc.exit_code, standalone_mode=standalone_mode)
        except _click.exceptions.Abort:
            if not standalone_mode:
                raise
            typer.echo("Aborted!", err=True)
            raise SystemExit(1) from None
        else:
            if standalone_mode and isinstance(result, int):
                raise SystemExit(result)
            return result


app = typer.Typer(
    add_completion=False,
    cls=_GlobalOptionGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
    help=_ROOT_HELP,
    epilog=_ROOT_EPILOG,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)


@app.callback()
@bind_global_options
def main_callback(
    ctx: typer.Context,
    context_name: str | None,
    env_file: Path | None,
    output_format: str,
    columns: str | None,
    *,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            callback=_version_callback,
            is_eager=True,
            help="Print the installed dsctl version as plain text and exit.",
            rich_help_panel="Getting started",
        ),
    ] = False,
    show_completion: Annotated[
        CompletionShell | None,
        typer.Option(
            "--show-completion",
            callback=show_callback,
            is_eager=True,
            help="Show completion for SHELL and exit.",
            metavar="SHELL",
            rich_help_panel="Getting started",
        ),
    ] = None,
    install_completion: Annotated[
        CompletionShell | None,
        typer.Option(
            "--install-completion",
            callback=install_callback,
            is_eager=True,
            help="Install completion for SHELL and exit.",
            metavar="SHELL",
            rich_help_panel="Getting started",
        ),
    ] = None,
) -> None:
    """Initialize shared command state."""
    del version, show_completion, install_completion
    if context_name is not None and env_file is not None:
        message = "--context and --env-file are mutually exclusive"
        raise typer.BadParameter(message)
    format_choice = _parse_output_format(output_format)
    try:
        parsed_columns = parse_columns(columns)
    except UserInputError as exc:
        raise typer.BadParameter(exc.message) from exc
    state = AppState(
        env_file=env_file,
        action_preflight=preflight_selected_action,
        invocation_scope=lambda: invocation_scope(
            context_name=context_name, env_file=env_file
        ),
        result_postprocess=annotate_target_result,
        error_postprocess=annotate_target_error,
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
configure_semantic_help(app)


def _render_usage_error(exc: _click.exceptions.UsageError, args: list[str]) -> None:
    message = exc.format_message()
    command_path = exc.ctx.command_path if exc.ctx is not None else "dsctl"
    suggestion = f"Run `{command_path} --help` to review the accepted syntax."
    output_format = _usage_error_format(args)
    if output_format in {"json", "json-compact"}:
        details: JsonObject = {}
        if exc.ctx is not None:
            details["usage"] = exc.ctx.get_usage().removeprefix("Usage: ")
        if isinstance(exc, _click.exceptions.MissingParameter):
            parameter = exc.param
            missing: JsonObject = {
                "kind": exc.param_type
                or ("parameter" if parameter is None else parameter.param_type_name)
            }
            if parameter is not None and parameter.name is not None:
                missing["parameter"] = parameter.name
            if isinstance(parameter, TyperOption) and parameter.opts:
                missing["flag"] = parameter.opts[0]
            details["missing"] = missing
        payload = error_payload(
            _usage_action(exc.ctx),
            UserInputError(
                message,
                details=details,
                suggestion=suggestion,
            ),
        )
        compact = output_format == "json-compact"
        typer.echo(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=None if compact else 2,
                sort_keys=True,
                separators=(",", ":") if compact else None,
            ),
            err=True,
        )
        return
    typer.echo(f"Error: {message}", err=True)
    typer.echo(f"Hint: {suggestion}", err=True)


def _usage_error_format(args: list[str]) -> OutputFormat:
    selected: OutputFormat = "json"
    index = 0
    while index < len(args):
        argument = args[index]
        if argument == "--":
            break
        value: str | None = None
        if argument == "--format":
            if index + 1 >= len(args):
                return "json"
            value = args[index + 1]
            index += 1
        elif argument.startswith("--format="):
            value = argument.partition("=")[2]
        if value is not None:
            normalized = value.lower()
            if normalized not in OUTPUT_FORMAT_CHOICES:
                return "json"
            selected = normalized
        index += 1
    return selected


def _usage_action(ctx: _click.Context | None) -> str:
    if ctx is None:
        return "cli"
    route: list[str] = []
    current = ctx
    while current.parent is not None:
        if current.info_name is not None:
            route.append(current.info_name)
        current = current.parent
    command_route = tuple(reversed(route))
    contract = next(
        (item for item in COMMAND_CATALOG.commands if item.route == command_route),
        None,
    )
    return "cli" if contract is None else contract.action


def _usage_exit(exit_code: int, *, standalone_mode: bool) -> int:
    if standalone_mode:
        raise SystemExit(exit_code)
    raise _click.exceptions.Exit(exit_code)


def _parse_output_format(value: str) -> OutputFormat:
    """Parse one Typer string option into the stable format literal."""
    try:
        normalized = COMMAND_CATALOG.validate_global_values({"format": value})["format"]
    except CommandBindingError as exc:
        message = (
            f"Unsupported output format: {value}. "
            f"Choose one of: {', '.join(OUTPUT_FORMAT_CHOICES)}."
        )
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
