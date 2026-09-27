from __future__ import annotations

import inspect
from contextlib import ExitStack, contextmanager
from copy import copy
from pathlib import Path
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Literal,
    ParamSpec,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import typer
from typer.core import TyperArgument, TyperCommand, TyperGroup, TyperOption

from dsctl.command_contract import (
    COMMAND_CATALOG,
    CommandContract,
    CommandContractError,
    InputContract,
    MissingDefault,
    PathRules,
    semantic_value_name,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from typer.models import ArgumentInfo, OptionInfo


@contextmanager
def _semantic_argument_help_types(
    parameters: Sequence[object],
) -> Iterator[None]:
    """Hide Typer's redundant generic type column while Rich renders help."""
    with ExitStack() as restore:
        for parameter in parameters:
            if (
                not isinstance(parameter, TyperArgument)
                or parameter.metavar is None
                or parameter.type.name == "choice"
            ):
                continue
            restore.callback(setattr, parameter, "type", parameter.type)
            display_type = copy(parameter.type)
            # Typer's Rich table suppresses BOOLEAN in its separate type column.
            # The copy exists only during help rendering, so parsing and errors keep
            # the original Click type name and conversion behavior.
            display_type.name = "boolean"
            parameter.type = display_type
        yield


class SemanticMetavarCommand(TyperCommand):
    """Render semantic arguments and inherited options without changing parsing."""

    def format_help(
        self,
        ctx: object,
        formatter: object,
    ) -> None:
        """Render semantic argument names without a second TEXT/INTEGER column."""
        display = copy(self)
        context = cast("typer.Context", ctx)
        contract = _help_contract(context)
        display.params = [*self.params, *_inherited_options(context, contract)]
        with _semantic_argument_help_types(display.params):
            render = cast(
                "Callable[[object, object], None]",
                super(SemanticMetavarCommand, display).format_help,
            )
            render(ctx, formatter)


def _help_contract(ctx: typer.Context) -> CommandContract | None:
    """Resolve the registered route without depending on the executable name."""
    route: list[str] = []
    while ctx.parent is not None:
        if ctx.info_name is not None:
            route.append(ctx.info_name)
        ctx = cast("typer.Context", ctx.parent)
    command_route = tuple(reversed(route))
    return next(
        (item for item in COMMAND_CATALOG.commands if item.route == command_route),
        None,
    )


def _inherited_options(
    ctx: typer.Context, contract: CommandContract | None
) -> list[TyperOption]:
    """Copy real root options into a help-only panel, retaining parser metadata."""
    flags = {option.flag for option in COMMAND_CATALOG.global_options}
    if contract is not None and contract.raw_output_format is not None:
        flags -= {"--format", "--columns"}
    inherited = []
    for parameter in ctx.find_root().command.params:
        if isinstance(parameter, TyperOption) and flags.intersection(parameter.opts):
            display = copy(parameter)
            display.rich_help_panel = "Global options"
            inherited.append(display)
    return inherited


class SemanticMetavarGroup(TyperGroup):
    """Typer group whose Rich help follows the semantic metavar policy."""

    def format_help(
        self,
        ctx: object,
        formatter: object,
    ) -> None:
        """Render semantic argument names without a generic type duplicate."""
        with _semantic_argument_help_types(self.params):
            render = cast(
                "Callable[[object, object], None]",
                super().format_help,
            )
            render(ctx, formatter)


P = ParamSpec("P")
R = TypeVar("R")


def bind_command(action: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Bind invocation metadata to an ordinary typed callback without wrapping it.

    The catalog owns Typer annotations and omission defaults. The callback keeps
    Python types and executable service orchestration; disagreement fails at import.
    """
    contract = COMMAND_CATALOG.command(action)
    return _bind_inputs(
        action, (*contract.arguments, *contract.options), summary=contract.summary
    )


def bind_global_options(callback: Callable[P, R]) -> Callable[P, R]:
    """Bind catalog options beside the three eager framework root options."""
    return _bind_inputs(
        "global",
        tuple(option.input for option in COMMAND_CATALOG.global_options),
        passthrough_names=("version", "show_completion", "install_completion"),
    )(callback)


def _bind_inputs(
    action: str,
    contracts: tuple[InputContract, ...],
    *,
    summary: str | None = None,
    passthrough_names: tuple[str, ...] = (),
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def bind(callback: Callable[P, R]) -> Callable[P, R]:
        signature = inspect.signature(callback)
        hints = get_type_hints(callback, include_extras=True)
        inputs = {
            item.parameter_name or item.name.replace("-", "_"): item
            for item in contracts
        }
        names = tuple(
            name
            for name in signature.parameters
            if hints.get(name) is not typer.Context
        )
        expected_names = (*tuple(inputs), *passthrough_names)
        if len(inputs) != len(contracts) or names != expected_names:
            message = (
                f"{action} callback inputs differ from its command contract: "
                f"expected {expected_names!r}, got {names!r}"
            )
            raise CommandContractError(message)
        parameters = []
        annotations = dict(hints)
        for parameter in signature.parameters.values():
            item = inputs.get(parameter.name)
            if item is None:
                parameters.append(
                    parameter.replace(
                        annotation=hints.get(parameter.name, parameter.annotation)
                    )
                )
                continue
            if parameter.default is not inspect.Parameter.empty:
                message = f"{action}.{parameter.name} defaults belong in the catalog"
                raise CommandContractError(message)
            annotation = hints[parameter.name]
            _validate_callback_type(action, item, annotation)
            info = typer_option(item) if item.kind == "option" else typer_argument(item)
            annotations[parameter.name] = Annotated[annotation, info]
            default = (
                inspect.Parameter.empty
                if isinstance(item.parse_default, MissingDefault)
                else item.parse_default
            )
            parameters.append(
                parameter.replace(
                    annotation=annotations[parameter.name], default=default
                )
            )
        projected = signature.replace(parameters=parameters)
        setattr(callback, "__signature__", projected)  # noqa: B010 - Callable has no declared signature slot.
        callback.__annotations__ = annotations
        callback.__doc__ = summary or callback.__doc__
        positional = [
            p
            for p in parameters
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        defaults = tuple(
            p.default for p in positional if p.default is not inspect.Parameter.empty
        )
        setattr(callback, "__defaults__", defaults or None)  # noqa: B010 - Preserve Python defaults.
        setattr(  # noqa: B010 - Preserve Python keyword defaults.
            callback,
            "__kwdefaults__",
            {
                p.name: p.default
                for p in parameters
                if p.kind == p.KEYWORD_ONLY and p.default is not inspect.Parameter.empty
            }
            or None,
        )
        return callback

    return bind


def _validate_callback_type(
    action: str, contract: InputContract, annotation: object
) -> None:
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        members = [
            member for member in get_args(annotation) if member is not type(None)
        ]
        if len(members) != 1:
            msg = f"{action}.{contract.name} has an ambiguous callback type"
            raise CommandContractError(msg)
        annotation = members[0]
        origin = get_origin(annotation)
    if origin is list:
        if not contract.multiple:
            msg = f"{action}.{contract.name} callback repeats a scalar input"
            raise CommandContractError(msg)
        annotation = get_args(annotation)[0]
        origin = get_origin(annotation)
    elif contract.multiple:
        msg = f"{action}.{contract.name} callback must accept a list"
        raise CommandContractError(msg)
    if origin is Literal:
        choices = get_args(annotation)
        if choices != contract.parser_choices:
            msg = f"{action}.{contract.name} callback choices differ from its contract"
            raise CommandContractError(msg)
        annotation = type(choices[0])
    elif contract.parser_choices:
        msg = f"{action}.{contract.name} callback omits parser choices"
        raise CommandContractError(msg)
    expected = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "path": Path,
    }[contract.value_type]
    expected = str if contract.path_as_string else expected
    if annotation is not expected:
        msg = f"{action}.{contract.name} callback type differs from its contract"
        raise CommandContractError(msg)


def typer_argument(contract: InputContract) -> ArgumentInfo:
    """Project positional metadata without changing the Python value type."""
    if contract.kind != "argument":
        msg = f"{contract.name!r} is not an argument"
        raise CommandContractError(msg)
    rules = contract.path_rules or PathRules()
    return cast(
        "ArgumentInfo",
        typer.Argument(
            help=contract.description,
            metavar=contract.value_name,
            show_choices=contract.show_choices,
            hidden=contract.hidden,
            min=(
                contract.minimum
                if isinstance(contract.parser_minimum, MissingDefault)
                else contract.parser_minimum
            ),
            max=contract.maximum,
            exists=rules.exists,
            file_okay=rules.file_okay,
            dir_okay=rules.dir_okay,
            readable=rules.readable,
            resolve_path=rules.resolve_path,
        ),
    )


def typer_option(contract: InputContract) -> OptionInfo:
    """Project canonical syntax, validation, and help into Typer."""
    flag = contract.flag
    if flag is None:
        msg = f"{contract.name!r} is not an option"
        raise CommandContractError(msg)
    rules = contract.path_rules or PathRules()
    return cast(
        "OptionInfo",
        typer.Option(
            *(contract.flags or (flag,)),
            help=contract.description,
            metavar=contract.value_name,
            show_choices=contract.show_choices,
            hidden=contract.hidden,
            min=(
                contract.minimum
                if isinstance(contract.parser_minimum, MissingDefault)
                else contract.parser_minimum
            ),
            max=contract.maximum,
            exists=rules.exists,
            file_okay=rules.file_okay,
            dir_okay=rules.dir_okay,
            readable=rules.readable,
            resolve_path=rules.resolve_path,
        ),
    )


def configure_semantic_help(app: typer.Typer, *, route: tuple[str, ...] = ()) -> None:
    """Install semantic Rich-help classes across one registered Typer tree."""
    contracts = {item.route: item for item in COMMAND_CATALOG.commands}
    for command_info in app.registered_commands:
        if command_info.cls is TyperCommand:
            command_info.cls = SemanticMetavarCommand
        name = command_info.name
        if name is None and command_info.callback is not None:
            name = command_info.callback.__name__.replace("_", "-")
        contract = contracts.get((*route, name)) if name is not None else None
        if contract is not None:
            command_info.epilog = _command_help_epilog(contract)
    for group_info in app.registered_groups:
        if not isinstance(group_info.cls, type) or group_info.cls is TyperGroup:
            group_info.cls = SemanticMetavarGroup
        child_app = group_info.typer_instance
        if child_app is not None:
            configure_semantic_help(child_app, route=(*route, group_info.name or ""))


def _command_help_epilog(contract: CommandContract) -> str:
    """Keep only output rules and discovery hints relevant to this action."""
    hints = []
    options = {option.name for option in contract.options}
    if contract.raw_output_format is not None:
        hints.append(
            f"Successful output is raw {contract.raw_output_format.upper()}; "
            "display options do not alter it."
        )
    elif "raw" in options:
        hints.append("With --raw, display options do not alter the artifact output.")
    if "all" in options:
        hints.append("Lists read one page by default; --all reads all pages.")
    if "dry-run" in options:
        hints.append(
            "Dry-run shows prepared effects; add --columns requests for "
            "the ordered REST audit view. Apply prepares against current state."
        )
    schema_command = COMMAND_CATALOG.render(
        "schema", global_values={}, values={"command": contract.action}
    )
    hints.append(f"Schema: {schema_command}")
    return "\n\n".join(hints)


def apply_semantic_metavars(command: TyperCommand | TyperGroup) -> None:
    """Give every free-form CLI value a semantic metavar in one tree walk."""
    for parameter in command.params:
        if parameter.metavar is not None or parameter.type.name == "choice":
            continue
        if isinstance(parameter, TyperOption):
            if parameter.is_flag or parameter.count:
                continue
            source_name = next(
                (option[2:] for option in parameter.opts if option.startswith("--")),
                parameter.name,
            )
        elif isinstance(parameter, TyperArgument):
            source_name = parameter.name
        else:
            continue
        if source_name is not None:
            parameter.metavar = semantic_value_name(source_name)

    if isinstance(command, TyperGroup):
        for child in command.commands.values():
            apply_semantic_metavars(cast("TyperCommand | TyperGroup", child))


__all__ = [
    "SemanticMetavarGroup",
    "apply_semantic_metavars",
    "configure_semantic_help",
    "typer_option",
]
