from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias

from dsctl.cli_surface import stable_leaf_actions

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

InputKind = Literal["argument", "option"]
ValueType = Literal["boolean", "integer", "number", "path", "string"]
Normalization = Literal["identity", "lowercase"]
RemoteEffect = Literal["none", "read", "write"]
LocalEffect = Literal["none", "configuration", "file"]
CliScalar: TypeAlias = str | int | float | bool
CliBinding: TypeAlias = CliScalar | tuple[CliScalar, ...] | None


def semantic_value_name(name: str) -> str:
    """Return the stable, shell-safe metavar for one CLI input name."""
    return name.replace("-", "_").upper()


@dataclass(frozen=True, slots=True)
class MissingDefault:
    """Marker distinguishing an absent default from an explicit null value."""


MISSING_DEFAULT = MissingDefault()
DefaultValue: TypeAlias = CliScalar | None | MissingDefault


@dataclass(frozen=True, slots=True)
class PathRules:
    """Parser rules that must survive a command-contract projection."""

    exists: bool = False
    file_okay: bool = True
    dir_okay: bool = True
    readable: bool = True
    resolve_path: bool = False


@dataclass(frozen=True, slots=True)
class ValueResolution:
    """Ordered value sources used when an option is omitted."""

    precedence: tuple[str, ...]
    fallback: CliScalar | None

    def __post_init__(self) -> None:
        """Reject ambiguous source order before it reaches an adapter."""
        if not self.precedence or self.precedence[-1] != "default":
            message = "value resolution must end with the default source"
            raise CommandContractError(message)
        if len(set(self.precedence)) != len(self.precedence):
            message = "value resolution precedence must not repeat a source"
            raise CommandContractError(message)


@dataclass(frozen=True, slots=True)
class InputContract:
    """One canonical positional argument or command-local option."""

    name: str
    kind: InputKind
    value_type: ValueType
    description: str
    required: bool = False
    parse_default: DefaultValue = MISSING_DEFAULT
    fixed_default: DefaultValue = MISSING_DEFAULT
    choices: tuple[CliScalar, ...] = ()
    multiple: bool = False
    selector: str | None = None
    discovery_command: str | None = None
    discovery_command_pattern: str | None = None
    value_name: str | None = None
    schema_value_name: str | None = None
    path_rules: PathRules | None = None
    resolution: ValueResolution | None = None
    normalization: Normalization = "identity"
    binding_name: str | None = None
    parameter_name: str | None = None
    path_as_string: bool = False
    flags: tuple[str, ...] = ()
    minimum: int | float | None = None
    maximum: int | float | None = None
    parser_minimum: int | float | None | MissingDefault = MISSING_DEFAULT
    legacy_default: bool = False
    hidden: bool = False
    show_choices: bool = True
    examples: tuple[str, ...] = ()
    supported_keys: tuple[str, ...] = ()
    parser_choices: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Keep fixed and multi-source omission semantics unambiguous."""
        if (
            isinstance(self.fixed_default, MissingDefault)
            and self.resolution is None
            and self.parse_default is not None
            and not isinstance(self.parse_default, MissingDefault)
        ):
            object.__setattr__(self, "fixed_default", self.parse_default)
        if not self.choices and self.parser_choices:
            object.__setattr__(self, "choices", self.parser_choices)
        if (
            self.discovery_command is not None
            and self.discovery_command_pattern is not None
        ):
            message = "discovery command and pattern are mutually exclusive"
            raise CommandContractError(message)
        if self.resolution is not None and not isinstance(
            self.fixed_default,
            MissingDefault,
        ):
            message = (
                f"{self.name!r} fixed default cannot coexist with value resolution"
            )
            raise CommandContractError(message)

    @property
    def key(self) -> str:
        """Return the unambiguous invocation binding name."""
        return self.binding_name or self.name

    @property
    def flag(self) -> str | None:
        """Return the long option flag, or none for positional arguments."""
        if self.kind == "argument":
            return None
        return self.flags[0].split("/")[0] if self.flags else f"--{self.name}"

    def normalize(self, value: CliScalar) -> CliScalar:
        """Apply only the explicitly declared parser normalization."""
        if self.normalization == "lowercase" and isinstance(value, str):
            return value.lower()
        return value


@dataclass(frozen=True, slots=True)
class GlobalOptionContract:
    """One root option shared by parsing, schema, and command rendering."""

    input: InputContract
    arity: int
    example: str
    schema_order: int
    invocation_order: int
    requirement: tuple[str, str] | None = None

    @property
    def name(self) -> str:
        """Return the canonical field name."""
        return self.input.name

    @property
    def flag(self) -> str:
        """Return the canonical long flag."""
        flag = self.input.flag
        if flag is None:
            message = "global options must expose a flag"
            raise CommandContractError(message)
        return flag


@dataclass(frozen=True, slots=True)
class CommandDryRunEffects:
    """Network and local effects when a command's compile dry-run is selected."""

    remote: RemoteEffect
    local: LocalEffect


@dataclass(frozen=True, slots=True)
class CommandEffects:
    """Reviewed effects for a normal invocation and its optional compile dry-run."""

    remote: RemoteEffect
    local: LocalEffect
    dry_run: CommandDryRunEffects | None = None


NO_EFFECTS = CommandEffects(remote="none", local="none")
REMOTE_READ = CommandEffects(remote="read", local="none")
REMOTE_WRITE = CommandEffects(remote="write", local="none")
LOCAL_CONFIGURATION_WRITE = CommandEffects(remote="none", local="configuration")
REMOTE_READ_LOCAL_FILE_WRITE = CommandEffects(remote="read", local="file")
REMOTE_WRITE_DRY_RUN_READ = CommandEffects(
    remote="write",
    local="none",
    dry_run=CommandDryRunEffects(remote="read", local="none"),
)


@dataclass(frozen=True, slots=True)
class CommandContract:
    """One canonical command action and its ordered invocation facts."""

    action: str
    route: tuple[str, ...]
    summary: str
    effects: CommandEffects
    arguments: tuple[InputContract, ...] = ()
    options: tuple[InputContract, ...] = ()
    activation_flags: tuple[str, ...] = ()
    # Unconditional success artifact; optional --raw remains an input contract.
    raw_output_format: Literal["yaml"] | None = None

    @property
    def name(self) -> str:
        """Return the final command-path segment."""
        return self.route[-1]

    @property
    def command_path(self) -> str:
        """Return the shell-safe executable path without input bindings."""
        return shlex.join(("dsctl", *self.route))

    @property
    def invocation_prefix(self) -> str:
        """Return the route plus flags selecting a group-level action."""
        return shlex.join(
            ("dsctl", *self.route, *(f"--{name}" for name in self.activation_flags))
        )

    def input(self, name: str) -> InputContract:
        """Return one declared input or fail instead of guessing a field."""
        for item in (*self.arguments, *self.options):
            if item.key == name:
                return item
        message = f"{self.action} has no input named {name!r}"
        raise KeyError(message)


class CommandContractError(ValueError):
    """Raised when the canonical command catalog is internally inconsistent."""


class CommandBindingError(ValueError):
    """Raised when an invocation cannot be rendered without guessing."""


@dataclass(frozen=True, slots=True)
class CommandCatalog:
    """Deep module for command facts and safe, deterministic invocations."""

    global_options: tuple[GlobalOptionContract, ...]
    commands: tuple[CommandContract, ...]

    def __post_init__(self) -> None:
        """Validate catalog identity and field-kind invariants once."""
        _require_unique(
            (item.name for item in self.global_options),
            label="global option name",
        )
        _require_unique((item.action for item in self.commands), label="action")
        _require_unique((item.route for item in self.commands), label="command route")
        stable_actions = stable_leaf_actions()
        for command in self.commands:
            if not command.route:
                message = f"{command.action} must declare an explicit command route"
                raise CommandContractError(message)
            if command.action not in stable_actions:
                message = f"{command.action} is not part of the stable CLI surface"
                raise CommandContractError(message)
            has_compile_dry_run = any(
                item.name == "dry-run" for item in command.options
            )
            if (command.effects.dry_run is not None) is not has_compile_dry_run:
                message = (
                    f"{command.action} compile dry-run effects must exactly match "
                    "a declared --dry-run option"
                )
                raise CommandContractError(message)
            _require_unique(
                (item.key for item in (*command.arguments, *command.options)),
                label=f"{command.action} input name",
            )
            for name in command.activation_flags:
                item = command.input(name)
                if item.kind != "option" or item.value_type != "boolean":
                    message = (
                        f"{command.action} activation flags must be boolean options"
                    )
                    raise CommandContractError(message)
            if any(item.kind != "argument" for item in command.arguments):
                message = f"{command.action} arguments must use kind='argument'"
                raise CommandContractError(message)
            if any(item.kind != "option" for item in command.options):
                message = f"{command.action} options must use kind='option'"
                raise CommandContractError(message)

    def command(self, action: str) -> CommandContract:
        """Return one action-local contract without deriving paths from names."""
        for command in self.commands:
            if command.action == action:
                return command
        message = f"unknown canonical command action: {action}"
        raise KeyError(message)

    def global_option(self, name: str) -> GlobalOptionContract:
        """Return one canonical root option by its stable field name."""
        for option in self.global_options:
            if option.name == name:
                return option
        message = f"unknown canonical global option: {name}"
        raise KeyError(message)

    def render(
        self,
        action: str,
        *,
        global_values: Mapping[str, CliBinding],
        values: Mapping[str, CliBinding],
    ) -> str:
        """Render one shell-safe command from validated, opaque bindings."""
        return shlex.join(
            self._argv(
                action,
                global_values=global_values,
                values=values,
            )
        )

    def validate_global_values(
        self,
        values: Mapping[str, CliBinding],
    ) -> dict[str, CliBinding]:
        """Normalize and validate root options through the canonical facts."""
        global_by_name = {item.name: item for item in self.global_options}
        _reject_unknown_bindings(
            values,
            allowed=frozenset(global_by_name),
            label="global option",
        )
        normalized = {
            name: _normalize_binding(global_by_name[name].input, value)
            for name, value in values.items()
        }
        _validate_global_requirements(
            self.global_options,
            global_values=normalized,
            global_by_name=global_by_name,
        )
        return normalized

    def _argv(
        self,
        action: str,
        *,
        global_values: Mapping[str, CliBinding],
        values: Mapping[str, CliBinding],
    ) -> list[str]:
        command = self.command(action)
        normalized_globals = self.validate_global_values(global_values)
        local_by_name = {
            item.key: item for item in (*command.arguments, *command.options)
        }
        _reject_unknown_bindings(
            values,
            allowed=frozenset(local_by_name),
            label=f"{action} input",
        )

        values = dict(values)
        for name in command.activation_flags:
            if name in values and values[name] is not True:
                message = f"{action} requires --{name}"
                raise CommandBindingError(message)
            values[name] = True
        argv = ["dsctl"]
        for option in sorted(
            self.global_options,
            key=lambda item: item.invocation_order,
        ):
            _append_binding(argv, option.input, normalized_globals.get(option.name))
        argv.extend(command.route)
        argument_argv: list[str] = []
        option_argv: list[str] = []
        for item in command.arguments:
            _append_declared_binding(argument_argv, action, item, values)
        for item in command.options:
            _append_declared_binding(option_argv, action, item, values)
        if any(token.startswith("-") for token in argument_argv):
            argv.extend(option_argv)
            argv.append("--")
            argv.extend(argument_argv)
        else:
            argv.extend(argument_argv)
            argv.extend(option_argv)
        return argv


def _append_declared_binding(
    argv: list[str],
    action: str,
    contract: InputContract,
    values: Mapping[str, CliBinding],
) -> None:
    value = values.get(contract.key)
    if contract.required and (contract.key not in values or value is None):
        message = f"{action} requires a value for {contract.name!r}"
        raise CommandBindingError(message)
    if contract.required and value in ("", ()):
        message = f"{action} requires a non-empty value for {contract.name!r}"
        raise CommandBindingError(message)
    _append_binding(argv, contract, value)


def _append_binding(
    argv: list[str],
    contract: InputContract,
    value: CliBinding,
) -> None:
    normalized = _normalize_binding(contract, value)
    if normalized is None:
        return
    if isinstance(normalized, tuple):
        for item in normalized:
            _append_scalar_binding(argv, contract, item)
        return
    _append_scalar_binding(argv, contract, normalized)


def _normalize_binding(
    contract: InputContract,
    value: CliBinding,
) -> CliBinding:
    if value is None:
        return None
    if isinstance(value, tuple):
        if not contract.multiple:
            label = contract.flag or contract.name
            message = f"{label} does not accept repeated bindings"
            raise CommandBindingError(message)
        return tuple(_normalize_scalar(contract, item) for item in value)
    return _normalize_scalar(contract, value)


def _normalize_scalar(
    contract: InputContract,
    value: CliScalar,
) -> CliScalar:
    normalized_value = contract.normalize(value)
    if contract.value_type == "boolean":
        if not isinstance(normalized_value, bool):
            message = f"{contract.name!r} expects a boolean binding"
            raise CommandBindingError(message)
        return normalized_value
    label = contract.flag or contract.name
    if contract.value_type == "integer":
        if isinstance(normalized_value, bool) or not isinstance(normalized_value, int):
            message = f"{label} expects an integer binding"
            raise CommandBindingError(message)
    elif contract.value_type == "number":
        if isinstance(normalized_value, bool) or not isinstance(
            normalized_value, (int, float)
        ):
            message = f"{label} expects a number binding"
            raise CommandBindingError(message)
    elif not isinstance(normalized_value, str):
        value_label = "string path" if contract.value_type == "path" else "string"
        message = f"{label} expects a {value_label} binding"
        raise CommandBindingError(message)
    if isinstance(normalized_value, str) and "\0" in normalized_value:
        message = f"{label} cannot contain a NUL character"
        raise CommandBindingError(message)
    rendered_value = str(normalized_value)
    if contract.choices and rendered_value not in tuple(
        str(choice) for choice in contract.choices
    ):
        label = contract.flag or contract.name
        choices = ", ".join(str(choice) for choice in contract.choices)
        message = f"{label} expects one of: {choices}"
        raise CommandBindingError(message)
    return normalized_value


def _append_scalar_binding(
    argv: list[str],
    contract: InputContract,
    value: CliScalar,
) -> None:
    if contract.value_type == "boolean":
        if value is True:
            flag = contract.flag
            if flag is None:
                message = f"boolean argument {contract.name!r} cannot be rendered"
                raise CommandBindingError(message)
            argv.append(flag)
        return
    flag = contract.flag
    if flag is not None:
        argv.append(flag)
    argv.append(str(value))


def _reject_unknown_bindings(
    values: Mapping[str, CliBinding],
    *,
    allowed: frozenset[str],
    label: str,
) -> None:
    unknown = sorted(set(values) - allowed)
    if unknown:
        message = f"unknown {label} bindings: {', '.join(unknown)}"
        raise CommandBindingError(message)


def _validate_global_requirements(
    contracts: tuple[GlobalOptionContract, ...],
    *,
    global_values: Mapping[str, CliBinding],
    global_by_name: Mapping[str, GlobalOptionContract],
) -> None:
    for contract in contracts:
        requirement = contract.requirement
        value = global_values.get(contract.name)
        if requirement is None or value in (None, False):
            continue
        required_name, expected_value = requirement
        required = global_by_name.get(required_name)
        if required is None:
            message = (
                f"{contract.flag} requires unknown global option --{required_name}"
            )
            raise CommandContractError(message)
        actual_value = global_values.get(required_name)
        if actual_value is None:
            parse_default = required.input.parse_default
            if isinstance(parse_default, MissingDefault):
                message = f"{contract.flag} requires an explicit {required.flag} value"
                raise CommandBindingError(message)
            actual_value = parse_default
        normalized_actual = _normalize_binding(required.input, actual_value)
        if isinstance(normalized_actual, tuple):
            message = f"global requirement target {required.flag} must be scalar"
            raise CommandContractError(message)
        actual_value = normalized_actual
        if actual_value != expected_value:
            message = f"{contract.flag} requires {required.flag}={expected_value}"
            raise CommandBindingError(message)


def _require_unique(
    values: Iterable[str | tuple[str, ...]],
    *,
    label: str,
) -> None:
    seen: set[str | tuple[str, ...]] = set()
    for value in values:
        if value in seen:
            message = f"duplicate {label}: {value!r}"
            raise CommandContractError(message)
        seen.add(value)
