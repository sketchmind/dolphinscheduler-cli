from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, TypeAlias, TypeGuard

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from typing_extensions import TypeAliasType

if TYPE_CHECKING:
    YamlValue: TypeAlias = (
        str | int | float | bool | None | list["YamlValue"] | dict[str, "YamlValue"]
    )
    YamlObject: TypeAlias = dict[str, YamlValue]
else:
    YamlValue = TypeAliasType(
        "YamlValue",
        str | int | float | bool | None | list["YamlValue"] | dict[str, "YamlValue"],
    )
    YamlObject = TypeAliasType("YamlObject", dict[str, YamlValue])


def is_yaml_value(value: object) -> TypeGuard[YamlValue]:
    """Return whether one runtime value is safe for YAML spec models."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and is_yaml_value(item) for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return all(is_yaml_value(item) for item in value)
    return False


def is_yaml_object(value: object) -> TypeGuard[YamlObject]:
    """Return whether one runtime value is a YAML-safe mapping."""
    if not isinstance(value, Mapping):
        return False
    return all(
        isinstance(key, str) and is_yaml_value(item) for key, item in value.items()
    )


@dataclass(frozen=True, slots=True)
class ModelValidationIssue:
    """One preserved Pydantic validation issue with a stable YAML-style path."""

    code: str
    path: str | None
    message: str


def yaml_value_validation_issue(
    value: object,
    *,
    path: str | None = None,
) -> ModelValidationIssue | None:
    """Locate the first value outside the external YAML model boundary."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return None
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                return ModelValidationIssue(
                    code="yaml_mapping_key_not_string",
                    path=path or "$",
                    message=(
                        "YAML mapping keys must be strings; quote this "
                        f"{type(key).__name__} key."
                    ),
                )
            issue = yaml_value_validation_issue(
                item,
                path=key if path is None else f"{path}.{key}",
            )
            if issue is not None:
                return issue
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            issue = yaml_value_validation_issue(
                item,
                path=f"[{index}]" if path is None else f"{path}[{index}]",
            )
            if issue is not None:
                return issue
        return None
    if isinstance(value, (date, datetime)):
        return ModelValidationIssue(
            code="yaml_date_time_not_string",
            path=path,
            message=("YAML date/time values must be quoted when a string is intended."),
        )
    return ModelValidationIssue(
        code="yaml_value_type_unsupported",
        path=path,
        message=f"YAML value type '{type(value).__name__}' is not supported.",
    )


class ModelValidationError(ValueError):
    """Keep every issue from one Pydantic validation stage for diagnostic callers."""

    def __init__(self, issues: Sequence[ModelValidationIssue]) -> None:
        """Store every issue while keeping the first issue as concise text."""
        preserved = tuple(issues)
        if not preserved:
            message = "Model validation failed without diagnostic details"
            raise ValueError(message)
        self.issues = preserved
        super().__init__(_validation_issue_message(preserved[0]))


def model_validation_issues(error: ValidationError) -> tuple[ModelValidationIssue, ...]:
    """Preserve all Pydantic errors from one model-validation call."""
    issues: list[ModelValidationIssue] = []
    for detail in error.errors(include_url=False):
        message = str(detail["msg"])
        if message.startswith("Value error, "):
            message = message.removeprefix("Value error, ")
        issues.append(
            ModelValidationIssue(
                code=str(detail["type"]),
                path=_validation_location_path(detail["loc"]),
                message=message,
            )
        )
    return tuple(issues)


def prefixed_model_validation_issues(
    issues: Sequence[ModelValidationIssue],
    *,
    prefix: str,
) -> tuple[ModelValidationIssue, ...]:
    """Prefix preserved model issues without flattening them into one message."""
    return tuple(
        ModelValidationIssue(
            code=issue.code,
            path=(prefix if issue.path is None else f"{prefix}.{issue.path}"),
            message=issue.message,
        )
        for issue in issues
    )


def first_validation_error_message(error: ValidationError) -> str:
    """Format the first Pydantic validation error as one stable dotted path."""
    return _validation_issue_message(model_validation_issues(error)[0])


def _validation_issue_message(issue: ModelValidationIssue) -> str:
    if issue.path is None:
        return issue.message
    if issue.message.startswith((f"{issue.path} ", f"{issue.path}:")):
        return issue.message
    return f"{issue.path}: {issue.message}"


def _validation_location_path(location: Sequence[str | int]) -> str | None:
    path = ""
    for part in location:
        if isinstance(part, int):
            path = f"{path}[{part}]"
        elif path:
            path = f"{path}.{part}"
        else:
            path = part
    return path or None


class _LabeledStrEnum(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> _LabeledStrEnum:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj


class Direct(StrEnum):
    """Direction for one workflow global parameter."""

    IN = "IN"
    OUT = "OUT"


class DataType(StrEnum):
    """Supported DS parameter data types for YAML global params."""

    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    LIST = "LIST"
    FILE = "FILE"


class FailureStrategy(_LabeledStrEnum):
    """Workflow schedule failure strategy."""

    END = ("END", 0, "end")
    CONTINUE = ("CONTINUE", 1, "continue")


class Priority(_LabeledStrEnum):
    """Workflow and task priority values."""

    HIGHEST = ("HIGHEST", 0, "highest")
    HIGH = ("HIGH", 1, "high")
    MEDIUM = ("MEDIUM", 2, "medium")
    LOW = ("LOW", 3, "low")
    LOWEST = ("LOWEST", 4, "lowest")


class ReleaseState(_LabeledStrEnum):
    """Workflow or schedule release lifecycle state."""

    OFFLINE = ("OFFLINE", 0, "offline")
    ONLINE = ("ONLINE", 1, "online")


class WorkflowExecutionType(_LabeledStrEnum):
    """Workflow execution mode."""

    PARALLEL = ("PARALLEL", 0, "parallel")
    SERIAL_WAIT = ("SERIAL_WAIT", 1, "serial wait")
    SERIAL_DISCARD = ("SERIAL_DISCARD", 2, "serial discard")
    SERIAL_PRIORITY = ("SERIAL_PRIORITY", 3, "serial priority")


class YamlSpecModel(BaseModel):
    """Base class for external YAML input models."""

    model_config = ConfigDict(extra="forbid")


class GlobalParamSpec(YamlSpecModel):
    """One workflow global parameter entry."""

    prop: str
    value: str | None = None
    direct: Direct = Direct.IN
    type: DataType = DataType.VARCHAR

    @field_validator("prop")
    @classmethod
    def _validate_prop(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            message = "Global parameter names must not be empty"
            raise ValueError(message)
        return normalized


class RetrySpec(YamlSpecModel):
    """Stable retry block shared by task YAML models."""

    times: int = Field(default=0, ge=0)
    interval: int = Field(default=0, ge=0)
