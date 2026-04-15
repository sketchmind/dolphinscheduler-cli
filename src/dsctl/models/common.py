from __future__ import annotations

from collections.abc import Mapping, Sequence
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


def first_validation_error_message(error: ValidationError) -> str:
    """Format the first Pydantic validation error as one stable dotted path."""
    first = error.errors(include_url=False)[0]
    location = ".".join(str(part) for part in first["loc"])
    message = str(first["msg"])
    if message.startswith("Value error, "):
        message = message.removeprefix("Value error, ")
    if not location:
        return message
    if message.startswith((f"{location} ", f"{location}:")):
        return message
    return f"{location}: {message}"


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
