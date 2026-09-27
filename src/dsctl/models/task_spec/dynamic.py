from __future__ import annotations

import re
from typing import TYPE_CHECKING, Annotated

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    contains_ds_parameter_placeholder,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )

DYNAMIC_PARAMETER_NAME_JSON_SCHEMA_PATTERN = r"^[A-Za-z_][A-Za-z0-9_.-]{0,255}$"
_DYNAMIC_VALUE_EDGE_WHITESPACE = (
    r"\x09\x0a\x0b\x0c\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
DYNAMIC_LITERAL_VALUE_JSON_SCHEMA_PATTERN = (
    rf"^(?=[\s\S])(?![{_DYNAMIC_VALUE_EDGE_WHITESPACE}])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"(?![\s\S]*,)"
    rf"(?![\s\S]*[{_DYNAMIC_VALUE_EDGE_WHITESPACE}](?![\s\S]))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*"
    r"(?![\s\S])"
)
DYNAMIC_NATIVE_VALUE_MAX_LENGTH = 256
DYNAMIC_MAX_FANOUT = 1024
DYNAMIC_MAX_WORKFLOW_CODE = 2**63 - 1
_DYNAMIC_PARAMETER_NAME_PATTERN = re.compile(DYNAMIC_PARAMETER_NAME_JSON_SCHEMA_PATTERN)
_DYNAMIC_LITERAL_VALUE_PATTERN = re.compile(DYNAMIC_LITERAL_VALUE_JSON_SCHEMA_PATTERN)


def _dynamic_utf16_code_units(value: str) -> int:
    """Measure the exact JavaScript/UI length unit used by the 3.2.2 form."""
    return len(value.encode("utf-16-le")) // 2


def validate_dynamic_parameter_name(value: str) -> str:
    """Keep one child-workflow startup parameter as a conservative key."""
    if not _DYNAMIC_PARAMETER_NAME_PATTERN.fullmatch(value):
        message = (
            "parameterName must be one literal ASCII identifier starting with a "
            "letter or underscore and containing only letters, digits, dot, "
            "underscore, or hyphen"
        )
        raise ValueError(message)
    if value.lower().startswith("system."):
        message = "parameterName must not use the reserved system.* namespace"
        raise ValueError(message)
    return value


def validate_dynamic_literal_value(value: str) -> str:
    """Keep comma projection lossless under upstream split-and-trim semantics."""
    if not _DYNAMIC_LITERAL_VALUE_PATTERN.fullmatch(value):
        message = (
            "values items must be nonblank literal strings without edge whitespace, "
            "comma, controls, surrogates, or DS placeholders"
        )
        raise ValueError(message)
    if _dynamic_utf16_code_units(value) > DYNAMIC_NATIVE_VALUE_MAX_LENGTH:
        message = (
            "values items must not exceed "
            f"{DYNAMIC_NATIVE_VALUE_MAX_LENGTH} UTF-16 code units"
        )
        raise ValueError(message)
    return value


DynamicLiteralValue = Annotated[
    str,
    Field(
        min_length=1,
        max_length=DYNAMIC_NATIVE_VALUE_MAX_LENGTH,
        json_schema_extra={"pattern": DYNAMIC_LITERAL_VALUE_JSON_SCHEMA_PATTERN},
    ),
]


class DynamicLiteralSingleDimensionFanoutTaskParamsSpec(TaskParamsSpec):
    """Closed bounded fan-out intent for the fixed DolphinScheduler 3.2.2 task."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    child_workflow_name: str = Field(alias="childWorkflowName", min_length=1)
    parameter_name: str = Field(
        alias="parameterName",
        min_length=1,
        max_length=256,
        json_schema_extra={"pattern": DYNAMIC_PARAMETER_NAME_JSON_SCHEMA_PATTERN},
    )
    values: list[DynamicLiteralValue] = Field(
        min_length=1,
        max_length=DYNAMIC_MAX_FANOUT,
    )
    degree_of_parallelism: int = Field(
        default=1,
        alias="degreeOfParallelism",
        gt=0,
        le=DYNAMIC_MAX_FANOUT,
    )

    @field_validator("parameter_name")
    @classmethod
    def validate_parameter_name(cls, value: str) -> str:
        """Reject names whose exact runtime map identity is ambiguous."""
        return validate_dynamic_parameter_name(value)

    @field_validator("child_workflow_name")
    @classmethod
    def validate_child_workflow_name(cls, value: str) -> str:
        """Require one exact literal same-project child workflow selector."""
        if not value.strip():
            message = "childWorkflowName must not be blank"
            raise ValueError(message)
        if value != value.strip():
            message = "childWorkflowName must not contain edge whitespace"
            raise ValueError(message)
        if contains_ds_parameter_placeholder(value):
            message = "childWorkflowName must be a literal name without DS placeholders"
            raise ValueError(message)
        return value

    @field_validator("values", mode="before")
    @classmethod
    def validate_values_collection(cls, value: YamlValue) -> YamlValue:
        """Require a strict nonempty list before item-level coercion."""
        if not isinstance(value, list) or not value:
            message = "values must be one non-empty strict list"
            raise ValueError(message)
        return value

    @field_validator("values")
    @classmethod
    def validate_values(cls, value: list[str]) -> list[str]:
        """Keep the one native comma-delimited dimension exact and bounded."""
        validated = [validate_dynamic_literal_value(item) for item in value]
        if len(validated) != len(set(validated)):
            message = "values items must be unique"
            raise ValueError(message)
        if (
            _dynamic_utf16_code_units(",".join(validated))
            > DYNAMIC_NATIVE_VALUE_MAX_LENGTH
        ):
            message = (
                "values joined native representation must not exceed "
                f"{DYNAMIC_NATIVE_VALUE_MAX_LENGTH} UTF-16 code units"
            )
            raise ValueError(message)
        return validated

    @model_validator(mode="after")
    def validate_parallelism(self) -> DynamicLiteralSingleDimensionFanoutTaskParamsSpec:
        """Prevent an idle or over-provisioned native polling loop."""
        if self.degree_of_parallelism > len(self.values):
            message = "degreeOfParallelism must not exceed the number of values"
            raise ValueError(message)
        return self

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Materialize the stable sequential fan-out default."""
        return ("degree_of_parallelism",)
