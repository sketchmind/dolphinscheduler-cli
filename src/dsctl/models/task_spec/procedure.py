from __future__ import annotations

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.common import GlobalParamSpec
from dsctl.models.procedure_call import (
    PROCEDURE_POSITIONAL_CALL_PATTERN,
    ProcedureCall,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference


class ProcedureTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for PROCEDURE task params."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    datasource_type: str = Field(
        alias="type",
        description="DS datasource DbType enum name.",
    )
    datasource: DatasourceReference = Field(
        description=(
            "Positive DS datasource id or exact datasource name used for the "
            "procedure call."
        ),
    )
    method: str = Field(
        pattern=PROCEDURE_POSITIONAL_CALL_PATTERN,
        description=(
            "Canonical JDBC call with positional ? placeholders in localParams order."
        ),
        json_schema_extra={
            "x-dsctl": {"placeholder_count_matches": "localParams.length"}
        },
    )
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
        description="Ordered JDBC IN/OUT parameter bindings.",
    )
    var_pool: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="varPool",
        max_length=0,
        description="Derived runtime output pool; authored value must remain empty.",
    )

    @field_validator("datasource_type")
    @classmethod
    def normalize_datasource_type(cls, value: str) -> str:
        """Normalize the DS DbType enum spelling used by procedure execution."""
        normalized = value.strip()
        if not normalized:
            message = "Procedure datasource type must not be empty"
            raise ValueError(message)
        return normalized.upper()

    @field_validator("method")
    @classmethod
    def validate_method(cls, value: str) -> str:
        """Reject blank JDBC procedure call text."""
        normalized = value.strip()
        if not normalized:
            message = "Procedure method must not be empty"
            raise ValueError(message)
        return normalized

    @model_validator(mode="after")
    def validate_canonical_call(self) -> ProcedureTaskParamsSpec:
        """Keep one version-neutral JDBC procedure-call subset."""
        call = ProcedureCall.parse(self.method)
        if call is None or not call.uses_positional_placeholders:
            message = (
                "method must use canonical JDBC procedure syntax such as "
                "{call schema.refresh_daily(?,?)}"
            )
            raise ValueError(message)
        if len(call.arguments) != len(self.local_params):
            message = "method placeholder count must match localParams"
            raise ValueError(message)
        unsupported_types = sorted(
            {
                parameter.type.value
                for parameter in self.local_params
                if parameter.type.value not in _PROCEDURE_PARAMETER_DATA_TYPES
            }
        )
        if unsupported_types:
            values = ", ".join(unsupported_types)
            message = f"PROCEDURE localParams use unsupported JDBC types: {values}"
            raise ValueError(message)
        return self


PROCEDURE_PARAMETER_DATA_TYPES = (
    "VARCHAR",
    "INTEGER",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "BOOLEAN",
)
_PROCEDURE_PARAMETER_DATA_TYPES = frozenset(PROCEDURE_PARAMETER_DATA_TYPES)
