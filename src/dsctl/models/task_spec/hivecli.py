from __future__ import annotations

from typing import Literal

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.common import GlobalParamSpec
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    _validate_unique_input_varchar_params,
    contains_ds_parameter_placeholder,
)


class HiveCliTaskParamsSpec(TaskParamsSpec):
    """Portable typed inline-SCRIPT shape for HIVECLI tasks."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    execution_type: Literal["SCRIPT"] = Field(alias="hiveCliTaskExecutionType")
    hive_sql_script: str = Field(alias="hiveSqlScript")
    hive_cli_options: str = Field(
        default="",
        alias="hiveCliOptions",
        description=(
            "Literal Hive CLI options; DS placeholder syntax is not supported."
        ),
    )
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )

    @field_validator("hive_sql_script")
    @classmethod
    def validate_hive_sql_script(cls, value: str) -> str:
        """Reject blank SQL while preserving the authored text exactly."""
        if not value.strip():
            message = "hiveSqlScript must not be empty"
            raise ValueError(message)
        return value

    @field_validator("hive_cli_options")
    @classmethod
    def validate_literal_hive_cli_options(cls, value: str) -> str:
        """Keep options literal across the 3.1 and 3.2 execution epochs."""
        if contains_ds_parameter_placeholder(value):
            message = "hiveCliOptions must not contain DS placeholders"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_local_params(self) -> HiveCliTaskParamsSpec:
        """Keep one portable SQL-substitution parameter subset."""
        _validate_unique_input_varchar_params(
            self.local_params,
            task_type="HIVECLI",
        )
        return self
