from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

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
    _DS_PARAMETER_PLACEHOLDER_PATTERN,
    _validate_unique_input_varchar_params,
    emr_placeholder_names,
    validate_json_object_text,
)

if TYPE_CHECKING:
    import re
    from collections.abc import Mapping

    from dsctl.models.common import (
        YamlObject,
        YamlValue,
    )


class EmrProgramType(StrEnum):
    """Canonical Amazon EMR operation selected by one task definition."""

    RUN_JOB_FLOW = "RUN_JOB_FLOW"
    ADD_JOB_FLOW_STEPS = "ADD_JOB_FLOW_STEPS"


def _emr_json_validation_candidate(
    value: str,
    *,
    placeholder_values: Mapping[str, str | None] | None,
    require_quoted_unresolved_placeholders: bool,
) -> str:
    def replace_placeholder(
        match: re.Match[str],
        *,
        resolving: frozenset[str] = frozenset(),
    ) -> str:
        name = match.group("braced")
        if name is None or placeholder_values is None or name not in placeholder_values:
            return (
                "__DS_UNRESOLVED_PLACEHOLDER__"
                if require_quoted_unresolved_placeholders
                else "0"
            )
        if name in resolving:
            message = f"circular EMR placeholder value for {name}"
            raise ValueError(message)
        replacement = placeholder_values[name] or ""
        return _DS_PARAMETER_PLACEHOLDER_PATTERN.sub(
            lambda nested: replace_placeholder(
                nested,
                resolving=resolving | {name},
            ),
            replacement,
        )

    return _DS_PARAMETER_PLACEHOLDER_PATTERN.sub(replace_placeholder, value)


def validate_emr_json_text(
    value: str,
    *,
    field: str,
    allow_unresolved_placeholders: bool,
    placeholder_values: Mapping[str, str | None] | None = None,
    require_quoted_unresolved_placeholders: bool = False,
) -> YamlObject:
    """Validate EMR JSON while optionally retaining pre-substitution templates."""
    candidate = value
    if allow_unresolved_placeholders and emr_placeholder_names(value):
        try:
            candidate = _emr_json_validation_candidate(
                value,
                placeholder_values=placeholder_values,
                require_quoted_unresolved_placeholders=(
                    require_quoted_unresolved_placeholders
                ),
            )
        except ValueError as exc:
            message = f"{field} must contain a JSON object"
            raise ValueError(message) from exc
    return validate_json_object_text(candidate, field=field)


class EmrTaskParamsSpec(TaskParamsSpec):
    """Typed canonical YAML shape for Amazon EMR tasks."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    program_type: EmrProgramType = Field(alias="programType")
    job_flow_define_json: str | None = Field(
        default=None,
        alias="jobFlowDefineJson",
    )
    steps_define_json: str | None = Field(
        default=None,
        alias="stepsDefineJson",
    )
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )

    @field_validator("program_type", mode="before")
    @classmethod
    def normalize_program_type(cls, value: YamlValue) -> YamlValue:
        """Normalize the DS-native EMR discriminator spelling."""
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("job_flow_define_json", "steps_define_json")
    @classmethod
    def reject_blank_json_text(cls, value: str | None) -> str | None:
        """Reject blank active or inactive JSON fields without rewriting text."""
        if value is not None and not value.strip():
            message = "EMR JSON text must not be empty"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_program_payload(self) -> EmrTaskParamsSpec:
        """Require one unambiguous operation payload and simple IN params."""
        if self.program_type is EmrProgramType.RUN_JOB_FLOW:
            active_name = "jobFlowDefineJson"
            active_value = self.job_flow_define_json
            inactive_name = "stepsDefineJson"
            inactive_value = self.steps_define_json
        else:
            active_name = "stepsDefineJson"
            active_value = self.steps_define_json
            inactive_name = "jobFlowDefineJson"
            inactive_value = self.job_flow_define_json
        if active_value is None:
            message = f"{active_name} is required for {self.program_type.value}"
            raise ValueError(message)
        if inactive_value is not None:
            message = f"{inactive_name} is inactive for {self.program_type.value}"
            raise ValueError(message)
        _validate_unique_input_varchar_params(self.local_params, task_type="EMR")
        if emr_placeholder_names(active_value):
            return self
        parsed = validate_emr_json_text(
            active_value,
            field=active_name,
            allow_unresolved_placeholders=False,
        )
        if self.program_type is EmrProgramType.ADD_JOB_FLOW_STEPS:
            steps = parsed.get("Steps")
            if not isinstance(steps, list) or len(steps) != 1:
                message = "stepsDefineJson must contain exactly one Steps entry"
                raise ValueError(message)
        return self
