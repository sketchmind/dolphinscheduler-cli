from __future__ import annotations

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)


class PigeonTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for one PIGEON remote TIS job trigger."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    target_job_name: str = Field(
        alias="targetJobName",
        description="Non-empty Pigeon/TIS job name sent as the appname header.",
    )

    @field_validator("target_job_name")
    @classmethod
    def validate_target_job_name(cls, value: str) -> str:
        """Match the plugin's non-blank targetJobName validation."""
        normalized = value.strip()
        if not normalized:
            message = "targetJobName must not be empty"
            raise ValueError(message)
        return normalized
