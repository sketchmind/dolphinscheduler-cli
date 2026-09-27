from __future__ import annotations

from enum import StrEnum

from pydantic import (
    ConfigDict,
)

from dsctl.models.common import (
    YamlObject,
    YamlSpecModel,
    YamlValue,
    is_yaml_object,
)


class TaskExecutionStatus(StrEnum):
    """Supported DS task execution status values."""

    SUBMITTED_SUCCESS = "SUBMITTED_SUCCESS"
    RUNNING_EXECUTION = "RUNNING_EXECUTION"
    PAUSE = "PAUSE"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    NEED_FAULT_TOLERANCE = "NEED_FAULT_TOLERANCE"
    KILL = "KILL"
    DELAY_EXECUTION = "DELAY_EXECUTION"
    FORCED_SUCCESS = "FORCED_SUCCESS"
    DISPATCH = "DISPATCH"


class TaskRunFlag(StrEnum):
    """Supported DS task run-flag values."""

    YES = "YES"
    NO = "NO"


class TaskTimeoutNotifyStrategy(StrEnum):
    """Supported DS task timeout notification strategies."""

    WARN = "WARN"
    FAILED = "FAILED"
    WARNFAILED = "WARNFAILED"


def normalize_task_run_flag(value: YamlValue) -> YamlValue:
    """Normalize one task run flag from YAML-friendly input."""
    if isinstance(value, bool):
        return TaskRunFlag.YES.value if value else TaskRunFlag.NO.value
    if isinstance(value, str):
        return value.strip().upper()
    return value


class TaskParamsSpec(YamlSpecModel):
    """Base class for typed workflow task_params models."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Return defaulted fields that must still be sent to DS."""
        return ()

    def to_payload(self) -> YamlObject:
        """Serialize one validated task params model back to plain YAML data."""
        payload = self.model_dump(
            by_alias=True,
            mode="json",
            exclude_none=True,
            exclude_unset=True,
        )
        if not is_yaml_object(payload):
            message = "Validated task params did not serialize to a YAML object"
            raise TypeError(message)
        for field_name in self.default_payload_field_names():
            field = type(self).model_fields[field_name]
            alias = field.alias or field_name
            payload.setdefault(alias, getattr(self, field_name))
        return payload
