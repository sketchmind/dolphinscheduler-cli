from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import yaml
from pydantic import Field, ValidationError, field_validator, model_validator

import dsctl.models.workflow_spec as workflow_spec_module
from dsctl.models.common import (
    GlobalParamSpec,
    Priority,
    ReleaseState,
    RetrySpec,
    WorkflowExecutionType,
    YamlObject,
    YamlSpecModel,
    YamlValue,
    first_validation_error_message,
)
from dsctl.models.task_spec import (
    TaskRunFlag,
    TaskTimeoutNotifyStrategy,
    normalize_task_run_flag,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_spec import WorkflowTaskSpec


class WorkflowPatchWorkflowSetSpec(YamlSpecModel):
    """Partial workflow metadata mutation accepted by `workflow edit`."""

    name: str | None = None
    description: str | None = None
    timeout: int | None = Field(default=None, ge=0)
    global_params: dict[str, str] | list[GlobalParamSpec] | None = None
    execution_type: WorkflowExecutionType | None = None
    release_state: ReleaseState | None = None

    @field_validator("name", "description")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject blank workflow text fields after trimming whitespace."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Workflow patch text fields must not be empty"
            raise ValueError(message)
        return normalized

    @model_validator(mode="after")
    def validate_non_empty_patch(self) -> WorkflowPatchWorkflowSetSpec:
        """Require at least one workflow field when the workflow patch is used."""
        if self.model_fields_set:
            return self
        message = "workflow.set must include at least one field"
        raise ValueError(message)


class WorkflowPatchTaskSetSpec(YamlSpecModel):
    """Partial task mutation accepted by `workflow edit`."""

    type: str | None = None
    description: str | None = None
    task_params: YamlObject | None = None
    command: str | None = None
    flag: TaskRunFlag | None = None
    worker_group: str | None = None
    environment_code: int | None = Field(default=None, ge=1)
    task_group_id: int | None = Field(default=None, ge=1)
    task_group_priority: int | None = Field(default=None, ge=0)
    priority: Priority | None = None
    retry: RetrySpec | None = None
    timeout_notify_strategy: TaskTimeoutNotifyStrategy | None = None
    timeout: int | None = Field(default=None, ge=0)
    delay: int | None = Field(default=None, ge=0)
    cpu_quota: int | None = Field(default=None, ge=-1)
    memory_max: int | None = Field(default=None, ge=-1)
    depends_on: list[str] | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_system_managed_fields(cls, value: YamlValue) -> YamlValue:
        """Reject DS-managed task identity fields from workflow patches."""
        if not isinstance(value, Mapping):
            return value
        for field_name in ("code", "version"):
            if field_name in value:
                message = (
                    f"Task patch field '{field_name}' is system-managed and cannot "
                    "be set"
                )
                raise ValueError(message)
        return value

    @field_validator("type", "description", "worker_group")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject blank task text fields after trimming whitespace."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task patch text fields must not be empty"
            raise ValueError(message)
        return normalized

    @field_validator("command")
    @classmethod
    def validate_command(cls, value: str | None) -> str | None:
        """Reject blank command patches while preserving exact script content."""
        if value is None:
            return None
        if not value.strip():
            message = "Task patch text fields must not be empty"
            raise ValueError(message)
        return value

    @field_validator("flag", mode="before")
    @classmethod
    def validate_run_flag(cls, value: YamlValue) -> YamlValue:
        """Normalize one patch task flag before enum validation."""
        return normalize_task_run_flag(value)

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, value: list[str] | None) -> list[str] | None:
        """Normalize task dependencies and reject duplicates or blanks."""
        if value is None:
            return None
        normalized: list[str] = []
        seen: set[str] = set()
        for dependency in value:
            candidate = dependency.strip()
            if not candidate:
                message = "Task dependencies must not contain empty names"
                raise ValueError(message)
            if candidate in seen:
                message = f"Task dependency '{candidate}' is duplicated"
                raise ValueError(message)
            seen.add(candidate)
            normalized.append(candidate)
        return normalized

    @model_validator(mode="after")
    def validate_non_empty_patch(self) -> WorkflowPatchTaskSetSpec:
        """Require at least one task field when the task patch is used."""
        if self.model_fields_set:
            if self.timeout == 0 and self.timeout_notify_strategy is not None:
                message = (
                    "tasks.update[].set.timeout_notify_strategy requires "
                    "timeout > 0 when timeout is set to 0"
                )
                raise ValueError(message)
            return self
        message = "tasks.update[].set must include at least one field"
        raise ValueError(message)


class WorkflowPatchTaskMatchSpec(YamlSpecModel):
    """Current task identity used for name-based patch matching."""

    name: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Reject blank task names after trimming whitespace."""
        normalized = value.strip()
        if not normalized:
            message = "Task match names must not be empty"
            raise ValueError(message)
        return normalized


class WorkflowPatchTaskUpdateSpec(YamlSpecModel):
    """One task update operation."""

    match: WorkflowPatchTaskMatchSpec
    set: WorkflowPatchTaskSetSpec


class WorkflowPatchTaskRenameSpec(YamlSpecModel):
    """One explicit task rename operation."""

    from_name: str = Field(alias="from")
    to_name: str = Field(alias="to")

    @field_validator("from_name", "to_name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Reject blank rename names after trimming whitespace."""
        normalized = value.strip()
        if not normalized:
            message = "Task rename fields must not be empty"
            raise ValueError(message)
        return normalized

    @model_validator(mode="after")
    def validate_distinct_names(self) -> WorkflowPatchTaskRenameSpec:
        """Reject no-op renames."""
        if self.from_name != self.to_name:
            return self
        message = "Task rename targets must differ"
        raise ValueError(message)


class WorkflowPatchTasksSpec(YamlSpecModel):
    """Task operation block accepted by `workflow edit`."""

    create: list[WorkflowTaskSpec] = Field(default_factory=list)
    update: list[WorkflowPatchTaskUpdateSpec] = Field(default_factory=list)
    rename: list[WorkflowPatchTaskRenameSpec] = Field(default_factory=list)
    delete: list[str] = Field(default_factory=list)

    @field_validator("delete")
    @classmethod
    def validate_delete_names(cls, value: list[str]) -> list[str]:
        """Reject duplicate or blank task delete entries."""
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            candidate = item.strip()
            if not candidate:
                message = "tasks.delete must not contain empty names"
                raise ValueError(message)
            if candidate in seen:
                message = f"Task '{candidate}' is duplicated in tasks.delete"
                raise ValueError(message)
            seen.add(candidate)
            normalized.append(candidate)
        return normalized

    @model_validator(mode="after")
    def validate_non_empty_patch(self) -> WorkflowPatchTasksSpec:
        """Require at least one task operation when the task patch is used."""
        if self.create or self.update or self.rename or self.delete:
            return self
        message = (
            "tasks must include at least one create, update, rename, "
            "or delete operation"
        )
        raise ValueError(message)


class WorkflowPatchWorkflowSpec(YamlSpecModel):
    """Workflow operation block accepted by `workflow edit`."""

    set: WorkflowPatchWorkflowSetSpec


class WorkflowPatchSpec(YamlSpecModel):
    """Patch YAML payload accepted by `workflow edit --patch`."""

    workflow: WorkflowPatchWorkflowSpec | None = None
    tasks: WorkflowPatchTasksSpec | None = None

    @model_validator(mode="after")
    def validate_non_empty_patch(self) -> WorkflowPatchSpec:
        """Require at least one workflow or task mutation."""
        if self.workflow is not None or self.tasks is not None:
            return self
        message = "Patch YAML must include workflow and/or tasks changes"
        raise ValueError(message)


class WorkflowPatchDocument(YamlSpecModel):
    """Top-level wrapper for workflow patch YAML documents."""

    patch: WorkflowPatchSpec


def load_workflow_patch(path: Path) -> WorkflowPatchSpec:
    """Load one workflow patch YAML file into the validated patch model."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        message = f"Could not read workflow patch YAML: {exc}"
        raise ValueError(message) from exc
    except yaml.YAMLError as exc:
        message = f"Workflow patch YAML is invalid: {exc}"
        raise ValueError(message) from exc

    if not isinstance(document, Mapping):
        message = "Workflow patch YAML root must be a mapping"
        raise TypeError(message)
    try:
        return WorkflowPatchDocument.model_validate(document).patch
    except ValidationError as exc:
        raise ValueError(first_validation_error_message(exc)) from exc


def _workflow_patch_types_namespace() -> dict[str, object]:
    return {"WorkflowTaskSpec": workflow_spec_module.WorkflowTaskSpec}


WorkflowPatchDocument.model_rebuild(_types_namespace=_workflow_patch_types_namespace())
