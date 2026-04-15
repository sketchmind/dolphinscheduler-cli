from __future__ import annotations

from pydantic import Field

from ...common.enums.flag import Flag
from ...common.enums.release_state import ReleaseState
from ...common.enums.workflow_execution_type_enum import WorkflowExecutionTypeEnum
from ...plugin.task_api.model.property import Property
from .schedule import Schedule
from .workflow_definition import WorkflowDefinition

class WorkflowDefinitionLog(WorkflowDefinition):
    operator: int = Field(default=0)
    operateTime: str | None = Field(default=None)

__all__ = ["WorkflowDefinitionLog"]
