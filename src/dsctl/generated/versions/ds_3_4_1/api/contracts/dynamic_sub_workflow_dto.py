from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

from ...common.enums.workflow_execution_status import WorkflowExecutionStatus

class DynamicSubWorkflowDto(BaseContractModel):
    workflowInstanceId: int = Field(default=0)
    name: str | None = Field(default=None)
    index: int = Field(default=0)
    parameters: dict[str, str] | None = Field(default=None)
    state: WorkflowExecutionStatus | None = Field(default=None)

__all__ = ["DynamicSubWorkflowDto"]
