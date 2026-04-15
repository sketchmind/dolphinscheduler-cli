from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

from ...common.enums.workflow_execution_status import WorkflowExecutionStatus

class WorkflowInstanceStatusCountDto(BaseContractModel):
    state: WorkflowExecutionStatus | None = Field(default=None)
    count: int = Field(default=0)

__all__ = ["WorkflowInstanceStatusCountDto"]
