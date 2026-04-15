from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

from ...dao.model.workflow_instance_status_count_dto import WorkflowInstanceStatusCountDto

class WorkflowInstanceCountVO(BaseViewModel):
    totalCount: int = Field(default=0)
    workflowInstanceStatusCounts: list[WorkflowInstanceStatusCountDto] | None = Field(default=None)

__all__ = ["WorkflowInstanceCountVO"]
