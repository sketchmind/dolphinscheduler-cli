from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class WorkflowInstanceSubWorkflowInstanceView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_querySubWorkflowInstanceByTaskId_dataMap."""
    subWorkflowInstanceId: int | None = Field(default=None)

__all__ = ["WorkflowInstanceSubWorkflowInstanceView"]

WorkflowInstanceSubWorkflowInstanceView.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkflowInstanceSubWorkflowInstanceView
EXECUTABLE_SCHEMA_DIGEST = 'sha256:6307f415267e5e7e5b7b815d8a10d5d420f89b56e02abc2ba14e25befe1a41f7'
