from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class WorkflowInstanceParentInstanceView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_queryParentInstanceBySubId_dataMap."""
    parentWorkflowInstance: int | None = Field(default=None)

__all__ = ["WorkflowInstanceParentInstanceView"]

WorkflowInstanceParentInstanceView.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkflowInstanceParentInstanceView
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c7e236042c413114611a63ae29a415a701ac92313a197e78d5d6bf58085ba122'
