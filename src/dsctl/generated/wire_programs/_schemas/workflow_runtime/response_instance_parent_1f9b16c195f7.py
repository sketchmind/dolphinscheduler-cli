from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ProcessInstanceServiceParentInstanceView(BaseViewModel):
    """AST-inferred view from generated.view.ProcessInstanceService_queryParentInstanceBySubId_dataMap."""
    parentWorkflowInstance: int | None = Field(default=None)

__all__ = ["ProcessInstanceServiceParentInstanceView"]

ProcessInstanceServiceParentInstanceView.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProcessInstanceServiceParentInstanceView
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1f9b16c195f75e818b44cc0f9e3006aa2c7a0dbae0a05576c99985375acdc880'
