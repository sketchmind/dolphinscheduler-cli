from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ProcessInstanceParentInstanceView(BaseViewModel):
    """AST-inferred view from generated.view.ProcessInstanceServiceImpl_queryParentInstanceBySubId_dataMap."""
    parentWorkflowInstance: int | None = Field(default=None)

__all__ = ["ProcessInstanceParentInstanceView"]

ProcessInstanceParentInstanceView.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProcessInstanceParentInstanceView
EXECUTABLE_SCHEMA_DIGEST = 'sha256:2f2ed6d34f5c0c5f2393dc0cf127e282a44ec2f3dbb1d9b19adf5a001337989d'
