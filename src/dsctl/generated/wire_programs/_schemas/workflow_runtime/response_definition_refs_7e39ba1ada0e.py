from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class WorkflowDefinitionSimpleItem(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowDefinitionServiceImpl_queryWorkflowDefinitionSimpleList_arrayNodeItem."""
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    projectCode: int | None = Field(default=None)

__all__ = ["WorkflowDefinitionSimpleItem"]

WorkflowDefinitionSimpleItem.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[WorkflowDefinitionSimpleItem]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7e39ba1ada0e3fe3556286d6180c787e616665b9b5a3e761b6e6a62f39c4b480'
