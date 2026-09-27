from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ProcessDefinitionServiceImplQueryProcessDefinitionSimpleListArrayNodeItem(BaseViewModel):
    """AST-inferred view from generated.view.ProcessDefinitionServiceImpl_queryProcessDefinitionSimpleList_arrayNodeItem."""
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    projectCode: int | None = Field(default=None)

__all__ = ["ProcessDefinitionServiceImplQueryProcessDefinitionSimpleListArrayNodeItem"]

ProcessDefinitionServiceImplQueryProcessDefinitionSimpleListArrayNodeItem.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[ProcessDefinitionServiceImplQueryProcessDefinitionSimpleListArrayNodeItem]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:787d0e30ec480270efcb4db01abaf127cbcc45b5f9646b4e4ee8e3955924314b'
