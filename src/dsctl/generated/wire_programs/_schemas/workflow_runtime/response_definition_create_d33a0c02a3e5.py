from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ProcessDefinitionServiceCreateProcessDefinitionResult(BaseViewModel):
    """AST-inferred view from generated.view.ProcessDefinitionService_createProcessDefinition_result."""
    processDefinitionId: int | None = Field(default=None)

__all__ = ["ProcessDefinitionServiceCreateProcessDefinitionResult"]

ProcessDefinitionServiceCreateProcessDefinitionResult.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProcessDefinitionServiceCreateProcessDefinitionResult | None
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d33a0c02a3e51d035cdf576299f78f6f8e56d08ec06d5e7e43cec8f817e8ba05'
