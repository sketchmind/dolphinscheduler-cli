from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ProcessInstanceServiceImplQuerySubProcessInstanceByTaskIdDataMap(BaseViewModel):
    """AST-inferred view from generated.view.ProcessInstanceServiceImpl_querySubProcessInstanceByTaskId_dataMap."""
    subProcessInstanceId: int | None = Field(default=None)

__all__ = ["ProcessInstanceServiceImplQuerySubProcessInstanceByTaskIdDataMap"]

ProcessInstanceServiceImplQuerySubProcessInstanceByTaskIdDataMap.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProcessInstanceServiceImplQuerySubProcessInstanceByTaskIdDataMap
EXECUTABLE_SCHEMA_DIGEST = 'sha256:0a2ff6a9198e2ad223a12e6ef34ae719ccf80de86f0d5e8163a2365bf378424c'
