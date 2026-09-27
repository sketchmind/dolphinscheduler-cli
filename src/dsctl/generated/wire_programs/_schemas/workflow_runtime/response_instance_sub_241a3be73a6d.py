from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ProcessInstanceServiceQuerySubProcessInstanceByTaskIdDataMap(BaseViewModel):
    """AST-inferred view from generated.view.ProcessInstanceService_querySubProcessInstanceByTaskId_dataMap."""
    subProcessInstanceId: int | None = Field(default=None)

__all__ = ["ProcessInstanceServiceQuerySubProcessInstanceByTaskIdDataMap"]

ProcessInstanceServiceQuerySubProcessInstanceByTaskIdDataMap.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProcessInstanceServiceQuerySubProcessInstanceByTaskIdDataMap
EXECUTABLE_SCHEMA_DIGEST = 'sha256:241a3be73a6de748e1ee3bc7a68e296f7e86b560c2735d10a00b5be573c60d39'
