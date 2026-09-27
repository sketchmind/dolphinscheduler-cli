from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class DependentLineageTask(BaseEntityModel):
    projectCode: int = Field(default=0)
    workflowDefinitionCode: int = Field(default=0)
    workflowDefinitionName: str | None = Field(default=None)
    taskDefinitionCode: int = Field(default=0)
    taskDefinitionName: str | None = Field(default=None)

__all__ = ["DependentLineageTask"]

DependentLineageTask.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[DependentLineageTask]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:81e5f9a4e4bd198ab26d76be0a016c07d4800e4278371f31584a356c198363df'
