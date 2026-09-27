from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class ProjectWorkerGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    projectCode: int | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ProjectWorkerGroup"]

ProjectWorkerGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[ProjectWorkerGroup]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:2e91f95dc0129d04b5d710a2282ae4600ad85c324727c9e53a7894897dc0d599'
