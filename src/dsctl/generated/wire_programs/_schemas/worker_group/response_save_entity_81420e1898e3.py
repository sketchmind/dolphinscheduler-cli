from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class WorkerGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    addrList: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    description: str | None = Field(default=None)
    systemDefault: bool = Field(default=False)

__all__ = ["WorkerGroup"]

WorkerGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkerGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:81420e1898e30166c8be07942821b96a0321270de3e48a5bc1c647421d49960d'
