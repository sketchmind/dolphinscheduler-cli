from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Queue(BaseEntityModel):
    id: int = Field(default=0)
    queueName: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["Queue"]

Queue.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Queue
EXECUTABLE_SCHEMA_DIGEST = 'sha256:acef8debf6a3153a23ffd2d2b66b75dd8baef4993178a5abf4c45841722c4f3f'
