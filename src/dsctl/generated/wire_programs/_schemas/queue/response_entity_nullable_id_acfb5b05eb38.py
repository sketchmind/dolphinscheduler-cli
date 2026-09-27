from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Queue(BaseEntityModel):
    id: int | None = Field(default=None)
    queueName: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["Queue"]

Queue.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Queue
EXECUTABLE_SCHEMA_DIGEST = 'sha256:acfb5b05eb389962406f8d5d5659c5a21284d8e5182ae61c430ee8b3dc7bc55b'
