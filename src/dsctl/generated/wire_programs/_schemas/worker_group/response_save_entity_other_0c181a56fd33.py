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
    otherParamsJson: str | None = Field(default=None)

__all__ = ["WorkerGroup"]

WorkerGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkerGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:0c181a56fd33e4fbe6e1432c807eb7f1c9f537b4853e165b33c2b6b4f3bd0156'
