from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class AlertGroup(BaseEntityModel):
    id: int = Field(default=0)
    groupName: str | None = Field(default=None)
    alertInstanceIds: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    createUserId: int = Field(default=0)

__all__ = ["AlertGroup"]

AlertGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AlertGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d3e4944c8fe271b38d0f0e6c6e8762f2b31ec4a4c50379f73b2ce8346d746d52'
