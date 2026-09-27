from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class AlertGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    groupName: str | None = Field(default=None)
    alertInstanceIds: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    createUserId: int = Field(default=0)

__all__ = ["AlertGroup"]

AlertGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AlertGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:bcea955499a91c122e88adead1c7e16ab6317fddcb8c479ce47cb204b49e3982'
