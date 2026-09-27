from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Tenant(BaseEntityModel):
    id: int | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    description: str | None = Field(default=None)
    queueId: int = Field(default=0)
    queueName: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["Tenant"]

Tenant.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Tenant
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a638e3627bed4bf53687bde61ab892b0e1cf671d893706fadd2a341c32c771ce'
