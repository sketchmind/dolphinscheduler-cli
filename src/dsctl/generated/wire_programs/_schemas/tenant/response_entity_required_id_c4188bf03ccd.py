from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Tenant(BaseEntityModel):
    id: int = Field(default=0)
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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c4188bf03ccd1843f3f673646e3957b72a16adb20689722c2fd308c7d974cace'
