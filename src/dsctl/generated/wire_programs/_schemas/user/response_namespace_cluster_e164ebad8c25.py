from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class K8sNamespace(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    namespace: str | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    clusterCode: int | None = Field(default=None)
    clusterName: str | None = Field(default=None)

__all__ = ["K8sNamespace"]

K8sNamespace.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[K8sNamespace]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e164ebad8c25dd9b5d7b8645d1f6e410f3ebed633cfe64421147874b7ba00b47'
