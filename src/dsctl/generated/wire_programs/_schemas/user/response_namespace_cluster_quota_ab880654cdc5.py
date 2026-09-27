from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class K8sNamespace(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    namespace: str | None = Field(default=None)
    limitsCpu: float | None = Field(default=None)
    limitsMemory: int | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    podRequestCpu: float = Field(default=0.0)
    podRequestMemory: int = Field(default=0)
    podReplicas: int = Field(default=0)
    clusterCode: int | None = Field(default=None)
    clusterName: str | None = Field(default=None)

__all__ = ["K8sNamespace"]

K8sNamespace.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[K8sNamespace]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ab880654cdc5667e2458878d9a4e1edd9aa48897bdb4b286c4c80ebe7e98e0e0'
