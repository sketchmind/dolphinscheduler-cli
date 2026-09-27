from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class K8sNamespace(BaseEntityModel):
    id: int | None = Field(default=None)
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
    onlineJobNum: int = Field(default=0)
    k8s: str | None = Field(default=None)

__all__ = ["K8sNamespace"]

K8sNamespace.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[K8sNamespace]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:bb3e20119cb63c585c79e6d95f83313b643160822b13680181daa1b7918b03d7'
