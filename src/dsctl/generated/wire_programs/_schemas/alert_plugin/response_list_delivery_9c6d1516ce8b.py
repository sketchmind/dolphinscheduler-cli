from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class AlertPluginInstanceVO(BaseViewModel):
    id: int = Field(default=0)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    instanceType: str | None = Field(default=None)
    warningType: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    alertPluginName: str | None = Field(default=None)

__all__ = ["AlertPluginInstanceVO"]

AlertPluginInstanceVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[AlertPluginInstanceVO]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9c6d1516ce8bdbe4f1ed3adee99d91f05ea4d0f3620d92c8b53deeb88993e8bd'
