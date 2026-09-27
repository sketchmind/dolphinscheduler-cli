from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class AlertPluginInstance(BaseEntityModel):
    id: int | None = Field(default=None)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["AlertPluginInstance"]

AlertPluginInstance.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AlertPluginInstance
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ebc2bdd4784b9177d52ea3c3046bced7d10dc9be83165df68644d2eb494a7133'
