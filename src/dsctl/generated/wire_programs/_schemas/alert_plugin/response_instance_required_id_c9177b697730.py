from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class AlertPluginInstance(BaseEntityModel):
    id: int = Field(default=0)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["AlertPluginInstance"]

AlertPluginInstance.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AlertPluginInstance
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c9177b697730c0e360949ee653e4960a473f8bfb604c51a6e3d183de5355049d'
