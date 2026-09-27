from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class PluginDefine(BaseEntityModel):
    id: int = Field(default=0)
    pluginName: str | None = Field(default=None)
    pluginType: str | None = Field(default=None)
    pluginParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["PluginDefine"]

PluginDefine.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PluginDefine
EXECUTABLE_SCHEMA_DIGEST = 'sha256:fbb21b781b5365b4483f958c32baefa08c69ba5813ad85af1992e0b970555007'
