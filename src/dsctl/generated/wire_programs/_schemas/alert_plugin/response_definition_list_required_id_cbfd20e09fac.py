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

RESPONSE_TYPE = list[PluginDefine]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:cbfd20e09fac520ccd1c014350b32a0ce51bff92b55f7f52f3056e70781753f0'
