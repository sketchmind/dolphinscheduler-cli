from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class PluginDefine(BaseEntityModel):
    id: int | None = Field(default=None)
    pluginName: str | None = Field(default=None)
    pluginType: str | None = Field(default=None)
    pluginParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["PluginDefine"]

PluginDefine.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[PluginDefine]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ea8cb1c0a2071c2fabd2bee48f988eedfe4a3bbd623366a341ea41118592442a'
