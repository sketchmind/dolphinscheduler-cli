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

RESPONSE_TYPE = PluginDefine
EXECUTABLE_SCHEMA_DIGEST = 'sha256:176fe24196beae2693d0e9abdbebc19a5e4a61279d394db30dd3e265e13f1312'
