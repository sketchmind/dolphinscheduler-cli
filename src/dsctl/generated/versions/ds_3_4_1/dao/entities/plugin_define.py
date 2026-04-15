from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class PluginDefine(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    pluginName: str | None = Field(default=None, description='plugin name')
    pluginType: str | None = Field(default=None, description='plugin_type')
    pluginParams: str | None = Field(default=None, description='plugin_params')
    createTime: str | None = Field(default=None, description='create_time')
    updateTime: str | None = Field(default=None, description='update_time')

__all__ = ["PluginDefine"]
