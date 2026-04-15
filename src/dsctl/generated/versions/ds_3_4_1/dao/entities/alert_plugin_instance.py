from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class AlertPluginInstance(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    pluginDefineId: int = Field(default=0, description='plugin_define_id')
    instanceName: str | None = Field(default=None, description='alert plugin instance name')
    pluginInstanceParams: str | None = Field(default=None, description='plugin_instance_params')
    createTime: str | None = Field(default=None, description='create_time')
    updateTime: str | None = Field(default=None, description='update_time')

__all__ = ["AlertPluginInstance"]
