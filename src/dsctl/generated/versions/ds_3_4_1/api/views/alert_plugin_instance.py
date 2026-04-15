from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

class AlertPluginInstanceVO(BaseViewModel):
    id: int = Field(default=0, description='id')
    pluginDefineId: int = Field(default=0, description='plugin_define_id')
    instanceName: str | None = Field(default=None, description='alert plugin instance name')
    instanceType: str | None = Field(default=None, description='alert plugin instance type')
    warningType: str | None = Field(default=None, description='alert plugin instance warning type')
    pluginInstanceParams: str | None = Field(default=None, description='plugin_instance_params')
    createTime: str | None = Field(default=None, description='create_time')
    updateTime: str | None = Field(default=None, description='update_time')
    alertPluginName: str | None = Field(default=None, description='alert plugin name')

__all__ = ["AlertPluginInstanceVO"]
