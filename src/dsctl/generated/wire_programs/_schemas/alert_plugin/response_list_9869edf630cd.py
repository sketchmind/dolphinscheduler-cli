from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class AlertPluginInstanceVO(BaseViewModel):
    id: int = Field(default=0)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    alertPluginName: str | None = Field(default=None)

__all__ = ["AlertPluginInstanceVO"]

AlertPluginInstanceVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[AlertPluginInstanceVO]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9869edf630cd09ae8cec41016e88f1acd0282b1d8972281e7a7bc05ca6b51987'
