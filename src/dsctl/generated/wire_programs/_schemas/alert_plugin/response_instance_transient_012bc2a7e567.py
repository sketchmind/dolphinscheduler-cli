from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_ffa48f6425c7eb789ce54f6f6741718f5ebd685531ca778b89ae19529094f9c2 import AlertPluginInstanceType as AlertPluginInstanceType

from ..._schemas._enums.enum_878d4d11e37dab4c1fa1553fc63169e22641320fdd5253b5ae993e948f231c32 import WarningType as WarningType

class AlertPluginInstance(BaseEntityModel):
    id: int | None = Field(default=None)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    instanceType: AlertPluginInstanceType | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["AlertPluginInstanceType", "WarningType", "AlertPluginInstance"]

AlertPluginInstance.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:2a292974d924c5ef43e029befda0643a37a8e41c1326f61b5c9a505b5cc63e1c'

RESPONSE_TYPE = AlertPluginInstance
EXECUTABLE_SCHEMA_DIGEST = 'sha256:012bc2a7e567e25e1b25d7d6649865f1227881a9e0abe1cb9ed8a2e8ab04eaf4'
