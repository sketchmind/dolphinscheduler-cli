from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_ffa48f6425c7eb789ce54f6f6741718f5ebd685531ca778b89ae19529094f9c2 import AlertPluginInstanceType as AlertPluginInstanceType

from ..._schemas._enums.enum_878d4d11e37dab4c1fa1553fc63169e22641320fdd5253b5ae993e948f231c32 import WarningType as WarningType

__all__ = ["AlertPluginInstanceType", "WarningType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class AlertPluginTransientCreateParams(BaseParamsModel):
    pluginDefineId: int
    instanceName: str
    instanceType: AlertPluginInstanceType
    warningType: WarningType
    pluginInstanceParams: str

SOURCE_CLOSURE_DIGEST = 'sha256:d4a9ed5f4eeb51b969cc287bedbd72fac7e504ed5b9137d9967a80e720bb0325'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:95f3006e000f7a996afe5a05df343a1bcbe5b1a09097e772232719cba6e9a0ca'
