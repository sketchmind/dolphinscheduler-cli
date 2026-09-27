from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_878d4d11e37dab4c1fa1553fc63169e22641320fdd5253b5ae993e948f231c32 import WarningType as WarningType

__all__ = ["WarningType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class AlertPluginTransientUpdateParams(BaseParamsModel):
    id: int
    instanceName: str
    warningType: WarningType
    pluginInstanceParams: str

SOURCE_CLOSURE_DIGEST = 'sha256:0105e1fb18fafbc18877660d778381f4b88495e97ecc1b5627f022ac868e0908'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7fd900597fd94efef3f63bdeca90537388f4af03b33f0caf344eaa621f4ce60f'
