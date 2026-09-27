from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_2b55f883bd37a56d33db5280826377d674ad7a38dd7bb8520adc62e4d736032e import PluginType as PluginType

__all__ = ["PluginType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class AlertPluginDefinitionTypeParams(BaseParamsModel):
    pluginType: PluginType

SOURCE_CLOSURE_DIGEST = 'sha256:669f6122cf206bff8bc4b305e38a43e88fb71aa57d155cbc1609addb74db35e9'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d25eff73839ddfd7f2158f747cdc3106b7344395c1c900ac7b27da2d168de721'
