from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_47a4a3e3a65c8347aba75f954c8bf048bc9168aef084c2c3bc7d0110cca7aac3 import AlertType as AlertType

__all__ = ["AlertType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class AlertGroupLegacyCreateParams(BaseParamsModel):
    groupName: str
    groupType: AlertType
    description: str | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:39bb1cefc040185e32e547c99f95afd3e844982872d87ed446505e99b234310d'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8c6aefa89476e6a0293bbe1fad4bce30a9b521650b20a7bae96bf4b63486f8ca'
