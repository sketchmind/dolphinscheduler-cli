from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_47a4a3e3a65c8347aba75f954c8bf048bc9168aef084c2c3bc7d0110cca7aac3 import AlertType as AlertType

__all__ = ["AlertType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class AlertGroupLegacyUpdateParams(BaseParamsModel):
    id: int
    groupName: str
    groupType: AlertType
    description: str | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:718710d6780ada756bf787324ef3edcdcd84e01626b2e481d28c01c5ffd817fd'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:5afb03e7fb489b5bd678211bb635daeb1c162138e12f6773a0800b0a78c8be41'
