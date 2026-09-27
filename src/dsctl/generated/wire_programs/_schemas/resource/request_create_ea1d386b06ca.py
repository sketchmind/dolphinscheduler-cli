from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_399430dcb8566f02b5fb650e3a5e67aa92ec10588807f1b093a92bb17ff7edec import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class CreateParams(BaseParamsModel):
    type: ResourceType
    fileName: str
    suffix: str
    content: str
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:2f0850fbc3337010267bfaa3f95d709e338005e9f849919bec831ad839cbeee4'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ea1d386b06ca579debe2d111012a5f0ea37c84d8f7b0d60df4fba2f13db0f063'
