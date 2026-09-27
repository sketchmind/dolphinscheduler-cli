from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_399430dcb8566f02b5fb650e3a5e67aa92ec10588807f1b093a92bb17ff7edec import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class BaseDirParams(BaseParamsModel):
    type: ResourceType

SOURCE_CLOSURE_DIGEST = 'sha256:2893e48da16a8bb0424b6f664844b4b40cbab7524211118fffc64aa5aa0a05ac'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b27f1f9f3aac2878312b4299f78709d244e31d7d77d0ac80acf6624b789937f9'
