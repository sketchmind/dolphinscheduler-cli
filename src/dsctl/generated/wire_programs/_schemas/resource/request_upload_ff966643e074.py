from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_399430dcb8566f02b5fb650e3a5e67aa92ec10588807f1b093a92bb17ff7edec import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class UploadParams(BaseParamsModel):
    type: ResourceType
    name: str
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:fcd76d8894c673ee8a549d66b460bbc1d70432f6255ef7212439dee3b569c6c2'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ff966643e074415c806446e1881d1de6ed19e9cc6e527b86f8500fb8bc31a6bd'
