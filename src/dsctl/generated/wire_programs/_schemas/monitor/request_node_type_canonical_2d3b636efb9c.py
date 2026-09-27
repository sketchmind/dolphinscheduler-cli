from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_6ddb5d12fd110756cb03636e0b6318893df1e8c6d88bb9fd2404f7cfe09aedba import RegistryNodeType as RegistryNodeType

__all__ = ["RegistryNodeType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class MonitorNodeTypeCanonicalParams(BaseParamsModel):
    nodeType: RegistryNodeType

SOURCE_CLOSURE_DIGEST = 'sha256:a1a026d72ae39bbfa83bc5eee23a318f10706015d7548cab788f476ff064d79d'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:2d3b636efb9cd00676363af6280a46f9c80c3c8e595bf01e172400492f6ce60f'
