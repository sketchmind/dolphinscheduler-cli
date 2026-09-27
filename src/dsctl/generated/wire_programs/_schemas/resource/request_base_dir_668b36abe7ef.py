from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_042208af0f3d7ce654cafe8b2a2ecbf82779bf76ad54923af79663905b70d26d import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class BaseDirParams(BaseParamsModel):
    type: ResourceType

SOURCE_CLOSURE_DIGEST = 'sha256:90d947a18dba4defd9e06e3725cc15c5a0a6a4b14abaf85b79d3066896228bec'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:668b36abe7ef95a60e33d0f1f2bb18648891ad24dfa6c676f9b91e7274bf06ff'
