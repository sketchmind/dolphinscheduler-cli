from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_7d5407323243938975d9a2555727912955d4d9da0bf0bc3ad99674350d454147 import DbType as DbType

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

class MonitorRecord(BaseEntityModel):
    dbType: DbType | None = Field(default=None)
    state: Flag | None = Field(default=None)
    maxConnections: int = Field(default=0)
    maxUsedConnections: int = Field(default=0)
    threadsConnections: int = Field(default=0)
    threadsRunningConnections: int = Field(default=0)
    date: str | None = Field(default=None)

__all__ = ["DbType", "Flag", "MonitorRecord"]

MonitorRecord.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:91906d83265997e26c5d6e216e6a362a8b4a940553be3f35541363d8860b2583'

RESPONSE_TYPE = list[MonitorRecord]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:bcbaf5872e434521d09af2fd6ee2f6a8289ed5fb3e90dd0ababc4c4e96c1ff45'
