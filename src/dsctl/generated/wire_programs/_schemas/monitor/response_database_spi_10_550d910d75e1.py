from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_4aec52e74ae3a9c3579966c298e6afbb81d0be67897fba3aa60d6fc18bce10c1 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:ede636c6482bf453af94d157479680552b7e6231c7bd939c70072a3a1df22ff4'

RESPONSE_TYPE = list[MonitorRecord]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:550d910d75e1b24dd4e1f998d2894efcd6f6246d58dc43d038d3a2d67890cd7e'
