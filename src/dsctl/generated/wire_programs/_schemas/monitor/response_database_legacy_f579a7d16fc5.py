from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_af09a232aa00e57cc077eeded778ccd92a92db99071af42f4858a2550ec94484 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:b799518fc818e2d9ed6f69748b0fc47aba77cca74f434d80791162f2fdb97915'

RESPONSE_TYPE = list[MonitorRecord]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:f579a7d16fc531698ef6600a47e17d3734898251917c68c323c9093561e5ce19'
