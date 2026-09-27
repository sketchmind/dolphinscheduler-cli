from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_867adbc57bc7fc423bd5487d46e7a24a517219e4f4d80444a54b8fdc66fe45ee import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:fd58546bc2009d45b96723d91b51f8cc4567af29cb6369f69119d85cea311921'

RESPONSE_TYPE = list[MonitorRecord]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7610c44022139e215133859d96da3d4b0ac17e79c53ef4fb328d86f0f7cf62b2'
