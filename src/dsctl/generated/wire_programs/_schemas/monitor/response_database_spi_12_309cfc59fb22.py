from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_64a31f78df280707aebf10647477999145daf0b04dab41d7dd996d76cd7f443a import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:ad566f0d9a89b56765d5d543274ec1680edc72f72ff404ea716ffeffb2a9fbe7'

RESPONSE_TYPE = list[MonitorRecord]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:309cfc59fb22362a6aaaf1381468e696db8b04f668eb22de8e80a232ae97b94b'
