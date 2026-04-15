from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ...._models import BaseContractModel

from ....external.mybatisplus.annotation.db_type import DbType

class DatabaseMetricsDatabaseHealthStatus(StrEnum):
    YES = 'YES'
    NO = 'NO'

class DatabaseMetrics(BaseContractModel):
    dbType: DbType | None = Field(default=None)
    state: DatabaseMetricsDatabaseHealthStatus | None = Field(default=None)
    maxConnections: int = Field(default=0)
    maxUsedConnections: int = Field(default=0)
    threadsConnections: int = Field(default=0)
    threadsRunningConnections: int = Field(default=0)
    date: str | None = Field(default=None)

__all__ = ["DatabaseMetricsDatabaseHealthStatus", "DatabaseMetrics"]
