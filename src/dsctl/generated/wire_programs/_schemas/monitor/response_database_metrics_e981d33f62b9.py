from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, JsonValue

from ..._schemas._enums.enum_a08761fa013b94ba88f85197c1f4195c450d5e289929fb4ed550fff04d7462e9 import DatabaseMetricsDatabaseHealthStatus as DatabaseMetricsDatabaseHealthStatus

class DatabaseMetrics(BaseContractModel):
    dbType: JsonValue | None = Field(default=None)
    state: DatabaseMetricsDatabaseHealthStatus | None = Field(default=None)
    maxConnections: int = Field(default=0)
    maxUsedConnections: int = Field(default=0)
    threadsConnections: int = Field(default=0)
    threadsRunningConnections: int = Field(default=0)
    date: str | None = Field(default=None)

__all__ = ["DatabaseMetricsDatabaseHealthStatus", "DatabaseMetrics"]

DatabaseMetrics.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:e2c42566c8211442ec7876a4ada8e10e8b1381261c6477b50671b2de4a139bf9'

RESPONSE_TYPE = list[DatabaseMetrics]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e981d33f62b913589a217e21a2e7d3cce0f821c2b5beeb53fd8a622c0ba3cee8'
