from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_c203d130ba407abef39441f76199290d0be22719505cf28180d5dba89d88d94e import DbConnectType as DbConnectType

from ..._schemas._enums.enum_af09a232aa00e57cc077eeded778ccd92a92db99071af42f4858a2550ec94484 import DbType as DbType

__all__ = ["DbConnectType", "DbType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class DataSourceUpdateLegacyParams(BaseParamsModel):
    id: int
    name: str
    note: str | None = Field(default=None)
    type: DbType
    host: str
    port: str
    database: str
    principal: str
    userName: str
    password: str
    connectType: DbConnectType
    other: str

SOURCE_CLOSURE_DIGEST = 'sha256:7851526eaf2129e7bc758034517e07a68ae1a0a43e637f9780b932f53c8cf7e0'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:2a4788a65a1812f5f9fe145685475db84f026f3be19506a7b2396bf3d72bca28'
