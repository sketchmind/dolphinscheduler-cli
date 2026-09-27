from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_c203d130ba407abef39441f76199290d0be22719505cf28180d5dba89d88d94e import DbConnectType as DbConnectType

from ..._schemas._enums.enum_af09a232aa00e57cc077eeded778ccd92a92db99071af42f4858a2550ec94484 import DbType as DbType

__all__ = ["DbConnectType", "DbType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class DataSourceCreateLegacyParams(BaseParamsModel):
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

SOURCE_CLOSURE_DIGEST = 'sha256:ae16bcee1d5c9f434ef7d6c75532595d18886fc82300554727a24e19303a7818'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:37a643752d78bbdc4557527323bd8754d6060accdf8075070a02b8a968546f6d'
