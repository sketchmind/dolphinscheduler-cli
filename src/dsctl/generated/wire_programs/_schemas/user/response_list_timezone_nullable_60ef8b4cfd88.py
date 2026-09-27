from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_5f167da067de0ba377456c3c11ed3d1da9cbc222bcf3294dcaf87b73597be08b import UserType as UserType

class User(BaseEntityModel):
    id: int | None = Field(default=None)
    userName: str | None = Field(default=None)
    userPassword: str | None = Field(default=None)
    email: str | None = Field(default=None)
    phone: str | None = Field(default=None)
    userType: UserType | None = Field(default=None)
    tenantId: int = Field(default=0)
    state: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    queueName: str | None = Field(default=None)
    alertGroup: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    timeZone: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["UserType", "User"]

User.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:c268fb9d311b2cac05ac2317f55e835e5d838a50e99d9b369aca2bdb74fec21a'

RESPONSE_TYPE = list[User]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:60ef8b4cfd88643b34a54f519c786f2524eee68a6c7e4fd067f8254ec57dbff4'
