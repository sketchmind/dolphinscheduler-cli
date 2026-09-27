from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_5f167da067de0ba377456c3c11ed3d1da9cbc222bcf3294dcaf87b73597be08b import UserType as UserType

class User(BaseEntityModel):
    id: int = Field(default=0)
    userName: str | None = Field(default=None)
    userPassword: str | None = Field(default=None)
    email: str | None = Field(default=None)
    phone: str | None = Field(default=None)
    userType: UserType | None = Field(default=None)
    tenantId: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    tenantName: str | None = Field(default=None)
    queueName: str | None = Field(default=None)
    alertGroup: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["UserType", "User"]

User.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:99740552c8a36e1758ed64a8fbecb0aff614e5ece91932d6482b6114b4177afb'

RESPONSE_TYPE = User
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d23a8b89cba3180cb4af5adfa1283f9655a8ec097b4643b026a528d16b4b1635'
