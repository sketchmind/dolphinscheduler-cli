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

SOURCE_CLOSURE_DIGEST = 'sha256:9fb575bc29cfa7012a0af0225b85438a9ffd1204b23a7a6c875a0ec6a5f47966'

RESPONSE_TYPE = User | None
EXECUTABLE_SCHEMA_DIGEST = 'sha256:f137d39dd42de7dd5e6d8a3c090d60c69b023bed21312dbd72e2a4f470e91fd8'
