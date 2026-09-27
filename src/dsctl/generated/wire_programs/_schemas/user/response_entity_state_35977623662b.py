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
    state: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    queueName: str | None = Field(default=None)
    alertGroup: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["UserType", "User"]

User.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:7fca7db53aaeb8771fe151cdc34246ffd38687e96351f44926e71f570f54bc16'

RESPONSE_TYPE = User
EXECUTABLE_SCHEMA_DIGEST = 'sha256:35977623662be39f58cfaaabb2abecd31f654a01c527dad6419b34ed86b16eb6'
