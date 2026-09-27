from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_5f167da067de0ba377456c3c11ed3d1da9cbc222bcf3294dcaf87b73597be08b import UserType as UserType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

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

class PageInfoUser(PageInfo[User]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.User>."""

__all__ = ["UserType", "PageInfo", "User", "PageInfoUser"]

PageInfo.model_rebuild(_types_namespace=globals())
User.model_rebuild(_types_namespace=globals())
PageInfo[User].model_rebuild(_types_namespace=globals())
PageInfoUser.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:a45f7bf855d0e49a29f92e9699d97d2cd3658adffe3f0caa35d7ea87eedb97ee'

RESPONSE_TYPE = PageInfoUser
EXECUTABLE_SCHEMA_DIGEST = 'sha256:2cf1665fd333e1fecfebb9dfefd58bd3152df90ada6d61fef76d7c3aeb52b354'
