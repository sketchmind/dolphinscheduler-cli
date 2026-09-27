from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_5f167da067de0ba377456c3c11ed3d1da9cbc222bcf3294dcaf87b73597be08b import UserType as UserType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

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

class PageInfoUser(PageInfo[User]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.User>."""

__all__ = ["UserType", "PageInfo", "User", "PageInfoUser"]

PageInfo.model_rebuild(_types_namespace=globals())
User.model_rebuild(_types_namespace=globals())
PageInfo[User].model_rebuild(_types_namespace=globals())
PageInfoUser.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:17a2c1fae6ccca433a7538420a6d6a3aa684784faebb52e5e5814e73af6f7b24'

RESPONSE_TYPE = PageInfoUser
EXECUTABLE_SCHEMA_DIGEST = 'sha256:cb792b9f54af047b27475d60314a2e65d51dddf6ff9876e15eb9fde00a8bd0b1'
