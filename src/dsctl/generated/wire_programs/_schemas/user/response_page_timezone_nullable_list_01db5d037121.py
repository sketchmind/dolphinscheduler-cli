from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_5f167da067de0ba377456c3c11ed3d1da9cbc222bcf3294dcaf87b73597be08b import UserType as UserType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

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

SOURCE_CLOSURE_DIGEST = 'sha256:a3ed1818f13a14a90ab1580d07dfe4897d349d318edb1e3349be7f89a04725a8'

RESPONSE_TYPE = PageInfoUser
EXECUTABLE_SCHEMA_DIGEST = 'sha256:01db5d0371219b7128541b83d9388919b00efbf54f7e6887f34b0fc1619abb1d'
