from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_5f167da067de0ba377456c3c11ed3d1da9cbc222bcf3294dcaf87b73597be08b import UserType as UserType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

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

class PageInfoUser(PageInfo[User]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.User>."""

__all__ = ["UserType", "PageInfo", "User", "PageInfoUser"]

PageInfo.model_rebuild(_types_namespace=globals())
User.model_rebuild(_types_namespace=globals())
PageInfo[User].model_rebuild(_types_namespace=globals())
PageInfoUser.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:0dcc5382ec5066d905efbc506957ac12f1c5e715c929809d931e863276c812ca'

RESPONSE_TYPE = PageInfoUser
EXECUTABLE_SCHEMA_DIGEST = 'sha256:54bfa4090ec675a880c150255c94756d033a1a98af021a3c10c0a5d8e0f3575d'
