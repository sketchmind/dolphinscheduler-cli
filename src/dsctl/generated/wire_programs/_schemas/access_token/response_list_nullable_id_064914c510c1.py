from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class AccessToken(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int = Field(default=0)
    token: str | None = Field(default=None)
    expireTime: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userName: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoAccessToken(PageInfo[AccessToken]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.AccessToken>."""

__all__ = ["AccessToken", "PageInfo", "PageInfoAccessToken"]

AccessToken.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AccessToken].model_rebuild(_types_namespace=globals())
PageInfoAccessToken.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAccessToken
EXECUTABLE_SCHEMA_DIGEST = 'sha256:064914c510c1339aa3683f93c7ffb2917e1295448288270e3ed0e98c818f3d5e'
