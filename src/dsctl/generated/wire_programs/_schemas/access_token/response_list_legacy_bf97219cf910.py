from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class AccessToken(BaseEntityModel):
    id: int = Field(default=0)
    userId: int = Field(default=0)
    token: str | None = Field(default=None)
    expireTime: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userName: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

class PageInfoAccessToken(PageInfo[AccessToken]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.AccessToken>."""

__all__ = ["AccessToken", "PageInfo", "PageInfoAccessToken"]

AccessToken.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AccessToken].model_rebuild(_types_namespace=globals())
PageInfoAccessToken.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAccessToken
EXECUTABLE_SCHEMA_DIGEST = 'sha256:bf97219cf9103ba7c97b7c1cad9d4b5ed238e6feb3c9b7d7b9153f853f977382'
