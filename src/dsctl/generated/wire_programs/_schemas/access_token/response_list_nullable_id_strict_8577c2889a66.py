from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
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
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoAccessToken(PageInfo[AccessToken]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.AccessToken>."""

__all__ = ["AccessToken", "PageInfo", "PageInfoAccessToken"]

AccessToken.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AccessToken].model_rebuild(_types_namespace=globals())
PageInfoAccessToken.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAccessToken
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8577c2889a66552a7f1e7bcd92b97bc681bbf15785172ca7912c8569d87b196f'
