from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

from ..._schemas._enums.enum_042208af0f3d7ce654cafe8b2a2ecbf82779bf76ad54923af79663905b70d26d import ResourceType as ResourceType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class StorageEntity(BaseContractModel):
    id: int = Field(default=0)
    fullName: str | None = Field(default=None)
    fileName: str | None = Field(default=None)
    alias: str | None = Field(default=None)
    pfullName: str | None = Field(default=None)
    isDirectory: bool = Field(default=False, alias='directory')
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    type: ResourceType | None = Field(default=None)
    size: int = Field(default=0)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoStorageEntity(PageInfo[StorageEntity]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.plugin.storage.api.StorageEntity>."""

__all__ = ["ResourceType", "PageInfo", "StorageEntity", "PageInfoStorageEntity"]

PageInfo.model_rebuild(_types_namespace=globals())
StorageEntity.model_rebuild(_types_namespace=globals())
PageInfo[StorageEntity].model_rebuild(_types_namespace=globals())
PageInfoStorageEntity.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:f2954ab323d27ad91943354a7f45e424a2dacde26e9d2239815fe91fafa745e9'

RESPONSE_TYPE = PageInfoStorageEntity
EXECUTABLE_SCHEMA_DIGEST = 'sha256:5046d69cdce308e9316b1c80b9160524fd68c364b98b49e454c62ab8710e8a7d'
