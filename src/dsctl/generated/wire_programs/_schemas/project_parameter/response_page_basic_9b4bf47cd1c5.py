from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class ProjectParameter(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int | None = Field(default=None)
    code: int = Field(default=0)
    projectCode: int = Field(default=0)
    paramName: str | None = Field(default=None)
    paramValue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoProjectParameter(PageInfo[ProjectParameter]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.ProjectParameter>."""

__all__ = ["PageInfo", "ProjectParameter", "PageInfoProjectParameter"]

PageInfo.model_rebuild(_types_namespace=globals())
ProjectParameter.model_rebuild(_types_namespace=globals())
PageInfo[ProjectParameter].model_rebuild(_types_namespace=globals())
PageInfoProjectParameter.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoProjectParameter
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9b4bf47cd1c53bbedc664e3908ce7eb08586646ebcd703cda746723880b98302'
