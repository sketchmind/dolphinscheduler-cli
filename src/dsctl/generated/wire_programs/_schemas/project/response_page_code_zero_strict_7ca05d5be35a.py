from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class Project(BaseEntityModel):
    id: int = Field(default=0)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    code: int = Field(default=0)
    name: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    perm: int = Field(default=0)
    defCount: int = Field(default=0)
    instRunningCount: int = Field(default=0)

class PageInfoProject(PageInfo[Project]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.Project>."""

__all__ = ["PageInfo", "Project", "PageInfoProject"]

PageInfo.model_rebuild(_types_namespace=globals())
Project.model_rebuild(_types_namespace=globals())
PageInfo[Project].model_rebuild(_types_namespace=globals())
PageInfoProject.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoProject
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7ca05d5be35add90a6f3c5d3a656b121c756d078a66f718563eb9e2ea78b0d6b'
