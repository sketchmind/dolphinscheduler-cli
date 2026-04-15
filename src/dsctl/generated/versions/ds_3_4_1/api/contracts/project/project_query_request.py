from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class ProjectQueryRequest(PageQueryDto):
    """Project query request"""
    searchVal: str | None = Field(default=None, examples=['pro123'])

__all__ = ["ProjectQueryRequest"]
