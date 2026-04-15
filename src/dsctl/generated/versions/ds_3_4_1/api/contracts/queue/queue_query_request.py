from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class QueueQueryRequest(PageQueryDto):
    """Queue query request"""
    searchVal: str | None = Field(default=None, examples=['queue11'])

__all__ = ["QueueQueryRequest"]
