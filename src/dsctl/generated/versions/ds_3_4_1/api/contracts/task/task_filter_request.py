from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class TaskFilterRequest(PageQueryDto):
    """Task query request"""
    projectName: str | None = Field(default=None, examples=['project-name'])
    name: str | None = Field(default=None, examples=['task-name'])
    taskType: str | None = Field(default=None)

__all__ = ["TaskFilterRequest"]
