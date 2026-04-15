from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class WorkflowInstanceQueryRequest(PageQueryDto):
    """Workflow instance request"""
    projectName: str | None = Field(default=None)
    workflowName: str | None = Field(default=None)
    host: str | None = Field(default=None)
    startTime: str | None = Field(default=None, alias='startDate')
    endTime: str | None = Field(default=None, alias='endDate')
    state: int | None = Field(default=None)

__all__ = ["WorkflowInstanceQueryRequest"]
