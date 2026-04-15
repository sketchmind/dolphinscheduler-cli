from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class WorkflowFilterRequest(PageQueryDto):
    """Workflow query response"""
    projectName: str | None = Field(default=None, examples=['project-name'])
    workflowName: str | None = Field(default=None, examples=['workflow-name'])
    releaseState: str | None = Field(default=None, examples=['ONLINE / OFFLINE'])
    scheduleReleaseState: str | None = Field(default=None, examples=['ONLINE / OFFLINE'])

__all__ = ["WorkflowFilterRequest"]
