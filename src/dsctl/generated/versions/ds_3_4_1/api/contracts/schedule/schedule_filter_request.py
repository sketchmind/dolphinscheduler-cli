from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class ScheduleFilterRequest(PageQueryDto):
    """Schedule query request"""
    projectName: str | None = Field(default=None, examples=['project-name'])
    workflowDefinitionName: str | None = Field(default=None, examples=['workflow-definition-name'])
    releaseState: str | None = Field(default=None, description='default OFFLINE if value not provide.', json_schema_extra={'allowable_values': ['ONLINE', 'OFFLINE']})

__all__ = ["ScheduleFilterRequest"]
