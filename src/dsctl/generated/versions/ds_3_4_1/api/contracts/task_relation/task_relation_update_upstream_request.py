from __future__ import annotations

from pydantic import Field

from ..page_query_dto import PageQueryDto

class TaskRelationUpdateUpstreamRequest(PageQueryDto):
    """Task relation update request"""
    workflowCode: int = Field(default=0, description='workflow code', examples=[1234587654321])
    upstreams: str = Field(description='upstream you want to update separated by comma', examples=['12345678,87654321'])

__all__ = ["TaskRelationUpdateUpstreamRequest"]
