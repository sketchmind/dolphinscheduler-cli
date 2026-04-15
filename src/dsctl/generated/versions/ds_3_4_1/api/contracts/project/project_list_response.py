from __future__ import annotations


from ....dao.entities.project import Project
from ..result import Result

class ProjectListResponse(Result[list[Project]]):
    """Project List response"""

__all__ = ["ProjectListResponse"]
