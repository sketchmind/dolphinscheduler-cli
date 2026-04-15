from __future__ import annotations


from ....dao.entities.project import Project
from ..result import Result

class ProjectQueryResponse(Result[Project]):
    """Project query response"""

__all__ = ["ProjectQueryResponse"]
