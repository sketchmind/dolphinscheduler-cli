from __future__ import annotations


from ....dao.entities.project import Project
from ..result import Result

class ProjectCreateResponse(Result[Project]):
    """Project create response"""

__all__ = ["ProjectCreateResponse"]
