from __future__ import annotations


from ....dao.entities.project import Project
from ..result import Result

class ProjectUpdateResponse(Result[Project]):
    """Project update response"""

__all__ = ["ProjectUpdateResponse"]
