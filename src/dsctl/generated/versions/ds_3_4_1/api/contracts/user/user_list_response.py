from __future__ import annotations


from ....dao.entities.user import User
from ..result import Result

class UserListResponse(Result[list[User]]):
    """User List response"""

__all__ = ["UserListResponse"]
