from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

class UsersBatchActivateFailed(BaseViewModel):
    """AST-inferred view from generated.view.UsersServiceImpl_batchActivateUser_resValue_2."""
    sum: int | None = Field(default=None)
    info: list[None] | None = Field(default=None)

class UsersBatchActivateResult(BaseViewModel):
    """AST-inferred view from generated.view.UsersServiceImpl_batchActivateUser_res."""
    success: UsersBatchActivateSuccess | None = Field(default=None)
    failed: UsersBatchActivateFailed | None = Field(default=None)

class UsersBatchActivateSuccess(BaseViewModel):
    """AST-inferred view from generated.view.UsersServiceImpl_batchActivateUser_resValue."""
    sum: int | None = Field(default=None)
    userName: list[str] | None = Field(default=None)

__all__ = ["UsersBatchActivateFailed", "UsersBatchActivateResult", "UsersBatchActivateSuccess"]
