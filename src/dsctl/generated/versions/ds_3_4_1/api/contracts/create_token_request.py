from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class CreateTokenRequest(BaseContractModel):
    userId: int = Field(examples=[1])
    expireTime: str = Field(examples=['2022-12-31 00:00:00'])
    token: str | None = Field(default=None, examples=['abc123xyz'])

__all__ = ["CreateTokenRequest"]
