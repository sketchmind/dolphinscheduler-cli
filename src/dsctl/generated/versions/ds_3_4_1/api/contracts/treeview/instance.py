from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class Instance(BaseContractModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None, description='node name')
    code: int = Field(default=0, description='node code')
    type: str | None = Field(default=None, description='node type')
    state: str | None = Field(default=None, description='node status')
    startTime: str | None = Field(default=None, description='node start time')
    endTime: str | None = Field(default=None, description='node end time')
    host: str | None = Field(default=None, description='node running on which host')
    duration: str | None = Field(default=None, description='node duration')
    subflowCode: int = Field(default=0)

__all__ = ["Instance"]
