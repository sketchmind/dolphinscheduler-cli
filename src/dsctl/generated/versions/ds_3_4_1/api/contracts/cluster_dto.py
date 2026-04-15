from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class ClusterDto(BaseContractModel):
    id: int = Field(default=0)
    code: int | None = Field(default=None, description='cluster code')
    name: str | None = Field(default=None, description='cluster name')
    config: str | None = Field(default=None, description='config content')
    description: str | None = Field(default=None)
    workflowDefinitions: list[str] | None = Field(default=None)
    operator: int | None = Field(default=None, description='operator user id')
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ClusterDto"]
