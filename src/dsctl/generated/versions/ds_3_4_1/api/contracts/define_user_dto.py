from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

from ...dao.model.workflow_definition_count_dto import WorkflowDefinitionCountDto

class DefineUserDto(BaseContractModel):
    count: int = Field(default=0)
    userList: list[WorkflowDefinitionCountDto] | None = Field(default=None)

__all__ = ["DefineUserDto"]
