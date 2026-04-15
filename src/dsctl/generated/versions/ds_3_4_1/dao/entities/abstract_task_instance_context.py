from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.context_type import ContextType

class AbstractTaskInstanceContext(BaseEntityModel):
    contextType: ContextType | None = Field(default=None)

__all__ = ["AbstractTaskInstanceContext"]
