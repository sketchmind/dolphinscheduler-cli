from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

class ScheduleInsertResult(BaseViewModel):
    """AST-inferred view from generated.view.SchedulerServiceImpl_insertSchedule_result."""
    scheduleId: int | None = Field(default=None)

__all__ = ["ScheduleInsertResult"]
