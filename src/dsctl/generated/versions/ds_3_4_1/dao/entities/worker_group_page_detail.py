from __future__ import annotations

from pydantic import Field

from ...common.enums.worker_group_source import WorkerGroupSource
from .worker_group import WorkerGroup

class WorkerGroupPageDetail(WorkerGroup):
    source: WorkerGroupSource | None = Field(default=None)

__all__ = ["WorkerGroupPageDetail"]
