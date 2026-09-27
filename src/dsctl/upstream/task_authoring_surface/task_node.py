from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TaskNodeWire = Literal["legacy-process-json", "task-definition-json"]


@dataclass(frozen=True, slots=True)
class TaskNodeAuthoringSurface:
    """Exact workflow payload family and stable task fields safe to author."""

    wire: TaskNodeWire
    unavailable_fields: frozenset[str]


_LEGACY_PROCESS_TASK_NODE = TaskNodeAuthoringSurface(
    wire="legacy-process-json",
    unavailable_fields=frozenset(
        {
            "environment_code",
            "task_group_id",
            "task_group_priority",
            "delay",
            "cpu_quota",
            "memory_max",
        }
    ),
)
_TASK_DEFINITION_NODE = TaskNodeAuthoringSurface(
    wire="task-definition-json",
    unavailable_fields=frozenset(),
)
