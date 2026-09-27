"""In-memory task types collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FakeTaskType:
    task_type_value: str | None
    is_collection_value: bool = False
    task_category_value: str | None = None

    @property
    def taskType(self) -> str | None:  # noqa: N802
        return self.task_type_value

    @property
    def isCollection(self) -> bool:  # noqa: N802
        return self.is_collection_value

    @property
    def taskCategory(self) -> str | None:  # noqa: N802
        return self.task_category_value


@dataclass
class FakeTaskTypeAdapter:
    task_types: list[FakeTaskType]

    def list(self) -> list[FakeTaskType]:
        return list(self.task_types)


def empty_task_type_adapter() -> FakeTaskTypeAdapter:
    return FakeTaskTypeAdapter(task_types=[])
