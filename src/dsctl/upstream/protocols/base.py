from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Sequence

ClientT = TypeVar("ClientT")


class StringEnumValue(Protocol):
    """Structural enum-like value that serializes to one stable string."""

    @property
    def value(self) -> str:
        """Return the wire/value representation."""


class TaskTypeRecord(Protocol):
    """Structural task-type payload returned by favourite task discovery."""

    @property
    def taskType(self) -> str | None:  # noqa: N802
        """DS task type name."""

    @property
    def isCollection(self) -> bool:  # noqa: N802
        """Whether the current user marked the task type as favourite."""

    @property
    def taskCategory(self) -> str | None:  # noqa: N802
        """DS task category label."""


class TaskTypeOperations(Protocol):
    """Bound task-type discovery operations exposed to the service layer."""

    def list(self) -> Sequence[TaskTypeRecord]:
        """Return DS default task types plus the user's favourite flags."""
