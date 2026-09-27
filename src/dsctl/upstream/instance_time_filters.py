"""Reviewed start-time predicates from all 36 exact instance mapper pairs.

Evidence: ProcessInstanceMapper/TaskInstanceMapper query paging predicates and
BaseService.checkAndParseDateParameters, tags 1.3.9 through 3.4.2. See
docs/development/luna-cli-efficiency-review.md for the source audit boundaries.
These predicates describe selection, never historical execution-state snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypedDict

from dsctl.errors import UserInputError

InstanceListAction = Literal["workflow-instance.list", "task-instance.list"]


class InstanceTimeFilterData(TypedDict):
    """Public selection semantics shared by schema and result metadata."""

    field: str
    interval: str
    supports_single_bound: bool
    state_observation: str
    atomic_snapshot: bool


@dataclass(frozen=True, slots=True)
class InstanceTimeFilterContract:
    """Exact source predicate, without a claimed server version."""

    lower_inclusive: bool
    supports_single_bound: bool

    def to_data(self) -> InstanceTimeFilterData:
        """Describe the source interval without changing supplied timestamps."""
        return {
            "field": "start_time",
            "interval": "[start,end]" if self.lower_inclusive else "(start,end]",
            "supports_single_bound": self.supports_single_bound,
            "state_observation": "current_at_read",
            "atomic_snapshot": False,
        }

    def validate(self, *, start: str | None, end: str | None) -> None:
        """Reject a single bound that the reviewed mapper cannot apply correctly."""
        if self.supports_single_bound or (start is None) == (end is None):
            return
        message = (
            "This target's instance list requires both --start and --end "
            "for time filtering."
        )
        raise UserInputError(
            message,
            details={
                "reason": "instance_time_filter_requires_both_bounds",
                "field": "start_time",
                "start": start,
                "end": end,
                "interval": self.to_data()["interval"],
            },
            suggestion=(
                "Pass both --start and --end in YYYY-MM-DD HH:MM:SS format, or "
                "omit both. These bounds select start time; returned states are "
                "current observations, not historical failure states."
            ),
        )


_LEGACY = frozenset(
    {
        "1.3.9",
        *(f"2.0.{patch}" for patch in range(10)),
        *(f"3.0.{patch}" for patch in range(7)),
    }
)
_MODERN = frozenset(
    {
        *(f"3.1.{patch}" for patch in range(10)),
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_CLOSED_LEGACY_WORKFLOW = frozenset(f"3.0.{patch}" for patch in range(1, 7))


def instance_time_filter_contract(
    version: str,
    action: InstanceListAction,
) -> InstanceTimeFilterContract:
    """Return only reviewed exact start-time semantics; future tags fail closed."""
    if version in _MODERN:
        return InstanceTimeFilterContract(True, True)
    if version in _LEGACY:
        return InstanceTimeFilterContract(
            action == "workflow-instance.list" and version in _CLOSED_LEGACY_WORKFLOW,
            False,
        )
    message = f"No reviewed instance time-filter contract for {version}"
    raise ValueError(message)


__all__ = ["InstanceTimeFilterContract", "instance_time_filter_contract"]
