"""Executor acceptance and instance identity are separate DolphinScheduler facts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


@dataclass(frozen=True)
class WorkflowExecutionReceipt:
    """A successful executor reply, without inferring identity from a trigger code."""

    workflow_instance_ids: tuple[int, ...] = ()
    instance_resolution: Literal["resolved", "pending", "unavailable"] = "unavailable"
    trigger_code: int | None = None

    def to_data(self) -> JsonObject:
        """Project the accepted reply while preserving unresolved identity."""
        data: JsonObject = {
            "accepted": True,
            "workflowInstanceIds": list(self.workflow_instance_ids),
            "instanceResolution": self.instance_resolution,
        }
        if self.trigger_code is not None:
            data["triggerCode"] = self.trigger_code
        return data
