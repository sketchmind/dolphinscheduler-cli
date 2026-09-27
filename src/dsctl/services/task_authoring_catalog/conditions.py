from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


def _validate_legacy_conditions_139_authored_params(
    task_params: YamlObject,
) -> None:
    """Enforce the exact single-success/single-failure TaskNode branch shape."""
    if set(task_params) != {"dependence", "conditionResult"}:
        message = (
            "CONDITIONS 1.3.9 typed authoring owns only dependence and conditionResult"
        )
        raise ValueError(message)
    raw_result = task_params.get("conditionResult")
    if not isinstance(raw_result, Mapping):
        message = "CONDITIONS conditionResult must be an object"
        raise TypeError(message)
    branches: dict[str, str] = {}
    for outcome in ("successNode", "failedNode"):
        raw_nodes = raw_result.get(outcome)
        if (
            not isinstance(raw_nodes, Sequence)
            or isinstance(raw_nodes, (bytes, bytearray, str))
            or len(raw_nodes) != 1
            or not isinstance(raw_nodes[0], str)
        ):
            message = f"CONDITIONS 1.3.9 {outcome} must contain exactly one task name"
            raise ValueError(message)
        branches[outcome] = raw_nodes[0]
    if branches["successNode"] == branches["failedNode"]:
        message = (
            "CONDITIONS 1.3.9 successNode and failedNode must name different tasks"
        )
        raise ValueError(message)
