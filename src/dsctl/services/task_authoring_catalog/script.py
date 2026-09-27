from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


def _validate_legacy_script_139_authored_params(
    task_params: YamlObject,
    *,
    task_type: str,
) -> None:
    """Close the authored 1.3.9 script shape around reversible resources."""
    owned_fields = {"rawScript", "localParams", "resourceList"}
    unexpected = sorted(set(task_params) - owned_fields)
    if unexpected:
        field = unexpected[0]
        message = f"{task_type} 1.3.9 typed authoring does not own {field!r}"
        raise ValueError(message)

    resources = task_params.get("resourceList", [])
    if not isinstance(resources, Sequence) or isinstance(
        resources,
        (bytes, bytearray, str),
    ):
        message = f"{task_type} resourceList must be a list"
        raise TypeError(message)
    if resources:
        message = (
            f"{task_type} 1.3.9 typed authoring requires resourceList to stay "
            "empty because the legacy full-name wire bypasses positive-ID "
            "permission checks"
        )
        raise ValueError(message)

    local_params = task_params.get("localParams", [])
    if not isinstance(local_params, Sequence) or isinstance(
        local_params,
        (bytes, bytearray, str),
    ):
        return
    names = [
        parameter.get("prop")
        for parameter in local_params
        if isinstance(parameter, Mapping) and isinstance(parameter.get("prop"), str)
    ]
    if len(names) != len(set(names)):
        message = f"{task_type} 1.3.9 localParams prop names must be unique"
        raise ValueError(message)
