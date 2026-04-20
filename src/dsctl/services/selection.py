from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from dsctl.errors import UserInputError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.services.runtime import ServiceRuntime

SelectionSource = Literal[
    "flag",
    "file",
    "context",
    "current_user",
    "project_preference",
    "default",
]
SelectionDataValue = int | str | None
SelectionData = dict[str, SelectionDataValue]


@dataclass(frozen=True)
class SelectedValue:
    """One resolved command input plus the source that supplied it."""

    value: str
    source: SelectionSource


def require_project_selection(
    explicit_project: str | None,
    *,
    runtime: ServiceRuntime,
) -> SelectedValue:
    """Resolve the effective project name from flag or stored context."""
    normalized_flag = _normalized_text(explicit_project)
    if normalized_flag is not None:
        return SelectedValue(value=normalized_flag, source="flag")

    normalized_context = _normalized_text(runtime.context.project)
    if normalized_context is not None:
        return SelectedValue(value=normalized_context, source="context")

    message = "Project is required; pass --project or set project context"
    raise UserInputError(
        message,
        suggestion="Pass --project NAME or run `dsctl use project NAME`.",
    )


def require_workflow_selection(
    explicit_workflow: str | None,
    *,
    runtime: ServiceRuntime,
) -> SelectedValue:
    """Resolve the effective workflow name from flag or context."""
    normalized_flag = _normalized_text(explicit_workflow)
    if normalized_flag is not None:
        return SelectedValue(value=normalized_flag, source="flag")

    normalized_context = _normalized_text(runtime.context.workflow)
    if normalized_context is not None:
        return SelectedValue(value=normalized_context, source="context")

    message = "Workflow is required; pass --workflow or set workflow context"
    raise UserInputError(
        message,
        suggestion="Pass --workflow NAME or run `dsctl use workflow NAME`.",
    )


def selected_value_data(selected: SelectedValue) -> dict[str, str]:
    """Render one scalar resolved input for the JSON envelope."""
    return {
        "value": selected.value,
        "source": selected.source,
    }


def with_selection_source(
    data: Mapping[str, SelectionDataValue],
    selected: SelectedValue,
) -> SelectionData:
    """Attach selection-source metadata to one resolved payload."""
    rendered: SelectionData = dict(data)
    rendered["source"] = selected.source
    return rendered


def _normalized_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None
