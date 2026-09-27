from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol

from dsctl.errors import UserInputError

if TYPE_CHECKING:
    from collections.abc import Mapping

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
WorkflowInputForm = Literal["argument", "option"]


@dataclass(frozen=True)
class ResourceDefaults:
    """Project scope supplied by the selected connection context."""

    project: str | None = None


class SelectionRuntime(Protocol):
    """Minimal runtime context required for selector defaults."""

    @property
    def context(self) -> ResourceDefaults:
        """Return defaults belonging to this runtime's selected connection."""


@dataclass(frozen=True)
class SelectedValue:
    """One resolved command input plus the source that supplied it."""

    value: str
    source: SelectionSource


def require_project_selection(
    explicit_project: str | None,
    *,
    runtime: SelectionRuntime,
) -> SelectedValue:
    """Resolve the effective project name from flag or stored context."""
    explicit_value = _nonblank_text(explicit_project)
    if explicit_value is not None:
        return SelectedValue(value=explicit_value, source="flag")

    context_project = _nonblank_text(runtime.context.project)
    if context_project is not None:
        return SelectedValue(value=context_project, source="context")

    message = "Project is required; pass --project or select a context with a project"
    raise UserInputError(
        message,
        suggestion=(
            "Pass --project NAME, or configure a project in the selected context."
        ),
    )


def require_workflow_selection(
    explicit_workflow: str | None,
    *,
    input_form: WorkflowInputForm = "option",
) -> SelectedValue:
    """Require an explicit workflow identity without persistent object defaults."""
    explicit_value = _nonblank_text(explicit_workflow)
    if explicit_value is not None:
        return SelectedValue(value=explicit_value, source="flag")

    explicit_input = "WORKFLOW" if input_form == "argument" else "--workflow NAME"
    message = "Workflow is required for the selected project"
    raise UserInputError(message, suggestion=f"Pass {explicit_input}.")


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


def _nonblank_text(value: str | None) -> str | None:
    return value if value is not None and value.strip() else None
