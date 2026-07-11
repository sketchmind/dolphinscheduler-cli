from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.context import SessionContext
from dsctl.errors import UserInputError
from dsctl.services.selection import SelectedValue, require_workflow_selection

if TYPE_CHECKING:
    from dsctl.services.runtime import ServiceRuntime


def _runtime_with_context(context: SessionContext) -> ServiceRuntime:
    return cast("ServiceRuntime", SimpleNamespace(context=context))


@pytest.mark.parametrize(
    "project_selection",
    [
        SelectedValue(value="flag-project", source="flag"),
        SelectedValue(value="file-project", source="file"),
    ],
)
def test_non_context_project_does_not_reuse_context_workflow(
    project_selection: SelectedValue,
) -> None:
    runtime = _runtime_with_context(
        SessionContext(project="context-project", workflow="daily-sync")
    )

    with pytest.raises(UserInputError, match="Workflow is required") as exc_info:
        require_workflow_selection(
            None,
            runtime=runtime,
            project_selection=project_selection,
        )

    assert exc_info.value.suggestion == (
        "Pass --workflow NAME for the selected project, or remove the explicit "
        "project selection to use the complete stored context tuple."
    )
    assert exc_info.value.message == (
        "Workflow is required for the explicitly selected project"
    )


@pytest.mark.parametrize(
    ("project_selection", "expected_suggestion"),
    [
        (
            SelectedValue(value="context-project", source="context"),
            "Pass WORKFLOW or run `dsctl use workflow NAME`.",
        ),
        (
            SelectedValue(value="flag-project", source="flag"),
            (
                "Pass WORKFLOW for the selected project, or remove the explicit "
                "project selection to use the complete stored context tuple."
            ),
        ),
    ],
)
def test_workflow_argument_form_uses_positional_suggestion(
    project_selection: SelectedValue,
    expected_suggestion: str,
) -> None:
    runtime = _runtime_with_context(SessionContext(project="context-project"))

    with pytest.raises(UserInputError, match="Workflow is required") as exc_info:
        require_workflow_selection(
            None,
            runtime=runtime,
            project_selection=project_selection,
            input_form="argument",
        )

    assert exc_info.value.suggestion == expected_suggestion


def test_context_project_reuses_workflow_from_same_context_tuple() -> None:
    runtime = _runtime_with_context(
        SessionContext(project="context-project", workflow="daily-sync")
    )

    selected = require_workflow_selection(
        None,
        runtime=runtime,
        project_selection=SelectedValue(
            value="context-project",
            source="context",
        ),
    )

    assert selected == SelectedValue(value="daily-sync", source="context")


def test_explicit_workflow_is_valid_with_explicit_project() -> None:
    runtime = _runtime_with_context(
        SessionContext(project="context-project", workflow="daily-sync")
    )

    selected = require_workflow_selection(
        "flag-workflow",
        runtime=runtime,
        project_selection=SelectedValue(value="flag-project", source="flag"),
    )

    assert selected == SelectedValue(value="flag-workflow", source="flag")
