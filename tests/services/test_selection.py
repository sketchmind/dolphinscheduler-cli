from types import SimpleNamespace

import pytest

from dsctl.errors import UserInputError
from dsctl.services.selection import (
    ResourceDefaults,
    SelectedValue,
    WorkflowInputForm,
    require_project_selection,
    require_workflow_selection,
)


def test_project_defaults_belong_to_selected_context_and_explicit_input_wins() -> None:
    runtime = SimpleNamespace(context=ResourceDefaults(project="analytics"))
    assert require_project_selection(None, runtime=runtime) == SelectedValue(
        "analytics", "context"
    )
    assert require_project_selection("finance", runtime=runtime) == SelectedValue(
        "finance", "flag"
    )
    assert runtime.context.project == "analytics"


def test_missing_project_has_actionable_explicit_selection() -> None:
    runtime = SimpleNamespace(context=ResourceDefaults())
    with pytest.raises(UserInputError) as error:
        require_project_selection(None, runtime=runtime)
    assert error.value.suggestion is not None
    assert "--project NAME" in error.value.suggestion


@pytest.mark.parametrize("workflow", [None, "", "   "])
@pytest.mark.parametrize(
    ("input_form", "suggestion"),
    [("argument", "Pass WORKFLOW."), ("option", "Pass --workflow NAME.")],
)
def test_missing_workflow_requires_explicit_identity(
    workflow: str | None, input_form: WorkflowInputForm, suggestion: str
) -> None:
    with pytest.raises(UserInputError, match="Workflow is required") as error:
        require_workflow_selection(workflow, input_form=input_form)
    assert error.value.suggestion == suggestion


@pytest.mark.parametrize("workflow", ["daily-etl", "17", "prod's workflow", " daily "])
def test_workflow_names_remain_opaque(workflow: str) -> None:
    assert require_workflow_selection(workflow) == SelectedValue(workflow, "flag")


def test_project_name_whitespace_is_preserved() -> None:
    runtime = SimpleNamespace(context=ResourceDefaults(project=" analytics "))
    assert require_project_selection(None, runtime=runtime).value == " analytics "
    assert require_project_selection(" finance ", runtime=runtime).value == " finance "
