from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.services.selection import (
    SelectedValue,
    SelectionRuntime,
    require_project_selection,
)
from dsctl.upstream.definition_models import NativeCode
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from dsctl.output import JsonObject
    from dsctl.upstream.definition_models import ProjectRef
    from dsctl.upstream.definition_reads import DefinitionReads


def resolve_code_project(
    explicit_project: str | None,
    *,
    runtime: SelectionRuntime,
    definitions: DefinitionReads,
) -> tuple[SelectedValue, ProjectRef, int]:
    """Resolve one project for features introduced after code identities."""
    selected = require_project_selection(explicit_project, runtime=runtime)
    project = definitions.resolve_project(selected.value)
    if not isinstance(project.native, NativeCode):
        message = "Project-scoped configuration requires a code-native DS profile"
        raise WireContractError(message)
    return selected, project, project.native.value


def selected_project_data(
    project: ProjectRef,
    selected: SelectedValue,
) -> JsonObject:
    """Render a resolved code-native project plus its selection source."""
    data = project.to_data()
    data["source"] = selected.source
    return data


__all__ = ["resolve_code_project", "selected_project_data"]
