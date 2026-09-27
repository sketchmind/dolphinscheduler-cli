from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from dsctl.services._workflow.authoring import (
    load_selected_task_authoring_catalog,
    workflow_authoring_context,
)
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_spec import WorkflowAuthoringContext
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog


def _selected_catalog(
    *,
    catalog: TaskAuthoringCatalog | None,
    env_file: str | Path | None,
) -> TaskAuthoringCatalog:
    if catalog is not None and env_file is not None:
        message = "Pass either catalog or env_file, not both."
        raise ValueError(message)
    return (
        load_selected_task_authoring_catalog(env_file) if catalog is None else catalog
    )


def _structural_authoring_context(
    catalog: TaskAuthoringCatalog,
) -> WorkflowAuthoringContext:
    """Retain exact schedule shape while deferring independent task semantics."""
    exact = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    return replace(
        exact,
        authorize_task_type=lambda _task_type: None,
        validate_task_identity=lambda _task_type, _task_name: None,
        normalize_task_params=lambda _task_type, params: params,
        validate_global_params=lambda _params: None,
        defer_graph_validation=True,
    )
