from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.definition_models import (
        ProjectRef,
        WorkflowRef,
    )


def _scoped_workflow_command(
    action: str,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    extra: Sequence[str] = (),
) -> str:
    """Render one executable command from exact resolved workflow selectors."""
    return shlex.join(
        (
            "dsctl",
            "workflow",
            action,
            str(workflow.native.value),
            "--project",
            str(project.native.value),
            *extra,
        )
    )


def _scoped_related_command(
    resource: str,
    action: str,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    extra: Sequence[str] = (),
) -> str:
    """Render one executable related-resource command from resolved selectors."""
    return shlex.join(
        (
            "dsctl",
            resource,
            action,
            "--project",
            str(project.native.value),
            "--workflow",
            str(workflow.native.value),
            *extra,
        )
    )
