from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.context import ContextScope, clear_context, load_context, update_context
from dsctl.output import CommandResult, require_json_object
from dsctl.services._validation import require_non_empty_text

if TYPE_CHECKING:
    from pathlib import Path

UseTarget = Literal["project", "workflow"]


class UseData(TypedDict):
    """JSON object emitted by `dsctl use` commands."""

    project: str | None
    workflow: str | None
    set_at: str | None


def set_context_value_result(
    target: UseTarget,
    value: str,
    *,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> CommandResult:
    """Store one context value and return the merged effective context."""
    normalized_value = require_non_empty_text(value, label=target)
    if target == "project":
        update_context(
            project=normalized_value,
            workflow=None,
            scope=scope,
            cwd=cwd,
        )
    else:
        update_context(workflow=normalized_value, scope=scope, cwd=cwd)
    return CommandResult(
        data=require_json_object(_use_data(cwd=cwd), label="use data"),
        resolved={
            "scope": scope,
            "target": target,
            "value": normalized_value,
        },
    )


def clear_context_result(
    *,
    target: UseTarget | None = None,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> CommandResult:
    """Clear one context value or the whole selected scope."""
    if target is None:
        clear_context(scope=scope, cwd=cwd)
    elif target == "project":
        update_context(project=None, workflow=None, scope=scope, cwd=cwd)
    else:
        update_context(workflow=None, scope=scope, cwd=cwd)

    return CommandResult(
        data=require_json_object(_use_data(cwd=cwd), label="use data"),
        resolved={
            "scope": scope,
            "target": target,
            "cleared": True,
        },
    )


def _use_data(*, cwd: Path | None = None) -> UseData:
    context = load_context(cwd=cwd)
    return {
        "project": context.project,
        "workflow": context.workflow,
        "set_at": context.set_at,
    }
