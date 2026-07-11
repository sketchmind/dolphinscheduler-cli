from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.context import (
    ContextScope,
    SessionContext,
    clear_context,
    load_context,
    update_context,
)
from dsctl.errors import ConfigError, UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._validation import require_non_empty_text

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.support.yaml_io import JsonObject

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
        updated_context = update_context(
            project=normalized_value,
            workflow=None,
            scope=scope,
            cwd=cwd,
        )
    else:
        effective_context = load_context(cwd=cwd)
        if effective_context.project is None:
            message = "Project context is required before setting workflow context"
            raise UserInputError(
                message,
                suggestion=(
                    f"Run `dsctl use project NAME --scope {scope}` before setting "
                    "workflow context."
                ),
            )
        updated_context = update_context(
            project=effective_context.project,
            workflow=normalized_value,
            scope=scope,
            cwd=cwd,
        )
    data, warnings, warning_details = _use_output(
        fallback=updated_context,
        scope=scope,
        cwd=cwd,
    )
    return CommandResult(
        data=require_json_object(data, label="use data"),
        resolved={
            "scope": scope,
            "target": target,
            "value": normalized_value,
        },
        warnings=warnings,
        warning_details=warning_details,
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
        updated_context = SessionContext()
    elif target == "project":
        updated_context = update_context(
            project=None,
            workflow=None,
            scope=scope,
            cwd=cwd,
        )
    else:
        updated_context = update_context(workflow=None, scope=scope, cwd=cwd)

    data, warnings, warning_details = _use_output(
        fallback=updated_context,
        scope=scope,
        cwd=cwd,
    )
    return CommandResult(
        data=require_json_object(data, label="use data"),
        resolved={
            "scope": scope,
            "target": target,
            "cleared": True,
        },
        warnings=warnings,
        warning_details=warning_details,
    )


def _use_data(*, cwd: Path | None = None) -> UseData:
    return _context_data(load_context(cwd=cwd))


def _use_output(
    *,
    fallback: SessionContext,
    scope: ContextScope,
    cwd: Path | None,
) -> tuple[UseData, list[str], list[JsonObject]]:
    """Return post-mutation context without turning a completed write into failure."""
    try:
        return _use_data(cwd=cwd), [], []
    except ConfigError as error:
        message = (
            "Context layer updated, but another persisted context layer is invalid."
        )
        detail: JsonObject = {
            "code": "context_layer_invalid_after_update",
            "message": message,
            "scope": scope,
            "error": error.to_payload(),
            "suggestion": error.suggestion,
        }
        return _context_data(fallback), [message], [detail]


def _context_data(context: SessionContext) -> UseData:
    return {
        "project": context.project,
        "workflow": context.workflow,
        "set_at": context.set_at,
    }
