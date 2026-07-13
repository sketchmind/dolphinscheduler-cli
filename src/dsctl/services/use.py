from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.context import (
    ContextScope,
    SessionContext,
    clear_context,
    read_context_layer,
    resolve_context,
    update_context,
)
from dsctl.errors import ConfigError, UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._validation import require_non_empty_text

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.support.yaml_io import JsonObject

UseTarget = Literal["project", "workflow"]
UseReadback = Literal["effective", "updated_layer_fallback"]


class UseData(TypedDict):
    """JSON object emitted by `dsctl use` commands."""

    project: str | None
    workflow: str | None
    set_at: str | None


@dataclass(frozen=True)
class _UseOutput:
    """Post-write effective readback and its interpretation."""

    data: UseData
    warnings: list[str]
    warning_details: list[JsonObject]
    effective_scope: ContextScope | None
    readback: UseReadback
    shadowed: bool | None


def set_context_value_result(
    target: UseTarget,
    value: str,
    *,
    project: str | None = None,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> CommandResult:
    """Store one context value and return the merged effective context."""
    if target != "workflow" and project is not None:
        message = (
            "Explicit project binding is only supported when setting workflow context"
        )
        raise UserInputError(
            message,
            suggestion="Pass the project name as the project context value instead.",
        )
    normalized_value = require_non_empty_text(value, label=target)
    project_binding: JsonObject | None = None
    if target == "project":
        updated_context = update_context(
            project=normalized_value,
            workflow=None,
            scope=scope,
            cwd=cwd,
        )
    else:
        workflow_project: str | None
        if project is not None:
            workflow_project = require_non_empty_text(project, label="project")
            project_binding = {
                "value": workflow_project,
                "source": "flag",
            }
        elif scope == "user":
            workflow_project = read_context_layer(
                scope="user",
                cwd=cwd,
            ).project
            project_binding = {
                "value": workflow_project,
                "source": "context",
                "scope": "user",
            }
        else:
            binding_context = resolve_context(cwd=cwd)
            workflow_project = binding_context.session.project
            project_binding = {
                "value": workflow_project,
                "source": "context",
                "scope": binding_context.scope,
            }
        if workflow_project is None:
            message = "Project context is required before setting workflow context"
            raise UserInputError(
                message,
                suggestion=(
                    "Run `dsctl use workflow NAME --project PROJECT --scope "
                    f"{scope}` to bind the workflow and project atomically, or run "
                    f"`dsctl use project NAME --scope {scope}` before setting "
                    "workflow context."
                ),
            )
        updated_context = update_context(
            project=workflow_project,
            workflow=normalized_value,
            scope=scope,
            cwd=cwd,
        )
    output = _use_output(
        fallback=updated_context,
        scope=scope,
        cwd=cwd,
    )
    resolved: JsonObject = {
        "scope": scope,
        "target": target,
        "value": normalized_value,
        "updated_context": require_json_object(
            _context_data(updated_context),
            label="updated context",
        ),
        "effective_scope": output.effective_scope,
        "readback": output.readback,
        "shadowed": output.shadowed,
        "remote_validation": "not_performed",
    }
    if project_binding is not None:
        resolved["project_binding"] = project_binding
    return CommandResult(
        data=require_json_object(output.data, label="use data"),
        resolved=resolved,
        warnings=output.warnings,
        warning_details=output.warning_details,
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

    output = _use_output(
        fallback=updated_context,
        scope=scope,
        cwd=cwd,
    )
    return CommandResult(
        data=require_json_object(output.data, label="use data"),
        resolved={
            "scope": scope,
            "target": target,
            "cleared": True,
            "updated_context": require_json_object(
                _context_data(updated_context),
                label="updated context",
            ),
            "effective_scope": output.effective_scope,
            "readback": output.readback,
            "shadowed": output.shadowed,
            "remote_validation": "not_performed",
        },
        warnings=output.warnings,
        warning_details=output.warning_details,
    )


def _use_output(
    *,
    fallback: SessionContext,
    scope: ContextScope,
    cwd: Path | None,
) -> _UseOutput:
    """Return post-mutation context without turning a completed write into failure."""
    try:
        effective_context = resolve_context(cwd=cwd)
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
        return _UseOutput(
            data=_context_data(fallback),
            warnings=[message],
            warning_details=[detail],
            effective_scope=None,
            readback="updated_layer_fallback",
            shadowed=None,
        )
    shadowed = (
        scope == "user"
        and fallback.project is not None
        and effective_context.scope == "project"
    )
    warnings: list[str] = []
    warning_details: list[JsonObject] = []
    if shadowed:
        message = "User context updated, but project context remains effective."
        warnings.append(message)
        warning_details.append(
            {
                "code": "context_update_shadowed",
                "message": message,
                "scope": scope,
                "effective_scope": effective_context.scope,
                "suggestion": (
                    "Keep project context to preserve the current target; run "
                    "`dsctl use --clear --scope project` only if the updated "
                    "user context should become effective."
                ),
            }
        )
    return _UseOutput(
        data=_context_data(effective_context.session),
        warnings=warnings,
        warning_details=warning_details,
        effective_scope=effective_context.scope,
        readback="effective",
        shadowed=shadowed,
    )


def _context_data(context: SessionContext) -> UseData:
    return {
        "project": context.project,
        "workflow": context.workflow,
        "set_at": context.set_at,
    }
