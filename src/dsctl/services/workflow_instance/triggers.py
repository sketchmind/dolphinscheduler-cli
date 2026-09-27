"""Resolve a native trigger receipt through the existing instance-list command."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.errors import (
    ApiResultError,
    PermissionDeniedError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.output import CommandResult
from dsctl.services._validation import require_positive_int
from dsctl.services.runtime import run_with_bound_domain_service_runtime
from dsctl.services.selection import require_project_selection, selected_value_data
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.runtime_instances import RUNTIME_INSTANCE_DOMAIN
from dsctl.upstream.serialization import optional_text

if TYPE_CHECKING:
    from dsctl.services.workflow_instance._types import RuntimeInstanceServiceRuntime

_TRIGGER_VERSIONS = frozenset(
    {"3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"}
)


def list_workflow_instances_by_trigger_result(
    trigger_code: int,
    *,
    project: str | None = None,
    workflow: str | None = None,
    search: str | None = None,
    executor: str | None = None,
    host: str | None = None,
    start: str | None = None,
    end: str | None = None,
    state: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Read identities for a trigger; ordinary paging/filtering does not apply."""
    normalized = require_positive_int(
        trigger_code, label="trigger_code", input_hint="--trigger-code"
    )
    filters = [
        name
        for name, value in {
            "workflow": workflow,
            "search": search,
            "executor": executor,
            "host": host,
            "start": start,
            "end": end,
            "state": state,
        }.items()
        if value is not None
    ]
    if page_no != 1:
        filters.append("page-no")
    if page_size != DEFAULT_PAGE_SIZE:
        filters.append("page-size")
    if all_pages:
        filters.append("all")
    if filters:
        message = (
            "Trigger lookup cannot be combined with ordinary filters or pagination."
        )
        raise UserInputError(
            message,
            details={"incompatible_options": filters},
            suggestion=(
                "Use --trigger-code with --project alone, "
                "then inspect the returned instance ids."
            ),
        )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _list_by_trigger,
        trigger_code=normalized,
        project=optional_text(project),
    )


def _list_by_trigger(
    runtime: RuntimeInstanceServiceRuntime, *, trigger_code: int, project: str | None
) -> CommandResult:
    if runtime.profile.ds_version not in _TRIGGER_VERSIONS:
        message = (
            "This exact DolphinScheduler version has no trigger-code instance query."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "ds_version": runtime.profile.ds_version,
                "triggerCode": trigger_code,
            },
        )
    selected = require_project_selection(project, runtime=runtime)
    workflows = runtime.domain.workflows
    if workflows is None:
        message = "Trigger lookup requires the bound workflow dependency."
        raise RuntimeError(message)
    project_ref = workflows.resolve_project(selected.value)
    try:
        ids = workflows.execution_wire.trigger_instance_ids(project_ref, trigger_code)
    except ApiResultError as exc:
        if exc.result_code in {30001, 30002}:
            message = (
                "The current user cannot inspect this project's workflow triggers."
            )
            raise PermissionDeniedError(
                message,
                details={"project": project_ref.to_data(), "triggerCode": trigger_code},
                source=exc.source,
                suggestion=(
                    "Ask an administrator to grant access to the selected project, "
                    "then retry the trigger query."
                ),
            ) from exc
        raise
    return CommandResult(
        data={
            "totalList": [{"id": identity} for identity in ids],
            "triggerCode": trigger_code,
            "instanceResolution": "resolved" if ids else "pending",
        },
        resolved={
            "project": {**project_ref.to_data(), **selected_value_data(selected)},
            "query": {
                "kind": "trigger",
                "paginated": False,
                "projection": "instance-identities",
            },
        },
    )
