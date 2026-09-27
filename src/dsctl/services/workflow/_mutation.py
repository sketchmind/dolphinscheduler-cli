from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

from dsctl.errors import (
    ApiResultError,
)
from dsctl.output import (
    build_dry_run_request,
    require_json_object,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.services._parameter_warnings import (
        ParameterWarningDetail,
    )
    from dsctl.services.workflow._types import (
        _PreparedWorkflowT,
    )
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.wire import WireRequest


def _workflow_wire_request_data(
    request: WireRequest,
    *,
    label: str,
) -> JsonObject:
    _require_workflow_wire_without_content(request, label=label)
    return build_dry_run_request(
        method=request.method,
        path=request.path,
        params=_workflow_wire_query(request, label=f"{label} query"),
        json_body=request.json,
        form_data=_workflow_wire_form(request, label=f"{label} form"),
    )


def _require_workflow_wire_without_content(
    request: WireRequest,
    *,
    label: str,
) -> None:
    if request.content is None:
        return
    message = f"{label} unexpectedly emitted a raw content body"
    raise RuntimeError(message)


def _workflow_wire_query(
    request: WireRequest,
    *,
    label: str,
) -> JsonObject | None:
    if request.query is None:
        return None
    return require_json_object(dict(request.query), label=label)


def _workflow_wire_form(
    request: WireRequest,
    *,
    label: str,
) -> JsonObject | None:
    if request.form is None:
        return None
    return require_json_object(dict(request.form), label=label)


def _prepare_workflow_definition_request(
    *,
    prepare: Callable[[], _PreparedWorkflowT],
    translate_error: Callable[[ApiResultError], NoReturn],
) -> _PreparedWorkflowT:
    """Prepare one definition request with stable pre-mutation error typing."""
    try:
        return prepare()
    except ApiResultError as error:
        translate_error(error)


def _parameter_expression_warning_json_details(
    details: list[ParameterWarningDetail],
) -> list[JsonObject]:
    return [
        require_json_object(
            detail,
            label="parameter expression warning detail",
        )
        for detail in details
    ]
