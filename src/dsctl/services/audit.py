from __future__ import annotations

from typing import TypeAlias

from dsctl.cli_surface import AUDIT_RESOURCE
from dsctl.errors import UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    AuditData,
    optional_text,
    serialize_audit_log,
    serialize_audit_model_type,
    serialize_audit_operation_type,
)
from dsctl.services._validation import (
    optional_ds_datetime,
    require_positive_int,
    validate_ds_datetime_range,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

AuditPageData: TypeAlias = PageData[AuditData]


def list_audit_logs_result(
    *,
    model_types: list[str] | None = None,
    operation_types: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    user_name: str | None = None,
    model_name: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List audit-log rows with optional DS-native filters."""
    normalized_model_types = _filter_values(model_types, label="model type")
    normalized_operation_types = _filter_values(
        operation_types,
        label="operation type",
    )
    normalized_start = optional_ds_datetime(start, label="start")
    normalized_end = optional_ds_datetime(end, label="end")
    validate_ds_datetime_range(normalized_start, normalized_end)
    return run_with_service_runtime(
        env_file,
        _list_audit_logs_result,
        model_types=normalized_model_types,
        operation_types=normalized_operation_types,
        start=normalized_start,
        end=normalized_end,
        user_name=optional_text(user_name),
        model_name=optional_text(model_name),
        page_no=require_positive_int(page_no, label="page_no"),
        page_size=require_positive_int(page_size, label="page_size"),
        all_pages=all_pages,
    )


def list_audit_model_types_result(
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return the DS audit model-type tree."""
    return run_with_service_runtime(env_file, _list_audit_model_types_result)


def list_audit_operation_types_result(
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return the DS audit operation-type list."""
    return run_with_service_runtime(env_file, _list_audit_operation_types_result)


def _list_audit_logs_result(
    runtime: ServiceRuntime,
    *,
    model_types: tuple[str, ...],
    operation_types: tuple[str, ...],
    start: str | None,
    end: str | None,
    user_name: str | None,
    model_name: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    data: AuditPageData = requested_page_data(
        # DS audit-log list uses one generic query wrapper code here rather
        # than stable domain-specific result codes, so raw upstream errors
        # are kept intentionally.
        lambda current_page_no, current_page_size: runtime.upstream.audits.list(
            page_no=current_page_no,
            page_size=current_page_size,
            model_types=model_types or None,
            operation_types=operation_types or None,
            start_date=start,
            end_date=end,
            user_name=user_name,
            model_name=model_name,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_audit_log,
        resource=AUDIT_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
    )
    return CommandResult(
        data=require_json_object(data, label="audit list data"),
        resolved={
            "modelTypes": list(model_types),
            "operationTypes": list(operation_types),
            "start": start,
            "end": end,
            "userName": user_name,
            "modelName": model_name,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _list_audit_model_types_result(runtime: ServiceRuntime) -> CommandResult:
    payload = [
        require_json_object(
            serialize_audit_model_type(item),
            label="audit model-type data item",
        )
        for item in runtime.upstream.audits.list_model_types()
    ]
    return CommandResult(
        data=payload,
        resolved={"source": "projects/audit/audit-log-model-type"},
    )


def _list_audit_operation_types_result(runtime: ServiceRuntime) -> CommandResult:
    payload = [
        require_json_object(
            serialize_audit_operation_type(item),
            label="audit operation-type data item",
        )
        for item in runtime.upstream.audits.list_operation_types()
    ]
    return CommandResult(
        data=payload,
        resolved={"source": "projects/audit/audit-log-operation-type"},
    )


def _filter_values(
    values: list[str] | None,
    *,
    label: str,
) -> tuple[str, ...]:
    if values is None:
        return ()
    normalized_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        if not normalized:
            message = f"{label} must not be empty"
            option_name = label.replace(" ", "-")
            raise UserInputError(
                message,
                suggestion=f"Pass a non-empty --{option_name} value.",
            )
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(normalized)
    return tuple(normalized_values)


__all__ = [
    "list_audit_logs_result",
    "list_audit_model_types_result",
    "list_audit_operation_types_result",
]
