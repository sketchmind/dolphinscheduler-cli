from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, cast

from dsctl.errors import ApiTransportError
from dsctl.upstream.read_models import ReadPage

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from dsctl.upstream._generated_types import OpaqueGeneratedValue

    ProjectionErrorFactory = Callable[..., ApiTransportError]

RecordT = TypeVar("RecordT")


def project_page(
    page: OpaqueGeneratedValue,
    items: Sequence[RecordT],
    *,
    requested_page_no: int,
    requested_page_size: int,
    ds_version: str,
    resource: str,
) -> ReadPage[RecordT]:
    """Project one exact generated page into the stable paging contract."""
    total = non_negative_int_field(
        page,
        "total",
        fallback=len(items),
        ds_version=ds_version,
        resource=resource,
    )
    total_pages = non_negative_int_field(
        page,
        "totalPage",
        fallback=_page_count(total, requested_page_size),
        ds_version=ds_version,
        resource=resource,
    )
    page_no = positive_int_field(
        page,
        "currentPage",
        fallback=requested_page_no,
        ds_version=ds_version,
        resource=resource,
    )
    return ReadPage(
        totalList=items,
        total=total,
        totalPage=total_pages,
        pageSize=requested_page_size,
        currentPage=page_no,
        pageNo=page_no,
    )


def preserved_page(
    page: OpaqueGeneratedValue,
    items: Sequence[RecordT] | None,
    *,
    ds_version: str,
    resource: str,
    error_factory: ProjectionErrorFactory | None = None,
) -> ReadPage[RecordT]:
    """Preserve nullable paging metadata from an exact generated response."""

    def field(name: str) -> OpaqueGeneratedValue:
        return response_field(
            page,
            name,
            ds_version=ds_version,
            resource=resource,
            error_factory=error_factory,
        )

    return ReadPage(
        totalList=items,
        total=cast("int | None", field("total")),
        totalPage=cast("int | None", field("totalPage")),
        pageSize=cast("int | None", field("pageSize")),
        currentPage=cast("int | None", field("currentPage")),
        pageNo=cast("int | None", field("pageNo")),
    )


def sequence_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> list[OpaqueGeneratedValue]:
    """Read one generated sequence field without accepting loose mappings."""
    value = response_field(
        item,
        name,
        ds_version=ds_version,
        resource=resource,
    )
    if value is None:
        return []
    if isinstance(value, list):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="page items are not a list",
    )


def nullable_sequence_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
    error_factory: ProjectionErrorFactory | None = None,
) -> list[OpaqueGeneratedValue] | None:
    """Read a generated list field while preserving an upstream null value."""
    value = response_field(
        item,
        name,
        ds_version=ds_version,
        resource=resource,
        error_factory=error_factory,
    )
    if value is None or isinstance(value, list):
        return value
    build_error = projection_error if error_factory is None else error_factory
    raise build_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="generated page items are not a list",
    )


def optional_text_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
    missing_is_none: bool = False,
) -> str | None:
    """Read one optional canonical text field from an exact generated model."""
    try:
        value = getattr(item, name)
    except AttributeError:
        if missing_is_none:
            return None
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field=name,
            reason="generated payload is missing a canonical field",
        ) from None
    if value is None or isinstance(value, str):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="payload field is not text or null",
    )


def optional_bool_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> bool:
    """Read one canonical boolean field from an exact generated model."""
    value = response_field(
        item,
        name,
        ds_version=ds_version,
        resource=resource,
    )
    if isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="payload field is not boolean",
    )


def optional_int_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> int | None:
    """Read one optional canonical integer field without accepting booleans."""
    value = response_field(
        item,
        name,
        ds_version=ds_version,
        resource=resource,
    )
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="payload field is not an integer or null",
    )


def optional_text_sequence_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> tuple[str, ...] | None:
    """Read an optional generated list of text as an immutable sequence."""
    value = response_field(
        item,
        name,
        ds_version=ds_version,
        resource=resource,
    )
    if value is None:
        return None
    if isinstance(value, list) and all(isinstance(entry, str) for entry in value):
        return tuple(value)
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="payload field is not a text list or null",
    )


def response_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
    error_factory: ProjectionErrorFactory | None = None,
) -> OpaqueGeneratedValue:
    """Require one field from an exact generated response model."""
    try:
        return getattr(item, name)
    except AttributeError as exc:
        build_error = projection_error if error_factory is None else error_factory
        raise build_error(
            ds_version=ds_version,
            resource=resource,
            field=name,
            reason="generated payload is missing a canonical field",
        ) from exc


def non_empty_text(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
    field: str,
) -> str:
    """Require non-empty generated identity text."""
    if isinstance(value, str) and value:
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="identity must be non-empty text",
    )


def positive_int(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
    field: str,
    error_factory: ProjectionErrorFactory | None = None,
) -> int:
    """Require one positive generated identity integer."""
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    build_error = projection_error if error_factory is None else error_factory
    raise build_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="identity must be a positive integer",
    )


def require_none(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
    field: str,
) -> None:
    """Require the exact null payload returned by a void operation."""
    if value is None:
        return
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="void operation returned a non-null payload",
    )


def require_boolean(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
    field: str,
) -> bool:
    """Require one exact boolean operation result."""
    if isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="operation result is not boolean",
    )


def non_negative_int_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    fallback: int,
    ds_version: str,
    resource: str,
) -> int:
    """Read non-negative paging metadata with an evidence-backed fallback."""
    value = getattr(item, name, None)
    if value is None:
        return fallback
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="page metadata is not a non-negative integer",
    )


def positive_int_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    fallback: int,
    ds_version: str,
    resource: str,
) -> int:
    """Read positive paging metadata with an evidence-backed fallback."""
    value = getattr(item, name, None)
    if value is None:
        return fallback
    return positive_int(
        value,
        ds_version=ds_version,
        resource=resource,
        field=name,
    )


def projection_error(
    *,
    ds_version: str,
    resource: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    """Build one stable exact-response projection failure."""
    return ApiTransportError(
        f"DolphinScheduler response cannot be projected to the {resource} contract.",
        details={
            "ds_version": ds_version,
            "resource": resource,
            "field": field,
            "reason": reason,
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion="Verify DS_VERSION matches the server and inspect API health.",
    )


def _page_count(total: int, page_size: int) -> int:
    quotient, remainder = divmod(total, page_size)
    return quotient + (1 if remainder else 0)


__all__ = [
    "non_empty_text",
    "non_negative_int_field",
    "nullable_sequence_field",
    "optional_bool_field",
    "optional_int_field",
    "optional_text_field",
    "optional_text_sequence_field",
    "positive_int",
    "positive_int_field",
    "preserved_page",
    "project_page",
    "projection_error",
    "require_boolean",
    "require_none",
    "response_field",
    "sequence_field",
]
