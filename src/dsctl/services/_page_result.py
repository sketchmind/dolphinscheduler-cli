from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from dsctl.output import CommandResult, require_json_object
from dsctl.upstream.pagination import (
    MAX_AUTO_EXHAUST_PAGES,
    requested_page_data,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.errors import ApiResultError
    from dsctl.output import JsonObject
    from dsctl.upstream.pagination import PageRecord

ItemT = TypeVar("ItemT")
OutputT = TypeVar("OutputT")


def paged_command_result(
    fetch_page: Callable[[int, int], PageRecord[ItemT]],
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    serialize_item: Callable[[ItemT], OutputT],
    resource: str,
    resolved: JsonObject | None = None,
    translate_error: Callable[[ApiResultError], Exception] | None = None,
) -> CommandResult:
    """Return one stable list result around the shared paging implementation."""
    data = requested_page_data(
        fetch_page,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_item,
        resource=resource,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=translate_error,
    )
    resolved_page: JsonObject = dict(resolved or {})
    resolved_page.update(
        {
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        }
    )
    return CommandResult(
        data=require_json_object(data, label=f"{resource} list data"),
        resolved=resolved_page,
    )


__all__ = ["paged_command_result"]
