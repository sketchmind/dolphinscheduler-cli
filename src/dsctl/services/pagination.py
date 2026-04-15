from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Protocol, TypedDict, TypeVar

from dsctl.cli_surface import resource_label
from dsctl.errors import ApiResultError, ApiTransportError, UserInputError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

ItemT = TypeVar("ItemT")
ItemT_co = TypeVar("ItemT_co", covariant=True)
OutputT = TypeVar("OutputT")

DEFAULT_PAGE_SIZE = 100
MAX_AUTO_EXHAUST_PAGES = 100


class PageData(TypedDict, Generic[ItemT]):
    """DS-style paging payload emitted by list commands."""

    totalList: list[ItemT]
    total: int
    totalPage: int
    pageSize: int
    currentPage: int
    pageNo: int


class PageRecord(Protocol[ItemT_co]):
    """Structural subset of DS paging fields used by CLI services."""

    @property
    def totalList(self) -> Sequence[ItemT_co] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


@dataclass(frozen=True)
class PageResult(Generic[ItemT]):
    """Normalized page metadata plus collected page items."""

    items: list[ItemT]
    page_no: int | None
    page_size: int | None
    total: int | None
    total_pages: int | None
    fetched_all: bool


def collect_page(page: PageRecord[ItemT]) -> PageResult[ItemT]:
    """Normalize a single generated page object."""
    return PageResult(
        items=list(_page_items(page)),
        page_no=_page_number(page),
        page_size=_page_size(page),
        total=_total(page),
        total_pages=_total_pages(page),
        fetched_all=False,
    )


def collect_all_pages(
    fetch_page: Callable[[int, int], PageRecord[ItemT]],
    *,
    page_no: int,
    page_size: int,
    max_pages: int = 100,
) -> PageResult[ItemT]:
    """Fetch all remaining pages from a paginated endpoint."""
    first_page = fetch_page(page_no, page_size)
    first_result: PageResult[ItemT] = collect_page(first_page)
    last_page = first_result.total_pages or first_result.page_no or page_no
    first_page_no = first_result.page_no or page_no
    page_count = last_page - first_page_no + 1
    if page_count > max_pages:
        message = "Refusing to auto-exhaust more pages than the safety limit"
        raise UserInputError(
            message,
            details={
                "page_no": first_page_no,
                "total_pages": last_page,
                "max_pages": max_pages,
            },
            suggestion=(
                "Retry without auto-exhaust, increase --page-size, or narrow "
                "the filter before using --all."
            ),
        )

    items = list(first_result.items)
    for current_page_no in range(first_page_no + 1, last_page + 1):
        next_page = fetch_page(current_page_no, page_size)
        items.extend(_page_items(next_page))

    return PageResult(
        items=items,
        page_no=first_result.page_no,
        page_size=first_result.page_size,
        total=first_result.total,
        total_pages=first_result.total_pages,
        fetched_all=True,
    )


def requested_page_data(
    fetch_page: Callable[[int, int], PageRecord[ItemT]],
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    serialize_item: Callable[[ItemT], OutputT],
    resource: str,
    max_pages: int = MAX_AUTO_EXHAUST_PAGES,
    translate_error: Callable[[ApiResultError], Exception] | None = None,
) -> PageData[OutputT]:
    """Fetch one requested page payload with optional auto-exhaust support."""
    try:
        if all_pages:
            page = collect_all_pages(
                fetch_page,
                page_no=page_no,
                page_size=page_size,
                max_pages=max_pages,
            )
            return materialize_page_data(
                page,
                serialize_item=serialize_item,
                resource=resource,
            )
        return render_page_data(
            fetch_page(page_no, page_size),
            serialize_item=serialize_item,
            resource=resource,
        )
    except ApiResultError as error:
        if translate_error is None:
            raise
        raise translate_error(error) from error


def render_page_data(
    page: PageRecord[ItemT],
    *,
    serialize_item: Callable[[ItemT], OutputT],
    resource: str,
) -> PageData[OutputT]:
    """Render one DS page payload into the stable CLI page shape."""
    normalized = collect_page(page)
    page_no = _require_page_value(
        normalized.page_no,
        resource=resource,
        field_name="pageNo",
    )
    total_pages = _require_page_value(
        normalized.total_pages,
        resource=resource,
        field_name="totalPage",
    )
    return {
        "totalList": [serialize_item(item) for item in page.totalList or []],
        "total": _require_page_value(
            normalized.total,
            resource=resource,
            field_name="total",
        ),
        "totalPage": total_pages,
        "pageSize": _require_page_value(
            normalized.page_size,
            resource=resource,
            field_name="pageSize",
        ),
        "currentPage": page_no,
        "pageNo": page_no,
    }


def materialize_page_data(
    page: PageResult[ItemT],
    *,
    serialize_item: Callable[[ItemT], OutputT],
    resource: str,
) -> PageData[OutputT]:
    """Materialize fetched pages into one DS-style page payload."""
    item_count = len(page.items)
    page_no = _require_page_value(
        page.page_no,
        resource=resource,
        field_name="pageNo",
    )
    page_size = _require_page_value(
        page.page_size,
        resource=resource,
        field_name="pageSize",
    )
    if item_count == 0:
        return {
            "totalList": [],
            "total": 0,
            "totalPage": 0,
            "pageSize": page_size,
            "currentPage": page_no,
            "pageNo": page_no,
        }
    return {
        "totalList": [serialize_item(item) for item in page.items],
        "total": item_count,
        "totalPage": 1,
        "pageSize": item_count,
        "currentPage": 1,
        "pageNo": 1,
    }


def _page_items(page: PageRecord[ItemT]) -> list[ItemT]:
    items = page.totalList
    return list(items or [])


def _page_number(page: PageRecord[ItemT]) -> int | None:
    current_page = page.currentPage
    if isinstance(current_page, int):
        return current_page
    page_no = page.pageNo
    if isinstance(page_no, int):
        return page_no
    return None


def _page_size(page: PageRecord[ItemT]) -> int | None:
    page_size = page.pageSize
    if isinstance(page_size, int):
        return page_size
    return None


def _total(page: PageRecord[ItemT]) -> int | None:
    total = page.total
    if isinstance(total, int):
        return total
    return None


def _total_pages(page: PageRecord[ItemT]) -> int | None:
    total_pages = page.totalPage
    if isinstance(total_pages, int):
        return total_pages
    total = _total(page)
    page_size = _page_size(page)
    if total is None or page_size is None or page_size == 0:
        return None
    quotient, remainder = divmod(total, page_size)
    return quotient if remainder == 0 else quotient + 1


def _require_page_value(
    value: int | None,
    *,
    resource: str,
    field_name: str,
) -> int:
    if value is None:
        message = (
            f"{resource_label(resource)} page payload was missing required field "
            f"{field_name!r}"
        )
        raise ApiTransportError(
            message,
            details={"resource": resource, "field": field_name},
        )
    return value
