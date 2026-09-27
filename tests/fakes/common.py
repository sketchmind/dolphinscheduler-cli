"""In-memory common collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Self, TypeVar

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


@dataclass(frozen=True)
class FakeEnumValue:
    value: str


@dataclass(frozen=True)
class FakeHttpClient:
    health_payload: JsonObject = field(default_factory=lambda: {"status": "UP"})

    def healthcheck(self) -> JsonObject:
        return dict(self.health_payload)


_PageItem = TypeVar("_PageItem")


@dataclass(frozen=True)
class _FakePage(Generic[_PageItem]):
    total_list_value: list[_PageItem] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[_PageItem] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value

    @classmethod
    def from_items(
        cls,
        items: list[_PageItem],
        *,
        page_no: int,
        page_size: int,
    ) -> Self:
        start = (page_no - 1) * page_size
        total = len(items)
        return cls(
            total_list_value=items[start : start + page_size],
            total=total,
            total_page_value=0 if total == 0 else ((total - 1) // page_size) + 1,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )
