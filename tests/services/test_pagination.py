import pytest
from tests.fakes import FakeProject, FakeProjectPage

from dsctl.errors import ApiResultError, PermissionDeniedError, UserInputError
from dsctl.services.pagination import (
    PageData,
    PageResult,
    collect_all_pages,
    collect_page,
    requested_page_data,
)


def test_collect_page_uses_page_number_fallback_when_current_page_is_missing() -> None:
    page = FakeProjectPage(
        total_list_value=[FakeProject(code=7, name="etl-prod")],
        total=1,
        total_page_value=1,
        page_size_value=50,
        current_page_value=None,
        page_no_value=3,
    )

    result: PageResult[FakeProject] = collect_page(page)

    assert result.page_no == 3
    assert result.page_size == 50
    assert result.total == 1
    assert result.total_pages == 1
    assert result.fetched_all is False
    assert [project.name for project in result.items] == ["etl-prod"]


def test_collect_all_pages_concatenates_items_from_remaining_pages() -> None:
    pages = {
        1: FakeProjectPage(
            total_list_value=[FakeProject(code=7, name="etl-prod")],
            total=3,
            total_page_value=2,
            page_size_value=2,
            current_page_value=1,
        ),
        2: FakeProjectPage(
            total_list_value=[
                FakeProject(code=8, name="streaming"),
                FakeProject(code=9, name="adhoc"),
            ],
            total=3,
            total_page_value=2,
            page_size_value=2,
            current_page_value=2,
        ),
    }

    result: PageResult[FakeProject] = collect_all_pages(
        lambda page_no, page_size: pages[page_no],
        page_no=1,
        page_size=2,
    )

    assert result.fetched_all is True
    assert [project.name for project in result.items] == [
        "etl-prod",
        "streaming",
        "adhoc",
    ]


def test_collect_all_pages_rejects_auto_exhaust_beyond_safety_limit() -> None:
    first_page = FakeProjectPage(
        total_list_value=[FakeProject(code=7, name="etl-prod")],
        total=500,
        total_page_value=250,
        page_size_value=2,
        current_page_value=1,
    )

    def fetch_page(page_no: int, page_size: int) -> FakeProjectPage:
        del page_no, page_size
        return first_page

    with pytest.raises(UserInputError, match="safety limit") as exc_info:
        collect_all_pages(fetch_page, page_no=1, page_size=2, max_pages=10)

    assert exc_info.value.details == {
        "page_no": 1,
        "total_pages": 250,
        "max_pages": 10,
    }
    assert exc_info.value.suggestion == (
        "Retry without auto-exhaust, increase --page-size, or narrow the "
        "filter before using --all."
    )


def test_requested_page_data_renders_one_remote_page_when_all_pages_disabled() -> None:
    page = FakeProjectPage(
        total_list_value=[FakeProject(code=7, name="etl-prod")],
        total=1,
        total_page_value=1,
        page_size_value=50,
        current_page_value=3,
    )

    data: PageData[str] = requested_page_data(
        lambda page_no, page_size: page,
        page_no=3,
        page_size=50,
        all_pages=False,
        serialize_item=lambda project: project.name or "",
        resource="project",
    )

    assert data == {
        "totalList": ["etl-prod"],
        "total": 1,
        "totalPage": 1,
        "pageSize": 50,
        "currentPage": 3,
        "pageNo": 3,
    }


def test_requested_page_data_materializes_all_pages_when_enabled() -> None:
    pages = {
        1: FakeProjectPage(
            total_list_value=[FakeProject(code=7, name="etl-prod")],
            total=2,
            total_page_value=2,
            page_size_value=1,
            current_page_value=1,
        ),
        2: FakeProjectPage(
            total_list_value=[FakeProject(code=8, name="streaming")],
            total=2,
            total_page_value=2,
            page_size_value=1,
            current_page_value=2,
        ),
    }

    data: PageData[str] = requested_page_data(
        lambda page_no, page_size: pages[page_no],
        page_no=1,
        page_size=1,
        all_pages=True,
        serialize_item=lambda project: project.name or "",
        resource="project",
    )

    assert data == {
        "totalList": ["etl-prod", "streaming"],
        "total": 2,
        "totalPage": 1,
        "pageSize": 2,
        "currentPage": 1,
        "pageNo": 1,
    }


def test_requested_page_data_translates_api_result_errors() -> None:
    def fetch_page(page_no: int, page_size: int) -> FakeProjectPage:
        del page_no, page_size
        raise ApiResultError(
            result_code=30001,
            result_message="permission denied",
        )

    with pytest.raises(PermissionDeniedError, match="list requires permissions"):
        requested_page_data(
            fetch_page,
            page_no=1,
            page_size=10,
            all_pages=False,
            serialize_item=lambda project: project.name or "",
            resource="project",
            translate_error=lambda error: PermissionDeniedError(
                "project list requires permissions",
                details={"result_code": error.result_code},
            ),
        )
