from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import FakeProject, FakeProjectAdapter, fake_service_runtime
from tests.support import make_profile

from dsctl.errors import UserInputError
from dsctl.services import project as project_service
from dsctl.services import runtime as runtime_service


def _install_project_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeProjectAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            adapter,
            profile=make_profile(),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_list_projects_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=1, name="alpha"),
            FakeProject(code=2, name="beta"),
            FakeProject(code=3, name="gamma"),
        ]
    )
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.list_projects_result(page_size=2)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert {
        "total": data["total"],
        "totalPage": data["totalPage"],
        "pageSize": data["pageSize"],
        "currentPage": data["currentPage"],
        "pageNo": data["pageNo"],
    } == {
        "total": 3,
        "totalPage": 2,
        "pageSize": 2,
        "currentPage": 1,
        "pageNo": 1,
    }
    assert list(items) == [
        {
            "id": None,
            "userId": None,
            "userName": None,
            "code": 1,
            "name": "alpha",
            "description": None,
            "createTime": None,
            "updateTime": None,
            "perm": 0,
            "defCount": 0,
        },
        {
            "id": None,
            "userId": None,
            "userName": None,
            "code": 2,
            "name": "beta",
            "description": None,
            "createTime": None,
            "updateTime": None,
            "perm": 0,
            "defCount": 0,
        },
    ]


def test_list_projects_result_can_auto_exhaust_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=1, name="alpha"),
            FakeProject(code=2, name="beta"),
            FakeProject(code=3, name="gamma"),
        ]
    )
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.list_projects_result(page_size=2, all_pages=True)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert [_mapping(item)["name"] for item in items] == ["alpha", "beta", "gamma"]
    assert data["total"] == 3
    assert data["totalPage"] == 1
    assert data["pageSize"] == 3
    assert data["currentPage"] == 1
    assert data["pageNo"] == 1


def test_get_project_result_resolves_name_then_fetches_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.get_project_result("etl-prod")
    data = _mapping(result.data)

    assert result.resolved == {
        "project": {"code": 7, "name": "etl-prod", "description": None}
    }
    assert data["code"] == 7
    assert data["name"] == "etl-prod"


def test_create_project_result_returns_created_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[])
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.create_project_result(
        name="demo",
        description="test project",
    )
    data = _mapping(result.data)

    assert data["code"] == 1
    assert data["name"] == "demo"
    assert data["description"] == "test project"


def test_update_project_result_preserves_existing_name_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod", description="before")]
    )
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.update_project_result(
        "etl-prod",
        description="after",
    )
    data = _mapping(result.data)

    assert data["code"] == 7
    assert data["name"] == "etl-prod"
    assert data["description"] == "after"


def test_update_project_result_can_clear_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod", description="before")]
    )
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.update_project_result(
        "etl-prod",
        description=None,
    )
    data = _mapping(result.data)

    assert data["description"] is None


def test_update_project_result_requires_a_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="requires at least one field change"):
        project_service.update_project_result("etl-prod")


def test_delete_project_result_requires_force(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="requires --force"):
        project_service.delete_project_result("etl-prod", force=False)


def test_delete_project_result_reports_deleted_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])
    _install_project_service_fakes(monkeypatch, adapter)

    result = project_service.delete_project_result("etl-prod", force=True)
    data = _mapping(result.data)

    assert data == {
        "deleted": True,
        "project": {"code": 7, "name": "etl-prod", "description": None},
    }
    assert adapter.projects == []
