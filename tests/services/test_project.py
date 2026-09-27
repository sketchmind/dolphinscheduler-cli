import shlex
from functools import partial
from typing import Literal, NoReturn

import pytest
from tests.fakes import (
    FakeNativeProjectMutations,
    FakeProject,
    FakeProjectAdapter,
    fake_bound_domain_service_runtime,
    fake_project_definitions,
    fake_read_service_runtime,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.services import project as project_service
from dsctl.services import runtime as runtime_service
from dsctl.upstream.projects import PROJECT_DOMAIN, ProjectDomain


def _install_project_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeProjectAdapter,
) -> None:
    def read_runtime_factory(
        *,
        env_file: str | None = None,
        cwd: object = None,
    ) -> object:
        del env_file, cwd
        return fake_read_service_runtime(
            adapter,
            profile=make_profile(),
        )

    monkeypatch.setattr(
        runtime_service,
        "open_read_service_runtime",
        read_runtime_factory,
    )

    def project_runtime_factory(
        domain: object,
        *,
        env_file: str | None = None,
        cwd: object = None,
    ) -> object:
        del env_file, cwd
        assert domain is PROJECT_DOMAIN
        return fake_bound_domain_service_runtime(
            ProjectDomain(
                definitions=fake_project_definitions(adapter),
                mutations=FakeNativeProjectMutations(adapter),
            ),
            profile=make_profile(),
        )

    monkeypatch.setattr(
        runtime_service,
        "open_bound_domain_service_runtime",
        project_runtime_factory,
    )


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


@pytest.mark.parametrize("operation", ["list", "get"])
@pytest.mark.parametrize("result_code", [30001, 30002])
def test_project_reads_translate_permission_denied_during_paging_or_name_lookup(
    monkeypatch: pytest.MonkeyPatch,
    operation: Literal["list", "get"],
    result_code: int,
) -> None:
    adapter = FakeProjectAdapter(projects=[])
    source_error = ApiResultError(
        result_code=result_code,
        result_message="upstream permission denied",
    )
    requests: list[str | None] = []

    def fail_page(
        *, page_no: int, page_size: int, search: str | None = None
    ) -> NoReturn:
        assert page_no == 1
        assert page_size > 0
        requests.append(search)
        raise source_error

    monkeypatch.setattr(adapter, "list", fail_page)
    _install_project_service_fakes(monkeypatch, adapter)
    read_project = (
        project_service.list_projects_result
        if operation == "list"
        else partial(project_service.get_project_result, "etl-prod")
    )

    with pytest.raises(PermissionDeniedError) as captured:
        read_project()

    expected_details = {"resource": "project", "operation": operation}
    if operation == "get":
        expected_details["name"] = "etl-prod"
    assert requests == [None if operation == "list" else "etl-prod"]
    assert captured.value.details == expected_details
    assert captured.value.suggestion == (
        "Ask a DolphinScheduler administrator or project owner to grant "
        "the required project permission, then retry."
    )
    assert captured.value.__cause__ is source_error


def test_get_project_result_translates_refresh_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])

    def fail_get(*, code: int) -> FakeProject:
        del code
        raise ApiResultError(
            result_code=30002,
            result_message="user has no project operation privilege",
        )

    monkeypatch.setattr(adapter, "get", fail_get)
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(PermissionDeniedError) as exc_info:
        project_service.get_project_result("etl-prod")

    assert exc_info.value.details["operation"] == "get"
    assert exc_info.value.details["code"] == 7


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


def test_create_project_result_translates_existing_name_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[])

    def fail_create(*, name: str, description: str | None = None) -> FakeProject:
        del description
        raise ApiResultError(
            result_code=10019,
            result_message=f"project {name} already exists",
        )

    monkeypatch.setattr(adapter, "create", fail_create)
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError) as exc_info:
        project_service.create_project_result(name="demo")

    assert exc_info.value.details == {
        "resource": "project",
        "operation": "create",
        "name": "demo",
    }
    assert exc_info.value.suggestion == (
        "Run `dsctl project list --search demo` to inspect the existing project, "
        "then retry with a unique --name."
    )


def test_create_project_conflict_suggestion_quotes_opaque_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[])
    name = "quarterly ETL; echo injected"

    def fail_create(*, name: str, description: str | None = None) -> FakeProject:
        del description
        raise ApiResultError(
            result_code=10019,
            result_message=f"project {name} already exists",
        )

    monkeypatch.setattr(adapter, "create", fail_create)
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError) as exc_info:
        project_service.create_project_result(name=name)

    suggestion = exc_info.value.suggestion
    assert suggestion is not None
    command = suggestion.removeprefix("Run `").split("`", maxsplit=1)[0]
    assert shlex.split(command) == [
        "dsctl",
        "project",
        "list",
        "--search",
        name,
    ]


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


def test_update_project_result_translates_project_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])

    def fail_update(
        *,
        code: int,
        name: str,
        description: str | None = None,
    ) -> FakeProject:
        del code, name, description
        raise ApiResultError(
            result_code=30003,
            result_message="user does not have write permission",
        )

    monkeypatch.setattr(adapter, "update", fail_update)
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(PermissionDeniedError) as exc_info:
        project_service.update_project_result("etl-prod", description="updated")

    assert exc_info.value.details["operation"] == "update"
    assert exc_info.value.details["code"] == 7
    assert "grant" in (exc_info.value.suggestion or "")


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


def test_delete_project_result_translates_nonempty_project_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])

    def fail_delete(*, code: int) -> bool:
        del code
        raise ApiResultError(
            result_code=10137,
            result_message="delete workflow definitions first",
        )

    monkeypatch.setattr(adapter, "delete", fail_delete)
    _install_project_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError) as exc_info:
        project_service.delete_project_result("etl-prod", force=True)

    assert exc_info.value.details["operation"] == "delete"
    assert exc_info.value.suggestion == (
        "Delete every workflow in the project, then retry project deletion."
    )
