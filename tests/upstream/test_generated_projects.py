from __future__ import annotations

from functools import partial
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.upstream.definition_models import NativeCode, NativeId
from dsctl.upstream.projects import PROJECT_DOMAIN, ProjectAdapter
from dsctl.upstream.read_models import ReadPage
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_MODERN_VERSIONS = (
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_OWNER_VERSIONS = frozenset(
    {"2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9", "3.2.0"}
)
_VOID_UPDATE_VERSIONS = frozenset({"2.0.0", "2.0.9", "3.0.0", "3.0.6"})


@pytest.mark.parametrize("ds_version", _MODERN_VERSIONS)
def test_modern_project_lifecycle_uses_exact_generated_contract(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    project = _project(code=7, name="etl-prod")
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]]]
    ] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal project
        requests_seen.append(
            (
                request.method,
                request.url.path,
                parse_qs(request.url.query.decode()),
                parse_qs(request.content.decode()),
            )
        )
        if request.method == "GET":
            created = _project(code=11, name="new-etl", description="new")
            return _success(
                {
                    "/dolphinscheduler/projects": _page(project),
                    "/dolphinscheduler/projects/7": project,
                    "/dolphinscheduler/projects/created-and-authed": [created],
                    "/dolphinscheduler/projects/11": created,
                }[request.url.path]
            )
        if request.method == "POST":
            return _success(
                17
                if ds_version == "2.0.0"
                else _project(code=11, name="new-etl", description="new")
            )
        if request.method == "PUT":
            project = _project(code=7, name="renamed-etl", description="updated")
            return _success(None if ds_version in _VOID_UPDATE_VERSIONS else project)
        if request.method == "DELETE":
            return _success(True)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ProjectAdapter.for_version(ds_version)
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        page = projects.list(page_no=2, page_size=25, search="etl")
        fetched = projects.get(code=7)
        created = projects.create(name="new-etl", description="new")
        updated = projects.update(
            code=7,
            name="renamed-etl",
            description="updated",
        )
        deleted = projects.delete(code=7)

    assert adapter.ds_version == ds_version
    assert isinstance(page, ReadPage)
    assert page.total == 1
    assert fetched.code == 7
    assert created.code == 11
    assert updated.name == "renamed-etl"
    assert deleted is True
    expected_requests = [
        (
            "GET",
            "/dolphinscheduler/projects",
            {"searchVal": ["etl"], "pageSize": ["25"], "pageNo": ["2"]},
            {},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}, {}),
        (
            "POST",
            "/dolphinscheduler/projects",
            {},
            {"projectName": ["new-etl"], "description": ["new"]},
        ),
    ]
    if ds_version == "2.0.0":
        expected_requests.extend(
            [
                ("GET", "/dolphinscheduler/projects/created-and-authed", {}, {}),
                ("GET", "/dolphinscheduler/projects/11", {}, {}),
            ]
        )
    if ds_version in _OWNER_VERSIONS:
        expected_requests.extend(
            [
                ("GET", "/dolphinscheduler/projects/7", {}, {}),
                (
                    "GET",
                    "/dolphinscheduler/projects",
                    {"searchVal": ["etl-prod"], "pageSize": ["100"], "pageNo": ["1"]},
                    {},
                ),
            ]
        )
    expected_requests.append(
        (
            "PUT",
            "/dolphinscheduler/projects/7",
            {},
            {
                "projectName": ["renamed-etl"],
                "description": ["updated"],
                **({"userName": ["admin"]} if ds_version in _OWNER_VERSIONS else {}),
            },
        )
    )
    if ds_version in _VOID_UPDATE_VERSIONS:
        expected_requests.append(("GET", "/dolphinscheduler/projects/7", {}, {}))
    expected_requests.append(("DELETE", "/dolphinscheduler/projects/7", {}, {}))
    assert requests_seen == expected_requests


@pytest.mark.parametrize(
    ("ds_version", "update_result"),
    [
        ("2.0.0", "none"),
        ("2.0.9", "none"),
        ("3.0.0", "none"),
        ("3.0.6", "none"),
        ("3.1.0", "project"),
        ("3.1.9", "project"),
        ("3.2.0", "project"),
    ],
)
def test_owner_preserving_project_update_reads_owner_from_the_exact_page(
    ds_version: str,
    update_result: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]]]
    ] = []
    project = _project(
        code=7,
        name="etl-prod",
        description="old",
        user_name="admin",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal project
        query = parse_qs(request.url.query.decode())
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, query, form))
        if request.method == "GET" and request.url.path.endswith("/projects/7"):
            detail = dict(project)
            detail["userName"] = None
            return _success(detail)
        if request.method == "GET" and request.url.path.endswith("/projects"):
            return _success(_page(project))
        if request.method == "PUT" and request.url.path.endswith("/projects/7"):
            assert form["userName"] == ["admin"]
            project = _project(
                code=7,
                name="renamed-etl",
                description="updated",
                user_name="admin",
            )
            return _success(project if update_result == "project" else None)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ProjectAdapter.for_version(ds_version)
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        updated = projects.update(
            code=7,
            name="renamed-etl",
            description="updated",
        )

    assert updated.code == 7
    assert updated.name == "renamed-etl"
    assert updated.description == "updated"
    expected_requests = [
        ("GET", "/dolphinscheduler/projects/7", {}, {}),
        (
            "GET",
            "/dolphinscheduler/projects",
            {
                "searchVal": ["etl-prod"],
                "pageSize": ["100"],
                "pageNo": ["1"],
            },
            {},
        ),
        (
            "PUT",
            "/dolphinscheduler/projects/7",
            {},
            {
                "projectName": ["renamed-etl"],
                "description": ["updated"],
                "userName": ["admin"],
            },
        ),
    ]
    if update_result == "none":
        expected_requests.append(("GET", "/dolphinscheduler/projects/7", {}, {}))
    assert requests_seen == expected_requests


def test_200_project_update_finds_the_exact_owner_on_a_later_page() -> None:
    profile = make_profile(ds_version="2.0.0")
    project = _project(
        code=7,
        name="etl-prod",
        description="old",
        user_name="admin",
    )
    page_numbers: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal project
        query = parse_qs(request.url.query.decode())
        form = parse_qs(request.content.decode())
        if request.method == "GET" and request.url.path.endswith("/projects/7"):
            detail = dict(project)
            detail["userName"] = None
            return _success(detail)
        if request.method == "GET" and request.url.path.endswith("/projects"):
            page_no = int(query["pageNo"][0])
            page_numbers.append(page_no)
            row = (
                _project(code=8, name="etl-prod-shadow", user_name="other")
                if page_no == 1
                else project
            )
            page = _page(row)
            page.update(
                {
                    "total": 2,
                    "totalPage": 2,
                    "currentPage": page_no,
                    "pageNo": page_no,
                }
            )
            return _success(page)
        if request.method == "PUT" and request.url.path.endswith("/projects/7"):
            assert form["userName"] == ["admin"]
            project = _project(
                code=7,
                name="renamed-etl",
                description="updated",
                user_name="admin",
            )
            return _success(None)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ProjectAdapter.for_version("2.0.0")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        updated = projects.update(
            code=7,
            name="renamed-etl",
            description="updated",
        )

    assert updated.name == "renamed-etl"
    assert page_numbers == [1, 2]


def test_200_project_owner_page_transport_failure_is_not_relabelled() -> None:
    profile = make_profile(ds_version="2.0.0")
    mutations = 0
    detail = _project(code=7, name="etl-prod", user_name="admin")
    detail["userName"] = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal mutations
        if request.method == "GET" and request.url.path.endswith("/projects/7"):
            return _success(detail)
        if request.method == "GET" and request.url.path.endswith("/projects"):
            message = "owner page unavailable"
            raise httpx.ConnectError(message, request=request)
        if request.method == "PUT":
            mutations += 1
            return _success(None)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ProjectAdapter.for_version("2.0.0")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            projects.update(
                code=7,
                name="renamed-etl",
                description="updated",
            )

    error = exc_info.value
    assert mutations == 0
    assert error.message != "Project update requires a non-empty current owner name"
    assert error.details.get("phase") != "precondition"
    assert "mutation_applied" not in error.details
    assert error.source is None or error.source.get("layer") != "response"


def test_200_project_owner_precondition_counts_every_page_read() -> None:
    profile = make_profile(ds_version="2.0.0")
    detail = _project(code=7, name="etl-prod", user_name="admin")
    detail["userName"] = None
    wrong_identity = _project(code=7, name="etl-prod", user_name="other")
    wrong_identity["userId"] = 99

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        if request.method == "GET" and request.url.path.endswith("/projects/7"):
            return _success(detail)
        if request.method == "GET" and request.url.path.endswith("/projects"):
            page_no = int(query["pageNo"][0])
            row = (
                _project(code=8, name="etl-prod-shadow", user_name="other")
                if page_no == 1
                else wrong_identity
            )
            page = _page(row)
            page.update(
                {
                    "total": 2,
                    "totalPage": 2,
                    "currentPage": page_no,
                    "pageNo": page_no,
                }
            )
            return _success(page)
        message = f"unexpected mutation request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ProjectAdapter.for_version("2.0.0")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            projects.update(code=7, name="renamed-etl", description="updated")

    assert exc_info.value.details["phase"] == "precondition"
    assert exc_info.value.details["mutation_applied"] is False
    assert exc_info.value.details["owner_lookup_pages"] == 2
    assert exc_info.value.details["request_budget"] == 5


def test_321_project_update_keeps_the_owner_free_exact_request() -> None:
    profile = make_profile(ds_version="3.2.1")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, form))
        assert request.method == "PUT"
        return _success(_project(code=7, name="renamed-etl", description="updated"))

    adapter = ProjectAdapter.for_version("3.2.1")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        projects.update(
            code=7,
            name="renamed-etl",
            description="updated",
        )

    assert requests_seen == [
        (
            "PUT",
            "/dolphinscheduler/projects/7",
            {"projectName": ["renamed-etl"], "description": ["updated"]},
        )
    ]


@pytest.mark.parametrize("mismatched_field", ["id", "userId"])
@pytest.mark.parametrize(
    ("ds_version", "expected_budget"),
    [
        ("2.0.0", 4),
        ("2.0.9", 4),
        ("3.0.0", 4),
        ("3.0.6", 4),
        ("3.1.0", 3),
        ("3.1.9", 3),
        ("3.2.0", 3),
    ],
)
def test_owner_preserving_project_update_rejects_a_different_page_identity(
    mismatched_field: str,
    ds_version: str,
    expected_budget: int,
) -> None:
    profile = make_profile(ds_version=ds_version)
    mutations = 0
    detail = _project(
        code=7,
        name="etl-prod",
        description="old",
        user_name="admin",
    )
    detail["userName"] = None
    page_row = _project(
        code=7,
        name="etl-prod",
        description="old",
        user_name="different-owner",
    )
    page_row[mismatched_field] = 99

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal mutations
        if request.method == "GET" and request.url.path.endswith("/projects/7"):
            return _success(detail)
        if request.method == "GET" and request.url.path.endswith("/projects"):
            return _success(_page(page_row))
        if request.method == "PUT":
            mutations += 1
            return _success(None)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ProjectAdapter.for_version(ds_version)
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            projects.update(
                code=7,
                name="renamed-etl",
                description="updated",
            )

    assert mutations == 0
    assert exc_info.value.details["phase"] == "precondition"
    assert exc_info.value.details["mutation_applied"] is False
    assert exc_info.value.details["request_budget"] == expected_budget


def test_legacy_project_domain_preserves_native_id_for_full_lifecycle() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []
    project = _legacy_project(project_id=7, name="legacy-etl", description="old")

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal project
        values = parse_qs(
            request.content.decode() or request.url.query.decode(),
            keep_blank_values=True,
        )
        requests_seen.append((request.method, request.url.path, values))
        if request.method == "POST" and request.url.path.endswith("/projects/create"):
            project = _legacy_project(
                project_id=11,
                name="new-etl",
                description="new",
            )
            return _success(11)
        if request.method == "GET" and request.url.path.endswith(
            "/projects/list-paging"
        ):
            return _success(_page(project))
        if request.method == "GET" and request.url.path.endswith(
            "/projects/query-by-id"
        ):
            return _success(project)
        if request.method == "POST" and request.url.path.endswith("/projects/update"):
            project = _legacy_project(
                project_id=7,
                name="renamed-etl",
                description="updated",
            )
            return _success(None)
        if request.method == "GET" and request.url.path.endswith("/projects/delete"):
            return _success(None)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        domain = PROJECT_DOMAIN.bind(profile, http_client=client)
        created = domain.mutations.create(name="new-etl", description="new")
        updated = domain.mutations.update(
            native=NativeId(7),
            name="renamed-etl",
            description="updated",
        )
        deleted = domain.mutations.delete(native=NativeId(7))

    assert created.ref.native == NativeId(11)
    assert "code" not in created.to_data()
    assert updated.ref.native == NativeId(7)
    assert updated.ref.name == "renamed-etl"
    assert deleted is True
    assert requests_seen == [
        (
            "POST",
            "/dolphinscheduler/projects/create",
            {"projectName": ["new-etl"], "description": ["new"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/list-paging",
            {"pageSize": ["100"], "pageNo": ["1"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/query-by-id",
            {"projectId": ["11"]},
        ),
        (
            "POST",
            "/dolphinscheduler/projects/update",
            {
                "projectId": ["7"],
                "projectName": ["renamed-etl"],
                "description": ["updated"],
            },
        ),
        (
            "GET",
            "/dolphinscheduler/projects/list-paging",
            {"pageSize": ["100"], "pageNo": ["1"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/query-by-id",
            {"projectId": ["7"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/delete",
            {"projectId": ["7"]},
        ),
    ]


def test_project_adapter_rejects_a_different_profile_before_transport() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _success({})

    profile = make_profile(ds_version="3.4.1")
    adapter = ProjectAdapter.for_version("3.4.2")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client, pytest.raises(WireContractError, match="client profile"):
        adapter.bind_projects(profile, http_client=client)

    assert calls == 0


def test_project_create_fails_closed_on_missing_critical_identity() -> None:
    profile = make_profile(ds_version="3.4.2")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        payload = _project(code=11, name="new-etl", description="new")
        payload.pop("code")
        return _success(payload)

    adapter = ProjectAdapter.for_version("3.4.2")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            projects.create(name="new-etl", description="new")

    error = exc_info.value
    assert error.details["field"] == "code"
    assert error.details["phase"] == "mutation_response"
    assert error.details["mutation_applied"] is True
    assert error.details["request_budget"] == 1


@pytest.mark.parametrize("ds_version", ["1.3.9", *_MODERN_VERSIONS])
def test_project_mutation_is_never_retried_after_http_failure(ds_version: str) -> None:
    attempts = 0
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        assert request.method == "POST"
        return httpx.Response(503, json={"message": "unavailable"})

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = PROJECT_DOMAIN.bind(profile, http_client=client).mutations
        with pytest.raises(ApiTransportError) as exc_info:
            projects.create(name="new-etl", description=None)

    assert attempts == 1
    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert "mutation_applied" not in exc_info.value.details


def test_project_delete_requires_a_result_envelope_before_returning_true() -> None:
    profile = make_profile(ds_version="3.4.2")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        return httpx.Response(200, json={"unexpected": True})

    adapter = ProjectAdapter.for_version("3.4.2")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            projects.delete(code=7)

    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.details["mutation_may_have_applied"] is True


def test_project_mutation_preserves_a_definitive_ds_result_error() -> None:
    profile = make_profile(ds_version="3.4.2")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        return httpx.Response(
            200,
            json={"code": 1001, "msg": "project is in use", "data": False},
        )

    adapter = ProjectAdapter.for_version("3.4.2")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        projects = adapter.bind_projects(profile, http_client=client)
        with pytest.raises(ApiResultError) as exc_info:
            projects.delete(code=7)

    assert exc_info.value.result_code == 1001
    assert "mutation_may_have_applied" not in exc_info.value.details


@pytest.mark.parametrize("ds_version", ["1.3.9", *_MODERN_VERSIONS])
def test_shared_project_reads_keep_numeric_identity_and_explicit_numeric_name_separate(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[tuple[str, str, dict[str, list[str]]]] = []
    legacy = ds_version == "1.3.9"
    normal = (
        _legacy_project(project_id=7, name="etl-prod", description=None)
        if legacy
        else _project(code=7, name="etl-prod", description=None)
    )
    numeric_name = (
        _legacy_project(project_id=8, name="7", description=None)
        if legacy
        else _project(code=8, name="7", description=None)
    )
    page_path = (
        "/dolphinscheduler/projects/list-paging"
        if legacy
        else "/dolphinscheduler/projects"
    )
    detail_path = (
        "/dolphinscheduler/projects/query-by-id"
        if legacy
        else "/dolphinscheduler/projects/7"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        requests.append((request.method, request.url.path, query))
        if request.url.path == page_path:
            payload = _page(numeric_name if query.get("searchVal") == ["7"] else normal)
            payload.update({"currentPage": 1, "pageNo": 1})
            return _success(payload)
        if (
            query.get("projectId") == ["8"]
            or request.url.path == "/dolphinscheduler/projects/8"
        ):
            return _success(numeric_name)
        assert request.url.path == detail_path
        return _success(normal)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        definitions = PROJECT_DOMAIN.bind(profile, http_client=client).definitions
        page = definitions.list_projects(
            page_no=2, page_size=25, search="etl", all_pages=False
        )
        numeric = definitions.get_project("7")
        named_numeric = definitions.resolve_project_by_name("7")
        named = definitions.get_project("etl-prod")

    assert [item.ref.name for item in page.totalList] == ["etl-prod"]
    assert numeric.project.native == (NativeId(7) if legacy else NativeCode(7))
    assert numeric.view.ref.description is None
    assert named_numeric.native == (NativeId(8) if legacy else NativeCode(8))
    assert named_numeric.name == "7"
    assert named.project == numeric.project
    detail_query = {"projectId": ["7"]} if legacy else {}
    expected = [
        ("GET", page_path, {"searchVal": ["etl"], "pageNo": ["2"], "pageSize": ["25"]})
    ]
    if legacy:
        expected.append(("GET", page_path, {"pageNo": ["1"], "pageSize": ["100"]}))
    expected.extend(
        [
            ("GET", detail_path, detail_query),
            (
                "GET",
                page_path,
                {"searchVal": ["7"], "pageNo": ["1"], "pageSize": ["100"]},
            ),
            (
                "GET",
                "/dolphinscheduler/projects/query-by-id"
                if legacy
                else "/dolphinscheduler/projects/8",
                {"projectId": ["8"]} if legacy else {},
            ),
            (
                "GET",
                page_path,
                {"searchVal": ["etl-prod"], "pageNo": ["1"], "pageSize": ["100"]},
            ),
            ("GET", detail_path, detail_query),
        ]
    )
    assert requests == expected


@pytest.mark.parametrize(
    ("failure", "phase", "field", "expected_paths"),
    [
        ("invalid-id", "mutation_response", "id", ["/projects"]),
        (
            "missing",
            "locate",
            "created-and-authed",
            ["/projects", "/projects/created-and-authed"],
        ),
        (
            "duplicate",
            "locate",
            "created-and-authed",
            ["/projects", "/projects/created-and-authed"],
        ),
        (
            "wrong-name",
            "locate",
            "created-and-authed",
            ["/projects", "/projects/created-and-authed"],
        ),
        (
            "wrong-code",
            "readback",
            "code",
            ["/projects", "/projects/created-and-authed", "/projects/11"],
        ),
        (
            "wrong-id",
            "readback",
            "id",
            ["/projects", "/projects/created-and-authed", "/projects/11"],
        ),
    ],
)
def test_200_project_create_stops_at_the_failed_identity_verification_phase(
    failure: str,
    phase: str,
    field: str,
    expected_paths: list[str],
) -> None:
    profile = make_profile(ds_version="2.0.0")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return _success(0 if failure == "invalid-id" else 17)
        project = _project(code=11, name="new-etl", description="new")
        if request.url.path.endswith("/created-and-authed"):
            if failure == "missing":
                return _success([])
            if failure == "duplicate":
                return _success([project, project])
            if failure == "wrong-name":
                project["name"] = "other"
            return _success([project])
        if failure == "wrong-code":
            project["code"] = 12
        if failure == "wrong-id":
            project["id"] = 18
        return _success(project)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        projects = ProjectAdapter.for_version("2.0.0").bind_projects(
            profile, http_client=client
        )
        with pytest.raises(ApiTransportError) as caught:
            projects.create(name="new-etl", description="new")

    assert caught.value.details["phase"] == phase
    assert caught.value.details["field"] == field
    assert caught.value.details["mutation_applied"] is True
    assert caught.value.details["request_budget"] == 3
    assert [request.url.path for request in requests] == [
        "/dolphinscheduler" + path for path in expected_paths
    ]
    assert [request.method for request in requests] == [
        "POST",
        *(["GET"] * (len(expected_paths) - 1)),
    ]


def test_200_project_create_locate_and_readback_retry_only_gets() -> None:
    profile = make_profile(ds_version="2.0.0").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests: list[tuple[str, str]] = []
    attempts: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        requests.append((request.method, path))
        attempts[path] = attempts.get(path, 0) + 1
        if request.method == "POST":
            return _success(17)
        if attempts[path] == 1:
            return httpx.Response(503, text="unavailable")
        project = _project(code=11, name="new-etl", description=None)
        return _success([project] if path.endswith("/created-and-authed") else project)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        created = PROJECT_DOMAIN.bind(profile, http_client=client).mutations.create(
            name="new-etl", description=None
        )

    assert created.ref.native == NativeCode(11)
    assert created.id == 17
    assert requests == [
        ("POST", "/dolphinscheduler/projects"),
        ("GET", "/dolphinscheduler/projects/created-and-authed"),
        ("GET", "/dolphinscheduler/projects/created-and-authed"),
        ("GET", "/dolphinscheduler/projects/11"),
        ("GET", "/dolphinscheduler/projects/11"),
    ]


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.0", "2.0.9", "3.4.2"])
@pytest.mark.parametrize("description", [None, ""])
def test_project_create_preserves_null_versus_empty_description(
    ds_version: str,
    description: str | None,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []
    project = (
        _legacy_project(project_id=17, name="new-etl", description=description)
        if ds_version == "1.3.9"
        else _project(code=11, name="new-etl", description=description)
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return _success(17 if ds_version in {"1.3.9", "2.0.0"} else project)
        if request.url.path.endswith("/created-and-authed"):
            return _success([project])
        if request.url.path.endswith("/list-paging"):
            return _success(_page(project))
        return _success(project)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        created = PROJECT_DOMAIN.bind(profile, http_client=client).mutations.create(
            name="new-etl", description=description
        )

    assert created.ref.description == description
    assert parse_qs(requests[0].content.decode(), keep_blank_values=True) == {
        "projectName": ["new-etl"],
        **({"description": [""]} if description == "" else {}),
    }
    assert sum(request.method == "POST" for request in requests) == 1


@pytest.mark.parametrize("ds_version", ["2.0.0", "3.2.0", "3.4.2"])
def test_project_list_keeps_exact_nullability_for_both_read_ports(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success({"totalList": None, "total": 0})

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        projects = ProjectAdapter.for_version(ds_version).bind_projects(
            profile, http_client=client
        )
        definitions = PROJECT_DOMAIN.bind(profile, http_client=client).definitions
        if ds_version == "2.0.0":
            direct = projects.list(page_no=1, page_size=25)
            shared = definitions.list_projects(
                page_no=1, page_size=25, search=None, all_pages=False
            )
            assert direct.totalList is None
            assert direct.total == 0
            assert shared.totalList == ()
            assert shared.total == 0
        else:
            with pytest.raises(
                ApiTransportError, match="generated API contract"
            ) as direct_error:
                projects.list(page_no=1, page_size=25)
            with pytest.raises(
                ApiTransportError, match="generated API contract"
            ) as shared_error:
                definitions.list_projects(
                    page_no=1, page_size=25, search=None, all_pages=False
                )
            assert direct_error.value.details["validation_error_count"] == 1
            assert shared_error.value.details["validation_error_count"] == 1
    assert [request.method for request in requests] == ["GET", "GET"]


@pytest.mark.parametrize(
    ("ds_version", "method"),
    [("1.3.9", "GET"), ("2.0.0", "DELETE"), ("3.4.2", "DELETE")],
)
def test_project_delete_is_once_only_with_the_exact_native_method(
    ds_version: str,
    method: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503, text="unavailable")

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        projects = PROJECT_DOMAIN.bind(profile, http_client=client).mutations
        native = NativeId(7) if ds_version == "1.3.9" else NativeCode(7)
        with pytest.raises(ApiTransportError) as caught:
            projects.delete(native=native)

    assert [request.method for request in requests] == [method]
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True
    assert caught.value.details["request_replay_safe"] is False
    assert caught.value.suggestion == (
        "Inspect the project before deciding whether to retry project delete; "
        "do not blindly repeat the mutation."
    )


def test_legacy_project_delete_response_loss_never_replays() -> None:
    profile = make_profile(ds_version="1.3.9").model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []
    existing_ids = {7}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/projects/delete"
        assert parse_qs(request.url.query.decode()) == {"projectId": ["7"]}
        assert len(requests) == 1, "must not replay or read back an uncertain deletion"
        existing_ids.remove(7)
        message = "response lost after deletion"
        raise httpx.ReadTimeout(message, request=request)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        PROJECT_DOMAIN.bind(profile, http_client=client).mutations.delete(
            native=NativeId(7)
        )

    assert existing_ids == set()
    assert len(requests) == 1
    assert caught.value.error_type == "api_transport_error"
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True
    assert caught.value.details["request_replay_safe"] is False
    assert caught.value.details["attempts"] == 1
    assert caught.value.details["max_attempts"] == 1
    assert caught.value.suggestion == (
        "Inspect the project before deciding whether to retry project delete; "
        "do not blindly repeat the mutation."
    )


def test_legacy_project_delete_keeps_optional_envelope_and_ignores_response_data() -> (
    None
):
    profile = make_profile(ds_version="1.3.9")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"unexpected": True})

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        deleted = PROJECT_DOMAIN.bind(profile, http_client=client).mutations.delete(
            native=NativeId(7)
        )

    assert deleted is True
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/delete")
    ]
    assert parse_qs(requests[0].url.query.decode()) == {"projectId": ["7"]}


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _project(
    *,
    code: int,
    name: str,
    description: str | None = "daily jobs",
    project_id: int = 17,
    user_name: str = "admin",
) -> dict[str, object]:
    return {
        "id": project_id,
        "userId": 1,
        "userName": user_name,
        "code": code,
        "name": name,
        "description": description,
        "perm": 7,
        "defCount": 0,
    }


def _legacy_project(
    *,
    project_id: int,
    name: str,
    description: str | None,
) -> dict[str, object]:
    return {
        "id": project_id,
        "userId": 1,
        "userName": "admin",
        "name": name,
        "description": description,
        "perm": 7,
        "defCount": 0,
    }


def _page(project: dict[str, object]) -> dict[str, object]:
    return {
        "totalList": [project],
        "total": 1,
        "totalPage": 1,
        "pageSize": 25,
        "currentPage": 2,
        "pageNo": 2,
    }


@pytest.mark.parametrize("operation", ["get", "list"])
def test_project_read_keeps_lifecycle_projection_diagnostics(operation: str) -> None:
    profile = make_profile(ds_version="3.4.2")
    invalid = _project(code=0, name="etl-prod")
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(
            lambda request: _success(_page(invalid) if operation == "list" else invalid)
        ),
    )
    with client:
        projects = ProjectAdapter.for_version("3.4.2").bind_projects(
            profile, http_client=client
        )
        read = (
            partial(projects.list, page_no=1, page_size=20)
            if operation == "list"
            else partial(projects.get, code=7)
        )
        with pytest.raises(ApiTransportError) as captured:
            read()

    assert captured.value.message == (
        "DolphinScheduler response cannot be projected to the project contract."
    )
    assert captured.value.details == {
        "ds_version": "3.4.2",
        "resource": "project",
        "field": "code",
        "reason": "project identity must be a positive integer",
    }
    assert captured.value.suggestion == (
        "Verify DS_VERSION matches the server and inspect API health."
    )
    assert captured.value.__cause__ is None
