from __future__ import annotations

from typing import TypeAlias
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.projects import ProjectAdapter
from tests.support import make_profile

RequestValues: TypeAlias = dict[str, list[str]]
RequestRecord: TypeAlias = tuple[str, str, RequestValues, RequestValues]


def test_project_name_read_and_mutations_keep_exact_http_exchanges() -> None:
    profile = make_profile(ds_version="3.4.2")
    requests_seen: list[RequestRecord] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query, form = _record_request(requests_seen, request)
        assert request.headers["token"] == profile.api_token
        method_path = request.method, request.url.path
        if method_path == ("GET", "/dolphinscheduler/projects"):
            assert query == {
                "searchVal": ["etl-prod"],
                "pageSize": ["100"],
                "pageNo": ["1"],
            }
            data: object = _project_page(page_size=100)
        elif method_path == ("GET", "/dolphinscheduler/projects/7"):
            data = _project_payload(
                code=7,
                name="etl-prod",
                description="daily jobs",
            )
        elif method_path in {
            ("POST", "/dolphinscheduler/projects"),
            ("PUT", "/dolphinscheduler/projects/8"),
        }:
            data = _project_payload(
                code=8,
                name=form["projectName"][0],
                description=form["description"][0],
            )
        elif method_path == ("DELETE", "/dolphinscheduler/projects/8"):
            data = None
        else:  # pragma: no cover - assertion reports unexpected wire drift
            message = f"unexpected request: {request.method} {request.url.path}"
            raise AssertionError(message)
        return _success_response(data)

    read_adapter = CodeNativeReadAdapter("3.4.2")
    project_adapter = ProjectAdapter("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        reads = read_adapter.bind_read(profile, http_client=http_client)
        projects = project_adapter.bind_projects(profile, http_client=http_client)
        resolved = reads.definitions.get_project("etl-prod")
        created = projects.create(name="new-project", description="created")
        updated = projects.update(
            code=8,
            name="renamed-project",
            description="updated",
        )
        deleted = projects.delete(code=8)

    assert resolved.project.native.value == 7
    assert resolved.project.name == "etl-prod"
    assert (created.code, created.name) == (8, "new-project")
    assert (updated.code, updated.name) == (8, "renamed-project")
    assert deleted is True
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects",
            {"searchVal": ["etl-prod"], "pageSize": ["100"], "pageNo": ["1"]},
            {},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}, {}),
        (
            "POST",
            "/dolphinscheduler/projects",
            {},
            {"projectName": ["new-project"], "description": ["created"]},
        ),
        (
            "PUT",
            "/dolphinscheduler/projects/8",
            {},
            {"projectName": ["renamed-project"], "description": ["updated"]},
        ),
        ("DELETE", "/dolphinscheduler/projects/8", {}, {}),
    ]


def test_numeric_project_selector_uses_one_exact_detail_exchange() -> None:
    profile = make_profile(ds_version="3.4.2")
    requests_seen: list[RequestRecord] = []

    def handler(request: httpx.Request) -> httpx.Response:
        _record_request(requests_seen, request)
        assert request.headers["token"] == profile.api_token
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/projects/7"
        return _success_response(
            _project_payload(
                code=7,
                name="etl-prod",
                description="daily jobs",
            )
        )

    adapter = CodeNativeReadAdapter("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        project = adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions.get_project("7")

    assert project.project.native.value == 7
    assert project.project.name == "etl-prod"
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7", {}, {}),
    ]


@pytest.mark.parametrize(
    ("version", "route_family", "detail_field", "relations_field"),
    [
        (
            "3.2.2",
            "process-definition",
            "processDefinition",
            "processTaskRelationList",
        ),
        (
            "3.4.2",
            "workflow-definition",
            "workflowDefinition",
            "workflowTaskRelationList",
        ),
    ],
)
def test_workflow_read_fails_closed_when_exact_detail_is_null(
    version: str,
    route_family: str,
    detail_field: str,
    relations_field: str,
) -> None:
    profile = make_profile(ds_version=version)
    requests_seen: list[RequestRecord] = []

    def handler(request: httpx.Request) -> httpx.Response:
        _record_request(requests_seen, request)
        assert request.headers["token"] == profile.api_token
        assert request.method == "GET"
        if request.url.path == "/dolphinscheduler/projects/7":
            return _success_response(
                _project_payload(
                    code=7,
                    name="etl-prod",
                    description="daily jobs",
                )
            )
        assert request.url.path == (f"/dolphinscheduler/projects/7/{route_family}/101")
        return _success_response(
            {
                detail_field: None,
                relations_field: [],
                "taskDefinitionList": [],
            }
        )

    adapter = CodeNativeReadAdapter(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        reads = adapter.bind_read(
            profile,
            http_client=http_client,
        )
        with pytest.raises(ApiTransportError) as captured:
            reads.definitions.resolve_workflow("7", "101")

    assert captured.value.details == {
        "ds_version": version,
        "resource": "workflow",
        "field": detail_field,
        "reason": "generated detail field is null",
    }
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7",
            {},
            {},
        ),
        (
            "GET",
            f"/dolphinscheduler/projects/7/{route_family}/101",
            {},
            {},
        ),
    ]


def _record_request(
    requests_seen: list[RequestRecord],
    request: httpx.Request,
) -> tuple[RequestValues, RequestValues]:
    query = parse_qs(request.url.query.decode())
    form = parse_qs(request.content.decode()) if request.content else {}
    requests_seen.append((request.method, request.url.path, query, form))
    return query, form


def _success_response(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _project_payload(*, code: int, name: str, description: str) -> dict[str, object]:
    return {
        "id": code,
        "code": code,
        "name": name,
        "description": description,
        "perm": 7,
        "defCount": 0,
    }


def _project_page(*, page_size: int) -> dict[str, object]:
    return {
        "totalList": [
            _project_payload(code=7, name="etl-prod", description="daily jobs")
        ],
        "total": 1,
        "totalPage": 1,
        "pageSize": page_size,
        "currentPage": 1,
    }
