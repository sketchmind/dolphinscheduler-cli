from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.id_native_reads import IdNativeReadAdapter
from tests.support import make_profile


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_every_target_version_binds_its_native_definition_identity(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    adapter = (
        IdNativeReadAdapter()
        if ds_version == "1.3.9"
        else CodeNativeReadAdapter.for_version(ds_version)
    )
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: httpx.Response(500, json={})),
    )

    with http_client:
        session = adapter.bind_read(profile, http_client=http_client)

    assert session.definitions.wire.identity_kind == (
        "id" if ds_version == "1.3.9" else "code"
    )


def test_139_exact_adapter_uses_scoped_discovery_before_detail() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        requests_seen.append((request.method, request.url.path, query))
        path = request.url.path
        if path == "/dolphinscheduler/projects/list-paging":
            data: object = _page([_project()])
        elif path.endswith("/process/list-paging"):
            data = _page([_workflow()])
        elif path.endswith("/process/select-by-id"):
            data = _workflow()
        elif path.endswith("/schedule/list-paging"):
            data = _page([_schedule()])
        else:  # pragma: no cover - assertion reports exact route drift
            message = f"unexpected request: {request.method} {path}"
            raise AssertionError(message)
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": data},
        )

    adapter = IdNativeReadAdapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        session = adapter.bind_read(profile, http_client=http_client)
        result = session.definitions.get_workflow(
            "7",
            "101",
            schedule_list_supported=False,
        )

    assert result.project.to_data() == {
        "id": 7,
        "name": "etl-prod",
        "description": "daily jobs",
    }
    assert result.workflow.to_data() == {
        "id": 101,
        "name": "daily-sync",
        "version": 3,
    }
    assert "code" not in result.view.to_data(attached_schedule=result.attached_schedule)
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/list-paging",
            {"pageSize": ["100"], "pageNo": ["1"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/etl-prod/process/list-paging",
            {"pageNo": ["1"], "pageSize": ["100"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/etl-prod/process/select-by-id",
            {"processId": ["101"]},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/etl-prod/schedule/list-paging",
            {
                "processDefinitionId": ["101"],
                "pageNo": ["1"],
                "pageSize": ["2"],
            },
        ),
    ]


def _page(items: list[dict[str, object]]) -> dict[str, object]:
    return {
        "totalList": items,
        "total": len(items),
        "totalPage": 1,
        "currentPage": 1,
    }


def _project() -> dict[str, object]:
    return {
        "id": 7,
        "userId": 1,
        "userName": "alice",
        "name": "etl-prod",
        "description": "daily jobs",
        "perm": 7,
        "defCount": 1,
    }


def _workflow() -> dict[str, object]:
    return {
        "id": 101,
        "name": "daily-sync",
        "version": 3,
        "releaseState": "ONLINE",
        "projectId": 7,
        "description": "daily workflow",
        "userId": 1,
        "userName": "alice",
        "projectName": "etl-prod",
        "scheduleReleaseState": "ONLINE",
        "timeout": 0,
    }


def _schedule() -> dict[str, object]:
    return {
        "id": 23,
        "processDefinitionId": 101,
        "processDefinitionName": "daily-sync",
        "projectName": "etl-prod",
        "startTime": "2026-01-01 00:00:00",
        "endTime": "2026-12-31 23:59:59",
        "crontab": "0 0 0 * * ?",
        "failureStrategy": "CONTINUE",
        "releaseState": "ONLINE",
        "processInstancePriority": "MEDIUM",
    }
