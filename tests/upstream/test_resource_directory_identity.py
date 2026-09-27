from __future__ import annotations

from io import BytesIO
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.resources import ResourceAdapter, bind_task_resource_resolver
from tests.support import make_profile

ID_VERSIONS = tuple(
    version
    for version in TARGET_DS_VERSIONS
    if ResourceAdapter.for_version(version).task_file_uses_id
)


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _row(full_name: str) -> dict[str, object]:
    return {
        "id": 7,
        "fullName": full_name,
        "alias": full_name.rsplit("/", 1)[-1],
        "directory": full_name == "/owned",
        "type": "FILE",
        "size": 0,
    }


def _operation_response(request: httpx.Request, operation: str) -> httpx.Response:
    if "fullName" in request.url.params:
        return _success(_row(request.url.params["fullName"]))
    if operation == "delete":
        return _success(None)
    if request.method == "GET":
        return _success({"totalList": [], "total": 0, "totalPage": 0, "currentPage": 1})
    return _success({} if operation == "mkdir" else None)


@pytest.mark.parametrize("ds_version", ID_VERSIONS)
@pytest.mark.parametrize("operation", ["list", "create", "mkdir", "upload", "delete"])
def test_native_directory_identity_is_valid_for_resource_operations(
    ds_version: str, operation: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _operation_response(request, operation)

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        resources = (
            ResourceAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .resources
        )
        if operation == "list":
            resources.list(directory="/owned", page_no=1, page_size=10)
        elif operation == "create":
            resources.create_from_content(
                current_dir="/owned",
                file_name="child",
                suffix="sql",
                content="select 1;",
            )
        elif operation == "mkdir":
            resources.create_directory(current_dir="/owned", name="child")
        elif operation == "upload":
            resources.upload(
                current_dir="/owned", name="child.sql", file=BytesIO(b"select 1;")
            )
        else:
            assert resources.delete(full_name="/owned") is True

    assert requests[0].url.params["fullName"] == "/owned"
    assert requests[0].url.params["type"] == "FILE"
    assert len(requests) == (3 if operation == "create" else 2)
    writes = [
        request
        for request in requests
        if request.method != "GET" or request.url.path.endswith("/resources/delete")
    ]
    assert len(writes) == (0 if operation == "list" else 1)
    if operation in {"create", "mkdir"}:
        assert parse_qs(writes[0].content.decode())["pid"] == ["7"]
    if operation == "create":
        assert requests[-1].url.params["fullName"] == "/owned/child.sql"


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.9", "3.1.9"])
@pytest.mark.parametrize(
    "operation", ["view", "download", "task_id", "task_file", "task_name"]
)
def test_directory_remains_invalid_for_file_only_operations(
    ds_version: str, operation: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(_row("/owned"))

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        resources = (
            ResourceAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .resources
        )
        resolver = bind_task_resource_resolver(ds_version, profile, http_client=client)

        def exercise_file_operation() -> None:
            if operation == "view":
                resources.view(full_name="/owned", skip_line_num=0, limit=1)
            elif operation == "download":
                resources.download(full_name="/owned")
            elif operation == "task_id":
                resolver.resolve_id("/owned")
            elif operation == "task_file":
                resolver.resolve_task_file("/owned")
            else:
                resolver.resolve_full_name(7)

        with pytest.raises(ApiResultError, match="not a file"):
            exercise_file_operation()
    assert len(requests) == 1
