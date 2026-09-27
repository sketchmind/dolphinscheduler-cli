from __future__ import annotations

from io import BytesIO
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, NotFoundError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.services._task_resource_refs import resolve_task_resource_refs
from dsctl.upstream import resources as resource_wire
from dsctl.upstream.resources import ResourceAdapter, ResourceDomain
from dsctl.upstream.wire import WireResponseDecodeError
from tests.support import make_profile


@pytest.mark.parametrize(
    "ds_version",
    [
        "1.3.9",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.4.1",
    ],
)
def test_task_resource_resolver_is_bound_for_every_reviewed_resource_epoch(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _: _success(None)),
    )

    with http_client:
        resolver = resource_wire.bind_task_resource_resolver(
            ds_version,
            profile,
            http_client=http_client,
        )

    assert resolver is not None


@pytest.mark.parametrize(
    "ds_version",
    ["3.2.0", "3.2.1", "3.2.2", "3.3.2"],
)
def test_name_backed_task_resource_resolver_verifies_one_exact_visible_file(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        if request.url.path.endswith("/resources/base-dir"):
            return _success(
                "/tenant/resources"
                if request.url.params["type"] == "FILE"
                else "/tenant/udfs"
            )
        return _success(
            {
                "totalList": [
                    {
                        "id": 71,
                        "fullName": "/tenant/resources/ml/train.py",
                        "directory": False,
                        "type": "FILE",
                        "size": 20,
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            }
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        resolver = resource_wire.bind_task_resource_resolver(
            ds_version,
            profile,
            http_client=http_client,
        )
        refs = resolve_task_resource_refs(
            resolver,
            ["/ml/train.py"],
            boundary_resource="workflow",
            action="create",
        )

    assert refs.verified_full_names == frozenset({"/ml/train.py"})
    assert dict(refs.id_by_full_name) == {}
    assert dict(refs.wire_full_name_by_full_name) == {
        "/ml/train.py": "/tenant/resources/ml/train.py"
    }
    storage_path_epoch = ds_version.startswith("3.2.")
    assert len(requests_seen) == (3 if storage_path_epoch else 2)
    assert requests_seen[0].url.path.endswith("/resources/base-dir")
    assert dict(requests_seen[0].url.params) == {"type": "FILE"}
    page_request_index = 1
    if storage_path_epoch:
        assert requests_seen[1].url.path.endswith("/resources/base-dir")
        assert dict(requests_seen[1].url.params) == {"type": "UDF"}
        page_request_index = 2
    assert dict(requests_seen[page_request_index].url.params) == {
        "fullName": "/tenant/resources/ml",
        "type": "FILE",
        **({"tenantCode": ""} if storage_path_epoch else {}),
        "searchVal": "train.py",
        "pageNo": "1",
        "pageSize": "100",
    }


def test_name_backed_task_resource_resolver_rejects_a_directory() -> None:
    profile = make_profile(ds_version="3.3.2")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/resources/base-dir"):
            return _success("/tenant/resources")
        return _success(
            {
                "totalList": [
                    {
                        "fullName": "/tenant/resources/ml/train.py",
                        "directory": True,
                        "type": "FILE",
                        "size": 0,
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            }
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        resolver = resource_wire.bind_task_resource_resolver(
            "3.3.2",
            profile,
            http_client=http_client,
        )
        with pytest.raises(NotFoundError, match="not found or visible"):
            resolve_task_resource_refs(
                resolver,
                ["/ml/train.py"],
                boundary_resource="workflow",
                action="create",
            )


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.2.2"])
def test_32_task_resource_resolver_rejects_an_admin_all_resource_base(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success("/data/resources")

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        resolver = resource_wire.bind_task_resource_resolver(
            ds_version,
            profile,
            http_client=http_client,
        )
        with pytest.raises(ApiTransportError) as captured:
            resolve_task_resource_refs(
                resolver,
                ["/ml/train.py"],
                boundary_resource="workflow",
                action="create",
            )

    assert captured.value.details["field"] == "baseDir"
    assert "administrator ALL-resource root" in str(captured.value.details["reason"])
    assert [request.url.params["type"] for request in requests_seen] == [
        "FILE",
        "UDF",
    ]


def test_32_task_resource_resolver_accepts_object_key_sibling_bases() -> None:
    profile = make_profile(ds_version="3.2.2")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/resources/base-dir"):
            return _success(
                "ds/tenant/resources/"
                if request.url.params["type"] == "FILE"
                else "ds/tenant/udfs/"
            )
        return _success(
            {
                "totalList": [
                    {
                        "fullName": "ds/tenant/resources/ml/train.py",
                        "directory": False,
                        "type": "FILE",
                        "size": 20,
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            }
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        resolver = resource_wire.bind_task_resource_resolver(
            "3.2.2",
            profile,
            http_client=http_client,
        )
        resolution = resolver.resolve_task_file("/ml/train.py")

    assert resolution.wire_full_name == "ds/tenant/resources/ml/train.py"


@pytest.mark.parametrize(
    ("file_base", "udf_base"),
    [
        ("/tenant-a/resources", "/tenant-b/udfs"),
        ("/tenant/resources", "/tenant/udf"),
        ("resources", "udfs"),
    ],
)
def test_32_task_resource_resolver_rejects_ambiguous_base_pairs(
    file_base: str,
    udf_base: str,
) -> None:
    profile = make_profile(ds_version="3.2.2")

    def handler(request: httpx.Request) -> httpx.Response:
        return _success(file_base if request.url.params["type"] == "FILE" else udf_base)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        resolver = resource_wire.bind_task_resource_resolver(
            "3.2.2",
            profile,
            http_client=http_client,
        )
        with pytest.raises(ApiTransportError) as captured:
            resolver.resolve_task_file("/ml/train.py")

    assert "administrator ALL-resource" in str(captured.value.details["reason"])


def test_341_resource_domain_keeps_json_multipart_and_binary_wires_distinct() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        if request.method == "GET" and request.url.path.endswith("/resources/base-dir"):
            return _success("/tenant/resources")
        if request.method == "GET" and request.url.path.endswith("/resources"):
            return _success(
                {
                    "totalList": [
                        {
                            "alias": "demo.sql",
                            "userName": "admin",
                            "fileName": "demo.sql",
                            "fullName": "/tenant/resources/demo.sql",
                            "directory": False,
                            "type": "FILE",
                            "size": 20,
                            "createTime": None,
                            "updateTime": None,
                        }
                    ],
                    "total": 1,
                    "totalPage": 1,
                    "currentPage": 1,
                }
            )
        if request.method == "GET" and request.url.path.endswith("/resources/download"):
            return httpx.Response(
                200,
                content=b"select 1;\nselect 2;\n",
                headers={"content-type": "text/plain; charset=utf-8"},
            )
        return _success(None)

    adapter = ResourceAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, ResourceDomain)
        resources = domain.resources
        assert resources.base_dir() == "/tenant/resources"
        page = resources.list(
            directory="/tenant/resources",
            page_no=1,
            page_size=10,
        )
        assert page.total == 1
        assert page.totalList is not None
        assert page.totalList[0].fullName == "/tenant/resources/demo.sql"
        resources.create_from_content(
            current_dir="/tenant/resources",
            file_name="notes",
            suffix="sql",
            content="select 42;",
        )
        resources.create_directory(
            current_dir="/tenant/resources",
            name="archive",
        )
        resources.upload(
            current_dir="/tenant/resources",
            name="upload.sql",
            file=BytesIO(b"select 3;\n"),
        )
        viewed = resources.view(
            full_name="/tenant/resources/demo.sql",
            skip_line_num=1,
            limit=1,
        )
        downloaded = resources.download(full_name="/tenant/resources/demo.sql")
        assert resources.delete(full_name="/tenant/resources/demo.sql") is True

    assert viewed.content == "select 2;"
    assert downloaded.content == b"select 1;\nselect 2;\n"
    methods_and_paths = [
        (request.method, request.url.path.rsplit("/dolphinscheduler/", 1)[-1])
        for request in requests_seen
    ]
    assert methods_and_paths == [
        ("GET", "resources/base-dir"),
        ("GET", "resources"),
        ("POST", "resources/online-create"),
        ("POST", "resources/directory"),
        ("POST", "resources"),
        ("GET", "resources/download"),
        ("GET", "resources/download"),
        ("DELETE", "resources"),
    ]
    list_request = requests_seen[1]
    assert dict(list_request.url.params) == {
        "fullName": "/tenant/resources",
        "type": "FILE",
        "searchVal": "",
        "pageNo": "1",
        "pageSize": "10",
    }
    assert parse_qs(requests_seen[2].content.decode()) == {
        "type": ["FILE"],
        "fileName": ["notes"],
        "suffix": ["sql"],
        "content": ["select 42;"],
        "currentDir": ["/tenant/resources"],
    }
    assert "multipart/form-data" in requests_seen[4].headers["content-type"]
    assert b'filename="upload.sql"' in requests_seen[4].content
    assert dict(requests_seen[5].url.params) == {
        "fullName": "/tenant/resources/demo.sql"
    }


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_reviewed_resource_epochs_preserve_selector_and_raw_transport_wires(  # noqa: C901
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []
    legacy = ds_version == "1.3.9"
    id_backed = ds_version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }
    storage = ds_version in {"3.2.0", "3.2.1", "3.2.2"}
    collection = "/resources/list-paging" if legacy else "/resources"

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        path = request.url.path
        if path.endswith("/resources/base-dir"):
            return _success("/base/")
        if (
            path.endswith(("/resources/queryResource", "/resources/-1", "/resources/7"))
            and request.method == "GET"
        ):
            return _success(
                _resource_row(
                    request.url.params.get("fullName", "/base/folder/demo.sql")
                )
            )
        if path.endswith(collection) and request.method == "GET":
            return _success(
                {
                    "totalList": [_resource_row()],
                    "total": 1,
                    "totalPage": 1,
                    "currentPage": 2,
                }
            )
        if path.endswith("/download"):
            return httpx.Response(
                200,
                content=b"line 1\nline 2\n",
                headers={"content-type": "text/plain"},
            )
        if path.endswith("/view"):
            return _success({"alias": "demo.sql", "content": "window"})
        if "multipart/form-data" in request.headers.get("content-type", ""):
            return _success("upload data intentionally ignored")
        if path.endswith("/resources/delete"):
            return _success("delete data intentionally ignored")
        if request.method == "POST" and id_backed and path.endswith("/online-create"):
            return _success(None)
        if request.method == "POST" and id_backed:
            return _success({})
        return _success(None)

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        operations = (
            ResourceAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .resources
        )
        base_dir = operations.base_dir()
        page = operations.list(
            directory="/base/folder",
            page_no=2,
            page_size=5,
            search=None,
        )
        viewed = operations.view(
            full_name="/base/folder/demo.sql",
            skip_line_num=1,
            limit=1,
        )
        operations.create_from_content(
            current_dir="/base/folder",
            file_name="notes",
            suffix="sql",
            content="select 1;",
        )
        operations.create_directory(
            current_dir="/base/folder",
            name="archive",
        )
        operations.upload(
            current_dir="/base/folder",
            name="upload.sql",
            file=BytesIO(b"select 2;"),
        )
        downloaded = operations.download(full_name="/base/folder/demo.sql")
        assert operations.delete(full_name="/base/folder/demo.sql") is True

    assert base_dir == ("/" if id_backed else "/base")
    assert page.currentPage == 2
    expected_view = (
        "window" if id_backed or storage or ds_version == "3.4.3" else "line 2"
    )
    assert viewed.content == expected_view
    assert downloaded.content == b"line 1\nline 2\n"
    page_request = next(
        request
        for request in requests_seen
        if request.method == "GET" and request.url.path.endswith(collection)
    )
    expected_page = {"type": "FILE", "pageNo": "2", "pageSize": "5"}
    expected_page.update({"id": "7"} if id_backed else {"fullName": "/base/folder"})
    if storage:
        expected_page["tenantCode"] = ""
    elif not id_backed:
        expected_page["searchVal"] = ""
    assert dict(page_request.url.params) == expected_page
    create_request = next(
        request
        for request in requests_seen
        if request.url.path.endswith("/online-create")
    )
    expected_directory = "/base/folder/" if storage else "/base/folder"
    create_form = {
        "type": ["FILE"],
        "fileName": ["notes"],
        "suffix": ["sql"],
        "content": ["select 1;"],
        "currentDir": [expected_directory],
    }
    if id_backed:
        create_form["pid"] = ["7"]
    assert (
        parse_qs(create_request.content.decode(), keep_blank_values=True) == create_form
    )
    mkdir_request = next(
        request
        for request in requests_seen
        if request.url.path.endswith("/directory/create" if legacy else "/directory")
    )
    mkdir_form = {
        "type": ["FILE"],
        "name": ["archive"],
        "currentDir": [expected_directory],
    }
    if id_backed or storage:
        mkdir_form["pid"] = ["7" if id_backed else "-1"]
    assert (
        parse_qs(mkdir_request.content.decode(), keep_blank_values=True) == mkdir_form
    )
    uploaded = next(
        request
        for request in requests_seen
        if "multipart/form-data" in request.headers.get("content-type", "")
    )
    assert uploaded.method == "POST"
    assert uploaded.url.path.endswith("/resources/create" if legacy else "/resources")
    for field, value in {
        "type": "FILE",
        "name": "upload.sql",
        "currentDir": expected_directory,
    }.items():
        assert f'name="{field}"\r\n\r\n{value}\r\n'.encode() in uploaded.content
    assert (b'name="pid"\r\n\r\n7\r\n' in uploaded.content) is id_backed
    assert b'name="description"' not in uploaded.content
    assert b'name="file"; filename="upload.sql"' in uploaded.content
    assert b"select 2;" in uploaded.content
    if id_backed or storage or ds_version == "3.4.3":
        viewed_request = next(
            request for request in requests_seen if request.url.path.endswith("/view")
        )
        expected_view_params = {"skipLineNum": "1", "limit": "1"}
        if legacy:
            expected_view_params["id"] = "7"
        elif storage:
            expected_view_params.update(fullName="/base/folder/demo.sql", tenantCode="")
        elif ds_version == "3.4.3":
            expected_view_params["fullName"] = "/base/folder/demo.sql"
        assert dict(viewed_request.url.params) == expected_view_params
    expected_download = (
        "/resources/7/download" if id_backed and not legacy else "/resources/download"
    )
    downloaded_request = next(
        request for request in requests_seen if request.url.path.endswith("/download")
    )
    assert downloaded_request.url.path.endswith(expected_download)
    expected_identity = (
        {"id": "7"}
        if legacy
        else {}
        if id_backed
        else {"fullName": "/base/folder/demo.sql"}
    )
    assert dict(downloaded_request.url.params) == expected_identity
    deleted = requests_seen[-1]
    assert deleted.method == ("GET" if legacy else "DELETE")
    expected_delete = (
        "/resources/delete" if legacy else "/resources/7" if id_backed else "/resources"
    )
    assert deleted.url.path.endswith(expected_delete)
    assert dict(deleted.url.params) == expected_identity


def test_id_create_readback_failure_does_not_retry_write() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return _success(None)
        return _success(_resource_row("/different.sql"))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        resources = (
            ResourceAdapter.for_version("1.3.9")
            .bind(profile, http_client=client)
            .resources
        )
        with pytest.raises(ApiTransportError) as captured:
            resources.create_from_content(
                current_dir="/",
                file_name="owned",
                suffix="sql",
                content="select 1;",
            )

    assert captured.value.details["phase"] == "readback"
    assert captured.value.details["mutation_applied"] is True
    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/dolphinscheduler/resources/online-create"),
        ("GET", "/dolphinscheduler/resources/queryResource"),
    ]


@pytest.mark.parametrize(
    "ds_version",
    [
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    ],
)
def test_old_resource_domain_resolves_one_exact_bidirectional_file_identity(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success(_resource_row())

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        resources = resource_wire.bind_task_resource_resolver(
            ds_version, profile, http_client=http_client
        )
        assert resources.resolve_id("/base/folder/demo.sql") == 7
        assert resources.resolve_full_name(7) == "/base/folder/demo.sql"

    assert len(requests_seen) == 2
    forward, reverse = requests_seen
    if ds_version == "1.3.9":
        assert forward.url.path.endswith("/resources/queryResource")
        assert reverse.url.path == forward.url.path
        assert dict(forward.url.params) == {
            "fullName": "/base/folder/demo.sql",
            "type": "FILE",
        }
        assert dict(reverse.url.params) == {
            "type": "FILE",
            "id": "7",
        }
    else:
        assert forward.url.path.endswith("/resources/-1")
        assert dict(forward.url.params) == {
            "fullName": "/base/folder/demo.sql",
            "type": "FILE",
        }
        assert reverse.url.path.endswith("/resources/7")
        assert dict(reverse.url.params) == {"type": "FILE"}


def _resource_row(full_name: str = "/base/folder/demo.sql") -> dict[str, object]:
    return {
        "id": 7,
        "alias": "demo.sql",
        "fileName": "demo.sql",
        "fullName": full_name,
        "directory": False,
        "type": "FILE",
        "size": 14,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


@pytest.mark.parametrize("version", ["2.0.1", "2.0.2", "2.0.3", "2.0.4", "2.0.5"])
@pytest.mark.parametrize("content", ["native map content", 17])
def test_native_map_view_projects_text_and_retains_exact_value_validation(
    version: str,
    content: str | int,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/view"):
            assert request.url.params["skipLineNum"] == "2"
            assert request.url.params["limit"] == "4"
            return _success({"content": content})
        return _success(_resource_row())

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            ResourceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .resources
        )
        if isinstance(content, str):
            result = operations.view(
                full_name="/base/folder/demo.sql", skip_line_num=2, limit=4
            )
            assert result.content == content
        else:
            with pytest.raises(WireResponseDecodeError):
                operations.view(
                    full_name="/base/folder/demo.sql", skip_line_num=2, limit=4
                )
    assert len(requests) == 2


@pytest.mark.parametrize(
    ("skip", "limit", "expected"),
    [(0, 1, "first"), (2, 1, "third"), (1, 2, "second\nthird")],
)
def test_fixed_native_preview343_uses_only_requested_line_window(
    skip: int, limit: int, expected: str
) -> None:
    profile = make_profile(ds_version="3.4.3")
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.url.path == "/dolphinscheduler/resources/view"
        assert dict(request.url.params) == {
            "fullName": "/tenant/resources/demo.sql",
            "skipLineNum": str(skip),
            "limit": str(limit),
        }
        lines = ["first", "second", "third", "fourth"]
        return _success({"content": "\n".join(lines[skip : skip + limit])})

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        domain = ResourceAdapter.for_version("3.4.3").bind(profile, http_client=client)
        content = domain.resources.view(
            full_name="/tenant/resources/demo.sql", skip_line_num=skip, limit=limit
        )
    assert content.content == expected
    assert len(seen) == 1
