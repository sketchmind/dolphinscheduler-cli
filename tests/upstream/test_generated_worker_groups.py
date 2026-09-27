from __future__ import annotations

import importlib
from contextlib import nullcontext
from copy import deepcopy
from typing import Any, cast
from urllib.parse import parse_qs

import httpx
import pytest
from pydantic import ValidationError

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.wire import WireContractError
from dsctl.upstream.worker_groups import (
    _WORKER_GROUP_PROGRAMS,
    WorkerGroupAdapter,
    WorkerGroupDomain,
)
from tests.support import make_profile

# WorkerGroup adds description in 3.1.0; every reviewed 3.1.x source retains it.
_DESCRIPTION_VERSIONS = frozenset(
    {
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
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_DESCRIPTION_ABSENT_VERSIONS = tuple(
    version for version in TARGET_DS_VERSIONS if version not in _DESCRIPTION_VERSIONS
)
_ENTITY_SAVE_VERSIONS = frozenset(
    {"3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)


def test_worker_group_strict_page_schema_cannot_use_the_coercive_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.worker_group")
    schemas = dict(module.RESPONSE_SCHEMAS)
    schemas["page_described_strict"] = schemas["page_described"]
    monkeypatch.setattr(module, "RESPONSE_SCHEMAS", schemas)

    with pytest.raises(
        WireContractError,
        match="page_described_strict response schema is invalid",
    ):
        _WORKER_GROUP_PROGRAMS.fresh_profile("3.1.9")


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_worker_group_domain_executes_the_reviewed_crud_recipe(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    legacy = ds_version == "1.3.9"
    collection_path = (
        "/dolphinscheduler/worker-group/list-paging"
        if legacy
        else "/dolphinscheduler/worker-groups"
    )
    current: dict[str, object] | None = None
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]]]
    ] = []

    def worker_group_payload(
        form: dict[str, list[str]],
        *,
        worker_group_id: int,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "id": worker_group_id,
            "name": form["name"][0],
            "addrList": form["addrList"][0],
            "createTime": None,
            "updateTime": None,
            "systemDefault": False,
        }
        if ds_version in _DESCRIPTION_VERSIONS:
            payload["description"] = form.get("description", [None])[0]
        return payload

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        query = parse_qs(request.url.query.decode())
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, query, form))
        if request.method == "GET" and request.url.path == collection_path:
            items = [] if current is None else [current]
            return _success(
                {
                    "totalList": items,
                    "total": len(items),
                    "totalPage": 0 if current is None else 1,
                    "currentPage": 1,
                }
            )
        save_path = "/dolphinscheduler/worker-group/save" if legacy else collection_path
        if request.method == "POST" and request.url.path == save_path:
            worker_group_id = int(form["id"][0]) or 8
            current = worker_group_payload(form, worker_group_id=worker_group_id)
            result = (
                current
                if ds_version in _ENTITY_SAVE_VERSIONS
                else {"future": "ignored by Void response"}
            )
            return _success(result)
        delete_path = (
            "/dolphinscheduler/worker-group/delete-by-id"
            if legacy
            else "/dolphinscheduler/worker-groups/8"
        )
        delete_method = "POST" if legacy else "DELETE"
        if request.method == delete_method and request.url.path == delete_path:
            if legacy:
                assert form["id"] == ["8"]
            current = None
            return _success(["ignored", "by", "Void", "response"])
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = WorkerGroupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    description = "created" if ds_version in _DESCRIPTION_VERSIONS else None
    updated_description = "updated" if ds_version in _DESCRIPTION_VERSIONS else None
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, WorkerGroupDomain)
        created = domain.worker_groups.create(
            name="worker-new",
            addr_list="worker-a:1234",
            description=description,
        )
        assert created.id is not None
        updated = domain.worker_groups.update(
            worker_group_id=created.id,
            name="worker-updated",
            addr_list="worker-b:1234",
            description=updated_description,
        )
        assert updated.id is not None
        deleted = domain.worker_groups.delete(worker_group_id=updated.id)

    assert created.id == 8
    assert created.name == "worker-new"
    assert updated.name == "worker-updated"
    assert updated.addrList == "worker-b:1234"
    assert updated.description == updated_description
    assert deleted is True
    mutations = [request for request in requests_seen if request[0] != "GET"]
    assert [(method, path) for method, path, _query, _form in mutations] == [
        (
            "POST",
            "/dolphinscheduler/worker-group/save"
            if legacy
            else "/dolphinscheduler/worker-groups",
        ),
        (
            "POST",
            "/dolphinscheduler/worker-group/save"
            if legacy
            else "/dolphinscheduler/worker-groups",
        ),
        (
            "POST" if legacy else "DELETE",
            "/dolphinscheduler/worker-group/delete-by-id"
            if legacy
            else "/dolphinscheduler/worker-groups/8",
        ),
    ]
    expected_create_form = {
        "id": ["0"],
        "name": ["worker-new"],
        "addrList": ["worker-a:1234"],
    }
    expected_update_form = {
        "id": ["8"],
        "name": ["worker-updated"],
        "addrList": ["worker-b:1234"],
    }
    if ds_version in _DESCRIPTION_VERSIONS:
        expected_create_form["description"] = ["created"]
        expected_update_form["description"] = ["updated"]
    assert mutations[0][3] == expected_create_form
    assert mutations[1][3] == expected_update_form
    readbacks = [request for request in requests_seen if request[0] == "GET"]
    assert readbacks[0][2] == {
        "pageNo": ["1"],
        "pageSize": ["100"],
        "searchVal": ["worker-new"],
    }
    assert readbacks[1][2] == {
        "pageNo": ["1"],
        "pageSize": ["100"],
        "searchVal": ["worker-updated"],
    }
    assert readbacks[2][2] == {"pageNo": ["1"], "pageSize": ["100"]}
    if legacy:
        assert mutations[2][3] == {"id": ["8"]}
    else:
        assert mutations[2][2:] == ({}, {})


@pytest.mark.parametrize("ds_version", sorted(_DESCRIPTION_VERSIONS))
@pytest.mark.parametrize("operation", ["create", "update"])
@pytest.mark.parametrize(
    ("description", "expected_form_description", "expected_description"),
    [
        (None, None, ""),
        ("", [""], ""),
        ("operator supplied", ["operator supplied"], "operator supplied"),
    ],
)
def test_worker_group_description_uses_exact_native_default_and_text(
    ds_version: str,
    operation: str,
    description: str | None,
    expected_form_description: list[str] | None,
    expected_description: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current: dict[str, object] | None = None
    saved_form: dict[str, list[str]] | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current, saved_form
        if request.method == "POST":
            saved_form = parse_qs(
                request.content.decode(),
                keep_blank_values=True,
            )
            current = _worker_group(description=saved_form.get("description", [""])[0])
            current.update(
                {
                    "id": int(saved_form["id"][0]) or 8,
                    "name": saved_form["name"][0],
                    "addrList": saved_form["addrList"][0],
                }
            )
            result: object = (
                current
                if ds_version in _ENTITY_SAVE_VERSIONS
                else {"future": "ignored by Void response"}
            )
            return _success(result)
        assert request.method == "GET"
        return _success(_page(current))

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    ) as http_client:
        worker_groups = (
            WorkerGroupAdapter.for_version(ds_version)
            .bind(
                profile,
                http_client=http_client,
            )
            .worker_groups
        )
        if operation == "create":
            saved = worker_groups.create(
                name="worker-new",
                addr_list="worker-a:1234",
                description=description,
            )
        else:
            saved = worker_groups.update(
                worker_group_id=8,
                name="worker-new",
                addr_list="worker-a:1234",
                description=description,
            )

    assert saved.description == expected_description
    assert saved_form is not None
    assert saved_form.get("description") == expected_form_description


@pytest.mark.parametrize(
    ("ds_version", "strict"),
    [("3.1.9", True), ("3.2.0", False)],
)
def test_worker_group_page_preserves_exact_integer_validation_epoch(
    ds_version: str,
    *,
    strict: bool,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _page(_worker_group(description="prod"))
    for field in ("total", "totalPage", "pageSize", "currentPage", "pageNo"):
        payload[field] = str(payload[field])
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    context = pytest.raises(ApiTransportError) if strict else nullcontext()
    with http_client, context:
        page = (
            WorkerGroupAdapter.for_version(ds_version)
            .bind(profile, http_client=http_client)
            .worker_groups.list(page_no=2, page_size=25)
        )
        assert page.total == 1


@pytest.mark.parametrize("response_data", [None, {"id": "not-an-integer"}])
def test_worker_group_entity_save_rejects_invalid_payload_before_readback(
    response_data: object,
) -> None:
    profile = make_profile(ds_version="3.2.2")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(response_data)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        WorkerGroupAdapter.for_version("3.2.2").bind(
            profile,
            http_client=http_client,
        ).worker_groups.create(name="new", addr_list="worker-a:1234")

    assert requests_seen == 1
    assert exc_info.value.details["phase"] == "mutation_response"


@pytest.mark.parametrize("operation", ["save", "delete"])
def test_worker_group_mutations_are_never_retried(operation: str) -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(503, json={"message": "unavailable"})

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    worker_groups = (
        WorkerGroupAdapter.for_version("3.4.1")
        .bind(
            profile,
            http_client=http_client,
        )
        .worker_groups
    )

    def mutate() -> object:
        if operation == "save":
            return worker_groups.create(name="new", addr_list="worker-a:1234")
        return worker_groups.delete(worker_group_id=8)

    with http_client, pytest.raises(ApiTransportError):
        mutate()

    assert requests_seen == 1


def test_worker_group_read_retries_and_accepts_a_raw_page() -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
    )
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        if requests_seen == 1:
            return httpx.Response(503, json={"message": "unavailable"})
        return httpx.Response(200, json=_page(_worker_group(description="prod")))

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = (
            WorkerGroupAdapter.for_version("3.4.1")
            .bind(
                profile,
                http_client=http_client,
            )
            .worker_groups.list(page_no=2, page_size=25)
        )

    assert requests_seen == 2
    assert page.total == 1


def test_worker_group_result_error_preserves_the_server_code() -> None:
    profile = make_profile(ds_version="3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"code": 1402001, "msg": "missing", "data": None},
            )
        ),
    )
    with http_client, pytest.raises(ApiResultError) as exc_info:
        WorkerGroupAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).worker_groups.list(page_no=1, page_size=20)

    assert exc_info.value.result_code == 1402001


@pytest.mark.parametrize(
    ("ds_version", "accepts_null"),
    [("3.0.6", True), ("3.1.0", False)],
)
def test_worker_group_total_list_nullability_preserves_the_exact_epoch(
    ds_version: str,
    *,
    accepts_null: bool,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _page()
    payload["totalList"] = None
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    context = nullcontext() if accepts_null else pytest.raises(ApiTransportError)
    with http_client, context:
        page = (
            WorkerGroupAdapter.for_version(ds_version)
            .bind(
                profile,
                http_client=http_client,
            )
            .worker_groups.list(page_no=1, page_size=20)
        )
        assert page.totalList == []


def test_worker_group_page_rejects_an_unknown_source_enum() -> None:
    profile = make_profile(ds_version="3.3.1")
    payload = _worker_group(description="prod")
    payload["source"] = "FUTURE"
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(_page(payload))),
    )
    with http_client, pytest.raises(ApiTransportError):
        WorkerGroupAdapter.for_version("3.3.1").bind(
            profile,
            http_client=http_client,
        ).worker_groups.list(page_no=1, page_size=20)


@pytest.mark.parametrize("raw_id", ["/", "%", " ", "雪", "", ".", ".."])
def test_worker_group_path_rejects_non_integer_segments_before_io(
    raw_id: str,
) -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ValidationError):
        WorkerGroupAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).worker_groups.delete(worker_group_id=cast("Any", raw_id))

    assert requests_seen == 0


def test_worker_group_path_uses_source_derived_integer_coercion_once() -> None:
    profile = make_profile(ds_version="3.4.1")
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.method == "DELETE":
            assert not request.url.query
            assert not request.content
            return _success(None)
        return _success(_page())

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        deleted = (
            WorkerGroupAdapter.for_version("3.4.1")
            .bind(
                profile,
                http_client=http_client,
            )
            .worker_groups.delete(worker_group_id=cast("Any", "8"))
        )

    assert deleted is True
    assert paths == [
        "/dolphinscheduler/worker-groups/8",
        "/dolphinscheduler/worker-groups",
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("path", "worker-groups"),
        ("path", "worker-groups/{workerId}"),
        ("path", "worker-groups/{id}/{id}"),
        ("path", "worker-groups/{id}/{other}"),
        ("path", "worker-groups/{id"),
        ("path", "worker-groups/{id}?force=1"),
        ("method", "POST"),
        ("channel", "form"),
        ("path_encoding", None),
    ],
)
def test_worker_group_path_codec_tampering_fails_before_io(
    field: str,
    value: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.worker_group")
    codecs = deepcopy(module.CODECS)
    codecs["delete_path_void"][field] = value
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError):
        _WORKER_GROUP_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize(
    ("ds_version", "expected_code"),
    [("3.2.1", 10174), ("3.2.2", 1402001)],
)
def test_worker_group_not_found_code_preserves_the_exact_epoch(
    ds_version: str,
    expected_code: int,
) -> None:
    profile = make_profile(ds_version=ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(_page())),
    )
    with http_client, pytest.raises(ApiResultError) as exc_info:
        WorkerGroupAdapter.for_version(ds_version).bind(
            profile,
            http_client=http_client,
        ).worker_groups.get(worker_group_id=8)

    assert exc_info.value.result_code == expected_code


def test_modern_config_worker_group_normalizes_missing_identity() -> None:
    profile = make_profile(ds_version="3.4.2")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/worker-groups"
        return _success(
            {
                "totalList": [
                    {
                        "id": None,
                        "name": "config-default",
                        "addrList": "worker-a:1234",
                        "createTime": None,
                        "updateTime": None,
                        "description": None,
                        "systemDefault": True,
                        "source": "CONFIG",
                    }
                ],
                "total": 0,
                "totalPage": 0,
                "currentPage": 1,
            }
        )

    adapter = WorkerGroupAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = adapter.bind(
            profile,
            http_client=http_client,
        ).worker_groups.list(page_no=1, page_size=20)

    assert page.totalList is not None
    assert len(page.totalList) == 1
    worker_group = page.totalList[0]
    assert worker_group.id is None
    assert worker_group.name == "config-default"
    assert worker_group.systemDefault is True


@pytest.mark.parametrize("ds_version", _DESCRIPTION_ABSENT_VERSIONS)
@pytest.mark.parametrize("operation", ["create", "update"])
def test_worker_group_description_absence_rejects_without_sending_a_request(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = WorkerGroupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        worker_groups = adapter.bind(
            profile,
            http_client=http_client,
        ).worker_groups

        def mutate_description() -> object:
            if operation == "create":
                return worker_groups.create(
                    name="worker-new",
                    addr_list="worker-a:1234",
                    description="unsupported",
                )
            return worker_groups.update(
                worker_group_id=8,
                name="worker-new",
                addr_list="worker-a:1234",
                description="unsupported",
            )

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            mutate_description()

    assert request_count == 0
    assert exc_info.value.details == {
        "action": f"worker-group.{operation}",
        "selected_version": ds_version,
        "facet": "description",
        "reason": "upstream_capability_absent",
    }


def test_worker_group_create_readback_failure_requires_reconciliation() -> None:
    profile = make_profile(ds_version="3.4.1")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _success(
                {
                    "id": 8,
                    "name": "worker-new",
                    "addrList": "worker-a:1234",
                    "description": None,
                    "systemDefault": False,
                }
            )
        return _success(
            {
                "totalList": [],
                "total": 0,
                "totalPage": 0,
                "currentPage": 1,
            }
        )

    adapter = WorkerGroupAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).worker_groups.create(
            name="worker-new",
            addr_list="worker-a:1234",
        )

    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def _worker_group(*, description: str | None = None) -> dict[str, object]:
    return {
        "id": 8,
        "name": "prod",
        "addrList": "worker-a:1234",
        "createTime": None,
        "updateTime": None,
        "description": description,
        "systemDefault": False,
        "source": "UI",
    }


def _page(item: dict[str, object] | None = None) -> dict[str, object]:
    items = [] if item is None else [item]
    return {
        "totalList": items,
        "total": len(items),
        "totalPage": 0 if item is None else 1,
        "pageSize": 25,
        "currentPage": 2,
        "pageNo": 2,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
