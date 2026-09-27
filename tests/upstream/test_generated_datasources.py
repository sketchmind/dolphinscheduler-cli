from __future__ import annotations

import json
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.datasources import (
    DATASOURCE_DOMAIN,
    DataSourceAdapter,
    DataSourceDomain,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_LEGACY_VERSION = "1.3.9"
_TYPED_BODY_VERSIONS = frozenset({"2.0.0", "2.0.9", "3.0.0", "3.0.6"})
_VOID_RESULT_VERSIONS = frozenset(
    {
        "1.3.9",
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
    }
)


def test_datasource_domain_rejects_an_unreviewed_version_decision() -> None:
    with pytest.raises(WireContractError, match="capability decision"):
        DATASOURCE_DOMAIN.adapter_for_version("9.9.9")


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_datasource_domain_executes_exact_crud_test_and_readback(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current = _datasource(datasource_id=7, name="warehouse")
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]], object]
    ] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        query = parse_qs(request.url.query.decode(), keep_blank_values=True)
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        body = _json_body(request)
        requests_seen.append((request.method, request.url.path, query, form, body))
        if _is_page_request(ds_version, request):
            return _success(
                _page(
                    _datasource_entity(current),
                    page_no=int(query["pageNo"][0]),
                    page_size=int(query["pageSize"][0]),
                )
            )
        if _is_detail_request(ds_version, request):
            return _success(_datasource_detail(current, legacy=ds_version == "1.3.9"))
        if _is_create_request(ds_version, request):
            payload = _request_payload(ds_version, form=form, body=body)
            current = _datasource_from_payload(payload, datasource_id=41)
            return _success(
                None
                if ds_version in _VOID_RESULT_VERSIONS
                else _datasource_entity(current)
            )
        if _is_update_request(ds_version, request):
            payload = _request_payload(ds_version, form=form, body=body)
            current = _datasource_from_payload(payload, datasource_id=41)
            return _success(
                None
                if ds_version in _VOID_RESULT_VERSIONS
                else _datasource_entity(current)
            )
        if _is_saved_test_request(ds_version, request):
            return _success(None if ds_version in _VOID_RESULT_VERSIONS else True)
        if _is_delete_request(ds_version, request):
            return _success(None if ds_version in _VOID_RESULT_VERSIONS else True)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = DataSourceAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    payload = {
        "name": "new-warehouse",
        "note": "created",
        "type": "MYSQL",
        "host": "db.example",
        "port": 3306,
        "database": "warehouse",
        "userName": "etl",
        "password": "secret",
        "other": {"serverTimezone": "UTC"},
    }
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, DataSourceDomain)
        datasources = domain.datasources
        page = datasources.list(page_no=2, page_size=25, search="warehouse")
        fetched = datasources.get(datasource_id=7)
        created = datasources.create(payload_json=json.dumps(payload))
        updated_payload = {**payload, "name": "renamed-warehouse", "note": "updated"}
        updated = datasources.update(
            datasource_id=41,
            payload_json=json.dumps(updated_payload),
        )
        connected = datasources.connection_test(datasource_id=41)
        deleted = datasources.delete(datasource_id=41)

    assert page.pageNo == 2
    assert page.pageSize == 25
    assert next(iter(page.totalList or [])).name == "warehouse"
    assert fetched["name"] == "warehouse"
    assert fetched["other"] == {"serverTimezone": "UTC"}
    assert created.id == 41
    assert created.name == "new-warehouse"
    assert updated.id == 41
    assert updated.name == "renamed-warehouse"
    assert connected is True
    assert deleted is True
    assert datasources.blank_password_preserves_existing is (
        ds_version != _LEGACY_VERSION
    )
    _assert_exact_wire(ds_version, requests_seen)


@pytest.mark.parametrize(
    ("ds_version", "method", "path"),
    [
        ("1.3.9", "GET", "/dolphinscheduler/datasources/delete"),
        ("2.0.0", "DELETE", "/dolphinscheduler/datasources/41"),
        ("3.4.1", "DELETE", "/dolphinscheduler/datasources/41"),
    ],
)
def test_datasource_delete_is_once_only_with_the_exact_native_method(
    ds_version: str, method: str, path: str
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert len(requests) == 1, "must not replay or read back a failed deletion"
        return httpx.Response(503, text="unavailable")

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        DATASOURCE_DOMAIN.bind(profile, http_client=client).datasources.delete(
            datasource_id=41
        )

    assert [(request.method, request.url.path) for request in requests] == [
        (method, path)
    ]
    assert parse_qs(requests[0].url.query.decode()) == (
        {"id": ["41"]} if ds_version == "1.3.9" else {}
    )
    assert caught.value.error_type == "api_transport_error"
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True
    assert caught.value.details["request_replay_safe"] is False
    assert caught.value.details["attempts"] == 1
    assert caught.value.details["max_attempts"] == 1
    assert caught.value.suggestion == (
        "Inspect the datasource before deciding whether to retry datasource delete; "
        "do not blindly repeat the mutation."
    )


def test_legacy_datasource_delete_response_loss_never_replays() -> None:
    profile = make_profile(ds_version="1.3.9").model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []
    existing_ids = {41}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/datasources/delete"
        assert parse_qs(request.url.query.decode()) == {"id": ["41"]}
        assert len(requests) == 1, "must not replay or read back an uncertain deletion"
        existing_ids.remove(41)
        message = "response lost after deletion"
        raise httpx.ReadTimeout(message, request=request)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        DATASOURCE_DOMAIN.bind(profile, http_client=client).datasources.delete(
            datasource_id=41
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
        "Inspect the datasource before deciding whether to retry datasource delete; "
        "do not blindly repeat the mutation."
    )


def test_legacy_datasource_delete_preserves_a_definitive_upstream_error() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"code": 20004, "msg": "resource not exist", "data": None}
        )

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiResultError) as caught,
    ):
        DATASOURCE_DOMAIN.bind(profile, http_client=client).datasources.delete(
            datasource_id=41
        )

    assert len(requests) == 1
    assert caught.value.result_code == 20004
    assert "mutation_may_have_applied" not in caught.value.details


def _assert_exact_wire(
    ds_version: str,
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]], object]
    ],
) -> None:
    page_path = (
        "/dolphinscheduler/datasources/list-paging"
        if ds_version == _LEGACY_VERSION
        else "/dolphinscheduler/datasources"
    )
    page_request = next(
        item for item in requests_seen if item[0] == "GET" and item[1] == page_path
    )
    assert page_request[2] == {
        "pageNo": ["2"],
        "pageSize": ["25"],
        "searchVal": ["warehouse"],
    }

    if ds_version == _LEGACY_VERSION:
        assert any(
            method == "POST"
            and path == "/dolphinscheduler/datasources/update-ui"
            and form == {"id": ["7"]}
            for method, path, _query, form, _body in requests_seen
        )
        assert any(
            method == "POST"
            and path == "/dolphinscheduler/datasources/create"
            and form.get("name") == ["new-warehouse"]
            for method, path, _query, form, _body in requests_seen
        )
        assert any(
            method == "POST"
            and path == "/dolphinscheduler/datasources/update"
            and form.get("id") == ["41"]
            for method, path, _query, form, _body in requests_seen
        )
        assert any(
            method == "GET"
            and path == "/dolphinscheduler/datasources/connect-by-id"
            and query == {"id": ["41"]}
            for method, path, query, _form, _body in requests_seen
        )
        assert any(
            method == "GET"
            and path == "/dolphinscheduler/datasources/delete"
            and query == {"id": ["41"]}
            for method, path, query, _form, _body in requests_seen
        )
        return

    assert any(
        method == "GET" and path == "/dolphinscheduler/datasources/7"
        for method, path, _query, _form, _body in requests_seen
    )
    assert any(
        method == "POST"
        and path == "/dolphinscheduler/datasources"
        and isinstance(body, dict)
        and body.get("name") == "new-warehouse"
        for method, path, _query, _form, body in requests_seen
    )
    assert any(
        method == "PUT"
        and path == "/dolphinscheduler/datasources/41"
        and isinstance(body, dict)
        and body.get("name") == "renamed-warehouse"
        for method, path, _query, _form, body in requests_seen
    )
    assert any(
        method == "GET" and path == "/dolphinscheduler/datasources/41/connect-test"
        for method, path, _query, _form, _body in requests_seen
    )
    assert any(
        method == "DELETE" and path == "/dolphinscheduler/datasources/41"
        for method, path, _query, _form, _body in requests_seen
    )


def _is_page_request(ds_version: str, request: httpx.Request) -> bool:
    expected = (
        "/dolphinscheduler/datasources/list-paging"
        if ds_version == _LEGACY_VERSION
        else "/dolphinscheduler/datasources"
    )
    return request.method == "GET" and request.url.path == expected


def _is_detail_request(ds_version: str, request: httpx.Request) -> bool:
    if ds_version == _LEGACY_VERSION:
        return (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/datasources/update-ui"
        )
    return (
        request.method == "GET"
        and request.url.path.startswith("/dolphinscheduler/datasources/")
        and not request.url.path.endswith("/connect-test")
    )


def _is_create_request(ds_version: str, request: httpx.Request) -> bool:
    path = (
        "/dolphinscheduler/datasources/create"
        if ds_version == _LEGACY_VERSION
        else "/dolphinscheduler/datasources"
    )
    return request.method == "POST" and request.url.path == path


def _is_update_request(ds_version: str, request: httpx.Request) -> bool:
    if ds_version == _LEGACY_VERSION:
        return (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/datasources/update"
        )
    return (
        request.method == "PUT"
        and request.url.path == "/dolphinscheduler/datasources/41"
    )


def _is_saved_test_request(ds_version: str, request: httpx.Request) -> bool:
    path = (
        "/dolphinscheduler/datasources/connect-by-id"
        if ds_version == _LEGACY_VERSION
        else "/dolphinscheduler/datasources/41/connect-test"
    )
    return request.method == "GET" and request.url.path == path


def _is_delete_request(ds_version: str, request: httpx.Request) -> bool:
    if ds_version == _LEGACY_VERSION:
        return (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/datasources/delete"
        )
    return (
        request.method == "DELETE"
        and request.url.path == "/dolphinscheduler/datasources/41"
    )


def _request_payload(
    ds_version: str,
    *,
    form: dict[str, list[str]],
    body: object,
) -> dict[str, object]:
    if ds_version == _LEGACY_VERSION:
        other = json.loads(form.get("other", ["{}"])[0])
        assert isinstance(other, dict)
        return {
            "name": form["name"][0],
            "note": form.get("note", [""])[0],
            "type": form["type"][0],
            "host": form["host"][0],
            "port": int(form["port"][0]),
            "database": form["database"][0],
            "userName": form.get("userName", [""])[0],
            "password": form.get("password", [""])[0],
            "other": other,
        }
    assert isinstance(body, dict)
    if ds_version in _TYPED_BODY_VERSIONS:
        assert body.get("type") == "MYSQL"
    return dict(body)


def _json_body(request: httpx.Request) -> object:
    content_type = request.headers.get("content-type", "")
    if "application/json" not in content_type or not request.content:
        return None
    return json.loads(request.content)


def _datasource_from_payload(
    payload: dict[str, object],
    *,
    datasource_id: int,
) -> dict[str, object]:
    return {
        **payload,
        "id": datasource_id,
        "userId": 1,
        "userName": payload.get("userName"),
        "createTime": "2026-08-05 10:00:00",
        "updateTime": "2026-08-05 10:00:00",
    }


def _datasource(*, datasource_id: int, name: str) -> dict[str, object]:
    return _datasource_from_payload(
        {
            "name": name,
            "note": "source",
            "type": "MYSQL",
            "host": "db.example",
            "port": 3306,
            "database": "warehouse",
            "userName": "etl",
            "password": "secret",
            "other": {"serverTimezone": "UTC"},
        },
        datasource_id=datasource_id,
    )


def _datasource_entity(datasource: dict[str, object]) -> dict[str, object]:
    return {
        "id": datasource["id"],
        "userId": datasource["userId"],
        "userName": datasource["userName"],
        "name": datasource["name"],
        "note": datasource["note"],
        "type": datasource["type"],
        "connectionParams": json.dumps(
            {
                "password": "******",
                "host": datasource["host"],
                "port": datasource["port"],
            }
        ),
        "createTime": datasource["createTime"],
        "updateTime": datasource["updateTime"],
    }


def _datasource_detail(
    datasource: dict[str, object],
    *,
    legacy: bool,
) -> dict[str, object]:
    detail = {
        key: datasource[key]
        for key in (
            "name",
            "note",
            "type",
            "host",
            "port",
            "database",
            "userName",
            "password",
            "other",
        )
    }
    if legacy:
        detail.update(
            {
                "port": str(detail["port"]),
                "principal": "",
                "connectType": None,
            }
        )
    else:
        detail["id"] = datasource["id"]
    return detail


def _page(
    item: dict[str, object],
    *,
    page_no: int,
    page_size: int,
) -> dict[str, object]:
    return {
        "totalList": [item],
        "total": 1,
        "totalPage": 1,
        "pageSize": page_size,
        "currentPage": page_no,
        "pageNo": page_no,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
