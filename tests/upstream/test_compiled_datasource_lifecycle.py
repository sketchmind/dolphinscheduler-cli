from __future__ import annotations

import json
from functools import partial
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiHttpError, ApiResultError, ApiTransportError
from dsctl.upstream.datasources import DATASOURCE_DOMAIN
from tests.support import make_profile
from tests.upstream.test_generated_datasources import (
    _datasource,
    _datasource_detail,
    _datasource_entity,
    _page,
    _success,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.support.json_types import JsonValue

_TYPED_VERSIONS = ("2.0.0", "2.0.9", "3.0.0", "3.0.6")
_RAW_VERSIONS = (
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
    "3.4.3",
)
_RAW_VOID_VERSIONS = ("3.1.0", "3.1.9", "3.2.0")
_PAYLOAD = (
    '{"name":"warehouse","type":"MYSQL","host":"db.example",'
    '"port":3306,"database":"warehouse"}'
)


def _client(
    ds_version: str, handler: Callable[[httpx.Request], httpx.Response]
) -> DolphinSchedulerClient:
    return DolphinSchedulerClient(
        make_profile(ds_version=ds_version).model_copy(
            update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
        ),
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.parametrize("ds_version", _TYPED_VERSIONS)
@pytest.mark.parametrize("operation", ["create", "update"])
def test_typed_body_preserves_exact_extra_policy_and_update_overwrites_body_id(
    ds_version: str, operation: str
) -> None:
    source = _datasource(datasource_id=41, name="warehouse")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            # Native void operations discard even a non-null successful result.
            return _success({"ignored": "void result"})
        if request.url.path == "/dolphinscheduler/datasources":
            assert dict(request.url.params) == {
                "pageNo": "1",
                "pageSize": "100",
                "searchVal": "warehouse",
            }
            return _success(_page(_datasource_entity(source), page_no=1, page_size=100))
        assert request.url.path == "/dolphinscheduler/datasources/41"
        return _success(_datasource_detail(source, legacy=False))

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        payload = (
            ' {"id":999,"name":"warehouse","type":"MYSQL","note":null,'
            '"other":{},"future":{"label":"中文","values":[1,null]}}\n'
        )
        result = (
            operations.create(payload_json=payload)
            if operation == "create"
            else operations.update(datasource_id=41, payload_json=payload)
        )
    expected_body: dict[str, object] = {
        "id": 999 if operation == "create" else 41,
        "name": "warehouse",
        "type": "MYSQL",
        "other": {},
    }
    if ds_version != "2.0.0":
        expected_body["future"] = {"label": "中文", "values": [1, None]}
    assert json.loads(requests[0].content) == expected_body
    assert requests[0].headers["content-type"] == "application/json"
    assert [(request.method, request.url.path) for request in requests] == (
        [
            ("POST", "/dolphinscheduler/datasources"),
            ("GET", "/dolphinscheduler/datasources"),
            ("GET", "/dolphinscheduler/datasources/41"),
        ]
        if operation == "create"
        else [
            ("PUT", "/dolphinscheduler/datasources/41"),
            ("GET", "/dolphinscheduler/datasources/41"),
        ]
    )
    assert result.id == 41
    assert result.name == "warehouse"


@pytest.mark.parametrize("ds_version", _RAW_VERSIONS)
@pytest.mark.parametrize("operation", ["create", "update"])
def test_raw_body_is_not_reencoded_and_does_not_rewrite_the_supplied_id(
    ds_version: str, operation: str
) -> None:
    source = _datasource(datasource_id=41, name="warehouse")
    requests: list[httpx.Request] = []
    raw = ' \n { "id":999, "name":"warehouse", "note":null, "future":"中文" } \t'

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return _success(
                None if ds_version in _RAW_VOID_VERSIONS else _datasource_entity(source)
            )
        if request.url.path == "/dolphinscheduler/datasources":
            assert dict(request.url.params) == {
                "pageNo": "1",
                "pageSize": "100",
                "searchVal": "warehouse",
            }
            return _success(_page(_datasource_entity(source), page_no=1, page_size=100))
        assert request.url.path == "/dolphinscheduler/datasources/41"
        return _success(_datasource_detail(source, legacy=False))

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        result = (
            operations.create(payload_json=raw)
            if operation == "create"
            else operations.update(datasource_id=41, payload_json=raw)
        )
    assert requests[0].content == raw.encode("utf-8")
    assert requests[0].headers["content-type"] == "application/json"
    assert [(request.method, request.url.path) for request in requests] == (
        [
            ("POST", "/dolphinscheduler/datasources"),
            *(
                [("GET", "/dolphinscheduler/datasources")]
                if ds_version in _RAW_VOID_VERSIONS
                else []
            ),
            ("GET", "/dolphinscheduler/datasources/41"),
        ]
        if operation == "create"
        else [
            ("PUT", "/dolphinscheduler/datasources/41"),
            ("GET", "/dolphinscheduler/datasources/41"),
        ]
    )
    assert result.id == 41
    assert result.name == "warehouse"


@pytest.mark.parametrize("operation", ["create", "update"])
@pytest.mark.parametrize("other", [{"z": "末", "a": "首"}, ' {"z" : "末"} '])
def test_legacy_form_keeps_defaults_and_exact_other_projection(
    operation: str, other: JsonValue
) -> None:
    source = _datasource(datasource_id=41, name="warehouse")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return _success(None)
        if request.url.path == "/dolphinscheduler/datasources/list-paging":
            return _success(_page(_datasource_entity(source), page_no=1, page_size=100))
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/datasources/update-ui"
        assert parse_qs(request.content.decode()) == {"id": ["41"]}
        return _success(_datasource_detail(source, legacy=True))

    with _client("1.3.9", handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        payload = json.dumps(
            {**json.loads(_PAYLOAD), "id": 999, "note": None, "other": other}
        )
        result = (
            operations.create(payload_json=payload)
            if operation == "create"
            else operations.update(datasource_id=41, payload_json=payload)
        )
    expected = {
        "name": ["warehouse"],
        "type": ["MYSQL"],
        "host": ["db.example"],
        "port": ["3306"],
        "database": ["warehouse"],
        "principal": [""],
        "userName": [""],
        "password": [""],
        "connectType": ["ORACLE_SERVICE_NAME"],
        "other": [
            '{"a":"首","z":"末"}' if isinstance(other, dict) else ' {"z" : "末"} '
        ],
    }
    if operation == "update":
        expected["id"] = ["41"]
    assert parse_qs(requests[0].content.decode(), keep_blank_values=True) == expected
    assert requests[0].method == "POST"
    assert requests[0].url.path == f"/dolphinscheduler/datasources/{operation}"
    assert requests[0].headers["content-type"] == "application/x-www-form-urlencoded"
    assert len(requests) == (3 if operation == "create" else 2)
    assert result.name == "warehouse"


@pytest.mark.parametrize(
    ("ds_version", "operation"),
    [
        ("1.3.9", "create"),
        ("1.3.9", "update"),
        ("1.3.9", "delete"),
        ("2.0.0", "create"),
        ("2.0.0", "update"),
        ("2.0.0", "delete"),
        ("3.1.0", "create"),
        ("3.2.1", "create"),
        ("3.2.1", "update"),
        ("3.2.1", "delete"),
    ],
)
def test_failed_mutation_is_not_replayed_or_followed_by_readback(
    ds_version: str, operation: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503)

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        call: Callable[[], object]
        if operation == "create":
            call = partial(operations.create, payload_json=_PAYLOAD)
        elif operation == "update":
            call = partial(operations.update, datasource_id=41, payload_json=_PAYLOAD)
        else:
            call = partial(operations.delete, datasource_id=41)
        with pytest.raises(ApiTransportError) as caught:
            call()
    assert len(requests) == 1
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True
    if ds_version == "1.3.9" and operation == "delete":
        assert requests[0].method == "GET"
        assert requests[0].url.path == "/dolphinscheduler/datasources/delete"
        assert dict(requests[0].url.params) == {"id": "41"}
        assert caught.value.details["request_replay_safe"] is False


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.2.1"])
def test_definitive_upstream_error_precedes_mutation_projection_and_readback(
    ds_version: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"code": 100, "msg": "denied", "data": "invalid entity"}
        )

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        with pytest.raises(ApiResultError) as caught:
            operations.create(payload_json=_PAYLOAD)
    assert caught.value.result_code == 100
    assert "mutation_may_have_applied" not in caught.value.details
    assert len(requests) == 1


@pytest.mark.parametrize("operation", ["delete", "connection_test"])
def test_legacy_get_delete_and_saved_test_keep_optional_envelopes(
    operation: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == (
            "/dolphinscheduler/datasources/delete"
            if operation == "delete"
            else "/dolphinscheduler/datasources/connect-by-id"
        )
        assert dict(request.url.params) == {"id": "41"}
        return (
            httpx.Response(503)
            if operation == "connection_test" and len(requests) == 1
            else httpx.Response(200, json={"ack": True})
        )

    with _client("1.3.9", handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        result = (
            operations.delete(datasource_id=41)
            if operation == "delete"
            else operations.connection_test(datasource_id=41)
        )
    assert result is True
    assert len(requests) == (2 if operation == "connection_test" else 1)


@pytest.mark.parametrize("status", [200, 503])
def test_legacy_post_detail_keeps_required_envelope_and_does_not_retry(
    status: int,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/datasources/update-ui"
        return httpx.Response(status, json={"name": "warehouse"})

    with _client("1.3.9", handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        with pytest.raises(ApiResultError if status == 200 else ApiHttpError):
            operations.get(datasource_id=41)
    assert len(requests) == 1


@pytest.mark.parametrize("ds_version", ["2.0.0", "3.2.1"])
@pytest.mark.parametrize("operation", ["create", "update"])
def test_empty_name_validation_keeps_its_existing_post_write_position(
    ds_version: str, operation: str
) -> None:
    source = _datasource(datasource_id=41, name="warehouse")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return _success(
                None if ds_version == "2.0.0" else _datasource_entity(source)
            )
        return _success(_datasource_detail(source, legacy=False))

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        payload = '{"name":"","type":"MYSQL"}'
        call = (
            partial(operations.create, payload_json=payload)
            if operation == "create"
            else partial(operations.update, datasource_id=41, payload_json=payload)
        )
        with pytest.raises(ApiTransportError) as caught:
            call()
    assert len(requests) == (1 if operation == "create" else 2)
    assert caught.value.details["field"] == "name"
    assert "phase" not in caught.value.details


@pytest.mark.parametrize(
    ("operation", "payload"),
    [("create", None), ("create", {}), ("update", None), ("update", {"id": 99})],
)
def test_invalid_entity_mutation_response_stops_before_readback(
    operation: str, payload: JsonValue
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with _client("3.2.1", handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        call = (
            partial(operations.create, payload_json=_PAYLOAD)
            if operation == "create"
            else partial(operations.update, datasource_id=41, payload_json=_PAYLOAD)
        )
        with pytest.raises(ApiTransportError) as caught:
            call()
    assert len(requests) == 1
    assert caught.value.details["phase"] == "mutation_response"
    assert caught.value.details["mutation_applied"] is True
    assert "mutation_may_have_applied" not in caught.value.details


@pytest.mark.parametrize("ds_version", ["2.0.0", "3.1.0"])
@pytest.mark.parametrize("matches", [0, 2])
def test_void_create_requires_one_exact_name_before_detail_readback(
    ds_version: str, matches: int
) -> None:
    requests: list[httpx.Request] = []
    source = _datasource(datasource_id=41, name="warehouse")

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return _success(None)
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/datasources"
        return _success(
            {
                "totalList": [_datasource_entity(source)] * matches,
                "total": matches,
                "totalPage": 1,
                "currentPage": 1,
                "pageSize": 100,
            }
        )

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        with pytest.raises(ApiTransportError) as caught:
            operations.create(payload_json=_PAYLOAD)
    assert len(requests) == 2
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["field"] == "mutationReadback"


@pytest.mark.parametrize("operation", ["create", "update"])
def test_entity_create_accepts_readback_name_but_update_checks_it(
    operation: str,
) -> None:
    source = _datasource(datasource_id=41, name="server-renamed")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(
            _datasource_entity(source)
            if len(requests) == 1
            else _datasource_detail(source, legacy=False)
        )

    with _client("3.2.1", handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        if operation == "create":
            result = operations.create(payload_json=_PAYLOAD)
            assert result.id == 41
            assert result.name == "server-renamed"
        else:
            with pytest.raises(ApiTransportError) as caught:
                operations.update(datasource_id=41, payload_json=_PAYLOAD)
            assert caught.value.details["phase"] == "readback"
            assert caught.value.details["field"] == "name"
    assert len(requests) == 2
    assert requests[1].method == "GET"
    assert requests[1].url.path == "/dolphinscheduler/datasources/41"


@pytest.mark.parametrize("ds_version", ["3.2.1", "3.4.2"])
@pytest.mark.parametrize("operation", ["delete", "connection_test"])
@pytest.mark.parametrize("payload", [False, None])
def test_boolean_false_is_a_result_but_null_is_a_nonretryable_response_error(
    ds_version: str, operation: str, *, payload: bool | None
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        call = (
            operations.delete if operation == "delete" else operations.connection_test
        )
        if payload is False:
            result = call(datasource_id=41)
            assert result is False
        else:
            with pytest.raises(ApiTransportError) as caught:
                call(datasource_id=41)
            assert caught.value.details.get("phase") == (
                "mutation_response" if operation == "delete" else None
            )
    assert len(requests) == 1


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.2.1"])
@pytest.mark.parametrize("operation", ["create", "update"])
@pytest.mark.parametrize("payload", ["{", "null"])
def test_invalid_payload_is_rejected_before_any_http(
    ds_version: str, operation: str, payload: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    with _client(ds_version, handler) as client:
        operations = DATASOURCE_DOMAIN.bind(
            client.profile, http_client=client
        ).datasources
        call = (
            partial(operations.create, payload_json=payload)
            if operation == "create"
            else partial(operations.update, datasource_id=41, payload_json=payload)
        )
        with pytest.raises(ApiTransportError) as caught:
            call()
    assert requests == []
    assert caught.value.details["field"] == "payload"
    assert "phase" not in caught.value.details
