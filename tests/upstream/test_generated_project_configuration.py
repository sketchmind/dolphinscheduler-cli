from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError, UnsupportedFeatureError
from dsctl.upstream.project_parameters import (
    PROJECT_PARAMETER_DOMAIN,
)
from dsctl.upstream.project_preferences import (
    PROJECT_PREFERENCE_DOMAIN,
)
from dsctl.upstream.project_worker_groups import (
    PROJECT_WORKER_GROUP_DOMAIN,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.config import ClusterProfile

_PARAMETER_VERSIONS = (
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_WORKER_GROUP_VERSIONS = (
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)
_EARLY_VERSIONS = ("1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9")
_MUTATIONS = (
    "parameter-create",
    "parameter-update",
    "parameter-delete",
    "preference-update",
    "preference-state",
    "worker-group-set",
)


@pytest.mark.parametrize("ds_version", _PARAMETER_VERSIONS)
def test_project_parameter_lifecycle_uses_exact_http_and_no_extra_readback(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        values = parse_qs(
            request.content.decode() or request.url.query.decode(),
            keep_blank_values=True,
        )
        requests_seen.append((request.method, request.url.path, values))
        if request.method == "GET" and request.url.path.endswith("/project-parameter"):
            return _success(_page(_parameter(code=11, name="warehouse")))
        if request.method == "GET":
            return _success(_parameter(code=11, name="warehouse"))
        if request.method == "POST" and not request.url.path.endswith("/delete"):
            return _success(_parameter(code=12, name="created"))
        if request.method == "PUT":
            return _success(_parameter(code=11, name="renamed"))
        if request.method == "POST" and request.url.path.endswith("/delete"):
            return _success(None)
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        parameters = PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters
        page = parameters.list(
            project_code=7,
            page_no=2,
            page_size=25,
            search="ware",
            data_type="VARCHAR",
        )
        fetched = parameters.get(project_code=7, code=11)
        created = parameters.create(
            project_code=7,
            name="created",
            value="v",
            data_type="VARCHAR",
        )
        updated = parameters.update(
            project_code=7,
            code=11,
            name="renamed",
            value="v2",
            data_type="VARCHAR",
        )
        deleted = parameters.delete(project_code=7, code=11)

    assert page.total == 1
    assert page.totalList is not None
    assert [item.paramName for item in page.totalList] == ["warehouse"]
    assert fetched.code == 11
    assert fetched.paramDataType == "VARCHAR"
    assert created.paramName == "created"
    assert updated.paramName == "renamed"
    assert deleted is True
    data_type_field = (
        {}
        if ds_version in {"3.2.0", "3.2.1", "3.2.2"}
        else {"projectParameterDataType": ["VARCHAR"]}
    )
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7/project-parameter",
            {
                "searchVal": ["ware"],
                **data_type_field,
                "pageNo": ["2"],
                "pageSize": ["25"],
            },
        ),
        (
            "GET",
            "/dolphinscheduler/projects/7/project-parameter/11",
            {},
        ),
        (
            "POST",
            "/dolphinscheduler/projects/7/project-parameter",
            {
                "projectParameterName": ["created"],
                "projectParameterValue": ["v"],
                **data_type_field,
            },
        ),
        (
            "PUT",
            "/dolphinscheduler/projects/7/project-parameter/11",
            {
                "projectParameterName": ["renamed"],
                "projectParameterValue": ["v2"],
                **data_type_field,
            },
        ),
        (
            "POST",
            "/dolphinscheduler/projects/7/project-parameter/delete",
            {"code": ["11"]},
        ),
    ]


@pytest.mark.parametrize("ds_version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"])
def test_modern_project_parameter_rejects_a_missing_data_type(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        payload = _parameter(code=11, name="warehouse")
        payload.pop("paramDataType")
        return _success(payload)

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        parameters = PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters
        with pytest.raises(ApiTransportError) as exc_info:
            parameters.get(project_code=7, code=11)

    assert exc_info.value.details["field"] == "paramDataType"


@pytest.mark.parametrize("ds_version", _PARAMETER_VERSIONS)
def test_project_preference_lifecycle_uses_exact_http_and_no_extra_readback(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        values = parse_qs(request.content.decode(), keep_blank_values=True)
        requests_seen.append((request.method, request.url.path, values))
        if request.method == "GET":
            return _success(_preference(preferences='{"priority":"HIGH"}'))
        if request.method == "PUT":
            return _success(_preference(preferences='{"priority":"LOW"}'))
        return _success(None)

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        preferences = PROJECT_PREFERENCE_DOMAIN.bind(
            profile, http_client=client
        ).preferences
        fetched = preferences.get(project_code=7)
        updated = preferences.update(
            project_code=7,
            preferences='{"priority":"LOW"}',
        )
        preferences.set_state(project_code=7, state=0)
        preferences.set_state(project_code=7, state=1)

    assert fetched is not None
    assert fetched.state == 1
    assert updated.preferences == '{"priority":"LOW"}'
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/project-preference", {}),
        (
            "PUT",
            "/dolphinscheduler/projects/7/project-preference",
            {"projectPreferences": ['{"priority":"LOW"}']},
        ),
        ("POST", "/dolphinscheduler/projects/7/project-preference", {"state": ["0"]}),
        ("POST", "/dolphinscheduler/projects/7/project-preference", {"state": ["1"]}),
    ]


@pytest.mark.parametrize("ds_version", _WORKER_GROUP_VERSIONS)
def test_project_worker_group_preserves_csv_order_duplicates_and_clear(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    request_bodies: list[dict[str, list[str]]] = []
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "status": "SUCCESS",
                    "msg": "success",
                    "data": [
                        _worker_group("default"),
                        _worker_group("gpu"),
                        _worker_group("default"),
                    ],
                },
            )
        request_bodies.append(
            parse_qs(request.content.decode(), keep_blank_values=True)
        )
        return _success(None)

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        worker_groups = PROJECT_WORKER_GROUP_DOMAIN.bind(
            profile, http_client=client
        ).worker_groups
        current = worker_groups.list(project_code=7)
        worker_groups.set(project_code=7, worker_groups=["gpu", "default", "gpu"])
        worker_groups.set(project_code=7, worker_groups=[])
        worker_groups.set(project_code=7, worker_groups=[""])

    assert [item.workerGroup for item in current] == ["default", "gpu", "default"]
    assert request_bodies == [
        {"workerGroups": ["gpu,default,gpu"]},
        {"workerGroups": [""]},
        {"workerGroups": [""]},
    ]
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/worker-group"),
        ("POST", "/dolphinscheduler/projects/7/worker-group"),
        ("POST", "/dolphinscheduler/projects/7/worker-group"),
        ("POST", "/dolphinscheduler/projects/7/worker-group"),
    ]


@pytest.mark.parametrize(
    ("resource", "version", "introduced_in"),
    [
        *(("project-parameter", version, "3.2.0") for version in _EARLY_VERSIONS),
        *(("project-preference", version, "3.2.0") for version in _EARLY_VERSIONS),
        *(
            ("project-worker-group", version, "3.2.2")
            for version in (*_EARLY_VERSIONS, "3.2.0", "3.2.1")
        ),
    ],
)
def test_absent_project_configuration_domain_fails_before_transport(
    resource: str,
    version: str,
    introduced_in: str,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _success(None)

    profile = make_profile(ds_version=version)
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    def bind() -> None:
        if resource == "project-parameter":
            PROJECT_PARAMETER_DOMAIN.bind(profile, http_client=client)
        elif resource == "project-preference":
            PROJECT_PREFERENCE_DOMAIN.bind(profile, http_client=client)
        else:
            PROJECT_WORKER_GROUP_DOMAIN.bind(profile, http_client=client)

    with client, pytest.raises(UnsupportedFeatureError) as exc_info:
        bind()

    assert calls == 0
    assert exc_info.value.details["reason"] == "upstream_capability_absent"
    assert exc_info.value.details["introduced_in"] == introduced_in
    assert exc_info.value.details["resource"] == resource
    assert exc_info.value.details["ds_version"] == version


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.2.2"])
@pytest.mark.parametrize("operation", ["list", "create", "update"])
def test_legacy_parameter_rejects_other_data_types_without_io(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(None)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        parameters = PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters

        def dispatch() -> None:
            if operation == "list":
                parameters.list(
                    project_code=7, page_no=1, page_size=10, data_type="INTEGER"
                )
            elif operation == "create":
                parameters.create(
                    project_code=7, name="n", value="1", data_type="INTEGER"
                )
            else:
                parameters.update(
                    project_code=7, code=11, name="n", value="1", data_type="INTEGER"
                )

        with pytest.raises(UnsupportedFeatureError) as caught:
            dispatch()

    assert requests == []
    assert caught.value.details["reason"] == "upstream_option_absent"
    assert caught.value.details["requested"] == "INTEGER"
    assert caught.value.details["supported"] == ["VARCHAR"]


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.2.2"])
def test_legacy_parameter_defaults_type_and_accepts_lowercase_varchar(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []
    payload = {"code": 11, "projectCode": 7, "paramName": "n"}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(_page(payload) if request.method == "GET" else payload)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        parameters = PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters
        page = parameters.list(project_code=7, page_no=1, page_size=10)
        created = parameters.create(
            project_code=7, name="n", value="", data_type="varchar"
        )

    assert page.totalList is not None
    assert [(item.paramDataType, item.paramValue) for item in page.totalList] == [
        ("VARCHAR", None)
    ]
    assert (created.paramDataType, created.paramValue, created.operator) == (
        "VARCHAR",
        None,
        None,
    )
    assert parse_qs(requests[0].url.query.decode()) == {
        "pageNo": ["1"],
        "pageSize": ["10"],
    }
    assert parse_qs(requests[1].content.decode(), keep_blank_values=True) == {
        "projectParameterName": ["n"],
        "projectParameterValue": [""],
    }


@pytest.mark.parametrize("ds_version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"])
def test_typed_parameter_forwards_type_spelling_and_normalizes_response(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []
    payload = _parameter(code=11, name="n") | {
        "paramDataType": "integer",
        "operator": 9,
        "createUser": "alice",
        "modifyUser": "bob",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        created = PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters.create(project_code=7, name="n", value="1", data_type="integer")

    assert (
        created.paramDataType,
        created.operator,
        created.createUser,
        created.modifyUser,
    ) == ("INTEGER", 9, "alice", "bob")
    assert len(requests) == 1
    assert parse_qs(requests[0].content.decode()) == {
        "projectParameterName": ["n"],
        "projectParameterValue": ["1"],
        "projectParameterDataType": ["integer"],
    }


@pytest.mark.parametrize("ds_version", _PARAMETER_VERSIONS)
def test_preference_get_allows_no_record_and_defaults_nullable_fields(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    responses = iter(
        (
            None,
            {"code": 11, "projectCode": 7},
            {"code": 11, "projectCode": 7, "state": -1},
        )
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(next(responses))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        preferences = PROJECT_PREFERENCE_DOMAIN.bind(
            profile, http_client=client
        ).preferences
        assert preferences.get(project_code=7) is None
        defaulted = preferences.get(project_code=7)
        negative_state = preferences.get(project_code=7)

    assert defaulted is not None
    assert (
        defaulted.code,
        defaulted.projectCode,
        defaulted.state,
        defaulted.preferences,
    ) == (11, 7, 0, None)
    assert (
        defaulted.id,
        defaulted.userId,
        defaulted.createTime,
        defaulted.updateTime,
    ) == (None, None, None, None)
    assert negative_state is not None
    assert negative_state.state == -1
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/7/project-preference")
    ] * 3


@pytest.mark.parametrize("operation", _MUTATIONS)
@pytest.mark.parametrize("failure", ["lost-response", "http-503"])
def test_project_configuration_writes_never_retry_or_read_back_after_dispatch_failure(
    operation: str,
    failure: str,
) -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if failure == "http-503":
            return httpx.Response(503, text="unavailable")
        message = "response lost"
        raise httpx.ReadError(message, request=request)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        _mutate(operation, profile=profile, client=client)

    assert len(requests) == 1
    assert requests[0].method != "GET"
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True


@pytest.mark.parametrize("operation", _MUTATIONS)
def test_project_configuration_writes_preserve_definitive_upstream_failure(
    operation: str,
) -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"code": 1234, "msg": "denied", "data": {"reason": "permission"}}
        )

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiResultError) as caught,
    ):
        _mutate(operation, profile=profile, client=client)

    assert len(requests) == 1
    assert caught.value.result_code == 1234
    assert caught.value.result_message == "denied"
    assert caught.value.data == {"reason": "permission"}
    assert "phase" not in caught.value.details


@pytest.mark.parametrize(
    ("operation", "field", "value"),
    [
        ("parameter-create", "paramName", "other"),
        ("parameter-create", "paramDataType", "INTEGER"),
        ("parameter-update", "code", 12),
        ("parameter-update", "projectCode", 8),
        ("preference-update", "projectCode", 8),
        ("preference-update", "preferences", "different"),
    ],
)
def test_project_configuration_checks_mutation_response_without_an_extra_get(
    operation: str,
    field: str,
    value: object,
) -> None:
    profile = make_profile(ds_version="3.4.1")
    requests: list[httpx.Request] = []
    payload = (
        _preference(preferences="{}")
        if operation == "preference-update"
        else _parameter(code=11, name="n")
    )
    payload[field] = value

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        _mutate(operation, profile=profile, client=client)

    assert len(requests) == 1
    assert requests[0].method != "GET"
    assert caught.value.details["field"] == field
    assert caught.value.details["phase"] == "mutation_response"
    assert caught.value.details["mutation_applied"] is True


@pytest.mark.parametrize(
    "operation", ["parameter-delete", "preference-state", "worker-group-set"]
)
def test_void_project_configuration_mutations_ignore_success_data(
    operation: str,
) -> None:
    profile = make_profile(ds_version="3.4.1")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success({"unexpected": "ignored by the native void contract"})

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        result = _mutate(operation, profile=profile, client=client)

    assert result is (True if operation == "parameter-delete" else None)
    assert len(requests) == 1


@pytest.mark.parametrize("resource", ["parameter", "preference", "worker-group"])
def test_project_configuration_reads_retry_transient_http_failures(
    resource: str,
) -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, text="unavailable")
        if resource == "parameter":
            return _success(_parameter(code=11, name="n"))
        if resource == "preference":
            return _success(None)
        return httpx.Response(200, json={"status": "SUCCESS", "data": []})

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        if resource == "parameter":
            result = PROJECT_PARAMETER_DOMAIN.bind(
                profile, http_client=client
            ).parameters.get(project_code=7, code=11)
            assert result.paramName == "n"
        elif resource == "preference":
            assert (
                PROJECT_PREFERENCE_DOMAIN.bind(
                    profile, http_client=client
                ).preferences.get(project_code=7)
                is None
            )
        else:
            assert (
                PROJECT_WORKER_GROUP_DOMAIN.bind(
                    profile, http_client=client
                ).worker_groups.list(project_code=7)
                == []
            )

    assert [request.method for request in requests] == ["GET", "GET"]
    assert requests[0].url == requests[1].url


@pytest.mark.parametrize(
    ("resource", "payload", "field"),
    [
        (
            "parameter",
            {
                "code": 12,
                "projectCode": 7,
                "paramName": "n",
                "paramDataType": "VARCHAR",
            },
            "code",
        ),
        (
            "parameter",
            {
                "code": 11,
                "projectCode": 8,
                "paramName": "n",
                "paramDataType": "VARCHAR",
            },
            "projectCode",
        ),
        ("preference", {"code": 0, "projectCode": 7}, "code"),
        ("preference", {"code": 11, "projectCode": 8}, "projectCode"),
        ("worker-group", [{"projectCode": 8, "workerGroup": "gpu"}], "projectCode"),
        ("worker-group", [{"projectCode": 7, "workerGroup": ""}], "workerGroup"),
    ],
)
def test_project_configuration_bad_read_projection_does_not_retry(
    resource: str,
    payload: object,
    field: str,
) -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    def read() -> None:
        if resource == "parameter":
            PROJECT_PARAMETER_DOMAIN.bind(profile, http_client=client).parameters.get(
                project_code=7, code=11
            )
        elif resource == "preference":
            PROJECT_PREFERENCE_DOMAIN.bind(profile, http_client=client).preferences.get(
                project_code=7
            )
        else:
            PROJECT_WORKER_GROUP_DOMAIN.bind(
                profile, http_client=client
            ).worker_groups.list(project_code=7)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        read()

    assert len(requests) == 1
    assert caught.value.details["field"] == field
    assert "phase" not in caught.value.details


@pytest.mark.parametrize(
    "payload", [None, [], {"code": 11, "projectCode": 7, "state": None}]
)
def test_preference_update_malformed_response_is_uncertain_not_replayed(
    payload: object,
) -> None:
    profile = make_profile(ds_version="3.4.1")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as caught,
    ):
        _mutate("preference-update", profile=profile, client=client)

    assert len(requests) == 1
    assert caught.value.details["phase"] == "mutation_response"
    assert caught.value.details["mutation_applied"] is True


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {
                "status": "SUCCESS",
                "dataList": [{"projectCode": 7, "workerGroup": "gpu"}],
            },
            ["gpu"],
        ),
        (
            {
                "status": "SUCCESS",
                "data": [],
                "dataList": [{"projectCode": 7, "workerGroup": "ignored"}],
            },
            [],
        ),
    ],
)
def test_worker_group_status_envelope_preserves_data_precedence(
    payload: dict[str, object], expected: list[str]
) -> None:
    profile = make_profile(ds_version="3.2.2")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        groups = PROJECT_WORKER_GROUP_DOMAIN.bind(
            profile, http_client=client
        ).worker_groups.list(project_code=7)

    assert [group.workerGroup for group in groups] == expected


@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE"])
def test_worker_group_null_data_does_not_fall_back_or_retry(status: str) -> None:
    profile = make_profile(ds_version="3.2.2")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "status": status,
                "msg": "lookup failed",
                "data": None,
                "dataList": [],
            },
        )

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError if status == "SUCCESS" else ApiResultError),
    ):
        PROJECT_WORKER_GROUP_DOMAIN.bind(
            profile, http_client=client
        ).worker_groups.list(project_code=7)

    assert len(requests) == 1


def test_project_configuration_keeps_long_project_and_parameter_codes_in_paths() -> (
    None
):
    profile = make_profile(ds_version="3.4.1")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(
            {
                "projectCode": 9_007_199_254_740_991,
                "code": 9_007_199_254_740_992,
                "paramName": "n",
                "paramDataType": "VARCHAR",
            }
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        parameter = PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters.get(project_code=9_007_199_254_740_991, code=9_007_199_254_740_992)

    assert parameter.code == 9_007_199_254_740_992
    assert [request.url.path for request in requests] == [
        "/dolphinscheduler/projects/9007199254740991/project-parameter/9007199254740992"
    ]


def _mutate(
    operation: str, *, profile: ClusterProfile, client: DolphinSchedulerClient
) -> object:
    if operation == "parameter-create":
        return PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters.create(project_code=7, name="n", value="v", data_type="VARCHAR")
    if operation == "parameter-update":
        return PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters.update(
            project_code=7, code=11, name="n", value="v", data_type="VARCHAR"
        )
    if operation == "parameter-delete":
        return PROJECT_PARAMETER_DOMAIN.bind(
            profile, http_client=client
        ).parameters.delete(project_code=7, code=11)
    if operation == "preference-update":
        return PROJECT_PREFERENCE_DOMAIN.bind(
            profile, http_client=client
        ).preferences.update(project_code=7, preferences="{}")
    if operation == "preference-state":
        PROJECT_PREFERENCE_DOMAIN.bind(
            profile, http_client=client
        ).preferences.set_state(project_code=7, state=0)
        return None
    assert operation == "worker-group-set"
    PROJECT_WORKER_GROUP_DOMAIN.bind(profile, http_client=client).worker_groups.set(
        project_code=7, worker_groups=["gpu"]
    )
    return None


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _parameter(*, code: int, name: str) -> dict[str, object]:
    return {
        "id": code,
        "code": code,
        "projectCode": 7,
        "paramName": name,
        "paramValue": "v",
        "paramDataType": "VARCHAR",
    }


def _preference(*, preferences: str) -> dict[str, object]:
    return {
        "id": 1,
        "code": 11,
        "projectCode": 7,
        "preferences": preferences,
        "state": 1,
    }


def _worker_group(name: str) -> dict[str, object]:
    return {"id": 1, "projectCode": 7, "workerGroup": name}


def _page(item: dict[str, object]) -> dict[str, object]:
    return {
        "totalList": [item],
        "total": 1,
        "totalPage": 1,
        "pageSize": 25,
        "currentPage": 2,
        "pageNo": 2,
    }
