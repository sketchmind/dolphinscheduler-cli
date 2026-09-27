from __future__ import annotations

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError, UserInputError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream import observability
from tests.support import make_profile

_AUDIT_ABSENT_VERSIONS = TARGET_DS_VERSIONS[: TARGET_DS_VERSIONS.index("3.0.0")]
_AUDIT_SINGULAR_VERSIONS = TARGET_DS_VERSIONS[
    TARGET_DS_VERSIONS.index("3.0.0") : TARGET_DS_VERSIONS.index("3.2.2")
]
_AUDIT_CSV_VERSIONS = TARGET_DS_VERSIONS[TARGET_DS_VERSIONS.index("3.2.2") :]


@pytest.mark.parametrize("ds_version", _AUDIT_SINGULAR_VERSIONS)
def test_legacy_audit_binding_projects_old_dto_and_singular_filters(
    ds_version: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(
            _audit_page(
                {
                    "userName": "alice",
                    "resource": "PROJECT_MODULE",
                    "resourceName": "daily-etl",
                    "operation": "UPDATE",
                    "time": "2026-08-05 10:00:00",
                }
            )
        )

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        page = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits.list(
                page_no=1,
                page_size=20,
                model_types=["PROJECT_MODULE"],
                operation_types=["UPDATE"],
                start_date="2026-08-05 09:00:00",
                end_date="2026-08-05 11:00:00",
                user_name="alice",
            )
        )

    assert [(item.method, item.url.path) for item in requests] == [
        ("GET", "/dolphinscheduler/projects/audit/audit-log-list")
    ]
    assert dict(requests[0].url.params) == {
        "pageNo": "1",
        "pageSize": "20",
        "resourceType": "PROJECT_MODULE",
        "operationType": "UPDATE",
        "startDate": "2026-08-05 09:00:00",
        "endDate": "2026-08-05 11:00:00",
        "userName": "alice",
    }
    assert page.totalList is not None
    item = page.totalList[0]
    assert item.modelType == "PROJECT_MODULE"
    assert item.modelName == "daily-etl"
    assert item.createTime == "2026-08-05 10:00:00"
    assert item.description is None
    assert item.detail is None
    assert item.latency is None

    assert page.total == page.totalPage == page.currentPage == 1


@pytest.mark.parametrize("ds_version", _AUDIT_SINGULAR_VERSIONS)
@pytest.mark.parametrize(
    ("model_types", "operation_types", "model_name", "message"),
    [
        (["USER_MODULE", "PROJECT_MODULE"], None, None, "accepts only one model type"),
        (None, ["READ", "UPDATE"], None, "accepts only one operation type"),
        (["UNKNOWN"], None, None, "Unsupported model type"),
        (None, ["UNKNOWN"], None, "Unsupported operation type"),
        (None, None, "daily-etl", "--model-name is unavailable"),
    ],
)
def test_legacy_audit_filters_fail_before_http(
    ds_version: str,
    model_types: list[str] | None,
    operation_types: list[str] | None,
    model_name: str | None,
    message: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(_unexpected_audit_http)
    ) as client:
        audits = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits
        )
        with pytest.raises(UserInputError, match=message):
            audits.list(
                page_no=1,
                page_size=20,
                model_types=model_types,
                operation_types=operation_types,
                model_name=model_name,
            )


@pytest.mark.parametrize("ds_version", _AUDIT_ABSENT_VERSIONS)
def test_audit_absence_fails_on_operation_without_http(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(_unexpected_audit_http)
    ) as client:
        audits = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits
        )
        with pytest.raises(UnsupportedFeatureError) as error:
            audits.list(page_no=1, page_size=20)
    assert error.value.details["action"] == "audit.list"
    assert error.value.details["introduced_in"] == "3.0.0"
    assert error.value.details["reason"] == "upstream_capability_absent"


@pytest.mark.parametrize(
    "ds_version", TARGET_DS_VERSIONS[: TARGET_DS_VERSIONS.index("3.2.2")]
)
def test_audit_metadata_absence_fails_without_http(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(_unexpected_audit_http)
    ) as client:
        audits = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits
        )
        with pytest.raises(UnsupportedFeatureError) as models_error:
            audits.list_model_types()
        with pytest.raises(UnsupportedFeatureError) as operations_error:
            audits.list_operation_types()
    assert models_error.value.details["action"] == "audit.model-types"
    assert operations_error.value.details["action"] == "audit.operation-types"
    assert models_error.value.details["introduced_in"] == "3.2.2"
    assert operations_error.value.details["introduced_in"] == "3.2.2"


@pytest.mark.parametrize("ds_version", _AUDIT_CSV_VERSIONS)
def test_audit_csv_filters_preserve_order_and_canonical_fields(ds_version: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(
            _audit_page(
                {
                    "userName": "alice",
                    "modelType": "Workflow",
                    "modelName": "daily-etl",
                    "operation": "Update",
                    "createTime": "2026-08-05 10:00:00",
                    "description": "changed",
                    "detail": "a → b",
                    "latency": "5",
                }
            )
        )

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        page = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits.list(
                page_no=1,
                page_size=20,
                model_types=["Task", "Workflow", "Task"],
                operation_types=["Update", "Read"],
                model_name="daily-etl",
            )
        )
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.path == "/dolphinscheduler/projects/audit/audit-log-list"
    assert dict(requests[0].url.params) == {
        "pageNo": "1",
        "pageSize": "20",
        "modelTypes": "Task,Workflow,Task",
        "operationTypes": "Update,Read",
        "modelName": "daily-etl",
    }
    assert page.totalList is not None
    assert page.totalList[0] == observability.AuditSnapshot(
        userName="alice",
        modelType="Workflow",
        modelName="daily-etl",
        operation="Update",
        createTime="2026-08-05 10:00:00",
        description="changed",
        detail="a → b",
        latency="5",
    )


@pytest.mark.parametrize("ds_version", _AUDIT_CSV_VERSIONS)
def test_audit_metadata_preserves_recursive_and_nullable_records(
    ds_version: str,
) -> None:
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert not request.url.params
        requests.append(request.url.path)
        if request.url.path == "/dolphinscheduler/projects/audit/audit-log-model-type":
            return _success(
                [
                    {
                        "name": "Workflow",
                        "child": [
                            {"name": "Task", "child": []},
                            {"name": None, "child": [{"name": "Leaf"}]},
                        ],
                    },
                    {},
                ]
            )
        if (
            request.url.path
            == "/dolphinscheduler/projects/audit/audit-log-operation-type"
        ):
            return _success([{"name": "Update"}, {}, {"name": "Read"}])
        return _unexpected_audit_http(request)

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        audits = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits
        )
        models = audits.list_model_types()
        operations = audits.list_operation_types()
    assert models == [
        observability.AuditModelTypeSnapshot(
            "Workflow",
            [
                observability.AuditModelTypeSnapshot("Task", []),
                observability.AuditModelTypeSnapshot(
                    None, [observability.AuditModelTypeSnapshot("Leaf", None)]
                ),
            ],
        ),
        observability.AuditModelTypeSnapshot(None, None),
    ]
    assert [operation.name for operation in operations] == ["Update", None, "Read"]
    assert requests == [
        "/dolphinscheduler/projects/audit/audit-log-model-type",
        "/dolphinscheduler/projects/audit/audit-log-operation-type",
    ]


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.0.1", "3.2.2", "3.4.2"])
def test_audit_read_retries_transport_failure_and_preserves_null_fields(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, json={"message": "temporary failure"})
        return _success(_audit_page({}))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        page = (
            observability.AuditAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .audits.list(page_no=1, page_size=20, model_types=[], operation_types=[])
        )
    assert len(requests) == 2
    assert requests[0].method == requests[1].method == "GET"
    assert requests[0].url == requests[1].url
    assert dict(requests[0].url.params) == {"pageNo": "1", "pageSize": "20"}
    assert page.totalList is not None
    assert page.totalList[0] == observability.AuditSnapshot(
        None, None, None, None, None, None, None, None
    )


@pytest.mark.parametrize("operation", ["page", "models", "operations"])
@pytest.mark.parametrize("payload", [None, "not-a-record", [None]])
def test_audit_rejects_malformed_responses_without_retry(
    operation: str, payload: object
) -> None:
    profile = make_profile(ds_version="3.4.2").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        audits = (
            observability.AuditAdapter.for_version("3.4.2")
            .bind(profile, http_client=client)
            .audits
        )
        invoke = {
            "page": lambda: audits.list(page_no=1, page_size=20),
            "models": audits.list_model_types,
            "operations": audits.list_operation_types,
        }[operation]
        with pytest.raises(ApiTransportError):
            invoke()
    assert len(requests) == 1


def _audit_page(item: dict[str, object]) -> dict[str, object]:
    return {"totalList": [item], "total": 1, "totalPage": 1, "currentPage": 1}


def _unexpected_audit_http(request: httpx.Request) -> httpx.Response:
    message = f"unexpected request {request.method} {request.url.path}"
    raise AssertionError(message)


@pytest.mark.parametrize(
    ("ds_version", "node_type", "path"),
    [
        ("1.3.9", "MASTER", "/dolphinscheduler/monitor/master/list"),
        ("1.3.9", "WORKER", "/dolphinscheduler/monitor/worker/list"),
        *(
            (version, node, f"/dolphinscheduler/monitor/{path}")
            for version in TARGET_DS_VERSIONS[
                TARGET_DS_VERSIONS.index("2.0.0") : TARGET_DS_VERSIONS.index("3.2.2")
            ]
            for node, path in (("MASTER", "masters"), ("WORKER", "workers"))
        ),
        *(
            (version, node, f"/dolphinscheduler/monitor/{node}")
            for version in TARGET_DS_VERSIONS[TARGET_DS_VERSIONS.index("3.2.2") :]
            for node in ("MASTER", "WORKER", "ALERT_SERVER")
        ),
    ],
)
def test_monitor_server_routes_and_native_field_epochs(
    ds_version: str, node_type: str, path: str
) -> None:
    payload: dict[str, object] = {
        "id": 2,
        "host": "node-1",
        "port": 1234,
        "createTime": "2026-08-05 10:00:00",
        "lastHeartbeatTime": "2026-08-05 10:01:00",
    }
    worker_collection = (
        ds_version in TARGET_DS_VERSIONS[: TARGET_DS_VERSIONS.index("3.2.2")]
        and node_type == "WORKER"
    )
    if ds_version in TARGET_DS_VERSIONS[TARGET_DS_VERSIONS.index("3.3.1") :]:
        payload.update(serverDirectory="/ds/node", heartBeatInfo="healthy")
    elif worker_collection:
        payload.update(
            zkDirectories=["/ds/workers/b", "/ds/workers/a", "/ds/workers/b"],
            resInfo="healthy",
        )
    else:
        payload.update(zkDirectory="/ds/node", resInfo="healthy")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success([payload, {}])

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        servers = observability.MONITOR_DOMAIN.bind(
            profile, http_client=client
        ).monitor.list_servers(node_type=node_type)

    assert [(item.method, item.url.path) for item in requests] == [("GET", path)]
    assert not requests[0].url.query
    assert requests[0].content == b""
    assert servers[0] == observability.MonitorServerSnapshot(
        id=2,
        host="node-1",
        port=1234,
        serverDirectories=(
            ["/ds/workers/a", "/ds/workers/b"] if worker_collection else ["/ds/node"]
        ),
        serverDirectory=None if worker_collection else "/ds/node",
        heartBeatInfo="healthy",
        createTime="2026-08-05 10:00:00",
        lastHeartbeatTime="2026-08-05 10:01:00",
    )
    assert servers[1] == observability.MonitorServerSnapshot(
        0, None, 0, [], None, None, None, None
    )


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_monitor_database_routes_nullable_fields_and_defaults(ds_version: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(
            [
                {
                    "dbType": "MYSQL",
                    "state": "YES",
                    "maxConnections": 10,
                    "maxUsedConnections": 2,
                    "threadsConnections": 1,
                    "threadsRunningConnections": 0,
                    "date": "2026-08-05 10:00:00",
                },
                {"dbType": None, "state": None, "date": None},
            ]
        )

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        databases = observability.MONITOR_DOMAIN.bind(
            profile, http_client=client
        ).monitor.list_databases()

    path = (
        "/dolphinscheduler/monitor/database"
        if ds_version == "1.3.9"
        else "/dolphinscheduler/monitor/databases"
    )
    assert [(item.method, item.url.path) for item in requests] == [("GET", path)]
    assert not requests[0].url.query
    assert requests[0].content == b""
    assert databases == [
        observability.MonitorDatabaseSnapshot(
            "MYSQL", "YES", 10, 2, 1, 0, "2026-08-05 10:00:00"
        ),
        observability.MonitorDatabaseSnapshot(None, None, 0, 0, 0, 0, None),
    ]


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.0", "2.0.1", "3.2.1"])
@pytest.mark.parametrize(
    ("directories", "expected_directories", "expected_scalar"),
    [
        (None, [], None),
        ([], [], None),
        (["/ds/worker", "/ds/worker"], ["/ds/worker"], "/ds/worker"),
    ],
)
def test_legacy_monitor_worker_directory_cardinality(
    ds_version: str,
    directories: list[str] | None,
    expected_directories: list[str],
    expected_scalar: str | None,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return _success([{"zkDirectories": directories}])

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        server = observability.MONITOR_DOMAIN.bind(
            profile, http_client=client
        ).monitor.list_servers(node_type="WORKER")[0]

    assert list(server.serverDirectories) == expected_directories
    assert server.serverDirectory == expected_scalar


@pytest.mark.parametrize(
    ("ds_version", "node_type"),
    [
        *((version, "MASTER/WORKER") for version in TARGET_DS_VERSIONS),
        *(
            (version, "ALERT_SERVER")
            for version in TARGET_DS_VERSIONS[: TARGET_DS_VERSIONS.index("3.2.2")]
        ),
    ],
)
def test_monitor_invalid_node_type_fails_before_http(
    ds_version: str, node_type: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        message = f"unexpected monitor request {request.method} {request.url.path}"
        raise AssertionError(message)

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        monitor = observability.MONITOR_DOMAIN.bind(profile, http_client=client).monitor
        with pytest.raises(UserInputError) as error:
            monitor.list_servers(node_type=node_type)

    assert error.value.message == (
        f"Monitor node type {node_type!r} is unavailable for the selected version"
    )
    assert error.value.details == {
        "node_type": node_type,
        "selected_version": ds_version,
    }
    assert error.value.suggestion == (
        "Retry with one of: master, worker."
        if ds_version in TARGET_DS_VERSIONS[: TARGET_DS_VERSIONS.index("3.2.2")]
        else "Retry with one of: master, worker, alert-server."
    )


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.2.2", "3.3.1"])
@pytest.mark.parametrize("operation", ["servers", "databases"])
@pytest.mark.parametrize("malformation", ["null", "scalar", "item", "integer"])
def test_monitor_malformed_response_fails_without_retry(
    ds_version: str, operation: str, malformation: str
) -> None:
    field = "id" if operation == "servers" else "maxConnections"
    payload: object = {
        "null": None,
        "scalar": "not-a-list",
        "item": [None],
        "integer": [{field: None}],
    }[malformation]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        monitor = observability.MONITOR_DOMAIN.bind(profile, http_client=client).monitor
        invoke = (
            (lambda: monitor.list_servers(node_type="MASTER"))
            if operation == "servers"
            else monitor.list_databases
        )
        with pytest.raises(ApiTransportError):
            invoke()
    assert len(requests) == 1


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.2.2", "3.3.1"])
@pytest.mark.parametrize(
    ("operation", "payload", "first_field"),
    [
        ("servers", {"id": -1, "port": -2}, "id"),
        (
            "databases",
            {"maxConnections": -1, "maxUsedConnections": -2},
            "maxConnections",
        ),
    ],
)
def test_monitor_projection_error_preserves_field_precedence(
    ds_version: str, operation: str, payload: dict[str, object], first_field: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success([payload])

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        monitor = observability.MONITOR_DOMAIN.bind(profile, http_client=client).monitor
        invoke = (
            (lambda: monitor.list_servers(node_type="MASTER"))
            if operation == "servers"
            else monitor.list_databases
        )
        with pytest.raises(ApiTransportError) as error:
            invoke()
    assert error.value.details["field"] == first_field
    assert len(requests) == 1


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.2.2", "3.3.1"])
@pytest.mark.parametrize("operation", ["servers", "databases"])
def test_monitor_reads_retry_transient_http_failure(
    ds_version: str, operation: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, json={"message": "temporary failure"})
        return _success([])

    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        monitor = observability.MONITOR_DOMAIN.bind(profile, http_client=client).monitor
        result = (
            monitor.list_servers(node_type="MASTER")
            if operation == "servers"
            else monitor.list_databases()
        )

    assert result == []
    assert len(requests) == 2
    assert requests[0].method == requests[1].method == "GET"
    assert requests[0].url == requests[1].url
    assert requests[0].content == requests[1].content == b""


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.4.2"])
def test_monitor_database_external_type_epoch_keeps_native_payload(
    ds_version: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return _success([{"dbType": "FUTURE_DATABASE", "state": "YES"}])

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        monitor = observability.MONITOR_DOMAIN.bind(profile, http_client=client).monitor
        if ds_version == "3.2.0":
            with pytest.raises(ApiTransportError):
                monitor.list_databases()
        else:
            item = monitor.list_databases()[0]
            assert item.dbType == "FUTURE_DATABASE"
            assert item.state == "YES"


def test_current_runtime_slice_binds_audit_and_monitor_without_broad_adapter() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        if request.url.path == "/dolphinscheduler/projects/audit/audit-log-list":
            return _success(
                {
                    "totalList": [
                        {
                            "userName": "alice",
                            "modelType": "Workflow",
                            "modelName": "daily-etl",
                            "operation": "Update",
                            "createTime": "2026-08-05 10:00:00",
                            "description": None,
                            "detail": None,
                            "latency": "5",
                        }
                    ],
                    "total": 1,
                    "totalPage": 1,
                    "currentPage": 1,
                }
            )
        if request.url.path == "/dolphinscheduler/monitor/MASTER":
            return _success(
                [
                    {
                        "id": 1,
                        "host": "master-1",
                        "port": 5678,
                        "serverDirectory": "/ds/master",
                        "heartBeatInfo": "healthy",
                        "createTime": None,
                        "lastHeartbeatTime": None,
                    }
                ]
            )
        if request.url.path == "/dolphinscheduler/monitor/databases":
            return _success(
                [
                    {
                        "dbType": "MYSQL",
                        "state": "YES",
                        "maxConnections": 10,
                        "maxUsedConnections": 2,
                        "threadsConnections": 1,
                        "threadsRunningConnections": 1,
                        "date": None,
                    }
                ]
            )
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    ) as http_client:
        audits = observability.AuditAdapter("3.4.1").bind(
            profile,
            http_client=http_client,
        )
        monitor = observability.MonitorAdapter("3.4.1").bind(
            profile,
            http_client=http_client,
        )
        audit_page = audits.audits.list(
            page_no=1,
            page_size=20,
            model_types=["Workflow", "Task"],
            operation_types=["Update"],
        )
        server = monitor.monitor.list_servers(node_type="MASTER")[0]
        database = monitor.monitor.list_databases()[0]

    audit_request = requests_seen[0]
    assert audit_request.url.params["modelTypes"] == "Workflow,Task"
    assert audit_page.totalList is not None
    assert audit_page.totalList[0].modelName == "daily-etl"
    assert list(server.serverDirectories) == ["/ds/master"]
    assert database.maxConnections == 10


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
