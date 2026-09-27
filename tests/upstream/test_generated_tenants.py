from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.tenants import TenantAdapter, TenantDomain
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_NULL_CREATE_VERSIONS = frozenset({"1.3.9", "2.0.0"})
_NULL_MUTATION_VERSIONS = frozenset(
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


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_tenant_domain_pages_through_its_exact_profile(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success({"totalList": [], "total": 0, "totalPage": 0, "currentPage": 2})

    adapter = TenantAdapter.for_version(ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = adapter.bind(profile, http_client=client)
        assert requests_seen == []
        page = domain.tenants.list(page_no=2, page_size=17, search="tenant-ops")

    assert adapter.ds_version == ds_version
    assert page.currentPage == 2
    assert page.pageSize == 17
    assert len(requests_seen) == 1
    request = requests_seen[0]
    assert request.method == "GET"
    assert request.url.path == (
        "/dolphinscheduler/tenant/list-paging"
        if ds_version == "1.3.9"
        else "/dolphinscheduler/tenants"
    )
    assert parse_qs(request.url.query.decode()) == {
        "pageNo": ["2"],
        "pageSize": ["17"],
        "searchVal": ["tenant-ops"],
    }


def test_139_create_synthesizes_legacy_name_and_reads_back_canonical_tenant() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, form))
        if request.method == "POST":
            assert form["tenantName"] == ["tenant-ops"]
            return _success(None)
        if request.method == "GET":
            return _success(
                {
                    "totalList": [
                        {
                            "id": 7,
                            "tenantCode": "tenant-ops",
                            "tenantName": "tenant-ops",
                            "description": "ops tenant",
                            "queueId": 11,
                            "queueName": "default",
                            "queue": "root.default",
                        }
                    ],
                    "total": 1,
                    "totalPage": 1,
                    "currentPage": 1,
                }
            )
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = TenantAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, TenantDomain)
        created = domain.tenants.create(
            tenant_code="tenant-ops",
            queue_id=11,
            description="ops tenant",
        )

    assert created.id == 7
    assert created.tenantCode == "tenant-ops"
    assert created.queueId == 11
    assert requests_seen == [
        (
            "POST",
            "/dolphinscheduler/tenant/create",
            {
                "tenantCode": ["tenant-ops"],
                "tenantName": ["tenant-ops"],
                "queueId": ["11"],
                "description": ["ops tenant"],
            },
        ),
        ("GET", "/dolphinscheduler/tenant/list-paging", {}),
    ]


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_tenant_domain_executes_the_reviewed_crud_recipe(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    legacy = ds_version == "1.3.9"
    tenant_path = "/dolphinscheduler/tenant" if legacy else "/dolphinscheduler/tenants"
    queue_path = (
        "/dolphinscheduler/queue/list-paging" if legacy else "/dolphinscheduler/queues"
    )
    current: dict[str, object] | None = None
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def tenant_payload(
        form: dict[str, list[str]],
        *,
        tenant_id: int,
    ) -> dict[str, object]:
        queue_id = int(form["queueId"][0])
        payload: dict[str, object] = {
            "id": tenant_id,
            "tenantCode": form["tenantCode"][0],
            "description": form.get("description", [None])[0],
            "queueId": queue_id,
            "queueName": "default" if queue_id == 11 else "analytics",
            "queue": "root.default" if queue_id == 11 else "root.analytics",
            "createTime": None,
            "updateTime": None,
        }
        if legacy:
            payload["tenantName"] = form["tenantName"][0]
        return payload

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, form))
        if request.method == "GET" and request.url.path == queue_path:
            return _success(
                {
                    "totalList": [
                        {
                            "id": 11,
                            "queueName": "default",
                            "queue": "root.default",
                        },
                        {
                            "id": 12,
                            "queueName": "analytics",
                            "queue": "root.analytics",
                        },
                    ],
                    "total": 2,
                    "totalPage": 1,
                    "currentPage": 1,
                }
            )
        list_path = f"{tenant_path}/list-paging" if legacy else tenant_path
        if request.method == "GET" and request.url.path == list_path:
            items = [] if current is None else [current]
            return _success(
                {
                    "totalList": items,
                    "total": len(items),
                    "totalPage": 0 if current is None else 1,
                    "currentPage": 1,
                }
            )
        create_path = f"{tenant_path}/create" if legacy else tenant_path
        if request.method == "POST" and request.url.path == create_path:
            current = tenant_payload(form, tenant_id=8)
            result = None if ds_version in _NULL_CREATE_VERSIONS else current
            return _success(result)
        update_path = f"{tenant_path}/update" if legacy else f"{tenant_path}/8"
        update_method = "POST" if legacy else "PUT"
        if request.method == update_method and request.url.path == update_path:
            current = tenant_payload(form, tenant_id=8)
            return _success(None if ds_version in _NULL_MUTATION_VERSIONS else True)
        delete_path = f"{tenant_path}/delete" if legacy else f"{tenant_path}/8"
        delete_method = "POST" if legacy else "DELETE"
        if request.method == delete_method and request.url.path == delete_path:
            current = None
            return _success(None if ds_version in _NULL_MUTATION_VERSIONS else True)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = TenantAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        queues = domain.queues.list(page_no=1, page_size=100)
        assert [queue.id for queue in queues.totalList or []] == [11, 12]
        created = domain.tenants.create(
            tenant_code="tenant-new",
            queue_id=11,
            description="created",
        )
        assert created.id is not None
        updated = domain.tenants.update(
            tenant_id=created.id,
            current_tenant_code="tenant-new",
            queue_id=12,
            description="updated",
        )
        assert updated.id is not None
        deleted = domain.tenants.delete(tenant_id=updated.id)

    assert created.tenantCode == "tenant-new"
    assert updated.tenantCode == "tenant-new"
    assert updated.queueId == 12
    assert deleted is True
    mutations = [request for request in requests_seen if request[0] != "GET"]
    if legacy:
        assert [(method, path) for method, path, _form in mutations] == [
            ("POST", "/dolphinscheduler/tenant/create"),
            ("POST", "/dolphinscheduler/tenant/update"),
            ("POST", "/dolphinscheduler/tenant/delete"),
        ]
        assert mutations[0][2]["tenantName"] == ["tenant-new"]
        assert mutations[1][2]["tenantName"] == ["tenant-new"]
    else:
        assert [(method, path) for method, path, _form in mutations] == [
            ("POST", "/dolphinscheduler/tenants"),
            ("PUT", "/dolphinscheduler/tenants/8"),
            ("DELETE", "/dolphinscheduler/tenants/8"),
        ]


def test_create_readback_failure_requires_reconciliation_before_retry() -> None:
    profile = make_profile(ds_version="3.4.1")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _success(
                {
                    "id": 8,
                    "tenantCode": "tenant-new",
                    "description": None,
                    "queueId": 11,
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

    adapter = TenantAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).tenants.create(
            tenant_code="tenant-new",
            queue_id=11,
        )

    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def test_definitive_create_result_error_keeps_its_upstream_code() -> None:
    profile = make_profile(ds_version="3.4.1")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": 10009,
                "msg": "tenant code already exists",
                "data": None,
            },
        )

    adapter = TenantAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiResultError) as exc_info:
        adapter.bind(profile, http_client=http_client).tenants.create(
            tenant_code="tenant-existing",
            queue_id=11,
        )

    assert exc_info.value.result_code == 10009


@pytest.mark.parametrize("legacy_name", ["Tenant Display Name", None, ""])
def test_139_update_preserves_the_separate_legacy_name(
    legacy_name: str | None,
) -> None:
    profile = make_profile(ds_version="1.3.9")
    requests_seen: list[httpx.Request] = []
    tenant = {
        "id": 8,
        "tenantCode": "tenant-ops",
        "tenantName": legacy_name,
        "description": None,
        "queueId": 11,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        if request.method == "GET":
            return _success(
                {"totalList": [tenant], "total": 1, "totalPage": 1, "currentPage": 1}
            )
        assert parse_qs(request.content.decode()) == {
            "id": ["8"],
            "tenantCode": ["tenant-ops"],
            "tenantName": ["Tenant Display Name"],
            "queueId": ["11"],
        }
        return _success(None)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = TenantAdapter.for_version("1.3.9").bind(profile, http_client=client)
        if legacy_name:
            updated = domain.tenants.update(
                tenant_id=8, current_tenant_code="tenant-ops", queue_id=11
            )
            assert updated.tenantCode == "tenant-ops"
            assert [request.method for request in requests_seen] == [
                "GET",
                "POST",
                "GET",
            ]
        else:
            with pytest.raises(ApiTransportError) as exc_info:
                domain.tenants.update(
                    tenant_id=8, current_tenant_code="tenant-ops", queue_id=11
                )
            assert exc_info.value.details["field"] == "tenantName"
            assert [request.method for request in requests_seen] == ["GET"]


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_tenant_read_retries_and_accepts_an_optional_envelope(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
    )
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={"totalList": [], "total": 0, "totalPage": 0, "currentPage": 1},
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = TenantAdapter.for_version(ds_version).bind(profile, http_client=client)
        page = domain.tenants.list(page_no=1, page_size=20)

    assert page.total == 0
    assert attempts == 2


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_tenant_mutation_is_never_retried(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = TenantAdapter.for_version(ds_version).bind(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            domain.tenants.delete(tenant_id=8)

    assert attempts == 1
    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.details["mutation_may_have_applied"] is True


def test_tenant_rejects_a_different_selected_profile_before_io() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success(None)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireContractError, match="does not match"),
    ):
        TenantAdapter.for_version("3.4.2").bind(profile, http_client=client)

    assert requests_seen == []


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


@pytest.mark.parametrize(
    ("ds_version", "tenant_id", "tenant_code"),
    [("3.1.9", -1, "default"), ("3.4.3", -1, "other"), ("3.4.3", 0, "default")],
)
@pytest.mark.parametrize("operation", ["update", "delete"])
def test_tenant_mutations_reject_unreviewed_negative_or_zero_identity(
    ds_version: str, tenant_id: int, tenant_code: str, operation: str
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return _success(
            {
                "totalList": [
                    {"id": tenant_id, "tenantCode": tenant_code, "queueId": 1}
                ],
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            }
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = TenantAdapter.for_version(ds_version).bind(profile, http_client=client)
        if operation == "update":
            with pytest.raises(ApiTransportError):
                domain.tenants.update(
                    tenant_id=tenant_id,
                    current_tenant_code=tenant_code,
                    queue_id=1,
                    description="edited",
                )
        else:
            with pytest.raises(ApiTransportError):
                domain.tenants.delete(tenant_id=tenant_id)
    assert all(request.method == "GET" for request in requests)
