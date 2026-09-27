from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.namespaces import (
    NAMESPACE_DOMAIN,
    NamespaceAdapter,
    NamespaceDomain,
)
from dsctl.upstream.resolver import namespace as resolve_namespace
from dsctl.upstream.serialization import serialize_namespace
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

# K8sNamespaceController uses k8s in 3.0.x, clusterCode from 3.1.0, and
# drops quota in 3.2.1. Creation returns an entity starting with 3.2.2.
_ABSENT_VERSIONS = frozenset(
    {
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
    }
)
_SUPPORTED_VERSIONS = tuple(
    version for version in TARGET_DS_VERSIONS if version not in _ABSENT_VERSIONS
)
_K8S_SELECTOR_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }
)
_DESTRUCTIVE_DELETE_VERSIONS = _K8S_SELECTOR_VERSIONS | {
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
_QUOTA_VERSIONS = _DESTRUCTIVE_DELETE_VERSIONS | {"3.2.0"}
_VOID_CREATE_VERSIONS = _QUOTA_VERSIONS | {"3.2.1"}


def test_namespace_domain_rejects_an_unreviewed_version_decision() -> None:
    with pytest.raises(WireContractError, match="capability decision"):
        NAMESPACE_DOMAIN.adapter_for_version("9.9.9")


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_namespace_domain_support_is_explicit_and_absent_is_zero_request(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        if ds_version in _ABSENT_VERSIONS:
            with pytest.raises(UnsupportedFeatureError) as exc_info:
                NAMESPACE_DOMAIN.bind(profile, http_client=http_client)
            assert exc_info.value.details["reason"] == "upstream_capability_absent"
            assert exc_info.value.details["introduced_in"] == "3.0.0"
        else:
            domain = NAMESPACE_DOMAIN.bind(profile, http_client=http_client)
            assert isinstance(domain, NamespaceDomain)

    assert requests_seen == 0


@pytest.mark.parametrize(
    ("selected_version", "client_version"),
    [("3.4.2", "3.4.2"), ("3.4.1", "3.4.2")],
)
def test_namespace_binding_rejects_mismatched_profile_without_request(
    selected_version: str,
    client_version: str,
) -> None:
    adapter = NamespaceAdapter.for_version("3.4.1")

    def handler(request: httpx.Request) -> httpx.Response:
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    with (
        DolphinSchedulerClient(
            make_profile(ds_version=client_version),
            transport=httpx.MockTransport(handler),
        ) as http_client,
        pytest.raises(WireContractError, match="client profile"),
    ):
        adapter.bind(make_profile(ds_version=selected_version), http_client=http_client)


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_namespace_domain_executes_exact_reads_create_delete_and_readback(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current = _namespace(namespace_id=21, namespace="etl-prod")
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]]]
    ] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        query = parse_qs(request.url.query.decode(), keep_blank_values=True)
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        requests_seen.append((request.method, request.url.path, query, form))
        if request.method == "GET" and request.url.path.endswith("/k8s-namespace"):
            return _success(
                _page(
                    current,
                    page_no=int(query["pageNo"][0]),
                    page_size=int(query["pageSize"][0]),
                )
            )
        if request.method == "GET" and request.url.path.endswith("/available-list"):
            return _success([current])
        if request.method == "POST" and request.url.path.endswith("/k8s-namespace"):
            current = _namespace_from_form(form, namespace_id=31)
            return _success(None if ds_version in _VOID_CREATE_VERSIONS else current)
        if request.method == "POST" and request.url.path.endswith("/delete"):
            return _success(None)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = NamespaceAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    cluster_code: int | None = None
    k8s: str | None = None
    limits_cpu: float | None = None
    limits_memory: int | None = None
    if ds_version in _K8S_SELECTOR_VERSIONS:
        k8s = "k8s-prod"
    else:
        cluster_code = 9001
    if ds_version in _QUOTA_VERSIONS:
        limits_cpu = 2.5
        limits_memory = 8

    with http_client:
        namespaces = adapter.bind(profile, http_client=http_client).namespaces
        page = namespaces.list(page_no=2, page_size=25, search="etl")
        resolved = resolve_namespace("etl-prod", adapter=namespaces)
        available = namespaces.available()
        created = namespaces.create(
            namespace="etl-new",
            cluster_code=cluster_code,
            k8s=k8s,
            limits_cpu=limits_cpu,
            limits_memory=limits_memory,
        )
        deleted = namespaces.delete(namespace_id=31)

    assert page.pageNo == 2
    assert page.pageSize == 25
    assert next(iter(page.totalList or [])).namespace == "etl-prod"
    assert resolved.id == 21
    assert resolved.namespace_name == "etl-prod"
    assert available[0].namespace == "etl-prod"
    assert created.id == 31
    assert created.namespace == "etl-new"
    assert deleted is True
    assert namespaces.deletes_kubernetes_namespace is (
        ds_version in _DESTRUCTIVE_DELETE_VERSIONS
    )
    page_request = next(
        item
        for item in requests_seen
        if item[0] == "GET"
        and item[1] == "/dolphinscheduler/k8s-namespace"
        and item[2].get("pageNo") == ["2"]
    )
    assert page_request[2] == {
        "pageNo": ["2"],
        "pageSize": ["25"],
        "searchVal": ["etl"],
    }
    assert any(
        method == "GET" and path == "/dolphinscheduler/k8s-namespace/available-list"
        for method, path, _query, _form in requests_seen
    )
    assert any(
        method == "POST"
        and path == "/dolphinscheduler/k8s-namespace/delete"
        and form == {"id": ["31"]}
        for method, path, _query, form in requests_seen
    )
    create_request = next(
        item
        for item in requests_seen
        if item[0] == "POST" and item[1].endswith("/k8s-namespace")
    )
    expected_form = {"namespace": ["etl-new"]}
    if ds_version in _K8S_SELECTOR_VERSIONS:
        expected_form["k8s"] = ["k8s-prod"]
    else:
        expected_form["clusterCode"] = ["9001"]
    if ds_version in _QUOTA_VERSIONS:
        expected_form.update(
            {
                "limitsCpu": ["2.5"],
                "limitsMemory": ["8"],
            }
        )
    assert create_request[3] == expected_form


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_namespace_lists_preserve_exact_fields_and_supported_nulls(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    # Extra fields deliberately cross epochs: the exact decoder owns their
    # availability, while values never decide which output columns exist.
    rows = [
        {"id": 21, "namespace": "unset", "limitsCpu": None, "k8s": None},
        {
            "id": 22,
            "namespace": "configured",
            "code": 102,
            "clusterCode": 9001,
            "clusterName": "production",
            "k8s": "production",
            "limitsCpu": 2.5,
            "limitsMemory": 8,
            "onlineJobNum": 3,
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/available-list"):
            return _success(rows)
        assert request.url.path.endswith("/k8s-namespace")
        return _success(
            {"totalList": rows, "total": 2, "pageSize": 100, "currentPage": 1}
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = (
            NamespaceAdapter.for_version(ds_version)
            .bind(profile, http_client=http_client)
            .namespaces
        )
        page_items = list(namespaces.list(page_no=1, page_size=100).totalList or ())
        available_items = list(namespaces.available())

    expected_fields = {
        "id",
        "namespace",
        "userId",
        "userName",
        "createTime",
        "updateTime",
    }
    if ds_version in _K8S_SELECTOR_VERSIONS:
        expected_fields |= {"k8s", "onlineJobNum"}
    else:
        expected_fields |= {"code", "clusterCode", "clusterName"}
    if ds_version in _QUOTA_VERSIONS:
        expected_fields |= {
            "limitsCpu",
            "limitsMemory",
            "podRequestCpu",
            "podRequestMemory",
            "podReplicas",
        }

    page_rows = [serialize_namespace(item) for item in page_items]
    assert page_rows == [serialize_namespace(item) for item in available_items]
    assert [row["id"] for row in page_rows] == [21, 22]
    assert all(set(row) == expected_fields for row in page_rows)
    assert all(item.supported_fields == expected_fields for item in page_items)
    assert page_rows[0]["userName"] is None
    if ds_version in _QUOTA_VERSIONS:
        assert page_rows[0]["limitsCpu"] is None
        assert page_rows[1]["limitsCpu"] == 2.5
        assert page_rows[0]["podRequestCpu"] == 0.0
    if ds_version in _K8S_SELECTOR_VERSIONS:
        assert page_rows[0]["k8s"] is None
        assert page_rows[1]["k8s"] == "production"
    else:
        assert page_rows[0]["clusterCode"] is None
        assert page_rows[1]["clusterCode"] == 9001


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_namespace_create_rejects_wrong_exact_version_options_before_request(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    adapter = NamespaceAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        namespaces = adapter.bind(profile, http_client=http_client).namespaces
        wrong_cluster_code = 9001 if ds_version in _K8S_SELECTOR_VERSIONS else None
        wrong_k8s = None if ds_version in _K8S_SELECTOR_VERSIONS else "k8s-prod"
        with pytest.raises(UserInputError):
            namespaces.create(
                namespace="etl-new",
                cluster_code=wrong_cluster_code,
                k8s=wrong_k8s,
            )

    assert requests_seen == 0


@pytest.mark.parametrize(
    "ds_version",
    tuple(version for version in _SUPPORTED_VERSIONS if version not in _QUOTA_VERSIONS),
)
def test_namespace_create_rejects_removed_quota_options_before_request(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    adapter = NamespaceAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        namespaces = adapter.bind(profile, http_client=http_client).namespaces
        with pytest.raises(UserInputError, match="quota inputs are not accepted"):
            namespaces.create(
                namespace="etl-new",
                cluster_code=9001,
                limits_cpu=1.0,
            )

    assert requests_seen == 0


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.4.1"])
@pytest.mark.parametrize("operation", ["create", "delete"])
def test_namespace_writes_are_once_only_without_failure_readback(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        message = "response lost"
        raise httpx.ReadError(message, request=request)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = NAMESPACE_DOMAIN.bind(profile, http_client=http_client).namespaces

        def dispatch() -> None:
            if operation == "create":
                namespaces.create(
                    namespace="etl-new",
                    k8s="k8s-prod" if ds_version == "3.0.0" else None,
                    cluster_code=None if ds_version == "3.0.0" else 9001,
                )
            else:
                namespaces.delete(namespace_id=31)

        with pytest.raises(ApiTransportError) as caught:
            dispatch()

    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.4.1"])
def test_namespace_create_readback_retries_only_reads_not_the_mutation(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "POST":
            return _success(
                None
                if ds_version == "3.0.0"
                else _namespace(namespace_id=31, namespace="etl-new")
            )
        message = "readback unavailable"
        raise httpx.ReadError(message, request=request)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = NAMESPACE_DOMAIN.bind(profile, http_client=http_client).namespaces
        with pytest.raises(ApiTransportError) as caught:
            namespaces.create(
                namespace="etl-new",
                k8s="k8s-prod" if ds_version == "3.0.0" else None,
                cluster_code=None if ds_version == "3.0.0" else 9001,
            )

    assert methods == ["POST", "GET", "GET", "GET"]
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["mutation_applied"] is True


def test_namespace_delete_preserves_a_definitive_upstream_error() -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"code": 10010, "msg": "delete rejected", "data": False}
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = NAMESPACE_DOMAIN.bind(profile, http_client=http_client).namespaces
        with pytest.raises(ApiResultError) as caught:
            namespaces.delete(namespace_id=31)

    assert caught.value.result_code == 10010
    assert caught.value.result_message == "delete rejected"
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/dolphinscheduler/k8s-namespace/delete"


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.4.1"])
@pytest.mark.parametrize("payload", [None, {}, [None]])
def test_namespace_available_rejects_non_list_or_malformed_members(
    ds_version: str,
    payload: object,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(payload)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = NAMESPACE_DOMAIN.bind(profile, http_client=http_client).namespaces
        with pytest.raises(ApiTransportError):
            namespaces.available()

    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.path == "/dolphinscheduler/k8s-namespace/available-list"


@pytest.mark.parametrize("field", ["limitsCpu", "limitsMemory"])
def test_namespace_create_checks_requested_quota_in_readback(field: str) -> None:
    profile = make_profile(ds_version="3.2.0")
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "POST":
            return _success(None)
        row = _namespace(
            namespace_id=31, namespace="etl-new", limits_cpu=2.5, limits_memory=8
        )
        row[field] = 0
        return _success(_page(row, page_no=1, page_size=100))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = NAMESPACE_DOMAIN.bind(profile, http_client=http_client).namespaces
        with pytest.raises(ApiTransportError) as caught:
            namespaces.create(
                namespace="etl-new", cluster_code=9001, limits_cpu=2.5, limits_memory=8
            )

    assert methods == ["POST", "GET"]
    assert caught.value.details["field"] == field
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["mutation_applied"] is True


def test_legacy_k8s_selector_strips_form_but_matches_original_readback() -> None:
    profile = make_profile(ds_version="3.0.0")
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "POST":
            assert parse_qs(request.content.decode()) == {
                "namespace": ["etl-new"],
                "k8s": ["k8s-prod"],
            }
            return _success(None)
        row = _namespace(
            namespace_id=31, namespace="etl-new", cluster_code=None, k8s="k8s-prod"
        )
        return _success(_page(row, page_no=1, page_size=100))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        namespaces = NAMESPACE_DOMAIN.bind(profile, http_client=http_client).namespaces
        with pytest.raises(ApiTransportError) as caught:
            namespaces.create(namespace="etl-new", k8s=" k8s-prod ")

    assert methods == ["POST", "GET"]
    assert caught.value.details["field"] == "mutationReadback"
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["mutation_applied"] is True


def _namespace_from_form(
    form: dict[str, list[str]],
    *,
    namespace_id: int,
) -> dict[str, object]:
    cluster_code = int(form["clusterCode"][0]) if "clusterCode" in form else None
    return _namespace(
        namespace_id=namespace_id,
        namespace=form["namespace"][0],
        cluster_code=cluster_code,
        k8s=form.get("k8s", [None])[0],
        limits_cpu=(float(form["limitsCpu"][0]) if "limitsCpu" in form else None),
        limits_memory=(
            int(form["limitsMemory"][0]) if "limitsMemory" in form else None
        ),
    )


def _namespace(
    *,
    namespace_id: int,
    namespace: str,
    cluster_code: int | None = 9001,
    k8s: str | None = None,
    limits_cpu: float | None = None,
    limits_memory: int | None = None,
) -> dict[str, object]:
    return {
        "id": namespace_id,
        "code": 7000 + namespace_id,
        "namespace": namespace,
        "clusterCode": cluster_code,
        "clusterName": "prod-cluster" if cluster_code is not None else None,
        "k8s": k8s,
        "limitsCpu": limits_cpu,
        "limitsMemory": limits_memory,
        "podRequestCpu": 0.0,
        "podRequestMemory": 0,
        "podReplicas": 0,
        "onlineJobNum": 0,
        "userId": 1,
        "userName": "admin",
        "createTime": "2026-08-05 10:00:00",
        "updateTime": "2026-08-05 10:00:00",
    }


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
