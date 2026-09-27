from __future__ import annotations

import importlib
from contextlib import nullcontext
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.clusters import (
    _CLUSTER_PROGRAMS,
    CLUSTER_DOMAIN,
    ClusterAdapter,
    ClusterDomain,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

# ClusterController is absent before 3.1.0; ClusterDto renames the attached
# definitions in 3.3.1. Update/delete service results carry no data through 3.2.0.
_SUPPORTED_VERSIONS = (
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
)
_VOID_MUTATION_VERSIONS = frozenset(
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
    }
)
_PROCESS_DEFINITION_VERSIONS = _VOID_MUTATION_VERSIONS | {"3.2.1", "3.2.2"}


def test_cluster_strict_page_schema_cannot_be_swapped_for_coercive_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    schemas = dict(module.RESPONSE_SCHEMAS)
    schemas["page_process_strict"] = schemas["page_process"]
    monkeypatch.setattr(module, "RESPONSE_SCHEMAS", schemas)

    with pytest.raises(
        WireContractError,
        match="page_process_strict response schema is invalid",
    ):
        _CLUSTER_PROGRAMS.fresh_profile("3.1.9")


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_cluster_domain_support_is_explicit_and_absent_is_zero_request(
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
        if ds_version not in _SUPPORTED_VERSIONS:
            with pytest.raises(UnsupportedFeatureError) as exc_info:
                CLUSTER_DOMAIN.bind(profile, http_client=http_client)
            assert exc_info.value.details["reason"] == "upstream_capability_absent"
            assert exc_info.value.details["introduced_in"] == "3.1.0"
        else:
            domain = CLUSTER_DOMAIN.bind(profile, http_client=http_client)
            assert isinstance(domain, ClusterDomain)

    assert requests_seen == 0


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_cluster_domain_executes_exact_crud_and_canonical_projection(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    definitions_field = (
        "processDefinitions"
        if ds_version in _PROCESS_DEFINITION_VERSIONS
        else "workflowDefinitions"
    )
    current = _cluster(
        code=7,
        name="prod",
        config="kube-prod",
        definitions_field=definitions_field,
    )
    requests_seen: list[
        tuple[
            str,
            str,
            dict[str, list[str]],
            dict[str, list[str]],
        ]
    ] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        query = parse_qs(request.url.query.decode())
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, query, form))
        if request.method == "GET" and request.url.path.endswith("/list-paging"):
            return _success(_page(current))
        if request.method == "GET" and request.url.path.endswith("/query-by-code"):
            return _success(current)
        if request.method == "POST" and request.url.path.endswith("/create"):
            current = _cluster_from_form(
                form,
                code=41,
                definitions_field=definitions_field,
            )
            return _success(41)
        if request.method == "POST" and request.url.path.endswith("/update"):
            current = _cluster_from_form(
                form,
                code=41,
                definitions_field=definitions_field,
            )
            update_result = (
                None
                if ds_version in _VOID_MUTATION_VERSIONS
                else _cluster_entity(current, definitions_field=definitions_field)
            )
            return _success(update_result)
        if request.method == "POST" and request.url.path.endswith("/delete"):
            delete_result = None if ds_version in _VOID_MUTATION_VERSIONS else True
            return _success(delete_result)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ClusterAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        clusters = adapter.bind(profile, http_client=http_client).clusters
        page = clusters.list(page_no=2, page_size=25, search="prod")
        fetched = clusters.get(code=7)
        created = clusters.create(
            name="new-cluster",
            config="kube-new",
            description="created",
        )
        updated = clusters.update(
            code=41,
            name="renamed-cluster",
            config="kube-updated",
            description="updated",
        )
        deleted = clusters.delete(code=41)

    assert page.pageNo == 2
    assert page.pageSize == 25
    assert next(iter(page.totalList or [])).workflowDefinitions == ("daily-etl",)
    assert fetched.code == 7
    assert list(fetched.workflowDefinitions or []) == ["daily-etl"]
    assert created.name == "new-cluster"
    assert updated.name == "renamed-cluster"
    assert deleted is True
    detail_path = (
        "/dolphinscheduler/cluster/list-paging"
        if ds_version == "3.4.3"
        else "/dolphinscheduler/cluster/query-by-code"
    )
    assert [(method, path) for method, path, _query, _form in requests_seen] == [
        ("GET", "/dolphinscheduler/cluster/list-paging"),
        ("GET", detail_path),
        ("POST", "/dolphinscheduler/cluster/create"),
        ("GET", detail_path),
        ("POST", "/dolphinscheduler/cluster/update"),
        ("GET", detail_path),
        ("POST", "/dolphinscheduler/cluster/delete"),
    ]
    assert requests_seen[0][2] == {
        "pageNo": ["2"],
        "pageSize": ["25"],
        "searchVal": ["prod"],
    }
    assert requests_seen[1][2] == (
        {"pageNo": ["1"], "pageSize": ["100"]}
        if ds_version == "3.4.3"
        else {"clusterCode": ["7"]}
    )
    assert requests_seen[2][3] == {
        "config": ["kube-new"],
        "description": ["created"],
        "name": ["new-cluster"],
    }
    assert requests_seen[3][2] == (
        {"pageNo": ["1"], "pageSize": ["100"]}
        if ds_version == "3.4.3"
        else {"clusterCode": ["41"]}
    )
    assert requests_seen[4][3] == {
        "code": ["41"],
        "config": ["kube-updated"],
        "description": ["updated"],
        "name": ["renamed-cluster"],
    }
    assert requests_seen[5][2] == (
        {"pageNo": ["1"], "pageSize": ["100"]}
        if ds_version == "3.4.3"
        else {"clusterCode": ["41"]}
    )
    assert requests_seen[6][3] == {"clusterCode": ["41"]}


def test_cluster_mutation_transport_failure_is_not_retried() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(503, json={"message": "unavailable"})

    adapter = ClusterAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).clusters.create(
            name="new-cluster",
            config="kube-new",
        )

    assert requests_seen == 1
    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def test_cluster_mutation_requires_a_result_envelope_and_is_not_retried() -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(200, json=41)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        ClusterAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).clusters.create(name="new-cluster", config="kube-new")

    assert requests_seen == 1
    assert exc_info.value.details["mutation_may_have_applied"] is True


def test_cluster_read_uses_optional_envelope_and_preserves_retries() -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
    )
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        if requests_seen == 1:
            return httpx.Response(503, json={"message": "unavailable"})
        return httpx.Response(
            200,
            json=_page(
                _cluster(
                    code=7,
                    name="prod",
                    config="kube-prod",
                    definitions_field="workflowDefinitions",
                )
            ),
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = (
            ClusterAdapter.for_version("3.4.1")
            .bind(
                profile,
                http_client=http_client,
            )
            .clusters.list(page_no=2, page_size=25)
        )

    assert requests_seen == 2
    assert page.total == 1


@pytest.mark.parametrize(
    ("ds_version", "strict"),
    [("3.1.9", True), ("3.2.0", False)],
)
def test_cluster_page_preserves_exact_integer_validation_epoch(
    ds_version: str,
    *,
    strict: bool,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _page(
        _cluster(
            code=7,
            name="prod",
            config="kube-prod",
            definitions_field="processDefinitions",
        )
    )
    for field in ("total", "totalPage", "pageSize", "currentPage", "pageNo"):
        payload[field] = str(payload[field])
    payload["futureField"] = {"ignored": True}
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    context = pytest.raises(ApiTransportError) if strict else nullcontext()
    with http_client, context:
        page = (
            ClusterAdapter.for_version(ds_version)
            .bind(
                profile,
                http_client=http_client,
            )
            .clusters.list(page_no=2, page_size=25)
        )
        assert page.total == 1


def test_legacy_cluster_void_mutations_ignore_arbitrary_success_data() -> None:
    profile = make_profile(ds_version="3.1.0")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return _success(
                _cluster(
                    code=41,
                    name="renamed",
                    config="kube-new",
                    description="updated",
                    definitions_field="processDefinitions",
                )
            )
        if request.url.path.endswith("/update"):
            return _success({"future": "ignored"})
        return _success(["also", "ignored"])

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        clusters = (
            ClusterAdapter.for_version("3.1.0")
            .bind(
                profile,
                http_client=http_client,
            )
            .clusters
        )
        updated = clusters.update(
            code=41,
            name="renamed",
            config="kube-new",
            description="updated",
        )
        deleted = clusters.delete(code=41)

    assert updated.code == 41
    assert deleted is True


def test_modern_cluster_delete_preserves_false_result() -> None:
    profile = make_profile(ds_version="3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(False)),
    )
    with http_client:
        deleted = (
            ClusterAdapter.for_version("3.4.1")
            .bind(
                profile,
                http_client=http_client,
            )
            .clusters.delete(code=41)
        )

    assert deleted is False


def test_modern_cluster_update_rejects_mutation_response_identity_mismatch() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(
            _cluster_entity(
                _cluster(
                    code=99,
                    name="other",
                    config="wrong",
                    definitions_field="workflowDefinitions",
                ),
                definitions_field="workflowDefinitions",
            )
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        ClusterAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).clusters.update(code=41, name="renamed", config="kube-new")

    assert requests_seen == 1
    assert exc_info.value.details["phase"] == "mutation_response"


def test_cluster_readback_mismatch_requires_reconciliation() -> None:
    profile = make_profile(ds_version="3.4.1")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _success(41)
        return _success(
            _cluster(
                code=41,
                name="other",
                config="wrong",
                definitions_field="workflowDefinitions",
            )
        )

    adapter = ClusterAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).clusters.create(
            name="new-cluster",
            config="kube-new",
        )

    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True


def _cluster_from_form(
    form: dict[str, list[str]],
    *,
    code: int,
    definitions_field: str,
) -> dict[str, object]:
    return _cluster(
        code=code,
        name=form["name"][0],
        config=form["config"][0],
        description=form.get("description", [None])[0],
        definitions_field=definitions_field,
    )


def _cluster(
    *,
    code: int,
    name: str,
    config: str,
    definitions_field: str,
    description: str | None = None,
) -> dict[str, object]:
    return {
        "id": code,
        "code": code,
        "name": name,
        "config": config,
        "description": description,
        definitions_field: ["daily-etl"],
        "operator": 1,
        "createTime": "2026-08-05 10:00:00",
        "updateTime": "2026-08-05 10:00:00",
    }


def _cluster_entity(
    cluster: dict[str, object],
    *,
    definitions_field: str,
) -> dict[str, object]:
    return {key: value for key, value in cluster.items() if key != definitions_field}


def _page(item: dict[str, object]) -> dict[str, object]:
    return {
        "totalList": [item],
        "total": 1,
        "totalPage": 1,
        "pageSize": 25,
        "currentPage": 2,
        "pageNo": 2,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
