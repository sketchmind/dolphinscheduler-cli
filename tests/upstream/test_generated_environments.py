from __future__ import annotations

import importlib
import json
from contextlib import nullcontext
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.environments import (
    _ENVIRONMENT_PROGRAMS,
    ENVIRONMENT_DOMAIN,
    EnvironmentAdapter,
    EnvironmentDomain,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_SUPPORTED_VERSIONS = tuple(
    version for version in TARGET_DS_VERSIONS if version != "1.3.9"
)
_VOID_UPDATE_VERSIONS = frozenset(
    {"2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9", "3.2.0"}
)


def test_environment_strict_page_schema_cannot_be_swapped_for_coercive_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.environment")
    schemas = dict(module.RESPONSE_SCHEMAS)
    schemas["page_list_strict"] = schemas["page_list"]
    monkeypatch.setattr(module, "RESPONSE_SCHEMAS", schemas)

    with pytest.raises(
        WireContractError,
        match="page_list_strict response schema is invalid",
    ):
        _ENVIRONMENT_PROGRAMS.fresh_profile("3.1.9")


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_environment_domain_support_is_explicit_and_absent_is_zero_request(
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
        if ds_version == "1.3.9":
            with pytest.raises(UnsupportedFeatureError) as exc_info:
                ENVIRONMENT_DOMAIN.bind(profile, http_client=http_client)
            assert exc_info.value.details["reason"] == "upstream_capability_absent"
            assert exc_info.value.details["introduced_in"] == "2.0.0"
        else:
            domain = ENVIRONMENT_DOMAIN.bind(profile, http_client=http_client)
            assert isinstance(domain, EnvironmentDomain)

    assert requests_seen == 0


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_environment_domain_executes_exact_crud_and_canonical_readback(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current = _environment(code=7, name="prod", config="export JAVA_HOME=/java")
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
            current = _environment_from_form(form, code=41)
            return _success(41)
        if request.method == "POST" and request.url.path.endswith("/update"):
            current = _environment_from_form(form, code=41)
            result = (
                None
                if ds_version in _VOID_UPDATE_VERSIONS
                else _environment_entity(current)
            )
            return _success(result)
        if request.method == "POST" and request.url.path.endswith("/delete"):
            current = _environment(code=41, name="deleted", config="deleted")
            return _success(None)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = EnvironmentAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        environments = adapter.bind(profile, http_client=http_client).environments
        page = environments.list(page_no=2, page_size=25, search="prod")
        fetched = environments.get(code=7)
        created = environments.create(
            name="new-env",
            config="export JAVA_HOME=/java21",
            description="created",
            worker_groups=["default", "gpu"],
        )
        updated = environments.update(
            code=41,
            name="renamed-env",
            config="export JAVA_HOME=/java22",
            description="updated",
            worker_groups=["gpu"],
        )
        deleted = environments.delete(code=41)

    assert page.pageNo == 2
    assert page.pageSize == 25
    assert fetched.code == 7
    assert created.name == "new-env"
    assert list(created.workerGroups or []) == ["default", "gpu"]
    assert updated.name == "renamed-env"
    assert list(updated.workerGroups or []) == ["gpu"]
    assert deleted is True
    assert [(method, path) for method, path, _query, _form in requests_seen] == [
        ("GET", "/dolphinscheduler/environment/list-paging"),
        ("GET", "/dolphinscheduler/environment/query-by-code"),
        ("POST", "/dolphinscheduler/environment/create"),
        ("GET", "/dolphinscheduler/environment/query-by-code"),
        ("POST", "/dolphinscheduler/environment/update"),
        ("GET", "/dolphinscheduler/environment/query-by-code"),
        ("POST", "/dolphinscheduler/environment/delete"),
    ]
    assert requests_seen[0][2] == {
        "pageNo": ["2"],
        "pageSize": ["25"],
        "searchVal": ["prod"],
    }
    assert requests_seen[1][2] == {"environmentCode": ["7"]}
    assert requests_seen[2][3] == {
        "config": ["export JAVA_HOME=/java21"],
        "description": ["created"],
        "name": ["new-env"],
        "workerGroups": ['["default","gpu"]'],
    }
    assert requests_seen[3][2] == {"environmentCode": ["41"]}
    assert requests_seen[4][3] == {
        "code": ["41"],
        "config": ["export JAVA_HOME=/java22"],
        "description": ["updated"],
        "name": ["renamed-env"],
        "workerGroups": ['["gpu"]'],
    }
    assert requests_seen[5][2] == {"environmentCode": ["41"]}
    assert requests_seen[6][3] == {"environmentCode": ["41"]}


def test_environment_mutation_transport_failure_is_not_retried() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(503, json={"message": "unavailable"})

    adapter = EnvironmentAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).environments.create(
            name="new-env",
            config="export JAVA_HOME=/java21",
        )

    assert requests_seen == 1
    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def test_environment_mutation_requires_a_result_envelope_and_is_not_retried() -> None:
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
        EnvironmentAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).environments.create(name="new-env", config="export JAVA_HOME=/java21")

    assert requests_seen == 1
    assert exc_info.value.details["mutation_may_have_applied"] is True


def test_environment_read_uses_optional_envelope_and_preserves_retries() -> None:
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
            json=_page(_environment(code=7, name="prod", config="java")),
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = (
            EnvironmentAdapter.for_version("3.4.1")
            .bind(profile, http_client=http_client)
            .environments.list(page_no=2, page_size=25)
        )

    assert requests_seen == 2
    assert page.total == 1


@pytest.mark.parametrize(
    ("ds_version", "strict"),
    [("3.1.9", True), ("3.2.0", False)],
)
def test_environment_page_preserves_exact_integer_validation_epoch(
    ds_version: str,
    *,
    strict: bool,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _page(_environment(code=7, name="prod", config="java"))
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
            EnvironmentAdapter.for_version(ds_version)
            .bind(profile, http_client=http_client)
            .environments.list(page_no=2, page_size=25)
        )
        assert page.total == 1


@pytest.mark.parametrize(
    ("ds_version", "expected_id"),
    [("2.0.0", 0), ("3.1.0", None)],
)
def test_environment_get_preserves_identifier_default_epoch(
    ds_version: str,
    expected_id: int | None,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _environment(code=7, name="prod", config="java")
    del payload["id"]
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )

    with http_client:
        item = (
            EnvironmentAdapter.for_version(ds_version)
            .bind(profile, http_client=http_client)
            .environments.get(code=7)
        )

    assert item.id == expected_id


def test_legacy_environment_void_mutations_ignore_arbitrary_success_data() -> None:
    profile = make_profile(ds_version="3.1.0")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return _success(
                _environment(
                    code=41,
                    name="renamed",
                    config="java",
                    description="updated",
                    worker_groups=["gpu"],
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
        environments = (
            EnvironmentAdapter.for_version("3.1.0")
            .bind(profile, http_client=http_client)
            .environments
        )
        updated = environments.update(
            code=41,
            name="renamed",
            config="java",
            description="updated",
            worker_groups=["gpu"],
        )
        deleted = environments.delete(code=41)

    assert updated.code == 41
    assert deleted is True


def test_modern_environment_update_rejects_response_identity_mismatch() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(
            _environment_entity(_environment(code=99, name="other", config="wrong"))
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        EnvironmentAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).environments.update(
            code=41,
            name="renamed",
            config="java",
            worker_groups=["gpu"],
        )

    assert requests_seen == 1
    assert exc_info.value.details["phase"] == "mutation_response"


def test_environment_optional_request_fields_are_omitted() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen: list[tuple[str, dict[str, list[str]], dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        form = parse_qs(request.content.decode())
        requests_seen.append((request.url.path, query, form))
        if request.url.path.endswith("/list-paging"):
            return _success(_page(_environment(code=7, name="prod", config="java")))
        if request.url.path.endswith("/create"):
            return _success(41)
        return _success(_environment(code=41, name="new", config="java"))

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        environments = (
            EnvironmentAdapter.for_version("3.4.1")
            .bind(profile, http_client=http_client)
            .environments
        )
        environments.list(page_no=1, page_size=10)
        environments.create(name="new", config="java")

    assert requests_seen[0][1] == {"pageNo": ["1"], "pageSize": ["10"]}
    assert requests_seen[1][2] == {"config": ["java"], "name": ["new"]}
    assert requests_seen[2][1] == {"environmentCode": ["41"]}


def test_environment_readback_mismatch_requires_reconciliation() -> None:
    profile = make_profile(ds_version="3.4.1")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _success(41)
        return _success(_environment(code=41, name="other", config="wrong"))

    adapter = EnvironmentAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).environments.create(
            name="new-env",
            config="export JAVA_HOME=/java21",
        )

    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True


def _environment_from_form(
    form: dict[str, list[str]],
    *,
    code: int,
) -> dict[str, object]:
    worker_groups_raw = form.get("workerGroups", ["[]"])[0]
    worker_groups = json.loads(worker_groups_raw)
    assert isinstance(worker_groups, list)
    return _environment(
        code=code,
        name=form["name"][0],
        config=form["config"][0],
        description=form.get("description", [None])[0],
        worker_groups=[str(item) for item in worker_groups],
    )


def _environment(
    *,
    code: int,
    name: str,
    config: str,
    description: str | None = None,
    worker_groups: list[str] | None = None,
) -> dict[str, object]:
    return {
        "id": code,
        "code": code,
        "name": name,
        "config": config,
        "description": description,
        "workerGroups": worker_groups,
        "operator": 1,
        "createTime": "2026-08-05 10:00:00",
        "updateTime": "2026-08-05 10:00:00",
    }


def _environment_entity(environment: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in environment.items() if key != "workerGroups"}


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
