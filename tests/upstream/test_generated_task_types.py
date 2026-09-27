from __future__ import annotations

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.task_type_inventory import (
    TASK_TYPE_DOMAIN,
    TaskTypeAdapter,
    TaskTypeDomain,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

# FavTaskController appears in 3.1.0. FavTaskDto retains taskName throughout
# 3.1.x and replaces it with taskCategory in 3.2.0.
_ABSENT_VERSIONS = (
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
)
_SUPPORTED_VERSIONS = tuple(
    version for version in TARGET_DS_VERSIONS if version not in _ABSENT_VERSIONS
)
_TASK_CATEGORY_ABSENT_VERSIONS = (
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
)
_TASK_CATEGORY_VERSIONS = (
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


def test_task_type_domain_rejects_an_unreviewed_version_decision() -> None:
    with pytest.raises(WireContractError, match="capability decision"):
        TASK_TYPE_DOMAIN.adapter_for_version("9.9.9")


@pytest.mark.parametrize("ds_version", _ABSENT_VERSIONS)
def test_task_type_absence_is_explicit_and_zero_request(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success([])

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(UnsupportedFeatureError) as exc_info:
        TASK_TYPE_DOMAIN.bind(profile, http_client=http_client)

    assert requests_seen == 0
    assert exc_info.value.details["reason"] == "upstream_capability_absent"
    assert exc_info.value.details["introduced_in"] == "3.1.0"


@pytest.mark.parametrize("ds_version", _TASK_CATEGORY_VERSIONS)
def test_task_type_domain_executes_exact_catalog_read(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        return _success(
            [
                {
                    "taskType": "SHELL",
                    "collection": True,
                    "taskCategory": "Universal",
                }
            ]
        )

    adapter = TaskTypeAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, TaskTypeDomain)
        task_types = domain.task_types.list()

    assert requests_seen == [("GET", "/dolphinscheduler/favourite/taskTypes")]
    assert len(task_types) == 1
    assert task_types[0].taskType == "SHELL"
    assert task_types[0].isCollection is True
    assert task_types[0].taskCategory == "Universal"


@pytest.mark.parametrize("ds_version", _TASK_CATEGORY_ABSENT_VERSIONS)
def test_task_type_domain_projects_legacy_name_and_category(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)

    def handler(_request: httpx.Request) -> httpx.Response:
        return _success(
            [
                {
                    "taskName": "SHELL",
                    "taskType": "Universal",
                    "collection": True,
                }
            ]
        )

    adapter = TaskTypeAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        task_types = adapter.bind(
            profile,
            http_client=http_client,
        ).task_types.list()

    assert len(task_types) == 1
    assert task_types[0].taskType == "SHELL"
    assert task_types[0].isCollection is True
    assert task_types[0].taskCategory == "Universal"


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_task_type_response_alias_defaults_and_future_fields(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success(
            [
                {"futureField": "preserved upstream only"},
                {"isCollection": True},
                {"collection": False, "isCollection": True},
            ]
        )

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    ) as http_client:
        task_types = TASK_TYPE_DOMAIN.bind(
            profile, http_client=http_client
        ).task_types.list()

    assert [
        (item.taskType, item.isCollection, item.taskCategory) for item in task_types
    ] == [
        (None, False, None),
        (None, True, None),
        (None, False, None),
    ]
    assert [(request.method, request.url.path) for request in requests_seen] == [
        ("GET", "/dolphinscheduler/favourite/taskTypes")
    ]
    assert not requests_seen[0].url.query


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
@pytest.mark.parametrize("payload", [None, [None], [{"collection": None}]])
def test_task_type_malformed_response_fails_without_retry(
    ds_version: str, payload: object
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success(payload)

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    ) as http_client:
        task_types = TASK_TYPE_DOMAIN.bind(profile, http_client=http_client).task_types
        with pytest.raises(ApiTransportError):
            task_types.list()

    assert len(requests_seen) == 1


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
