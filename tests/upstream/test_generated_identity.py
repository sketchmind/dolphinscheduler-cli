from __future__ import annotations

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.identity import (
    CurrentUserSnapshot,
    IdentityAdapter,
)
from dsctl.upstream.users import UserAdapter
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_identity_adapter_projects_exact_current_user(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "id": 1,
                    "userName": "admin",
                    "userType": "GENERAL_USER",
                    "tenantCode": "tenant-a",
                    "queueName": "queue-a",
                    "queue": "root.default",
                    "timeZone": "Asia/Shanghai",
                },
            },
        )

    adapter = IdentityAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        current = adapter.bind_identity(
            profile,
            http_client=http_client,
        ).current()

    assert isinstance(current, CurrentUserSnapshot)
    assert current.userName == "admin"
    assert current.userType is not None
    assert current.userType.value == "GENERAL_USER"
    assert current.tenantCode == "tenant-a"
    assert current.queueName == "queue-a"
    assert current.queue == "root.default"
    assert current.timeZone == (
        None
        if ds_version in {"1.3.9", *(f"2.0.{patch}" for patch in range(10))}
        else "Asia/Shanghai"
    )
    assert requests_seen == [("GET", "/dolphinscheduler/users/get-user-info")]


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.2"])
def test_identity_keeps_raw_queue_while_user_domain_projects_effective_queue(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "id": 7,
                    "userName": "alice",
                    "userType": "GENERAL_USER",
                    "tenantId": 11,
                    "tenantCode": "tenant-a",
                    "state": 1,
                    "queue": "",
                    "queueName": "root.tenant",
                },
            },
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        identity = (
            IdentityAdapter.for_version(ds_version)
            .bind_identity(profile, http_client=client)
            .current()
        )
        user = (
            UserAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .users.current()
        )

    assert identity.queue == ""
    assert identity.queueName == "root.tenant"
    assert user.queue == "root.tenant"
    assert requests == [("GET", "/dolphinscheduler/users/get-user-info")] * 2


@pytest.mark.parametrize(
    ("selected_version", "client_version"),
    [("3.4.1", "3.4.1"), ("3.4.2", "3.4.1")],
)
def test_identity_adapter_rejects_identity_mismatches_before_transport(
    selected_version: str,
    client_version: str,
) -> None:
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": {}})

    adapter = IdentityAdapter.for_version("3.4.2")
    adapter_profile = make_profile(ds_version=selected_version)
    client_profile = make_profile(ds_version=client_version)
    http_client = DolphinSchedulerClient(
        client_profile,
        transport=httpx.MockTransport(handler),
    )
    with (
        http_client,
        pytest.raises(
            WireContractError,
            match="client profile",
        ),
    ):
        adapter.bind_identity(adapter_profile, http_client=http_client)

    assert requests_seen == []


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.2"])
def test_identity_does_not_require_user_record_identity_fields(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/users/get-user-info"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "userName": "admin",
                    "queue": "",
                    "queueName": "root.tenant",
                },
            },
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        current = (
            IdentityAdapter.for_version(ds_version)
            .bind_identity(profile, http_client=client)
            .current()
        )

    assert current.userName == "admin"
    assert current.queue == ""
    assert current.queueName == "root.tenant"
    assert current.userType is None
    assert current.tenantCode is None
    assert current.timeZone is None
