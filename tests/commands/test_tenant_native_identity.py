from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.client import DolphinSchedulerClient
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.services import runtime as runtime_service
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.tenants import TENANT_DOMAIN, TenantDomain
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.upstream.bound_domain import BoundDomain

_SEEDED_VERSIONS = (
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


def _install_transport(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    *,
    native_id: int = -1,
    native_code: str = "default",
    native_queue_id: int | None = 1,
) -> list[httpx.Request]:
    profile = make_profile(ds_version=version)
    requests: list[httpx.Request] = []
    rows = [
        {"id": native_id, "tenantCode": native_code, "queueId": native_queue_id},
        {"id": 7, "tenantCode": "owned", "queueId": 1},
    ]

    if native_queue_id is None:
        rows[0].pop("queueId")

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "DELETE":
            assert request.url.path.endswith("/tenants/-1")
            return httpx.Response(200, json={"code": 0, "msg": "success", "data": True})
        if request.method == "PUT":
            assert request.url.path.endswith("/tenants/-1")
            form = parse_qs(request.content.decode())
            assert form["queueId"] == ["0"]
            rows[0]["description"] = form["description"][0]
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": None if version == "3.2.0" else True,
                },
            )
        assert request.method == "GET"
        search = parse_qs(request.url.query.decode()).get("searchVal", [None])[0]
        selected = [
            row for row in rows if search is None or row["tenantCode"] == search
        ]
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": selected,
                    "total": len(selected),
                    "totalPage": 1,
                    "currentPage": 1,
                },
            },
        )

    @contextmanager
    def open_runtime(
        domain: BoundDomain[TenantDomain], *, env_file: str | None = None
    ) -> Iterator[BoundDomainServiceRuntime[TenantDomain]]:
        assert domain is TENANT_DOMAIN
        with DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client:
            yield BoundDomainServiceRuntime(
                profile=profile,
                context=ResourceDefaults(),
                http_client=client,
                domain=TENANT_DOMAIN.bind(profile, http_client=client),
            )

    monkeypatch.setattr(
        runtime_service, "open_bound_domain_service_runtime", open_runtime
    )
    return requests


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_native_default_tenant_identity_is_exactly_scoped(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    requests = _install_transport(monkeypatch, version)
    result = CliRunner().invoke(app, ["tenant", "list"])
    if version in _SEEDED_VERSIONS:
        assert result.exit_code == 0, result.output
        rows = json.loads(result.stdout)["data"]["totalList"]
        assert [(row["id"], row["tenantCode"]) for row in rows] == [
            (-1, "default"),
            (7, "owned"),
        ]
    else:
        assert result.exit_code == 1
        error = json.loads(result.stderr)["error"]
        assert error["type"] == "api_transport_error"
        assert error["details"]["field"] == "id"
    assert all(request.method == "GET" for request in requests)


@pytest.mark.parametrize("version", _SEEDED_VERSIONS)
@pytest.mark.parametrize(("name", "expected_id"), [("owned", 7), ("default", -1)])
def test_tenant_get_survives_native_default_row_in_full_readback(
    monkeypatch: pytest.MonkeyPatch, version: str, name: str, expected_id: int
) -> None:
    _install_transport(monkeypatch, version)
    result = CliRunner().invoke(app, ["tenant", "get", name])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["id"] == expected_id


@pytest.mark.parametrize(
    ("native_id", "native_code"),
    [(0, "default"), (-2, "default"), (-1, "other"), (False, "default")],
)
def test_other_invalid_tenant_identities_still_fail_closed(
    monkeypatch: pytest.MonkeyPatch, native_id: int, native_code: str
) -> None:
    _install_transport(
        monkeypatch, "3.4.3", native_id=native_id, native_code=native_code
    )
    result = CliRunner().invoke(app, ["tenant", "list"])
    assert result.exit_code == 1
    assert json.loads(result.stderr)["error"]["type"] == "api_transport_error"


def test_explicit_default_delete_preserves_native_mutation_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _install_transport(monkeypatch, "3.4.3")
    result = CliRunner().invoke(app, ["tenant", "delete", "default", "--force"])
    assert result.exit_code == 0, result.output
    assert [request.method for request in requests].count("DELETE") == 1
    assert json.loads(result.stdout)["data"]["deleted"] is True


@pytest.mark.parametrize("version", _SEEDED_VERSIONS)
def test_upgraded_default_tenant_preserves_unassigned_queue_on_description_update(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    requests = _install_transport(monkeypatch, version, native_queue_id=0)
    ordinary = CliRunner().invoke(app, ["tenant", "get", "owned"])
    assert ordinary.exit_code == 0, ordinary.output
    result = CliRunner().invoke(
        app, ["tenant", "update", "default", "--description", "updated default"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert (data["id"], data["tenantCode"], data["queueId"]) == (-1, "default", 0)
    assert data["description"] == "updated default"
    assert sum(request.method == "PUT" for request in requests) == 1


@pytest.mark.parametrize(
    ("tenant_id", "tenant_code", "queue_id"),
    [
        (7, "ordinary", 0),
        (-1, "default", -1),
        (-1, "default", False),
        (-1, "default", None),
    ],
)
def test_tenant_unassigned_queue_exception_rejects_other_native_rows(
    monkeypatch: pytest.MonkeyPatch,
    tenant_id: int,
    tenant_code: str,
    queue_id: int | None,
) -> None:
    requests = _install_transport(
        monkeypatch,
        "3.4.3",
        native_id=tenant_id,
        native_code=tenant_code,
        native_queue_id=queue_id,
    )
    result = CliRunner().invoke(app, ["tenant", "list"])
    assert result.exit_code == 1
    assert json.loads(result.stderr)["error"]["type"] == "api_transport_error"
    assert all(request.method == "GET" for request in requests)
