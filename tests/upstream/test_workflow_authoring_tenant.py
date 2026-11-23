from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.upstream.definition_models import NativeCode, ProjectRef
from dsctl.upstream.workflows import WorkflowAdapter
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.upstream.workflow_graph import WorkflowCreatePayload
    from dsctl.upstream.workflows import WorkflowDomain


_CURRENT_USER = "/dolphinscheduler/users/get-user-info"
_TENANTS = "/dolphinscheduler/tenants"
_DEFINITIONS = "/dolphinscheduler/projects/101/process-definition"
_PROJECT = ProjectRef(NativeCode(101), "etl", None)
_PAYLOAD: WorkflowCreatePayload = {
    "name": "daily",
    "description": None,
    "globalParams": "[]",
    "locations": "[]",
    "timeout": 0,
    "taskDefinitionJson": "[]",
    "taskRelationJson": "[]",
    "executionType": "PARALLEL",
}


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


@contextmanager
def _workflow(
    version: str, *responses: httpx.Response
) -> Iterator[tuple[WorkflowDomain, list[httpx.Request]]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert len(requests) < len(responses), f"unexpected request: {request}"
        requests.append(request)
        return responses[len(requests) - 1]

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        yield (
            WorkflowAdapter.for_version(version).bind(profile, http_client=client),
            requests,
        )


def test_317_create_resolves_assigned_id_and_posts_the_prepared_tenant() -> None:
    current = _success(
        {
            "userName": "admin",
            "userType": "ADMIN_USER",
            "tenantCode": None,
            "tenantId": 6,
        }
    )
    tenants = _success(
        {
            "currentPage": 1,
            "totalPage": 1,
            "total": 2,
            "totalList": [
                {"id": 1, "tenantCode": "default", "queueId": 1},
                {"id": 6, "tenantCode": "analytics", "queueId": 1},
            ],
        }
    )
    created = _success({"code": 202, "name": "daily", "projectCode": 101})
    with _workflow("3.1.7", current, tenants, created) as (domain, requests):
        prepared = domain.prepare_definition_create(
            _PROJECT, payload=_PAYLOAD, name="daily", description=None
        )
        assert [(r.method, r.url.path) for r in requests] == [
            ("GET", _CURRENT_USER),
            ("GET", _TENANTS),
        ]
        assert prepared.request.form is not None
        assert prepared.request.form["tenantCode"] == "analytics"
        assert "tenantId" not in prepared.request.form

        domain.workflows.apply_create(prepared)

    assert len(requests) == 3
    post = requests[-1]
    assert (post.method, post.url.path) == ("POST", _DEFINITIONS)
    assert parse_qs(post.content.decode())["tenantCode"] == ["analytics"]


def test_317_create_uses_current_user_code_without_tenant_lookup() -> None:
    current = _success({"tenantCode": "reported", "tenantId": 6})
    with _workflow("3.1.7", current) as (domain, requests):
        prepared = domain.prepare_definition_create(
            _PROJECT, payload=_PAYLOAD, name="daily", description=None
        )

    assert prepared.request.form is not None
    assert prepared.request.form["tenantCode"] == "reported"
    assert [(r.method, r.url.path) for r in requests] == [("GET", _CURRENT_USER)]


def test_320_create_omits_definition_tenant_without_reading_identity() -> None:
    with _workflow("3.2.0") as (domain, requests):
        prepared = domain.prepare_definition_create(
            _PROJECT, payload=_PAYLOAD, name="daily", description=None
        )

    assert prepared.request.form is not None
    assert "tenantCode" not in prepared.request.form
    assert requests == []


@pytest.mark.parametrize("id_fields", [{}, {"tenantId": 0}, {"tenantId": -1}])
def test_317_no_usable_assignment_stops_before_tenant_lookup_or_write(
    id_fields: dict[str, int],
) -> None:
    current = _success({"tenantCode": None, **id_fields})
    with (
        _workflow("3.1.7", current) as (domain, requests),
        pytest.raises(UserInputError),
    ):
        domain.prepare_definition_create(
            _PROJECT, payload=_PAYLOAD, name="daily", description=None
        )

    assert [(r.method, r.url.path) for r in requests] == [("GET", _CURRENT_USER)]


def test_317_invisible_assignment_never_uses_another_tenants_code() -> None:
    current = _success({"tenantCode": None, "tenantId": 6})
    tenants = _success(
        {
            "currentPage": 1,
            "totalPage": 1,
            "total": 1,
            "totalList": [{"id": 1, "tenantCode": "default", "queueId": 1}],
        }
    )
    with (
        _workflow("3.1.7", current, tenants) as (domain, requests),
        pytest.raises(NotFoundError) as error,
    ):
        domain.prepare_definition_create(
            _PROJECT, payload=_PAYLOAD, name="daily", description=None
        )

    assert error.value.suggestion
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", _CURRENT_USER),
        ("GET", _TENANTS),
    ]


def test_317_denied_tenant_lookup_stops_before_workflow_write() -> None:
    current = _success({"tenantCode": None, "tenantId": 6})
    denied = httpx.Response(
        200, json={"code": 30001, "msg": "permission denied", "data": None}
    )
    with (
        _workflow("3.1.7", current, denied) as (domain, requests),
        pytest.raises(PermissionDeniedError) as error,
    ):
        domain.prepare_definition_create(
            _PROJECT, payload=_PAYLOAD, name="daily", description=None
        )

    assert "tenant" in error.value.message.lower()
    assert error.value.suggestion
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", _CURRENT_USER),
        ("GET", _TENANTS),
    ]


def _definition(tenant: dict[str, object]) -> httpx.Response:
    return _success(
        {
            "processDefinition": {
                "code": 202,
                "projectCode": 101,
                "name": "daily",
                "version": 3,
                **tenant,
            },
            "processTaskRelationList": [],
            "taskDefinitionList": [],
        }
    )


@pytest.mark.parametrize("version", ["2.0.0", "3.1.7", "3.1.9"])
@pytest.mark.parametrize(
    ("tenant", "expected", "lookup"),
    [
        ({"tenantId": 23, "tenantCode": "workflow-owner"}, "workflow-owner", False),
        ({"tenantId": -1, "tenantCode": None}, "default", False),
        ({"tenantId": 23, "tenantCode": None}, "workflow-owner", True),
    ],
)
def test_edit_preserves_the_stored_definition_tenant(
    version: str, tenant: dict[str, object], expected: str, *, lookup: bool
) -> None:
    responses = [_definition(tenant)]
    expected_calls = [("GET", f"{_DEFINITIONS}/202")]
    if lookup:
        responses.append(
            _success(
                {
                    "total": 1,
                    "totalPage": 1,
                    "currentPage": 1,
                    "totalList": [
                        {"id": 23, "tenantCode": "workflow-owner", "queueId": 1}
                    ],
                }
            )
        )
        expected_calls.append(("GET", _TENANTS))
    with _workflow(version, *responses) as (domain, requests):
        scope = domain.workflows.resolve_workflow_by_code(_PROJECT, 202)
        prepared = domain.prepare_definition_update(
            scope,
            payload={**_PAYLOAD, "releaseState": "OFFLINE"},
            name="daily",
            description=None,
        )
    assert [(r.method, r.url.path) for r in requests] == expected_calls
    assert prepared.request.form is not None
    assert prepared.request.form["tenantCode"] == expected


@pytest.mark.parametrize(
    "tenant",
    [
        {},
        {"tenantId": 0},
        {"tenantId": True},
        {"tenantId": "-1"},
        {"tenantId": 23, "tenantCode": "default"},
    ],
)
def test_edit_rejects_unrepresentable_definition_tenant(
    tenant: dict[str, object],
) -> None:
    with _workflow("3.1.7", _definition(tenant)) as (domain, requests):
        scope = domain.workflows.resolve_workflow_by_code(_PROJECT, 202)
        with pytest.raises(ApiTransportError) as error:
            domain.prepare_definition_update(
                scope,
                payload={**_PAYLOAD, "releaseState": "OFFLINE"},
                name="daily",
                description=None,
            )
    assert error.value.details["field"] == "tenantCode"
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", f"{_DEFINITIONS}/202")
    ]


def test_whole_workflow_task_edit_rejects_conflicting_dag_tenant_code() -> None:
    definition = _definition({"tenantId": 23, "tenantCode": "workflow-owner"})
    with _workflow("2.0.0", definition) as (domain, requests):
        scope = domain.workflows.resolve_workflow_by_code(_PROJECT, 202)
        with pytest.raises(ConflictError) as error:
            domain.workflows.prepare_update(
                scope,
                tenant_code="different-tenant",
                name="daily",
                description=None,
                global_params="[]",
                locations="[]",
                timeout=0,
                task_relation_json="[]",
                task_definition_json="[]",
                execution_type=None,
                release_state="OFFLINE",
            )
    assert error.value.details["mutation_applied"] is False
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", f"{_DEFINITIONS}/202")
    ]
