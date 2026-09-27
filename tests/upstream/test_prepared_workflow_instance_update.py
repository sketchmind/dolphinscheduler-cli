from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.runtime_instance_profiles import RUNTIME_INSTANCE_PROFILES
from dsctl.upstream.definition_models import NativeCode, ProjectRef
from dsctl.upstream.runtime_instances import (
    LocatedWorkflowInstance,
    RuntimeInstanceAdapter,
    WorkflowInstanceSnapshot,
    WorkflowMutationSnapshot,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue


_TENANT_VERSIONS = ("2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9")
_ALL_TENANT_VERSIONS = tuple(
    version
    for version, profile in RUNTIME_INSTANCE_PROFILES.items()
    if profile.update_shape == "tenant"
)
_PROCESS_VERSIONS = (*_TENANT_VERSIONS, "3.2.0", "3.2.1", "3.2.2")
_MODERN_VERSIONS = (*_PROCESS_VERSIONS, "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2")


def _located(
    version: str,
    *,
    tenant: str | None = "tenant-a",
    tenant_id: int | None = 11,
) -> LocatedWorkflowInstance:
    project = ProjectRef(NativeCode(7), "etl-prod", None)
    instance = WorkflowInstanceSnapshot(
        ds_version=version,
        id=31,
        project=project,
        workflow_native=NativeCode(13),
        workflowDefinitionVersion=1,
        state="SUCCESS",
        recovery=None,
        startTime=None,
        endTime=None,
        runTimes=1,
        name="daily-orders-1",
        host=None,
        commandType=None,
        taskDependType=None,
        failureStrategy=None,
        warningType=None,
        scheduleTime=None,
        executorId=7,
        executorName=None,
        tenantCode=tenant,
        queue=None,
        duration=None,
        workflowInstancePriority=None,
        workerGroup=None,
        environmentCode=None,
        timeout=0,
        dryRun=0,
        restartTime=None,
        dagData=None,
        tenant_id=tenant_id,
    )
    return LocatedWorkflowInstance(project, instance)


@pytest.mark.parametrize("version", _MODERN_VERSIONS)
def test_modern_instance_update_previews_and_applies_the_same_exact_wire(
    version: str,
) -> None:
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return httpx.Response(
            200,
            json={"code": 0, "data": {"code": 13, "name": "daily", "version": 2}},
        )

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .instances
        )
        prepared = operations.prepare_workflow_instance_update(
            _located(version),
            task_relation_json="[]",
            task_definition_json="[]",
            sync_define=True,
            global_params="[]",
            locations="[]",
            timeout=45,
        )
        family = "process" if version in _PROCESS_VERSIONS else "workflow"
        path = f"/projects/7/{family}-instances/31"
        expected_form: dict[str, str | int | bool] = {
            "taskRelationJson": "[]",
            "taskDefinitionJson": "[]",
            "syncDefine": True,
            "globalParams": "[]",
            "locations": "[]",
            "timeout": 45,
        }
        if version in _TENANT_VERSIONS:
            expected_form["tenantCode"] = "tenant-a"
        assert requests_seen == []
        assert prepared.request.method == "PUT"
        assert prepared.request.path == path
        assert prepared.request.form == expected_form

        preview = prepared.request
        assert isinstance(preview.form, dict)
        preview.form["timeout"] = 999
        assert prepared.request.form == expected_form
        assert operations.apply_workflow_instance_update(
            prepared
        ) == WorkflowMutationSnapshot(code=13, name="daily", version=2)

    assert len(requests_seen) == 1
    request = requests_seen[0]
    assert request.method == "PUT"
    assert request.url.path == f"/dolphinscheduler{path}"
    expected_wire = {key: [str(value)] for key, value in expected_form.items()}
    expected_wire["syncDefine"] = ["true"]
    assert parse_qs(request.content.decode(), keep_blank_values=True) == expected_wire


@pytest.mark.parametrize("version", _ALL_TENANT_VERSIONS)
def test_prepare_recovers_missing_tenant_from_matching_definition(
    version: str,
) -> None:
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        assert request.method == "GET"
        assert request.url.path == (
            "/dolphinscheduler/projects/7/process-definition/13"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "processDefinition": {
                        "code": 13,
                        "name": "daily-orders",
                        "version": 1,
                        "projectCode": 7,
                        "tenantId": 11,
                        "tenantCode": "tenant-a",
                    },
                    "processTaskRelationList": [],
                    "taskDefinitionList": [],
                },
            },
        )

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .instances
        )
        prepared = operations.prepare_workflow_instance_update(
            _located(version, tenant=None),
            task_relation_json="[]",
            task_definition_json="[]",
            sync_define=False,
            global_params=None,
            locations=None,
            timeout=None,
        )
    assert len(requests_seen) == 1
    assert isinstance(prepared.request.form, dict)
    assert prepared.request.form["tenantCode"] == "tenant-a"


@pytest.mark.parametrize("version", ["2.0.0", "3.0.0", "3.1.0"])
def test_prepare_rejects_definition_tenant_mismatch(version: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "processDefinition": {
                        "code": 13,
                        "name": "daily-orders",
                        "version": 1,
                        "projectCode": 7,
                        "tenantId": 12,
                        "tenantCode": "tenant-b",
                    },
                    "processTaskRelationList": [],
                    "taskDefinitionList": [],
                },
            },
        )

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .instances
        )
        with pytest.raises(ApiTransportError) as error:
            operations.prepare_workflow_instance_update(
                _located(version, tenant=None),
                task_relation_json="[]",
                task_definition_json="[]",
                sync_define=False,
                global_params=None,
                locations=None,
                timeout=None,
            )
    assert error.value.details["field"] == "tenantCode"
    assert error.value.details["reason"] == (
        "instance tenantId does not match the workflow definition tenantId; "
        "refusing to substitute a tenant code"
    )


@pytest.mark.parametrize("version", _ALL_TENANT_VERSIONS)
def test_prepare_preserves_native_default_tenant_sentinel(version: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"Default-tenant preparation sent {request.method} {request.url}")

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .instances
        )
        prepared = operations.prepare_workflow_instance_update(
            _located(version, tenant=None, tenant_id=-1),
            task_relation_json="[]",
            task_definition_json="[]",
            sync_define=False,
            global_params=None,
            locations=None,
            timeout=None,
        )
    assert isinstance(prepared.request.form, dict)
    assert prepared.request.form["tenantCode"] == "default"


def test_prepare_does_not_infer_tenant_when_identity_is_missing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"Missing-tenant preparation sent {request.method} {request.url}")

    version = "3.1.0"
    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .instances
        )
        with pytest.raises(ApiTransportError) as error:
            operations.prepare_workflow_instance_update(
                _located(version, tenant=None, tenant_id=None),
                task_relation_json="[]",
                task_definition_json="[]",
                sync_define=False,
                global_params=None,
                locations=None,
                timeout=None,
            )
    assert error.value.details["field"] == "tenantId"


@pytest.mark.parametrize("mismatch", ["profile", "codec", "request"])
def test_prepared_instance_update_rejects_mismatched_identity_before_io(
    mismatch: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"Invalid prepared update sent {request.method} {request.url}")

    profile = make_profile(ds_version="3.4.1")
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version("3.4.1")
            .bind(profile, http_client=client)
            .instances
        )
        prepared = operations.prepare_workflow_instance_update(
            _located("3.4.1"),
            task_relation_json="[]",
            task_definition_json="[]",
            sync_define=False,
            global_params=None,
            locations=None,
            timeout=None,
        )
        call = prepared._wire_call
        if mismatch == "profile":
            call = replace(call, ds_version="3.4.0")
        elif mismatch == "codec":
            call = replace(call, _codec=replace(call._codec))
        else:
            call = replace(call, _request=replace(call.request, form={"timeout": 999}))
        with pytest.raises(WireContractError):
            operations.apply_workflow_instance_update(
                replace(prepared, _wire_call=call)
            )


@pytest.mark.parametrize(
    ("response", "phase"),
    [(None, "mutation_request"), ("invalid-definition", "mutation_response")],
)
def test_instance_update_keeps_uncertain_dispatch_and_decode_errors(
    response: JsonValue,
    phase: str,
) -> None:
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        if response is None:
            message = "disconnected after mutation"
            raise httpx.ReadTimeout(message, request=request)
        return httpx.Response(200, json={"code": 0, "data": response})

    profile = make_profile(ds_version="3.4.1")
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version("3.4.1")
            .bind(profile, http_client=client)
            .instances
        )
        with pytest.raises(ApiTransportError) as error:
            operations.update_workflow_instance(
                _located("3.4.1"),
                task_relation_json="[]",
                task_definition_json="[]",
                sync_define=False,
                global_params="[]",
                locations="[]",
                timeout=0,
            )
    assert len(requests_seen) == 1
    assert error.value.details["phase"] == phase
    outcome = (
        "mutation_may_have_applied"
        if phase == "mutation_request"
        else "mutation_applied"
    )
    assert error.value.details[outcome] is True


def test_modern_prepare_does_not_replace_legacy_instance_edit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"Unsupported prepare sent {request.method} {request.url}")

    profile = make_profile(ds_version="1.3.9")
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version("1.3.9")
            .bind(profile, http_client=client)
            .instances
        )
        with pytest.raises(UnsupportedFeatureError):
            operations.prepare_workflow_instance_update(
                _located("1.3.9"),
                task_relation_json="[]",
                task_definition_json="[]",
                sync_define=False,
                global_params=None,
                locations=None,
                timeout=None,
            )


@pytest.mark.parametrize(
    "version", ["2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.9", "3.4.1"]
)
@pytest.mark.parametrize("sync_define", [False, True])
def test_null_instance_update_payload_is_exact_and_requires_no_sync(
    version: str, *, sync_define: bool
) -> None:
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return httpx.Response(200, json={"code": 0, "data": None})

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            RuntimeInstanceAdapter.for_version(version)
            .bind(profile, http_client=client)
            .instances
        )
        prepared = operations.prepare_workflow_instance_update(
            _located(version),
            task_relation_json="[]",
            task_definition_json="[]",
            sync_define=sync_define,
            global_params="[]",
            locations="[]",
            timeout=45,
        )
        if version in {"2.0.0", "2.0.1", "2.0.2"} and not sync_define:
            assert operations.apply_workflow_instance_update(prepared) is None
        else:
            with pytest.raises(ApiTransportError):
                operations.apply_workflow_instance_update(prepared)
    assert len(requests_seen) == 1
