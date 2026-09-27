from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest

import dsctl.upstream.task_definition_wire as task_definition_wire_module
from dsctl.client import DolphinSchedulerClient
from dsctl.services._workflow.identity import task_identities_by_name
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.task_definition_wire import (
    bind_task_definition_wire,
    project_exact_workflow_dag,
    task_update_contract_features,
)
from dsctl.upstream.wire import WireContractError, WireResponseDecodeError
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.config import ClusterProfile


_CODE_NATIVE_VERSIONS = (
    *(f"2.0.{patch}" for patch in range(10)),
    *(f"3.0.{patch}" for patch in range(7)),
    *(f"3.1.{patch}" for patch in range(10)),
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_EXECUTABLE_UPDATE_VERSIONS = _CODE_NATIVE_VERSIONS[4:]
_DEPENDENCE_GETTER_VERSIONS = frozenset(_CODE_NATIVE_VERSIONS[:-5])


def test_ds200_task_update_contract_features_include_whole_workflow_dependencies() -> (
    None
):
    features = task_update_contract_features("2.0.0")

    assert features.request_fields == frozenset(
        {
            "delayTime",
            "description",
            "environmentCode",
            "failRetryInterval",
            "failRetryTimes",
            "flag",
            "name",
            "resourceIds",
            "taskParams",
            "taskPriority",
            "taskType",
            "timeout",
            "timeoutFlag",
            "timeoutNotifyStrategy",
            "workerGroup",
        }
    )
    assert features.dependency_update is True


@pytest.mark.parametrize("ds_version", _EXECUTABLE_UPDATE_VERSIONS)
def test_every_code_native_profile_binds_its_exact_generated_task_recipe(
    ds_version: str,
) -> None:
    client = DolphinSchedulerClient(_profile(ds_version))

    with client:
        wire = bind_task_definition_wire(client)
        prepared = wire.prepare_update(
            project_code=7,
            task_code=7001,
            task_definition_json='{"name":"report"}',
            upstream_codes=[6001],
        )

    dependency_update = tuple(map(int, ds_version.split("."))) >= (3, 2, 1)
    expected_path = "/projects/7/task-definition/7001"
    if dependency_update:
        expected_path += "/with-upstream"
    assert wire.ds_version == ds_version
    assert wire.update_policy.dependency_update is dependency_update
    assert wire.update_policy.requires_unique_workflow_binding is (
        not dependency_update
    )
    assert prepared.request.path == expected_path
    assert prepared.request.form == {
        "taskDefinitionJsonObj": '{"name":"report"}',
        **({"upstreamCodes": "6001"} if dependency_update else {}),
    }
    assert ("isCache" in wire.top_level_field_policy.opaque_preservation) is (
        ds_version in {"3.2.0", "3.2.1", "3.2.2"}
    )
    assert ("dependence" in wire.top_level_field_policy.response_derived) is (
        ds_version in _DEPENDENCE_GETTER_VERSIONS
    )
    assert "dependence" not in wire.top_level_field_policy.request_payload


def test_ds200_read_recipe_binds_without_an_update_program() -> None:
    client = DolphinSchedulerClient(_profile("2.0.0"))

    with client:
        wire = bind_task_definition_wire(client)

    assert wire.ds_version == "2.0.0"
    assert wire.update_policy.update_available is False
    assert wire.update_policy.dependency_update is True
    with pytest.raises(
        WireContractError,
        match="no coherent standalone task-update program",
    ):
        wire.prepare_update(
            project_code=7,
            task_code=7001,
            task_definition_json='{"name":"report"}',
            upstream_codes=[],
        )


@pytest.mark.parametrize(
    "ds_version",
    ["3.4.1", "3.4.2"],
)
def test_exact_task_detail_returns_typed_and_lossless_payload(
    ds_version: str,
) -> None:
    calls = 0
    raw_task = {
        "id": 17,
        "code": 7001,
        "name": "report",
        "version": 3,
        "projectCode": 7,
        "taskType": "SQL",
        "taskParams": {
            "type": "MYSQL",
            "datasource": 1,
            "sql": "select 1",
            "sqlType": 0,
            "futureNested": {"preserve": True},
        },
        "futureTopLevel": {"preserve": True},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/projects/7/task-definition/7001"
        assert request.headers["token"] == "runtime-secret"
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": raw_task},
        )

    client = DolphinSchedulerClient(
        _profile(ds_version),
        transport=httpx.MockTransport(handler),
    )
    with client:
        result = bind_task_definition_wire(client).get(project_code=7, task_code=7001)

    assert calls == 1
    assert result.payload.code == 7001
    assert result.payload.projectCode == 7
    assert result.raw_payload == raw_task
    assert result.request.path == "/projects/7/task-definition/7001"


@pytest.mark.parametrize(
    ("ds_version", "response_data"),
    [("3.4.1", None), ("3.4.2", 7001)],
)
def test_exact_task_update_prepares_and_applies_the_same_generated_form(
    ds_version: str,
    response_data: int | None,
) -> None:
    calls = 0
    task_json = '{"name":"report","futureTopLevel":{"preserve":true}}'

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.method == "PUT"
        assert request.url.path == (
            "/dolphinscheduler/projects/7/task-definition/7001/with-upstream"
        )
        assert parse_qs(request.content.decode(), strict_parsing=True) == {
            "taskDefinitionJsonObj": [task_json],
            "upstreamCodes": ["6001,6002"],
        }
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": response_data},
        )

    client = DolphinSchedulerClient(
        _profile(ds_version),
        transport=httpx.MockTransport(handler),
    )
    with client:
        wire = bind_task_definition_wire(client)
        prepared = wire.prepare_update(
            project_code=7,
            task_code=7001,
            task_definition_json=task_json,
            upstream_codes=[6001, 6002],
        )
        assert calls == 0
        result = wire.apply_update(prepared)

    assert calls == 1
    assert result.payload == response_data
    assert result.raw_payload == response_data
    assert result.request == prepared.request
    assert prepared.request.form == {
        "taskDefinitionJsonObj": task_json,
        "upstreamCodes": "6001,6002",
    }


def test_342_task_update_marks_a_null_response_as_completed_before_decode() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    client = DolphinSchedulerClient(
        _profile("3.4.2"),
        transport=httpx.MockTransport(handler),
    )
    with client:
        wire = bind_task_definition_wire(client)
        prepared = wire.prepare_update(
            project_code=7,
            task_code=7001,
            task_definition_json='{"name":"report"}',
            upstream_codes=[],
        )
        with pytest.raises(
            WireResponseDecodeError,
            match="generated API contract",
        ) as exc_info:
            wire.apply_update(prepared)

    assert calls == 1
    assert exc_info.value.details["wire_response_received"] is True


@pytest.mark.parametrize("ds_version", ["3.1.9", "3.2.0"])
@pytest.mark.parametrize("response_data", ["7001", 7001.0, True])
def test_standalone_task_update_rejects_coerced_integer_responses(
    ds_version: str,
    response_data: object,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.method == "PUT"
        assert request.url.path == "/dolphinscheduler/projects/7/task-definition/7001"
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": response_data},
        )

    client = DolphinSchedulerClient(
        _profile(ds_version),
        transport=httpx.MockTransport(handler),
    )
    with client:
        wire = bind_task_definition_wire(client)
        prepared = wire.prepare_update(
            project_code=7,
            task_code=7001,
            task_definition_json='{"name":"report"}',
            upstream_codes=[],
        )
        with pytest.raises(
            WireResponseDecodeError,
            match="generated API contract",
        ) as exc_info:
            wire.apply_update(prepared)

    assert calls == 1
    assert exc_info.value.details["wire_response_received"] is True


def test_exact_task_recipe_fingerprints_are_profile_bound() -> None:
    client_341 = DolphinSchedulerClient(_profile("3.4.1"))
    client_342 = DolphinSchedulerClient(_profile("3.4.2"))
    with client_341, client_342:
        wire_341 = bind_task_definition_wire(client_341)
        wire_342 = bind_task_definition_wire(client_342)

    assert wire_341.ds_version == "3.4.1"
    assert wire_342.ds_version == "3.4.2"
    assert wire_341.recipe_fingerprint.startswith("sha256:")
    assert wire_342.recipe_fingerprint.startswith("sha256:")
    assert wire_341.recipe_fingerprint != wire_342.recipe_fingerprint
    policy_341 = wire_341.top_level_field_policy
    policy_342 = wire_342.top_level_field_policy
    assert policy_341 == policy_342
    assert "taskParams" in policy_342.request_payload
    assert "taskParams" in policy_342.opaque_preservation
    assert "version" in policy_342.server_managed
    assert "taskParamMap" in policy_342.response_derived
    assert "workflowTaskRelationList" in policy_342.relation_projection
    assert "futureTopLevel" not in policy_342.classified_fields


def test_task_field_policy_fails_closed_on_generated_field_drift() -> None:
    client = DolphinSchedulerClient(_profile("3.4.2"))
    with client:
        policy = bind_task_definition_wire(client).top_level_field_policy

    with pytest.raises(WireContractError, match=r"unclassified=.*futureTopLevel"):
        policy.validate_generated_fields({*policy.classified_fields, "futureTopLevel"})


def test_task_wire_rejects_computed_fields_already_declared_by_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = task_definition_wire_module._TASK_SPEC_BY_VERSION["2.0.9"]
    monkeypatch.setitem(
        task_definition_wire_module._TASK_SPEC_BY_VERSION,
        "2.0.9",
        replace(
            spec,
            computed_response_fields=frozenset({"taskParamMap"}),
        ),
    )
    client = DolphinSchedulerClient(_profile("2.0.9"))

    with (
        client,
        pytest.raises(
            WireContractError,
            match=r"computed task response fields overlap.*taskParamMap",
        ),
    ):
        bind_task_definition_wire(client)


@pytest.mark.parametrize(
    ("ds_version", "wire_cache"),
    [
        ("3.2.0", "NO"),
        ("3.2.1", "YES"),
        ("3.2.2", "NO"),
        ("3.3.1", None),
    ],
)
def test_exact_dag_projection_preserves_task_cache_for_workflow_edit(
    ds_version: str,
    wire_cache: str | None,
) -> None:
    workflow_field = (
        "processDefinition" if ds_version.startswith("3.2.") else "workflowDefinition"
    )
    relation_field = (
        "processTaskRelationList"
        if ds_version.startswith("3.2.")
        else "workflowTaskRelationList"
    )
    task_values: dict[str, object] = {
        "code": 7_001,
        "name": "report",
        "version": 3,
        "projectCode": 7,
    }
    if wire_cache is not None:
        task_values["isCache"] = wire_cache
    adapter = (
        WORKFLOW_PROGRAMS.profile(ds_version)
        .program("definition_get")
        .codec.response_adapter
    )
    assert adapter is not None
    dag = adapter.validate_python(
        {
            workflow_field: {"code": 8_001, "projectCode": 7, "version": 2},
            relation_field: [],
            "taskDefinitionList": [task_values],
        }
    )

    projected = project_exact_workflow_dag(
        dag,
        ds_version=ds_version,
        expected_project_code=7,
        expected_workflow_code=8_001,
    )
    assert projected.taskDefinitionList is not None
    projected_task = projected.taskDefinitionList[0]

    assert getattr(projected_task, "isCache", None) == wire_cache
    assert task_identities_by_name(projected)["report"].is_cache == wire_cache


def _profile(ds_version: str) -> ClusterProfile:
    return make_profile(ds_version=ds_version, api_token="runtime-secret")
