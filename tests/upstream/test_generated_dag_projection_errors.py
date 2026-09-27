from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.runtime_instances import RuntimeInstanceAdapter
from dsctl.upstream.wire import WireContractError
from dsctl.upstream.workflows import WorkflowAdapter
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


def _scope() -> WorkflowScope:
    project = ProjectRef(NativeCode(101), "demo", None)
    workflow = WorkflowRef(NativeCode(202), "daily", 7)
    return WorkflowScope(
        project=project,
        workflow=workflow,
        view=WorkflowView(
            ref=workflow,
            project_native=project.native,
            id=1,
            description=None,
            global_params="[]",
            global_param_map=None,
            create_time=None,
            update_time=None,
            user_id=1,
            user_name="admin",
            project_name=project.name,
            timeout=0,
            release_state="ONLINE",
            execution_type=None,
            include_execution_type=False,
        ),
    )


@pytest.mark.parametrize(
    ("version", "path", "definition_field", "relation_field"),
    [
        (
            "2.0.9",
            "/dolphinscheduler/projects/101/process-definition/202",
            "processDefinition",
            "processTaskRelationList",
        ),
        (
            "3.4.2",
            "/dolphinscheduler/projects/101/workflow-definition/202",
            "workflowDefinition",
            "workflowTaskRelationList",
        ),
    ],
)
def test_workflow_dag_missing_definition_reports_its_exact_family_field(
    version: str,
    path: str,
    definition_field: str,
    relation_field: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == path
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    definition_field: None,
                    relation_field: [],
                    "taskDefinitionList": [],
                },
            },
        )

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(profile.ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).workflows.dag(
            _scope(),
            action="workflow.get",
        )

    assert exc_info.value.details == {
        "ds_version": version,
        "resource": "workflow",
        "field": definition_field,
        "reason": "workflow DAG omitted its definition",
    }


@pytest.mark.parametrize(
    (
        "version",
        "definition_field",
        "relation_field",
    ),
    [
        (
            "2.0.9",
            "processDefinition",
            "processTaskRelationList",
        ),
        (
            "3.4.2",
            "workflowDefinition",
            "workflowTaskRelationList",
        ),
    ],
)
def test_workflow_dag_malformed_relations_report_their_exact_family_field(
    version: str,
    definition_field: str,
    relation_field: str,
) -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    definition = SimpleNamespace(
        id=1,
        code=202,
        name="daily",
        version=7,
        projectCode=101,
        description=None,
        globalParams="[]",
        globalParamMap=None,
        createTime=None,
        updateTime=None,
        userId=1,
        userName="admin",
        projectName="demo",
        timeout=0,
        releaseState="ONLINE",
        scheduleReleaseState=None,
    )
    payload = SimpleNamespace(
        **{
            definition_field: definition,
            relation_field: {"unexpected": "mapping"},
            "taskDefinitionList": [],
        }
    )

    def malformed_detail(primitive: str, args: JsonObject) -> SimpleNamespace:
        assert primitive == "definition_get"
        assert args == {"projectCode": 101, "code": 202}
        return payload

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(profile.ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_client:
        operations = adapter.bind(profile, http_client=http_client).workflows
        operations = replace(
            operations,
            programs=cast("Any", SimpleNamespace(call=malformed_detail)),
        )
        with pytest.raises(ApiTransportError) as exc_info:
            operations.dag(_scope(), action="workflow.get")

    assert exc_info.value.details == {
        "ds_version": version,
        "resource": "workflow",
        "field": relation_field,
        "reason": "generated workflow relations violated their exact contract",
    }
    assert requests_seen == 0


def test_workflow_dag_contract_failure_is_a_stable_response_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == (
            "/dolphinscheduler/projects/101/process-definition/202"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "processDefinition": {
                        "code": 202,
                        "projectCode": 101,
                        "version": 7,
                    },
                    "processTaskRelationList": [],
                    "taskDefinitionList": [
                        {
                            "code": 303,
                            "name": "extract",
                            "version": 4,
                            "projectCode": 999,
                        }
                    ],
                },
            },
        )

    profile = make_profile(ds_version="2.0.9")
    adapter = WorkflowAdapter.for_version(profile.ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).workflows.dag(
            _scope(),
            action="workflow.get",
        )

    error = exc_info.value
    assert error.details == {
        "ds_version": "2.0.9",
        "resource": "workflow",
        "field": "taskDefinitionList",
        "reason": "generated workflow DAG violated its exact task contract",
    }
    assert error.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "response",
    }
    assert isinstance(error.__cause__, WireContractError)


@pytest.mark.parametrize(
    (
        "version",
        "instance_path",
        "definition_code_field",
        "definition_field",
        "relation_field",
    ),
    [
        (
            "2.0.9",
            "/dolphinscheduler/projects/900/process-instances/31",
            "processDefinitionCode",
            "processDefinition",
            "processTaskRelationList",
        ),
        (
            "3.4.2",
            "/dolphinscheduler/projects/900/workflow-instances/31",
            "workflowDefinitionCode",
            "workflowDefinition",
            "workflowTaskRelationList",
        ),
    ],
)
def test_runtime_dag_missing_definition_reports_its_exact_family_field(
    version: str,
    instance_path: str,
    definition_code_field: str,
    definition_field: str,
    relation_field: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path == "/dolphinscheduler/projects/900":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {"code": 900, "name": "orders"},
                },
            )
        assert request.url.path == instance_path
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "id": 31,
                    definition_code_field: 1300,
                    "processDefinitionVersion": 2,
                    "dagData": {
                        definition_field: None,
                        relation_field: [],
                        "taskDefinitionList": [],
                    },
                },
            },
        )

    profile = make_profile(ds_version=version)
    adapter = RuntimeInstanceAdapter.for_version(profile.ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(
            profile,
            http_client=http_client,
        ).instances.get_workflow_instance(
            project_selector="900",
            workflow_instance_id=31,
        )

    assert exc_info.value.details == {
        "ds_version": version,
        "resource": "workflow-instance",
        "field": definition_field,
        "reason": "workflow DAG omitted its definition",
    }


def test_runtime_dag_contract_failure_is_a_stable_response_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path == "/dolphinscheduler/projects/900":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {"code": 900, "name": "orders"},
                },
            )
        assert request.url.path == (
            "/dolphinscheduler/projects/900/process-instances/31"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "id": 31,
                    "processDefinitionCode": 1300,
                    "processDefinitionVersion": 2,
                    "dagData": {
                        "processDefinition": {
                            "code": 1300,
                            "projectCode": 900,
                            "version": 2,
                        },
                        "processTaskRelationList": [],
                        "taskDefinitionList": [
                            {
                                "code": 8800,
                                "name": "load",
                                "version": 3,
                                "projectCode": 999,
                            }
                        ],
                    },
                },
            },
        )

    profile = make_profile(ds_version="2.0.9")
    adapter = RuntimeInstanceAdapter.for_version(profile.ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(
            profile,
            http_client=http_client,
        ).instances.get_workflow_instance(
            project_selector="900",
            workflow_instance_id=31,
        )

    error = exc_info.value
    assert error.details == {
        "ds_version": "2.0.9",
        "resource": "workflow-instance",
        "field": "taskDefinitionList",
        "reason": "generated workflow DAG violated its exact task contract",
    }
    assert error.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "response",
    }
    assert isinstance(error.__cause__, WireContractError)
