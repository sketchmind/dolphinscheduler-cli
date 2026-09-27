from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import ConflictError, InvalidStateError, UnsupportedFeatureError
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.services._whole_workflow_task_update import (
    CodeNativeWholeWorkflowTaskUpdate,
    PreparedWorkflowUpdate,
)
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream.definition_reads import DefinitionReads
from dsctl.upstream.definition_wire import CodeDefinitionWire
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_definition_wire import (
    TaskTopLevelFieldPolicy,
    TaskUpdateWirePolicy,
)
from dsctl.upstream.task_definitions import (
    TaskDefinitions,
    TaskSelector,
    TaskUpdateIntent,
)
from dsctl.upstream.task_update import compile_task_update
from dsctl.upstream.wire import (
    PreparedWireCallToken,
    WireExecution,
    WireRequest,
)
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeScheduleAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from dsctl.client import HttpFormValue
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.definition_models import WorkflowScope
    from dsctl.upstream.protocol import TaskPayloadRecord, WorkflowDagRecord


_DS200_REQUEST_FIELDS = frozenset(
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
_DS200_SERVER_FIELDS = frozenset(
    {"code", "createTime", "id", "projectCode", "updateTime", "userId", "version"}
)
_DS200_DERIVED_FIELDS = frozenset(
    {
        "dependence",
        "modifyBy",
        "projectName",
        "taskParamList",
        "taskParamMap",
        "userName",
    }
)
_DS200_FIELD_POLICY = TaskTopLevelFieldPolicy(
    request_payload=_DS200_REQUEST_FIELDS,
    server_managed=_DS200_SERVER_FIELDS,
    response_derived=_DS200_DERIVED_FIELDS,
    relation_projection=frozenset(),
    opaque_preservation=frozenset({"taskParams"}),
)
_DS200_UPDATE_POLICY = TaskUpdateWirePolicy(
    update_available=False,
    dependency_update=True,
    unsupported_patch_fields=frozenset(
        {"task_group_id", "task_group_priority", "cpu_quota", "memory_max"}
    ),
)


@dataclass
class _Ds200TaskWire:
    workflow_adapter: FakeWorkflowAdapter
    ds_version: str = "2.0.0"
    recipe_fingerprint: str = "sha256:ds200-task-read-recipe"
    get_calls: list[tuple[int, int]] = field(default_factory=list)
    describe_calls: list[tuple[int, int]] = field(default_factory=list)
    raw_revision: int = 0

    @property
    def top_level_field_policy(self) -> TaskTopLevelFieldPolicy:
        return _DS200_FIELD_POLICY

    @property
    def update_policy(self) -> TaskUpdateWirePolicy:
        return replace(
            _DS200_UPDATE_POLICY, dependency_update=self.ds_version != "2.0.3"
        )

    def get(
        self,
        *,
        project_code: int,
        task_code: int,
    ) -> WireExecution[TaskPayloadRecord]:
        self.get_calls.append((project_code, task_code))
        task = next(
            task
            for dag in self.workflow_adapter.dags.values()
            for task in (dag.taskDefinitionList or ())
            if task.code == task_code and task.projectCode == project_code
        )
        return WireExecution(
            payload=cast("TaskPayloadRecord", task),
            raw_payload=_task_raw(task),
            request=WireRequest(
                method="GET",
                path=f"/projects/{project_code}/task-definition/{task_code}",
                query=None,
                form=None,
                json=None,
                content=None,
            ),
        )

    def describe(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WireExecution[WorkflowDagRecord]:
        self.describe_calls.append((project_code, workflow_code))
        dag = self.workflow_adapter.describe(
            project_code=project_code,
            code=workflow_code,
        )
        return WireExecution(
            payload=cast("WorkflowDagRecord", dag),
            raw_payload=_dag_raw(dag, revision=self.raw_revision),
            request=WireRequest(
                method="GET",
                path=f"/projects/{project_code}/process-definition/{workflow_code}",
                query=None,
                form=None,
                json=None,
                content=None,
            ),
        )

    def prepare_update(
        self,
        *,
        project_code: int,
        task_code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> PreparedWireCallToken:
        del project_code, task_code, task_definition_json, upstream_codes
        message = "DS 2.0.0 must not use its standalone task update"
        raise AssertionError(message)

    def apply_update(
        self,
        prepared: PreparedWireCallToken,
    ) -> WireExecution[int | None]:
        del prepared
        message = "DS 2.0.0 must not use its standalone task update"
        raise AssertionError(message)


@dataclass
class _PreparedWorkflowUpdate:
    request: WireRequest
    apply: Callable[[], None]


@dataclass
class _Ds200WorkflowOperations:
    workflow_adapter: FakeWorkflowAdapter
    ds_version: str = "2.0.0"
    prepared: list[_PreparedWorkflowUpdate] = field(default_factory=list)
    applied: list[_PreparedWorkflowUpdate] = field(default_factory=list)

    def prepare_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> _PreparedWorkflowUpdate:
        form: dict[str, HttpFormValue] = {
            "name": name,
            "globalParams": global_params,
            "locations": locations,
            "timeout": timeout,
            "tenantCode": tenant_code or "default",
            "taskRelationJson": task_relation_json,
            "taskDefinitionJson": task_definition_json,
            "releaseState": release_state or "OFFLINE",
        }
        if description is not None:
            form["description"] = description
        prepared = _PreparedWorkflowUpdate(
            request=WireRequest(
                method="PUT",
                path=(
                    f"/projects/{scope.project.native.value}/process-definition/"
                    f"{scope.workflow.native.value}"
                ),
                query=None,
                form=form,
                json=None,
                content=None,
            ),
            apply=lambda: self.workflow_adapter.update(
                project_code=scope.project.native.value,
                workflow_code=scope.workflow.native.value,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
                release_state=release_state,
            ),
        )
        self.prepared.append(prepared)
        return prepared

    def apply_update(self, prepared: PreparedWorkflowUpdate) -> None:
        concrete = cast("_PreparedWorkflowUpdate", prepared)
        self.applied.append(concrete)
        concrete.apply()


def _module(
    ds_version: str = "2.0.0",
) -> tuple[
    TaskDefinitions,
    _Ds200TaskWire,
    _Ds200WorkflowOperations,
    FakeWorkflowAdapter,
]:
    project = FakeProject(code=7, name="etl-prod")
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        project_name_value="etl-prod",
        description="daily workflow",
        global_params_value='[{"prop":"bizdate","value":"2026-08-13"}]',
        global_param_map_value={"bizdate": "2026-08-13"},
        user_id_value=11,
        timeout=30,
        release_state_value=FakeEnumValue("OFFLINE"),
        version=5,
    )
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        version=3,
        project_code_value=7,
        description="source task",
        task_type_value="SHELL",
        task_params_value=(
            ' { "rawScript" : "echo source", "futureNested" : {"keep":true} } '
        ),
        worker_group_value="default",
        task_priority_value=FakeEnumValue("MEDIUM"),
        timeout_flag_value=FakeEnumValue("CLOSE"),
        flag_value=FakeEnumValue("YES"),
    )
    report = FakeTaskDefinition(
        code=7001,
        name="report",
        version=4,
        project_code_value=7,
        description="current description",
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo report","futureNested":{"keep":true}}',
        worker_group_value="analytics",
        task_priority_value=FakeEnumValue("MEDIUM"),
        timeout_flag_value=FakeEnumValue("CLOSE"),
        flag_value=FakeEnumValue("YES"),
    )
    dag = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[extract, report],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=0,
                post_task_code_value=201,
                post_task_version_value=3,
                condition_params_value={"futureRoot": True},
            ),
            FakeWorkflowTaskRelation(
                pre_task_code_value=201,
                post_task_code_value=7001,
                pre_task_version_value=3,
                post_task_version_value=4,
                condition_params_value={"futureEdge": "keep"},
            ),
        ],
    )
    workflow_adapter = FakeWorkflowAdapter([workflow], {101: dag})
    wire = _Ds200TaskWire(workflow_adapter, ds_version=ds_version)
    operations = _Ds200WorkflowOperations(workflow_adapter, ds_version=ds_version)
    whole_update = CodeNativeWholeWorkflowTaskUpdate(
        profile_version=ds_version,
        operations=operations,
        catalog=get_task_authoring_catalog(ds_version),
        compile_update=prepare_workflow_mutation_plan,
        task_request_fields=_DS200_REQUEST_FIELDS,
    )
    definitions = TaskDefinitions(
        profile_version=ds_version,
        definitions=DefinitionReads(
            CodeDefinitionWire(
                projects=FakeProjectAdapter([project]),
                workflows=workflow_adapter,
                schedules=FakeScheduleAdapter([]),
            )
        ),
        wire=wire,
        compile_update=compile_task_update,
        whole_workflow_update=whole_update,
    )
    return definitions, wire, operations, workflow_adapter


def _intent(
    document: JsonObject,
    *requested_fields: str,
) -> TaskUpdateIntent:
    return TaskUpdateIntent(
        selector=TaskSelector(
            project="etl-prod",
            workflow="daily-sync",
            task="report",
        ),
        patch=WorkflowPatchTaskSetSpec.model_validate(document),
        requested_fields=requested_fields,
    )


def test_ds200_prepare_captures_one_exact_whole_workflow_request_without_mutation() -> (
    None
):
    definitions, _wire, operations, workflow_adapter = _module()

    prepared = definitions.prepare_update(
        _intent({"description": "updated description"}, "description")
    )

    assert prepared.request.method == "PUT"
    assert prepared.request.path == "/projects/7/process-definition/101"
    assert prepared.request.form is not None
    assert prepared.request.form["tenantCode"] == "analytics"
    assert len(operations.prepared) == 1
    assert operations.applied == []
    assert workflow_adapter.update_calls == []


def test_ds200_apply_executes_the_captured_request_and_reads_back_the_task() -> None:
    definitions, _wire, operations, workflow_adapter = _module()
    prepared = definitions.prepare_update(
        _intent({"description": "updated description"}, "description")
    )

    outcome = definitions.apply(prepared)

    assert outcome.mutation_applied is True
    assert outcome.value.view.to_data()["description"] == "updated description"
    assert outcome.value.view.to_data()["version"] == 5
    assert operations.applied == [operations.prepared[0]]
    assert operations.applied[0] is operations.prepared[0]
    assert len(workflow_adapter.update_calls) == 1
    relation = next(
        relation
        for relation in workflow_adapter.dags[101].workflowTaskRelationList or ()
        if relation.postTaskCode == 7001
    )
    assert relation.postTaskVersion == 5


def test_ds200_request_preserves_native_payload_and_relation_fields() -> None:
    definitions, _wire, _operations, _workflow_adapter = _module()

    prepared = definitions.prepare_update(
        _intent({"description": "updated description"}, "description")
    )

    assert prepared.request.form is not None
    tasks = cast(
        "list[JsonObject]",
        json.loads(cast("str", prepared.request.form["taskDefinitionJson"])),
    )
    extract = next(task for task in tasks if task["code"] == 201)
    report = next(task for task in tasks if task["code"] == 7001)
    assert extract["taskParams"] == (
        ' { "rawScript" : "echo source", "futureNested" : {"keep":true} } '
    )
    assert extract["futureTaskField"] == {"preserve": "extract"}
    assert report["taskParams"] == (
        '{"rawScript":"echo report","futureNested":{"keep":true}}'
    )
    assert report["futureTaskField"] == {"preserve": "report"}
    assert report["description"] == "updated description"
    relations = cast(
        "list[JsonObject]",
        json.loads(cast("str", prepared.request.form["taskRelationJson"])),
    )
    edge = next(
        relation
        for relation in relations
        if relation["preTaskCode"] == 201 and relation["postTaskCode"] == 7001
    )
    assert edge["conditionParams"] == {"futureEdge": "keep"}
    assert edge["futureRelationField"] == "preserve"
    assert prepared.request.form["locations"] == (
        '{"201":{"x":13,"y":21,"future":"extract"},'
        '"7001":{"x":377,"y":34,"future":"report"}}'
    )


def test_ds200_command_update_preserves_unknown_target_task_parameters() -> None:
    definitions, _wire, _operations, _workflow_adapter = _module()

    prepared = definitions.prepare_update(
        _intent({"command": "echo updated"}, "command")
    )

    assert prepared.request.form is not None
    tasks = cast(
        "list[JsonObject]",
        json.loads(cast("str", prepared.request.form["taskDefinitionJson"])),
    )
    report = next(task for task in tasks if task["code"] == 7001)
    assert json.loads(cast("str", report["taskParams"])) == {
        "rawScript": "echo updated",
        "localParams": [],
        "resourceList": [],
        "futureNested": {"keep": True},
    }


def test_ds200_whole_workflow_update_can_clear_the_final_dependency() -> None:
    definitions, _wire, operations, workflow_adapter = _module()
    prepared = definitions.prepare_update(_intent({"depends_on": []}, "depends_on"))

    outcome = definitions.apply(prepared)

    assert outcome.mutation_applied is True
    assert outcome.value.view.to_data()["version"] == 4
    assert len(operations.applied) == 1
    relations = workflow_adapter.dags[101].workflowTaskRelationList or []
    assert all(
        not (relation.preTaskCode == 201 and relation.postTaskCode == 7001)
        for relation in relations
    )
    assert any(
        relation.preTaskCode == 0 and relation.postTaskCode == 7001
        for relation in relations
    )


def test_ds200_apply_rejects_any_stale_whole_graph_before_mutation() -> None:
    definitions, wire, operations, workflow_adapter = _module()
    prepared = definitions.prepare_update(
        _intent({"description": "updated description"}, "description")
    )
    dag = workflow_adapter.dags[101]
    assert dag.task_definition_list_value is not None
    dag.task_definition_list_value[0] = replace(
        dag.task_definition_list_value[0],
        description="concurrent unrelated edit",
    )
    wire.raw_revision += 1

    with pytest.raises(ConflictError, match=r"whole workflow.*stale") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_applied"] is False
    assert exc_info.value.details["reason"] == "whole_workflow_graph_changed"
    assert operations.applied == []
    assert workflow_adapter.update_calls == []


def test_ds200_no_op_prepares_preview_but_sends_no_mutation() -> None:
    definitions, _wire, operations, workflow_adapter = _module()
    prepared = definitions.prepare_update(
        _intent({"description": "current description"}, "description")
    )

    outcome = definitions.apply(prepared)

    assert prepared.no_change is True
    assert outcome.mutation_applied is False
    assert len(operations.prepared) == 1
    assert operations.applied == []
    assert workflow_adapter.update_calls == []


def test_ds200_online_workflow_is_rejected_before_preparing_a_mutation() -> None:
    definitions, _wire, operations, workflow_adapter = _module()
    workflow = workflow_adapter.workflows[0]
    online = replace(workflow, release_state_value=FakeEnumValue("ONLINE"))
    workflow_adapter.workflows[0] = online
    workflow_adapter.dags[101] = replace(
        workflow_adapter.dags[101],
        workflow_definition_value=online,
    )

    with pytest.raises(InvalidStateError, match="workflow to be offline"):
        definitions.prepare_update(
            _intent({"description": "updated description"}, "description")
        )

    assert operations.prepared == []
    assert operations.applied == []
    assert workflow_adapter.update_calls == []


def test_ds200_unsupported_task_field_is_rejected_before_any_read_or_mutation() -> None:
    definitions, wire, operations, workflow_adapter = _module()

    with pytest.raises(
        UnsupportedFeatureError,
        match="cannot express the requested task update fields",
    ) as exc_info:
        definitions.prepare_update(_intent({"cpu_quota": 2}, "cpu_quota"))

    assert exc_info.value.details["unsupported_fields"] == ["cpu_quota"]
    assert wire.describe_calls == []
    assert wire.get_calls == []
    assert operations.prepared == []
    assert operations.applied == []
    assert workflow_adapter.update_calls == []


def _task_raw(task: FakeTaskDefinition) -> JsonObject:
    return {
        "id": task.id,
        "code": task.code,
        "name": task.name,
        "version": task.version,
        "description": task.description,
        "projectCode": task.projectCode,
        "userId": 11,
        "taskType": task.taskType,
        "taskParams": task.taskParams,
        "taskParamList": [],
        "taskParamMap": {},
        "flag": enum_value(task.flag),
        "taskPriority": enum_value(task.taskPriority),
        "userName": "alice",
        "projectName": "etl-prod",
        "workerGroup": task.workerGroup,
        "environmentCode": task.environmentCode,
        "failRetryTimes": task.failRetryTimes,
        "failRetryInterval": task.failRetryInterval,
        "timeoutFlag": enum_value(task.timeoutFlag),
        "timeoutNotifyStrategy": enum_value(task.timeoutNotifyStrategy),
        "timeout": task.timeout,
        "delayTime": task.delayTime,
        "resourceIds": task.resourceIds,
        "createTime": task.createTime,
        "updateTime": task.updateTime,
        "modifyBy": task.modifyBy,
    }


def _dag_raw(dag: FakeDag, *, revision: int) -> JsonObject:
    workflow = dag.workflowDefinition
    assert workflow is not None
    tasks = list(dag.taskDefinitionList or ())
    relations = list(dag.workflowTaskRelationList or ())
    return {
        "processDefinition": {
            "id": workflow.id,
            "code": workflow.code,
            "name": workflow.name,
            "version": workflow.version,
            "releaseState": enum_value(workflow.releaseState),
            "projectCode": workflow.projectCode,
            "description": workflow.description,
            "globalParams": workflow.globalParams,
            "globalParamMap": workflow.globalParamMap,
            "locations": (
                '{"201":{"x":13,"y":21,"future":"extract"},'
                '"7001":{"x":377,"y":34,"future":"report"}}'
            ),
            "timeout": workflow.timeout,
            "tenantCode": "analytics",
            "futureWorkflowField": {"preserve": True, "revision": revision},
        },
        "taskDefinitionList": [
            {
                **_task_raw(task),
                "futureTaskField": {"preserve": cast("str", task.name)},
            }
            for task in tasks
        ],
        "processTaskRelationList": [
            {
                "id": index + 1,
                "name": "",
                "processDefinitionVersion": workflow.version,
                "projectCode": workflow.projectCode,
                "processDefinitionCode": workflow.code,
                "preTaskCode": relation.preTaskCode,
                "preTaskVersion": relation.preTaskVersion,
                "postTaskCode": relation.postTaskCode,
                "postTaskVersion": relation.postTaskVersion,
                "conditionType": 0,
                "conditionParams": relation.conditionParams,
                "futureRelationField": "preserve",
            }
            for index, relation in enumerate(relations)
        ],
    }


@pytest.mark.parametrize("ds_version", ["2.0.1", "2.0.2", "2.0.3"])
def test_intermediate_whole_workflow_update_preserves_the_complete_graph(
    ds_version: str,
) -> None:
    definitions, _wire, operations, adapter = _module(ds_version)
    prepared = definitions.prepare_update(
        _intent({"description": "changed"}, "description")
    )
    outcome = definitions.apply(prepared)
    assert outcome.mutation_applied is True
    assert outcome.value.view.to_data()["description"] == "changed"
    assert len(operations.applied) == 1
    assert len(adapter.dags[101].workflowTaskRelationList or ()) == 2


def test_203_rejects_dependency_edits_even_with_a_whole_workflow_route() -> None:
    definitions, _wire, operations, adapter = _module("2.0.3")
    with pytest.raises(UnsupportedFeatureError) as raised:
        definitions.prepare_update(_intent({"depends_on": []}, "depends_on"))
    assert raised.value.details["unsupported_fields"] == ["depends_on"]
    assert raised.value.details["mutation_applied"] is False
    assert "workflow edit" not in (raised.value.suggestion or "")
    assert operations.prepared == []
    assert adapter.update_calls == []
