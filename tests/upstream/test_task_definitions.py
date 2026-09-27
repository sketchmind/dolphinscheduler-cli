from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    NotFoundError,
    UnsupportedFeatureError,
)
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.output import require_json_object
from dsctl.upstream.definition_reads import DefinitionReads
from dsctl.upstream.definition_wire import CodeDefinitionWire
from dsctl.upstream.task_definition_wire import (
    TaskTopLevelFieldPolicy,
    TaskUpdateWirePolicy,
    bind_task_definition_wire,
)
from dsctl.upstream.task_definitions import (
    NativeTaskSnapshot,
    TaskDefinitions,
    TaskSelector,
    TaskUpdateIntent,
    WorkflowSelector,
)
from dsctl.upstream.task_update import TaskUpdateCompilation, compile_task_update
from dsctl.upstream.wire import (
    PreparedWireCallToken,
    WireExecution,
    WireRequest,
    WireResponseDecodeError,
)
from tests.fake_wire import FakePreparedWireCall
from tests.fakes import (
    FakeDag,
    FakeProject,
    FakeProjectAdapter,
    FakeScheduleAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.protocol import (
        TaskPayloadRecord,
        WorkflowDagRecord,
    )


_FAKE_TASK_FIELD_POLICY = TaskTopLevelFieldPolicy(
    request_payload=frozenset({"name", "description", "taskType", "taskParams"}),
    server_managed=frozenset({"id", "code", "version", "projectCode", "createTime"}),
    response_derived=frozenset(),
    relation_projection=frozenset(),
    opaque_preservation=frozenset({"taskParams"}),
)
_CURRENT_UPDATE_POLICY = TaskUpdateWirePolicy(
    update_available=True,
    dependency_update=True,
    unsupported_patch_fields=frozenset(),
)


@dataclass
class _FakeTaskWire:
    snapshots: list[NativeTaskSnapshot]
    ds_version: str = "3.4.1"
    recipe_fingerprint: str = "sha256:task-recipe"
    top_level_field_policy: TaskTopLevelFieldPolicy = _FAKE_TASK_FIELD_POLICY
    update_policy_value: TaskUpdateWirePolicy = _CURRENT_UPDATE_POLICY
    returned_code: int | None = 7001
    fail_get_calls: set[int] = field(default_factory=set)
    get_calls: list[tuple[int, int]] = field(default_factory=list)
    prepare_calls: list[JsonObject] = field(default_factory=list)
    apply_calls: list[PreparedWireCallToken] = field(default_factory=list)
    apply_error: BaseException | None = None
    get_error: BaseException | None = None
    current_snapshot_index: int = 0
    dag: FakeDag | None = None
    sync_dag_on_snapshot_change: bool = True

    @property
    def update_policy(self) -> TaskUpdateWirePolicy:
        return self.update_policy_value

    def describe(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WireExecution[WorkflowDagRecord]:
        if self.dag is None:
            message = "fake DAG is not configured"
            raise AssertionError(message)
        return WireExecution(
            payload=self.dag,
            raw_payload={},
            request=WireRequest(
                method="GET",
                path=(f"/projects/{project_code}/workflow-definition/{workflow_code}"),
                query=None,
                form=None,
                json=None,
                content=None,
            ),
        )

    def get(
        self,
        *,
        project_code: int,
        task_code: int,
    ) -> WireExecution[TaskPayloadRecord]:
        self.get_calls.append((project_code, task_code))
        call_number = len(self.get_calls)
        if call_number > 2 and self.get_error is not None:
            raise self.get_error
        if call_number in self.fail_get_calls:
            message = "readback unavailable"
            raise ApiTransportError(message)
        snapshot = self.snapshots[self.current_snapshot_index]
        return WireExecution(
            payload=snapshot.record,
            raw_payload=snapshot.raw,
            request=WireRequest(
                method="GET",
                path=f"/projects/{project_code}/task-definition/{task_code}",
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
    ) -> FakePreparedWireCall[JsonObject]:
        call: JsonObject = {
            "project_code": project_code,
            "task_code": task_code,
            "task_definition_json": task_definition_json,
            "upstream_codes": list(upstream_codes),
        }
        self.prepare_calls.append(call)
        form: dict[str, str] = {
            "taskDefinitionJsonObj": task_definition_json,
        }
        if upstream_codes and self.update_policy.dependency_update:
            form["upstreamCodes"] = ",".join(str(code) for code in upstream_codes)
        return FakePreparedWireCall(
            ds_version=self.ds_version,
            program_fingerprint="sha256:update-wire",
            args=call,
            request=WireRequest(
                method="PUT",
                path=(
                    f"/projects/{project_code}/task-definition/{task_code}"
                    + ("/with-upstream" if self.update_policy.dependency_update else "")
                ),
                query=None,
                form=form,
                json=None,
                content=None,
            ),
        )

    def apply_update(
        self,
        prepared: PreparedWireCallToken,
    ) -> WireExecution[int | None]:
        self.apply_calls.append(prepared)
        if self.apply_error is not None:
            raise self.apply_error
        if self.current_snapshot_index + 1 < len(self.snapshots):
            self.select_snapshot(self.current_snapshot_index + 1)
        return WireExecution(
            payload=self.returned_code,
            raw_payload=self.returned_code,
            request=prepared.request,
        )

    def select_snapshot(self, index: int) -> None:
        self.current_snapshot_index = index
        if self.dag is None or not self.sync_dag_on_snapshot_change:
            return
        replacement = cast("FakeTaskDefinition", self.snapshots[index].record)
        tasks = self.dag.task_definition_list_value
        if tasks is not None:
            tasks[:] = [
                replacement if task.code == replacement.code else task for task in tasks
            ]
        relations = self.dag.workflow_task_relation_list_value
        if relations is not None:
            relations[:] = [
                replace(
                    relation,
                    pre_task_version_value=(
                        replacement.version or 0
                        if relation.preTaskCode == replacement.code
                        else relation.preTaskVersion
                    ),
                    post_task_version_value=(
                        replacement.version or 0
                        if relation.postTaskCode == replacement.code
                        else relation.postTaskVersion
                    ),
                )
                for relation in relations
            ]


@dataclass
class _FakeCompiler:
    no_change: bool = False
    calls: int = 0

    def __call__(
        self,
        *,
        current_task: TaskPayloadRecord,
        dag: WorkflowDagRecord,
        update_spec: WorkflowPatchTaskSetSpec,
        requested_fields: Sequence[str],
        task_code: int,
    ) -> TaskUpdateCompilation:
        del dag, update_spec, requested_fields, task_code
        self.calls += 1
        task = cast("FakeTaskDefinition", current_task)
        expected_description = (
            task.description if self.no_change else "updated description"
        )
        updated_fields = (
            () if task.description == expected_description else ("description",)
        )
        return TaskUpdateCompilation(
            payload={
                "name": task.name,
                "description": expected_description,
                "taskType": task.taskType,
                "taskParams": json.dumps(
                    task.taskParams,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            },
            current_upstream_codes=(),
            updated_upstream_codes=(),
            current_projection={"description": task.description},
            expected_projection={"description": expected_description},
            updated_fields=updated_fields,
            no_change=not updated_fields,
        )


def _module(
    *,
    wire: _FakeTaskWire | None = None,
    compiler: _FakeCompiler | None = None,
    profile_version: str = "3.4.1",
) -> tuple[TaskDefinitions, _FakeTaskWire, _FakeCompiler]:
    project = FakeProject(code=7, name="etl-prod")
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
    )
    task = _task(description="current description")
    dag = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    effective_wire = wire or _FakeTaskWire([_snapshot(task)])
    effective_wire.dag = dag
    effective_compiler = compiler or _FakeCompiler()
    return (
        TaskDefinitions(
            profile_version=profile_version,
            definitions=_definitions(
                FakeProjectAdapter([project]),
                FakeWorkflowAdapter([workflow], {101: dag}),
            ),
            wire=effective_wire,
            compile_update=effective_compiler,
        ),
        effective_wire,
        effective_compiler,
    )


def _dependency_module(
    *,
    refreshed: FakeTaskDefinition | None = None,
    profile_version: str = "3.4.1",
    update_policy: TaskUpdateWirePolicy = _CURRENT_UPDATE_POLICY,
    top_level_field_policy: TaskTopLevelFieldPolicy = _FAKE_TASK_FIELD_POLICY,
    dependence: str | None = None,
) -> tuple[TaskDefinitions, _FakeTaskWire]:
    project = FakeProject(code=7, name="etl-prod")
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
    )
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
    )
    transform = FakeTaskDefinition(
        code=203,
        name="transform",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo transform"}',
    )
    current = _task(description="current description")
    dag = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[extract, transform, current],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=extract.code,
                post_task_code_value=current.code,
                pre_task_version_value=extract.version or 0,
                post_task_version_value=current.version or 0,
            )
        ],
    )
    snapshots = [_snapshot(current, dependence=dependence)]
    if refreshed is not None:
        snapshots.append(_snapshot(refreshed))
    wire = _FakeTaskWire(
        snapshots,
        ds_version=profile_version,
        top_level_field_policy=top_level_field_policy,
        update_policy_value=update_policy,
    )
    wire.dag = dag
    workflow_adapter = FakeWorkflowAdapter([workflow], {workflow.code: dag})
    return (
        TaskDefinitions(
            profile_version=profile_version,
            definitions=_definitions(
                FakeProjectAdapter([project]),
                workflow_adapter,
            ),
            wire=wire,
            compile_update=compile_task_update,
        ),
        wire,
    )


def _definitions(
    projects: FakeProjectAdapter,
    workflows: FakeWorkflowAdapter,
) -> DefinitionReads:
    return DefinitionReads(
        CodeDefinitionWire(
            projects=projects,
            workflows=workflows,
            schedules=FakeScheduleAdapter([]),
        )
    )


def _task(*, description: str, version: int = 1) -> FakeTaskDefinition:
    return FakeTaskDefinition(
        code=7001,
        name="report",
        version=version,
        project_code_value=7,
        description=description,
        task_type_value="SQL",
        task_params_value={
            "type": "MYSQL",
            "datasource": 1,
            "sql": "select 1",
            "sqlType": 0,
            "sqlSource": "FILE",
            "sqlResource": "/queries/report.sql",
            "futureNested": {"enabled": True},
        },
    )


def _snapshot(
    task: FakeTaskDefinition,
    *,
    dependence: str | None = None,
    top_level_extension: bool = False,
    is_cache: str | None = None,
) -> NativeTaskSnapshot:
    raw: dict[str, object] = {
        "id": 17,
        "code": task.code,
        "name": task.name,
        "version": task.version,
        "projectCode": task.projectCode,
        "description": task.description,
        "taskType": task.taskType,
        "taskParams": task.taskParams,
        "createTime": "2026-08-04 08:00:00",
    }
    if dependence is not None:
        raw["dependence"] = dependence
    if top_level_extension:
        raw["futureTopLevel"] = {"mode": "safe"}
    if is_cache is not None:
        raw["isCache"] = is_cache
    return NativeTaskSnapshot(
        record=task,
        raw=require_json_object(raw, label="fake task snapshot"),
    )


def _intent(task: str = "report") -> TaskUpdateIntent:
    return TaskUpdateIntent(
        selector=TaskSelector(
            project="etl-prod",
            workflow="daily-sync",
            task=task,
        ),
        patch=WorkflowPatchTaskSetSpec.model_validate(
            {"description": "updated description"}
        ),
        requested_fields=("description",),
    )


def _depends_intent(dependencies: list[str]) -> TaskUpdateIntent:
    return TaskUpdateIntent(
        selector=TaskSelector(
            project="etl-prod",
            workflow="daily-sync",
            task="report",
        ),
        patch=WorkflowPatchTaskSetSpec.model_validate({"depends_on": dependencies}),
        requested_fields=("depends_on",),
    )


def test_get_resolves_membership_then_uses_project_scoped_detail() -> None:
    definitions, wire, _compiler = _module()

    result = definitions.get(_intent().selector)

    assert result.scope.project.code == 7
    assert result.scope.workflow.code == 101
    assert result.scope.task.code == 7001
    assert result.view.to_data()["taskType"] == "SQL"
    assert wire.get_calls == [(7, 7001)]


def test_list_resolves_one_workflow_without_task_detail_requests() -> None:
    definitions, wire, _compiler = _module()

    result = definitions.list(
        WorkflowSelector(project="etl-prod", workflow="daily-sync")
    )

    assert result.scope.project.code == 7
    assert result.scope.workflow.code == 101
    assert [task.to_data() for task in result.tasks] == [
        {"code": 7001, "name": "report", "version": 1}
    ]
    assert wire.get_calls == []


def test_prepare_is_read_only_and_preserves_opaque_task_params() -> None:
    definitions, wire, compiler = _module()

    prepared = definitions.prepare_update(_intent())

    assert compiler.calls == 1
    assert wire.apply_calls == []
    assert prepared.request.method == "PUT"
    assert prepared.request.path == ("/projects/7/task-definition/7001/with-upstream")
    assert prepared.request.form is not None
    payload = json.loads(str(prepared.request.form["taskDefinitionJsonObj"]))
    assert payload["taskParams"]
    task_params = json.loads(payload["taskParams"])
    assert task_params["sqlResource"] == "/queries/report.sql"
    assert task_params["futureNested"] == {"enabled": True}
    assert "id" not in payload
    assert "upstreamCodes" not in prepared.request.form


@pytest.mark.parametrize(
    ("ds_version", "expected_path"),
    [
        ("2.0.9", "/projects/7/task-definition/7001"),
        ("3.2.2", "/projects/7/task-definition/7001/with-upstream"),
    ],
)
def test_prepare_accepts_computed_dependence_without_echoing_it(
    ds_version: str,
    expected_path: str,
) -> None:
    client = DolphinSchedulerClient(make_profile(ds_version=ds_version))
    with client:
        exact_wire = bind_task_definition_wire(client)
    field_policy = exact_wire.top_level_field_policy
    assert "dependence" in field_policy.response_derived
    assert "dependence" not in field_policy.request_payload
    assert "dependence" not in field_policy.opaque_preservation
    assert "futureTopLevel" not in field_policy.classified_fields
    definitions, wire = _dependency_module(
        profile_version=ds_version,
        top_level_field_policy=field_policy,
        update_policy=exact_wire.update_policy,
        dependence='{"relation":"AND"}',
    )

    prepared = definitions.prepare_update(_intent())

    assert prepared.request.form is not None
    assert prepared.request.path == expected_path
    payload = json.loads(str(prepared.request.form["taskDefinitionJsonObj"]))
    assert "dependence" not in payload
    assert wire.apply_calls == []


def test_prepare_rejects_unreviewed_top_level_fields_before_mutation() -> None:
    task = _task(description="current description")
    client = DolphinSchedulerClient(make_profile(ds_version="2.0.9"))
    with client:
        exact_wire = bind_task_definition_wire(client)
    wire = _FakeTaskWire(
        [_snapshot(task, top_level_extension=True)],
        ds_version="2.0.9",
        top_level_field_policy=exact_wire.top_level_field_policy,
        update_policy_value=exact_wire.update_policy,
    )
    definitions, wire, compiler = _module(
        wire=wire,
        profile_version="2.0.9",
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="unreviewed top-level fields",
    ) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.details["unexpected_fields"] == ["futureTopLevel"]
    assert exc_info.value.details["mutation_applied"] is False
    assert compiler.calls == 1
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_ds331_prepare_rejects_dependence_removed_from_the_response_epoch() -> None:
    task = _task(description="current description")
    client = DolphinSchedulerClient(make_profile(ds_version="3.3.1"))
    with client:
        exact_wire = bind_task_definition_wire(client)
    wire = _FakeTaskWire(
        [_snapshot(task, dependence='{"relation":"AND"}')],
        ds_version="3.3.1",
        top_level_field_policy=exact_wire.top_level_field_policy,
        update_policy_value=exact_wire.update_policy,
    )
    definitions, wire, compiler = _module(
        wire=wire,
        profile_version="3.3.1",
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="unreviewed top-level fields",
    ) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.details["unexpected_fields"] == ["dependence"]
    assert exc_info.value.details["mutation_applied"] is False
    assert compiler.calls == 1
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_32x_prepare_preserves_the_opaque_is_cache_field() -> None:
    task = _task(description="current description")
    cache_policy = replace(
        _FAKE_TASK_FIELD_POLICY,
        request_payload=(
            _FAKE_TASK_FIELD_POLICY.request_payload | frozenset({"isCache"})
        ),
        opaque_preservation=(
            _FAKE_TASK_FIELD_POLICY.opaque_preservation | frozenset({"isCache"})
        ),
    )
    wire = _FakeTaskWire(
        [_snapshot(task, is_cache="YES")],
        ds_version="3.2.2",
        top_level_field_policy=cache_policy,
    )
    definitions, wire, _compiler = _module(
        wire=wire,
        profile_version="3.2.2",
    )

    prepared = definitions.prepare_update(_intent())

    assert prepared.request.form is not None
    payload = json.loads(str(prepared.request.form["taskDefinitionJsonObj"]))
    assert payload["isCache"] == "YES"
    assert wire.apply_calls == []


def test_prepare_rejects_inconsistent_detail_and_dag_versions() -> None:
    detail = _task(description="current description", version=2)
    wire = _FakeTaskWire([_snapshot(detail)])
    definitions, wire, compiler = _module(wire=wire)

    with pytest.raises(
        ApiTransportError,
        match="versions are inconsistent",
    ) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.details["mutation_applied"] is False
    assert exc_info.value.details["issues"] == [
        {
            "kind": "task_version_mismatch",
            "detail_version": 2,
            "dag_version": 1,
        }
    ]
    assert compiler.calls == 0
    assert wire.prepare_calls == []


def test_prepare_rejects_clearing_the_final_dependency_before_transport() -> None:
    definitions, wire = _dependency_module()

    with pytest.raises(
        UnsupportedFeatureError,
        match="cannot remove the final upstream dependency",
    ) as exc_info:
        definitions.prepare_update(_depends_intent([]))

    assert exc_info.value.details["reason"] == (
        "main_api_cannot_clear_all_upstream_relations"
    )
    assert exc_info.value.details["mutation_applied"] is False
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_ds200_rejects_every_update_before_any_wire_request() -> None:
    policy = TaskUpdateWirePolicy(
        update_available=False,
        dependency_update=False,
        unsupported_patch_fields=frozenset(
            {"task_group_id", "task_group_priority", "cpu_quota", "memory_max"}
        ),
    )
    definitions, wire = _dependency_module(
        profile_version="2.0.0",
        update_policy=policy,
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="cannot coherently update",
    ) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.details["reason"] == (
        "standalone_task_update_does_not_advance_workflow_relation_versions"
    )
    assert exc_info.value.details["upstream_capability_limited"] is True
    assert exc_info.value.details["mutation_applied"] is False
    assert wire.get_calls == []
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_ds209_rejects_explicit_dependency_edit_before_any_wire_request() -> None:
    policy = TaskUpdateWirePolicy(
        update_available=True,
        dependency_update=False,
        unsupported_patch_fields=frozenset(
            {"task_group_id", "task_group_priority", "cpu_quota", "memory_max"}
        ),
    )
    definitions, wire = _dependency_module(
        profile_version="2.0.9",
        update_policy=policy,
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="cannot express the requested task update fields",
    ) as exc_info:
        definitions.prepare_update(_depends_intent(["transform"]))

    assert exc_info.value.details["unsupported_fields"] == ["depends_on"]
    assert exc_info.value.details["dependency_update"] is False
    assert wire.get_calls == []
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_ds209_ordinary_edit_advances_the_relation_bound_task_version() -> None:
    policy = TaskUpdateWirePolicy(
        update_available=True,
        dependency_update=False,
        unsupported_patch_fields=frozenset(
            {"task_group_id", "task_group_priority", "cpu_quota", "memory_max"}
        ),
    )
    refreshed = _task(description="updated description", version=2)
    definitions, wire = _dependency_module(
        refreshed=refreshed,
        profile_version="2.0.9",
        update_policy=policy,
    )

    prepared = definitions.prepare_update(_intent())
    outcome = definitions.apply(prepared)

    assert prepared.request.form is not None
    assert prepared.request.form == {
        "taskDefinitionJsonObj": prepared.request.form["taskDefinitionJsonObj"]
    }
    assert prepared.request.path == "/projects/7/task-definition/7001"
    assert outcome.mutation_applied is True
    assert outcome.value.view.to_data()["version"] == 2
    assert wire.dag is not None
    assert wire.dag.workflowTaskRelationList is not None
    relation = wire.dag.workflowTaskRelationList[0]
    assert relation.postTaskVersion == 2
    assert len(wire.apply_calls) == 1


def test_unreviewed_top_level_fields_do_not_block_a_no_op() -> None:
    task = _task(description="current description")
    wire = _FakeTaskWire([_snapshot(task, top_level_extension=True)])
    compiler = _FakeCompiler(no_change=True)
    definitions, wire, _compiler = _module(wire=wire, compiler=compiler)

    prepared = definitions.prepare_update(_intent())
    outcome = definitions.apply(prepared)

    assert outcome.mutation_applied is False
    assert wire.apply_calls == []


def test_apply_mutates_once_and_owns_project_scoped_readback() -> None:
    current = _task(description="current description")
    refreshed = _task(description="updated description", version=2)
    wire = _FakeTaskWire([_snapshot(current), _snapshot(refreshed)])
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    outcome = definitions.apply(prepared)

    assert outcome.mutation_applied is True
    assert outcome.value.view.to_data()["description"] == "updated description"
    assert len(wire.apply_calls) == 1
    assert wire.get_calls == [(7, 7001), (7, 7001), (7, 7001)]


def test_apply_rejects_success_when_requested_fields_were_not_saved() -> None:
    current = _task(description="current description")
    wire = _FakeTaskWire([_snapshot(current)], returned_code=None)
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    with pytest.raises(ApiTransportError, match="did not persist") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_applied"] is True
    assert exc_info.value.details["mismatched_fields"] == ["description"]
    assert len(wire.apply_calls) == 1


def test_apply_rejects_success_when_task_version_did_not_advance() -> None:
    current = _task(description="current description")
    saved_without_version = _task(description="updated description")
    wire = _FakeTaskWire(
        [_snapshot(current), _snapshot(saved_without_version)],
        returned_code=None,
    )
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    with pytest.raises(ApiTransportError, match="did not persist") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mismatched_fields"] == []
    assert exc_info.value.details["task_version_advanced"] is False
    assert len(wire.apply_calls) == 1


def test_apply_rejects_an_incoherent_dag_readback_after_mutation() -> None:
    current = _task(description="current description")
    refreshed = _task(description="updated description", version=2)
    wire = _FakeTaskWire([_snapshot(current), _snapshot(refreshed)])
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())
    wire.sync_dag_on_snapshot_change = False

    with pytest.raises(
        ApiTransportError,
        match="versions are inconsistent",
    ) as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_applied"] is True
    assert exc_info.value.details["phase"] == "readback"
    assert len(wire.apply_calls) == 1


def test_apply_rejects_success_when_dependency_readback_did_not_change() -> None:
    refreshed = _task(description="current description", version=2)
    definitions, wire = _dependency_module(refreshed=refreshed)
    prepared = definitions.prepare_update(_depends_intent(["transform"]))

    with pytest.raises(ApiTransportError, match="did not persist") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mismatched_fields"] == ["depends_on"]
    assert exc_info.value.details["expected_upstream_codes"] == [203]
    assert exc_info.value.details["actual_upstream_codes"] == [201]
    assert len(wire.apply_calls) == 1


def test_apply_rejects_a_stale_prepared_snapshot_before_mutation() -> None:
    current = _task(description="current description")
    concurrent = _task(description="concurrent description", version=2)
    wire = _FakeTaskWire([_snapshot(current), _snapshot(concurrent)])
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())
    wire.select_snapshot(1)

    with pytest.raises(ConflictError, match="stale") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_applied"] is False
    assert exc_info.value.details["prepared_task_version"] == 1
    assert exc_info.value.details["current_task_version"] == 2
    assert wire.apply_calls == []


def test_apply_marks_transport_failure_as_maybe_applied() -> None:
    wire = _FakeTaskWire(
        [_snapshot(_task(description="current description"))],
        apply_error=ApiTransportError(
            "connection reset",
            details={"transport": "reset"},
            source={"kind": "remote", "layer": "transport"},
        ),
    )
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    with pytest.raises(ApiTransportError, match="may have been applied") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert "mutation_applied" not in exc_info.value.details
    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.suggestion is not None
    assert "dsctl task get 7001 --project 7 --workflow 101" in (
        exc_info.value.suggestion
    )
    assert len(wire.apply_calls) == 1


def test_no_change_plan_sends_no_mutation_or_readback() -> None:
    compiler = _FakeCompiler(no_change=True)
    definitions, wire, _compiler = _module(compiler=compiler)
    prepared = definitions.prepare_update(_intent())

    outcome = definitions.apply(prepared)

    assert outcome.mutation_applied is False
    assert wire.apply_calls == []
    assert wire.get_calls == [(7, 7001)]


def test_prepared_plan_cannot_cross_an_exact_profile_recipe() -> None:
    definitions, wire, _compiler = _module()
    prepared = definitions.prepare_update(_intent())
    foreign = replace(prepared, profile_version="3.4.2")

    with pytest.raises(ApiTransportError, match="does not belong") as exc_info:
        definitions.apply(foreign)

    assert exc_info.value.details["mutation_applied"] is False
    assert wire.apply_calls == []


def test_readback_failure_preserves_that_the_mutation_was_applied() -> None:
    wire = _FakeTaskWire([_snapshot(_task(description="current"))])
    wire.fail_get_calls.add(3)
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    with pytest.raises(ApiTransportError, match="update succeeded") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_applied"] is True
    assert len(wire.apply_calls) == 1


def test_readback_does_not_mask_programmer_errors() -> None:
    wire = _FakeTaskWire(
        [_snapshot(_task(description="current"))],
        get_error=AssertionError("broken readback invariant"),
    )
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    with pytest.raises(AssertionError, match="broken readback invariant"):
        definitions.apply(prepared)

    assert len(wire.apply_calls) == 1


def test_update_response_decode_failure_preserves_that_mutation_was_applied() -> None:
    wire = _FakeTaskWire(
        [_snapshot(_task(description="current"))],
        apply_error=WireResponseDecodeError(
            "DolphinScheduler response payload did not match the generated "
            "API contract.",
            details={"wire_response_received": True},
        ),
    )
    definitions, wire, _compiler = _module(wire=wire)
    prepared = definitions.prepare_update(_intent())

    with pytest.raises(ApiTransportError, match="accepted the task update") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["mutation_applied"] is True
    assert exc_info.value.details["wire_response_received"] is True
    assert len(wire.apply_calls) == 1
    assert wire.get_calls == [(7, 7001), (7, 7001)]


def test_foreign_task_code_is_rejected_before_task_detail_request() -> None:
    definitions, wire, _compiler = _module()

    with pytest.raises(NotFoundError, match="Task code 999 was not found"):
        definitions.get(_intent("999").selector)

    assert wire.get_calls == []


def test_task_definition_module_rejects_a_mismatched_wire_version() -> None:
    wire = _FakeTaskWire([_snapshot(_task(description="current"))], ds_version="3.4.2")

    with pytest.raises(ValueError, match="does not match the selected profile"):
        _module(wire=wire)
