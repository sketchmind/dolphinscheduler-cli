from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeHttpClient, FakeResourceAdapter, FakeResourceItem
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.services import workflow_instance as workflow_instance_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services._workflow.authoring import workflow_authoring_catalog_for_version
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.services.version_resolution import RuntimeSelection
from dsctl.services.workflow_instance import actions, edit, reads, watch
from dsctl.upstream.definition_models import (
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.runtime_instances import (
    LocatedWorkflowInstance,
    RuntimeInstanceDomain,
    RuntimeInstanceOperations,
    WorkflowInstanceSnapshot,
)
from dsctl.upstream.wire import WireRequest
from dsctl.upstream.workflows import (
    LegacyWorkflowDefinitionSnapshot,
    WorkflowOperations,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from dsctl.upstream.protocols.governance import TaskResourceResolver


_PROJECT = ProjectRef(
    native=NativeId(7),
    name="legacy-project",
    description="DS 1.3 project",
)


@dataclass(frozen=True)
class _PreparedLegacyUpdate:
    request: WireRequest


@dataclass
class _LegacyInstanceOperations:
    snapshots: list[WorkflowInstanceSnapshot]
    prepared: list[_PreparedLegacyUpdate] = field(default_factory=list)
    applied: list[_PreparedLegacyUpdate] = field(default_factory=list)
    read_count: int = 0
    task_code_requests: int = 0

    def get_workflow_instance(
        self,
        *,
        project_selector: str,
        workflow_instance_id: int,
    ) -> LocatedWorkflowInstance:
        assert project_selector == "legacy-project"
        snapshot = self.snapshots[min(self.read_count, len(self.snapshots) - 1)]
        self.read_count += 1
        assert snapshot.id == workflow_instance_id
        return LocatedWorkflowInstance(project=snapshot.project, instance=snapshot)

    def prepare_legacy_workflow_instance_update(
        self,
        located: LocatedWorkflowInstance,
        *,
        process_instance_json: str,
        locations: str,
        connects: str,
        sync_define: bool,
    ) -> _PreparedLegacyUpdate:
        prepared = _PreparedLegacyUpdate(
            request=WireRequest(
                method="POST",
                path=f"/projects/{located.project.name}/instance/update",
                query=None,
                form={
                    "processInstanceJson": process_instance_json,
                    "processInstanceId": located.instance.id,
                    "syncDefine": sync_define,
                    "locations": locations,
                    "connects": connects,
                },
                json=None,
                content=None,
            )
        )
        self.prepared.append(prepared)
        return prepared

    def apply_legacy_workflow_instance_update(
        self,
        prepared: _PreparedLegacyUpdate,
    ) -> None:
        assert self.prepared[-1] is prepared
        self.applied.append(prepared)

    def generate_task_codes(self, *_args: object, **_kwargs: object) -> list[int]:
        self.task_code_requests += 1
        message = "DS 1.3 instance edit must not request task codes"
        raise AssertionError(message)


def _legacy_graph_strings() -> tuple[str, str, str]:
    process_data = {
        "globalParams": [
            {
                "prop": "env",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "prod",
            }
        ],
        "tasks": [
            {
                "id": "tasks-11",
                "name": "extract",
                "type": "SHELL",
                "description": "",
                "params": {
                    "rawScript": "echo extract",
                    "resourceList": [],
                    "localParams": [],
                    "futureParam": {"keep": True},
                },
                "preTasks": [],
                "runFlag": "NORMAL",
                "maxRetryTimes": 0,
                "retryInterval": 1,
                "taskInstancePriority": "MEDIUM",
                "workerGroup": "default",
                "futureTaskField": {"keep": True},
            },
            {
                "id": "tasks-22",
                "name": "load",
                "type": "SHELL",
                "description": "",
                "params": {
                    "rawScript": "echo load",
                    "resourceList": [],
                    "localParams": [],
                },
                "preTasks": ["extract"],
                "runFlag": "NORMAL",
                "maxRetryTimes": 0,
                "retryInterval": 1,
                "taskInstancePriority": "MEDIUM",
                "workerGroup": "default",
            },
        ],
        "timeout": 30,
        "tenantId": 9,
        "futureProcessField": {"keep": True},
    }
    locations = {
        "tasks-11": {
            "name": "extract",
            "targetarr": "",
            "nodenumber": 1,
            "x": 17,
            "y": 23,
            "futureLocationField": "keep",
        },
        "tasks-22": {
            "name": "load",
            "targetarr": "tasks-11",
            "nodenumber": 0,
            "x": 317,
            "y": 23,
        },
    }
    connects = [
        {
            "endPointSourceId": "tasks-11",
            "endPointTargetId": "tasks-22",
        }
    ]
    return (
        json.dumps(process_data, ensure_ascii=False, separators=(",", ":")),
        json.dumps(locations, ensure_ascii=False, separators=(",", ":")),
        json.dumps(connects, ensure_ascii=False, separators=(",", ":")),
    )


def _legacy_snapshot(*, state: str = "SUCCESS") -> WorkflowInstanceSnapshot:
    process_instance_json, locations, connects = _legacy_graph_strings()
    return WorkflowInstanceSnapshot(
        ds_version="1.3.9",
        id=902,
        project=_PROJECT,
        workflow_native=NativeId(13),
        workflowDefinitionVersion=4,
        state=state,
        recovery=None,
        startTime="2026-08-12 01:00:00",
        endTime="2026-08-12 01:01:00",
        runTimes=1,
        name="daily-sync",
        host="master-1",
        commandType="START_PROCESS",
        taskDependType="TASK_POST",
        failureStrategy="CONTINUE",
        warningType="NONE",
        scheduleTime=None,
        executorId=11,
        executorName="alice",
        tenantCode=None,
        queue=None,
        duration="1m",
        workflowInstancePriority="MEDIUM",
        workerGroup="default",
        environmentCode=None,
        timeout=30,
        dryRun=0,
        restartTime=None,
        dagData=None,
        processInstanceJson=process_instance_json,
        locations=locations,
        connects=connects,
    )


def _legacy_mr_snapshot() -> WorkflowInstanceSnapshot:
    snapshot = _legacy_snapshot()
    assert snapshot.processInstanceJson is not None
    process = cast("dict[str, object]", json.loads(snapshot.processInstanceJson))
    task = cast("dict[str, object]", cast("list[object]", process["tasks"])[0])
    task["type"] = "MR"
    task["params"] = {
        "localParams": [],
        "mainJar": {"id": 731},
        "mainClass": "com.example.WordCount",
        "mainArgs": "hdfs:///input hdfs:///output",
        "others": "",
        "appName": "",
        "resourceList": [],
        "programType": "JAVA",
    }
    return replace(
        snapshot,
        processInstanceJson=json.dumps(process, separators=(",", ":")),
    )


def _install_legacy_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *snapshots: WorkflowInstanceSnapshot,
    legacy_workflows: object | None = None,
    resource_adapter: FakeResourceAdapter | None = None,
) -> _LegacyInstanceOperations:
    operations = _LegacyInstanceOperations(snapshots=list(snapshots))
    runtime = BoundDomainServiceRuntime(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="legacy-project"),
        http_client=FakeHttpClient(),
        domain=RuntimeInstanceDomain(
            instances=cast("RuntimeInstanceOperations", operations),
            legacy_workflows=cast(
                "WorkflowOperations | None",
                legacy_workflows,
            ),
            task_resource_resolver=cast(
                "TaskResourceResolver | None",
                resource_adapter,
            ),
        ),
    )

    def run(
        env_file: str | None,
        domain: object,
        operation: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        del env_file, domain
        return operation(runtime, *args, **kwargs)

    for service in (actions, reads, watch):
        monkeypatch.setattr(service, "run_with_bound_domain_service_runtime", run)
    monkeypatch.setattr(edit, "run_with_bound_domain_selection", run)
    monkeypatch.setattr(
        edit,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(make_profile(ds_version="1.3.9")),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: workflow_authoring_catalog_for_version("1.3.9"),
    )
    return operations


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, dict)
    return value


def test_legacy_export_roundtrips_through_full_file_edit_without_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = _install_legacy_runtime(monkeypatch, _legacy_snapshot())

    exported = workflow_instance_service.export_workflow_instance_yaml_result(902)
    workflow_file = tmp_path / "legacy-instance.yaml"
    workflow_file.write_text(str(_mapping(exported.data)["yaml"]), encoding="utf-8")
    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        file=workflow_file,
    )

    assert "project: legacy-project" in workflow_file.read_text(encoding="utf-8")
    assert "name: extract" in workflow_file.read_text(encoding="utf-8")
    assert result.warning_details == [
        {
            "code": "workflow_instance_edit_no_persistent_change",
            "message": (
                "workflow file produced no persistent workflow instance change; "
                "no edit request was sent"
            ),
            "no_change": True,
            "request_sent": False,
        }
    ]
    assert operations.prepared == []
    assert operations.applied == []
    assert operations.task_code_requests == 0


def test_legacy_mr_instance_export_and_edit_bind_the_visible_resource_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    resource_adapter = FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="wordcount.jar",
                full_name_value="/jobs/wordcount.jar",
                is_directory_value=False,
                id_value=731,
            )
        ]
    )
    operations = _install_legacy_runtime(
        monkeypatch,
        _legacy_mr_snapshot(),
        resource_adapter=resource_adapter,
    )

    exported = workflow_instance_service.export_workflow_instance_yaml_result(902)
    document = _mapping(yaml.safe_load(str(_mapping(exported.data)["yaml"])))
    tasks = cast("list[object]", document["tasks"])
    assert _mapping(tasks[0])["task_params"] == {
        "mainJar": "/jobs/wordcount.jar",
        "mainClass": "com.example.WordCount",
        "mainArgs": ["hdfs:///input", "hdfs:///output"],
    }

    patch_file = tmp_path / "legacy-mr-instance.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n",
        encoding="utf-8",
    )
    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    process = _mapping(json.loads(str(form["processInstanceJson"])))
    edited_tasks = cast("list[object]", process["tasks"])
    edited_mr_params = _mapping(_mapping(edited_tasks[0])["params"])
    assert edited_mr_params["mainJar"] == {"id": 731}
    assert len(operations.prepared) == 1
    assert operations.applied == []


def test_legacy_mr_instance_new_missing_resource_fails_before_prepare(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = _install_legacy_runtime(
        monkeypatch,
        _legacy_mr_snapshot(),
        resource_adapter=FakeResourceAdapter(resources=[]),
    )
    patch_file = tmp_path / "legacy-mr-missing.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: extract
        set:
          task_params:
            mainJar: /jobs/missing.jar
            mainClass: com.example.WordCount
            mainArgs: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(NotFoundError, match="not found or visible"):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            patch=patch_file,
        )

    assert operations.prepared == []
    assert operations.applied == []


def test_legacy_instance_export_keeps_dependent_identity_opaque(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _legacy_snapshot()
    assert snapshot.processInstanceJson is not None
    process = cast("dict[str, object]", json.loads(snapshot.processInstanceJson))
    task = cast("dict[str, object]", cast("list[object]", process["tasks"])[0])
    task["type"] = "DEPENDENT"
    task["params"] = {}
    task["dependence"] = {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "OR",
                "dependItemList": [
                    {
                        "projectId": 7,
                        "definitionId": 202,
                        "depTasks": "publish",
                        "cycle": "day",
                        "dateValue": "last1Days",
                    }
                ],
            }
        ],
    }
    opaque_snapshot = replace(
        snapshot,
        processInstanceJson=json.dumps(process, separators=(",", ":")),
    )
    _install_legacy_runtime(monkeypatch, opaque_snapshot)

    exported = workflow_instance_service.export_workflow_instance_yaml_result(902)

    yaml_text = str(_mapping(exported.data)["yaml"])
    document = _mapping(yaml.safe_load(yaml_text))
    tasks = cast("list[object]", document["tasks"])
    exported_task = _mapping(tasks[0])
    assert exported_task["type"] == "DEPENDENT"
    assert exported_task["task_params"] == {}
    assert "projectName" not in yaml_text
    assert "projectId" not in yaml_text


def test_legacy_patch_dry_run_uses_exact_prepared_update_and_preserves_native_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = _install_legacy_runtime(monkeypatch, _legacy_snapshot())
    patch_file = tmp_path / "legacy-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
  tasks:
    update:
      - match:
          name: extract
        set:
          command: echo extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    process_data = json.loads(str(form["processInstanceJson"]))
    locations = json.loads(str(form["locations"]))

    assert request == {
        "method": "POST",
        "path": "/projects/legacy-project/instance/update",
        "form": form,
    }
    assert form["processInstanceId"] == 902
    assert form["syncDefine"] is False
    assert set(form) == {
        "processInstanceJson",
        "processInstanceId",
        "syncDefine",
        "locations",
        "connects",
    }
    assert process_data["timeout"] == 45
    assert process_data["futureProcessField"] == {"keep": True}
    task_by_name = {task["name"]: task for task in process_data["tasks"]}
    assert task_by_name["extract"]["id"] == "tasks-11"
    assert task_by_name["extract"]["params"]["rawScript"] == "echo extract-v2"
    assert task_by_name["extract"]["params"]["futureParam"] == {"keep": True}
    assert task_by_name["extract"]["futureTaskField"] == {"keep": True}
    assert all(
        "code" not in task and "version" not in task for task in process_data["tasks"]
    )
    assert locations["tasks-11"]["futureLocationField"] == "keep"
    assert len(operations.prepared) == 1
    assert operations.applied == []
    assert operations.task_code_requests == 0


def test_legacy_instance_edit_translates_invalid_preserved_runtime_edge(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot = _legacy_snapshot()
    assert snapshot.processInstanceJson is not None
    process = cast("dict[str, object]", json.loads(snapshot.processInstanceJson))
    task = cast("dict[str, object]", cast("list[object]", process["tasks"])[0])
    params = cast("dict[str, object]", task["params"])
    params["processDefinitionId"] = "1_0"
    invalid_snapshot = replace(
        snapshot,
        processInstanceJson=json.dumps(process, separators=(",", ":")),
    )
    operations = _install_legacy_runtime(monkeypatch, invalid_snapshot)
    patch_file = tmp_path / "legacy-invalid-edge.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n",
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="descendants could not be safely validated",
    ):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            patch=patch_file,
            dry_run=True,
        )

    assert operations.prepared == []
    assert operations.applied == []


def test_legacy_patch_apply_executes_the_same_prepared_object(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = _install_legacy_runtime(
        monkeypatch,
        _legacy_snapshot(),
        _legacy_snapshot(),
    )
    patch_file = tmp_path / "legacy-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        sync_definition=True,
    )
    form = operations.prepared[0].request.form
    assert form is not None
    process_data = json.loads(str(form["processInstanceJson"]))

    assert operations.applied == [operations.prepared[0]]
    assert operations.applied[0] is operations.prepared[0]
    assert form["syncDefine"] is True
    assert [(task["name"], task["id"]) for task in process_data["tasks"]][:2] == [
        ("extract", "tasks-11"),
        ("load", "tasks-22"),
    ]
    assert process_data["tasks"][2]["name"] == "verify"
    assert isinstance(process_data["tasks"][2]["id"], str)
    assert "code" not in process_data["tasks"][2]
    assert operations.task_code_requests == 0
    assert _mapping(result.resolved["workflow"]) == {
        "id": 13,
        "name": "daily-sync",
        "version": 4,
    }


def test_legacy_instance_edit_resolves_sub_workflow_name_before_compile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "legacy-instance-sub-workflow.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    create:
      - name: invoke-child
        type: SUB_WORKFLOW
        task_params:
          childWorkflowName: child-daily
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    child_scope = WorkflowScope(
        project=_PROJECT,
        workflow=WorkflowRef(
            native=NativeId(202),
            name="child-daily",
            version=3,
        ),
        view=cast("WorkflowView", object()),
    )

    class ReferenceOperations:
        def resolve_workflow_by_name(
            self,
            project: ProjectRef,
            workflow_name: str,
        ) -> WorkflowScope:
            assert project == _PROJECT
            assert workflow_name == "child-daily"
            return child_scope

        def resolve_workflow_by_id(
            self,
            project: ProjectRef,
            workflow_id: int,
        ) -> WorkflowScope:
            assert project == _PROJECT
            assert workflow_id == 202
            return child_scope

        def legacy_definition(
            self,
            scope: WorkflowScope,
            *,
            action: str,
        ) -> LegacyWorkflowDefinitionSnapshot:
            assert scope is child_scope
            assert action == "workflow-instance.edit"
            process_definition_json, locations, connects = _legacy_graph_strings()
            return LegacyWorkflowDefinitionSnapshot(
                scope=scope,
                name="child-daily",
                description=None,
                release_state="ONLINE",
                process_definition_json=process_definition_json,
                locations=locations,
                connects=connects,
            )

    reference_operations = ReferenceOperations()
    operations = _install_legacy_runtime(
        monkeypatch,
        _legacy_snapshot(),
        legacy_workflows=reference_operations,
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    form = _mapping(request["form"])
    process_data = _mapping(json.loads(str(form["processInstanceJson"])))
    tasks = [_mapping(task) for task in cast("list[object]", process_data["tasks"])]
    created = next(task for task in tasks if task["name"] == "invoke-child")
    assert created["type"] == "SUB_PROCESS"
    assert created["params"] == {"processDefinitionId": 202}
    assert operations.applied == []
    assert operations.task_code_requests == 0
    assert _mapping(result.resolved["workflow"]) == {
        "id": 13,
        "name": "daily-sync",
        "version": 4,
    }


def test_legacy_instance_edit_resolves_dependent_names_before_compile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "legacy-instance-dependent.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    create:
      - name: wait-child
        type: DEPENDENT
        task_params:
          dependence:
            relation: AND
            dependTaskList:
              - relation: OR
                dependItemList:
                  - dependentType: DEPENDENT_ON_TASK
                    projectName: legacy-project
                    workflowName: child-daily
                    taskName: publish
                    cycle: day
                    dateValue: last1Days
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    child_scope = WorkflowScope(
        project=_PROJECT,
        workflow=WorkflowRef(
            native=NativeId(202),
            name="child-daily",
            version=3,
        ),
        view=cast("WorkflowView", object()),
    )

    class ReferenceOperations:
        def resolve_project_by_name(self, project_name: str) -> ProjectRef:
            assert project_name == "legacy-project"
            return _PROJECT

        def resolve_workflow_by_name(
            self,
            project: ProjectRef,
            workflow_name: str,
        ) -> WorkflowScope:
            assert project == _PROJECT
            assert workflow_name == "child-daily"
            return child_scope

        def legacy_definition(
            self,
            scope: WorkflowScope,
            *,
            action: str,
        ) -> LegacyWorkflowDefinitionSnapshot:
            assert scope is child_scope
            assert action == "workflow-instance.edit"
            return LegacyWorkflowDefinitionSnapshot(
                scope=scope,
                name="child-daily",
                description=None,
                release_state="ONLINE",
                process_definition_json=json.dumps({"tasks": [{"name": "publish"}]}),
                locations="{}",
                connects="[]",
            )

    operations = _install_legacy_runtime(
        monkeypatch,
        _legacy_snapshot(),
        legacy_workflows=ReferenceOperations(),
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    form = _mapping(request["form"])
    process_data = _mapping(json.loads(str(form["processInstanceJson"])))
    tasks = [_mapping(task) for task in cast("list[object]", process_data["tasks"])]
    created = next(task for task in tasks if task["name"] == "wait-child")
    assert created["params"] == {}
    assert created["dependence"] == {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "OR",
                "dependItemList": [
                    {
                        "projectId": 7,
                        "definitionId": 202,
                        "depTasks": "publish",
                        "cycle": "day",
                        "dateValue": "last1Days",
                    }
                ],
            }
        ],
    }
    assert operations.applied == []


def test_legacy_instance_edit_translates_child_workflow_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "legacy-instance-forbidden-child.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    create:
      - name: invoke-child
        type: SUB_WORKFLOW
        task_params:
          childWorkflowName: forbidden-child
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    class DeniedReferenceOperations:
        def resolve_workflow_by_name(
            self,
            project: ProjectRef,
            workflow_name: str,
        ) -> WorkflowScope:
            assert project == _PROJECT
            assert workflow_name == "forbidden-child"
            raise ApiResultError(
                result_code=30001,
                result_message="no workflow permission",
            )

    operations = _install_legacy_runtime(
        monkeypatch,
        _legacy_snapshot(),
        legacy_workflows=DeniedReferenceOperations(),
    )

    with pytest.raises(PermissionDeniedError, match="forbidden-child"):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            patch=patch_file,
            dry_run=True,
        )

    assert operations.prepared == []
    assert operations.applied == []


@pytest.mark.parametrize(
    ("snapshot", "error_type"),
    [
        (_legacy_snapshot(state="RUNNING_EXECUTION"), InvalidStateError),
        (replace(_legacy_snapshot(), locations="{}"), ApiTransportError),
    ],
)
def test_legacy_edit_validation_errors_happen_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    snapshot: WorkflowInstanceSnapshot,
    error_type: type[Exception],
) -> None:
    operations = _install_legacy_runtime(monkeypatch, snapshot)
    patch_file = tmp_path / "legacy-instance.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n",
        encoding="utf-8",
    )

    with pytest.raises(error_type):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            patch=patch_file,
        )

    assert operations.prepared == []
    assert operations.applied == []
    assert operations.task_code_requests == 0
