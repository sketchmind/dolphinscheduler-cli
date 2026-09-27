from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import ConflictError, InvalidStateError, NotFoundError
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.services._legacy_workflow_mutation import (
    prepare_legacy_workflow_mutation_plan,
)
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream.definition_models import (
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.legacy_task_definitions import (
    LegacyTaskDefinitions,
    LegacyTaskSelector,
    LegacyWorkflowOperations,
    LegacyWorkflowSelector,
)
from dsctl.upstream.wire import WireRequest
from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot

if TYPE_CHECKING:
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.workflows import PreparedLegacyWorkflowUpdate


def _scope(*, release_state: str = "OFFLINE") -> WorkflowScope:
    project = ProjectRef(
        native=NativeId(7),
        name="etl-prod",
        description=None,
    )
    workflow = WorkflowRef(native=NativeId(101), name="daily-sync", version=1)
    return WorkflowScope(
        project=project,
        workflow=workflow,
        view=WorkflowView(
            ref=workflow,
            project_native=project.native,
            id=101,
            description="nightly",
            global_params="[]",
            global_param_map={},
            create_time="2026-08-13 09:00:00",
            update_time="2026-08-13 09:00:00",
            user_id=11,
            user_name="admin",
            project_name="etl-prod",
            timeout=0,
            release_state=release_state,
            execution_type=None,
            include_execution_type=False,
        ),
    )


def _snapshot(scope: WorkflowScope | None = None) -> LegacyWorkflowDefinitionSnapshot:
    selected_scope = scope or _scope()
    return LegacyWorkflowDefinitionSnapshot(
        scope=selected_scope,
        name="daily-sync",
        description="nightly",
        release_state=selected_scope.view.release_state,
        process_definition_json=json.dumps(
            {
                "globalParams": [],
                "tasks": [
                    {
                        "id": "extract-opaque-id",
                        "name": "extract",
                        "type": "SHELL",
                        "description": "extract source",
                        "params": {
                            "rawScript": "echo extract",
                            "resourceList": [{"id": 9}],
                            "localParams": [],
                            "futureParam": {"keep": True},
                        },
                        "preTasks": [],
                        "runFlag": "NORMAL",
                        "maxRetryTimes": 1,
                        "retryInterval": 2,
                        "taskInstancePriority": "HIGH",
                        "workerGroup": "analytics",
                        "futureTaskField": {"keep": True},
                    },
                    {
                        "id": "load-opaque-id",
                        "name": "load",
                        "type": "SHELL",
                        "description": "load target",
                        "params": {
                            "rawScript": "echo load",
                            "resourceList": [],
                            "localParams": [],
                        },
                        "preTasks": ["extract"],
                        "runFlag": "FORBIDDEN",
                        "maxRetryTimes": 0,
                        "retryInterval": 1,
                        "taskInstancePriority": "MEDIUM",
                        "workerGroup": "default",
                        "timeout": {
                            "enable": True,
                            "strategy": "WARN,FAILED",
                            "interval": 30,
                        },
                    },
                ],
                "timeout": 0,
                "tenantId": 4,
                "futureProcessField": "keep",
            },
            separators=(",", ":"),
        ),
        locations=json.dumps(
            {
                "extract-opaque-id": {
                    "name": "extract",
                    "targetarr": "",
                    "nodenumber": 1,
                    "x": 10,
                    "y": 20,
                },
                "load-opaque-id": {
                    "name": "load",
                    "targetarr": "extract-opaque-id",
                    "nodenumber": 0,
                    "x": 310,
                    "y": 20,
                },
            },
            separators=(",", ":"),
        ),
        connects=(
            '[{"endPointSourceId":"extract-opaque-id",'
            '"endPointTargetId":"load-opaque-id"}]'
        ),
    )


@dataclass
class _FakePreparedUpdate:
    request: WireRequest
    values: dict[str, str | None]


class _FakeOperations:
    ds_version = "1.3.9"

    def __init__(self) -> None:
        self.scope = _scope()
        self.snapshot = _snapshot(self.scope)
        self.applied: list[_FakePreparedUpdate] = []

    def resolve_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> WorkflowScope:
        assert project_selector == "etl-prod"
        assert workflow_selector in {"daily-sync", "101"}
        return self.scope

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        assert scope == self.scope
        assert action in {"task.list", "task.get", "task.update"}
        return self.snapshot

    def prepare_legacy_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> PreparedLegacyWorkflowUpdate:
        del scope
        values = {
            "name": name,
            "description": description,
            "process_definition_json": process_definition_json,
            "locations": locations,
            "connects": connects,
        }
        return _FakePreparedUpdate(  # type: ignore[return-value]
            request=WireRequest(
                method="POST",
                path="/projects/etl-prod/process/update",
                query=None,
                form={
                    "id": 101,
                    "name": name,
                    "description": description,
                    "processDefinitionJson": process_definition_json,
                    "locations": locations,
                    "connects": connects,
                },
                json=None,
                content=None,
            ),
            values=dict(values),
        )

    def apply_update(self, prepared: PreparedLegacyWorkflowUpdate) -> None:
        fake = cast("_FakePreparedUpdate", prepared)
        self.applied.append(fake)
        self.snapshot = LegacyWorkflowDefinitionSnapshot(
            scope=self.scope,
            name=str(fake.values["name"]),
            description=str(fake.values["description"]),
            release_state="OFFLINE",
            process_definition_json=str(fake.values["process_definition_json"]),
            locations=str(fake.values["locations"]),
            connects=str(fake.values["connects"]),
        )


def _definitions() -> LegacyTaskDefinitions[TaskAuthoringCatalog]:
    return LegacyTaskDefinitions(
        profile_version="1.3.9",
        operations=cast("LegacyWorkflowOperations", _FakeOperations()),
        catalog=get_task_authoring_catalog("1.3.9"),
        compile_update=prepare_legacy_workflow_mutation_plan,
    )


def test_list_projects_native_task_identity_without_fake_code_or_version() -> None:
    listing = _definitions().list(
        LegacyWorkflowSelector(project="etl-prod", workflow="daily-sync")
    )

    assert [task.to_data() for task in listing.tasks] == [
        {"id": "extract-opaque-id", "name": "extract"},
        {"id": "load-opaque-id", "name": "load"},
    ]


def test_get_projects_one_legacy_task_and_preserves_nested_params() -> None:
    task = _definitions().get(
        LegacyTaskSelector(
            project="etl-prod",
            workflow="daily-sync",
            task="load",
        )
    )

    assert task.scope.task.to_data() == {
        "id": "load-opaque-id",
        "name": "load",
    }
    assert task.view.to_data() == {
        "id": "load-opaque-id",
        "name": "load",
        "description": "load target",
        "taskType": "SHELL",
        "taskParams": {
            "rawScript": "echo load",
            "resourceList": [],
            "localParams": [],
        },
        "workerGroup": "default",
        "failRetryTimes": 0,
        "failRetryInterval": 1,
        "timeout": 30,
        "timeoutFlag": "OPEN",
        "timeoutNotifyStrategy": "WARNFAILED",
        "taskPriority": "MEDIUM",
        "flag": "NO",
        "dependsOn": ["extract"],
    }


def test_get_selects_by_exact_name_only_not_opaque_native_id() -> None:
    definitions = _definitions()

    with pytest.raises(NotFoundError, match="was not found"):
        definitions.get(
            LegacyTaskSelector(
                project="etl-prod",
                workflow="daily-sync",
                task="load-opaque-id",
            )
        )


def test_prepare_update_compiles_whole_workflow_and_preserves_unknown_fields() -> None:
    operations = _FakeOperations()
    definitions = LegacyTaskDefinitions(
        profile_version="1.3.9",
        operations=cast("LegacyWorkflowOperations", operations),
        catalog=get_task_authoring_catalog("1.3.9"),
        compile_update=prepare_legacy_workflow_mutation_plan,
    )

    prepared = definitions.prepare_update(
        LegacyTaskSelector(
            project="etl-prod",
            workflow="daily-sync",
            task="extract",
        ),
        patch=WorkflowPatchTaskSetSpec.model_validate(
            {"command": "echo changed", "retry": {"times": 3, "interval": 5}}
        ),
        requested_fields=("command", "retry.times", "retry.interval"),
    )

    assert prepared.request.path == "/projects/etl-prod/process/update"
    assert prepared.updated_fields == ("command", "retry.times", "retry.interval")
    assert prepared.no_change is False
    assert prepared.current.scope.task.to_data() == {
        "id": "extract-opaque-id",
        "name": "extract",
    }
    assert prepared.request.form is not None
    process_data = json.loads(str(prepared.request.form["processDefinitionJson"]))
    task_by_name = {task["name"]: task for task in process_data["tasks"]}
    assert process_data["futureProcessField"] == "keep"
    assert task_by_name["extract"]["id"] == "extract-opaque-id"
    assert task_by_name["extract"]["futureTaskField"] == {"keep": True}
    assert task_by_name["extract"]["params"] == {
        "rawScript": "echo changed",
        "resourceList": [{"id": 9}],
        "localParams": [],
        "futureParam": {"keep": True},
    }
    assert task_by_name["load"]["preTasks"] == ["extract"]
    assert operations.applied == []


def test_apply_rechecks_exact_graph_then_executes_same_prepared_request() -> None:
    operations = _FakeOperations()
    definitions = LegacyTaskDefinitions(
        profile_version="1.3.9",
        operations=cast("LegacyWorkflowOperations", operations),
        catalog=get_task_authoring_catalog("1.3.9"),
        compile_update=prepare_legacy_workflow_mutation_plan,
    )
    selector = LegacyTaskSelector("etl-prod", "daily-sync", "load")
    prepared = definitions.prepare_update(
        selector,
        patch=WorkflowPatchTaskSetSpec.model_validate({"flag": "YES"}),
        requested_fields=("flag",),
    )

    outcome = definitions.apply(prepared)

    assert outcome.mutation_applied is True
    assert operations.applied == [
        cast("_FakePreparedUpdate", prepared._prepared_update)
    ]
    assert outcome.value.view.to_data()["flag"] == "YES"


def test_noop_update_returns_current_task_without_sending_prepared_request() -> None:
    operations = _FakeOperations()
    definitions = LegacyTaskDefinitions(
        profile_version="1.3.9",
        operations=cast("LegacyWorkflowOperations", operations),
        catalog=get_task_authoring_catalog("1.3.9"),
        compile_update=prepare_legacy_workflow_mutation_plan,
    )
    prepared = definitions.prepare_update(
        LegacyTaskSelector("etl-prod", "daily-sync", "extract"),
        patch=WorkflowPatchTaskSetSpec.model_validate({"command": "echo extract"}),
        requested_fields=("command",),
    )

    outcome = definitions.apply(prepared)

    assert prepared.no_change is True
    assert prepared.updated_fields == ()
    assert outcome.mutation_applied is False
    assert outcome.value == prepared.current
    assert operations.applied == []


def test_apply_rejects_stale_whole_workflow_before_mutation() -> None:
    operations = _FakeOperations()
    definitions = LegacyTaskDefinitions(
        profile_version="1.3.9",
        operations=cast("LegacyWorkflowOperations", operations),
        catalog=get_task_authoring_catalog("1.3.9"),
        compile_update=prepare_legacy_workflow_mutation_plan,
    )
    prepared = definitions.prepare_update(
        LegacyTaskSelector("etl-prod", "daily-sync", "load"),
        patch=WorkflowPatchTaskSetSpec.model_validate({"flag": "YES"}),
        requested_fields=("flag",),
    )
    live = json.loads(operations.snapshot.process_definition_json)
    live["futureConcurrentChange"] = True
    operations.snapshot = LegacyWorkflowDefinitionSnapshot(
        scope=operations.scope,
        name="daily-sync",
        description="nightly",
        release_state="OFFLINE",
        process_definition_json=json.dumps(live, separators=(",", ":")),
        locations=operations.snapshot.locations,
        connects=operations.snapshot.connects,
    )

    with pytest.raises(ConflictError, match="stale"):
        definitions.apply(prepared)

    assert operations.applied == []


def test_prepare_rejects_online_workflow_before_compiling_update() -> None:
    operations = _FakeOperations()
    operations.scope = _scope(release_state="ONLINE")
    operations.snapshot = _snapshot(operations.scope)
    definitions = LegacyTaskDefinitions(
        profile_version="1.3.9",
        operations=cast("LegacyWorkflowOperations", operations),
        catalog=get_task_authoring_catalog("1.3.9"),
        compile_update=prepare_legacy_workflow_mutation_plan,
    )

    with pytest.raises(InvalidStateError, match="offline"):
        definitions.prepare_update(
            LegacyTaskSelector("etl-prod", "daily-sync", "load"),
            patch=WorkflowPatchTaskSetSpec.model_validate({"flag": "YES"}),
            requested_fields=("flag",),
        )

    assert operations.applied == []
