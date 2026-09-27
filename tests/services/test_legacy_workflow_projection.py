from __future__ import annotations

from dataclasses import replace

from dsctl.services._legacy_workflow_projection import LegacyWorkflowReadProjection
from dsctl.upstream.definition_models import (
    NativeId,
    ProjectRef,
    ScheduleView,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
)
from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot


def test_legacy_projection_exports_name_native_canonical_yaml() -> None:
    projection = _projection()

    rendered = projection.yaml_text()

    assert rendered == (
        "workflow:\n"
        "  name: daily-sync\n"
        "  timeout: 30\n"
        "  global_params:\n"
        "  - prop: biz_date\n"
        "    direct: IN\n"
        "    type: VARCHAR\n"
        "    value: '2026-08-13'\n"
        "  release_state: ONLINE\n"
        "  project: etl-prod\n"
        "  description: Nightly sync\n"
        "tasks:\n"
        "- name: extract\n"
        "  type: SHELL\n"
        "  task_params:\n"
        "    rawScript: echo extract\n"
        "    resourceList: []\n"
        "    localParams: []\n"
        "  flag: 'YES'\n"
        "  worker_group: analytics\n"
        "  priority: HIGH\n"
        "  retry:\n"
        "    times: 1\n"
        "    interval: 2\n"
        "  depends_on: []\n"
        "- name: load\n"
        "  type: SHELL\n"
        "  task_params:\n"
        "    rawScript: echo load\n"
        "    resourceList: []\n"
        "    localParams: []\n"
        "  flag: 'YES'\n"
        "  worker_group: default\n"
        "  priority: MEDIUM\n"
        "  retry:\n"
        "    times: 0\n"
        "    interval: 0\n"
        "  depends_on:\n"
        "  - extract\n"
    )
    assert "tasks-11" not in rendered
    assert "tasks-22" not in rendered
    assert "code:" not in rendered


def test_legacy_projection_export_preserves_complete_attached_schedule() -> None:
    projection = _projection(attached_schedule=_schedule())

    rendered = projection.yaml_text()

    assert rendered.endswith(
        "schedule:\n"
        "  cron: 0 0 0 * * ?\n"
        "  timezone: UTC\n"
        "  start: '2026-01-01 00:00:00'\n"
        "  end: '2026-12-31 23:59:59'\n"
        "  failure_strategy: CONTINUE\n"
        "  priority: MEDIUM\n"
        "  release_state: ONLINE\n"
    )


def test_legacy_projection_exports_server_local_schedule_without_timezone() -> None:
    projection = _projection(attached_schedule=replace(_schedule(), timezone_id=None))

    rendered = projection.yaml_text()

    assert "schedule:\n" in rendered
    assert "  cron: 0 0 0 * * ?\n" in rendered
    assert "  timezone:" not in rendered
    assert "  start: '2026-01-01 00:00:00'\n" in rendered


def test_legacy_projection_describes_native_ids_and_name_dependencies() -> None:
    projection = _projection(attached_schedule=_schedule())

    described = projection.describe()

    assert described == {
        "workflow": {
            "id": 101,
            "name": "daily-sync",
            "version": 3,
            "projectId": 7,
            "description": "Nightly sync",
            "globalParams": (
                '[{"prop":"biz_date","direct":"IN","type":"VARCHAR",'
                '"value":"2026-08-13"}]'
            ),
            "globalParamMap": {"biz_date": "2026-08-13"},
            "createTime": "2026-08-01 08:00:00",
            "updateTime": "2026-08-13 09:30:00",
            "userId": 1,
            "userName": "admin",
            "projectName": "etl-prod",
            "timeout": 30,
            "releaseState": "ONLINE",
            "scheduleReleaseState": "ONLINE",
            "schedule": {
                "id": 23,
                "startTime": "2026-01-01 00:00:00",
                "endTime": "2026-12-31 23:59:59",
                "timezoneId": "UTC",
                "crontab": "0 0 0 * * ?",
                "failureStrategy": "CONTINUE",
                "workflowInstancePriority": "MEDIUM",
                "releaseState": "ONLINE",
            },
        },
        "tasks": [
            {
                "id": "tasks-11",
                "name": "extract",
                "type": "SHELL",
                "taskParams": {
                    "rawScript": "echo extract",
                    "resourceList": [],
                    "localParams": [],
                },
                "dependsOn": [],
            },
            {
                "id": "tasks-22",
                "name": "load",
                "type": "SHELL",
                "taskParams": {
                    "rawScript": "echo load",
                    "resourceList": [],
                    "localParams": [],
                },
                "dependsOn": ["extract"],
            },
        ],
        "relations": [
            {
                "preTaskId": "tasks-11",
                "preTaskName": "extract",
                "postTaskId": "tasks-22",
                "postTaskName": "load",
            }
        ],
    }
    workflow = described["workflow"]
    assert isinstance(workflow, dict)
    assert "code" not in workflow


def test_legacy_projection_digests_graph_with_string_native_task_refs() -> None:
    projection = _projection(attached_schedule=_schedule())

    digested = projection.digest()

    assert digested == {
        "workflow": {
            "id": 101,
            "name": "daily-sync",
            "version": 3,
            "projectId": 7,
            "projectName": "etl-prod",
            "description": "Nightly sync",
            "releaseState": "ONLINE",
            "scheduleReleaseState": "ONLINE",
            "timeout": 30,
            "schedule": {
                "id": 23,
                "startTime": "2026-01-01 00:00:00",
                "endTime": "2026-12-31 23:59:59",
                "timezoneId": "UTC",
                "crontab": "0 0 0 * * ?",
                "failureStrategy": "CONTINUE",
                "workflowInstancePriority": "MEDIUM",
                "releaseState": "ONLINE",
            },
        },
        "taskCount": 2,
        "relationCount": 1,
        "taskTypeCounts": {"SHELL": 2},
        "globalParamNames": ["biz_date"],
        "rootTasks": [{"id": "tasks-11", "name": "extract"}],
        "leafTasks": [{"id": "tasks-22", "name": "load"}],
        "isolatedTasks": [],
        "tasks": [
            {
                "id": "tasks-11",
                "name": "extract",
                "taskType": "SHELL",
                "upstreamTasks": [],
                "downstreamTasks": [{"id": "tasks-22", "name": "load"}],
                "isRoot": True,
                "isLeaf": False,
            },
            {
                "id": "tasks-22",
                "name": "load",
                "taskType": "SHELL",
                "upstreamTasks": [{"id": "tasks-11", "name": "extract"}],
                "downstreamTasks": [],
                "isRoot": False,
                "isLeaf": True,
            },
        ],
    }
    assert "code" not in repr(digested)


def _projection(
    *, attached_schedule: ScheduleView | None = None
) -> LegacyWorkflowReadProjection:
    project = ProjectRef(NativeId(7), "etl-prod", "ETL project")
    workflow = WorkflowRef(NativeId(101), "daily-sync", 3)
    scope = WorkflowScope(
        project=project,
        workflow=workflow,
        view=WorkflowView(
            ref=workflow,
            project_native=project.native,
            id=101,
            description="Nightly sync",
            global_params=(
                '[{"prop":"biz_date","direct":"IN","type":"VARCHAR",'
                '"value":"2026-08-13"}]'
            ),
            global_param_map={"biz_date": "2026-08-13"},
            create_time="2026-08-01 08:00:00",
            update_time="2026-08-13 09:30:00",
            user_id=1,
            user_name="admin",
            project_name="etl-prod",
            timeout=30,
            release_state="ONLINE",
            execution_type=None,
            include_execution_type=False,
        ),
    )
    process_definition_json = (
        '{"globalParams":[{"prop":"biz_date","direct":"IN",'
        '"type":"VARCHAR","value":"2026-08-13"}],"tasks":['
        '{"id":"tasks-11","name":"extract","type":"SHELL",'
        '"params":{"rawScript":"echo extract","resourceList":[],'
        '"localParams":[]},"preTasks":[],"runFlag":"NORMAL",'
        '"maxRetryTimes":1,"retryInterval":2,'
        '"taskInstancePriority":"HIGH","workerGroup":"analytics"},'
        '{"id":"tasks-22","name":"load","type":"SHELL",'
        '"params":{"rawScript":"echo load","resourceList":[],'
        '"localParams":[]},"preTasks":["extract"],"runFlag":"NORMAL",'
        '"maxRetryTimes":0,"retryInterval":0,'
        '"taskInstancePriority":"MEDIUM","workerGroup":"default"}'
        '],"timeout":30,"tenantId":7}'
    )
    locations = (
        '{"tasks-11":{"name":"extract","targetarr":"",'
        '"nodenumber":1,"x":10,"y":20},'
        '"tasks-22":{"name":"load","targetarr":"tasks-11",'
        '"nodenumber":0,"x":310,"y":20}}'
    )
    connects = '[{"endPointSourceId":"tasks-11","endPointTargetId":"tasks-22"}]'
    snapshot = LegacyWorkflowDefinitionSnapshot(
        scope=scope,
        name="daily-sync",
        description="Nightly sync",
        release_state="ONLINE",
        process_definition_json=process_definition_json,
        locations=locations,
        connects=connects,
    )
    graph: DecodedLegacyWorkflowGraph = decode_legacy_workflow_graph(
        process_definition_json,
        locations,
        connects,
    )
    return LegacyWorkflowReadProjection(
        scope=scope,
        snapshot=snapshot,
        graph=graph,
        attached_schedule=attached_schedule,
    )


def _schedule() -> ScheduleView:
    return ScheduleView(
        id=23,
        workflow_native=NativeId(101),
        workflow_name="daily-sync",
        project_name="etl-prod",
        start_time="2026-01-01 00:00:00",
        end_time="2026-12-31 23:59:59",
        timezone_id="UTC",
        crontab="0 0 0 * * ?",
        failure_strategy="CONTINUE",
        workflow_instance_priority="MEDIUM",
        release_state="ONLINE",
    )
