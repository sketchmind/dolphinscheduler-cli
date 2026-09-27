from __future__ import annotations

import json
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs

import httpx
import pytest
import yaml

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.models.workflow_patch import WorkflowPatchSpec
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services._workflow.render import (
    serialize_workflow_dag,
    workflow_yaml_document,
)
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream import runtime_instances, task_logs
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    ProjectRef,
    WorkflowRef,
)
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.runtime_instances import (
    LocatedTaskInstance,
    LocatedWorkflowInstance,
    RuntimeInstanceAdapter,
    RuntimeInstanceOperations,
    TaskInstanceSnapshot,
    WorkflowInstanceSnapshot,
)
from dsctl.upstream.workflow_graph_requests import workflow_instance_update_arguments
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.support.json_types import JsonObject


@dataclass
class _Programs:
    ds_version: str
    invoke: Callable[[str, JsonObject], object]

    def call(self, primitive: str, args: JsonObject) -> object:
        return self.invoke(primitive, args)


@dataclass(frozen=True)
class _ProjectView:
    ref: ProjectRef


class _Definitions:
    def __init__(self, project: ProjectRef, workflow: WorkflowRef) -> None:
        self.project = project
        self.workflow = workflow
        self.calls: list[tuple[str, str]] = []

    def list_projects(self, **_: object) -> SimpleNamespace:
        self.calls.append(("list_projects", ""))
        return SimpleNamespace(totalList=(_ProjectView(self.project),))

    def resolve_project(self, selector: str) -> ProjectRef:
        self.calls.append(("resolve_project", selector))
        assert selector in {
            str(self.project.native.value),
            self.project.name,
        }
        return self.project

    def resolve_workflow(self, project: str, workflow: str) -> SimpleNamespace:
        self.calls.append(("resolve_workflow", workflow))
        assert project in {str(self.project.native.value), self.project.name}
        assert workflow in {str(self.workflow.native.value), self.workflow.name}
        return SimpleNamespace(workflow=self.workflow)


def _page(items: list[object]) -> SimpleNamespace:
    return SimpleNamespace(
        totalList=items,
        total=len(items),
        totalPage=1,
        pageSize=100,
        currentPage=1,
        pageNo=1,
    )


def _workflow_row(*, legacy: bool, instance_id: int = 31) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": instance_id,
        "state": "SUCCESS",
        "name": "daily-orders-1",
        "runTimes": 1,
        "executorId": 7,
        "timeout": 0,
        "globalParams": "[]",
    }
    if legacy:
        values.update(
            {
                "processDefinitionId": 13,
                "processInstanceJson": (
                    '{"futureTop":{"keep":true},"globalParams":[],"tasks":[],'
                    '"tenantId":-1,"timeout":0}'
                ),
                "locations": '{"futureLocation":{"keep":true}}',
                "connects": "[]",
            }
        )
    else:
        adapter = (
            WORKFLOW_PROGRAMS.profile("3.4.2")
            .program("definition_get")
            .codec.response_adapter
        )
        assert adapter is not None
        values.update(
            {
                "workflowDefinitionCode": 1300,
                "workflowDefinitionVersion": 2,
                "dagData": adapter.validate_python(
                    {
                        "workflowDefinition": {
                            "code": 1300,
                            "projectCode": 900,
                            "version": 2,
                        },
                        "workflowTaskRelationList": [],
                        "taskDefinitionList": [],
                    }
                ),
            }
        )
    return SimpleNamespace(**values)


def _exact_process_instance(version: str) -> object:
    task_values: dict[str, object] = {
        "code": 8800,
        "name": "load-orders",
        "projectCode": 900,
        "version": 3,
    }
    if version.startswith(("3.0.", "3.1.")):
        task_values.update(taskGroupId=12, taskGroupPriority=3)
    if version == "3.1.0":
        task_values.update(
            taskExecuteType="STREAM",
            cpuQuota=8,
            memoryMax=1024,
        )
    adapter = (
        WORKFLOW_PROGRAMS.profile(version)
        .program("instance_get")
        .codec.response_adapter
    )
    assert adapter is not None
    return adapter.validate_python(
        {
            "id": 31,
            "processDefinitionCode": 1300,
            "processDefinitionVersion": 2,
            "name": "daily-orders-1",
            "dagData": {
                "processDefinition": {"code": 1300, "projectCode": 900, "version": 2},
                "processTaskRelationList": [],
                "taskDefinitionList": [task_values],
            },
        }
    )


@pytest.mark.parametrize(
    ("version", "expected_group", "expected_resource"),
    [
        ("2.0.0", (0, 0), (None, None, None)),
        ("2.0.9", (0, 0), (None, None, None)),
        ("3.0.0", (12, 3), (None, None, None)),
        ("3.0.6", (12, 3), (None, None, None)),
        ("3.1.0", (12, 3), ("STREAM", 8, 1024)),
    ],
)
def test_exact_runtime_instance_projects_nonempty_nested_dag_tasks(
    version: str,
    expected_group: tuple[int, int],
    expected_resource: tuple[str | None, int | None, int | None],
) -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "instance_get"
        assert args == {"projectCode": 900, "id": 31}
        return _exact_process_instance(version)

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs(version, read)),
        runtime_instances._RECIPE_BY_VERSION[version],
        cast("Any", _Definitions(project, workflow)),
    )

    located = operations.get_workflow_instance(
        project_selector="orders",
        workflow_instance_id=31,
    )

    assert located.instance.dagData is not None
    rendered = serialize_workflow_dag(
        located.instance.dagData,
        attached_schedule=None,
    )
    task = cast("dict[str, object]", rendered["tasks"][0])
    assert (task["taskGroupId"], task["taskGroupPriority"]) == expected_group
    assert (
        task["taskExecuteType"],
        task["cpuQuota"],
        task["memoryMax"],
    ) == expected_resource


class _LegacyWorkflowGroup:
    def __init__(self) -> None:
        self.page_params: list[SimpleNamespace] = []
        self.get_params: list[SimpleNamespace] = []

    def call(self, primitive: str, args: JsonObject) -> object:
        assert args["projectName"] == "legacy-project"
        params = SimpleNamespace(**args)
        if primitive == "instance_page":
            self.page_params.append(params)
            return _page([_workflow_row(legacy=True)])
        assert primitive == "instance_get"
        self.get_params.append(params)
        return _workflow_row(legacy=True, instance_id=params.processInstanceId)


def test_legacy_instance_projection_preserves_native_ids_and_name_route() -> None:
    project = ProjectRef(NativeId(9), "legacy-project", None)
    workflow = WorkflowRef(NativeId(13), "daily-orders", 1)
    group = _LegacyWorkflowGroup()
    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("1.3.9", group.call)),
        runtime_instances._RECIPE_BY_VERSION["1.3.9"],
        cast("Any", _Definitions(project, workflow)),
    )

    listing = operations.list_workflow_instances(
        project_selector="legacy-project",
        workflow_selector="daily-orders",
        page_no=1,
        page_size=100,
        search=None,
        executor=None,
        host=None,
        start_time=None,
        end_time=None,
        state=None,
    )
    item = next(iter(listing.page.totalList or ()))
    assert item.to_data()["projectId"] == 9
    assert item.to_data()["workflowDefinitionId"] == 13
    assert "projectCode" not in item.to_data()
    assert "workflowDefinitionCode" not in item.to_data()
    assert group.page_params[-1].processDefinitionId == 13

    located = operations.get_workflow_instance(
        project_selector="legacy-project",
        workflow_instance_id=31,
    )
    assert located.project == project
    assert group.get_params[-1].processInstanceId == 31
    assert located.instance.processInstanceJson is not None
    assert located.instance.locations == '{"futureLocation":{"keep":true}}'
    assert located.instance.connects == "[]"


@pytest.mark.parametrize(
    ("version", "wire_field", "output_field", "absent_field", "missing_value"),
    [
        (
            "1.3.9",
            "processDefinitionId",
            "workflowDefinitionId",
            "workflowDefinitionCode",
            0,
        ),
        (
            "3.4.1",
            "workflowDefinitionCode",
            "workflowDefinitionCode",
            "workflowDefinitionId",
            None,
        ),
    ],
)
def test_instance_list_keeps_exact_definition_field_for_missing_identity(
    version: str,
    wire_field: str,
    output_field: str,
    absent_field: str,
    missing_value: int | None,
) -> None:
    native_type = NativeId if version == "1.3.9" else NativeCode
    project = ProjectRef(native_type(9), "orders", None)
    workflow = WorkflowRef(native_type(13), "daily-orders", 1)
    adapter = (
        WORKFLOW_PROGRAMS.profile(version)
        .program("instance_page")
        .codec.response_adapter
    )
    assert adapter is not None

    def read(primitive: str, _args: JsonObject) -> object:
        assert primitive == "instance_page"
        return adapter.validate_python(
            {
                "totalList": [{"id": 31, wire_field: 13}, {"id": 32}],
                "total": 2,
                "pageSize": 100,
                "currentPage": 1,
            }
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs(version, read)),
        runtime_instances._RECIPE_BY_VERSION[version],
        cast("Any", _Definitions(project, workflow)),
    )
    listing = operations.list_workflow_instances(
        project_selector="orders",
        workflow_selector=None,
        page_no=1,
        page_size=100,
        search=None,
        executor=None,
        host=None,
        start_time=None,
        end_time=None,
        state=None,
    )
    instances = list(listing.page.totalList or ())
    rows = [item.to_data() for item in instances]
    assert [row[output_field] for row in rows] == [13, missing_value]
    assert rows[0].keys() == rows[1].keys()
    assert all(absent_field not in row for row in rows)

    # The legacy wire defaults its primitive id to zero; an unknown snapshot
    # still retains its exact field without inventing the other identity kind.
    unknown = replace(instances[0], workflow_native=None).to_data()
    assert unknown[output_field] is None
    assert unknown.keys() == rows[0].keys()
    assert absent_field not in unknown


def test_legacy_instance_update_prepares_and_applies_one_exact_generated_form() -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    profile = make_profile(ds_version="1.3.9")
    adapter = RuntimeInstanceAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    project = ProjectRef(NativeId(9), "legacy-project", None)
    instance = WorkflowInstanceSnapshot(
        ds_version="1.3.9",
        id=31,
        project=project,
        workflow_native=NativeId(13),
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
        scheduleTime="2026-08-13 00:00:00",
        executorId=7,
        executorName="alice",
        tenantCode=None,
        queue=None,
        duration=None,
        workflowInstancePriority="MEDIUM",
        workerGroup="default",
        environmentCode=None,
        timeout=0,
        dryRun=0,
        restartTime=None,
        dagData=None,
        processInstanceJson=(
            '{"futureTop":{"keep":true},"globalParams":[],"tasks":[],'
            '"tenantId":-1,"timeout":0}'
        ),
        locations='{ "futureLocation": {"keep": true} }',
        connects="[]",
    )
    located = LocatedWorkflowInstance(project=project, instance=instance)
    process_instance_json = instance.processInstanceJson
    locations = instance.locations
    connects = instance.connects
    assert process_instance_json is not None
    assert locations is not None
    assert connects is not None

    with http_client:
        operations = adapter.bind(profile, http_client=http_client).instances
        prepared = operations.prepare_legacy_workflow_instance_update(
            located,
            process_instance_json=process_instance_json,
            locations=locations,
            connects=connects,
            sync_define=False,
        )

        assert requests_seen == []
        assert prepared.request.method == "POST"
        assert prepared.request.path == ("/projects/legacy-project/instance/update")
        assert prepared.request.form == {
            "processInstanceJson": process_instance_json,
            "processInstanceId": 31,
            "syncDefine": False,
            "locations": locations,
            "connects": "[]",
        }

        operations.apply_legacy_workflow_instance_update(prepared)

    assert requests_seen == [
        (
            "/dolphinscheduler/projects/legacy-project/instance/update",
            {
                "processInstanceJson": [process_instance_json],
                "processInstanceId": ["31"],
                "syncDefine": ["false"],
                "locations": [locations],
                "connects": ["[]"],
            },
        )
    ]


class _WorkflowGroup:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, object]] = []

    def query_workflow_instance_list(
        self,
        project_code: int,
        params: SimpleNamespace,
    ) -> SimpleNamespace:
        self.calls.append(("page", project_code, params))
        return _page([_workflow_row(legacy=False)])

    def query_workflow_instance_by_id(
        self,
        project_code: int,
        instance_id: int,
    ) -> SimpleNamespace:
        self.calls.append(("get", project_code, instance_id))
        return _workflow_row(legacy=False, instance_id=instance_id)

    def query_parent_instance_by_sub_id(
        self,
        project_code: int,
        params: SimpleNamespace,
    ) -> dict[str, int]:
        self.calls.append(("parent", project_code, params))
        return {"parentWorkflowInstance": 12}

    def query_sub_workflow_instance_by_task_id(
        self,
        project_code: int,
        params: SimpleNamespace,
    ) -> dict[str, int]:
        self.calls.append(("sub", project_code, params))
        return {"subWorkflowInstanceId": 44}


class _TaskGroup:
    def __init__(self) -> None:
        self.calls: list[tuple[int, SimpleNamespace]] = []
        self.actions: list[tuple[str, int, int]] = []

    def query_task_list_paging(
        self,
        project_code: int,
        params: SimpleNamespace,
    ) -> SimpleNamespace:
        self.calls.append((project_code, params))
        return _page(
            [
                {
                    "id": 88,
                    "name": "load-orders",
                    "taskType": "SQL",
                    "workflowInstanceId": 31,
                    "workflowInstanceName": "daily-orders-1",
                    "taskCode": 8800,
                    "taskDefinitionVersion": 3,
                    "workflowDefinitionName": "daily-orders",
                    "state": "FAILURE",
                }
            ]
        )

    def force_task_success(self, project_code: int, task_id: int) -> None:
        self.actions.append(("force-success", project_code, task_id))

    def task_save_point(self, project_code: int, task_id: int) -> None:
        self.actions.append(("savepoint", project_code, task_id))

    def stop_task(self, project_code: int, task_id: int) -> None:
        self.actions.append(("stop", project_code, task_id))


def _modern_operations() -> tuple[
    RuntimeInstanceOperations,
    _WorkflowGroup,
    _TaskGroup,
]:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    workflow_group = _WorkflowGroup()
    task_group = _TaskGroup()

    def read(primitive: str, args: JsonObject) -> object:
        project_code = cast("int", args["projectCode"])
        if primitive == "instance_get":
            return workflow_group.query_workflow_instance_by_id(
                project_code, cast("int", args["id"])
            )
        params = SimpleNamespace(**args)
        if primitive == "instance_page":
            return workflow_group.query_workflow_instance_list(project_code, params)
        if primitive == "instance_parent":
            return workflow_group.query_parent_instance_by_sub_id(project_code, params)
        assert primitive == "task_instance_page"
        return task_group.query_task_list_paging(project_code, params)

    return (
        RuntimeInstanceOperations(
            cast("Any", _Programs("3.4.2", read)),
            runtime_instances._RECIPE_BY_VERSION["3.4.2"],
            cast("Any", _Definitions(project, workflow)),
        ),
        workflow_group,
        task_group,
    )


def test_modern_task_get_uses_project_page_not_removed_v2_detail() -> None:
    operations, workflow_group, task_group = _modern_operations()

    located = operations.get_task_instance(
        project_selector="orders",
        task_instance_id=88,
        workflow_instance_id=31,
    )

    assert located.project.native == NativeCode(900)
    assert located.task.taskCode == 8800
    assert task_group.calls[-1][1].workflowInstanceId == 31
    assert [call[0] for call in workflow_group.calls].count("get") == 1


def test_project_only_task_get_checks_batch_and_stream_without_workflow_detail() -> (
    None
):
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    calls: list[dict[str, object]] = []

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        calls.append(dict(args))
        rows: list[object] = (
            [
                {
                    "id": 99,
                    "name": "stream-orders",
                    "taskType": "FLINK_STREAM",
                    "workflowInstanceId": 0,
                    "taskExecuteType": "STREAM",
                    "state": "RUNNING_EXECUTION",
                }
            ]
            if args["taskExecuteType"] == "STREAM"
            else []
        )
        return _page(rows)

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    located = operations.get_task_instance(
        project_selector="orders",
        task_instance_id=99,
        workflow_instance_id=None,
    )

    assert located.workflow_instance is None
    assert located.task.workflowInstanceId == 0
    assert [call["taskExecuteType"] for call in calls] == ["BATCH", "STREAM"]
    assert all(call["workflowInstanceId"] is None for call in calls)


def test_project_only_task_get_returns_first_page_match_before_large_history() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    calls: list[tuple[str, int]] = []

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        execute_type = cast("str", args["taskExecuteType"])
        page_no = cast("int", args["pageNo"])
        calls.append((execute_type, page_no))
        return SimpleNamespace(
            totalList=[
                {
                    "id": 88,
                    "name": "batch-orders",
                    "workflowInstanceId": 31,
                    "taskExecuteType": "BATCH",
                    "state": "SUCCESS",
                }
            ],
            total=20_000,
            totalPage=200,
            pageSize=100,
            currentPage=page_no,
            pageNo=page_no,
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    located = operations.get_task_instance(
        project_selector="orders",
        task_instance_id=88,
        workflow_instance_id=None,
    )

    assert located.task.id == 88
    assert calls == [("BATCH", 1)]


def test_project_only_task_get_checks_stream_before_large_batch_history() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    calls: list[tuple[str, int]] = []

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        execute_type = cast("str", args["taskExecuteType"])
        page_no = cast("int", args["pageNo"])
        calls.append((execute_type, page_no))
        rows = (
            [
                {
                    "id": 99,
                    "name": "stream-orders",
                    "workflowInstanceId": 0,
                    "taskExecuteType": "STREAM",
                    "state": "RUNNING_EXECUTION",
                }
            ]
            if execute_type == "STREAM"
            else []
        )
        return SimpleNamespace(
            totalList=rows,
            total=20_000 if execute_type == "BATCH" else 1,
            totalPage=200 if execute_type == "BATCH" else 1,
            pageSize=100,
            currentPage=page_no,
            pageNo=page_no,
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    located = operations.get_task_instance(
        project_selector="orders",
        task_instance_id=99,
        workflow_instance_id=None,
    )

    assert located.task.workflowInstanceId == 0
    assert calls == [("BATCH", 1), ("STREAM", 1)]


def test_project_only_task_get_exhausts_multiple_pages_before_matching() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    calls: list[tuple[str, int]] = []

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        execute_type = cast("str", args["taskExecuteType"])
        page_no = cast("int", args["pageNo"])
        calls.append((execute_type, page_no))
        rows: list[object] = []
        if execute_type == "BATCH" and page_no == 2:
            rows = [
                {
                    "id": 88,
                    "name": "batch-orders",
                    "workflowInstanceId": 31,
                    "taskExecuteType": "BATCH",
                    "state": "SUCCESS",
                }
            ]
        return SimpleNamespace(
            totalList=rows,
            total=1 if execute_type == "BATCH" else 0,
            totalPage=2 if execute_type == "BATCH" else 1,
            pageSize=100,
            currentPage=page_no,
            pageNo=page_no,
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    located = operations.get_task_instance(
        project_selector="orders",
        task_instance_id=88,
        workflow_instance_id=None,
    )

    assert located.task.id == 88
    assert calls == [("BATCH", 1), ("STREAM", 1), ("BATCH", 2)]


def test_project_only_task_get_on_old_profile_uses_one_unspecialized_scan() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    calls: list[dict[str, object]] = []

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        calls.append(dict(args))
        return _page(
            [
                {
                    "id": 88,
                    "name": "batch-orders",
                    "processInstanceId": 31,
                    "state": "SUCCESS",
                }
            ]
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.0.6", read)),
        runtime_instances._RECIPE_BY_VERSION["3.0.6"],
        cast("Any", _Definitions(project, workflow)),
    )

    located = operations.get_task_instance(
        project_selector="orders",
        task_instance_id=88,
        workflow_instance_id=None,
    )

    assert located.task.id == 88
    assert len(calls) == 1
    assert "taskExecuteType" not in calls[0]


def test_project_only_task_get_reports_not_found_only_after_complete_scan() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    calls: list[tuple[str, int]] = []

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        execute_type = cast("str", args["taskExecuteType"])
        page_no = cast("int", args["pageNo"])
        calls.append((execute_type, page_no))
        return SimpleNamespace(
            totalList=[],
            total=200 if execute_type == "BATCH" else 0,
            totalPage=2 if execute_type == "BATCH" else 0,
            pageSize=100,
            currentPage=page_no,
            pageNo=page_no,
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    with pytest.raises(ApiResultError) as exc_info:
        operations.get_task_instance(
            project_selector="orders",
            task_instance_id=88,
            workflow_instance_id=None,
        )

    assert exc_info.value.result_code == 10008
    assert calls == [("BATCH", 1), ("STREAM", 1), ("BATCH", 2)]
    coverage = cast("dict[str, object]", exc_info.value.details["coverage"])
    assert coverage["scope_complete"] is True


def test_project_only_task_get_propagates_permission_error_with_coverage() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)
    denied = ApiResultError(result_code=10015, result_message="permission denied")

    def read(primitive: str, _args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        raise denied

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    with pytest.raises(ApiResultError) as exc_info:
        operations.get_task_instance(
            project_selector="orders",
            task_instance_id=88,
            workflow_instance_id=None,
        )

    assert exc_info.value is denied
    coverage = cast("dict[str, object]", denied.details["coverage"])
    assert coverage["scope_complete"] is False


def test_project_only_task_get_rejects_missing_pagination_metadata() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)

    def read(primitive: str, _args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        return SimpleNamespace(totalList=[])

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    with pytest.raises(UserInputError, match="before the project scope") as exc_info:
        operations.get_task_instance(
            project_selector="orders",
            task_instance_id=88,
            workflow_instance_id=None,
        )

    assert exc_info.value.details["reason"] == "pagination_metadata_incomplete"
    assert "--workflow-instance" in cast("str", exc_info.value.suggestion)
    coverage = cast("dict[str, object]", exc_info.value.details["coverage"])
    assert coverage["pages_read"] == 1
    assert coverage["scope_complete"] is False


def test_project_only_task_get_does_not_report_not_found_past_scan_limit() -> None:
    project = ProjectRef(NativeCode(900), "orders", None)
    workflow = WorkflowRef(NativeCode(1300), "daily-orders", 2)

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "task_instance_page"
        return SimpleNamespace(
            totalList=[],
            total=10_001,
            totalPage=101,
            pageSize=100,
            currentPage=args["pageNo"],
            pageNo=args["pageNo"],
        )

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("3.4.2", read)),
        runtime_instances._RECIPE_BY_VERSION["3.4.2"],
        cast("Any", _Definitions(project, workflow)),
    )

    with pytest.raises(UserInputError, match="before the project scope") as exc_info:
        operations.get_task_instance(
            project_selector="orders",
            task_instance_id=88,
            workflow_instance_id=None,
        )

    assert exc_info.value.details["max_pages"] == 100
    assert exc_info.value.details["reason"] == "page_safety_limit"
    assert "--workflow-instance" in cast("str", exc_info.value.suggestion)
    coverage = cast("dict[str, object]", exc_info.value.details["coverage"])
    assert coverage["pages_read"] == 200
    assert coverage["scope_complete"] is False


def test_workflow_selector_is_resolved_only_inside_selected_project() -> None:
    operations, workflow_group, _ = _modern_operations()

    listing = operations.list_workflow_instances(
        project_selector="orders",
        workflow_selector="daily-orders",
        page_no=1,
        page_size=100,
        search=None,
        executor=None,
        host=None,
        start_time=None,
        end_time=None,
        state=None,
    )

    assert listing.project.name == "orders"
    assert [item.id for item in (listing.page.totalList or ())] == [31]
    page_call = next(call for call in workflow_group.calls if call[0] == "page")
    assert isinstance(page_call[2], SimpleNamespace)
    assert page_call[2].workflowDefinitionCode == 1300


def test_legacy_task_name_filter_fails_before_generated_request() -> None:
    project = ProjectRef(NativeId(9), "legacy-project", None)
    workflow = WorkflowRef(NativeId(13), "daily-orders", 1)
    requests: list[object] = []
    definitions = _Definitions(project, workflow)
    operations = RuntimeInstanceOperations(
        cast(
            "Any",
            _Programs(
                "1.3.9", lambda primitive, args: requests.append((primitive, args))
            ),
        ),
        runtime_instances._RECIPE_BY_VERSION["1.3.9"],
        cast("Any", definitions),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        operations.list_task_instances(
            project_selector="legacy-project",
            workflow_instance_id=None,
            workflow_instance_name="daily-orders-1",
            page_no=1,
            page_size=100,
            search=None,
            task_name=None,
            task_code=None,
            executor=None,
            state=None,
            host=None,
            start_time=None,
            end_time=None,
            task_execute_type=None,
        )

    assert exc_info.value.details["flag"] == "--workflow-instance-name"
    assert requests == []
    assert definitions.calls == []


@pytest.mark.parametrize(
    (
        "version",
        "flag",
        "workflow_instance_name",
        "task_code",
        "task_execute_type",
    ),
    [
        ("1.3.9", "--workflow-instance-name", "run-1", None, None),
        ("2.0.9", "--execute-type", None, None, "BATCH"),
        ("3.1.9", "--task-code", None, 42, None),
    ],
)
def test_task_list_facet_guards_run_before_selector_and_page_io(
    version: str,
    flag: str,
    workflow_instance_name: str | None,
    task_code: int | None,
    task_execute_type: str | None,
) -> None:
    recipe = runtime_instances._RECIPE_BY_VERSION[version]
    project = ProjectRef(
        NativeId(9) if recipe.project_identity == "name" else NativeCode(9),
        "project",
        None,
    )
    workflow = WorkflowRef(
        NativeId(13) if recipe.definition_identity == "id" else NativeCode(13),
        "workflow",
        1,
    )
    definitions = _Definitions(project, workflow)
    page_calls: list[object] = []
    operations = RuntimeInstanceOperations(
        cast(
            "Any",
            _Programs(
                version, lambda primitive, args: page_calls.append((primitive, args))
            ),
        ),
        recipe,
        cast("Any", definitions),
    )
    with pytest.raises(UnsupportedFeatureError) as exc_info:
        operations.list_task_instances(
            project_selector="project",
            workflow_instance_id=None,
            workflow_instance_name=workflow_instance_name,
            page_no=1,
            page_size=100,
            search=None,
            task_name=None,
            task_code=task_code,
            executor=None,
            state=None,
            host=None,
            start_time=None,
            end_time=None,
            task_execute_type=task_execute_type,
        )

    assert exc_info.value.details["flag"] == flag
    assert definitions.calls == []
    assert page_calls == []


def test_relation_maps_are_normalized() -> None:
    operations, _, _ = _modern_operations()
    workflow = operations.get_workflow_instance(
        project_selector="orders",
        workflow_instance_id=31,
    )

    assert operations.parent_workflow_instance_id(workflow) == 12


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2", "3.4.2", "3.4.3"])
@pytest.mark.parametrize(
    ("scope", "native_scope"),
    [("self", "TASK_ONLY"), ("pre", "TASK_PRE"), ("post", "TASK_POST")],
)
def test_execute_task_scope_reaches_exact_compiled_wire(
    version: str,
    scope: str,
    native_scope: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": None})

    profile = make_profile(ds_version=version)
    project = ProjectRef(NativeCode(7), "project", None)
    located = LocatedWorkflowInstance(
        project,
        cast("Any", SimpleNamespace(id=91)),
    )
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        programs = WORKFLOW_PROGRAMS.bind(
            WORKFLOW_PROGRAMS.profile(version), profile, http_client=client
        )
        operations = RuntimeInstanceOperations(
            programs,
            runtime_instances._RECIPE_BY_VERSION[version],
            cast("Any", object()),
        )
        operations.execute_task(located, task_code=123, scope=scope)

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert request.url.path.endswith("/projects/7/executors/execute-task")
    instance_field = (
        "processInstanceId" if version.startswith("3.2.") else "workflowInstanceId"
    )
    assert parse_qs(request.content.decode()) == {
        instance_field: ["91"],
        "startNodeList": ["123"],
        "taskDependType": [native_scope],
    }


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.4.2"])
def test_exact_log_epochs_return_one_canonical_tail(version: str) -> None:
    recipe = runtime_instances._RECIPE_BY_VERSION[version]
    requests: list[SimpleNamespace] = []
    header = "" if version == "1.3.9" else "[LOG-PATH]: /tmp/task.log\n"
    body = "" if version == "2.0.0" else "line\r\n" * 1000
    first = header + body

    def read_log(params: SimpleNamespace) -> object:
        requests.append(params)
        responses = (first, "tail-1\r\ntail-2\r\n", "")
        text = responses[min(len(requests) - 1, 2)]
        if recipe.log_epoch in {"query-log-string", "log-detail-string"}:
            return text
        return SimpleNamespace(lineNum=37, message=text)

    operations = RuntimeInstanceOperations(
        cast(
            "Any",
            _Programs(
                version, lambda primitive, args: read_log(SimpleNamespace(**args))
            ),
        ),
        recipe,
        cast("Any", object()),
    )

    result = operations.tail_task_log(
        task_instance_id=88,
        max_lines=2,
    )
    if version == "2.0.0":
        assert result.text == header.rstrip()
        assert result.line_count == 1
        assert [request.skipLineNum for request in requests] == [0]
    else:
        assert result.text == "tail-1\ntail-2"
        assert result.line_count == 2
        assert [request.skipLineNum for request in requests] == [0, 1000, 1002]


def test_task_log_tail_refuses_to_exceed_the_chunk_safety_limit() -> None:
    calls = 0

    def endless_log(
        *,
        task_instance_id: int,
        skip_line_num: int,
        limit: int,
    ) -> task_logs.TaskLogChunk:
        nonlocal calls
        del task_instance_id, skip_line_num, limit
        calls += 1
        return task_logs.TaskLogChunk(message=None, cursor_advance=1000, eof=False)

    with pytest.raises(UserInputError):
        task_logs.tail_task_log(
            task_instance_id=88,
            max_lines=2,
            read_chunk=endless_log,
        )

    assert calls == 200


@pytest.mark.parametrize("version", tuple(runtime_instances._RECIPE_BY_VERSION))
def test_sub_workflow_relation_uses_the_exact_version_result_key(
    version: str,
) -> None:
    recipe = runtime_instances._RECIPE_BY_VERSION[version]
    project = ProjectRef(
        NativeId(9) if recipe.project_identity == "name" else NativeCode(9),
        "project",
        None,
    )
    calls: list[tuple[object, object]] = []

    def relation(route: object, params: object) -> dict[str, int]:
        calls.append((route, params))
        return {recipe.workflow_sub_result_key: 44}

    def read(primitive: str, args: JsonObject) -> object:
        assert primitive == "instance_sub"
        route = args["projectName"] if version == "1.3.9" else args["projectCode"]
        return relation(route, SimpleNamespace(taskId=args["taskId"]))

    operations = RuntimeInstanceOperations(
        cast("Any", _Programs(version, read)),
        recipe,
        cast("Any", object()),
    )

    assert (
        operations.sub_workflow_instance_id(
            cast(
                "Any",
                SimpleNamespace(project=project, task=SimpleNamespace(id=88)),
            )
        )
        == 44
    )
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("version", "action"),
    [
        ("1.3.9", "force-success"),
        ("2.0.0", "savepoint"),
        ("3.0.6", "stop"),
    ],
)
def test_terminal_task_actions_fail_before_generated_request(
    version: str,
    action: str,
) -> None:
    recipe = runtime_instances._RECIPE_BY_VERSION[version]
    project = ProjectRef(
        NativeId(1) if recipe.project_identity == "name" else NativeCode(1),
        "project",
        None,
    )
    workflow = WorkflowInstanceSnapshot(
        ds_version=version,
        id=2,
        project=project,
        workflow_native=None,
        workflowDefinitionVersion=0,
        state="FAILURE",
        recovery=None,
        startTime=None,
        endTime=None,
        runTimes=1,
        name=None,
        host=None,
        commandType=None,
        taskDependType=None,
        failureStrategy=None,
        warningType=None,
        scheduleTime=None,
        executorId=0,
        executorName=None,
        tenantCode=None,
        queue=None,
        duration=None,
        workflowInstancePriority=None,
        workerGroup=None,
        environmentCode=None,
        timeout=0,
        dryRun=0,
        restartTime=None,
        dagData=None,
    )
    task = TaskInstanceSnapshot(
        ds_version=version,
        id=3,
        project=project,
        name=None,
        taskType=None,
        workflowInstanceId=2,
        workflowInstanceName=None,
        taskCode=None,
        taskDefinitionVersion=None,
        workflowDefinitionName=None,
        state="FAILURE",
        firstSubmitTime=None,
        submitTime=None,
        startTime=None,
        endTime=None,
        host=None,
        logPath=None,
        retryTimes=0,
        duration=None,
        executorName=None,
        workerGroup=None,
        environmentCode=None,
        delayTime=0,
        taskParams=None,
        dryRun=0,
        taskGroupId=0,
        taskExecuteType=None,
    )
    requests: list[object] = []
    operations = RuntimeInstanceOperations(
        cast(
            "Any",
            _Programs(
                version, lambda primitive, args: requests.append((primitive, args))
            ),
        ),
        recipe,
        cast("Any", object()),
    )

    with pytest.raises(UnsupportedFeatureError):
        operations.task_action(
            LocatedTaskInstance(project, workflow, task),
            action=action,
        )

    assert requests == []


@pytest.mark.parametrize("version", tuple(runtime_instances._RECIPE_BY_VERSION))
def test_exact_logger_window_passes_native_offset_and_limit(version: str) -> None:
    recipe = runtime_instances._RECIPE_BY_VERSION[version]
    requests: list[SimpleNamespace] = []

    def read_log(params: SimpleNamespace) -> str | SimpleNamespace:
        requests.append(params)
        if recipe.log_epoch in {"query-log-string", "log-detail-string"}:
            return "source-42\r\nsource-43\r\n"
        return SimpleNamespace(lineNum=2, message="source-42\r\nsource-43\r\n")

    operations = RuntimeInstanceOperations(
        cast(
            "Any",
            _Programs(
                version, lambda primitive, args: read_log(SimpleNamespace(**args))
            ),
        ),
        recipe,
        cast("Any", object()),
    )
    result = operations.window_task_log(task_instance_id=88, start_line=42, limit=1)
    assert result.text == "source-42"
    assert result.window is not None
    assert result.window["next_start_line"] == 43
    assert [(p.skipLineNum, p.limit) for p in requests] == [(41, 2)]


def test_legacy_single_time_bound_is_rejected_before_project_or_page_requests() -> None:
    definitions = _Definitions(
        ProjectRef(NativeId(9), "legacy-project", None),
        WorkflowRef(NativeId(13), "daily-orders", 1),
    )
    group = _LegacyWorkflowGroup()
    operations = RuntimeInstanceOperations(
        cast("Any", _Programs("1.3.9", group.call)),
        runtime_instances._RECIPE_BY_VERSION["1.3.9"],
        cast("Any", definitions),
    )
    with pytest.raises(UserInputError, match="both --start and --end"):
        operations.list_workflow_instances(
            project_selector="legacy-project",
            workflow_selector=None,
            page_no=1,
            page_size=100,
            search=None,
            executor=None,
            host=None,
            start_time=None,
            end_time="2026-09-02 00:00:00",
            state=None,
        )
    assert definitions.calls == []
    assert group.page_params == []


def test_native_summary_list_and_trigger_do_not_require_heavy_detail_fields() -> None:
    version = "3.4.3"
    profile = make_profile(ds_version=version)
    project = ProjectRef(NativeCode(9), "orders", None)
    workflow = WorkflowRef(NativeCode(13), "daily-orders", 1)
    calls: list[tuple[str, str]] = []
    summary = {
        "id": 31,
        "workflowDefinitionCode": 13,
        "workflowDefinitionVersion": 2,
        "projectCode": 9,
        "name": "daily-orders-1",
        "state": "SUCCESS",
        "runTimes": 1,
        "executorId": 7,
        "timeout": 0,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path.endswith("/trigger"):
            return httpx.Response(
                200, json={"code": 0, "msg": "success", "data": [summary]}
            )
        if request.url.path.endswith("/workflow-instances/31"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        **summary,
                        "queue": "detail-only",
                        "commandParam": "{}",
                        "globalParams": "[]",
                        "dagData": None,
                    },
                },
            )
        if request.url.path.endswith("/workflow-instances"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [summary],
                        "total": 1,
                        "pageSize": 100,
                        "currentPage": 1,
                    },
                },
            )
        message = f"unexpected {request.method} {request.url.path}"
        raise AssertionError(message)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        programs = WORKFLOW_PROGRAMS.bind(
            WORKFLOW_PROGRAMS.profile(version), profile, http_client=client
        )
        operations = RuntimeInstanceOperations(
            programs,
            runtime_instances._RECIPE_BY_VERSION[version],
            cast("Any", _Definitions(project, workflow)),
        )
        listing = operations.list_workflow_instances(
            project_selector="orders",
            workflow_selector=None,
            page_no=1,
            page_size=100,
            search=None,
            executor=None,
            host=None,
            start_time=None,
            end_time=None,
            state=None,
        )
        listed = next(iter(listing.page.totalList or ()))
        assert listed.workflowDefinitionCode == 13
        assert listed.workflowDefinitionVersion == 2
        assert listed.to_data()["state"] == "SUCCESS"
        assert "queue" not in listed.to_data()
        assert listed.dagData is None
        assert len(calls) == 1
        trigger = programs.call(
            "instance_trigger", {"projectCode": 9, "triggerCode": 123}
        )
        assert isinstance(trigger, list)
        assert len(trigger) == 1
        assert trigger[0].id == 31
        assert "commandParam" not in type(trigger[0]).model_fields
        assert "globalParams" not in type(trigger[0]).model_fields
        detailed = operations.get_workflow_instance(
            project_selector="orders", workflow_instance_id=31
        ).instance
        assert detailed.to_data()["queue"] == "detail-only"
    assert calls == [
        ("GET", "/dolphinscheduler/projects/9/workflow-instances"),
        (
            "GET",
            "/dolphinscheduler/projects/9/workflow-instances/trigger",
        ),
        ("GET", "/dolphinscheduler/projects/9/workflow-instances/31"),
    ]


def _instance_scalar_wire(version: str, global_params: str | None) -> JsonObject:
    family = "workflow" if version.startswith(("3.3.", "3.4.")) else "process"
    return {
        "id": 31,
        f"{family}DefinitionCode": 1300,
        f"{family}DefinitionVersion": 2,
        "globalParams": global_params,
        "timeout": 45,
        "dagData": {
            f"{family}Definition": {
                "code": 1300,
                "projectCode": 900,
                "version": 2,
                "name": "daily-orders",
                "timeout": 30,
                "globalParams": '[{"prop":"env","value":"definition"}]',
                "globalParamMap": {"env": "definition"},
            },
            f"{family}TaskRelationList": [],
            "taskDefinitionList": [
                {
                    "code": 8800,
                    "name": "load-orders",
                    "projectCode": 900,
                    "version": 3,
                    "taskType": "SHELL",
                    "taskParams": '{"rawScript":"echo orders"}',
                    "workerGroup": "default",
                    "flag": "YES",
                    "timeoutFlag": "CLOSE",
                }
            ],
        },
    }


def _instance_scalar_operations(
    version: str, wire: JsonObject
) -> RuntimeInstanceOperations:
    adapter = (
        WORKFLOW_PROGRAMS.profile(version)
        .program("instance_get")
        .codec.response_adapter
    )
    assert adapter is not None

    def read(primitive: str, _args: JsonObject) -> object:
        assert primitive == "instance_get"
        return adapter.validate_python(wire)

    return RuntimeInstanceOperations(
        cast("Any", _Programs(version, read)),
        runtime_instances._RECIPE_BY_VERSION[version],
        cast(
            "Any",
            _Definitions(
                ProjectRef(NativeCode(900), "orders", None),
                WorkflowRef(NativeCode(1300), "daily-orders", 2),
            ),
        ),
    )


@pytest.mark.parametrize(
    "version",
    [version for version in runtime_instances._RECIPE_BY_VERSION if version != "1.3.9"],
)
def test_modern_instance_dag_uses_actual_instance_scalar_baseline(version: str) -> None:
    raw = '[{"prop":"env","value":"runtime","type":"VARCHAR","direct":"IN"}]'
    operations = _instance_scalar_operations(
        version, _instance_scalar_wire(version, raw)
    )
    snapshot = operations.get_workflow_instance(
        project_selector="orders", workflow_instance_id=31
    ).instance
    assert snapshot.dagData is not None
    definition = snapshot.dagData.workflowDefinition
    assert definition is not None
    assert definition.timeout == 45
    assert definition.globalParams == raw
    assert definition.globalParamMap == {"env": "runtime"}


@pytest.mark.parametrize("global_params", [None, "", "[]"])
def test_empty_instance_globals_do_not_fall_back_to_definition(
    global_params: str | None,
) -> None:
    operations = _instance_scalar_operations(
        "2.0.0", _instance_scalar_wire("2.0.0", global_params)
    )
    snapshot = operations.get_workflow_instance(
        project_selector="orders", workflow_instance_id=31
    ).instance
    assert snapshot.dagData is not None
    definition = snapshot.dagData.workflowDefinition
    assert definition is not None
    assert definition.globalParams == "[]"
    assert definition.globalParamMap == {}


@pytest.mark.parametrize(
    "global_params",
    [
        "bad-json",
        "null",
        "{}",
        '[{"prop":"env","value":42}]',
        '[{"prop":"env","value":"a"},{"prop":"env","value":"b"}]',
    ],
)
def test_invalid_instance_globals_fail_at_projection_boundary(
    global_params: str,
) -> None:
    operations = _instance_scalar_operations(
        "2.0.0", _instance_scalar_wire("2.0.0", global_params)
    )
    with pytest.raises(ApiTransportError) as error:
        operations.get_workflow_instance(
            project_selector="orders", workflow_instance_id=31
        )
    assert error.value.details["field"] == "globalParams"


@pytest.mark.parametrize("version", ["2.0.0", "2.0.2", "2.0.3", "3.4.1"])
def test_consecutive_instance_scalar_edits_preserve_the_other_runtime_scalar(
    version: str,
) -> None:

    raw = '[{"prop":"env","value":"runtime","type":"VARCHAR","direct":"IN"}]'
    wire = _instance_scalar_wire(version, raw)
    operations = _instance_scalar_operations(version, wire)
    project = ResolvedProject(code=900, name="orders", description=None)
    catalog = get_task_authoring_catalog(version)
    for change in ({"timeout": 60}, {"global_params": {"env": "updated"}}):
        snapshot = operations.get_workflow_instance(
            project_selector="orders", workflow_instance_id=31
        ).instance
        assert snapshot.dagData is not None
        mutation = prepare_workflow_mutation_plan(
            dag=snapshot.dagData,
            project=project,
            patch=WorkflowPatchSpec.model_validate({"workflow": {"set": change}}),
            release_state=None,
            catalog=catalog,
        )
        payload = mutation.compilation.preview()
        definition = snapshot.dagData.workflowDefinition
        assert definition is not None
        request = workflow_instance_update_arguments(
            payload,
            sync_definition=False,
            preserved_global_params=definition.globalParams
            if "timeout" in change
            else None,
        )
        assert request["timeout"] == 60
        if "timeout" in change:
            assert request["global_params"] == raw
        else:
            assert '"updated"' in payload["globalParams"]
        # The native instance-only scalar update leaves its definition DAG intact.
        wire["timeout"] = request["timeout"]
        wire["globalParams"] = request["global_params"]


def test_instance_yaml_export_uses_runtime_scalars() -> None:
    operations = _instance_scalar_operations(
        "2.0.0", _instance_scalar_wire("2.0.0", '[{"prop":"env","value":"runtime"}]')
    )
    snapshot = operations.get_workflow_instance(
        project_selector="orders", workflow_instance_id=31
    ).instance
    assert snapshot.dagData is not None
    document = yaml.safe_load(
        workflow_yaml_document(
            snapshot.dagData,
            project=ResolvedProject(code=900, name="orders", description=None),
            attached_schedule=None,
            catalog=get_task_authoring_catalog("2.0.0"),
        )
    )
    assert document["workflow"]["timeout"] == 45
    assert document["workflow"]["global_params"] == {"env": "runtime"}


@pytest.mark.parametrize("raw", ['[{"prop":"env","value":null}]', '[{"prop":"env"}]'])
def test_nullable_instance_globals_survive_export_and_consecutive_scalar_edits(
    raw: str,
) -> None:
    wire = _instance_scalar_wire("2.0.0", raw)
    operations = _instance_scalar_operations("2.0.0", wire)
    project = ResolvedProject(code=900, name="orders", description=None)
    catalog = get_task_authoring_catalog("2.0.0")
    snapshot = operations.get_workflow_instance(
        project_selector="orders", workflow_instance_id=31
    ).instance
    assert snapshot.dagData is not None
    document = yaml.safe_load(
        workflow_yaml_document(
            snapshot.dagData,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    assert document["workflow"]["global_params"] == {"env": None}
    for change in ({"timeout": 60}, {"global_params": {"env": None, "added": "yes"}}):
        snapshot = operations.get_workflow_instance(
            project_selector="orders", workflow_instance_id=31
        ).instance
        assert snapshot.dagData is not None
        mutation = prepare_workflow_mutation_plan(
            dag=snapshot.dagData,
            project=project,
            patch=WorkflowPatchSpec.model_validate({"workflow": {"set": change}}),
            release_state=None,
            catalog=catalog,
        )
        payload = mutation.compilation.preview()
        definition = snapshot.dagData.workflowDefinition
        assert definition is not None
        request = workflow_instance_update_arguments(
            payload,
            sync_definition=False,
            preserved_global_params=definition.globalParams
            if "timeout" in change
            else None,
        )
        assert request["timeout"] == 60
        if "timeout" in change:
            assert request["global_params"] == raw
        else:
            assert {
                item["prop"]: item.get("value")
                for item in json.loads(payload["globalParams"])
            } == {"env": None, "added": "yes"}
        wire["timeout"] = request["timeout"]
        wire["globalParams"] = request["global_params"]
