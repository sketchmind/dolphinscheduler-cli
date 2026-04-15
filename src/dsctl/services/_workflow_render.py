from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

import yaml
from pydantic import ValidationError

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.models.workflow_spec import WorkflowSpec
from dsctl.output import require_json_object, require_json_value
from dsctl.services._serialization import (
    TaskData,
    enum_value,
    optional_text,
    require_resource_int,
    serialize_task,
)
from dsctl.services._workflow_identity import (
    WorkflowLiveBaseline,
    task_identities_by_name,
)
from dsctl.support.yaml_io import (
    JsonObject,
    JsonValue,
    compact_yaml_mapping,
    dump_yaml_document,
    parse_json_text,
)

if TYPE_CHECKING:
    from dsctl.services.resolver import ResolvedProject
    from dsctl.upstream.protocol import (
        ScheduleRecord,
        WorkflowDagRecord,
        WorkflowPayloadRecord,
        WorkflowRecord,
    )


class WorkflowListItem(TypedDict):
    """JSON object emitted for one workflow in `workflow list`."""

    code: int
    name: str | None
    version: int | None


class ScheduleData(TypedDict):
    """JSON object emitted for one attached schedule."""

    startTime: str | None
    endTime: str | None
    timezoneId: str | None
    crontab: str | None
    failureStrategy: str | None
    workflowInstancePriority: str | None
    releaseState: str | None


class WorkflowData(TypedDict):
    """JSON object emitted for one workflow payload."""

    id: int | None
    code: int
    name: str | None
    version: int | None
    projectCode: int
    description: str | None
    globalParams: str | None
    globalParamMap: dict[str, str] | None
    createTime: str | None
    updateTime: str | None
    userId: int
    userName: str | None
    projectName: str | None
    timeout: int
    releaseState: str | None
    scheduleReleaseState: str | None
    executionType: str | None
    schedule: ScheduleData | None


class WorkflowRelationData(TypedDict):
    """JSON object emitted for one workflow task relation."""

    preTaskCode: int
    preTaskName: str | None
    postTaskCode: int
    postTaskName: str | None


class WorkflowDescribeData(TypedDict):
    """Rich workflow describe payload."""

    workflow: WorkflowData
    tasks: list[TaskData]
    relations: list[WorkflowRelationData]


def serialize_workflow_ref(workflow: WorkflowRecord) -> WorkflowListItem:
    """Serialize one workflow list item."""
    return {
        "code": require_resource_int(
            workflow.code,
            resource=WORKFLOW_RESOURCE,
            field_name="workflow.code",
        ),
        "name": workflow.name,
        "version": workflow.version,
    }


def serialize_workflow(workflow: WorkflowPayloadRecord) -> WorkflowData:
    """Serialize one workflow payload."""
    return {
        "id": workflow.id,
        "code": workflow.code,
        "name": workflow.name,
        "version": workflow.version,
        "projectCode": workflow.projectCode,
        "description": workflow.description,
        "globalParams": workflow.globalParams,
        "globalParamMap": workflow.globalParamMap,
        "createTime": workflow.createTime,
        "updateTime": workflow.updateTime,
        "userId": workflow.userId,
        "userName": workflow.userName,
        "projectName": workflow.projectName,
        "timeout": workflow.timeout,
        "releaseState": enum_value(workflow.releaseState),
        "scheduleReleaseState": enum_value(workflow.scheduleReleaseState),
        "executionType": enum_value(workflow.executionType),
        "schedule": _serialize_schedule(workflow.schedule),
    }


def serialize_workflow_dag(dag: WorkflowDagRecord) -> WorkflowDescribeData:
    """Serialize one workflow DAG payload with expanded task relations."""
    workflow = dag.workflowDefinition
    if workflow is None:
        message = "Workflow DAG payload was missing workflowDefinition"
        raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
    tasks = [serialize_task(task) for task in dag.taskDefinitionList or []]
    task_names = {task["code"]: task["name"] for task in tasks}
    relations: list[WorkflowRelationData] = [
        {
            "preTaskCode": relation.preTaskCode,
            "preTaskName": task_names.get(relation.preTaskCode),
            "postTaskCode": relation.postTaskCode,
            "postTaskName": task_names.get(relation.postTaskCode),
        }
        for relation in dag.workflowTaskRelationList or []
    ]
    return {
        "workflow": serialize_workflow(workflow),
        "tasks": tasks,
        "relations": relations,
    }


def workflow_yaml_document(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
) -> str:
    """Render one DS workflow DAG into the CLI YAML workflow document."""
    description = serialize_workflow_dag(dag)
    workflow_data = description["workflow"]
    tasks = description["tasks"]
    relations = description["relations"]
    depends_on = _task_dependencies(relations)
    task_name_by_code = {
        require_resource_int(
            task["code"],
            resource=WORKFLOW_RESOURCE,
            field_name="task.code",
        ): str(task["name"])
        for task in tasks
        if task["name"] is not None
    }
    yaml_tasks: list[JsonObject] = []
    for task in tasks:
        task_params = parse_json_text(task["taskParams"])
        task_document: JsonObject = {
            "name": task["name"],
            "type": task["taskType"],
            "description": task["description"],
            "task_params": _yaml_task_params_document(
                task_type=str(task["taskType"]),
                payload=task_params,
                task_name_by_code=task_name_by_code,
            ),
            "worker_group": task["workerGroup"],
            "priority": task["taskPriority"],
            "retry": {
                "times": task["failRetryTimes"],
                "interval": task["failRetryInterval"],
            },
            "timeout": task["timeout"],
            "delay": task["delayTime"],
            "depends_on": depends_on.get(
                require_resource_int(
                    task["code"],
                    resource=WORKFLOW_RESOURCE,
                    field_name="task.code",
                ),
                [],
            ),
        }
        flag = optional_text(task.get("flag"))
        if flag is not None and flag != "YES":
            task_document["flag"] = flag
        environment_code = task.get("environmentCode")
        if isinstance(environment_code, int) and environment_code > 0:
            task_document["environment_code"] = environment_code
        timeout_notify_strategy = optional_text(task.get("timeoutNotifyStrategy"))
        if (
            isinstance(task["timeout"], int)
            and task["timeout"] > 0
            and timeout_notify_strategy is not None
            and timeout_notify_strategy != "WARN"
        ):
            task_document["timeout_notify_strategy"] = timeout_notify_strategy
        cpu_quota = task.get("cpuQuota")
        if isinstance(cpu_quota, int) and cpu_quota != -1:
            task_document["cpu_quota"] = cpu_quota
        memory_max = task.get("memoryMax")
        if isinstance(memory_max, int) and memory_max != -1:
            task_document["memory_max"] = memory_max
        task_group_id = task.get("taskGroupId")
        if isinstance(task_group_id, int) and task_group_id > 0:
            task_document["task_group_id"] = task_group_id
            task_group_priority = task.get("taskGroupPriority")
            if isinstance(task_group_priority, int):
                task_document["task_group_priority"] = task_group_priority
        yaml_tasks.append(compact_yaml_mapping(task_document))

    document: JsonObject = {
        "workflow": compact_yaml_mapping(
            {
                "name": workflow_data["name"],
                "project": project.name,
                "description": workflow_data["description"],
                "timeout": workflow_data["timeout"],
                "global_params": workflow_data["globalParamMap"],
                "execution_type": workflow_data["executionType"],
                "release_state": workflow_data["releaseState"],
            }
        ),
        "tasks": yaml_tasks,
    }
    schedule_document = _workflow_yaml_schedule_document(workflow_data["schedule"])
    if schedule_document is not None:
        document["schedule"] = schedule_document
    return dump_yaml_document(compact_yaml_mapping(document))


def workflow_live_baseline(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
) -> WorkflowLiveBaseline:
    """Round-trip one live workflow DAG into authoring spec plus task identity."""
    task_identities = task_identities_by_name(dag)
    try:
        document = yaml.safe_load(
            workflow_yaml_document(
                dag,
                project=project,
            )
        )
    except yaml.YAMLError as exc:
        message = "Workflow export could not be converted back into a spec model"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE},
        ) from exc
    try:
        return WorkflowLiveBaseline(
            spec=WorkflowSpec.model_validate(document),
            task_identities=task_identities,
        )
    except ValidationError as exc:
        message = "Workflow export did not round-trip back into a valid workflow spec"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE},
        ) from exc


def _serialize_schedule(schedule: ScheduleRecord | None) -> ScheduleData | None:
    if schedule is None:
        return None
    return {
        "startTime": schedule.startTime,
        "endTime": schedule.endTime,
        "timezoneId": schedule.timezoneId,
        "crontab": schedule.crontab,
        "failureStrategy": enum_value(schedule.failureStrategy),
        "workflowInstancePriority": enum_value(schedule.workflowInstancePriority),
        "releaseState": enum_value(schedule.releaseState),
    }


def _workflow_yaml_schedule_document(
    schedule: ScheduleData | None,
) -> JsonObject | None:
    if schedule is None:
        return None
    cron = optional_text(schedule["crontab"])
    timezone = optional_text(schedule["timezoneId"])
    start = optional_text(schedule["startTime"])
    end = optional_text(schedule["endTime"])
    if cron is None or timezone is None or start is None or end is None:
        return None
    return compact_yaml_mapping(
        {
            "cron": cron,
            "timezone": timezone,
            "start": start,
            "end": end,
            "failure_strategy": schedule["failureStrategy"],
            "priority": schedule["workflowInstancePriority"],
            "release_state": schedule["releaseState"],
        }
    )


def _yaml_task_params_document(
    *,
    task_type: str,
    payload: object,
    task_name_by_code: Mapping[int, str],
) -> JsonObject:
    task_params = require_json_object(payload, label="workflow task params export")
    normalized_task_type = task_type.upper()
    if normalized_task_type == "SWITCH":
        return _yaml_switch_task_params_document(
            payload=task_params,
            task_name_by_code=task_name_by_code,
        )
    if normalized_task_type == "CONDITIONS":
        return _yaml_conditions_task_params_document(
            payload=task_params,
            task_name_by_code=task_name_by_code,
        )
    return task_params


def _yaml_switch_task_params_document(
    *,
    payload: JsonObject,
    task_name_by_code: Mapping[int, str],
) -> JsonObject:
    exported: JsonObject = dict(payload)
    switch_result = payload.get("switchResult")
    if isinstance(switch_result, Mapping):
        exported_switch_result: JsonObject = dict(switch_result)
        depend_task_list = switch_result.get("dependTaskList")
        if isinstance(depend_task_list, list):
            exported_depend_task_list: list[JsonObject] = []
            for branch in depend_task_list:
                if not isinstance(branch, Mapping):
                    continue
                exported_branch: JsonObject = dict(branch)
                next_node = branch.get("nextNode")
                if next_node is not None:
                    exported_branch["nextNode"] = _yaml_task_name_ref(
                        next_node,
                        task_name_by_code=task_name_by_code,
                    )
                exported_depend_task_list.append(exported_branch)
            exported_switch_result["dependTaskList"] = exported_depend_task_list
        default_next_node = switch_result.get("nextNode")
        if default_next_node is not None:
            exported_switch_result["nextNode"] = _yaml_task_name_ref(
                default_next_node,
                task_name_by_code=task_name_by_code,
            )
        exported["switchResult"] = exported_switch_result
    next_branch = payload.get("nextBranch")
    if next_branch is not None:
        exported["nextBranch"] = _yaml_task_name_ref(
            next_branch,
            task_name_by_code=task_name_by_code,
        )
    return exported


def _yaml_conditions_task_params_document(
    *,
    payload: JsonObject,
    task_name_by_code: Mapping[int, str],
) -> JsonObject:
    exported: JsonObject = dict(payload)
    condition_result = payload.get("conditionResult")
    if isinstance(condition_result, Mapping):
        exported_condition_result: JsonObject = dict(condition_result)
        for key in ("successNode", "failedNode"):
            nodes = condition_result.get(key)
            if isinstance(nodes, list):
                exported_condition_result[key] = [
                    _yaml_task_name_ref(node, task_name_by_code=task_name_by_code)
                    for node in nodes
                ]
        exported["conditionResult"] = exported_condition_result
    return exported


def _yaml_task_name_ref(
    value: object,
    *,
    task_name_by_code: Mapping[int, str],
) -> JsonValue:
    if isinstance(value, int):
        return task_name_by_code.get(value, value)
    return require_json_value(value, label="workflow task reference export")


def _task_dependencies(
    relations: list[WorkflowRelationData],
) -> dict[int, list[str]]:
    dependencies: dict[int, list[str]] = {}
    for relation in relations:
        post_task_name = relation["postTaskName"]
        pre_task_name = relation["preTaskName"]
        # DolphinScheduler uses preTaskCode == 0 for synthetic root edges.
        # Those edges never map to a real upstream task name, so they are
        # intentionally skipped from YAML `depends_on` output.
        if post_task_name is None or pre_task_name is None:
            continue
        dependencies.setdefault(relation["postTaskCode"], []).append(pre_task_name)
    return dependencies
