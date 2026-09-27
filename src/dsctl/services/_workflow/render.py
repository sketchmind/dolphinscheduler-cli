from __future__ import annotations

from typing import TYPE_CHECKING, NotRequired, TypedDict, cast

import yaml
from pydantic import ValidationError

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.output import require_json_object
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.identity import (
    WorkflowLiveBaseline,
    task_identities_by_name,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    default_task_authoring_catalog,
)
from dsctl.support.yaml_io import (
    JsonObject,
    compact_yaml_mapping,
    dump_yaml_document,
    parse_json_text,
)
from dsctl.upstream.definition_models import schedule_has_missed_fire_policy
from dsctl.upstream.protocols.design import ScheduleMissedFireRecord
from dsctl.upstream.serialization import (
    TaskData,
    enum_value,
    optional_text,
    require_resource_int,
    serialize_task,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskGraphContext,
    TaskRefIndex,
    TaskResourceRefIndex,
    TaskWorkflowRefIndex,
    decode_task_parameters_with_provenance,
)
from dsctl.upstream.task_settings import (
    decode_task_node_fields,
    optional_task_node_fields,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.protocol import (
        ScheduleRecord,
        WorkflowDagRecord,
        WorkflowListRecord,
        WorkflowPayloadRecord,
    )
    from dsctl.upstream.resolver import ResolvedProject


class WorkflowListItem(TypedDict):
    """JSON object emitted for one workflow in `workflow list`."""

    code: int
    name: str | None
    version: int | None
    releaseState: str | None
    scheduleReleaseState: str | None
    scheduleId: int | None


class ScheduleData(TypedDict):
    """JSON object emitted for one attached schedule."""

    id: int | None
    startTime: str | None
    endTime: str | None
    timezoneId: str | None
    crontab: str | None
    failureStrategy: str | None
    workflowInstancePriority: str | None
    releaseState: str | None
    missedFirePolicy: NotRequired[str | None]


class WorkflowData(TypedDict):
    """JSON object emitted for one workflow payload."""

    id: int | None
    code: int
    name: str | None
    version: int | None
    projectCode: int
    description: str | None
    globalParams: str | None
    globalParamMap: dict[str, str | None] | None
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


def serialize_workflow_list_item(workflow: WorkflowListRecord) -> WorkflowListItem:
    """Serialize one workflow list item."""
    schedule = workflow.schedule
    return {
        "code": require_resource_int(
            workflow.code,
            resource=WORKFLOW_RESOURCE,
            field_name="workflow.code",
        ),
        "name": workflow.name,
        "version": workflow.version,
        "releaseState": enum_value(workflow.releaseState),
        "scheduleReleaseState": enum_value(workflow.scheduleReleaseState),
        "scheduleId": None if schedule is None else schedule.id,
    }


def serialize_workflow(
    workflow: WorkflowPayloadRecord,
    *,
    attached_schedule: ScheduleRecord | None,
) -> WorkflowData:
    """Serialize one workflow payload with authoritative attached-schedule state."""
    return {
        "id": workflow.id,
        "code": workflow.code,
        "name": workflow.name,
        "version": workflow.version,
        "projectCode": workflow.projectCode,
        "description": workflow.description,
        "globalParams": workflow.globalParams,
        "globalParamMap": None
        if workflow.globalParamMap is None
        else dict(workflow.globalParamMap),
        "createTime": workflow.createTime,
        "updateTime": workflow.updateTime,
        "userId": workflow.userId,
        "userName": workflow.userName,
        "projectName": workflow.projectName,
        "timeout": workflow.timeout,
        "releaseState": enum_value(workflow.releaseState),
        "scheduleReleaseState": (
            None
            if attached_schedule is None
            else enum_value(attached_schedule.releaseState)
        ),
        "executionType": enum_value(workflow.executionType),
        "schedule": _serialize_schedule(attached_schedule),
    }


def serialize_workflow_dag(
    dag: WorkflowDagRecord,
    *,
    attached_schedule: ScheduleRecord | None,
) -> WorkflowDescribeData:
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
        "workflow": serialize_workflow(
            workflow,
            attached_schedule=attached_schedule,
        ),
        "tasks": tasks,
        "relations": relations,
    }


def workflow_yaml_document(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
    attached_schedule: ScheduleRecord | None,
    catalog: TaskAuthoringCatalog | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
) -> str:
    """Render one DS workflow DAG into the CLI YAML workflow document."""
    yaml_text, _ = _workflow_yaml_document_with_projection_sources(
        dag,
        project=project,
        attached_schedule=attached_schedule,
        catalog=catalog,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    return yaml_text


def _workflow_yaml_document_with_projection_sources(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
    attached_schedule: ScheduleRecord | None,
    catalog: TaskAuthoringCatalog | None,
    resource_refs: TaskResourceRefIndex | None,
    workflow_refs: TaskWorkflowRefIndex | None,
) -> tuple[str, dict[str, ProjectionSource]]:
    """Render YAML and retain each decoded task's semantic re-encode source."""
    description = serialize_workflow_dag(
        dag,
        attached_schedule=attached_schedule,
    )
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
    task_refs = TaskRefIndex(
        code_by_name={name: code for code, name in task_name_by_code.items()},
        name_by_code=task_name_by_code,
    )
    relation_edges = frozenset(
        (relation["preTaskCode"], relation["postTaskCode"])
        for relation in relations
        if relation["preTaskCode"] > 0 and relation["postTaskCode"] > 0
    )
    selected_catalog = default_task_authoring_catalog() if catalog is None else catalog
    authoring_context = workflow_authoring_context(
        catalog=selected_catalog,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    yaml_tasks: list[JsonObject] = []
    projection_sources: dict[str, ProjectionSource] = {}
    for task in tasks:
        task_code = require_resource_int(
            task["code"],
            resource=WORKFLOW_RESOURCE,
            field_name="task.code",
        )
        task_params = parse_json_text(task["taskParams"])
        task_type = str(task["taskType"])
        decoded = decode_task_parameters_with_provenance(
            version=selected_catalog.profile_version,
            task_type=task_type,
            task_params=require_json_object(
                task_params,
                label="workflow task params export",
            ),
            refs=task_refs,
            source=ProjectionSource.OPAQUE_PRESERVE,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
            graph_context=(
                TaskGraphContext(
                    task_code=task_code,
                    relation_edges=relation_edges,
                )
                if task_type.strip().upper() == "BLOCKING"
                else None
            ),
        )
        projected = decoded.task
        exported_task_params = projected.task_params
        exported_task_type = projected.task_type
        _record_task_projection_source(
            projection_sources,
            task_name=task["name"],
            source=decoded.reencode_source,
        )
        task_document: JsonObject = {
            "name": task["name"],
            "type": exported_task_type,
            "description": task["description"],
            "task_params": authoring_context.normalize_task_params(
                exported_task_type,
                cast("YamlObject", exported_task_params),
            ),
            **decode_task_node_fields(task),
            "depends_on": depends_on.get(
                task_code,
                [],
            ),
        }
        task_document.update(optional_task_node_fields(task))
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
    return dump_yaml_document(compact_yaml_mapping(document)), projection_sources


def _record_task_projection_source(
    sources: dict[str, ProjectionSource],
    *,
    task_name: str | None,
    source: ProjectionSource,
) -> None:
    """Record provenance only for a valid serialized task name."""
    name = optional_text(task_name)
    if name is not None:
        sources[name] = source


def workflow_live_baseline(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
    catalog: TaskAuthoringCatalog | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
) -> WorkflowLiveBaseline:
    """Round-trip one live workflow definition without attached schedule state."""
    task_identities = task_identities_by_name(dag)
    try:
        yaml_text, projection_sources = _workflow_yaml_document_with_projection_sources(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        )
        document = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        message = "Workflow export could not be converted back into a spec model"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE},
        ) from exc
    try:
        return WorkflowLiveBaseline(
            spec=validate_workflow_document(
                document,
                authoring_context=workflow_authoring_context(
                    catalog=catalog,
                    intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
                ),
            ),
            task_identities=task_identities,
            projection_sources=projection_sources,
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
    data: ScheduleData = {
        "id": schedule.id,
        "startTime": schedule.startTime,
        "endTime": schedule.endTime,
        "timezoneId": schedule.timezoneId,
        "crontab": schedule.crontab,
        "failureStrategy": enum_value(schedule.failureStrategy),
        "workflowInstancePriority": enum_value(schedule.workflowInstancePriority),
        "releaseState": enum_value(schedule.releaseState),
    }
    if schedule_has_missed_fire_policy(schedule) and isinstance(
        schedule, ScheduleMissedFireRecord
    ):
        data["missedFirePolicy"] = enum_value(schedule.missedFirePolicy)
    return data


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
            "missed_fire_policy": schedule.get("missedFirePolicy"),
            "release_state": schedule["releaseState"],
        }
    )


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
