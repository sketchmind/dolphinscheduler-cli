"""In-memory definitions collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)
from tests.fakes.values import (
    _json_array,
    _optional_enum,
    _optional_json_value,
    _optional_string,
    _require_int,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonValue
    from tests.fakes.schedules import (
        FakeSchedule,
    )


@dataclass(frozen=True)
class FakeWorkflow:
    code: int
    name: str | None
    version: int | None = 1
    project_code_value: int = 0
    description: str | None = None
    global_params_value: str | None = None
    global_param_map_value: dict[str, str] | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    project_name_value: str | None = None
    timeout: int = 0
    release_state_value: FakeEnumValue | None = None
    schedule_release_state_value: FakeEnumValue | None = None
    execution_type_value: FakeEnumValue | None = None
    schedule_value: FakeSchedule | None = None
    id: int | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def globalParams(self) -> str | None:  # noqa: N802
        return self.global_params_value

    @property
    def globalParamMap(self) -> dict[str, str] | None:  # noqa: N802
        return self.global_param_map_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def userId(self) -> int:  # noqa: N802
        return self.user_id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def releaseState(self) -> FakeEnumValue | None:  # noqa: N802
        return self.release_state_value

    @property
    def scheduleReleaseState(self) -> FakeEnumValue | None:  # noqa: N802
        return self.schedule_release_state_value

    @property
    def executionType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.execution_type_value

    @property
    def schedule(self) -> FakeSchedule | None:
        return self.schedule_value


@dataclass(frozen=True)
class FakeWorkflowPage(_FakePage[FakeWorkflow]):
    pass


@dataclass(frozen=True)
class FakeTaskDefinition:
    code: int
    name: str | None
    version: int | None = 1
    project_code_value: int = 0
    description: str | None = None
    task_type_value: str | None = None
    task_params_value: JsonValue | None = None
    user_name_value: str | None = None
    project_name_value: str | None = None
    worker_group_value: str | None = None
    fail_retry_times_value: int = 0
    fail_retry_interval_value: int = 0
    timeout: int = 0
    delay_time_value: int = 0
    resource_ids_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    modify_by_value: str | None = None
    task_group_id_value: int = 0
    task_group_priority_value: int = 0
    environment_code_value: int = 0
    task_priority_value: FakeEnumValue | None = None
    timeout_flag_value: FakeEnumValue | None = None
    timeout_notify_strategy_value: FakeEnumValue | None = None
    task_execute_type_value: FakeEnumValue | None = None
    flag_value: FakeEnumValue | None = None
    is_cache_value: FakeEnumValue | None = None
    cpu_quota_value: int | None = None
    memory_max_value: int | None = None
    id: int | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def taskType(self) -> str | None:  # noqa: N802
        return self.task_type_value

    @property
    def taskParams(self) -> JsonValue | None:  # noqa: N802
        return self.task_params_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def failRetryTimes(self) -> int:  # noqa: N802
        return self.fail_retry_times_value

    @property
    def failRetryInterval(self) -> int:  # noqa: N802
        return self.fail_retry_interval_value

    @property
    def delayTime(self) -> int:  # noqa: N802
        return self.delay_time_value

    @property
    def resourceIds(self) -> str | None:  # noqa: N802
        return self.resource_ids_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def modifyBy(self) -> str | None:  # noqa: N802
        return self.modify_by_value

    @property
    def taskGroupId(self) -> int:  # noqa: N802
        return self.task_group_id_value

    @property
    def taskGroupPriority(self) -> int:  # noqa: N802
        return self.task_group_priority_value

    @property
    def environmentCode(self) -> int:  # noqa: N802
        return self.environment_code_value

    @property
    def taskPriority(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_priority_value

    @property
    def timeoutFlag(self) -> FakeEnumValue | None:  # noqa: N802
        return self.timeout_flag_value

    @property
    def timeoutNotifyStrategy(self) -> FakeEnumValue | None:  # noqa: N802
        return self.timeout_notify_strategy_value

    @property
    def taskExecuteType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_execute_type_value

    @property
    def flag(self) -> FakeEnumValue | None:
        return self.flag_value

    @property
    def isCache(self) -> FakeEnumValue | None:  # noqa: N802
        return self.is_cache_value

    @property
    def cpuQuota(self) -> int | None:  # noqa: N802
        return self.cpu_quota_value

    @property
    def memoryMax(self) -> int | None:  # noqa: N802
        return self.memory_max_value


@dataclass(frozen=True)
class FakeWorkflowTaskRelation:
    pre_task_code_value: int
    post_task_code_value: int
    pre_task_version_value: int = 0
    post_task_version_value: int = 0
    condition_params_value: JsonValue | None = None

    @property
    def preTaskCode(self) -> int:  # noqa: N802
        return self.pre_task_code_value

    @property
    def postTaskCode(self) -> int:  # noqa: N802
        return self.post_task_code_value

    @property
    def preTaskVersion(self) -> int:  # noqa: N802
        return self.pre_task_version_value

    @property
    def postTaskVersion(self) -> int:  # noqa: N802
        return self.post_task_version_value

    @property
    def conditionParams(self) -> JsonValue | None:  # noqa: N802
        return self.condition_params_value


@dataclass(frozen=True)
class FakeDag:
    workflow_definition_value: FakeWorkflow | None
    task_definition_list_value: list[FakeTaskDefinition] | None
    workflow_task_relation_list_value: list[FakeWorkflowTaskRelation] | None

    @property
    def workflowDefinition(self) -> FakeWorkflow | None:  # noqa: N802
        return self.workflow_definition_value

    @property
    def taskDefinitionList(self) -> list[FakeTaskDefinition] | None:  # noqa: N802
        return self.task_definition_list_value

    @property
    def workflowTaskRelationList(self) -> list[FakeWorkflowTaskRelation] | None:  # noqa: N802
        return self.workflow_task_relation_list_value


def _fake_task_definition_from_payload(
    payload: dict[str, object],
    *,
    project_code: int,
) -> FakeTaskDefinition:
    task_priority = payload.get("taskPriority")
    timeout_flag = payload.get("timeoutFlag")
    task_execute_type = payload.get("taskExecuteType")
    return FakeTaskDefinition(
        code=_require_int(payload["code"]),
        name=str(payload["name"]),
        version=_require_int(payload.get("version", 1)),
        project_code_value=project_code,
        description=_optional_string(payload.get("description")),
        task_type_value=_optional_string(payload.get("taskType")),
        task_params_value=_optional_json_value(payload.get("taskParams")),
        worker_group_value=_optional_string(payload.get("workerGroup")),
        fail_retry_times_value=_require_int(payload.get("failRetryTimes", 0)),
        fail_retry_interval_value=_require_int(payload.get("failRetryInterval", 0)),
        timeout=_require_int(payload.get("timeout", 0)),
        delay_time_value=_require_int(payload.get("delayTime", 0)),
        resource_ids_value=_optional_string(payload.get("resourceIds")),
        environment_code_value=_require_int(payload.get("environmentCode", 0)),
        task_priority_value=_optional_enum(task_priority),
        timeout_flag_value=_optional_enum(timeout_flag),
        task_execute_type_value=_optional_enum(task_execute_type),
        is_cache_value=_optional_enum(payload.get("isCache")),
    )


def _fake_task_relation_from_payload(
    payload: dict[str, object],
) -> FakeWorkflowTaskRelation:
    return FakeWorkflowTaskRelation(
        pre_task_code_value=_require_int(payload["preTaskCode"]),
        post_task_code_value=_require_int(payload["postTaskCode"]),
        pre_task_version_value=_require_int(payload.get("preTaskVersion", 0)),
        post_task_version_value=_require_int(payload.get("postTaskVersion", 0)),
        condition_params_value=_optional_json_value(payload.get("conditionParams")),
    )


def _normalized_task_definition_state(task: FakeTaskDefinition) -> dict[str, object]:
    timeout = task.timeout
    task_priority = None if task.taskPriority is None else task.taskPriority.value
    timeout_flag = None if task.timeoutFlag is None else task.timeoutFlag.value
    timeout_notify_strategy = (
        None if task.timeoutNotifyStrategy is None else task.timeoutNotifyStrategy.value
    )
    task_execute_type = (
        None if task.taskExecuteType is None else task.taskExecuteType.value
    )
    flag = None if task.flag is None else task.flag.value
    return {
        "name": task.name,
        "description": "" if task.description is None else task.description,
        "taskType": task.taskType,
        "taskParams": task.taskParams,
        "workerGroup": "default" if task.workerGroup is None else task.workerGroup,
        "environmentCode": -1 if task.environmentCode <= 0 else task.environmentCode,
        "failRetryTimes": task.failRetryTimes,
        "failRetryInterval": task.failRetryInterval,
        "timeout": timeout,
        "timeoutFlag": (
            "OPEN"
            if timeout > 0 and timeout_flag is None
            else "CLOSE"
            if timeout <= 0 and timeout_flag is None
            else timeout_flag
        ),
        "timeoutNotifyStrategy": (
            "WARN"
            if timeout > 0 and timeout_notify_strategy is None
            else None
            if timeout <= 0 and timeout_notify_strategy is None
            else timeout_notify_strategy
        ),
        "delayTime": task.delayTime,
        "resourceIds": "" if task.resourceIds is None else task.resourceIds,
        "taskPriority": "MEDIUM" if task_priority is None else task_priority,
        "taskExecuteType": "BATCH" if task_execute_type is None else task_execute_type,
        "flag": "YES" if flag is None else flag,
        "taskGroupId": task.taskGroupId,
        "taskGroupPriority": task.taskGroupPriority,
        "cpuQuota": -1 if task.cpuQuota is None else task.cpuQuota,
        "memoryMax": -1 if task.memoryMax is None else task.memoryMax,
    }


def _updated_fake_workflow_tasks_and_relations(
    *,
    dags: Mapping[int, FakeDag],
    workflow_code: int,
    project_code: int,
    task_definition_json: str,
    task_relation_json: str,
) -> tuple[list[FakeTaskDefinition], list[FakeWorkflowTaskRelation]]:
    current_dag = dags.get(workflow_code)
    current_tasks = (
        [] if current_dag is None else list(current_dag.taskDefinitionList or [])
    )
    current_tasks_by_code = {task.code: task for task in current_tasks}
    task_definitions: list[FakeTaskDefinition] = []
    effective_versions_by_code: dict[int, int] = {}
    for item in _json_array(task_definition_json, label="task_definition_json"):
        candidate = _fake_task_definition_from_payload(
            item,
            project_code=project_code,
        )
        current_task = current_tasks_by_code.get(candidate.code)
        if current_task is None:
            task_definitions.append(candidate)
            effective_versions_by_code[candidate.code] = candidate.version or 1
            continue
        changed = _normalized_task_definition_state(current_task) != (
            _normalized_task_definition_state(candidate)
        )
        current_version = current_task.version or 1
        effective_version = current_version + 1 if changed else current_version
        updated_task = replace(candidate, version=effective_version)
        task_definitions.append(updated_task)
        effective_versions_by_code[updated_task.code] = effective_version
    task_relations = _updated_fake_workflow_relations(
        task_relation_json,
        effective_versions_by_code=effective_versions_by_code,
    )
    return task_definitions, task_relations


def _updated_fake_workflow_relations(
    task_relation_json: str,
    *,
    effective_versions_by_code: Mapping[int, int],
) -> list[FakeWorkflowTaskRelation]:
    task_relations: list[FakeWorkflowTaskRelation] = []
    for item in _json_array(task_relation_json, label="task_relation_json"):
        relation_payload = dict(item)
        pre_task_code = _require_int(relation_payload["preTaskCode"])
        post_task_code = _require_int(relation_payload["postTaskCode"])
        if pre_task_code != 0 and pre_task_code in effective_versions_by_code:
            relation_payload["preTaskVersion"] = effective_versions_by_code[
                pre_task_code
            ]
        if post_task_code in effective_versions_by_code:
            relation_payload["postTaskVersion"] = effective_versions_by_code[
                post_task_code
            ]
        task_relations.append(_fake_task_relation_from_payload(relation_payload))
    return task_relations
