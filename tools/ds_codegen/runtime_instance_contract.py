"""Reviewed exact-version recipes for workflow and task runtime instances.

The instance plane is deliberately bound to the project-scoped controllers in
every supported release.  DolphinScheduler's short-lived V2 instance
controllers (3.2.0--3.4.1) are not part of this contract: 3.4.2 removed them,
while the project-scoped routes retain the same user-visible semantics and
permission boundary across the complete support window.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, NotRequired, TypedDict, cast

from ds_codegen.profile_ledger import select_exact_versions

TARGET_RUNTIME_INSTANCE_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "2.0.9",
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

STOP_RESULT_MAY_BE_UNKNOWN_BY_VERSION = {
    version: version
    in {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
    for version in TARGET_RUNTIME_INSTANCE_VERSIONS
}

WORKFLOW_INSTANCE_SEMANTIC_OPERATIONS = (
    "workflow-instance.list",
    "workflow-instance.get",
    "workflow-instance.export",
    "workflow-instance.parent",
    "workflow-instance.digest",
    "workflow-instance.edit",
    "workflow-instance.watch",
    "workflow-instance.stop",
    "workflow-instance.rerun",
    "workflow-instance.recover-failed",
    "workflow-instance.execute-task",
)
TASK_INSTANCE_SEMANTIC_OPERATIONS = (
    "task-instance.list",
    "task-instance.get",
    "task-instance.watch",
    "task-instance.sub-workflow",
    "task-instance.log",
    "task-instance.force-success",
    "task-instance.savepoint",
    "task-instance.stop",
)
RUNTIME_INSTANCE_SEMANTIC_OPERATIONS = (
    *WORKFLOW_INSTANCE_SEMANTIC_OPERATIONS,
    *TASK_INSTANCE_SEMANTIC_OPERATIONS,
)
RUNTIME_INSTANCE_PROFILE_SCHEMA_VERSION = 5
GENERATED_RUNTIME_INSTANCE_PROFILE_PATH = Path("generated/runtime_instance_profiles.py")
SCHEDULE_ENVIRONMENT_FACTS_PATH = Path(__file__).with_name(
    "schedule_environment_facts.json"
)

Support = Literal["supported", "limited", "absent"]
ProjectIdentity = Literal["id", "code"]
ProjectRoute = Literal["name", "code"]
DefinitionIdentity = Literal["id", "code"]
UpdateShape = Literal["legacy-process-data", "modern-with-tenant", "modern"]
TaskPageShape = Literal["entity", "map", "entity-or-map"]
TaskLogWireEpoch = Literal[
    "query-log-string",
    "log-detail-string",
    "log-detail-record",
    "query-log-record",
]
TaskLogShape = Literal["string", "response-task-log"]
SubResultKey = Literal["subProcessInstanceId", "subWorkflowInstanceId"]
EvidenceKind = Literal["controller", "ui", "snapshot"]
TerminalReason = Literal[
    "upstream_capability_absent",
    "upstream_capability_limited",
]


@dataclass(frozen=True)
class Evidence:
    """One exact-version source coordinate supporting a reviewed decision."""

    version: str
    kind: EvidenceKind
    source: str
    symbol: str
    conclusion: str

    @property
    def reference(self) -> str:
        """Return the source reference serialized into generated profiles."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class TaskLogWireEpochContract:
    """One named task-log wire epoch and its exact version membership."""

    members: tuple[str, ...]
    source_operation: str
    response_shape: TaskLogShape


@dataclass(frozen=True)
class RuntimeInstanceRecipe:
    """Wire and result decisions for one exact instance-controller family."""

    project_identity: ProjectIdentity
    project_route: ProjectRoute
    definition_identity: DefinitionIdentity
    workflow_controller: str
    workflow_page_operation: str
    workflow_get_operation: str
    workflow_parent_operation: str
    workflow_sub_operation: str
    workflow_sub_result_key: SubResultKey
    workflow_update_operation: str
    workflow_get_query_params: bool
    workflow_model: str
    workflow_definition_model: str
    workflow_state_enum: str
    update_shape: UpdateShape
    task_page_shape: TaskPageShape
    task_state_enum: str
    task_execute_type_enum: str | None
    task_log_wire_epoch: TaskLogWireEpoch
    log_first_page_header: bool
    control_operation: str
    execute_task: bool
    force_success: bool
    savepoint: bool
    stop_task: bool
    task_code_filter: bool
    task_execute_type_filter: bool
    task_workflow_name_filter: bool
    task_definition_name_filter: bool
    other_workflow_filters: bool
    stop_result_may_be_unknown: bool
    instance_dag_edit_requires_sync: bool
    schedule_forwards_environment: bool = False
    task_inherits_workflow_environment: bool = False
    workflow_summary_model: str | None = None

    @property
    def log_operation(self) -> str:
        """Return the source operation compiled by the named log epoch."""
        return TASK_LOG_WIRE_EPOCH_CONTRACTS[self.task_log_wire_epoch].source_operation

    @property
    def log_shape(self) -> TaskLogShape:
        """Return the response shape compiled by the named log epoch."""
        return TASK_LOG_WIRE_EPOCH_CONTRACTS[self.task_log_wire_epoch].response_shape


@dataclass(frozen=True)
class TerminalDecision:
    """One stable action proven unavailable or limited upstream."""

    semantic_operation: str
    reason: TerminalReason
    constraint: str
    evidence: Evidence


_PROJECT_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ProjectController.java"
)
_PROCESS_INSTANCE_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ProcessInstanceController.java"
)
_WORKFLOW_INSTANCE_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/WorkflowInstanceController.java"
)
_TASK_INSTANCE_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/TaskInstanceController.java"
)
_EXECUTOR_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ExecutorController.java"
)
_LOGGER_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/LoggerController.java"
)
_TASK_DEFINITION_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/TaskDefinitionController.java"
)
_PROCESS_DEFINITION_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ProcessDefinitionController.java"
)
_WORKFLOW_DEFINITION_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/WorkflowDefinitionController.java"
)
_LEGACY_DAG_UI = "dolphinscheduler-ui/src/js/conf/home/store/dag/actions.js"
_LEGACY_INSTANCE_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/instance/"
    "pages/list/_source/list.vue"
)
_PROCESS_INSTANCE_UI = (
    "dolphinscheduler-ui/src/service/modules/process-instances/index.ts"
)
_WORKFLOW_INSTANCE_UI = (
    "dolphinscheduler-ui/src/service/modules/workflow-instances/index.ts"
)
_TASK_INSTANCE_UI = "dolphinscheduler-ui/src/service/modules/task-instances/index.ts"

_PAGE_INFO_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"
_RESULT_MODEL = "org.apache.dolphinscheduler.api.utils.Result"
_PROJECT_MODEL = "org.apache.dolphinscheduler.dao.entity.Project"
_PROCESS_INSTANCE_MODEL = "org.apache.dolphinscheduler.dao.entity.ProcessInstance"
_WORKFLOW_INSTANCE_MODEL = "org.apache.dolphinscheduler.dao.entity.WorkflowInstance"
_TASK_INSTANCE_MODEL = "org.apache.dolphinscheduler.dao.entity.TaskInstance"
_PROCESS_DEFINITION_MODEL = "org.apache.dolphinscheduler.dao.entity.ProcessDefinition"
_WORKFLOW_DEFINITION_MODEL = "org.apache.dolphinscheduler.dao.entity.WorkflowDefinition"
_RESPONSE_TASK_LOG_MODEL = "org.apache.dolphinscheduler.dao.entity.ResponseTaskLog"
_EXECUTE_TYPE_ENUM = "org.apache.dolphinscheduler.api.enums.ExecuteType"
_TASK_DEPEND_TYPE_ENUM = "org.apache.dolphinscheduler.common.enums.TaskDependType"
_LEGACY_EXECUTION_STATUS_ENUM = (
    "org.apache.dolphinscheduler.common.enums.ExecutionStatus"
)
_PLUGIN_EXECUTION_STATUS_ENUM = (
    "org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus"
)
_WORKFLOW_EXECUTION_STATUS_ENUM = (
    "org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus"
)
_TASK_EXECUTION_STATUS_ENUM = (
    "org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus"
)
_TASK_EXECUTE_TYPE_ENUM = "org.apache.dolphinscheduler.common.enums.TaskExecuteType"

TASK_LOG_WIRE_EPOCH_CONTRACTS: dict[TaskLogWireEpoch, TaskLogWireEpochContract] = {
    "query-log-string": TaskLogWireEpochContract(
        members=("1.3.9", "2.0.0", "2.0.1"),
        source_operation="LoggerController.queryLog",
        response_shape="string",
    ),
    "log-detail-string": TaskLogWireEpochContract(
        members=(
            "2.0.2",
            "2.0.3",
            "2.0.4",
            "2.0.5",
            "2.0.6",
            "2.0.7",
            "2.0.8",
            "2.0.9",
        ),
        source_operation="LoggerController.queryLog__get_log_detail",
        response_shape="string",
    ),
    "log-detail-record": TaskLogWireEpochContract(
        members=(
            "3.0.0",
            "3.0.1",
            "3.0.2",
            "3.0.3",
            "3.0.4",
            "3.0.5",
            "3.0.6",
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.8",
            "3.1.9",
            "3.2.0",
            "3.2.1",
            "3.2.2",
            "3.3.1",
            "3.3.2",
            "3.4.0",
            "3.4.1",
        ),
        source_operation="LoggerController.queryLog__get_log_detail",
        response_shape="response-task-log",
    ),
    "query-log-record": TaskLogWireEpochContract(
        members=("3.4.2", "3.4.3"),
        source_operation="LoggerController.queryLog",
        response_shape="response-task-log",
    ),
}

TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION = {
    "1.3.9": False,
    "2.0.0": True,
    "2.0.1": True,
    "2.0.2": True,
    "2.0.3": True,
    "2.0.4": True,
    "2.0.5": True,
    "2.0.6": True,
    "2.0.7": True,
    "2.0.8": True,
    "2.0.9": True,
    "3.0.0": True,
    "3.0.1": True,
    "3.0.2": True,
    "3.0.3": True,
    "3.0.4": True,
    "3.0.5": True,
    "3.0.6": True,
    "3.1.0": True,
    "3.1.1": True,
    "3.1.2": True,
    "3.1.3": True,
    "3.1.4": True,
    "3.1.5": True,
    "3.1.6": True,
    "3.1.7": True,
    "3.1.8": True,
    "3.1.9": True,
    "3.2.0": True,
    "3.2.1": True,
    "3.2.2": True,
    "3.3.1": True,
    "3.3.2": True,
    "3.4.0": True,
    "3.4.1": True,
    "3.4.2": True,
    "3.4.3": True,
}


def _task_log_wire_epoch(version: str) -> TaskLogWireEpoch:
    matches = tuple(
        epoch
        for epoch, contract in TASK_LOG_WIRE_EPOCH_CONTRACTS.items()
        if version in contract.members
    )
    if len(matches) != 1:
        message = f"DS {version} must belong to exactly one task-log epoch"
        raise ValueError(message)
    return matches[0]


def _recipe(
    *,
    project_identity: ProjectIdentity,
    project_route: ProjectRoute,
    definition_identity: DefinitionIdentity,
    modern_names: bool,
    update_shape: UpdateShape,
    workflow_state_enum: str,
    task_state_enum: str,
    task_execute_type_enum: str | None,
    task_page_shape: TaskPageShape,
    task_log_wire_epoch: TaskLogWireEpoch,
    log_first_page_header: bool,
    control_operation: str,
    execute_task: bool,
    force_success: bool,
    savepoint: bool,
    stop_task: bool,
    stop_result_may_be_unknown: bool = False,
) -> RuntimeInstanceRecipe:
    workflow_controller = (
        "WorkflowInstanceController" if modern_names else "ProcessInstanceController"
    )
    return RuntimeInstanceRecipe(
        project_identity=project_identity,
        project_route=project_route,
        definition_identity=definition_identity,
        workflow_controller=workflow_controller,
        workflow_page_operation=(
            f"{workflow_controller}.queryWorkflowInstanceList"
            if modern_names
            else f"{workflow_controller}.queryProcessInstanceList"
        ),
        workflow_get_operation=(
            f"{workflow_controller}.queryWorkflowInstanceById"
            if modern_names
            else f"{workflow_controller}.queryProcessInstanceById"
        ),
        workflow_parent_operation=(f"{workflow_controller}.queryParentInstanceBySubId"),
        workflow_sub_operation=(
            f"{workflow_controller}.querySubWorkflowInstanceByTaskId"
            if modern_names
            else f"{workflow_controller}.querySubProcessInstanceByTaskId"
        ),
        workflow_sub_result_key=(
            "subWorkflowInstanceId" if modern_names else "subProcessInstanceId"
        ),
        workflow_update_operation=(
            f"{workflow_controller}.updateWorkflowInstance"
            if modern_names
            else f"{workflow_controller}.updateProcessInstance"
        ),
        workflow_get_query_params=project_route == "name",
        workflow_model=(
            _WORKFLOW_INSTANCE_MODEL if modern_names else _PROCESS_INSTANCE_MODEL
        ),
        workflow_definition_model=(
            _WORKFLOW_DEFINITION_MODEL if modern_names else _PROCESS_DEFINITION_MODEL
        ),
        workflow_state_enum=workflow_state_enum,
        update_shape=update_shape,
        task_page_shape=task_page_shape,
        task_state_enum=task_state_enum,
        task_execute_type_enum=task_execute_type_enum,
        task_log_wire_epoch=task_log_wire_epoch,
        log_first_page_header=log_first_page_header,
        control_operation=control_operation,
        execute_task=execute_task,
        force_success=force_success,
        savepoint=savepoint,
        stop_task=stop_task,
        task_code_filter=execute_task,
        task_execute_type_filter=task_execute_type_enum is not None,
        task_workflow_name_filter=project_route == "code",
        task_definition_name_filter=task_execute_type_enum is not None,
        other_workflow_filters=task_execute_type_enum is not None,
        stop_result_may_be_unknown=stop_result_may_be_unknown,
        instance_dag_edit_requires_sync=False,
    )


_V139 = _recipe(
    project_identity="id",
    project_route="name",
    definition_identity="id",
    modern_names=False,
    update_shape="legacy-process-data",
    workflow_state_enum=_LEGACY_EXECUTION_STATUS_ENUM,
    task_state_enum=_LEGACY_EXECUTION_STATUS_ENUM,
    task_execute_type_enum=None,
    task_page_shape="entity",
    task_log_wire_epoch=_task_log_wire_epoch("1.3.9"),
    log_first_page_header=TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION["1.3.9"],
    control_operation="ExecutorController.execute",
    execute_task=False,
    force_success=False,
    savepoint=False,
    stop_task=False,
)
_V200 = _recipe(
    project_identity="code",
    project_route="code",
    definition_identity="code",
    modern_names=False,
    update_shape="modern-with-tenant",
    workflow_state_enum=_LEGACY_EXECUTION_STATUS_ENUM,
    task_state_enum=_LEGACY_EXECUTION_STATUS_ENUM,
    task_execute_type_enum=None,
    task_page_shape="map",
    task_log_wire_epoch=_task_log_wire_epoch("2.0.0"),
    log_first_page_header=TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION["2.0.0"],
    control_operation="ExecutorController.execute",
    execute_task=False,
    force_success=True,
    savepoint=False,
    stop_task=False,
)


def _process_recipe(
    *,
    version: str,
    execute_task: bool,
    savepoint: bool,
) -> RuntimeInstanceRecipe:
    status_split = (
        version
        in TARGET_RUNTIME_INSTANCE_VERSIONS[
            TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.1.0") :
        ]
    )
    return _recipe(
        project_identity="code",
        project_route="code",
        definition_identity="code",
        modern_names=False,
        update_shape=(
            "modern"
            if version
            in TARGET_RUNTIME_INSTANCE_VERSIONS[
                TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.2.0") :
            ]
            else "modern-with-tenant"
        ),
        workflow_state_enum=(
            _WORKFLOW_EXECUTION_STATUS_ENUM
            if status_split
            else _PLUGIN_EXECUTION_STATUS_ENUM
            if version
            in TARGET_RUNTIME_INSTANCE_VERSIONS[
                TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.0.0") :
            ]
            else _LEGACY_EXECUTION_STATUS_ENUM
        ),
        task_state_enum=(
            _TASK_EXECUTION_STATUS_ENUM
            if status_split
            else _PLUGIN_EXECUTION_STATUS_ENUM
            if version
            in TARGET_RUNTIME_INSTANCE_VERSIONS[
                TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.0.0") :
            ]
            else _LEGACY_EXECUTION_STATUS_ENUM
        ),
        task_execute_type_enum=_TASK_EXECUTE_TYPE_ENUM if status_split else None,
        task_page_shape="map",
        task_log_wire_epoch=_task_log_wire_epoch(version),
        log_first_page_header=TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION[version],
        control_operation="ExecutorController.execute",
        execute_task=execute_task,
        force_success=True,
        savepoint=savepoint,
        stop_task=savepoint,
        stop_result_may_be_unknown=STOP_RESULT_MAY_BE_UNKNOWN_BY_VERSION[version],
    )


def _workflow_recipe(version: str) -> RuntimeInstanceRecipe:
    return _recipe(
        project_identity="code",
        project_route="code",
        definition_identity="code",
        modern_names=True,
        update_shape="modern",
        workflow_state_enum=_WORKFLOW_EXECUTION_STATUS_ENUM,
        task_state_enum=_TASK_EXECUTION_STATUS_ENUM,
        task_execute_type_enum=_TASK_EXECUTE_TYPE_ENUM,
        task_page_shape="entity",
        task_log_wire_epoch=_task_log_wire_epoch(version),
        log_first_page_header=TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION[version],
        control_operation="ExecutorController.controlWorkflowInstance",
        execute_task=True,
        force_success=True,
        savepoint=True,
        stop_task=True,
        stop_result_may_be_unknown=STOP_RESULT_MAY_BE_UNKNOWN_BY_VERSION[version],
    )


RUNTIME_INSTANCE_CONTRACTS: dict[str, RuntimeInstanceRecipe] = {
    "1.3.9": _V139,
    "2.0.0": replace(_V200, instance_dag_edit_requires_sync=True),
    "2.0.1": replace(_V200, instance_dag_edit_requires_sync=True),
    "2.0.2": replace(
        _process_recipe(version="2.0.2", execute_task=False, savepoint=False),
        instance_dag_edit_requires_sync=True,
    ),
    "2.0.3": _process_recipe(version="2.0.3", execute_task=False, savepoint=False),
    "2.0.4": _process_recipe(version="2.0.4", execute_task=False, savepoint=False),
    "2.0.5": _process_recipe(version="2.0.5", execute_task=False, savepoint=False),
    "2.0.6": _process_recipe(version="2.0.6", execute_task=False, savepoint=False),
    "2.0.7": _process_recipe(version="2.0.7", execute_task=False, savepoint=False),
    "2.0.8": _process_recipe(version="2.0.8", execute_task=False, savepoint=False),
    "2.0.9": _process_recipe(version="2.0.9", execute_task=False, savepoint=False),
    "3.0.0": _process_recipe(version="3.0.0", execute_task=False, savepoint=False),
    "3.0.1": _process_recipe(version="3.0.1", execute_task=False, savepoint=False),
    "3.0.2": _process_recipe(version="3.0.2", execute_task=False, savepoint=False),
    "3.0.3": _process_recipe(version="3.0.3", execute_task=False, savepoint=False),
    "3.0.4": _process_recipe(version="3.0.4", execute_task=False, savepoint=False),
    "3.0.5": _process_recipe(version="3.0.5", execute_task=False, savepoint=False),
    "3.0.6": _process_recipe(version="3.0.6", execute_task=False, savepoint=False),
    "3.1.0": _process_recipe(version="3.1.0", execute_task=False, savepoint=True),
    "3.1.1": _process_recipe(version="3.1.1", execute_task=False, savepoint=True),
    "3.1.2": _process_recipe(version="3.1.2", execute_task=False, savepoint=True),
    "3.1.3": _process_recipe(version="3.1.3", execute_task=False, savepoint=True),
    "3.1.4": _process_recipe(version="3.1.4", execute_task=False, savepoint=True),
    "3.1.5": _process_recipe(version="3.1.5", execute_task=False, savepoint=True),
    "3.1.6": _process_recipe(version="3.1.6", execute_task=False, savepoint=True),
    "3.1.7": _process_recipe(version="3.1.7", execute_task=False, savepoint=True),
    "3.1.8": _process_recipe(version="3.1.8", execute_task=False, savepoint=True),
    "3.1.9": _process_recipe(version="3.1.9", execute_task=False, savepoint=True),
    "3.2.0": _process_recipe(version="3.2.0", execute_task=True, savepoint=True),
    "3.2.1": _process_recipe(version="3.2.1", execute_task=True, savepoint=True),
    "3.2.2": _process_recipe(version="3.2.2", execute_task=True, savepoint=True),
    "3.3.1": _workflow_recipe("3.3.1"),
    "3.3.2": _workflow_recipe("3.3.2"),
    "3.4.0": _workflow_recipe("3.4.0"),
    "3.4.1": _workflow_recipe("3.4.1"),
    "3.4.2": _workflow_recipe("3.4.2"),
    "3.4.3": replace(
        _workflow_recipe("3.4.3"),
        workflow_summary_model="org.apache.dolphinscheduler.api.vo.WorkflowInstanceSummaryVO",
    ),
}


class ScheduleEnvironmentFacts(TypedDict):
    """Reviewed runtime decisions and exact upstream source coordinates."""

    version: str
    schedule_forwards_environment: bool
    task_inherits_workflow_environment: bool
    scheduler_source: str
    scheduler_evidence_line: int
    task_source: str
    task_evidence_line: int
    utility_source: NotRequired[str]
    utility_empty_code_evidence_line: NotRequired[int]
    utility_fallback_evidence_line: NotRequired[int]


def _schedule_environment_facts() -> dict[str, ScheduleEnvironmentFacts]:
    """Load the reviewed exact-source coordinates and both runtime decisions."""
    document = json.loads(SCHEDULE_ENVIRONMENT_FACTS_PATH.read_text(encoding="utf-8"))
    if document.get("kind") != "schedule_environment_source_review":
        message = "invalid schedule environment source review"
        raise ValueError(message)
    rows = document.get("records")
    if not isinstance(rows, list):
        message = "schedule environment source review has no records"
        raise TypeError(message)
    facts = {row["version"]: row for row in rows}
    expected = set(TARGET_RUNTIME_INSTANCE_VERSIONS) - {"1.3.9"}
    if len(facts) != len(rows) or set(facts) != expected:
        message = "schedule environment source review has incomplete exact membership"
        raise ValueError(message)
    for version, row in facts.items():
        for field in (
            "schedule_forwards_environment",
            "task_inherits_workflow_environment",
        ):
            if type(row.get(field)) is not bool:
                message = f"DS {version} {field} must be a reviewed boolean"
                raise TypeError(message)
        source_fields = ["scheduler_source", "task_source"]
        line_fields = ["scheduler_evidence_line", "task_evidence_line"]
        if row["task_inherits_workflow_environment"]:
            source_fields.append("utility_source")
            line_fields.extend(
                ["utility_empty_code_evidence_line", "utility_fallback_evidence_line"]
            )
        for field in source_fields:
            value = row.get(field)
            if not isinstance(value, str) or not value:
                message = f"DS {version} {field} must name an exact source"
                raise TypeError(message)
        for field in line_fields:
            value = row.get(field)
            if type(value) is not int or value <= 0:
                message = f"DS {version} {field} must be a positive line number"
                raise TypeError(message)
    return cast("dict[str, ScheduleEnvironmentFacts]", facts)


SCHEDULE_ENVIRONMENT_FACTS = _schedule_environment_facts()
RUNTIME_INSTANCE_CONTRACTS = {
    version: replace(
        recipe,
        schedule_forwards_environment=(
            SCHEDULE_ENVIRONMENT_FACTS[version]["schedule_forwards_environment"] is True
            if version != "1.3.9"
            else False
        ),
        task_inherits_workflow_environment=(
            SCHEDULE_ENVIRONMENT_FACTS[version]["task_inherits_workflow_environment"]
            is True
            if version != "1.3.9"
            else False
        ),
    )
    for version, recipe in RUNTIME_INSTANCE_CONTRACTS.items()
}


def validate_schedule_environment_sources(source_roots: dict[str, Path]) -> None:
    """Bind each reviewed decision to its exact Quartz and master source."""
    selected = set(source_roots)
    if not selected <= set(TARGET_RUNTIME_INSTANCE_VERSIONS):
        message = "unknown exact source in schedule environment validation"
        raise ValueError(message)
    for version, root in source_roots.items():
        if version == "1.3.9":
            schedule = root / (
                "dolphinscheduler-dao/src/main/java/org/apache/dolphinscheduler/"
                "dao/entity/Schedule.java"
            )
            if "environmentCode" in schedule.read_text(encoding="utf-8"):
                message = "DS 1.3.9 unexpectedly has a schedule environment"
                raise ValueError(message)
            continue
        row = SCHEDULE_ENVIRONMENT_FACTS[version]
        scheduler = (root / row["scheduler_source"]).read_text(encoding="utf-8")
        task = (root / row["task_source"]).read_text(encoding="utf-8")
        scheduler_line = scheduler.splitlines()[row["scheduler_evidence_line"] - 1]
        task_line = task.splitlines()[row["task_evidence_line"] - 1]
        forwards = bool(
            re.search(
                r"(?:setEnvironmentCode|environmentCode)\s*\(\s*"
                r"schedule\.getEnvironmentCode\s*\(\s*\)\s*\)",
                scheduler,
            )
        )
        if forwards != row["schedule_forwards_environment"]:
            message = f"DS {version} Quartz schedule environment source changed"
            raise ValueError(message)
        if forwards:
            if "schedule.getEnvironmentCode()" not in scheduler_line:
                message = f"DS {version} Quartz evidence line changed"
                raise ValueError(message)
        elif "CommandType.SCHEDULER" not in scheduler_line:
            message = f"DS {version} Quartz omission evidence line changed"
            raise ValueError(message)
        null_only = bool(
            re.search(
                r"Objects\.isNull\s*\(\s*taskNode\.getEnvironmentCode\s*"
                r"\(\s*\)\s*\)\s*\?\s*processEnvironmentCode\s*:\s*"
                r"taskNode\.getEnvironmentCode\s*\(\s*\)",
                task,
            )
        )
        empty_code = bool(
            re.search(
                r"(?:EnvironmentUtils\.)?getEnvironmentCodeOrDefault\s*"
                r"\(\s*(?:taskEnvironmentCode|taskInstance\.getEnvironmentCode\s*"
                r"\(\s*\))\s*,\s*(?:processEnvironmentCode|"
                r"workflowInstance\.getEnvironmentCode\s*\(\s*\))\s*\)",
                task,
            )
        )
        if (null_only, empty_code) != (
            not row["task_inherits_workflow_environment"],
            row["task_inherits_workflow_environment"],
        ):
            message = f"DS {version} master task environment source changed"
            raise ValueError(message)
        expected_line = (
            "getEnvironmentCodeOrDefault"
            if empty_code
            else "taskNode.getEnvironmentCode()"
        )
        if expected_line not in task_line:
            message = f"DS {version} master evidence line changed"
            raise ValueError(message)
        if empty_code:
            utility = (root / row["utility_source"]).read_text(encoding="utf-8")
            utility_lines = utility.splitlines()
            empty_line = utility_lines[row["utility_empty_code_evidence_line"] - 1]
            fallback_line = utility_lines[row["utility_fallback_evidence_line"] - 1]
            if not re.search(
                r"return\s+environmentCode\s*==\s*null\s*\|\|\s*"
                r"environmentCode\s*<=\s*0\s*;",
                empty_line,
            ) or not re.search(
                r"return\s+isEnvironmentCodeEmpty\s*\(\s*environmentCode\s*"
                r"\)\s*\?\s*defaultEnvironmentCode\s*:\s*environmentCode\s*;",
                fallback_line,
            ):
                message = f"DS {version} empty environment utility source changed"
                raise ValueError(message)


def runtime_instance_contract(version: str) -> RuntimeInstanceRecipe:
    """Return one reviewed exact contract, rejecting version inference."""
    try:
        return RUNTIME_INSTANCE_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed runtime-instance contract"
        raise ValueError(message) from exc


_RUNTIME_PROFILE_FIELD_TYPES = {
    "family": 'Literal["process", "workflow"]',
    "project_identity": 'Literal["name", "code"]',
    "definition_identity": 'Literal["id", "code"]',
    "workflow_module": "str",
    "workflow_group": "str",
    "workflow_page_params": "str",
    "workflow_summary": "bool",
    "workflow_page_method": "str",
    "workflow_get_params": "str | None",
    "workflow_get_method": "str",
    "workflow_parent_params": "str",
    "workflow_parent_method": "str",
    "workflow_sub_params": "str",
    "workflow_sub_method": "str",
    "workflow_sub_result_key": (
        'Literal["subProcessInstanceId", "subWorkflowInstanceId"]'
    ),
    "workflow_update_params": "str | None",
    "workflow_update_method": "str | None",
    "update_shape": 'Literal["legacy", "tenant", "modern"]',
    "task_page_shape": 'Literal["entity", "map"]',
    "log_params": "str",
    "log_method": "str",
    "log_epoch": (
        'Literal["query-log-string", "log-detail-string", '
        '"log-detail-record", "query-log-record"]'
    ),
    "log_first_page_header": "bool",
    "control_params": "str",
    "control_method": "str",
    "execute_task": "bool",
    "force_success": "bool",
    "savepoint": "bool",
    "stop_task": "bool",
    "has_task_code_filter": "bool",
    "has_task_execute_type": "bool",
    "has_task_workflow_name": "bool",
    "has_task_definition_name": "bool",
    "has_other_workflow_params": "bool",
    "stop_result_may_be_unknown": "bool",
    "instance_dag_edit_requires_sync": "bool",
    "schedule_forwards_environment": "bool",
    "task_inherits_workflow_environment": "bool",
}


def runtime_instance_profile_data(
    versions: tuple[str, ...] | None = None,
) -> dict[str, object]:
    """Project the reviewed contracts into the publishable runtime recipe seam."""
    selected = select_exact_versions(
        TARGET_RUNTIME_INSTANCE_VERSIONS, versions, label="runtime_instance reviews"
    )
    profiles = {
        version: _runtime_instance_profile(runtime_instance_contract(version))
        for version in selected
    }
    return {
        "schema_version": RUNTIME_INSTANCE_PROFILE_SCHEMA_VERSION,
        "target_versions": list(selected),
        "profiles": profiles,
    }


def _runtime_instance_profile(recipe: RuntimeInstanceRecipe) -> dict[str, object]:
    if recipe.task_page_shape == "entity-or-map":
        message = (
            "runtime instance profile requires one exact task page shape, not "
            "entity-or-map"
        )
        raise ValueError(message)
    family: Literal["process", "workflow"] = (
        "workflow"
        if recipe.workflow_controller == "WorkflowInstanceController"
        else "process"
    )
    workflow_module = _controller_module_name(recipe.workflow_controller)
    update_operation = recipe.workflow_update_operation
    return {
        "family": family,
        "project_identity": recipe.project_route,
        "definition_identity": recipe.definition_identity,
        "workflow_module": workflow_module,
        "workflow_group": workflow_module,
        "workflow_page_params": _operation_params_name(recipe.workflow_page_operation),
        "workflow_summary": recipe.workflow_summary_model is not None,
        "workflow_page_method": _operation_method_name(recipe.workflow_page_operation),
        "workflow_get_params": (
            _operation_params_name(recipe.workflow_get_operation)
            if recipe.workflow_get_query_params
            else None
        ),
        "workflow_get_method": _operation_method_name(recipe.workflow_get_operation),
        "workflow_parent_params": _operation_params_name(
            recipe.workflow_parent_operation
        ),
        "workflow_parent_method": _operation_method_name(
            recipe.workflow_parent_operation
        ),
        "workflow_sub_params": _operation_params_name(recipe.workflow_sub_operation),
        "workflow_sub_method": _operation_method_name(recipe.workflow_sub_operation),
        "workflow_sub_result_key": recipe.workflow_sub_result_key,
        "workflow_update_params": (
            None
            if update_operation is None
            else _operation_params_name(update_operation)
        ),
        "workflow_update_method": (
            None
            if update_operation is None
            else _operation_method_name(update_operation)
        ),
        "update_shape": {
            "legacy-process-data": "legacy",
            "modern-with-tenant": "tenant",
            "modern": "modern",
        }[recipe.update_shape],
        "task_page_shape": recipe.task_page_shape,
        "log_params": _operation_params_name(recipe.log_operation),
        "log_method": _operation_method_name(recipe.log_operation),
        "log_epoch": recipe.task_log_wire_epoch,
        "log_first_page_header": recipe.log_first_page_header,
        "control_params": _operation_params_name(recipe.control_operation),
        "control_method": _operation_method_name(recipe.control_operation),
        "execute_task": recipe.execute_task,
        "force_success": recipe.force_success,
        "savepoint": recipe.savepoint,
        "stop_task": recipe.stop_task,
        "has_task_code_filter": recipe.task_code_filter,
        "has_task_execute_type": recipe.task_execute_type_filter,
        "has_task_workflow_name": recipe.task_workflow_name_filter,
        "has_task_definition_name": recipe.task_definition_name_filter,
        "has_other_workflow_params": recipe.other_workflow_filters,
        "stop_result_may_be_unknown": recipe.stop_result_may_be_unknown,
        "instance_dag_edit_requires_sync": recipe.instance_dag_edit_requires_sync,
        "schedule_forwards_environment": recipe.schedule_forwards_environment,
        "task_inherits_workflow_environment": recipe.task_inherits_workflow_environment,
    }


def _controller_module_name(controller: str) -> str:
    return _snake_case(controller.removesuffix("Controller"))


def _operation_method_name(operation: str) -> str:
    _, separator, suffix = operation.partition(".")
    if not separator or not suffix:
        message = f"invalid runtime instance operation id: {operation!r}"
        raise ValueError(message)
    return _snake_case(suffix).strip("_")


def _operation_params_name(operation: str) -> str:
    _, separator, suffix = operation.partition(".")
    if not separator or not suffix:
        message = f"invalid runtime instance operation id: {operation!r}"
        raise ValueError(message)
    class_name = "".join(
        part[:1].upper() + part[1:] for part in re.split(r"[._]+", suffix) if part
    ).removesuffix("Request")
    return f"{class_name}Params"


def _snake_case(name: str) -> str:
    sanitized = re.sub(r"[^0-9A-Za-z]+", "_", name)
    parts: list[str] = []
    for index, char in enumerate(sanitized):
        if char.isupper() and index > 0 and not sanitized[index - 1].isupper():
            parts.append("_")
        parts.append(char.lower())
    return "".join(parts)


def _python_literal(value: object) -> str:
    """Render profile literals in the repository's stable Python style."""
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True)
    return repr(value)


def render_runtime_instance_profiles(
    data: dict[str, object] | None = None,
) -> str:
    """Render a typed runtime artifact without duplicating version decisions."""
    projected = runtime_instance_profile_data() if data is None else data
    raw_versions = projected.get("target_versions")
    raw_profiles = projected.get("profiles")
    if not isinstance(raw_versions, list) or not isinstance(raw_profiles, dict):
        message = "runtime-instance profile projection has an invalid shape"
        raise TypeError(message)
    profile_pool: list[dict[str, object]] = []
    profile_indexes: dict[str, int] = {}
    profile_specs: dict[str, int] = {}
    expected_fields = tuple(_RUNTIME_PROFILE_FIELD_TYPES)
    for raw_version in raw_versions:
        if not isinstance(raw_version, str):
            message = "runtime-instance profile version must be text"
            raise TypeError(message)
        raw_profile = raw_profiles.get(raw_version)
        if not isinstance(raw_profile, dict) or tuple(raw_profile) != expected_fields:
            message = f"runtime-instance profile {raw_version} has an invalid shape"
            raise TypeError(message)
        key = json.dumps(raw_profile, sort_keys=True, separators=(",", ":"))
        profile_index = profile_indexes.get(key)
        if profile_index is None:
            profile_index = len(profile_pool)
            profile_indexes[key] = profile_index
            profile_pool.append(raw_profile)
        profile_specs[raw_version] = profile_index

    lines = [
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass",
        "from typing import Literal",
        "",
        "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
        f"RUNTIME_INSTANCE_PROFILE_SCHEMA_VERSION = {projected['schema_version']}",
        "",
        "",
        "@dataclass(frozen=True)",
        "class RuntimeInstanceProfile:",
        '    """Generated exact-version runtime invocation recipe."""',
        "",
    ]
    lines.extend(
        f"    {field}: {annotation}"
        for field, annotation in _RUNTIME_PROFILE_FIELD_TYPES.items()
    )
    lines.extend(("", "", "_RUNTIME_INSTANCE_PROFILE_POOL = ("))
    for profile in profile_pool:
        lines.append("    RuntimeInstanceProfile(")
        lines.extend(
            f"        {field}={_python_literal(profile[field])},"
            for field in _RUNTIME_PROFILE_FIELD_TYPES
        )
        lines.append("    ),")
    lines.extend((")", "", "_RUNTIME_INSTANCE_PROFILE_SPECS = {"))
    lines.extend(
        f"    {_python_literal(version)}: {profile_specs[version]},"
        for version in raw_versions
    )
    lines.extend(
        (
            "}",
            "TARGET_RUNTIME_INSTANCE_VERSIONS = tuple(_RUNTIME_INSTANCE_PROFILE_SPECS)",
            "RUNTIME_INSTANCE_PROFILES = {",
            "    version: _RUNTIME_INSTANCE_PROFILE_POOL[profile_index]",
            (
                "    for version, profile_index in "
                "(_RUNTIME_INSTANCE_PROFILE_SPECS.items())"
            ),
            "}",
            "",
        )
    )
    return "\n".join(lines)


def write_runtime_instance_profiles(
    output_root: Path, *, versions: tuple[str, ...] | None = None
) -> Path:
    """Write the runtime projection beside the exact generated bundles."""
    output_path = output_root / GENERATED_RUNTIME_INSTANCE_PROFILE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_runtime_instance_profiles(runtime_instance_profile_data(versions)),
        encoding="utf-8",
    )
    return output_path


def _project_scope_sources(recipe: RuntimeInstanceRecipe) -> tuple[str, ...]:
    project_get = (
        "ProjectController.queryProjectById"
        if recipe.project_identity == "id"
        else "ProjectController.queryProjectByCode"
    )
    return ("ProjectController.queryProjectListPaging", project_get)


def _workflow_definition_sources(
    recipe: RuntimeInstanceRecipe,
) -> tuple[str, ...]:
    if recipe.definition_identity == "id":
        return (
            "ProcessDefinitionController.queryProcessDefinitionList",
            "ProcessDefinitionController.queryProcessDefinitionById",
        )
    controller = (
        "WorkflowDefinitionController"
        if recipe.workflow_controller == "WorkflowInstanceController"
        else "ProcessDefinitionController"
    )
    prefix = "Workflow" if controller.startswith("Workflow") else "Process"
    return (
        f"{controller}.query{prefix}DefinitionSimpleList",
        f"{controller}.query{prefix}DefinitionByCode",
    )


def _workflow_get(recipe: RuntimeInstanceRecipe) -> tuple[str, ...]:
    return (*_project_scope_sources(recipe), recipe.workflow_get_operation)


def _task_get(recipe: RuntimeInstanceRecipe) -> tuple[str, ...]:
    return (*_workflow_get(recipe), "TaskInstanceController.queryTaskListPaging")


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return the complete remote closure of every executable stable action."""
    recipe = runtime_instance_contract(version)
    workflow_get = _workflow_get(recipe)
    task_get = _task_get(recipe)
    project_scope = _project_scope_sources(recipe)
    workflow_definition_scope = _workflow_definition_sources(recipe)
    sources: dict[str, tuple[str, ...]] = {
        "workflow-instance.list": (
            *project_scope,
            *workflow_definition_scope,
            recipe.workflow_page_operation,
        ),
        "workflow-instance.get": workflow_get,
        "workflow-instance.parent": (*workflow_get, recipe.workflow_parent_operation),
        "workflow-instance.digest": (
            *workflow_get,
            "TaskInstanceController.queryTaskListPaging",
        ),
        "workflow-instance.watch": workflow_get,
        "workflow-instance.stop": (*workflow_get, recipe.control_operation),
        "workflow-instance.rerun": (*workflow_get, recipe.control_operation),
        "workflow-instance.recover-failed": (
            *workflow_get,
            recipe.control_operation,
        ),
        "task-instance.list": (
            *project_scope,
            recipe.workflow_get_operation,
            "TaskInstanceController.queryTaskListPaging",
        ),
        "task-instance.get": task_get,
        "task-instance.watch": task_get,
        "task-instance.sub-workflow": (*task_get, recipe.workflow_sub_operation),
        "task-instance.log": (recipe.log_operation,),
    }
    if recipe.execute_task:
        sources["workflow-instance.list"] += (
            recipe.workflow_controller
            + ".query"
            + (
                "Workflow"
                if recipe.workflow_controller == "WorkflowInstanceController"
                else "Process"
            )
            + "InstancesByTriggerCode",
        )
    sources["workflow-instance.export"] = workflow_get
    sources["workflow-instance.edit"] = (
        *workflow_get,
        *(
            ("TaskDefinitionController.genTaskCodeList",)
            if recipe.update_shape != "legacy-process-data"
            else ()
        ),
        recipe.workflow_update_operation,
    )
    if recipe.execute_task:
        sources["workflow-instance.execute-task"] = (
            *workflow_get,
            "ExecutorController.executeTask",
        )
    if recipe.force_success:
        sources["task-instance.force-success"] = (
            *task_get,
            "TaskInstanceController.forceTaskSuccess",
        )
    if recipe.savepoint:
        sources["task-instance.savepoint"] = (
            *task_get,
            "TaskInstanceController.taskSavePoint",
        )
    if recipe.stop_task:
        sources["task-instance.stop"] = (
            *task_get,
            "TaskInstanceController.stopTask",
        )
    return sources


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit structured roots for each selected runtime slice."""
    recipe = runtime_instance_contract(version)
    project_scope = (_RESULT_MODEL, _PAGE_INFO_MODEL, _PROJECT_MODEL)
    workflow_scope = (*project_scope, recipe.workflow_model)
    roots: dict[str, tuple[str, ...]] = {}
    for operation in semantic_operation_sources(version):
        if operation == "task-instance.log":
            selected = [_RESULT_MODEL]
        elif operation == "workflow-instance.list":
            selected = [*workflow_scope, recipe.workflow_definition_model]
            if recipe.workflow_summary_model is not None:
                selected.append(recipe.workflow_summary_model)
        else:
            selected = [*workflow_scope]
        # 2.0.1 exposes task rows only through a map-shaped payload; its
        # exact controller closure contains no TaskInstance model.
        if version != "2.0.1" and operation in {
            "workflow-instance.digest",
            "task-instance.list",
            "task-instance.get",
            "task-instance.watch",
            "task-instance.sub-workflow",
            "task-instance.force-success",
            "task-instance.savepoint",
            "task-instance.stop",
        }:
            selected.append(_TASK_INSTANCE_MODEL)
        if operation == "task-instance.log" and recipe.log_shape == (
            "response-task-log"
        ):
            selected.append(_RESPONSE_TASK_LOG_MODEL)
        roots[operation] = tuple(dict.fromkeys(selected))
    return roots


def semantic_operation_enum_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return exact enum roots required by selected requests and responses."""
    recipe = runtime_instance_contract(version)
    roots: dict[str, tuple[str, ...]] = {}
    for operation in semantic_operation_sources(version):
        enums: list[str] = []
        if operation.startswith("workflow-instance.") or operation in {
            "task-instance.list",
            "task-instance.get",
            "task-instance.watch",
            "task-instance.sub-workflow",
            "task-instance.force-success",
            "task-instance.savepoint",
            "task-instance.stop",
        }:
            enums.append(recipe.workflow_state_enum)
        if operation in {
            "workflow-instance.digest",
            "task-instance.list",
            "task-instance.get",
            "task-instance.watch",
            "task-instance.sub-workflow",
            "task-instance.force-success",
            "task-instance.savepoint",
            "task-instance.stop",
        }:
            enums.append(recipe.task_state_enum)
        if operation in {
            "workflow-instance.stop",
            "workflow-instance.rerun",
            "workflow-instance.recover-failed",
        }:
            enums.append(_EXECUTE_TYPE_ENUM)
        if operation == "workflow-instance.execute-task":
            enums.append(_TASK_DEPEND_TYPE_ENUM)
        if operation == "task-instance.list" and recipe.task_execute_type_enum:
            enums.append(recipe.task_execute_type_enum)
        roots[operation] = tuple(dict.fromkeys(enums))
    return roots


def action_support(version: str) -> dict[str, Support]:
    """Return terminal support decisions for every stable instance action."""
    recipe = runtime_instance_contract(version)
    support: dict[str, Support] = dict.fromkeys(
        RUNTIME_INSTANCE_SEMANTIC_OPERATIONS,
        "supported",
    )
    if not recipe.execute_task:
        support["workflow-instance.execute-task"] = "absent"
    if not recipe.force_success:
        support["task-instance.force-success"] = "absent"
    if not recipe.savepoint:
        support["task-instance.savepoint"] = "absent"
    if not recipe.stop_task:
        support["task-instance.stop"] = "absent"
    return support


def terminal_decisions(version: str) -> dict[str, TerminalDecision]:
    """Return evidence-backed zero-request terminal instance coordinates."""
    recipe = runtime_instance_contract(version)
    decisions: dict[str, TerminalDecision] = {}
    absence_specs = (
        (
            "workflow-instance.execute-task",
            recipe.execute_task,
            "3.2.0",
            _EXECUTOR_CONTROLLER,
            "executeTask",
        ),
        (
            "task-instance.force-success",
            recipe.force_success,
            "2.0.0",
            _TASK_INSTANCE_CONTROLLER,
            "forceTaskSuccess",
        ),
        (
            "task-instance.savepoint",
            recipe.savepoint,
            "3.1.0",
            _TASK_INSTANCE_CONTROLLER,
            "taskSavePoint",
        ),
        (
            "task-instance.stop",
            recipe.stop_task,
            "3.1.0",
            _TASK_INSTANCE_CONTROLLER,
            "stopTask",
        ),
    )
    for operation, available, introduced, controller, symbol in absence_specs:
        if available:
            continue
        decisions[operation] = TerminalDecision(
            semantic_operation=operation,
            reason="upstream_capability_absent",
            constraint=(
                f"This DolphinScheduler release predates {operation}, "
                f"introduced in {introduced}."
            ),
            evidence=Evidence(
                version=version,
                kind="snapshot",
                source=f"build/ds_contract/snapshots-v2/ds-{version}-contract.json",
                symbol="operations",
                conclusion=(
                    "The exact source inventory contains no "
                    f"{controller.rsplit('/', 1)[-1]}"
                    f"#{symbol} operation."
                ),
            ),
        )
    return decisions


def _ui_sources(version: str, operation: str) -> tuple[str, ...]:
    if (
        version
        in TARGET_RUNTIME_INSTANCE_VERSIONS[
            : TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.0.0")
        ]
    ):
        return (
            _LEGACY_INSTANCE_UI,
            *(
                (_LEGACY_DAG_UI,)
                if operation in {"workflow-instance.export", "workflow-instance.edit"}
                else ()
            ),
        )
    workflow_ui = (
        _WORKFLOW_INSTANCE_UI
        if version
        in TARGET_RUNTIME_INSTANCE_VERSIONS[
            TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.3.1") :
        ]
        else _PROCESS_INSTANCE_UI
    )
    if operation.startswith("task-instance.") or operation == (
        "workflow-instance.digest"
    ):
        return (workflow_ui, _TASK_INSTANCE_UI)
    return (workflow_ui,)


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return exact controller and retained-UI evidence for executable actions."""
    evidence: dict[str, tuple[Evidence, ...]] = {}
    controller_paths = {
        "ProjectController": _PROJECT_CONTROLLER,
        "ProcessInstanceController": _PROCESS_INSTANCE_CONTROLLER,
        "WorkflowInstanceController": _WORKFLOW_INSTANCE_CONTROLLER,
        "TaskInstanceController": _TASK_INSTANCE_CONTROLLER,
        "ExecutorController": _EXECUTOR_CONTROLLER,
        "LoggerController": _LOGGER_CONTROLLER,
        "TaskDefinitionController": _TASK_DEFINITION_CONTROLLER,
        "ProcessDefinitionController": _PROCESS_DEFINITION_CONTROLLER,
        "WorkflowDefinitionController": _WORKFLOW_DEFINITION_CONTROLLER,
    }
    for operation, source_operations in semantic_operation_sources(version).items():
        symbols: dict[str, list[str]] = {}
        for source_operation in source_operations:
            controller, _, method = source_operation.partition(".")
            symbols.setdefault(controller, []).append(method)
        evidence[operation] = (
            *(
                Evidence(
                    version=version,
                    kind="controller",
                    source=controller_paths[controller],
                    symbol=";".join(dict.fromkeys(methods)),
                    conclusion="Exact controller closure used by the stable action.",
                )
                for controller, methods in symbols.items()
            ),
            *(
                Evidence(
                    version=version,
                    kind="ui",
                    source=ui,
                    symbol="request",
                    conclusion=(
                        "The retained UI confirms route use, defaults, and the "
                        "user-visible instance workflow."
                    ),
                )
                for ui in _ui_sources(version, operation)
            ),
        )
    return evidence


def semantic_operation_facets(version: str) -> dict[str, dict[str, object]]:
    """Project reviewed instance behavior for generated profile metadata."""
    recipe = runtime_instance_contract(version)
    facets: dict[str, dict[str, object]] = {}
    for operation in semantic_operation_sources(version):
        result = "page" if operation.endswith(".list") else "entity"
        if operation.endswith(".watch"):
            result = "local-watch"
        elif operation.endswith(".digest"):
            result = "local-digest"
        elif operation.endswith(".export"):
            result = "local-authoring-document"
        elif operation.endswith(".log"):
            result = "log-chunk"
        elif operation in {
            "workflow-instance.stop",
            "workflow-instance.rerun",
            "workflow-instance.recover-failed",
            "workflow-instance.execute-task",
            "workflow-instance.edit",
            "task-instance.force-success",
            "task-instance.savepoint",
            "task-instance.stop",
        }:
            result = "verified-entity"
        facets[operation] = {
            "result": result,
            "project_identity": recipe.project_identity,
            "project_route": recipe.project_route,
            "definition_identity": recipe.definition_identity,
            "project_scoped": operation != "task-instance.log",
            "v2_route": False,
        }
    return facets


__all__ = [
    "GENERATED_RUNTIME_INSTANCE_PROFILE_PATH",
    "RUNTIME_INSTANCE_CONTRACTS",
    "RUNTIME_INSTANCE_PROFILE_SCHEMA_VERSION",
    "RUNTIME_INSTANCE_SEMANTIC_OPERATIONS",
    "TARGET_RUNTIME_INSTANCE_VERSIONS",
    "TASK_INSTANCE_SEMANTIC_OPERATIONS",
    "TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION",
    "TASK_LOG_WIRE_EPOCH_CONTRACTS",
    "WORKFLOW_INSTANCE_SEMANTIC_OPERATIONS",
    "Evidence",
    "RuntimeInstanceRecipe",
    "TaskLogWireEpoch",
    "TaskLogWireEpochContract",
    "TerminalDecision",
    "action_support",
    "render_runtime_instance_profiles",
    "runtime_instance_contract",
    "runtime_instance_profile_data",
    "semantic_operation_enum_roots",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "terminal_decisions",
    "write_runtime_instance_profiles",
]
