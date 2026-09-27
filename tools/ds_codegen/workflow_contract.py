"""Reviewed exact-version recipes for workflow definitions, execution, and lineage.

This module records the wire boundaries that the stable ``dsctl workflow``
surface crosses.  Equal recipes are repeated for every exact release on
purpose: neighbouring-version inference is not compatibility evidence.

The 1.3.9 definition stores string-native tasks inside
``processDefinitionJson`` and separate ``connects``.  Its reviewed adapter
therefore preserves those native identities and fields instead of inventing
task codes.  Definition authoring, DAG inspection, and task-scoped execution
remain executable through the exact legacy representation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ds_codegen.profile_ledger import select_exact_versions

TARGET_WORKFLOW_VERSIONS = (
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

WORKFLOW_SEMANTIC_OPERATIONS = (
    "workflow.create",
    "workflow.edit",
    "workflow.delete",
    "workflow.online",
    "workflow.offline",
    "workflow.run",
    "workflow.run-task",
    "workflow.backfill",
    "workflow.describe",
    "workflow.digest",
    "workflow.export",
    "workflow.lineage.list",
    "workflow.lineage.get",
    "workflow.lineage.dependent-tasks",
)
WORKFLOW_PROFILE_SCHEMA_VERSION = 4
GENERATED_WORKFLOW_PROFILE_PATH = Path("generated/workflow_profiles.py")

Support = Literal["supported", "absent", "limited"]
DefinitionFamily = Literal["legacy-json", "process", "workflow"]
NativeIdentity = Literal["id", "code"]
ProjectRoute = Literal["name", "code"]
DagWire = Literal["legacy-json", "process", "workflow"]
ExecutionResult = Literal["none", "trigger-code", "id-list"]
LineageProjection = Literal["absent", "legacy-direct", "canonical"]
DependentProjection = Literal["absent", "task-main-info", "dependent-lineage-task"]
WarningGroupOmission = Literal["zero", "omit"]
ExecutionScheduleTimeShape = Literal["comma-range", "json"]
EvidenceKind = Literal["controller", "mapper", "service", "ui"]
TerminalReason = Literal[
    "upstream_capability_absent",
    "upstream_capability_limited",
]

# These exact source-backed request-parameter epochs are independently checked
# against every controller operation by compiled_workflow_definitions.
WARNING_GROUP_PRIMITIVE_VERSIONS = frozenset(
    {
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
    }
)
WARNING_GROUP_DEFAULT_ZERO_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
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
    }
)
COMMA_RANGE_EXECUTION_SCHEDULE_VERSIONS = frozenset(
    {
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
    }
)


@dataclass(frozen=True)
class Evidence:
    """One exact upstream coordinate supporting a workflow decision."""

    version: str
    kind: EvidenceKind
    source: str
    symbol: str
    conclusion: str

    @property
    def reference(self) -> str:
        """Return the conventional source reference consumed by profiles."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class DefinitionRecipe:
    """Definition lifecycle and DAG wire for one exact release."""

    family: DefinitionFamily
    project_route: ProjectRoute
    native_identity: NativeIdentity
    dag_wire: DagWire
    dag_support: Support
    authoring_support: Support
    detail_operation: str
    create_operation: str
    update_operation: str
    delete_operation: str
    release_operation: str
    code_allocation_operation: str | None
    tenant_code: bool
    create_other_params: bool
    update_other_params: bool
    execution_type: bool
    release_result: Literal["none", "boolean", "map"]
    definition_delete_lineage_guard: bool = False


@dataclass(frozen=True)
class ExecutionRecipe:
    """Whole-workflow and task-scoped executor request vocabulary."""

    operation: str
    result: ExecutionResult
    task_scope: Support
    start_node_identity: Literal["name", "code"]
    tenant_code: bool
    environment_code: bool
    start_params: bool
    execution_dry_run: bool
    expected_parallelism_number: bool
    complement_dependent_mode: bool
    all_level_dependent: bool
    execution_order: bool
    timeout: bool
    test_flag: bool
    version: bool


@dataclass(frozen=True)
class LineageRecipe:
    """Lineage graph and dependent-task availability/projection."""

    graph_support: Support
    graph_projection: LineageProjection
    list_operation: str | None
    get_operation: str | None
    dependent_support: Support
    dependent_projection: DependentProjection
    dependent_operation: str | None


@dataclass(frozen=True)
class WorkflowVersionContract:
    """Reviewed workflow family contract for one exact DS version."""

    version: str
    definition: DefinitionRecipe
    execution: ExecutionRecipe
    lineage: LineageRecipe


@dataclass(frozen=True)
class TerminalDecision:
    """One stable workflow action that must perform zero requests."""

    semantic_operation: str
    reason: TerminalReason
    constraint: str
    evidence: Evidence


_DEFINITION_CONTROLLER_139 = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ProcessDefinitionController.java"
)
_DEFINITION_CONTROLLER_PROCESS = _DEFINITION_CONTROLLER_139
_DEFINITION_CONTROLLER_WORKFLOW = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/WorkflowDefinitionController.java"
)
_EXECUTOR_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ExecutorController.java"
)
_LINEAGE_CONTROLLER_LEGACY = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/WorkFlowLineageController.java"
)
_LINEAGE_CONTROLLER_CURRENT = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/WorkflowLineageController.java"
)
_WORKFLOW_DEFINITION_SERVICE = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/service/impl/WorkflowDefinitionServiceImpl.java"
)
_WORKFLOW_LINEAGE_SERVICE = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/service/impl/WorkflowLineageServiceImpl.java"
)
_WORKFLOW_LINEAGE_MAPPER = (
    "dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/"
    "dao/mapper/WorkflowTaskLineageMapper.xml"
)
_LEGACY_DAG_UI = "dolphinscheduler-ui/src/js/conf/home/store/dag/actions.js"
_LEGACY_LINEAGE_UI = "dolphinscheduler-ui/src/js/conf/home/store/kinship/actions.js"
_PROCESS_UI = "dolphinscheduler-ui/src/service/modules/process-definition/index.ts"
_WORKFLOW_UI = "dolphinscheduler-ui/src/service/modules/workflow-definition/index.ts"
_EXECUTOR_UI = "dolphinscheduler-ui/src/service/modules/executors/index.ts"
_LINEAGE_UI = "dolphinscheduler-ui/src/service/modules/lineages/index.ts"


_LEGACY_DEFINITION = DefinitionRecipe(
    family="legacy-json",
    project_route="name",
    native_identity="id",
    dag_wire="legacy-json",
    dag_support="supported",
    authoring_support="supported",
    detail_operation="ProcessDefinitionController.queryProcessDefinitionById",
    create_operation="ProcessDefinitionController.createProcessDefinition",
    update_operation="ProcessDefinitionController.updateProcessDefinition",
    delete_operation="ProcessDefinitionController.deleteProcessDefinitionById",
    release_operation="ProcessDefinitionController.releaseProcessDefinition",
    code_allocation_operation=None,
    tenant_code=False,
    create_other_params=False,
    update_other_params=False,
    execution_type=False,
    release_result="map",
)
_PROCESS_20 = DefinitionRecipe(
    family="process",
    project_route="code",
    native_identity="code",
    dag_wire="process",
    dag_support="supported",
    authoring_support="supported",
    detail_operation="ProcessDefinitionController.queryProcessDefinitionByCode",
    create_operation="ProcessDefinitionController.createProcessDefinition",
    update_operation="ProcessDefinitionController.updateProcessDefinition",
    delete_operation="ProcessDefinitionController.deleteProcessDefinitionByCode",
    release_operation="ProcessDefinitionController.releaseProcessDefinition",
    code_allocation_operation="TaskDefinitionController.genTaskCodeList",
    tenant_code=True,
    create_other_params=False,
    update_other_params=False,
    execution_type=False,
    release_result="none",
)
_PROCESS_30 = replace(_PROCESS_20, execution_type=True)
_PROCESS_31 = replace(
    _PROCESS_30,
    create_other_params=True,
    update_other_params=True,
)
_PROCESS_320 = replace(
    _PROCESS_31,
    tenant_code=False,
)
_PROCESS_321 = replace(
    _PROCESS_320,
    update_other_params=False,
    release_result="boolean",
)
_WORKFLOW = replace(
    _PROCESS_321,
    family="workflow",
    dag_wire="workflow",
    detail_operation="WorkflowDefinitionController.queryWorkflowDefinitionByCode",
    create_operation="WorkflowDefinitionController.createWorkflowDefinition",
    update_operation="WorkflowDefinitionController.updateWorkflowDefinition",
    delete_operation="WorkflowDefinitionController.deleteWorkflowDefinitionByCode",
    release_operation="WorkflowDefinitionController.releaseWorkflowDefinition",
)
_WORKFLOW_DELETE_LINEAGE_GUARD = replace(
    _WORKFLOW,
    definition_delete_lineage_guard=True,
)

_EXECUTION_139 = ExecutionRecipe(
    operation="ExecutorController.startProcessInstance",
    result="none",
    task_scope="supported",
    start_node_identity="name",
    tenant_code=False,
    environment_code=False,
    start_params=False,
    execution_dry_run=False,
    expected_parallelism_number=False,
    complement_dependent_mode=False,
    all_level_dependent=False,
    execution_order=False,
    timeout=True,
    test_flag=False,
    version=False,
)
_EXECUTION_20 = replace(
    _EXECUTION_139,
    task_scope="supported",
    start_node_identity="code",
    environment_code=True,
    start_params=True,
    execution_dry_run=True,
    expected_parallelism_number=True,
)
_EXECUTION_30 = replace(_EXECUTION_20, complement_dependent_mode=True)
_EXECUTION_320 = replace(
    _EXECUTION_30,
    result="trigger-code",
    tenant_code=True,
    all_level_dependent=True,
    execution_order=True,
    test_flag=True,
    version=True,
)
_EXECUTION_WORKFLOW = replace(
    _EXECUTION_320,
    operation="ExecutorController.triggerWorkflowDefinition",
    result="id-list",
    timeout=False,
    test_flag=False,
    version=False,
)

_LINEAGE_ABSENT = LineageRecipe(
    graph_support="absent",
    graph_projection="absent",
    list_operation=None,
    get_operation=None,
    dependent_support="absent",
    dependent_projection="absent",
    dependent_operation=None,
)
_LINEAGE_LEGACY = LineageRecipe(
    graph_support="supported",
    graph_projection="legacy-direct",
    list_operation="WorkFlowLineageController.queryWorkFlowLineage",
    get_operation="WorkFlowLineageController.queryWorkFlowLineageByCode",
    dependent_support="absent",
    dependent_projection="absent",
    dependent_operation=None,
)
_LINEAGE_322 = replace(
    _LINEAGE_LEGACY,
    dependent_support="supported",
    dependent_projection="task-main-info",
    dependent_operation=("WorkFlowLineageController.queryDownstreamDependentTaskList"),
)
_LINEAGE_CURRENT = LineageRecipe(
    graph_support="supported",
    graph_projection="canonical",
    list_operation="WorkflowLineageController.queryWorkFlowLineage",
    get_operation="WorkflowLineageController.queryWorkFlowLineageByCode",
    dependent_support="supported",
    dependent_projection="dependent-lineage-task",
    dependent_operation="WorkflowLineageController.queryDependentTasks",
)


WORKFLOW_CONTRACTS: dict[str, WorkflowVersionContract] = {
    "1.3.9": WorkflowVersionContract(
        "1.3.9", _LEGACY_DEFINITION, _EXECUTION_139, _LINEAGE_ABSENT
    ),
    "2.0.0": WorkflowVersionContract(
        "2.0.0", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.1": WorkflowVersionContract(
        "2.0.1", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.2": WorkflowVersionContract(
        "2.0.2", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.3": WorkflowVersionContract(
        "2.0.3", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.4": WorkflowVersionContract(
        "2.0.4", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.5": WorkflowVersionContract(
        "2.0.5", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.6": WorkflowVersionContract(
        "2.0.6", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.7": WorkflowVersionContract(
        "2.0.7", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.8": WorkflowVersionContract(
        "2.0.8", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "2.0.9": WorkflowVersionContract(
        "2.0.9", _PROCESS_20, _EXECUTION_20, _LINEAGE_LEGACY
    ),
    "3.0.0": WorkflowVersionContract(
        "3.0.0", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.0.1": WorkflowVersionContract(
        "3.0.1", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.0.2": WorkflowVersionContract(
        "3.0.2", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.0.3": WorkflowVersionContract(
        "3.0.3", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.0.4": WorkflowVersionContract(
        "3.0.4", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.0.5": WorkflowVersionContract(
        "3.0.5", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.0.6": WorkflowVersionContract(
        "3.0.6", _PROCESS_30, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.0": WorkflowVersionContract(
        "3.1.0", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.1": WorkflowVersionContract(
        "3.1.1", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.2": WorkflowVersionContract(
        "3.1.2", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.3": WorkflowVersionContract(
        "3.1.3", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.4": WorkflowVersionContract(
        "3.1.4", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.5": WorkflowVersionContract(
        "3.1.5", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.6": WorkflowVersionContract(
        "3.1.6", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.7": WorkflowVersionContract(
        "3.1.7", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.8": WorkflowVersionContract(
        "3.1.8", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.1.9": WorkflowVersionContract(
        "3.1.9", _PROCESS_31, _EXECUTION_30, _LINEAGE_LEGACY
    ),
    "3.2.0": WorkflowVersionContract(
        "3.2.0", _PROCESS_320, _EXECUTION_320, _LINEAGE_LEGACY
    ),
    "3.2.1": WorkflowVersionContract(
        "3.2.1", _PROCESS_321, _EXECUTION_320, _LINEAGE_LEGACY
    ),
    "3.2.2": WorkflowVersionContract(
        "3.2.2", _PROCESS_321, _EXECUTION_320, _LINEAGE_322
    ),
    "3.3.1": WorkflowVersionContract(
        "3.3.1",
        _WORKFLOW_DELETE_LINEAGE_GUARD,
        _EXECUTION_WORKFLOW,
        _LINEAGE_CURRENT,
    ),
    "3.3.2": WorkflowVersionContract(
        "3.3.2",
        _WORKFLOW_DELETE_LINEAGE_GUARD,
        _EXECUTION_WORKFLOW,
        _LINEAGE_CURRENT,
    ),
    "3.4.0": WorkflowVersionContract(
        "3.4.0", _WORKFLOW, _EXECUTION_WORKFLOW, _LINEAGE_CURRENT
    ),
    "3.4.1": WorkflowVersionContract(
        "3.4.1", _WORKFLOW, _EXECUTION_WORKFLOW, _LINEAGE_CURRENT
    ),
    "3.4.2": WorkflowVersionContract(
        "3.4.2", _WORKFLOW, _EXECUTION_WORKFLOW, _LINEAGE_CURRENT
    ),
    "3.4.3": WorkflowVersionContract(
        "3.4.3", _WORKFLOW, _EXECUTION_WORKFLOW, _LINEAGE_CURRENT
    ),
}


def workflow_contract(version: str) -> WorkflowVersionContract:
    """Return one reviewed exact contract without range inference."""
    try:
        return WORKFLOW_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed workflow-family contract"
        raise ValueError(message) from exc


def terminal_decisions(version: str) -> dict[str, TerminalDecision]:
    """Return terminally absent/limited actions for one exact release."""
    contract = workflow_contract(version)
    decisions: dict[str, TerminalDecision] = {}
    definition = contract.definition
    if definition.authoring_support != "supported":
        for operation in ("workflow.create", "workflow.edit"):
            decisions[operation] = _terminal(
                version,
                operation,
                reason="upstream_capability_limited",
                source=_definition_controller(version),
                symbol=definition.create_operation.partition(".")[2],
                constraint=(
                    "DS 1.3.9 stores string-native tasks in processDefinitionJson; "
                    "the stable code/version-native authoring contract cannot be "
                    "projected without inventing task identities."
                ),
            )
    if definition.dag_support != "supported":
        for operation in ("workflow.describe", "workflow.digest", "workflow.export"):
            decisions[operation] = _terminal(
                version,
                operation,
                reason="upstream_capability_limited",
                source=_definition_controller(version),
                symbol=definition.detail_operation.partition(".")[2],
                constraint=(
                    "DS 1.3.9 exposes a string-native legacy graph that cannot be "
                    "losslessly represented by WorkflowDagRecord."
                ),
            )
    if contract.execution.task_scope != "supported":
        decisions["workflow.run-task"] = _terminal(
            version,
            "workflow.run-task",
            reason="upstream_capability_limited",
            source=_EXECUTOR_CONTROLLER,
            symbol=contract.execution.operation.partition(".")[2],
            constraint=(
                "DS 1.3.9 addresses start nodes by legacy string task name; the "
                "stable task selector resolves code-native task identities."
            ),
        )
    lineage = contract.lineage
    if lineage.graph_support == "absent":
        for operation in (
            "workflow.lineage.list",
            "workflow.lineage.get",
            "workflow.lineage.dependent-tasks",
        ):
            decisions[operation] = _terminal(
                version,
                operation,
                reason="upstream_capability_absent",
                source=_definition_controller(version),
                symbol="",
                constraint="This DolphinScheduler release has no lineage controller.",
            )
    elif lineage.dependent_support == "absent":
        decisions["workflow.lineage.dependent-tasks"] = _terminal(
            version,
            "workflow.lineage.dependent-tasks",
            reason="upstream_capability_absent",
            source=_lineage_controller(version),
            symbol=_required_operation(
                lineage.get_operation,
                version=version,
            ).partition(".")[2],
            constraint=(
                "The lineage controller exposes graph reads but no dependent-task "
                "query in this exact release."
            ),
        )
    return decisions


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return exact generated source closure for executable workflow actions."""
    contract = workflow_contract(version)
    definition = contract.definition
    execution = contract.execution
    lineage = contract.lineage
    terminal = terminal_decisions(version)
    project_scope = (
        "ProjectController.queryProjectListPaging",
        (
            "ProjectController.queryProjectById"
            if definition.native_identity == "id"
            else "ProjectController.queryProjectByCode"
        ),
    )
    workflow_scope = (
        *project_scope,
        (
            "ProcessDefinitionController.queryProcessDefinitionList"
            if definition.family == "legacy-json"
            else (
                "ProcessDefinitionController.queryProcessDefinitionSimpleList"
                if definition.family == "process"
                else "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList"
            )
        ),
        definition.detail_operation,
    )
    schedule_scope = (*workflow_scope, "SchedulerController.queryScheduleListPaging")
    preference = (
        ("ProjectPreferenceController.queryProjectPreferenceByProjectCode",)
        if version
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
        else ()
    )
    execution_sources: tuple[str, ...] = (execution.operation,)
    if execution.result == "trigger-code":
        execution_sources += (
            "ProcessInstanceController.queryProcessInstancesByTriggerCode",
        )
    code_allocation = (
        ()
        if definition.code_allocation_operation is None
        else (definition.code_allocation_operation,)
    )
    definition_delete_lineage_preflight = (
        (_required_operation(lineage.get_operation, version=version),)
        if definition.definition_delete_lineage_guard
        else ()
    )
    sources: dict[str, tuple[str, ...]] = {
        "workflow.delete": (
            *schedule_scope,
            *definition_delete_lineage_preflight,
            definition.delete_operation,
        ),
        "workflow.online": (*schedule_scope, definition.release_operation),
        "workflow.offline": (*schedule_scope, definition.release_operation),
        "workflow.run": (*workflow_scope, *preference, *execution_sources),
        "workflow.backfill": (*workflow_scope, *preference, *execution_sources),
    }
    if "workflow.create" not in terminal:
        sources["workflow.create"] = (
            *project_scope,
            *code_allocation,
            definition.create_operation,
        )
    if "workflow.edit" not in terminal:
        sources["workflow.edit"] = (
            *schedule_scope,
            *code_allocation,
            definition.update_operation,
        )
    if "workflow.run-task" not in terminal:
        sources["workflow.run-task"] = (
            *workflow_scope,
            *preference,
            *execution_sources,
        )
    for operation in ("workflow.describe", "workflow.digest", "workflow.export"):
        if operation not in terminal:
            sources[operation] = schedule_scope
    if lineage.graph_support == "supported":
        sources["workflow.lineage.list"] = (
            *project_scope,
            _required_operation(lineage.list_operation, version=version),
        )
        sources["workflow.lineage.get"] = (
            *workflow_scope,
            _required_operation(lineage.get_operation, version=version),
        )
    if lineage.dependent_support == "supported":
        sources["workflow.lineage.dependent-tasks"] = (
            *workflow_scope,
            _required_operation(lineage.dependent_operation, version=version),
        )
    return sources


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit structured-type roots beyond operation-derived closure."""
    return dict.fromkeys(
        semantic_operation_sources(version),
        ("org.apache.dolphinscheduler.api.utils.Result",),
    )


def semantic_operation_enum_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit enum roots beyond operation-derived closure."""
    return dict.fromkeys(semantic_operation_sources(version), ())


def semantic_operation_facets(version: str) -> dict[str, dict[str, object]]:
    """Return compact reviewed wire facets for every executable action."""
    contract = workflow_contract(version)
    definition = contract.definition
    execution = contract.execution
    lineage = contract.lineage
    facets: dict[str, dict[str, object]] = {}
    for operation in semantic_operation_sources(version):
        value: dict[str, object] = {
            "definition_family": definition.family,
            "project_route": definition.project_route,
            "native_identity": definition.native_identity,
        }
        if operation in {"workflow.create", "workflow.edit"}:
            value.update(
                {
                    "tenant_code": definition.tenant_code,
                    "other_params": (
                        definition.create_other_params
                        if operation == "workflow.create"
                        else definition.update_other_params
                    ),
                    "execution_type": definition.execution_type,
                }
            )
        if operation == "workflow.delete":
            value["definition_delete_lineage_guard"] = (
                definition.definition_delete_lineage_guard
            )
        if operation in {
            "workflow.run",
            "workflow.run-task",
            "workflow.backfill",
        }:
            value.update(
                {
                    "execution_result": execution.result,
                    "start_node_identity": execution.start_node_identity,
                    "tenant_code": execution.tenant_code,
                    "environment_code": execution.environment_code,
                    "execution_dry_run": execution.execution_dry_run,
                    "complement_dependent_mode": (execution.complement_dependent_mode),
                    "all_level_dependent": execution.all_level_dependent,
                    "execution_order": execution.execution_order,
                }
            )
        if operation.startswith("workflow.lineage."):
            value.update(
                {
                    "lineage_projection": lineage.graph_projection,
                    "dependent_projection": lineage.dependent_projection,
                }
            )
        facets[operation] = value
    return facets


def runtime_profile_facts() -> dict[str, dict[str, object]]:
    """Project reviewed contracts into the lightweight publishable runtime facts."""
    facts: dict[str, dict[str, object]] = {}
    for version, contract in WORKFLOW_CONTRACTS.items():
        definition = contract.definition
        execution = contract.execution
        lineage = contract.lineage
        warning_group_omission: WarningGroupOmission = (
            "zero" if version in WARNING_GROUP_PRIMITIVE_VERSIONS else "omit"
        )
        execution_schedule_time_shape: ExecutionScheduleTimeShape = (
            "comma-range"
            if version in COMMA_RANGE_EXECUTION_SCHEDULE_VERSIONS
            else "json"
        )
        facts[version] = {
            "family": definition.family,
            "native_identity": definition.native_identity,
            "create_supported": definition.authoring_support == "supported",
            "dag_supported": definition.dag_support == "supported",
            "task_scope_supported": execution.task_scope == "supported",
            "definition_tenant_code": definition.tenant_code,
            "execution_tenant_code": execution.tenant_code,
            "create_other_params": definition.create_other_params,
            "update_other_params": definition.update_other_params,
            "execution_type": definition.execution_type,
            "execution_result": execution.result,
            "environment_code": execution.environment_code,
            "start_params": execution.start_params,
            "execution_dry_run": execution.execution_dry_run,
            "expected_parallelism_number": execution.expected_parallelism_number,
            "complement_dependent_mode": execution.complement_dependent_mode,
            "all_level_dependent": execution.all_level_dependent,
            "execution_order": execution.execution_order,
            "test_flag": execution.test_flag,
            "definition_version": execution.version,
            "timeout": execution.timeout,
            "warning_group_omission": warning_group_omission,
            "execution_schedule_time_shape": execution_schedule_time_shape,
            "lineage_projection": lineage.graph_projection,
            "dependent_projection": lineage.dependent_projection,
            "definition_delete_lineage_guard": (
                definition.definition_delete_lineage_guard
            ),
        }
    return facts


def workflow_profile_data(versions: tuple[str, ...] | None = None) -> dict[str, object]:
    """Return the complete publishable projection of reviewed workflow facts."""
    selected = select_exact_versions(
        TARGET_WORKFLOW_VERSIONS, versions, label="workflow reviews"
    )
    facts = runtime_profile_facts()
    return {
        "schema_version": WORKFLOW_PROFILE_SCHEMA_VERSION,
        "target_versions": list(selected),
        "profiles": {version: facts[version] for version in selected},
    }


def render_workflow_profiles(data: dict[str, object] | None = None) -> str:
    """Render compact runtime facts without duplicating reviewed decisions."""
    projected = workflow_profile_data() if data is None else data
    raw_versions = projected["target_versions"]
    raw_profiles = projected["profiles"]
    if not isinstance(raw_versions, list) or not isinstance(raw_profiles, dict):
        message = "workflow profile projection has an invalid shape"
        raise TypeError(message)

    profile_pool: list[object] = []
    profile_indexes: dict[str, int] = {}
    profile_specs: dict[str, int] = {}
    for raw_version in raw_versions:
        if not isinstance(raw_version, str):
            message = "workflow profile version must be a string"
            raise TypeError(message)
        profile = raw_profiles[raw_version]
        key = json.dumps(profile, sort_keys=True, separators=(",", ":"))
        profile_index = profile_indexes.get(key)
        if profile_index is None:
            profile_index = len(profile_pool)
            profile_indexes[key] = profile_index
            profile_pool.append(profile)
        profile_specs[raw_version] = profile_index

    compact = {
        "schema_version": projected["schema_version"],
        "target_versions": raw_versions,
        "profile_pool": profile_pool,
        "profile_specs": profile_specs,
    }
    payload = json.dumps(
        compact,
        ensure_ascii=True,
        indent=2,
        separators=(",", ": "),
    )
    return "\n".join(
        (
            "from __future__ import annotations",
            "",
            "import json as _json",
            "",
            "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
            "_WORKFLOW_PROFILE_JSON = r'''",
            payload,
            "'''",
            "WORKFLOW_PROFILE_DATA = _json.loads(_WORKFLOW_PROFILE_JSON)",
            "WORKFLOW_PROFILE_SCHEMA_VERSION = (",
            "    WORKFLOW_PROFILE_DATA['schema_version']",
            ")",
            "TARGET_WORKFLOW_VERSIONS = tuple(",
            "    WORKFLOW_PROFILE_DATA['target_versions']",
            ")",
            "_WORKFLOW_PROFILE_POOL = WORKFLOW_PROFILE_DATA['profile_pool']",
            "WORKFLOW_PROFILE_FACTS = {",
            "    _version: dict(_WORKFLOW_PROFILE_POOL[_profile_index])",
            "    for _version, _profile_index in (",
            "        WORKFLOW_PROFILE_DATA['profile_specs'].items()",
            "    )",
            "}",
            "",
        )
    )


def write_workflow_profiles(
    output_root: Path, *, versions: tuple[str, ...] | None = None
) -> Path:
    """Write workflow facts beside the exact generated wire bundles."""
    output_path = output_root / GENERATED_WORKFLOW_PROFILE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_workflow_profiles(workflow_profile_data(versions)), encoding="utf-8"
    )
    return output_path


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return controller plus UI evidence for each executable action."""
    evidence: dict[str, tuple[Evidence, ...]] = {}
    for operation, sources in semantic_operation_sources(version).items():
        controller_source = _controller_for_operation(version, operation)
        ui_source = _ui_for_operation(version, operation)
        conclusion = "exact controller request and UI invocation agree"
        if operation in {"workflow.run", "workflow.backfill"}:
            result = workflow_contract(version).execution.result
            conclusion = f"executor request is exact; result wire is {result}"
        action_evidence: tuple[Evidence, ...] = (
            Evidence(
                version,
                "controller",
                controller_source,
                ";".join(item.partition(".")[2] for item in sources),
                conclusion,
            ),
            Evidence(
                version,
                "ui",
                ui_source,
                operation.partition("workflow.")[2],
                conclusion,
            ),
        )
        if operation == "workflow.delete":
            action_evidence += _definition_delete_lineage_evidence(version)
        evidence[operation] = action_evidence
    return evidence


def _definition_delete_lineage_evidence(version: str) -> tuple[Evidence, ...]:
    """Return exact service/mapper evidence for the definition-delete guard."""
    definition = workflow_contract(version).definition
    if definition.definition_delete_lineage_guard:
        conclusion = (
            "whole-definition delete omits owner-lineage cleanup; the lineage read "
            "returns owner rows without a workflow-version filter"
        )
        return (
            Evidence(
                version,
                "service",
                _WORKFLOW_DEFINITION_SERVICE,
                "deleteWorkflowDefinitionByCode",
                conclusion,
            ),
            Evidence(
                version,
                "service",
                _WORKFLOW_LINEAGE_SERVICE,
                "queryWorkFlowLineageByCode",
                conclusion,
            ),
            Evidence(
                version,
                "mapper",
                _WORKFLOW_LINEAGE_MAPPER,
                "queryByWorkflowDefinitionCode",
                conclusion,
            ),
        )
    if version == "3.4.0":
        return (
            Evidence(
                version,
                "service",
                _WORKFLOW_DEFINITION_SERVICE,
                "deleteWorkflowDefinitionByCode",
                "whole-definition delete invokes owner-lineage cleanup",
            ),
        )
    return ()


def _terminal(
    version: str,
    semantic_operation: str,
    *,
    reason: TerminalReason,
    source: str,
    symbol: str,
    constraint: str,
) -> TerminalDecision:
    return TerminalDecision(
        semantic_operation=semantic_operation,
        reason=reason,
        constraint=constraint,
        evidence=Evidence(
            version,
            "controller",
            source,
            symbol,
            constraint,
        ),
    )


def _required_operation(value: str | None, *, version: str) -> str:
    if value is not None:
        return value
    message = f"DS {version} workflow recipe is missing a supported operation"
    raise ValueError(message)


def _definition_controller(version: str) -> str:
    if version == "1.3.9":
        return _DEFINITION_CONTROLLER_139
    if workflow_contract(version).definition.family == "process":
        return _DEFINITION_CONTROLLER_PROCESS
    return _DEFINITION_CONTROLLER_WORKFLOW


def _lineage_controller(version: str) -> str:
    return (
        _LINEAGE_CONTROLLER_LEGACY
        if workflow_contract(version).definition.family == "process"
        else _LINEAGE_CONTROLLER_CURRENT
    )


def _controller_for_operation(version: str, operation: str) -> str:
    if operation.startswith("workflow.lineage."):
        return _lineage_controller(version)
    if operation in {"workflow.run", "workflow.run-task", "workflow.backfill"}:
        return _EXECUTOR_CONTROLLER
    if operation in {"workflow.create", "workflow.edit"}:
        # The source closure also contains the task-code allocator, but the
        # definition controller remains the user-facing mutation contract.
        return _definition_controller(version)
    return _definition_controller(version)


def _ui_for_operation(version: str, operation: str) -> str:
    if operation.startswith("workflow.lineage."):
        return (
            _LEGACY_LINEAGE_UI
            if version
            in {
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
            }
            else _LINEAGE_UI
        )
    if operation in {"workflow.run", "workflow.run-task", "workflow.backfill"}:
        return (
            _LEGACY_DAG_UI
            if version
            in {
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
            }
            else _EXECUTOR_UI
        )
    if version in {
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
    }:
        return _LEGACY_DAG_UI
    return (
        _PROCESS_UI
        if workflow_contract(version).definition.family == "process"
        else _WORKFLOW_UI
    )


__all__ = [
    "GENERATED_WORKFLOW_PROFILE_PATH",
    "TARGET_WORKFLOW_VERSIONS",
    "WORKFLOW_CONTRACTS",
    "WORKFLOW_PROFILE_SCHEMA_VERSION",
    "WORKFLOW_SEMANTIC_OPERATIONS",
    "DefinitionRecipe",
    "Evidence",
    "ExecutionRecipe",
    "LineageRecipe",
    "TerminalDecision",
    "WorkflowVersionContract",
    "render_workflow_profiles",
    "runtime_profile_facts",
    "semantic_operation_enum_roots",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "terminal_decisions",
    "workflow_contract",
    "workflow_profile_data",
    "write_workflow_profiles",
]
