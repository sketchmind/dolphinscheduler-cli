"""Reviewed exact-version recipes for task groups and their queues.

Task groups do not exist in the supported 1.3/2.0 releases.  The first
controller appears in 3.0.0, its queue operation is renamed in 3.2.1, its
workflow-instance filter is renamed on the wire in 3.3.1, and create/update
begin returning their entity only in 3.2.2.  The table below records every tag
separately so equal recipes are an explicit review result, never range-based
version inference.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TARGET_TASK_GROUP_VERSIONS = (
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

TASK_GROUP_SEMANTIC_OPERATIONS = (
    "task-group.page",
    "task-group.get",
    "task-group.create",
    "task-group.update",
    "task-group.close",
    "task-group.start",
    "task-group.queue.page",
    "task-group.queue.force-start",
    "task-group.queue.set-priority",
)

Support = Literal["supported", "absent"]
MutationResult = Literal["none", "entity"]
QueueFilter = Literal["processInstanceName", "workflowInstanceName"]
QueueIdentity = Literal["process", "workflow"]
EvidenceKind = Literal["controller", "ui", "mapper", "snapshot"]


@dataclass(frozen=True)
class Evidence:
    """One exact source coordinate supporting a task-group decision."""

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
class TaskGroupRecipe:
    """Wire and result decisions for one exact task-group controller."""

    support: Support
    queue_operation: str | None
    queue_workflow_filter: QueueFilter | None
    queue_identity: QueueIdentity | None
    create_result: MutationResult
    update_result: MutationResult
    numeric_group_status: bool
    typed_results: bool


@dataclass(frozen=True)
class QueuePageProjection:
    """Columns actually selected by the native queue paging query."""

    task_id_projected: bool
    in_queue_projected: bool
    select_sha256: str


@dataclass(frozen=True)
class TaskGroupVersionContract:
    """Reviewed task-group contract for one exact DolphinScheduler tag."""

    version: str
    task_group: TaskGroupRecipe
    queue_page_projection: QueuePageProjection | None = None


@dataclass(frozen=True)
class TerminalDecision:
    """One stable action proven absent in an exact upstream tag."""

    semantic_operation: str
    reason: Literal["upstream_capability_absent"]
    constraint: str
    evidence: Evidence


_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/TaskGroupController.java"
)
_UI = "dolphinscheduler-ui/src/service/modules/task-group/index.ts"
_PAGE_INFO_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"
_TASK_GROUP_MODEL = "org.apache.dolphinscheduler.dao.entity.TaskGroup"
_TASK_GROUP_QUEUE_MODEL = "org.apache.dolphinscheduler.dao.entity.TaskGroupQueue"
_QUEUE_PAGE_MAPPER = (
    "dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/"
    "dao/mapper/TaskGroupQueueMapper.xml"
)

_QUEUE_PAGE_EARLY = QueuePageProjection(
    task_id_projected=False,
    in_queue_projected=False,
    select_sha256="be9f7536864d249419ef6404a9e5bbfe82f79b266f18b31b12917fa7a225ed65",
)
_QUEUE_PAGE_PROCESS = QueuePageProjection(
    task_id_projected=False,
    in_queue_projected=True,
    select_sha256="32ad6b8d625182c285539c53778e93d52e3617719e86131bafb82b84a0c68a2c",
)
_QUEUE_PAGE_WORKFLOW = QueuePageProjection(
    task_id_projected=False,
    in_queue_projected=True,
    select_sha256="f8b2ae44084b9e41aac671a933a6a8343d94205b0054b54e54121ba38e3235b8",
)

_ABSENT = TaskGroupRecipe(
    support="absent",
    queue_operation=None,
    queue_workflow_filter=None,
    queue_identity=None,
    create_result="none",
    update_result="none",
    numeric_group_status=False,
    typed_results=False,
)
_QUEUE_LEGACY_VOID = TaskGroupRecipe(
    support="supported",
    queue_operation="TaskGroupController.queryTasksByGroupId",
    queue_workflow_filter="processInstanceName",
    queue_identity="process",
    create_result="none",
    update_result="none",
    numeric_group_status=True,
    typed_results=False,
)
_QUEUE_RENAMED_VOID = TaskGroupRecipe(
    support="supported",
    queue_operation="TaskGroupController.queryTaskGroupQueues",
    queue_workflow_filter="processInstanceName",
    queue_identity="process",
    create_result="none",
    update_result="none",
    numeric_group_status=False,
    typed_results=False,
)
_QUEUE_RENAMED_ENTITY = TaskGroupRecipe(
    support="supported",
    queue_operation="TaskGroupController.queryTaskGroupQueues",
    queue_workflow_filter="processInstanceName",
    queue_identity="process",
    create_result="entity",
    update_result="entity",
    numeric_group_status=False,
    typed_results=False,
)
_WORKFLOW_FILTER_ENTITY = TaskGroupRecipe(
    support="supported",
    queue_operation="TaskGroupController.queryTaskGroupQueues",
    queue_workflow_filter="workflowInstanceName",
    queue_identity="workflow",
    create_result="entity",
    update_result="entity",
    numeric_group_status=False,
    typed_results=False,
)
_TYPED_ENTITY = TaskGroupRecipe(
    support="supported",
    queue_operation="TaskGroupController.queryTaskGroupQueues",
    queue_workflow_filter="workflowInstanceName",
    queue_identity="workflow",
    create_result="entity",
    update_result="entity",
    numeric_group_status=False,
    typed_results=True,
)


TASK_GROUP_CONTRACTS: dict[str, TaskGroupVersionContract] = {
    "1.3.9": TaskGroupVersionContract("1.3.9", _ABSENT),
    "2.0.0": TaskGroupVersionContract("2.0.0", _ABSENT),
    "2.0.1": TaskGroupVersionContract("2.0.1", _ABSENT),
    "2.0.2": TaskGroupVersionContract("2.0.2", _ABSENT),
    "2.0.3": TaskGroupVersionContract("2.0.3", _ABSENT),
    "2.0.4": TaskGroupVersionContract("2.0.4", _ABSENT),
    "2.0.5": TaskGroupVersionContract("2.0.5", _ABSENT),
    "2.0.6": TaskGroupVersionContract("2.0.6", _ABSENT),
    "2.0.7": TaskGroupVersionContract("2.0.7", _ABSENT),
    "2.0.8": TaskGroupVersionContract("2.0.8", _ABSENT),
    "2.0.9": TaskGroupVersionContract("2.0.9", _ABSENT),
    "3.0.0": TaskGroupVersionContract("3.0.0", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.0.1": TaskGroupVersionContract("3.0.1", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.0.2": TaskGroupVersionContract("3.0.2", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.0.3": TaskGroupVersionContract("3.0.3", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.0.4": TaskGroupVersionContract("3.0.4", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.0.5": TaskGroupVersionContract("3.0.5", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.0.6": TaskGroupVersionContract("3.0.6", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.0": TaskGroupVersionContract("3.1.0", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.1": TaskGroupVersionContract("3.1.1", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.2": TaskGroupVersionContract("3.1.2", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.3": TaskGroupVersionContract("3.1.3", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.4": TaskGroupVersionContract("3.1.4", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.5": TaskGroupVersionContract("3.1.5", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.6": TaskGroupVersionContract("3.1.6", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.7": TaskGroupVersionContract("3.1.7", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.8": TaskGroupVersionContract("3.1.8", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.1.9": TaskGroupVersionContract("3.1.9", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.2.0": TaskGroupVersionContract("3.2.0", _QUEUE_LEGACY_VOID, _QUEUE_PAGE_EARLY),
    "3.2.1": TaskGroupVersionContract(
        "3.2.1", _QUEUE_RENAMED_VOID, _QUEUE_PAGE_PROCESS
    ),
    "3.2.2": TaskGroupVersionContract(
        "3.2.2", _QUEUE_RENAMED_ENTITY, _QUEUE_PAGE_PROCESS
    ),
    "3.3.1": TaskGroupVersionContract(
        "3.3.1", _WORKFLOW_FILTER_ENTITY, _QUEUE_PAGE_WORKFLOW
    ),
    "3.3.2": TaskGroupVersionContract(
        "3.3.2", _WORKFLOW_FILTER_ENTITY, _QUEUE_PAGE_WORKFLOW
    ),
    "3.4.0": TaskGroupVersionContract(
        "3.4.0", _WORKFLOW_FILTER_ENTITY, _QUEUE_PAGE_WORKFLOW
    ),
    "3.4.1": TaskGroupVersionContract(
        "3.4.1", _WORKFLOW_FILTER_ENTITY, _QUEUE_PAGE_WORKFLOW
    ),
    "3.4.2": TaskGroupVersionContract("3.4.2", _TYPED_ENTITY, _QUEUE_PAGE_WORKFLOW),
    "3.4.3": TaskGroupVersionContract("3.4.3", _TYPED_ENTITY, _QUEUE_PAGE_WORKFLOW),
}


def task_group_contract(version: str) -> TaskGroupVersionContract:
    """Return one reviewed exact contract, rejecting version inference."""
    try:
        return TASK_GROUP_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed task-group contract"
        raise ValueError(message) from exc


def action_support(version: str) -> dict[str, Support]:
    """Return support for every stable task-group semantic operation."""
    support = task_group_contract(version).task_group.support
    return dict.fromkeys(TASK_GROUP_SEMANTIC_OPERATIONS, support)


def terminal_decisions(version: str) -> dict[str, TerminalDecision]:
    """Return evidence-backed zero-request terminal coordinates."""
    if task_group_contract(version).task_group.support == "supported":
        return {}
    constraint = (
        f"DolphinScheduler {version} predates task groups; no task-group or "
        "task-group queue request can be represented for this server."
    )
    evidence = Evidence(
        version=version,
        kind="snapshot",
        source=f"build/ds_contract/snapshots-v2/ds-{version}-contract.json",
        symbol="operations",
        conclusion=(
            "The exact source inventory contains no TaskGroupController; the "
            "domain is introduced in DolphinScheduler 3.0.0."
        ),
    )
    return {
        operation: TerminalDecision(
            semantic_operation=operation,
            reason="upstream_capability_absent",
            constraint=constraint,
            evidence=evidence,
        )
        for operation in TASK_GROUP_SEMANTIC_OPERATIONS
    }


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return complete source closure for each executable stable operation."""
    recipe = task_group_contract(version).task_group
    if recipe.support == "absent":
        return {}
    if recipe.queue_operation is None:
        message = f"DS {version} task-group queue operation is missing"
        raise ValueError(message)

    page = "TaskGroupController.queryAllTaskGroup"
    project_page = "TaskGroupController.queryTaskGroupByCode"
    return {
        "task-group.page": (page, project_page),
        "task-group.get": (page,),
        "task-group.create": (
            project_page,
            "TaskGroupController.createTaskGroup",
        ),
        "task-group.update": (
            page,
            project_page,
            "TaskGroupController.updateTaskGroup",
        ),
        "task-group.close": (page, "TaskGroupController.closeTaskGroup"),
        "task-group.start": (page, "TaskGroupController.startTaskGroup"),
        "task-group.queue.page": (page, recipe.queue_operation),
        "task-group.queue.force-start": ("TaskGroupController.forceStart",),
        "task-group.queue.set-priority": ("TaskGroupController.modifyPriority",),
    }


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit model roots required by each task-group runtime slice."""
    sources = semantic_operation_sources(version)
    roots: dict[str, tuple[str, ...]] = {}
    for operation in sources:
        if operation == "task-group.queue.page":
            roots[operation] = (
                _PAGE_INFO_MODEL,
                _TASK_GROUP_MODEL,
                _TASK_GROUP_QUEUE_MODEL,
            )
        elif operation.startswith("task-group.queue."):
            roots[operation] = ()
        else:
            roots[operation] = (_PAGE_INFO_MODEL, _TASK_GROUP_MODEL)
    return roots


def semantic_operation_enum_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return the empty explicit enum closure for task-group operations."""
    return dict.fromkeys(semantic_operation_sources(version), ())


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return exact controller, UI, and queue paging source evidence."""
    evidence: dict[str, tuple[Evidence, ...]] = {}
    for operation, source_operations in semantic_operation_sources(version).items():
        symbols = tuple(item.partition(".")[2] for item in source_operations)
        sources: tuple[Evidence, ...] = (
            Evidence(
                version=version,
                kind="controller",
                source=_CONTROLLER,
                symbol=";".join(symbols),
                conclusion=(
                    "Exact controller operations and wire parameters used by "
                    "the stable task-group action."
                ),
            ),
            Evidence(
                version=version,
                kind="ui",
                source=_UI,
                symbol="request",
                conclusion=(
                    "The retained UI confirms route selection, form transport, "
                    "and the user-visible task-group workflow."
                ),
            ),
        )
        if operation == "task-group.queue.page":
            sources += (
                Evidence(
                    version=version,
                    kind="mapper",
                    source=_QUEUE_PAGE_MAPPER,
                    symbol="queryTaskGroupQueueByTaskGroupIdPaging",
                    conclusion=(
                        "The exact paging SELECT determines which queue entity "
                        "columns can be read from this response."
                    ),
                ),
            )
        evidence[operation] = sources
    return evidence


def semantic_operation_facets(version: str) -> dict[str, dict[str, object]]:
    """Project reviewed task-group behavior into profile metadata."""
    reviewed = task_group_contract(version)
    recipe = reviewed.task_group
    facets: dict[str, dict[str, object]] = {}
    for operation in semantic_operation_sources(version):
        if operation == "task-group.page":
            facets[operation] = {
                "result": "page",
                "project_scoped_variant": True,
            }
        elif operation == "task-group.get":
            facets[operation] = {
                "result": "entity",
                "selection": "paged-id-or-exact-name",
            }
        elif operation in {"task-group.create", "task-group.update"}:
            mutation = operation.rpartition(".")[2]
            facets[operation] = {
                "result": getattr(recipe, f"{mutation}_result"),
                "verified_readback": True,
            }
        elif operation in {"task-group.close", "task-group.start"}:
            facets[operation] = {"result": "none", "verified_readback": True}
        elif operation == "task-group.queue.page":
            projection = reviewed.queue_page_projection
            if projection is None:
                message = f"DS {version} has no reviewed queue paging projection"
                raise ValueError(message)
            facets[operation] = {
                "result": "page",
                "operation": recipe.queue_operation,
                "workflow_instance_filter": recipe.queue_workflow_filter,
                "queue_identity": recipe.queue_identity,
                "paging_projection": {
                    "taskId": projection.task_id_projected,
                    "inQueue": projection.in_queue_projected,
                },
            }
        else:
            facets[operation] = {"result": "none"}
        facets[operation]["typed_results"] = recipe.typed_results
    return facets


__all__ = [
    "TARGET_TASK_GROUP_VERSIONS",
    "TASK_GROUP_CONTRACTS",
    "TASK_GROUP_SEMANTIC_OPERATIONS",
    "Evidence",
    "QueuePageProjection",
    "TaskGroupRecipe",
    "TaskGroupVersionContract",
    "TerminalDecision",
    "action_support",
    "semantic_operation_enum_roots",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "task_group_contract",
    "terminal_decisions",
]
