from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypedDict

DataShapeKind = Literal["page", "collection", "object", "summary"]


class DataShapeSchema(TypedDict, total=False):
    """Schema payload describing how callers can read command data rows."""

    kind: str
    row_path: str
    default_columns: list[str]
    column_discovery: str


@dataclass(frozen=True)
class DataShape:
    """One low-entropy row model shared by schema and display rendering."""

    kind: DataShapeKind
    row_path: str | None = None
    default_columns: tuple[str, ...] = ()
    column_discovery: str = "runtime_row_keys"

    def to_schema(self) -> DataShapeSchema:
        """Return the JSON-safe schema representation for this shape."""
        payload = DataShapeSchema(kind=self.kind)
        if self.row_path is not None:
            payload["row_path"] = self.row_path
        if self.default_columns:
            payload["default_columns"] = list(self.default_columns)
        payload["column_discovery"] = self.column_discovery
        return payload


PAGE_LIST_DEFAULTS: dict[str, tuple[str, ...]] = {
    "access-token.list": ("id", "userName", "expireTime"),
    "alert-group.list": ("id", "groupName", "description"),
    "alert-plugin.list": ("id", "pluginInstanceName", "pluginDefineName"),
    "audit.list": ("id", "modelName", "operationType", "userName", "createTime"),
    "cluster.list": ("code", "name", "config"),
    "datasource.list": ("id", "name", "type", "userName"),
    "env.list": ("code", "name", "workerGroups", "description"),
    "namespace.list": ("id", "namespace", "clusterName"),
    "project-parameter.list": ("code", "paramName", "paramValue", "paramDataType"),
    "project.list": ("code", "name", "description"),
    "queue.list": ("id", "queueName", "queue"),
    "resource.list": ("fullName", "fileName", "type", "size"),
    "schedule.list": (
        "id",
        "workflowDefinitionName",
        "releaseState",
        "startTime",
        "endTime",
    ),
    "task-group.list": ("id", "name", "groupSize", "status"),
    "task-group.queue.list": ("id", "taskName", "workflowInstanceName", "status"),
    "task-instance.list": (
        "id",
        "name",
        "state",
        "taskType",
        "startTime",
        "endTime",
        "duration",
        "host",
    ),
    "tenant.list": ("id", "tenantCode", "queueName", "description"),
    "user.list": ("id", "userName", "userType", "tenantCode", "state"),
    "worker-group.list": ("id", "name", "addrList", "description"),
    "workflow-instance.list": (
        "id",
        "name",
        "state",
        "scheduleTime",
        "startTime",
        "endTime",
        "duration",
        "host",
    ),
}

COLLECTION_DEFAULTS: dict[str, tuple[str, ...]] = {
    "audit.model-types": ("name",),
    "audit.operation-types": ("name",),
    "monitor.database": ("dbType", "state", "threadsConnections", "date"),
    "monitor.server": ("id", "host", "port", "lastHeartbeatTime"),
    "namespace.available": ("id", "namespace", "clusterName"),
    "project-worker-group.list": ("id", "workerGroup", "projectCode"),
    "task.list": ("code", "name", "version"),
    "workflow.lineage.dependent-tasks": (
        "workflowDefinitionName",
        "taskDefinitionName",
        "projectCode",
    ),
    "workflow.list": ("code", "name", "version"),
}

OBJECT_DEFAULTS: dict[str, tuple[str, ...]] = {
    **{
        action.removesuffix(".list") + ".get": columns
        for action, columns in PAGE_LIST_DEFAULTS.items()
    },
    **{
        action.removesuffix(".list") + ".get": columns
        for action, columns in COLLECTION_DEFAULTS.items()
        if action.endswith(".list")
    },
    "project-preference.get": (),
    "workflow.lineage.get": (),
    "workflow.describe": ("workflow", "tasks", "relations"),
    "workflow.digest": (
        "taskCount",
        "relationCount",
        "taskTypeCounts",
        "rootTasks",
        "leafTasks",
    ),
    "workflow-instance.digest": (
        "taskCount",
        "progress",
        "taskStateCounts",
        "runningTasks",
        "failedTasks",
    ),
}

NESTED_ROW_SHAPES: dict[str, DataShape] = {
    "doctor": DataShape(
        kind="summary",
        row_path="data.checks",
        default_columns=("name", "status", "message", "suggestion"),
    ),
    "enum.list": DataShape(
        kind="summary",
        row_path="data.members",
        default_columns=("name", "value", "attributes"),
    ),
    "task-type.list": DataShape(
        kind="summary",
        row_path="data.taskTypes",
        default_columns=("taskType", "taskCategory", "isCollection"),
    ),
    "workflow.lineage.list": DataShape(
        kind="summary",
        row_path="data.workFlowRelationDetailList",
        default_columns=("workFlowCode", "workFlowName", "workFlowPublishStatus"),
    ),
}

DATA_SHAPES: dict[str, DataShape] = {
    **{
        action: DataShape(
            kind="page",
            row_path="data.totalList",
            default_columns=columns,
        )
        for action, columns in PAGE_LIST_DEFAULTS.items()
    },
    **{
        action: DataShape(
            kind="collection",
            row_path="data",
            default_columns=columns,
        )
        for action, columns in COLLECTION_DEFAULTS.items()
    },
    **{
        action: DataShape(
            kind="object",
            row_path="data",
            default_columns=columns,
        )
        for action, columns in OBJECT_DEFAULTS.items()
    },
    **NESTED_ROW_SHAPES,
}


def data_shape_for_action(action: str) -> DataShape | None:
    """Return display/schema row metadata for one stable command action."""
    return DATA_SHAPES.get(action)


def data_shape_schema_for_action(action: str) -> DataShapeSchema | None:
    """Return one JSON-safe schema data-shape payload when available."""
    shape = data_shape_for_action(action)
    if shape is None:
        return None
    return shape.to_schema()


__all__ = [
    "DataShape",
    "DataShapeSchema",
    "data_shape_for_action",
    "data_shape_schema_for_action",
]
