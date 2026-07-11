from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypedDict

from dsctl.cli_surface import stable_leaf_actions

DataShapeKind = Literal["page", "collection", "object", "summary"]


class DataShapeSchema(TypedDict, total=False):
    """Schema payload describing how callers can read command data rows."""

    kind: str
    row_path: str
    default_columns: list[str]
    column_discovery: str


@dataclass(frozen=True)
class DataShape:
    """One low-entropy row model shared by services and output rendering."""

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
    "alert-plugin.list": ("id", "instanceName", "alertPluginName"),
    "audit.list": ("modelType", "modelName", "operation", "userName", "createTime"),
    "cluster.list": ("code", "name", "config"),
    "datasource.list": ("id", "name", "type", "createTime"),
    "environment.list": ("code", "name", "workerGroups", "description"),
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
    "enum.names": ("name", "list_command"),
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

_STABLE_LEAF_ACTIONS = stable_leaf_actions()


def _stable_get_defaults(
    list_defaults: dict[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    get_defaults: dict[str, tuple[str, ...]] = {}
    for action, columns in list_defaults.items():
        if not action.endswith(".list"):
            continue
        get_action = action.removesuffix(".list") + ".get"
        if get_action in _STABLE_LEAF_ACTIONS:
            get_defaults[get_action] = columns
    return get_defaults


OBJECT_DEFAULTS: dict[str, tuple[str, ...]] = {
    **_stable_get_defaults(PAGE_LIST_DEFAULTS),
    **_stable_get_defaults(COLLECTION_DEFAULTS),
    "datasource.get": ("id", "name", "type", "host", "port", "database"),
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
    "alert-plugin.definition.list": DataShape(
        kind="summary",
        row_path="data.definitions",
        default_columns=("id", "pluginName", "pluginType"),
    ),
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
    "task-type.get": DataShape(
        kind="summary",
        row_path="data.rows",
        default_columns=("kind", "name", "summary", "command"),
    ),
    "task-type.schema": DataShape(
        kind="summary",
        row_path="data.fields",
        default_columns=(
            "path",
            "type",
            "required",
            "default",
            "choice_source",
            "active_when",
        ),
    ),
    "template.environment": DataShape(
        kind="summary",
        row_path="data.lines",
        default_columns=("line", "purpose"),
    ),
    "template.cluster": DataShape(
        kind="summary",
        row_path="data.fields",
        default_columns=("name", "required", "value_type", "description"),
    ),
    "template.datasource": DataShape(
        kind="summary",
        row_path="data.rows",
    ),
    "template.task": DataShape(
        kind="summary",
        row_path="data.rows",
        default_columns=(
            "task_type",
            "kind",
            "category",
            "default_variant",
            "next_command",
        ),
    ),
    "template.workflow": DataShape(
        kind="summary",
        row_path="data.lines",
        default_columns=("line_no", "line"),
    ),
    "template.workflow-patch": DataShape(
        kind="summary",
        row_path="data.lines",
        default_columns=("line_no", "line"),
    ),
    "template.workflow-instance-patch": DataShape(
        kind="summary",
        row_path="data.lines",
        default_columns=("line_no", "line"),
    ),
    "workflow.lineage.list": DataShape(
        kind="summary",
        row_path="data.workFlowRelationDetailList",
        default_columns=("workFlowCode", "workFlowName", "workFlowPublishStatus"),
    ),
}

SCHEMA_VIEW_SHAPES: dict[str, DataShape] = {
    "index": DataShape(
        kind="summary",
        row_path="data.groups",
        default_columns=("name", "summary", "action_count", "schema_command"),
    ),
    "groups": DataShape(
        kind="summary",
        row_path="data",
        default_columns=("name", "summary", "action_count", "schema_command"),
    ),
    "commands": DataShape(
        kind="summary",
        row_path="data",
        default_columns=("action", "group", "name", "summary", "schema_command"),
    ),
    "group": DataShape(
        kind="summary",
        row_path="data.actions",
        default_columns=("action", "name", "summary", "schema_command"),
    ),
    "command": DataShape(
        kind="summary",
        row_path="data.command",
        default_columns=(
            "kind",
            "name",
            "flag",
            "type",
            "required",
            "value",
            "discovery_command",
            "invocation",
        ),
    ),
    "full": DataShape(
        kind="summary",
        row_path="data.commands",
        default_columns=("kind", "name", "summary"),
    ),
    "full_group": DataShape(
        kind="summary",
        row_path="data.rows",
        default_columns=("kind", "action", "name", "summary", "schema_command"),
    ),
    "full_command": DataShape(
        kind="summary",
        row_path="data.rows",
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


def data_shape_for_action(action: str, *, view: str | None = None) -> DataShape | None:
    """Return display/schema row metadata for one stable command action."""
    if action == "schema":
        return SCHEMA_VIEW_SHAPES.get("index" if view is None else view)
    return DATA_SHAPES.get(action)


def data_shape_schema_for_action(
    action: str,
    *,
    view: str | None = None,
) -> DataShapeSchema | None:
    """Return one JSON-safe schema data-shape payload when available."""
    shape = data_shape_for_action(action, view=view)
    if shape is None:
        return None
    return shape.to_schema()


def schema_view_data_shapes() -> dict[str, DataShapeSchema]:
    """Return every view-dependent row model for the schema command."""
    return {view: shape.to_schema() for view, shape in SCHEMA_VIEW_SHAPES.items()}


__all__ = [
    "DataShape",
    "DataShapeSchema",
    "data_shape_for_action",
    "data_shape_schema_for_action",
    "schema_view_data_shapes",
]
