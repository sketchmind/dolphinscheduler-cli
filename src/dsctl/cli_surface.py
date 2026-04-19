from __future__ import annotations

from dataclasses import dataclass

USE_RESOURCE = "use"
ENUM_RESOURCE = "enum"
LINT_RESOURCE = "lint"
ENV_RESOURCE = "environment"
CLUSTER_RESOURCE = "cluster"
DATASOURCE_RESOURCE = "datasource"
NAMESPACE_RESOURCE = "namespace"
RESOURCE_RESOURCE = "resource"
QUEUE_RESOURCE = "queue"
WORKER_GROUP_RESOURCE = "worker-group"
TASK_GROUP_RESOURCE = "task-group"
ALERT_PLUGIN_RESOURCE = "alert-plugin"
ALERT_GROUP_RESOURCE = "alert-group"
TENANT_RESOURCE = "tenant"
USER_RESOURCE = "user"
ACCESS_TOKEN_RESOURCE = "access-token"
MONITOR_RESOURCE = "monitor"
AUDIT_RESOURCE = "audit"
PROJECT_RESOURCE = "project"
PROJECT_PARAMETER_RESOURCE = "project-parameter"
PROJECT_PREFERENCE_RESOURCE = "project-preference"
PROJECT_WORKER_GROUP_RESOURCE = "project-worker-group"
SCHEDULE_RESOURCE = "schedule"
TEMPLATE_RESOURCE = "template"
TASK_TYPE_RESOURCE = "task-type"
WORKFLOW_RESOURCE = "workflow"
WORKFLOW_INSTANCE_RESOURCE = "workflow-instance"
TASK_RESOURCE = "task"
TASK_INSTANCE_RESOURCE = "task-instance"

GOVERNANCE_RESOURCES: tuple[str, ...] = (
    ENV_RESOURCE,
    CLUSTER_RESOURCE,
    DATASOURCE_RESOURCE,
    NAMESPACE_RESOURCE,
    RESOURCE_RESOURCE,
    QUEUE_RESOURCE,
    WORKER_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    ALERT_GROUP_RESOURCE,
    TENANT_RESOURCE,
    USER_RESOURCE,
    ACCESS_TOKEN_RESOURCE,
)
TOP_LEVEL_COMMANDS: tuple[str, ...] = (
    "version",
    "context",
    "doctor",
    "schema",
    "capabilities",
)
NAME_FIRST_RESOURCES: tuple[str, ...] = (
    PROJECT_RESOURCE,
    ENV_RESOURCE,
    CLUSTER_RESOURCE,
    DATASOURCE_RESOURCE,
    NAMESPACE_RESOURCE,
    QUEUE_RESOURCE,
    WORKER_GROUP_RESOURCE,
    TASK_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    ALERT_GROUP_RESOURCE,
    TENANT_RESOURCE,
    USER_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    WORKFLOW_RESOURCE,
    TASK_RESOURCE,
)
ID_FIRST_RESOURCES: tuple[str, ...] = (
    SCHEDULE_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    ACCESS_TOKEN_RESOURCE,
)
PATH_FIRST_RESOURCES: tuple[str, ...] = (RESOURCE_RESOURCE,)


@dataclass(frozen=True)
class SurfaceCommand:
    """One stable command or nested command group in the shared CLI surface."""

    name: str
    commands: tuple[SurfaceCommand, ...] = ()


def _surface_command(name: str, *commands: SurfaceCommand) -> SurfaceCommand:
    """Build one stable CLI surface command node."""
    return SurfaceCommand(name=name, commands=commands)


RESOURCE_COMMAND_TREE: dict[str, tuple[SurfaceCommand, ...]] = {
    USE_RESOURCE: (
        _surface_command(PROJECT_RESOURCE),
        _surface_command(WORKFLOW_RESOURCE),
    ),
    ENUM_RESOURCE: (_surface_command("names"), _surface_command("list")),
    LINT_RESOURCE: (_surface_command("workflow"),),
    ENV_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    CLUSTER_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    DATASOURCE_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
        _surface_command("test"),
    ),
    NAMESPACE_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("available"),
        _surface_command("create"),
        _surface_command("delete"),
    ),
    RESOURCE_RESOURCE: (
        _surface_command("list"),
        _surface_command("view"),
        _surface_command("upload"),
        _surface_command("create"),
        _surface_command("mkdir"),
        _surface_command("download"),
        _surface_command("delete"),
    ),
    QUEUE_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    WORKER_GROUP_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    TASK_GROUP_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("close"),
        _surface_command("start"),
        _surface_command(
            "queue",
            _surface_command("list"),
            _surface_command("force-start"),
            _surface_command("set-priority"),
        ),
    ),
    ALERT_PLUGIN_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("schema"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
        _surface_command("test"),
        _surface_command(
            "definition",
            _surface_command("list"),
        ),
    ),
    ALERT_GROUP_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    TENANT_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    USER_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
        _surface_command(
            "grant",
            _surface_command("project"),
            _surface_command("datasource"),
            _surface_command("namespace"),
        ),
        _surface_command(
            "revoke",
            _surface_command("project"),
            _surface_command("datasource"),
            _surface_command("namespace"),
        ),
    ),
    ACCESS_TOKEN_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
        _surface_command("generate"),
    ),
    MONITOR_RESOURCE: (
        _surface_command("health"),
        _surface_command("server"),
        _surface_command("database"),
    ),
    AUDIT_RESOURCE: (
        _surface_command("list"),
        _surface_command("model-types"),
        _surface_command("operation-types"),
    ),
    PROJECT_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    PROJECT_PARAMETER_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
    ),
    PROJECT_PREFERENCE_RESOURCE: (
        _surface_command("get"),
        _surface_command("update"),
        _surface_command("enable"),
        _surface_command("disable"),
    ),
    PROJECT_WORKER_GROUP_RESOURCE: (
        _surface_command("list"),
        _surface_command("set"),
        _surface_command("clear"),
    ),
    SCHEDULE_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("preview"),
        _surface_command("explain"),
        _surface_command("create"),
        _surface_command("update"),
        _surface_command("delete"),
        _surface_command("online"),
        _surface_command("offline"),
    ),
    TEMPLATE_RESOURCE: (
        _surface_command(WORKFLOW_RESOURCE),
        _surface_command("params"),
        _surface_command(ENV_RESOURCE),
        _surface_command(DATASOURCE_RESOURCE),
        _surface_command(TASK_RESOURCE),
    ),
    TASK_TYPE_RESOURCE: (_surface_command("list"),),
    WORKFLOW_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("describe"),
        _surface_command("digest"),
        _surface_command("create"),
        _surface_command("edit"),
        _surface_command("online"),
        _surface_command("offline"),
        _surface_command("run"),
        _surface_command("run-task"),
        _surface_command("backfill"),
        _surface_command("delete"),
        _surface_command(
            "lineage",
            _surface_command("list"),
            _surface_command("get"),
            _surface_command("dependent-tasks"),
        ),
    ),
    WORKFLOW_INSTANCE_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("parent"),
        _surface_command("digest"),
        _surface_command("update"),
        _surface_command("watch"),
        _surface_command("stop"),
        _surface_command("rerun"),
        _surface_command("recover-failed"),
        _surface_command("execute-task"),
    ),
    TASK_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("update"),
    ),
    TASK_INSTANCE_RESOURCE: (
        _surface_command("list"),
        _surface_command("get"),
        _surface_command("watch"),
        _surface_command("sub-workflow"),
        _surface_command("log"),
        _surface_command("force-success"),
        _surface_command("savepoint"),
        _surface_command("stop"),
    ),
}
RESOURCE_COMMANDS: dict[str, tuple[str, ...]] = {
    resource: tuple(command.name for command in commands)
    for resource, commands in RESOURCE_COMMAND_TREE.items()
}
COMMAND_GROUPS: tuple[str, ...] = tuple(RESOURCE_COMMAND_TREE)
META_PLANE_COMMANDS: tuple[str, ...] = (
    *TOP_LEVEL_COMMANDS,
    TEMPLATE_RESOURCE,
    TASK_TYPE_RESOURCE,
    USE_RESOURCE,
    ENUM_RESOURCE,
    LINT_RESOURCE,
)
PROJECT_PLANE_RESOURCES: tuple[str, ...] = (
    PROJECT_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    PROJECT_PREFERENCE_RESOURCE,
    PROJECT_WORKER_GROUP_RESOURCE,
    TASK_GROUP_RESOURCE,
)
DESIGN_PLANE_RESOURCES: tuple[str, ...] = (
    WORKFLOW_RESOURCE,
    TASK_RESOURCE,
    SCHEDULE_RESOURCE,
)
RUNTIME_PLANE_RESOURCES: tuple[str, ...] = (
    MONITOR_RESOURCE,
    AUDIT_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    TASK_INSTANCE_RESOURCE,
)
SURFACE_PLANES: dict[str, tuple[str, ...]] = {
    "meta": META_PLANE_COMMANDS,
    "project": PROJECT_PLANE_RESOURCES,
    "governance": GOVERNANCE_RESOURCES,
    "design": DESIGN_PLANE_RESOURCES,
    "runtime": RUNTIME_PLANE_RESOURCES,
}


def resource_label(resource: str) -> str:
    """Render one stable resource slug as a human-readable title."""
    return resource.replace("_", " ").replace("-", " ").title()
