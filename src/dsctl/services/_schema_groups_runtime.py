from __future__ import annotations

from dsctl.services._schema_primitives import argument, command, group, option
from dsctl.services.monitor import MONITOR_SERVER_TYPE_CHOICES
from dsctl.services.pagination import DEFAULT_PAGE_SIZE
from dsctl.services.task_instance import (
    DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
)
from dsctl.services.workflow_instance import (
    DEFAULT_WATCH_INTERVAL_SECONDS,
    DEFAULT_WATCH_TIMEOUT_SECONDS,
)


def audit_group() -> dict[str, object]:
    """Build the audit command group schema."""
    return group(
        "audit",
        summary="Inspect DolphinScheduler audit logs and filter metadata.",
        commands=[
            command(
                "list",
                action="audit.list",
                summary="List audit-log rows with optional filters.",
                options=[
                    option(
                        "model-type",
                        value_type="string",
                        description="Audit model type filter. Repeat as needed.",
                        multiple=True,
                    ),
                    option(
                        "operation-type",
                        value_type="string",
                        description=("Audit operation type filter. Repeat as needed."),
                        multiple=True,
                    ),
                    option(
                        "start",
                        value_type="string",
                        description=(
                            "Start datetime in DS format 'YYYY-MM-DD HH:MM:SS'."
                        ),
                    ),
                    option(
                        "end",
                        value_type="string",
                        description=(
                            "End datetime in DS format 'YYYY-MM-DD HH:MM:SS'."
                        ),
                    ),
                    option(
                        "user-name",
                        value_type="string",
                        description="Filter by audit actor user name.",
                    ),
                    option(
                        "model-name",
                        value_type="string",
                        description="Filter by audited model name.",
                    ),
                    option(
                        "page-no",
                        value_type="integer",
                        description="Page number to fetch when not using --all.",
                        default=1,
                    ),
                    option(
                        "page-size",
                        value_type="integer",
                        description="Page size to request from the upstream API.",
                        default=DEFAULT_PAGE_SIZE,
                    ),
                    option(
                        "all",
                        value_type="boolean",
                        description="Fetch all remaining pages up to the safety limit.",
                        default=False,
                    ),
                ],
            ),
            command(
                "model-types",
                action="audit.model-types",
                summary="List DS audit model types.",
            ),
            command(
                "operation-types",
                action="audit.operation-types",
                summary="List DS audit operation types.",
            ),
        ],
    )


def monitor_group() -> dict[str, object]:
    """Build the monitor command group schema."""
    return group(
        "monitor",
        summary="Inspect DolphinScheduler platform health and runtime state.",
        commands=[
            command(
                "health",
                action="monitor.health",
                summary="Get the API server actuator health payload.",
            ),
            command(
                "server",
                action="monitor.server",
                summary="List registry-backed servers for one node type.",
                arguments=[
                    argument(
                        "node_type",
                        value_type="string",
                        description="Server node type.",
                        choices=list(MONITOR_SERVER_TYPE_CHOICES),
                    )
                ],
            ),
            command(
                "database",
                action="monitor.database",
                summary="List database health metrics reported by the monitor API.",
            ),
        ],
    )


def workflow_instance_group() -> dict[str, object]:
    """Build the workflow-instance command group schema."""
    return group(
        "workflow-instance",
        summary="Inspect DolphinScheduler workflow instances.",
        commands=[
            command(
                "list",
                action="workflow-instance.list",
                summary="List workflow instances using explicit runtime filters.",
                options=[
                    option(
                        "page-no",
                        value_type="integer",
                        description="Remote page number.",
                        default=1,
                    ),
                    option(
                        "page-size",
                        value_type="integer",
                        description="Remote page size.",
                        default=DEFAULT_PAGE_SIZE,
                    ),
                    option(
                        "all",
                        value_type="boolean",
                        description="Fetch all remaining pages up to the safety limit.",
                        default=False,
                    ),
                    option(
                        "project",
                        value_type="string",
                        description="Filter by project name.",
                        selector="opaque_name",
                    ),
                    option(
                        "workflow",
                        value_type="string",
                        description="Filter by workflow name.",
                        selector="opaque_name",
                    ),
                    option(
                        "state",
                        value_type="string",
                        description="Filter by DS workflow execution status name.",
                    ),
                ],
            ),
            command(
                "get",
                action="workflow-instance.get",
                summary="Get one workflow instance by id.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "parent",
                action="workflow-instance.parent",
                summary=(
                    "Return the parent workflow instance for one sub-workflow instance."
                ),
                arguments=[
                    argument(
                        "sub_workflow_instance",
                        value_type="integer",
                        description="Sub-workflow instance id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "digest",
                action="workflow-instance.digest",
                summary="Return one compact workflow-instance runtime digest.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "update",
                action="workflow-instance.update",
                summary="Edit one finished workflow instance from a YAML patch file.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Finished workflow instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "patch",
                        value_type="path",
                        description="Path to one workflow patch YAML file.",
                        required=True,
                    ),
                    option(
                        "sync-definition",
                        value_type="boolean",
                        description=(
                            "Also write the edited DAG back to the current "
                            "workflow definition."
                        ),
                        default=False,
                    ),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Compile the merged workflow-instance update payload "
                            "without sending it."
                        ),
                        default=False,
                    ),
                ],
            ),
            command(
                "watch",
                action="workflow-instance.watch",
                summary="Poll one workflow instance until it reaches a final state.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "interval-seconds",
                        value_type="integer",
                        description="Polling interval in seconds.",
                        default=DEFAULT_WATCH_INTERVAL_SECONDS,
                    ),
                    option(
                        "timeout-seconds",
                        value_type="integer",
                        description=(
                            "Maximum seconds to wait. Use 0 to wait indefinitely."
                        ),
                        default=DEFAULT_WATCH_TIMEOUT_SECONDS,
                    ),
                ],
            ),
            command(
                "stop",
                action="workflow-instance.stop",
                summary="Request stop for one workflow instance.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "rerun",
                action="workflow-instance.rerun",
                summary="Request rerun for one finished workflow instance.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "recover-failed",
                action="workflow-instance.recover-failed",
                summary="Recover one failed workflow instance from failed tasks.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "execute-task",
                action="workflow-instance.execute-task",
                summary="Execute one task inside one finished workflow instance.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description="Workflow instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "task",
                        value_type="string",
                        description=(
                            "Task name or task code within the workflow definition."
                        ),
                        required=True,
                        selector="name_or_code",
                    ),
                    option(
                        "scope",
                        value_type="string",
                        description="Task execution scope.",
                        default="self",
                        choices=["self", "pre", "post"],
                    ),
                ],
            ),
        ],
    )


def task_instance_group() -> dict[str, object]:
    """Build the task-instance command group schema."""
    return group(
        "task-instance",
        summary="Inspect and control DolphinScheduler task instances.",
        commands=[
            command(
                "list",
                action="task-instance.list",
                summary="List task instances inside one workflow instance.",
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to scope the "
                            "task-instance query."
                        ),
                        required=True,
                    ),
                    option(
                        "page-no",
                        value_type="integer",
                        description="Remote page number.",
                        default=1,
                    ),
                    option(
                        "page-size",
                        value_type="integer",
                        description="Remote page size.",
                        default=DEFAULT_PAGE_SIZE,
                    ),
                    option(
                        "all",
                        value_type="boolean",
                        description="Fetch all remaining pages up to the safety limit.",
                        default=False,
                    ),
                    option(
                        "search",
                        value_type="string",
                        description="Filter task instances by upstream searchVal.",
                    ),
                    option(
                        "state",
                        value_type="string",
                        description="Filter by DS task execution status name.",
                    ),
                ],
            ),
            command(
                "get",
                action="task-instance.get",
                summary="Get one task instance by id within one workflow instance.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning project."
                        ),
                        required=True,
                    )
                ],
            ),
            command(
                "watch",
                action="task-instance.watch",
                summary="Poll one task instance until it reaches a finished state.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning project."
                        ),
                        required=True,
                    ),
                    option(
                        "interval-seconds",
                        value_type="integer",
                        description="Polling interval in seconds.",
                        default=DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
                    ),
                    option(
                        "timeout-seconds",
                        value_type="integer",
                        description=(
                            "Maximum seconds to wait. Use 0 to wait indefinitely."
                        ),
                        default=DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
                    ),
                ],
            ),
            command(
                "sub-workflow",
                action="task-instance.sub-workflow",
                summary=(
                    "Return the child workflow instance for one SUB_WORKFLOW "
                    "task instance."
                ),
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to scope the "
                            "task-instance relation."
                        ),
                        required=True,
                    )
                ],
            ),
            command(
                "log",
                action="task-instance.log",
                summary="Fetch the tail of one task-instance log.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "tail",
                        value_type="integer",
                        description=(
                            "Return the last N log lines by chunking the "
                            "upstream logger API."
                        ),
                        default=200,
                    )
                ],
            ),
            command(
                "force-success",
                action="task-instance.force-success",
                summary="Force one failed task instance into FORCED_SUCCESS.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning project."
                        ),
                        required=True,
                    )
                ],
            ),
            command(
                "savepoint",
                action="task-instance.savepoint",
                summary="Request one savepoint for a running task instance.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning project."
                        ),
                        required=True,
                    )
                ],
            ),
            command(
                "stop",
                action="task-instance.stop",
                summary="Request stop for one task instance.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description="Task instance id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning project."
                        ),
                        required=True,
                    )
                ],
            ),
        ],
    )
