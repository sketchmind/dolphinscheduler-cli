from __future__ import annotations

from dsctl.services._schema_primitives import (
    argument,
    command,
    confirm_risk_option,
    group,
    option,
)
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
                        description=(
                            "Audit model type filter. Repeat as needed; run "
                            "`dsctl audit model-types` to discover values."
                        ),
                        multiple=True,
                        discovery_command="dsctl audit model-types",
                    ),
                    option(
                        "operation-type",
                        value_type="string",
                        description=(
                            "Audit operation type filter. Repeat as needed; run "
                            "`dsctl audit operation-types` to discover values."
                        ),
                        multiple=True,
                        discovery_command="dsctl audit operation-types",
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
                        description=(
                            "Project name or code for project-scoped filters. "
                            "Run `dsctl project list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl project list",
                    ),
                    option(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or code filter. With --project, "
                            "resolved inside that project; run `dsctl workflow "
                            "list` to discover values."
                        ),
                        selector="opaque_name",
                        discovery_command="dsctl workflow list",
                    ),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter workflow instances by upstream searchVal; "
                            "requires --project."
                        ),
                    ),
                    option(
                        "executor",
                        value_type="string",
                        description="Filter by executor user name; requires --project.",
                    ),
                    option(
                        "host",
                        value_type="string",
                        description="Filter by workflow instance host.",
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
                        "state",
                        value_type="string",
                        description=(
                            "Filter by DS workflow execution status name. Run "
                            "`dsctl enum list workflow-execution-status` to "
                            "discover values."
                        ),
                        discovery_command="dsctl enum list workflow-execution-status",
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
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
                    )
                ],
            ),
            command(
                "export",
                action="workflow-instance.export",
                summary=(
                    "Export one workflow instance DAG as an editable YAML document."
                ),
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
                    )
                ],
                payload={
                    "format": "yaml",
                    "output": "raw_document",
                    "target_command": "dsctl workflow-instance edit ID --file FILE",
                },
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
                        description=(
                            "Sub-workflow instance id. Run `dsctl "
                            "workflow-instance list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
                    )
                ],
            ),
            command(
                "edit",
                action="workflow-instance.edit",
                summary=(
                    "Edit one finished workflow instance from a YAML patch or "
                    "full workflow YAML file."
                ),
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description=(
                            "Finished workflow instance id. Run `dsctl "
                            "workflow-instance list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
                    )
                ],
                options=[
                    option(
                        "patch",
                        value_type="path",
                        description=(
                            "Path to one workflow-instance patch YAML file. "
                            "Use exactly one of --patch or --file. Inspect the "
                            "current instance DAG with `dsctl workflow-instance "
                            "export ID`, then write only the intended "
                            "delta. Start from `dsctl template "
                            "workflow-instance-patch --raw`; "
                            "`tasks.create[]` uses full task fragments from "
                            "`dsctl template task`; `tasks.update[].set` uses "
                            "partial task fields discovered with `dsctl "
                            "task-type schema TYPE`."
                        ),
                        required=False,
                    ),
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one full workflow-instance YAML file "
                            "describing the desired repaired DAG state. Use "
                            "exactly one of --patch or --file. Start from "
                            "`dsctl workflow-instance export ID`; "
                            "use --dry-run to inspect the compiled diff. "
                            "Full-file edits match task identity by exact task "
                            "name and do not infer renames."
                        ),
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
                            "Compile the merged workflow-instance edit payload "
                            "without sending it."
                        ),
                        default=False,
                    ),
                    confirm_risk_option(),
                ],
                payload={
                    "format": "yaml",
                    "source_options": ["--patch PATH", "--file PATH"],
                    "patch_template_command": (
                        "dsctl template workflow-instance-patch --raw"
                    ),
                    "file_source_command": "dsctl workflow-instance export ID",
                    "target_commands": [
                        "dsctl workflow-instance edit ID --patch FILE",
                        "dsctl workflow-instance edit ID --file FILE",
                    ],
                },
            ),
            command(
                "watch",
                action="workflow-instance.watch",
                summary="Poll one workflow instance until it reaches a final state.",
                arguments=[
                    argument(
                        "workflow_instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Workflow instance id. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl workflow-instance list",
                    )
                ],
                options=[
                    option(
                        "task",
                        value_type="string",
                        description=(
                            "Task name or task code within the workflow "
                            "instance. Run `dsctl task-instance list "
                            "--workflow-instance WORKFLOW_INSTANCE` to discover "
                            "values."
                        ),
                        required=True,
                        selector="name_or_code",
                        discovery_command=(
                            "dsctl task-instance list --workflow-instance "
                            "WORKFLOW_INSTANCE"
                        ),
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
                summary="List task instances with project-scoped runtime filters.",
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to narrow the "
                            "task-instance query. Run `dsctl workflow-instance "
                            "list` to discover ids."
                        ),
                        discovery_command="dsctl workflow-instance list",
                    ),
                    option(
                        "project",
                        value_type="string",
                        description=(
                            "Project name or code for the project-scoped query. "
                            "Run `dsctl project list` to discover values; "
                            "required via flag or context when "
                            "--workflow-instance is omitted."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl project list",
                    ),
                    option(
                        "workflow-instance-name",
                        value_type="string",
                        description="Filter by workflow instance name.",
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
                        description=(
                            "Free-text upstream searchVal filter. Use --task for "
                            "an exact task instance name filter."
                        ),
                    ),
                    option(
                        "task",
                        value_type="string",
                        description="Filter by exact task instance name.",
                    ),
                    option(
                        "task-code",
                        value_type="integer",
                        description=(
                            "Filter by task definition code. Run `dsctl task "
                            "list` to discover values."
                        ),
                        discovery_command="dsctl task list",
                    ),
                    option(
                        "executor",
                        value_type="string",
                        description="Filter by executor user name.",
                    ),
                    option(
                        "state",
                        value_type="string",
                        description=(
                            "Filter by DS task execution status name. Run "
                            "`dsctl enum list task-execution-status` to discover "
                            "values."
                        ),
                        discovery_command="dsctl enum list task-execution-status",
                    ),
                    option(
                        "host",
                        value_type="string",
                        description="Filter by worker host.",
                    ),
                    option(
                        "start",
                        value_type="string",
                        description=(
                            "Task start-time lower bound in DS format "
                            "'YYYY-MM-DD HH:MM:SS'."
                        ),
                    ),
                    option(
                        "end",
                        value_type="string",
                        description=(
                            "Task start-time upper bound in DS format "
                            "'YYYY-MM-DD HH:MM:SS'."
                        ),
                    ),
                    option(
                        "execute-type",
                        value_type="string",
                        description=(
                            "Filter by DS task execute type: BATCH or STREAM. "
                            "Run `dsctl enum list task-execute-type` to discover "
                            "values."
                        ),
                        choices=["BATCH", "STREAM"],
                        discovery_command="dsctl enum list task-execute-type",
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
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning "
                            "project. Run `dsctl workflow-instance list` to "
                            "discover ids."
                        ),
                        required=True,
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning "
                            "project. Run `dsctl workflow-instance list` to "
                            "discover ids."
                        ),
                        required=True,
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to scope the "
                            "task-instance relation. Run `dsctl "
                            "workflow-instance list` to discover ids."
                        ),
                        required=True,
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
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
                    ),
                    option(
                        "raw",
                        value_type="boolean",
                        description=(
                            "Print only the log text, without the JSON envelope."
                        ),
                        default=False,
                    ),
                ],
                payload={
                    "raw_option": "--raw",
                    "raw_field": "data.text",
                },
            ),
            command(
                "force-success",
                action="task-instance.force-success",
                summary="Force one failed task instance into FORCED_SUCCESS.",
                arguments=[
                    argument(
                        "task_instance",
                        value_type="integer",
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning "
                            "project. Run `dsctl workflow-instance list` to "
                            "discover ids."
                        ),
                        required=True,
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning "
                            "project. Run `dsctl workflow-instance list` to "
                            "discover ids."
                        ),
                        required=True,
                        discovery_command="dsctl workflow-instance list",
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
                        description=(
                            "Task instance id. Run `dsctl task-instance list` "
                            "to discover ids."
                        ),
                        selector="id",
                        discovery_command="dsctl task-instance list",
                    )
                ],
                options=[
                    option(
                        "workflow-instance",
                        value_type="integer",
                        description=(
                            "Workflow instance id used to resolve the owning "
                            "project. Run `dsctl workflow-instance list` to "
                            "discover ids."
                        ),
                        required=True,
                        discovery_command="dsctl workflow-instance list",
                    )
                ],
            ),
        ],
    )
