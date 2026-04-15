from __future__ import annotations

from dsctl.services._schema_primitives import (
    argument,
    command,
    confirm_risk_option,
    group,
    option,
    project_option,
    workflow_option,
)
from dsctl.services.pagination import DEFAULT_PAGE_SIZE
from dsctl.services.template import (
    supported_parameter_syntax_topics,
    supported_task_template_variants,
)


def schedule_group() -> dict[str, object]:
    """Build the schedule command group schema."""
    return group(
        "schedule",
        summary="Manage DolphinScheduler schedules.",
        commands=[
            command(
                "list",
                action="schedule.list",
                summary="List schedules inside one project.",
                options=[
                    project_option(),
                    workflow_option(
                        description=(
                            "Exact workflow name or code to narrow the project "
                            "schedule list."
                        )
                    ),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter schedules by workflow name substring within the "
                            "selected project."
                        ),
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
                "get",
                action="schedule.get",
                summary="Get one schedule by id.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Schedule id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "preview",
                action="schedule.preview",
                summary="Preview the next fire times for a schedule.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Existing schedule id to preview.",
                        required=False,
                        selector="id",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "cron",
                        value_type="string",
                        description=(
                            "Quartz cron expression for an ad hoc preview "
                            "(6 or 7 fields, seconds first)."
                        ),
                    ),
                    option(
                        "start",
                        value_type="string",
                        description="Schedule start time in DS datetime string format.",
                    ),
                    option(
                        "end",
                        value_type="string",
                        description="Schedule end time in DS datetime string format.",
                    ),
                    option(
                        "timezone",
                        value_type="string",
                        description="Timezone id, for example Asia/Shanghai.",
                    ),
                ],
            ),
            command(
                "explain",
                action="schedule.explain",
                summary="Explain one schedule create or update mutation.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Existing schedule id to explain as an update.",
                        required=False,
                        selector="id",
                    )
                ],
                options=[
                    workflow_option(
                        description=(
                            "Workflow name or code. Falls back to workflow context "
                            "for create explain."
                        )
                    ),
                    project_option(),
                    option(
                        "cron",
                        value_type="string",
                        description=(
                            "Quartz cron expression (6 or 7 fields, seconds first)."
                        ),
                    ),
                    option(
                        "start",
                        value_type="string",
                        description="Schedule start time in DS datetime string format.",
                    ),
                    option(
                        "end",
                        value_type="string",
                        description="Schedule end time in DS datetime string format.",
                    ),
                    option(
                        "timezone",
                        value_type="string",
                        description="Timezone id, for example Asia/Shanghai.",
                    ),
                    option(
                        "failure-strategy",
                        value_type="string",
                        description="Failure strategy: CONTINUE or END.",
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id for create explain or updated value "
                            "for update explain. Create explain can also inherit an "
                            "enabled project preference when omitted."
                        ),
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description=(
                            "Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, "
                            "or LOWEST."
                        ),
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group for create explain or updated value for "
                            "update explain. Create explain can also inherit an "
                            "enabled project preference when omitted."
                        ),
                    ),
                    option(
                        "tenant-code",
                        value_type="string",
                        description=(
                            "Tenant code for create explain. Create explain can "
                            "also inherit an enabled project preference when omitted."
                        ),
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment code for create explain or updated value "
                            "for update explain. Create explain can also inherit "
                            "an enabled project preference when omitted."
                        ),
                    ),
                ],
            ),
            command(
                "create",
                action="schedule.create",
                summary="Create one schedule.",
                options=[
                    workflow_option(
                        description=(
                            "Workflow name or code. Falls back to workflow context."
                        )
                    ),
                    project_option(),
                    option(
                        "cron",
                        value_type="string",
                        description=(
                            "Quartz cron expression (6 or 7 fields, seconds first)."
                        ),
                        required=True,
                    ),
                    option(
                        "start",
                        value_type="string",
                        description="Schedule start time in DS datetime string format.",
                        required=True,
                    ),
                    option(
                        "end",
                        value_type="string",
                        description="Schedule end time in DS datetime string format.",
                        required=True,
                    ),
                    option(
                        "timezone",
                        value_type="string",
                        description="Timezone id, for example Asia/Shanghai.",
                        required=True,
                    ),
                    option(
                        "failure-strategy",
                        value_type="string",
                        description="Failure strategy: CONTINUE or END.",
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id. Omit to keep the CLI fallback "
                            "chain, including enabled project preference."
                        ),
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description=(
                            "Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, "
                            "or LOWEST."
                        ),
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group. Omit to allow enabled project preference."
                        ),
                    ),
                    option(
                        "tenant-code",
                        value_type="string",
                        description=(
                            "Tenant code. Omit to allow enabled project preference."
                        ),
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment code. Omit to keep the CLI fallback "
                            "chain, including enabled project preference."
                        ),
                    ),
                    confirm_risk_option(),
                ],
            ),
            command(
                "update",
                action="schedule.update",
                summary="Update one schedule.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Schedule id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "cron",
                        value_type="string",
                        description=(
                            "Updated Quartz cron expression (6 or 7 fields, "
                            "seconds first). Omit to keep the current value."
                        ),
                    ),
                    option(
                        "start",
                        value_type="string",
                        description=(
                            "Updated schedule start time. Omit to keep the "
                            "current value."
                        ),
                    ),
                    option(
                        "end",
                        value_type="string",
                        description=(
                            "Updated schedule end time. Omit to keep the current value."
                        ),
                    ),
                    option(
                        "timezone",
                        value_type="string",
                        description=(
                            "Updated timezone id. Omit to keep the current value."
                        ),
                    ),
                    option(
                        "failure-strategy",
                        value_type="string",
                        description="Failure strategy: CONTINUE or END.",
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Updated warning group id. Omit to keep the current value."
                        ),
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description=(
                            "Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, "
                            "or LOWEST."
                        ),
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Updated worker group. Omit to keep the current value."
                        ),
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Updated environment code. Omit to keep the current value."
                        ),
                    ),
                    confirm_risk_option(),
                ],
            ),
            command(
                "delete",
                action="schedule.delete",
                summary="Delete one schedule.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Schedule id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm schedule deletion without prompting.",
                        default=False,
                    )
                ],
            ),
            command(
                "online",
                action="schedule.online",
                summary="Bring one schedule online.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Schedule id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "offline",
                action="schedule.offline",
                summary="Bring one schedule offline.",
                arguments=[
                    argument(
                        "schedule_id",
                        value_type="integer",
                        description="Schedule id.",
                        selector="id",
                    )
                ],
            ),
        ],
    )


def template_group(task_types: list[str]) -> dict[str, object]:
    """Build the template command group schema."""
    return group(
        "template",
        summary="Emit stable YAML templates for workflow authoring.",
        commands=[
            command(
                "workflow",
                action="template.workflow",
                summary="Emit the stable workflow YAML template.",
                options=[
                    option(
                        "with-schedule",
                        value_type="boolean",
                        description=(
                            "Include one optional schedule block in the emitted "
                            "template."
                        ),
                        default=False,
                    )
                ],
            ),
            command(
                "params",
                action="template.params",
                summary="Emit DS parameter syntax metadata and examples.",
                options=[
                    option(
                        "topic",
                        value_type="string",
                        description=(
                            "Parameter syntax topic. Omit for compact discovery."
                        ),
                        choices=supported_parameter_syntax_topics(),
                    )
                ],
            ),
            command(
                "task",
                action="template.task",
                summary="Emit one task YAML template or list supported task types.",
                arguments=[
                    argument(
                        "task_type",
                        value_type="string",
                        description="Task type to template. Required unless --list.",
                        required=False,
                        choices=task_types,
                    )
                ],
                options=[
                    option(
                        "list",
                        value_type="boolean",
                        description=(
                            "List supported stable task template types instead "
                            "of emitting YAML."
                        ),
                        default=False,
                    ),
                    option(
                        "variant",
                        value_type="string",
                        description=(
                            "Task template scenario. Valid choices depend on "
                            "the selected task type and are discoverable through "
                            "`template task --list`."
                        ),
                        choices=supported_task_template_variants(),
                    ),
                ],
            ),
        ],
    )


def workflow_group() -> dict[str, object]:
    """Build the workflow command group schema."""
    return group(
        "workflow",
        summary="Manage DolphinScheduler workflows.",
        commands=[
            command(
                "list",
                action="workflow.list",
                summary="List workflows inside one project.",
                options=[
                    project_option(),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter workflows by name substring after fetching "
                            "the project list."
                        ),
                    ),
                ],
            ),
            command(
                "get",
                action="workflow.get",
                summary="Get one workflow by name or code.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "format",
                        value_type="string",
                        description="Output format.",
                        default="json",
                        choices=["json", "yaml"],
                    ),
                ],
            ),
            command(
                "describe",
                action="workflow.describe",
                summary="Describe one workflow with tasks and relations.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[project_option()],
            ),
            command(
                "digest",
                action="workflow.digest",
                summary="Return one compact workflow graph summary.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[project_option()],
            ),
            command(
                "create",
                action="workflow.create",
                summary="Create one workflow definition from a YAML file.",
                options=[
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one workflow YAML specification file. Start "
                            "from `dsctl template workflow` when authoring a new "
                            "file."
                        ),
                        required=True,
                    ),
                    option(
                        "project",
                        value_type="string",
                        description="Override workflow.project from the YAML file.",
                        selector="name_or_code",
                    ),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Compile the workflow payload without sending the "
                            "create request."
                        ),
                        default=False,
                    ),
                    confirm_risk_option(),
                ],
            ),
            command(
                "edit",
                action="workflow.edit",
                summary="Edit one workflow definition from a YAML patch file.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "patch",
                        value_type="path",
                        description=(
                            "Path to one workflow patch YAML file. Use --dry-run "
                            "to inspect the compiled diff before apply."
                        ),
                        required=True,
                    ),
                    project_option(),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Compile the merged workflow update payload without "
                            "sending it."
                        ),
                        default=False,
                    ),
                ],
            ),
            command(
                "online",
                action="workflow.online",
                summary="Bring one workflow definition online.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[project_option()],
            ),
            command(
                "offline",
                action="workflow.offline",
                summary="Bring one workflow definition offline.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[project_option()],
            ),
            command(
                "run",
                action="workflow.run",
                summary=(
                    "Trigger one workflow definition and return created "
                    "workflow instance ids."
                ),
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Override the worker group used to start the "
                            "workflow instance. Omit to allow enabled project "
                            "preference before the DS fallback `default` worker "
                            "group."
                        ),
                    ),
                    option(
                        "tenant",
                        value_type="string",
                        description=(
                            "Override the tenant code used to start the "
                            "workflow instance. Omit to allow enabled project "
                            "preference before the DS fallback `default` tenant."
                        ),
                    ),
                    option(
                        "failure-strategy",
                        value_type="string",
                        description="Failure strategy.",
                        default="continue",
                        choices=["continue", "end"],
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description="Workflow instance priority.",
                        default="medium",
                        choices=["highest", "high", "medium", "low", "lowest"],
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type.",
                        default="none",
                        choices=["none", "success", "failure", "all"],
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id. Omit to allow enabled project "
                            "preference."
                        ),
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment code. Omit to allow enabled project "
                            "preference."
                        ),
                    ),
                    option(
                        "param",
                        value_type="string",
                        description=(
                            "Workflow start parameter in KEY=VALUE form. Repeat "
                            "for multiple parameters."
                        ),
                        multiple=True,
                        examples=["bizdate=20260415", "region=cn"],
                    ),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Resolve and compile the start request without sending it."
                        ),
                        default=False,
                    ),
                    option(
                        "execution-dry-run",
                        value_type="boolean",
                        description=(
                            "Set DolphinScheduler dryRun=1; DS creates dry-run "
                            "instances and skips task plugin trigger execution."
                        ),
                        default=False,
                    ),
                ],
            ),
            command(
                "run-task",
                action="workflow.run-task",
                summary="Start one workflow definition from a selected task.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
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
                    project_option(),
                    option(
                        "scope",
                        value_type="string",
                        description="Task execution scope.",
                        default="self",
                        choices=["self", "pre", "post"],
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Override the worker group used to start the "
                            "workflow instance. Omit to allow enabled project "
                            "preference before the DS fallback `default` worker "
                            "group."
                        ),
                    ),
                    option(
                        "tenant",
                        value_type="string",
                        description=(
                            "Override the tenant code used to start the "
                            "workflow instance. Omit to allow enabled project "
                            "preference before the DS fallback `default` tenant."
                        ),
                    ),
                    option(
                        "failure-strategy",
                        value_type="string",
                        description="Failure strategy.",
                        default="continue",
                        choices=["continue", "end"],
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description="Workflow instance priority.",
                        default="medium",
                        choices=["highest", "high", "medium", "low", "lowest"],
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type.",
                        default="none",
                        choices=["none", "success", "failure", "all"],
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id. Omit to allow enabled project "
                            "preference."
                        ),
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment code. Omit to allow enabled project "
                            "preference."
                        ),
                    ),
                    option(
                        "param",
                        value_type="string",
                        description=(
                            "Workflow start parameter in KEY=VALUE form. Repeat "
                            "for multiple parameters."
                        ),
                        multiple=True,
                        examples=["bizdate=20260415", "region=cn"],
                    ),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Resolve and compile the start request without sending it."
                        ),
                        default=False,
                    ),
                    option(
                        "execution-dry-run",
                        value_type="boolean",
                        description=(
                            "Set DolphinScheduler dryRun=1; DS creates dry-run "
                            "instances and skips task plugin trigger execution."
                        ),
                        default=False,
                    ),
                ],
            ),
            command(
                "backfill",
                action="workflow.backfill",
                summary=(
                    "Backfill one workflow definition and return created "
                    "workflow instance ids."
                ),
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "start",
                        value_type="string",
                        description=("Complement start datetime for range backfill."),
                    ),
                    option(
                        "end",
                        value_type="string",
                        description="Complement end datetime for range backfill.",
                    ),
                    option(
                        "date",
                        value_type="string",
                        description=(
                            "Explicit complement schedule datetime. Repeat for "
                            "multiple dates instead of using --start/--end."
                        ),
                        multiple=True,
                        examples=["2026-04-01 00:00:00"],
                    ),
                    option(
                        "task",
                        value_type="string",
                        description="Optional task name or task code to backfill from.",
                        selector="name_or_code",
                    ),
                    option(
                        "scope",
                        value_type="string",
                        description="Task execution scope when --task is set.",
                        default="self",
                        choices=["self", "pre", "post"],
                    ),
                    option(
                        "run-mode",
                        value_type="string",
                        description="Complement run mode.",
                        default="serial",
                        choices=["serial", "parallel"],
                    ),
                    option(
                        "expected-parallelism-number",
                        value_type="integer",
                        description=(
                            "Expected parallelism number when --run-mode "
                            "parallel is used."
                        ),
                        default=2,
                    ),
                    option(
                        "complement-dependent-mode",
                        value_type="string",
                        description="Complement dependent mode.",
                        default="off",
                        choices=["off", "all"],
                    ),
                    option(
                        "all-level-dependent",
                        value_type="boolean",
                        description=(
                            "Enable all-level dependent complement when "
                            "dependent mode is all."
                        ),
                        default=False,
                    ),
                    option(
                        "execution-order",
                        value_type="string",
                        description="Complement execution order.",
                        default="desc",
                        choices=["desc", "asc"],
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Override the worker group used to start the "
                            "workflow instance. Omit to allow enabled project "
                            "preference before the DS fallback `default` worker "
                            "group."
                        ),
                    ),
                    option(
                        "tenant",
                        value_type="string",
                        description=(
                            "Override the tenant code used to start the "
                            "workflow instance. Omit to allow enabled project "
                            "preference before the DS fallback `default` tenant."
                        ),
                    ),
                    option(
                        "failure-strategy",
                        value_type="string",
                        description="Failure strategy.",
                        default="continue",
                        choices=["continue", "end"],
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description="Workflow instance priority.",
                        default="medium",
                        choices=["highest", "high", "medium", "low", "lowest"],
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type.",
                        default="none",
                        choices=["none", "success", "failure", "all"],
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id. Omit to allow enabled project "
                            "preference."
                        ),
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment code. Omit to allow enabled project "
                            "preference."
                        ),
                    ),
                    option(
                        "param",
                        value_type="string",
                        description=(
                            "Workflow start parameter in KEY=VALUE form. Repeat "
                            "for multiple parameters."
                        ),
                        multiple=True,
                        examples=["bizdate=20260415", "region=cn"],
                    ),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Resolve and compile the backfill request without "
                            "sending it."
                        ),
                        default=False,
                    ),
                    option(
                        "execution-dry-run",
                        value_type="boolean",
                        description=(
                            "Set DolphinScheduler dryRun=1; DS creates dry-run "
                            "instances and skips task plugin trigger execution."
                        ),
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="workflow.delete",
                summary="Delete one workflow definition.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=(
                            "Workflow name or numeric code. Falls back to "
                            "workflow context when omitted."
                        ),
                        required=False,
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "force",
                        value_type="boolean",
                        description=("Confirm workflow deletion without prompting."),
                        default=False,
                    ),
                ],
            ),
            group(
                "lineage",
                summary="Inspect DolphinScheduler workflow lineage.",
                commands=[
                    command(
                        "list",
                        action="workflow.lineage.list",
                        summary="Return the project-wide workflow lineage graph.",
                        options=[project_option()],
                    ),
                    command(
                        "get",
                        action="workflow.lineage.get",
                        summary="Return the lineage graph anchored on one workflow.",
                        arguments=[
                            argument(
                                "workflow",
                                value_type="string",
                                description=(
                                    "Workflow name or numeric code. Falls back to "
                                    "workflow context when omitted."
                                ),
                                required=False,
                                selector="name_or_code",
                            )
                        ],
                        options=[project_option()],
                    ),
                    command(
                        "dependent-tasks",
                        action="workflow.lineage.dependent-tasks",
                        summary=(
                            "Return workflows or tasks that depend on one "
                            "workflow or task."
                        ),
                        arguments=[
                            argument(
                                "workflow",
                                value_type="string",
                                description=(
                                    "Workflow name or numeric code. Falls back to "
                                    "workflow context when omitted."
                                ),
                                required=False,
                                selector="name_or_code",
                            )
                        ],
                        options=[
                            project_option(),
                            option(
                                "task",
                                value_type="string",
                                description=(
                                    "Task name or numeric code inside the selected "
                                    "workflow."
                                ),
                                selector="name_or_code",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


def task_group() -> dict[str, object]:
    """Build the task command group schema."""
    return group(
        "task",
        summary="Manage DolphinScheduler task definitions inside workflows.",
        commands=[
            command(
                "list",
                action="task.list",
                summary="List tasks inside one workflow.",
                options=[
                    project_option(),
                    workflow_option(
                        description=(
                            "Workflow name or code. Falls back to workflow context."
                        )
                    ),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter tasks by name substring after fetching the "
                            "workflow task list."
                        ),
                    ),
                ],
            ),
            command(
                "get",
                action="task.get",
                summary="Get one task definition by name or code.",
                arguments=[
                    argument(
                        "task",
                        value_type="string",
                        description="Task name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    workflow_option(
                        description=(
                            "Workflow name or code. Falls back to workflow context."
                        )
                    ),
                ],
            ),
            command(
                "update",
                action="task.update",
                summary="Update one task definition by name or code.",
                arguments=[
                    argument(
                        "task",
                        value_type="string",
                        description="Task name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    workflow_option(
                        description=(
                            "Workflow name or code. Falls back to workflow context."
                        )
                    ),
                    option(
                        "set",
                        value_type="string",
                        description=(
                            "Inline task update in KEY=VALUE form. Repeat for "
                            "multiple fields. Inspect `dsctl schema` for "
                            "supported keys and examples."
                        ),
                        multiple=True,
                        required=True,
                        examples=[
                            "command=python v2.py",
                            "retry.times=5",
                            "task_group_id=12",
                            "timeout_notify_strategy=FAILED",
                        ],
                        supported_keys=[
                            "command",
                            "cpu_quota",
                            "delay",
                            "depends_on",
                            "description",
                            "environment_code",
                            "flag",
                            "memory_max",
                            "priority",
                            "retry.interval",
                            "retry.times",
                            "task_group_id",
                            "task_group_priority",
                            "timeout",
                            "timeout_notify_strategy",
                            "worker_group",
                        ],
                    ),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Compile the native task update request without sending it."
                        ),
                        default=False,
                    ),
                ],
            ),
        ],
    )
