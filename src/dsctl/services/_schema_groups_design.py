from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dsctl.command_contract import COMMAND_CATALOG, InputContract, ValueResolution
from dsctl.services._schema_primitives import (
    argument,
    command,
    command_from_contract,
    confirm_risk_option,
    group,
    option,
    option_from_contract,
    project_option,
    workflow_option,
)
from dsctl.services.pagination import DEFAULT_PAGE_SIZE
from dsctl.services.template import (
    supported_datasource_types,
    supported_parameter_syntax_topics,
    supported_task_template_variants,
)

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject

_WORKFLOW_ARGUMENT_DESCRIPTION = (
    "Workflow name or numeric code. When omitted, uses workflow context only "
    "when project also comes from context; otherwise pass WORKFLOW."
)
_WORKFLOW_EDIT_ARGUMENT_DESCRIPTION = (
    "Workflow name or numeric code. Required with --file; with --patch, uses "
    "workflow context only when project also comes from context; otherwise "
    "pass WORKFLOW."
)
_WORKFLOW_OPTION_DESCRIPTION = (
    "Workflow name or code. When omitted, uses workflow context only when "
    "project also comes from context; otherwise pass --workflow."
)
_WORKFLOW_CREATE_CONTRACT = COMMAND_CATALOG.command("workflow.create")
_WORKFLOW_RUNTIME_PRECEDENCE = ("flag", "project_preference", "default")
_WORKFLOW_RUNTIME_WORKER_GROUP = InputContract(
    name="worker-group",
    kind="option",
    value_type="string",
    description=(
        "Override the worker group used to start the workflow instance. Omit to "
        "allow enabled project preference before the DS fallback `default` worker "
        "group."
    ),
    discovery_command="dsctl worker-group list",
    resolution=ValueResolution(
        precedence=_WORKFLOW_RUNTIME_PRECEDENCE,
        fallback="default",
    ),
)
_WORKFLOW_RUNTIME_TENANT = InputContract(
    name="tenant",
    kind="option",
    value_type="string",
    description=(
        "Override the tenant code used to start the workflow instance. Omit to "
        "allow enabled project preference before the DS fallback `default` tenant."
    ),
    discovery_command="dsctl tenant list",
    resolution=ValueResolution(
        precedence=_WORKFLOW_RUNTIME_PRECEDENCE,
        fallback="default",
    ),
)
_WORKFLOW_RUNTIME_PRIORITY = InputContract(
    name="priority",
    kind="option",
    value_type="string",
    description=(
        "Workflow instance priority. Omit to allow enabled project preference "
        "before medium."
    ),
    choices=("highest", "high", "medium", "low", "lowest"),
    resolution=ValueResolution(
        precedence=_WORKFLOW_RUNTIME_PRECEDENCE,
        fallback="medium",
    ),
)
_WORKFLOW_RUNTIME_WARNING_TYPE = InputContract(
    name="warning-type",
    kind="option",
    value_type="string",
    description=("Warning type. Omit to allow enabled project preference before none."),
    choices=("none", "success", "failure", "all"),
    resolution=ValueResolution(
        precedence=_WORKFLOW_RUNTIME_PRECEDENCE,
        fallback="none",
    ),
)
_WORKFLOW_RUNTIME_WARNING_GROUP = InputContract(
    name="warning-group-id",
    kind="option",
    value_type="integer",
    description="Warning group id. Omit to allow enabled project preference.",
    discovery_command="dsctl alert-group list",
    resolution=ValueResolution(
        precedence=_WORKFLOW_RUNTIME_PRECEDENCE,
        fallback=None,
    ),
)
_WORKFLOW_RUNTIME_ENVIRONMENT = InputContract(
    name="environment-code",
    kind="option",
    value_type="integer",
    description="Environment code. Omit to allow enabled project preference.",
    discovery_command="dsctl environment list",
    resolution=ValueResolution(
        precedence=_WORKFLOW_RUNTIME_PRECEDENCE,
        fallback=None,
    ),
)
_SCHEMA_V2_RUNTIME_DEFAULT_FIELDS = frozenset({"priority", "warning-type"})


def _workflow_runtime_option(contract: InputContract) -> JsonObject:
    """Project runtime resolution plus the schema-v2 legacy default fields."""
    data = option_from_contract(contract)
    if contract.name not in _SCHEMA_V2_RUNTIME_DEFAULT_FIELDS:
        return data
    resolution = contract.resolution
    if resolution is None:
        message = f"{contract.name!r} needs resolution for its v2 default projection"
        raise RuntimeError(message)
    data["default"] = resolution.fallback
    return data


def _workflow_runtime_options() -> list[JsonObject]:
    """Build the shared start-time option block in stable CLI order."""
    return [
        _workflow_runtime_option(_WORKFLOW_RUNTIME_WORKER_GROUP),
        _workflow_runtime_option(_WORKFLOW_RUNTIME_TENANT),
        cast(
            "JsonObject",
            option(
                "failure-strategy",
                value_type="string",
                description="Failure strategy.",
                default="continue",
                choices=["continue", "end"],
            ),
        ),
        _workflow_runtime_option(_WORKFLOW_RUNTIME_PRIORITY),
        _workflow_runtime_option(_WORKFLOW_RUNTIME_WARNING_TYPE),
        _workflow_runtime_option(_WORKFLOW_RUNTIME_WARNING_GROUP),
        _workflow_runtime_option(_WORKFLOW_RUNTIME_ENVIRONMENT),
    ]


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
                        description=(
                            "Schedule id. Use `dsctl schedule list` to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl schedule list",
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
                        description=(
                            "Existing schedule id to preview. Use `dsctl schedule "
                            "list` to discover values."
                        ),
                        required=False,
                        selector="id",
                        discovery_command="dsctl schedule list",
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
                        description=(
                            "Existing schedule id to explain as an update. Use "
                            "`dsctl schedule list` to discover values."
                        ),
                        required=False,
                        selector="id",
                        discovery_command="dsctl schedule list",
                    )
                ],
                options=[
                    workflow_option(
                        description=(
                            "Workflow name or code for create explain only. When "
                            "schedule_id is omitted, uses workflow context only "
                            "when project also comes from context; do not pass "
                            "--workflow with schedule_id."
                        )
                    ),
                    option(
                        "project",
                        value_type="string",
                        description=(
                            "Project name or code for create explain only. When "
                            "schedule_id is omitted, falls back to stored project "
                            "context; do not pass --project with schedule_id."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl project list",
                    ),
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
                        choices=["CONTINUE", "END"],
                        discovery_command="dsctl enum list failure-strategy",
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
                        choices=["NONE", "SUCCESS", "FAILURE", "ALL"],
                        discovery_command="dsctl enum list warning-type",
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id for create explain or updated value "
                            "for update explain. Create explain can also inherit an "
                            "enabled project preference when omitted."
                        ),
                        discovery_command="dsctl alert-group list",
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description=(
                            "Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, "
                            "or LOWEST."
                        ),
                        choices=["HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"],
                        discovery_command="dsctl enum list priority",
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group for create explain or updated value for "
                            "update explain. Create explain can also inherit an "
                            "enabled project preference when omitted."
                        ),
                        discovery_command="dsctl worker-group list",
                    ),
                    option(
                        "tenant-code",
                        value_type="string",
                        description=(
                            "Tenant code for create explain. Create explain can "
                            "also inherit an enabled project preference when omitted."
                        ),
                        discovery_command="dsctl tenant list",
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment selection for create or update explain. "
                            "For create, omission allows enabled project preference "
                            "and zero explicitly selects no environment. For update, "
                            "omission preserves the current value and zero clears it."
                        ),
                        discovery_command="dsctl environment list",
                    ),
                ],
            ),
            command(
                "create",
                action="schedule.create",
                summary="Create one schedule.",
                options=[
                    workflow_option(description=_WORKFLOW_OPTION_DESCRIPTION),
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
                        choices=["CONTINUE", "END"],
                        discovery_command="dsctl enum list failure-strategy",
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
                        choices=["NONE", "SUCCESS", "FAILURE", "ALL"],
                        discovery_command="dsctl enum list warning-type",
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Warning group id. Omit to keep the CLI fallback "
                            "chain, including enabled project preference."
                        ),
                        discovery_command="dsctl alert-group list",
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description=(
                            "Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, "
                            "or LOWEST."
                        ),
                        choices=["HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"],
                        discovery_command="dsctl enum list priority",
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group. Omit to allow enabled project preference."
                        ),
                        discovery_command="dsctl worker-group list",
                    ),
                    option(
                        "tenant-code",
                        value_type="string",
                        description=(
                            "Tenant code. Omit to allow enabled project preference."
                        ),
                        discovery_command="dsctl tenant list",
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Environment selection. Omit to allow enabled project "
                            "preference and otherwise use no environment; pass 0 to "
                            "explicitly use no environment and bypass that preference."
                        ),
                        discovery_command="dsctl environment list",
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
                        description=(
                            "Schedule id. Use `dsctl schedule list` to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl schedule list",
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
                        choices=["CONTINUE", "END"],
                        discovery_command="dsctl enum list failure-strategy",
                    ),
                    option(
                        "warning-type",
                        value_type="string",
                        description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
                        choices=["NONE", "SUCCESS", "FAILURE", "ALL"],
                        discovery_command="dsctl enum list warning-type",
                    ),
                    option(
                        "warning-group-id",
                        value_type="integer",
                        description=(
                            "Updated warning group id. Omit to keep the current value."
                        ),
                        discovery_command="dsctl alert-group list",
                    ),
                    option(
                        "priority",
                        value_type="string",
                        description=(
                            "Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, "
                            "or LOWEST."
                        ),
                        choices=["HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"],
                        discovery_command="dsctl enum list priority",
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Updated worker group. Omit to keep the current value."
                        ),
                        discovery_command="dsctl worker-group list",
                    ),
                    option(
                        "environment-code",
                        value_type="integer",
                        description=(
                            "Updated environment selection. Omit to keep the current "
                            "value; pass 0 to clear the environment."
                        ),
                        discovery_command="dsctl environment list",
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
                        description=(
                            "Schedule id. Use `dsctl schedule list` to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl schedule list",
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
                        description=(
                            "Schedule id. Use `dsctl schedule list` to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl schedule list",
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
                        description=(
                            "Schedule id. Use `dsctl schedule list` to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl schedule list",
                    )
                ],
            ),
        ],
    )


def template_group(task_types: list[str]) -> dict[str, object]:
    """Build the template command group schema."""
    return group(
        "template",
        summary="Emit stable templates for workflow authoring and DS-native payloads.",
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
                    ),
                    option(
                        "raw",
                        value_type="boolean",
                        description=(
                            "Print only the workflow YAML template, without the "
                            "JSON envelope."
                        ),
                        default=False,
                    ),
                ],
                payload={
                    "format": "yaml",
                    "raw_option": "--raw",
                    "template_command": "dsctl template workflow --raw",
                    "target_command": "dsctl workflow create --file FILE",
                },
            ),
            command(
                "workflow-patch",
                action="template.workflow-patch",
                summary="Emit the stable workflow edit patch YAML template.",
                options=[
                    option(
                        "raw",
                        value_type="boolean",
                        description=(
                            "Print only the workflow patch YAML template, "
                            "without the JSON envelope."
                        ),
                        default=False,
                    ),
                ],
                payload={
                    "format": "yaml",
                    "raw_option": "--raw",
                    "template_command": "dsctl template workflow-patch --raw",
                    "target_command": "dsctl workflow edit WORKFLOW --patch FILE",
                },
            ),
            command(
                "workflow-instance-patch",
                action="template.workflow-instance-patch",
                summary="Emit the stable workflow-instance edit patch YAML template.",
                options=[
                    option(
                        "raw",
                        value_type="boolean",
                        description=(
                            "Print only the workflow-instance patch YAML template, "
                            "without the JSON envelope."
                        ),
                        default=False,
                    ),
                ],
                payload={
                    "format": "yaml",
                    "raw_option": "--raw",
                    "template_command": (
                        "dsctl template workflow-instance-patch --raw"
                    ),
                    "target_command": (
                        "dsctl workflow-instance edit WORKFLOW_INSTANCE --patch FILE"
                    ),
                },
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
                        discovery_command="dsctl template params",
                    )
                ],
            ),
            command(
                "environment",
                action="template.environment",
                summary="Emit a DS environment shell/export config template.",
            ),
            command(
                "cluster",
                action="template.cluster",
                summary="Emit a DS cluster config JSON template.",
            ),
            command(
                "datasource",
                action="template.datasource",
                summary=(
                    "Emit datasource JSON payload-template type discovery or one "
                    "template."
                ),
                options=[
                    option(
                        "type",
                        value_type="string",
                        description=(
                            "Datasource type. Omit for discovery; full values via "
                            "`dsctl enum list db-type`."
                        ),
                        choices=supported_datasource_types(),
                        discovery_command="dsctl template datasource",
                    )
                ],
            ),
            command(
                "task",
                action="template.task",
                summary=(
                    "Emit the compact task template catalog, or one task YAML "
                    "fragment when TASK_TYPE is provided."
                ),
                arguments=[
                    argument(
                        "task_type",
                        value_type="string",
                        description=(
                            "Task type to template. Omit for the compact template "
                            "catalog."
                        ),
                        required=False,
                        choices=task_types,
                        discovery_command="dsctl template task",
                    )
                ],
                options=[
                    option(
                        "variant",
                        value_type="string",
                        description=(
                            "Task template scenario. Valid choices depend on "
                            "the selected task type. Known variants include "
                            "minimal, params, resource, post-json, "
                            "pre-post-statements, branching, condition-routing, "
                            "workflow-dependency, child-workflow, and datasource; "
                            "inspect per-type values with `dsctl task-type get TYPE`."
                        ),
                        choices=supported_task_template_variants(),
                        discovery_command="dsctl task-type get TYPE",
                    ),
                    option(
                        "raw",
                        value_type="boolean",
                        description=(
                            "Print only the YAML task fragment, without the JSON "
                            "envelope."
                        ),
                        default=False,
                    ),
                ],
                payload={
                    "format": "yaml",
                    "raw_option": "--raw",
                    "template_command_pattern": "dsctl template task TYPE --raw",
                    "schema_command_pattern": "dsctl task-type schema TYPE",
                    "paste_into": "workflow YAML tasks[]",
                },
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
                summary=(
                    "List workflows with optional filtering and pagination controls."
                ),
                options=[
                    project_option(),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter workflows by name using the upstream search value."
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
                action="workflow.get",
                summary="Get one workflow by name or code.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
                    )
                ],
                options=[project_option()],
            ),
            command(
                "export",
                action="workflow.export",
                summary="Export one workflow as an editable YAML document.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
                    )
                ],
                options=[project_option()],
                payload={
                    "format": "yaml",
                    "output": "raw_document",
                    "target_command": "dsctl workflow edit WORKFLOW --file FILE",
                },
            ),
            command(
                "describe",
                action="workflow.describe",
                summary="Describe one workflow with tasks and relations.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
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
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
                    )
                ],
                options=[project_option()],
            ),
            cast(
                "dict[str, object]",
                command_from_contract(_WORKFLOW_CREATE_CONTRACT),
            ),
            command(
                "edit",
                action="workflow.edit",
                summary=(
                    "Edit one workflow definition from a YAML patch or full "
                    "workflow YAML file."
                ),
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=_WORKFLOW_EDIT_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
                    )
                ],
                options=[
                    option(
                        "patch",
                        value_type="path",
                        description=(
                            "Path to one workflow patch YAML file. Use exactly "
                            "one of --patch or --file. Inspect the current "
                            "definition with `dsctl workflow export WORKFLOW`, "
                            "then write only the intended "
                            "delta. Start from `dsctl template workflow-patch "
                            "--raw`; use --dry-run to inspect the compiled diff. "
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
                            "Path to one full workflow YAML file describing the "
                            "desired definition state. Use exactly one of "
                            "--patch or --file. Start from `dsctl workflow export "
                            "WORKFLOW` or `dsctl template workflow "
                            "--raw`; use --dry-run to inspect the compiled diff. "
                            "Full-file edits match task identity by exact task "
                            "name and do not infer renames."
                        ),
                    ),
                    project_option(),
                    option(
                        "dry-run",
                        value_type="boolean",
                        description=(
                            "Compile the merged workflow edit payload without "
                            "sending it."
                        ),
                        default=False,
                    ),
                    confirm_risk_option(),
                ],
                payload={
                    "format": "yaml",
                    "source_options": ["--patch PATH", "--file PATH"],
                    "patch_template_command": "dsctl template workflow-patch --raw",
                    "file_source_command": "dsctl workflow export WORKFLOW",
                    "file_template_command": "dsctl template workflow --raw",
                    "target_commands": [
                        "dsctl workflow edit WORKFLOW --patch FILE",
                        "dsctl workflow edit WORKFLOW --file FILE",
                    ],
                },
            ),
            command(
                "online",
                action="workflow.online",
                summary="Bring one workflow definition online.",
                arguments=[
                    argument(
                        "workflow",
                        value_type="string",
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
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
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
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
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
                    )
                ],
                options=[
                    project_option(),
                    *cast(
                        "list[dict[str, object]]",
                        _workflow_runtime_options(),
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
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
                    )
                ],
                options=[
                    option(
                        "task",
                        value_type="string",
                        description=(
                            "Task name or task code within the workflow "
                            "definition. Run `dsctl task list` to discover values."
                        ),
                        required=True,
                        selector="name_or_code",
                        discovery_command="dsctl task list",
                    ),
                    project_option(),
                    option(
                        "scope",
                        value_type="string",
                        description="Task execution scope.",
                        default="self",
                        choices=["self", "pre", "post"],
                    ),
                    *cast(
                        "list[dict[str, object]]",
                        _workflow_runtime_options(),
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
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
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
                        description=(
                            "Optional task name or task code to backfill from. "
                            "Run `dsctl task list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl task list",
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
                    *cast(
                        "list[dict[str, object]]",
                        _workflow_runtime_options(),
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
                        description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                        required=False,
                        selector="name_or_code",
                        discovery_command="dsctl workflow list",
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
                                description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                                required=False,
                                selector="name_or_code",
                                discovery_command="dsctl workflow list",
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
                                description=_WORKFLOW_ARGUMENT_DESCRIPTION,
                                required=False,
                                selector="name_or_code",
                                discovery_command="dsctl workflow list",
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
                                discovery_command="dsctl task list",
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
                    workflow_option(description=_WORKFLOW_OPTION_DESCRIPTION),
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
                        description=(
                            "Task name or numeric code. Use `dsctl task list` to "
                            "discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl task list",
                    )
                ],
                options=[
                    project_option(),
                    workflow_option(description=_WORKFLOW_OPTION_DESCRIPTION),
                ],
            ),
            command(
                "update",
                action="task.update",
                summary=(
                    "Update one task; use workflow edit for DAG changes, "
                    "workflow-instance edit for repairs."
                ),
                arguments=[
                    argument(
                        "task",
                        value_type="string",
                        description=(
                            "Task name or numeric code. Use `dsctl task list` to "
                            "discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl task list",
                    )
                ],
                options=[
                    project_option(),
                    workflow_option(description=_WORKFLOW_OPTION_DESCRIPTION),
                    option(
                        "set",
                        value_type="string",
                        description=(
                            "Inline KEY=VALUE update for this single task. "
                            "Repeat as needed. Common keys: command, "
                            "retry.times, timeout, depends_on. Run `dsctl "
                            "schema --command task.update` for all supported "
                            "keys."
                        ),
                        multiple=True,
                        required=True,
                        discovery_command="dsctl schema --command task.update",
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
                payload={
                    "scope": "workflow_definition",
                    "resource_scope": "single_existing_task",
                    "input_mode": "inline_set",
                    "inspect_command": "dsctl task get TASK --workflow WORKFLOW",
                    "supported_keys_command": "dsctl schema --command task.update",
                    "target_command": "dsctl task update TASK --set KEY=VALUE",
                    "use_workflow_edit_for": [
                        "create_task",
                        "delete_task",
                        "rename_task",
                        "task_type_change",
                        "multi_task_dag_edit",
                    ],
                    "use_workflow_instance_edit_for": [
                        "finished_instance_repair",
                    ],
                },
            ),
        ],
    )
