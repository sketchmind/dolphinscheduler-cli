"""Stable command declarations. Execution stays in explicitly typed callbacks.

Parser choices describe syntax; choices can additionally document service validation.
Parser omission defaults and effective fixed defaults are deliberately distinct.
"""

from dsctl._command_contract_model import (
    LOCAL_CONFIGURATION_WRITE,
    NO_EFFECTS,
    REMOTE_READ,
    REMOTE_READ_LOCAL_FILE_WRITE,
    REMOTE_WRITE,
    REMOTE_WRITE_DRY_RUN_READ,
    CommandContract,
    InputContract,
    PathRules,
    ValueResolution,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE

_PROJECT_NATIVE_IDENTITY = (
    "native numeric identity (id on DS 1.3.9, code on newer versions)"
)

_CONTEXT_NAME = InputContract(
    description="Saved context name. Run `dsctl context list` to discover names.",
    discovery_command="dsctl context list",
    kind="argument",
    name="name",
    required=True,
    selector="opaque_name",
    value_type="string",
)
_CONTEXT_PROJECT = InputContract(
    description=(
        f"Project name or {_PROJECT_NATIVE_IDENTITY} to bind to this context, "
        "without remote validation."
    ),
    discovery_command="dsctl project list",
    kind="option",
    name="project",
    parse_default=None,
    selector="name_or_native_identity",
    value_type="string",
)
_CONFIG_KEY = InputContract(
    description="User configuration key; only default-context is supported.",
    kind="argument",
    name="key",
    required=True,
    choices=("default-context",),
    value_type="string",
)
_BOUNDED_PAGE_NO = InputContract(
    description="Page number to fetch when not using --all.",
    kind="option",
    minimum=1,
    name="page-no",
    parse_default=1,
    value_type="integer",
)
_BOUNDED_PAGE_SIZE = InputContract(
    description="Page size to request from the upstream API.",
    kind="option",
    minimum=1,
    name="page-size",
    parse_default=DEFAULT_PAGE_SIZE,
    value_type="integer",
)
_ALL_PAGES = InputContract(
    description="Fetch all remaining pages up to the safety limit.",
    kind="option",
    name="all",
    parameter_name="all_pages",
    parse_default=False,
    value_type="boolean",
)
_ENVIRONMENT_ARGUMENT = InputContract(
    description=(
        "Environment name or numeric code. Run `dsctl environment list`"
        " to discover values."
    ),
    discovery_command="dsctl environment list",
    kind="argument",
    name="environment",
    required=True,
    selector="name_or_code",
    value_type="string",
)
_ENVIRONMENT_WORKER_GROUP_BINDINGS = InputContract(
    description=(
        "Worker group to bind to this environment. Repeat as needed; "
        "run `dsctl worker-group list` to discover values."
    ),
    discovery_command="dsctl worker-group list",
    kind="option",
    multiple=True,
    name="worker-group",
    parameter_name="worker_groups",
    parse_default=None,
    value_type="string",
)
_CLUSTER_ARGUMENT = InputContract(
    description=(
        "Cluster name or numeric code. Run `dsctl cluster list` to discover values."
    ),
    discovery_command="dsctl cluster list",
    kind="argument",
    name="cluster",
    required=True,
    selector="name_or_code",
    value_type="string",
)
_DATASOURCE_ARGUMENT = InputContract(
    description=(
        "Datasource name or numeric id. Run `dsctl datasource list` to discover values."
    ),
    discovery_command="dsctl datasource list",
    kind="argument",
    name="datasource",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_NAMESPACE_ARGUMENT = InputContract(
    description=(
        "Namespace name or numeric id. Run `dsctl namespace list` to discover values."
    ),
    discovery_command="dsctl namespace list",
    kind="argument",
    name="namespace",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_RESOURCE_ARGUMENT = InputContract(
    description=(
        "DS resource fullName path. Run `dsctl resource list --dir DIR`"
        " to discover paths."
    ),
    discovery_command_pattern="dsctl resource list --dir DIR",
    kind="argument",
    name="resource",
    required=True,
    selector="resource_path",
    value_type="string",
)
_RESOURCE_DESTINATION_DIRECTORY = InputContract(
    description=(
        "Destination DS directory fullName path. Defaults to the "
        "upstream base directory; run `dsctl resource list` to discover"
        " paths."
    ),
    discovery_command="dsctl resource list",
    kind="option",
    name="dir",
    parameter_name="directory",
    parse_default=None,
    value_type="string",
)
_QUEUE_ARGUMENT = InputContract(
    description="Queue name or numeric id. Run `dsctl queue list` to discover values.",
    discovery_command="dsctl queue list",
    kind="argument",
    name="queue",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_WORKER_GROUP_ARGUMENT = InputContract(
    description=(
        "Worker-group name or numeric id. Run `dsctl worker-group list`"
        " to discover values."
    ),
    discovery_command="dsctl worker-group list",
    kind="argument",
    name="worker_group",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_TASK_GROUP_PAGE_NO = InputContract(
    description="Page number to fetch.",
    kind="option",
    minimum=1,
    name="page-no",
    parse_default=1,
    value_type="integer",
)
_TASK_GROUP_PAGE_SIZE = InputContract(
    description="Page size to request.",
    kind="option",
    minimum=1,
    name="page-size",
    parse_default=DEFAULT_PAGE_SIZE,
    value_type="integer",
)
_TASK_GROUP_ARGUMENT = InputContract(
    description=(
        "Task-group name or numeric id. Run `dsctl task-group list` to discover values."
    ),
    discovery_command="dsctl task-group list",
    kind="argument",
    name="task_group",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_TASK_GROUP_QUEUE_ID = InputContract(
    description=(
        "Numeric task-group queue id. Run `dsctl task-group queue list "
        "TASK_GROUP` to discover ids."
    ),
    discovery_command="dsctl task-group queue list TASK_GROUP",
    kind="argument",
    name="queue_id",
    required=True,
    selector="id",
    value_type="integer",
)
_ALERT_PLUGIN_ARGUMENT = InputContract(
    description=(
        "Alert-plugin instance name or numeric id. Run `dsctl "
        "alert-plugin list` to discover values."
    ),
    discovery_command="dsctl alert-plugin list",
    kind="argument",
    name="alert-plugin",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_ALERT_GROUP_ARGUMENT = InputContract(
    description=(
        "Alert-group name or numeric id. Run `dsctl alert-group list` "
        "to discover values."
    ),
    discovery_command="dsctl alert-group list",
    kind="argument",
    name="alert-group",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_ALERT_PLUGIN_INSTANCE_BINDINGS = InputContract(
    description=(
        "Alert plugin instance id to bind to this group. Repeat as "
        "needed; run `dsctl alert-plugin list` to discover ids."
    ),
    discovery_command="dsctl alert-plugin list",
    kind="option",
    minimum=1,
    multiple=True,
    name="instance-id",
    parameter_name="instance_ids",
    parse_default=None,
    value_type="integer",
)
_TENANT_ARGUMENT = InputContract(
    description=(
        "Tenant code or numeric id. Run `dsctl tenant list` to discover values."
    ),
    discovery_command="dsctl tenant list",
    kind="argument",
    name="tenant",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_USER_ARGUMENT = InputContract(
    description="User name or numeric id. Run `dsctl user list` to discover values.",
    discovery_command="dsctl user list",
    kind="argument",
    name="user",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_PROJECT_PERMISSION_ARGUMENT = InputContract(
    description=(
        f"Project name or {_PROJECT_NATIVE_IDENTITY}. Run `dsctl project list` "
        "to discover values."
    ),
    discovery_command="dsctl project list",
    kind="argument",
    name="project",
    required=True,
    selector="name_or_native_identity",
    value_type="string",
)
_ACCESS_TOKEN_ARGUMENT = InputContract(
    description="Access-token id. Run `dsctl access-token list` to discover values.",
    discovery_command="dsctl access-token list",
    kind="argument",
    name="access-token",
    required=True,
    selector="id",
    value_type="integer",
)
_ACCESS_TOKEN_USER = InputContract(
    description="User name or numeric id. Run `dsctl user list` to discover values.",
    discovery_command="dsctl user list",
    kind="option",
    name="user",
    required=True,
    selector="name_or_id",
    value_type="string",
)
_ACCESS_TOKEN_EXPIRATION = InputContract(
    description="Token expiration time, for example '2027-01-01 00:00:00'.",
    kind="option",
    name="expire-time",
    required=True,
    value_type="string",
)
_PROJECT_ARGUMENT = InputContract(
    description=(
        f"Project name or {_PROJECT_NATIVE_IDENTITY}. Run `dsctl project list` "
        "to discover values."
    ),
    discovery_command="dsctl project list",
    kind="argument",
    name="project",
    required=True,
    selector="name_or_native_identity",
    value_type="string",
)
_PROJECT_CONTEXT_OPTION = InputContract(
    description=(
        f"Project name or {_PROJECT_NATIVE_IDENTITY}. Run `dsctl project list` "
        "to discover values; falls back to the selected context project."
    ),
    discovery_command="dsctl project list",
    kind="option",
    name="project",
    parse_default=None,
    selector="name_or_native_identity",
    value_type="string",
)
_PROJECT_PARAMETER_ARGUMENT = InputContract(
    description=(
        "Project parameter name or numeric code. Run `dsctl "
        "project-parameter list` in the selected project to discover "
        "values."
    ),
    discovery_command="dsctl project-parameter list",
    kind="argument",
    name="project-parameter",
    required=True,
    selector="name_or_code",
    value_type="string",
)
_SCHEDULE_ID_ARGUMENT = InputContract(
    description="Schedule id. Use `dsctl schedule list` to discover values.",
    discovery_command="dsctl schedule list",
    kind="argument",
    name="schedule_id",
    required=True,
    selector="id",
    value_type="integer",
)
_SCHEDULE_ID_PROJECT_OPTION = InputContract(
    description=(
        f"Project name or {_PROJECT_NATIVE_IDENTITY} to constrain schedule id "
        "lookup. Falls back to the selected context project. Without either scope, "
        "lookup searches the bounded visible project inventory; run `dsctl project "
        "list` to discover values."
    ),
    discovery_command="dsctl project list",
    kind="option",
    name="project",
    parse_default=None,
    selector="name_or_native_identity",
    value_type="string",
)
_SCHEDULE_START_TIME = InputContract(
    description="Schedule start time in DS datetime string format.",
    kind="option",
    name="start",
    parse_default=None,
    value_type="string",
)
_SCHEDULE_END_TIME = InputContract(
    description="Schedule end time in DS datetime string format.",
    kind="option",
    name="end",
    parse_default=None,
    value_type="string",
)
_SCHEDULE_PREVIEW_TIMEZONE = InputContract(
    description="Timezone id, for example Asia/Shanghai.",
    kind="option",
    name="timezone",
    parse_default=None,
    value_type="string",
)
_SCHEDULE_FAILURE_STRATEGY = InputContract(
    choices=("CONTINUE", "END"),
    description="Failure strategy: CONTINUE or END.",
    discovery_command="dsctl enum list failure-strategy",
    kind="option",
    name="failure-strategy",
    parse_default=None,
    value_type="string",
)
_SCHEDULE_MISSED_FIRE_POLICY = InputContract(
    description=(
        "Policy for missed scheduled fires, when supported by the selected DS "
        "version. Omit to use the native create default or preserve the current "
        "value on update."
    ),
    discovery_command="dsctl enum list schedule-missed-fire-policy",
    kind="option",
    name="missed-fire-policy",
    parse_default=None,
    value_type="string",
)
_SCHEDULE_WARNING_TYPE = InputContract(
    choices=("NONE", "SUCCESS", "FAILURE", "ALL"),
    description="Warning type: NONE, SUCCESS, FAILURE, or ALL.",
    discovery_command="dsctl enum list warning-type",
    kind="option",
    name="warning-type",
    parse_default=None,
    value_type="string",
)
_SCHEDULE_PRIORITY = InputContract(
    choices=("HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"),
    description="Workflow instance priority: HIGHEST, HIGH, MEDIUM, LOW, or LOWEST.",
    discovery_command="dsctl enum list priority",
    kind="option",
    name="priority",
    parse_default=None,
    value_type="string",
)
_WORKFLOW_OPTION = InputContract(
    description=(
        "Workflow name or code. Run `dsctl workflow list` in the "
        "selected project to discover values. Pass --workflow explicitly."
    ),
    discovery_command="dsctl workflow list",
    kind="option",
    name="workflow",
    required=True,
    selector="name_or_code",
    value_type="string",
)
_SCHEDULE_CONFIRM_RISK = InputContract(
    description="Confirm one high-risk schedule mutation token returned earlier.",
    kind="option",
    name="confirm-risk",
    parse_default=None,
    value_type="string",
)
_WORKFLOW_ARGUMENT = InputContract(
    description=(
        "Workflow name or numeric code. Run `dsctl workflow list` in "
        "the selected project to discover values. Pass WORKFLOW explicitly."
    ),
    discovery_command="dsctl workflow list",
    kind="argument",
    name="workflow",
    required=True,
    selector="name_or_code",
    value_type="string",
)
_START_WORKER_GROUP = InputContract(
    description=(
        "Override the worker group used to start the workflow instance."
        " Run `dsctl worker-group list` to discover values; omit to "
        "allow enabled project preference before the DS fallback "
        "`default` worker group."
    ),
    discovery_command="dsctl worker-group list",
    kind="option",
    name="worker-group",
    parse_default=None,
    resolution=ValueResolution(
        precedence=("flag", "project_preference", "default"), fallback="default"
    ),
    value_type="string",
)
_START_TENANT = InputContract(
    description=(
        "Override the tenant code used to start the workflow instance. "
        "Run `dsctl tenant list` to discover values; omit to allow "
        "enabled project preference before the DS fallback `default` "
        "tenant."
    ),
    discovery_command="dsctl tenant list",
    kind="option",
    name="tenant",
    parse_default=None,
    resolution=ValueResolution(
        precedence=("flag", "project_preference", "default"), fallback="default"
    ),
    value_type="string",
)
_START_FAILURE_STRATEGY = InputContract(
    choices=("continue", "end"),
    description="Failure strategy: continue or end. Defaults to DS UI continue.",
    fixed_default="continue",
    kind="option",
    name="failure-strategy",
    parse_default=None,
    value_type="string",
)
_START_PRIORITY = InputContract(
    choices=("highest", "high", "medium", "low", "lowest"),
    description=(
        "Workflow instance priority: highest, high, medium, low, or "
        "lowest. Omit to allow enabled project preference before "
        "medium."
    ),
    kind="option",
    name="priority",
    parse_default=None,
    resolution=ValueResolution(
        precedence=("flag", "project_preference", "default"), fallback="medium"
    ),
    value_type="string",
    legacy_default=True,
)
_START_WARNING_TYPE = InputContract(
    choices=("none", "success", "failure", "all"),
    description=(
        "Warning type: none, success, failure, or all. Omit to allow "
        "enabled project preference before none."
    ),
    kind="option",
    name="warning-type",
    parse_default=None,
    resolution=ValueResolution(
        precedence=("flag", "project_preference", "default"), fallback="none"
    ),
    value_type="string",
    legacy_default=True,
)
_START_WARNING_GROUP_ID = InputContract(
    description=(
        "Warning group id. Run `dsctl alert-group list` to discover "
        "ids; omit to allow enabled project preference."
    ),
    discovery_command="dsctl alert-group list",
    kind="option",
    name="warning-group-id",
    parse_default=None,
    resolution=ValueResolution(
        precedence=("flag", "project_preference", "default"), fallback=None
    ),
    value_type="integer",
)
_START_ENVIRONMENT_CODE = InputContract(
    description=(
        "Environment code. Run `dsctl environment list` to discover "
        "values; omit to allow enabled project preference, or pass 0 to bypass it. "
        "Positive codes require DS 3.2.2+; on older DS, set task environment_code."
    ),
    discovery_command="dsctl environment list",
    kind="option",
    name="environment-code",
    parse_default=None,
    resolution=ValueResolution(
        precedence=("flag", "project_preference", "default"), fallback=None
    ),
    value_type="integer",
)
_START_PARAMETERS = InputContract(
    description=(
        "Workflow start parameter in KEY=VALUE form. Repeat for multiple parameters."
    ),
    examples=("bizdate=20260415", "region=cn"),
    kind="option",
    multiple=True,
    name="param",
    parameter_name="params",
    parse_default=None,
    value_type="string",
)
_START_COMPILE_DRY_RUN = InputContract(
    description="Resolve and compile the start request without sending it.",
    kind="option",
    name="dry-run",
    parse_default=False,
    value_type="boolean",
)
_START_EXECUTION_DRY_RUN = InputContract(
    description=(
        "Set DolphinScheduler dryRun=1; DS creates dry-run instances "
        "and skips task plugin trigger execution."
    ),
    kind="option",
    name="execution-dry-run",
    parse_default=False,
    value_type="boolean",
)
_TASK_EXECUTION_SCOPE = InputContract(
    choices=("self", "pre", "post"),
    description="Task execution scope: self, pre, or post.",
    kind="option",
    name="scope",
    parse_default="self",
    value_type="string",
)
_WORKFLOW_TASK_OPTION = InputContract(
    description=(
        "Task name or numeric code inside the selected workflow. Run "
        "`dsctl task list --project PROJECT --workflow WORKFLOW` to discover values."
    ),
    discovery_command_pattern="dsctl task list --project PROJECT --workflow WORKFLOW",
    kind="option",
    name="task",
    parse_default=None,
    selector="name_or_code",
    value_type="string",
)
_RUNTIME_PAGE_NO = InputContract(
    description="Remote page number.",
    kind="option",
    name="page-no",
    parse_default=1,
    value_type="integer",
)
_RUNTIME_PAGE_SIZE = InputContract(
    description="Remote page size.",
    kind="option",
    name="page-size",
    parse_default=DEFAULT_PAGE_SIZE,
    value_type="integer",
)
_RUNTIME_PROJECT_CONTEXT_OPTION = InputContract(
    description=(
        f"Project name or {_PROJECT_NATIVE_IDENTITY}. Required via --project or "
        "the selected context project; run `dsctl project list` to discover values."
    ),
    discovery_command="dsctl project list",
    kind="option",
    name="project",
    parse_default=None,
    selector="name_or_native_identity",
    value_type="string",
)
_RUNTIME_EXECUTOR_FILTER = InputContract(
    description="Filter by executor user name.",
    kind="option",
    name="executor",
    parse_default=None,
    value_type="string",
)
_WORKFLOW_INSTANCE_ARGUMENT = InputContract(
    description=(
        "Workflow instance id. Discover ids with `dsctl "
        "workflow-instance list` scoped to the same project."
    ),
    discovery_command_pattern="dsctl workflow-instance list --project PROJECT",
    kind="argument",
    name="workflow_instance",
    required=True,
    selector="id",
    value_type="integer",
)
_WATCH_INTERVAL_SECONDS = InputContract(
    description="Polling interval in seconds.",
    kind="option",
    name="interval-seconds",
    minimum=1,
    parser_minimum=None,
    parse_default=5,
    value_type="integer",
)
_WATCH_TIMEOUT_SECONDS = InputContract(
    description="Maximum seconds to wait. Use 0 to wait indefinitely.",
    kind="option",
    name="timeout-seconds",
    minimum=0,
    parser_minimum=None,
    parse_default=600,
    value_type="integer",
)
_WATCH_EXIT_STATUS = InputContract(
    description="Exit nonzero if the observed execution finishes without success.",
    kind="option",
    name="exit-status",
    parse_default=False,
    value_type="boolean",
)
_TASK_ARGUMENT = InputContract(
    description=(
        "Task name or numeric code. Use "
        "`dsctl task list --project PROJECT --workflow WORKFLOW` to discover values."
    ),
    discovery_command_pattern="dsctl task list --project PROJECT --workflow WORKFLOW",
    kind="argument",
    name="task",
    required=True,
    selector="name_or_code",
    value_type="string",
)
_TASK_INSTANCE_ARGUMENT = InputContract(
    description=(
        "Task instance id. Discover ids with `dsctl task-instance list`"
        " scoped to the same project."
    ),
    discovery_command_pattern="dsctl task-instance list --project PROJECT",
    kind="argument",
    name="task_instance",
    required=True,
    selector="id",
    value_type="integer",
)
_TASK_WORKFLOW_INSTANCE_OPTION = InputContract(
    description=(
        "Workflow instance id inside the selected project. Discover ids"
        " with `dsctl workflow-instance list` scoped to that project."
    ),
    discovery_command_pattern="dsctl workflow-instance list --project PROJECT",
    kind="option",
    name="workflow-instance",
    required=True,
    value_type="integer",
)
_OPTIONAL_TASK_WORKFLOW_INSTANCE_OPTION = InputContract(
    description=(
        "Optional workflow instance id inside the selected project. Omit it to "
        "locate the task instance directly in that project, including standalone "
        "STREAM tasks."
    ),
    discovery_command_pattern="dsctl workflow-instance list --project PROJECT",
    kind="option",
    name="workflow-instance",
    parse_default=None,
    value_type="integer",
)
COMMANDS = (
    CommandContract(
        action="version",
        effects=NO_EFFECTS,
        route=("version",),
        summary="Print CLI and selectable DolphinScheduler version metadata.",
    ),
    CommandContract(
        action="context",
        effects=NO_EFFECTS,
        route=("context",),
        summary="Show the local target for later commands; no remote validation.",
    ),
    CommandContract(
        action="doctor",
        effects=REMOTE_READ,
        route=("doctor",),
        summary=(
            "Check connection settings, version and API access; no task execution."
        ),
    ),
    CommandContract(
        action="schema",
        effects=NO_EFFECTS,
        route=("schema",),
        summary=(
            "Discover exact contracts; use group or index only when action is unknown."
        ),
        options=(
            InputContract(
                name="group",
                kind="option",
                value_type="string",
                description=(
                    "Return one group's action index. Discover groups with `dsctl "
                    "schema` or `dsctl schema --list-groups`."
                ),
                parse_default=None,
                discovery_command="dsctl schema --list-groups",
            ),
            InputContract(
                name="command",
                kind="option",
                value_type="string",
                description=(
                    "Return one complete action-local contract. Discover actions "
                    "with `dsctl schema` or `dsctl schema --group GROUP`."
                ),
                parse_default=None,
                discovery_command="dsctl schema",
            ),
            InputContract(
                name="list-groups",
                kind="option",
                value_type="boolean",
                description="List valid values for --group.",
                parse_default=False,
            ),
            InputContract(
                name="list-commands",
                kind="option",
                value_type="boolean",
                description="List valid action names for --command.",
                parse_default=False,
            ),
            InputContract(
                name="full",
                kind="option",
                value_type="boolean",
                description=(
                    "Return the expanded schema representation. May be combined "
                    "with --group or --command."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="capabilities",
        effects=NO_EFFECTS,
        route=("capabilities",),
        summary=(
            "Discover supported features and versions; use schema for command syntax."
        ),
        options=(
            InputContract(
                name="summary",
                kind="option",
                value_type="boolean",
                description="Return the bounded default capability summary explicitly.",
                parse_default=False,
            ),
            InputContract(
                name="section",
                kind="option",
                value_type="string",
                description=(
                    "Return one top-level capability section. Supported: selection,"
                    " output, errors, resources, planes, authoring, schedule, "
                    "monitor, enums, runtime. Discover values with `dsctl schema "
                    "--command capabilities`."
                ),
                parse_default=None,
                discovery_command="dsctl schema --command capabilities",
                choices=(
                    "selection",
                    "output",
                    "errors",
                    "resources",
                    "planes",
                    "authoring",
                    "schedule",
                    "monitor",
                    "enums",
                    "runtime",
                ),
            ),
            InputContract(
                name="action",
                kind="option",
                value_type="string",
                description=(
                    "Return one action's exact selected-version availability and "
                    "verification evidence. Discover actions with `dsctl schema`."
                ),
                parse_default=None,
                discovery_command="dsctl schema",
            ),
            InputContract(
                name="full",
                kind="option",
                value_type="boolean",
                description="Return the complete expanded capability inventory.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="context.list",
        effects=NO_EFFECTS,
        route=("context", "list"),
        summary="List saved contexts without remote validation.",
    ),
    CommandContract(
        action="context.get",
        effects=NO_EFFECTS,
        route=("context", "get"),
        summary="Show one saved context without loading its connection file.",
        arguments=(_CONTEXT_NAME,),
    ),
    CommandContract(
        action="context.create",
        effects=LOCAL_CONFIGURATION_WRITE,
        route=("context", "create"),
        summary="Save a context referencing a locally validated connection file.",
        arguments=(_CONTEXT_NAME,),
        options=(
            InputContract(
                name="file",
                kind="option",
                value_type="path",
                description=(
                    "Dotenv connection file to reference; credentials are not copied."
                ),
                required=True,
                value_name="FILE",
            ),
            _CONTEXT_PROJECT,
        ),
    ),
    CommandContract(
        action="context.update",
        effects=LOCAL_CONFIGURATION_WRITE,
        route=("context", "update"),
        summary="Update a saved connection reference or project binding locally.",
        arguments=(_CONTEXT_NAME,),
        options=(
            InputContract(
                name="file",
                kind="option",
                value_type="path",
                description=(
                    "Replace the dotenv connection file reference and rebind its URL. "
                    "Clears the project unless --project is also supplied."
                ),
                parse_default=None,
                value_name="FILE",
            ),
            _CONTEXT_PROJECT,
            InputContract(
                name="clear-project",
                kind="option",
                value_type="boolean",
                description="Clear this context's saved project binding.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="context.delete",
        effects=LOCAL_CONFIGURATION_WRITE,
        route=("context", "delete"),
        summary="Delete one saved context after unsetting it as the default.",
        arguments=(_CONTEXT_NAME,),
    ),
    CommandContract(
        action="config.get",
        effects=NO_EFFECTS,
        route=("config", "get"),
        summary="Read one saved user configuration value.",
        arguments=(_CONFIG_KEY,),
    ),
    CommandContract(
        action="config.set",
        effects=LOCAL_CONFIGURATION_WRITE,
        route=("config", "set"),
        summary="Save the default context; higher-priority selectors still apply.",
        arguments=(
            _CONFIG_KEY,
            InputContract(
                name="value",
                kind="argument",
                value_type="string",
                description="Saved context name to use as the user default.",
                required=True,
            ),
        ),
    ),
    CommandContract(
        action="config.unset",
        effects=LOCAL_CONFIGURATION_WRITE,
        route=("config", "unset"),
        summary="Clear the saved default context.",
        arguments=(_CONFIG_KEY,),
    ),
    CommandContract(
        action="enum.names",
        effects=NO_EFFECTS,
        route=("enum", "names"),
        summary="List supported generated enum discovery names.",
    ),
    CommandContract(
        action="enum.list",
        effects=NO_EFFECTS,
        route=("enum", "list"),
        summary="List the members of one supported generated enum.",
        arguments=(
            InputContract(
                name="enum",
                kind="argument",
                value_type="string",
                description=(
                    "Stable enum discovery name. Run `dsctl enum names` to list "
                    "supported values."
                ),
                required=True,
                discovery_command="dsctl enum names",
                parameter_name="enum_name",
                show_choices=False,
            ),
        ),
    ),
    CommandContract(
        action="lint.workflow",
        effects=NO_EFFECTS,
        route=("lint", "workflow"),
        summary=(
            "Lint one workflow YAML file using the local spec and compile pipeline."
        ),
        arguments=(
            InputContract(
                name="file",
                kind="argument",
                value_type="path",
                description="Workflow YAML file to lint.",
                required=True,
                path_as_string=True,
            ),
        ),
    ),
    CommandContract(
        action="lint.workflow-patch",
        effects=NO_EFFECTS,
        route=("lint", "workflow-patch"),
        summary=(
            "Lint workflow patch structure and baseline-independent semantics "
            "without contacting DolphinScheduler."
        ),
        arguments=(
            InputContract(
                name="file",
                kind="argument",
                value_type="path",
                description="Workflow patch YAML file to lint.",
                required=True,
                path_as_string=True,
            ),
        ),
    ),
    CommandContract(
        action="lint.workflow-instance-patch",
        effects=NO_EFFECTS,
        route=("lint", "workflow-instance-patch"),
        summary=(
            "Lint workflow-instance patch structure and baseline-independent "
            "semantics without contacting DolphinScheduler."
        ),
        arguments=(
            InputContract(
                name="file",
                kind="argument",
                value_type="path",
                description="Workflow-instance patch YAML file to lint.",
                required=True,
                path_as_string=True,
            ),
        ),
    ),
    CommandContract(
        action="environment.list",
        effects=REMOTE_READ,
        route=("environment", "list"),
        summary="List environments with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter environments by name using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="environment.get",
        effects=REMOTE_READ,
        route=("environment", "get"),
        summary="Get one environment by name or code.",
        arguments=(_ENVIRONMENT_ARGUMENT,),
    ),
    CommandContract(
        action="environment.create",
        effects=REMOTE_WRITE,
        route=("environment", "create"),
        summary="Create one environment; pass --config or --config-file.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Environment name.",
                required=True,
            ),
            InputContract(
                name="config",
                kind="option",
                value_type="string",
                description=(
                    "Inline DS environment shell/export config. Prefer "
                    "--config-file for multiline configs; run `dsctl template "
                    "environment` for an example."
                ),
                parse_default=None,
                discovery_command="dsctl template environment",
                examples=("export JAVA_HOME=/opt/java",),
            ),
            InputContract(
                name="config-file",
                kind="option",
                value_type="path",
                description=(
                    "Path to a DS environment shell/export config file. Run `dsctl "
                    "template environment` for an example."
                ),
                parse_default=None,
                discovery_command="dsctl template environment",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional environment description.",
                parse_default=None,
            ),
            _ENVIRONMENT_WORKER_GROUP_BINDINGS,
        ),
    ),
    CommandContract(
        action="environment.update",
        effects=REMOTE_WRITE,
        route=("environment", "update"),
        summary="Update one environment; config may come from --config-file.",
        arguments=(_ENVIRONMENT_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated environment name. Omit to keep the current name.",
                parse_default=None,
            ),
            InputContract(
                name="config",
                kind="option",
                value_type="string",
                description=(
                    "Updated inline DS environment shell/export config. Omit to "
                    "keep the current config; prefer --config-file for multiline "
                    "configs."
                ),
                parse_default=None,
                discovery_command="dsctl template environment",
                examples=("export JAVA_HOME=/opt/java",),
            ),
            InputContract(
                name="config-file",
                kind="option",
                value_type="path",
                description=(
                    "Path to an updated DS environment shell/export config file. "
                    "Omit both config options to keep the current config."
                ),
                parse_default=None,
                discovery_command="dsctl template environment",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated environment description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the stored environment description.",
                parse_default=False,
            ),
            _ENVIRONMENT_WORKER_GROUP_BINDINGS,
            InputContract(
                name="clear-worker-groups",
                kind="option",
                value_type="boolean",
                description="Clear all bound worker groups.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="environment.delete",
        effects=REMOTE_WRITE,
        route=("environment", "delete"),
        summary="Delete one environment.",
        arguments=(_ENVIRONMENT_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete environment; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="cluster.list",
        effects=REMOTE_READ,
        route=("cluster", "list"),
        summary="List clusters with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter clusters by name using the upstream search value.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="cluster.get",
        effects=REMOTE_READ,
        route=("cluster", "get"),
        summary="Get one cluster by name or code.",
        arguments=(_CLUSTER_ARGUMENT,),
    ),
    CommandContract(
        action="cluster.create",
        effects=REMOTE_WRITE,
        route=("cluster", "create"),
        summary="Create one cluster; pass --config or --config-file.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Cluster name.",
                required=True,
            ),
            InputContract(
                name="config",
                kind="option",
                value_type="string",
                description=(
                    "Inline DS cluster config JSON. Prefer --config-file for "
                    "multiline Kubernetes configs; run `dsctl template cluster` for"
                    " an example."
                ),
                parse_default=None,
                discovery_command="dsctl template cluster",
            ),
            InputContract(
                name="config-file",
                kind="option",
                value_type="path",
                description=(
                    "Path to one DS cluster config JSON file. Run `dsctl template "
                    "cluster` for an example."
                ),
                parse_default=None,
                discovery_command="dsctl template cluster",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional cluster description.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="cluster.update",
        effects=REMOTE_WRITE,
        route=("cluster", "update"),
        summary="Update one cluster; config may come from --config-file.",
        arguments=(_CLUSTER_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated cluster name. Omit to keep the current name.",
                parse_default=None,
            ),
            InputContract(
                name="config",
                kind="option",
                value_type="string",
                description=(
                    "Updated inline DS cluster config JSON. Omit to keep the "
                    "current config; prefer --config-file for multiline Kubernetes "
                    "configs."
                ),
                parse_default=None,
                discovery_command="dsctl template cluster",
            ),
            InputContract(
                name="config-file",
                kind="option",
                value_type="path",
                description=(
                    "Path to an updated DS cluster config JSON file. Omit both "
                    "config options to keep the current config."
                ),
                parse_default=None,
                discovery_command="dsctl template cluster",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated cluster description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the stored cluster description.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="cluster.delete",
        effects=REMOTE_WRITE,
        route=("cluster", "delete"),
        summary="Delete one cluster.",
        arguments=(_CLUSTER_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete cluster; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="datasource.list",
        effects=REMOTE_READ,
        route=("datasource", "list"),
        summary="List datasource identities and summary fields.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter datasources by name using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="datasource.get",
        effects=REMOTE_READ,
        route=("datasource", "get"),
        summary="Get one datasource by name or id.",
        arguments=(_DATASOURCE_ARGUMENT,),
    ),
    CommandContract(
        action="datasource.create",
        effects=REMOTE_WRITE,
        route=("datasource", "create"),
        summary="Create one datasource from a JSON payload file.",
        options=(
            InputContract(
                name="file",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description=(
                    "Path to one DS-native datasource JSON payload file. Start with"
                    " `dsctl template datasource`, then `dsctl template datasource "
                    "--type TYPE` and pass the saved data.json path here."
                ),
                required=True,
                discovery_command="dsctl template datasource",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
        ),
    ),
    CommandContract(
        action="datasource.update",
        effects=REMOTE_WRITE,
        route=("datasource", "update"),
        summary="Update one datasource from a JSON payload file.",
        arguments=(_DATASOURCE_ARGUMENT,),
        options=(
            InputContract(
                name="file",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description=(
                    "Path to one DS-native datasource JSON payload file. Start from"
                    " `dsctl datasource get DATASOURCE` or `dsctl template "
                    "datasource --type TYPE`, then pass the saved JSON path here. "
                    "Masked password ****** preserves the existing password."
                ),
                required=True,
                discovery_command="dsctl template datasource",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
        ),
    ),
    CommandContract(
        action="datasource.delete",
        effects=REMOTE_WRITE,
        route=("datasource", "delete"),
        summary="Delete one datasource by name or id.",
        arguments=(_DATASOURCE_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete datasource; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="datasource.test",
        effects=REMOTE_READ,
        route=("datasource", "test"),
        summary="Run one datasource connection test after create or update.",
        arguments=(_DATASOURCE_ARGUMENT,),
    ),
    CommandContract(
        action="namespace.list",
        effects=REMOTE_READ,
        route=("namespace", "list"),
        summary="List namespaces with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter namespaces by namespace name using the upstream search "
                    "value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="namespace.get",
        effects=REMOTE_READ,
        route=("namespace", "get"),
        summary="Get one namespace by name or id.",
        arguments=(_NAMESPACE_ARGUMENT,),
    ),
    CommandContract(
        action="namespace.available",
        effects=REMOTE_READ,
        route=("namespace", "available"),
        summary="List namespaces available to the current login user.",
    ),
    CommandContract(
        action="namespace.create",
        effects=REMOTE_WRITE,
        route=("namespace", "create"),
        summary="Create and register one real Kubernetes namespace.",
        options=(
            InputContract(
                name="namespace",
                kind="option",
                value_type="string",
                description="Namespace name.",
                required=True,
            ),
            InputContract(
                name="cluster-code",
                kind="option",
                value_type="integer",
                description=(
                    "`dsctl cluster list` discovers cluster codes; required by DS "
                    "3.1.0+."
                ),
                parse_default=None,
                discovery_command="dsctl cluster list",
                minimum=1,
            ),
            InputContract(
                name="k8s",
                kind="option",
                value_type="string",
                description="Kubernetes cluster selector required by DS 3.0.x only.",
                parse_default=None,
            ),
            InputContract(
                name="limits-cpu",
                kind="option",
                value_type="number",
                description="Optional Kubernetes CPU quota supported through DS 3.2.0.",
                parse_default=None,
                minimum=0,
            ),
            InputContract(
                name="limits-memory",
                kind="option",
                value_type="integer",
                description=(
                    "Optional Kubernetes memory quota supported through DS 3.2.0."
                ),
                parse_default=None,
                minimum=0,
            ),
        ),
    ),
    CommandContract(
        action="namespace.delete",
        effects=REMOTE_WRITE,
        route=("namespace", "delete"),
        summary="Delete a namespace registration and, on DS 3.0-3.1, its K8s object.",
        arguments=(_NAMESPACE_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Confirm deletion. On DS 3.0.0-3.1.9 this also deletes the real"
                    " Kubernetes namespace; DS 3.2.0+ removes only the DS "
                    "registration."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="resource.list",
        effects=REMOTE_READ,
        route=("resource", "list"),
        summary="List resources inside one DS directory.",
        options=(
            InputContract(
                name="dir",
                kind="option",
                value_type="string",
                description=(
                    "DS directory fullName path. Defaults to the upstream base "
                    "directory; run `dsctl resource list` to discover paths."
                ),
                parse_default=None,
                discovery_command="dsctl resource list",
                parameter_name="directory",
            ),
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter resource names by the upstream search value.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="resource.view",
        effects=REMOTE_READ,
        route=("resource", "view"),
        summary="View one text content window for one resource file.",
        arguments=(_RESOURCE_ARGUMENT,),
        options=(
            InputContract(
                name="skip-line-num",
                kind="option",
                value_type="integer",
                description="Number of lines to skip before returning content.",
                parse_default=0,
                minimum=0,
            ),
            InputContract(
                name="limit",
                kind="option",
                value_type="integer",
                description="Maximum number of lines to fetch.",
                parse_default=100,
                minimum=1,
            ),
        ),
    ),
    CommandContract(
        action="resource.upload",
        effects=REMOTE_WRITE,
        route=("resource", "upload"),
        summary="Upload one local file into one DS directory.",
        options=(
            InputContract(
                name="file",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description="Local file path to upload.",
                required=True,
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            _RESOURCE_DESTINATION_DIRECTORY,
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description=(
                    "Override the remote leaf file name. Defaults to the local file"
                    " name."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="resource.create",
        effects=REMOTE_WRITE,
        route=("resource", "create"),
        summary="Create one text resource from inline content.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Remote leaf file name, including the file extension.",
                required=True,
            ),
            InputContract(
                name="content",
                kind="option",
                value_type="string",
                description=(
                    "Inline text content to write into the remote resource file. "
                    "For local files, use `dsctl resource upload --file FILE`."
                ),
                required=True,
            ),
            _RESOURCE_DESTINATION_DIRECTORY,
        ),
    ),
    CommandContract(
        action="resource.mkdir",
        effects=REMOTE_WRITE,
        route=("resource", "mkdir"),
        summary="Create one directory inside one DS resource directory.",
        arguments=(
            InputContract(
                name="name",
                kind="argument",
                value_type="string",
                description="Leaf directory name to create.",
                required=True,
            ),
        ),
        options=(
            InputContract(
                name="dir",
                kind="option",
                value_type="string",
                description=(
                    "Parent DS directory fullName path. Defaults to the upstream "
                    "base directory; run `dsctl resource list` to discover paths."
                ),
                parse_default=None,
                discovery_command="dsctl resource list",
                parameter_name="directory",
            ),
        ),
    ),
    CommandContract(
        action="resource.download",
        effects=REMOTE_READ_LOCAL_FILE_WRITE,
        route=("resource", "download"),
        summary="Download one remote resource to one local file path.",
        arguments=(_RESOURCE_ARGUMENT,),
        options=(
            InputContract(
                name="output",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description=(
                    "Local output file path or existing directory. Defaults to the "
                    "current working directory plus the remote leaf name."
                ),
                parse_default=None,
                path_rules=PathRules(
                    exists=False,
                    file_okay=True,
                    dir_okay=True,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="overwrite",
                kind="option",
                value_type="boolean",
                description="Replace an existing local output file.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="resource.delete",
        effects=REMOTE_WRITE,
        route=("resource", "delete"),
        summary="Delete one resource.",
        arguments=(_RESOURCE_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete resource; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="queue.list",
        effects=REMOTE_READ,
        route=("queue", "list"),
        summary="List queues with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter queues by queue name using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="queue.get",
        effects=REMOTE_READ,
        route=("queue", "get"),
        summary="Get one queue by name or id.",
        arguments=(_QUEUE_ARGUMENT,),
    ),
    CommandContract(
        action="queue.create",
        effects=REMOTE_WRITE,
        route=("queue", "create"),
        summary="Create one queue.",
        options=(
            InputContract(
                name="queue-name",
                kind="option",
                value_type="string",
                description="Human-facing DS queue name used as the selector label.",
                required=True,
            ),
            InputContract(
                name="queue",
                kind="option",
                value_type="string",
                description="Underlying YARN queue value stored in DolphinScheduler.",
                required=True,
            ),
        ),
    ),
    CommandContract(
        action="queue.update",
        effects=REMOTE_WRITE,
        route=("queue", "update"),
        summary="Update one queue.",
        arguments=(
            InputContract(
                name="queue",
                kind="argument",
                value_type="string",
                description=(
                    "Queue name or numeric id. Run `dsctl queue list` to discover "
                    "values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl queue list",
                parameter_name="queue_identifier",
                value_name="QUEUE",
                binding_name="queue-identifier",
            ),
        ),
        options=(
            InputContract(
                name="queue-name",
                kind="option",
                value_type="string",
                description=(
                    "Updated human-facing DS queue name. Omit to keep the current "
                    "queue name."
                ),
                parse_default=None,
            ),
            InputContract(
                name="queue",
                kind="option",
                value_type="string",
                description=(
                    "Updated underlying YARN queue value. Omit to keep the current "
                    "queue value."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="queue.delete",
        effects=REMOTE_WRITE,
        route=("queue", "delete"),
        summary="Delete one queue.",
        arguments=(_QUEUE_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete queue; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="worker-group.list",
        effects=REMOTE_READ,
        route=("worker-group", "list"),
        summary="List worker groups with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter UI worker groups by name using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="worker-group.get",
        effects=REMOTE_READ,
        route=("worker-group", "get"),
        summary="Get one worker group by name or id.",
        arguments=(_WORKER_GROUP_ARGUMENT,),
    ),
    CommandContract(
        action="worker-group.create",
        effects=REMOTE_WRITE,
        route=("worker-group", "create"),
        summary="Create one worker group.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Worker-group name.",
                required=True,
            ),
            InputContract(
                name="addr",
                kind="option",
                value_type="string",
                description=(
                    "Worker server address to include in addrList. Repeat as "
                    "needed; run `dsctl monitor server worker` to discover workers."
                ),
                parse_default=None,
                discovery_command="dsctl monitor server worker",
                parameter_name="addresses",
                multiple=True,
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional worker-group description.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="worker-group.update",
        effects=REMOTE_WRITE,
        route=("worker-group", "update"),
        summary="Update one worker group.",
        arguments=(_WORKER_GROUP_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated worker-group name. Omit to keep the current name.",
                parse_default=None,
            ),
            InputContract(
                name="addr",
                kind="option",
                value_type="string",
                description=(
                    "Replacement worker address list. Repeat as needed. Omit to "
                    "keep the current addrList; run `dsctl monitor server worker` "
                    "to discover workers."
                ),
                parse_default=None,
                discovery_command="dsctl monitor server worker",
                parameter_name="addresses",
                multiple=True,
            ),
            InputContract(
                name="clear-addrs",
                kind="option",
                value_type="boolean",
                description="Clear the current addrList.",
                parse_default=False,
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated worker-group description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the current worker-group description.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="worker-group.delete",
        effects=REMOTE_WRITE,
        route=("worker-group", "delete"),
        summary="Delete one worker group.",
        arguments=(_WORKER_GROUP_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete worker-group; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="task-group.list",
        effects=REMOTE_READ,
        route=("task-group", "list"),
        summary="List task groups.",
        options=(
            InputContract(
                name="project",
                kind="option",
                value_type="string",
                description=(
                    "Project name or code. Use only for project-scoped listing; run"
                    " `dsctl project list` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl project list",
            ),
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter task groups by task-group name.",
                parse_default=None,
            ),
            InputContract(
                name="status",
                kind="option",
                value_type="string",
                description="Filter task groups by status: open, closed, 1, or 0.",
                parse_default=None,
                choices=("open", "closed", "1", "0"),
            ),
            _TASK_GROUP_PAGE_NO,
            _TASK_GROUP_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="task-group.get",
        effects=REMOTE_READ,
        route=("task-group", "get"),
        summary="Get one task group by name or id.",
        arguments=(_TASK_GROUP_ARGUMENT,),
    ),
    CommandContract(
        action="task-group.create",
        effects=REMOTE_WRITE,
        route=("task-group", "create"),
        summary="Create one task group.",
        options=(
            InputContract(
                name="project",
                kind="option",
                value_type="string",
                description=(
                    "Project name or code. Falls back to the selected context project; "
                    "run `dsctl project list` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl project list",
            ),
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Task-group name.",
                required=True,
            ),
            InputContract(
                name="group-size",
                kind="option",
                value_type="integer",
                description="Task-group capacity.",
                required=True,
                minimum=1,
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional task-group description.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="task-group.update",
        effects=REMOTE_WRITE,
        route=("task-group", "update"),
        summary="Update one task group.",
        arguments=(_TASK_GROUP_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated task-group name.",
                parse_default=None,
            ),
            InputContract(
                name="group-size",
                kind="option",
                value_type="integer",
                description="Updated task-group capacity.",
                parse_default=None,
                minimum=1,
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated task-group description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the stored description.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="task-group.close",
        effects=REMOTE_WRITE,
        route=("task-group", "close"),
        summary="Close one task group.",
        arguments=(_TASK_GROUP_ARGUMENT,),
    ),
    CommandContract(
        action="task-group.start",
        effects=REMOTE_WRITE,
        route=("task-group", "start"),
        summary="Start one task group.",
        arguments=(_TASK_GROUP_ARGUMENT,),
    ),
    CommandContract(
        action="task-group.queue.list",
        effects=REMOTE_READ,
        route=("task-group", "queue", "list"),
        summary="List queue rows for one task group.",
        arguments=(_TASK_GROUP_ARGUMENT,),
        options=(
            InputContract(
                name="task-instance",
                kind="option",
                value_type="string",
                description="Filter by task-instance name.",
                parse_default=None,
            ),
            InputContract(
                name="workflow-instance",
                kind="option",
                value_type="string",
                description="Filter by workflow-instance name.",
                parse_default=None,
            ),
            InputContract(
                name="status",
                kind="option",
                value_type="string",
                description=(
                    "Filter by queue status: WAIT_QUEUE, ACQUIRE_SUCCESS, RELEASE, "
                    "-1, 1, or 2."
                ),
                parse_default=None,
                choices=("WAIT_QUEUE", "ACQUIRE_SUCCESS", "RELEASE", "-1", "1", "2"),
            ),
            _TASK_GROUP_PAGE_NO,
            _TASK_GROUP_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="task-group.queue.force-start",
        effects=REMOTE_WRITE,
        route=("task-group", "queue", "force-start"),
        summary=(
            "Request force-start for one waiting queue row. Acceptance does not "
            "prove task execution; inspect the queue and its task instance."
        ),
        arguments=(_TASK_GROUP_QUEUE_ID,),
    ),
    CommandContract(
        action="task-group.queue.set-priority",
        effects=REMOTE_WRITE,
        route=("task-group", "queue", "set-priority"),
        summary="Set one task-group queue priority.",
        arguments=(_TASK_GROUP_QUEUE_ID,),
        options=(
            InputContract(
                name="priority",
                kind="option",
                value_type="integer",
                description="Updated queue priority.",
                required=True,
                minimum=0,
            ),
        ),
    ),
    CommandContract(
        action="alert-plugin.list",
        effects=REMOTE_READ,
        route=("alert-plugin", "list"),
        summary="List alert-plugin instances with optional filtering and pagination.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter alert-plugin instances by instance name.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="alert-plugin.get",
        effects=REMOTE_READ,
        route=("alert-plugin", "get"),
        summary="Get one alert-plugin instance by name or id.",
        arguments=(_ALERT_PLUGIN_ARGUMENT,),
    ),
    CommandContract(
        action="alert-plugin.schema",
        effects=REMOTE_READ,
        route=("alert-plugin", "schema"),
        summary="Get one alert-plugin definition schema by name or id.",
        arguments=(
            InputContract(
                name="plugin",
                kind="argument",
                value_type="string",
                description=(
                    "Alert UI plugin definition name or numeric id. Run `dsctl "
                    "alert-plugin definition list` to discover values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl alert-plugin definition list",
            ),
        ),
    ),
    CommandContract(
        action="alert-plugin.create",
        effects=REMOTE_WRITE,
        route=("alert-plugin", "create"),
        summary="Create one alert-plugin instance.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Alert-plugin instance name.",
                required=True,
            ),
            InputContract(
                name="plugin",
                kind="option",
                value_type="string",
                description=(
                    "Alert UI plugin definition name or numeric id. Run `dsctl "
                    "alert-plugin definition list` to discover values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl alert-plugin definition list",
            ),
            InputContract(
                name="params-json",
                kind="option",
                value_type="string",
                description=(
                    "DS-native alert-plugin UI params JSON array. Run `dsctl "
                    "alert-plugin schema PLUGIN` to inspect fields."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl alert-plugin schema PLUGIN",
            ),
            InputContract(
                name="file",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description=(
                    "Path to one DS-native alert-plugin UI params JSON file. Run "
                    "`dsctl alert-plugin schema PLUGIN` to inspect fields."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl alert-plugin schema PLUGIN",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="param",
                schema_value_name="KEY=VALUE",
                kind="option",
                value_type="string",
                description=(
                    "Alert-plugin UI param in KEY=VALUE form. Repeat for multiple "
                    "fields; run `dsctl alert-plugin schema PLUGIN` to inspect "
                    "keys."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl alert-plugin schema PLUGIN",
                parameter_name="params",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="alert-plugin.update",
        effects=REMOTE_WRITE,
        route=("alert-plugin", "update"),
        summary="Update one alert-plugin instance.",
        arguments=(_ALERT_PLUGIN_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated alert-plugin instance name.",
                parse_default=None,
            ),
            InputContract(
                name="params-json",
                kind="option",
                value_type="string",
                description=(
                    "Replacement DS-native alert-plugin UI params JSON array. Run "
                    "`dsctl alert-plugin schema PLUGIN` to inspect fields."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl alert-plugin schema PLUGIN",
            ),
            InputContract(
                name="file",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description=(
                    "Path to one replacement DS-native alert-plugin UI params JSON "
                    "file. Run `dsctl alert-plugin schema PLUGIN` to inspect "
                    "fields."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl alert-plugin schema PLUGIN",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="param",
                schema_value_name="KEY=VALUE",
                kind="option",
                value_type="string",
                description=(
                    "Replacement alert-plugin UI param in KEY=VALUE form. Repeat "
                    "for multiple fields; omitted fields keep current values. Run "
                    "`dsctl alert-plugin schema PLUGIN` to inspect keys."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl alert-plugin schema PLUGIN",
                parameter_name="params",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="alert-plugin.delete",
        effects=REMOTE_WRITE,
        route=("alert-plugin", "delete"),
        summary="Delete one alert-plugin instance.",
        arguments=(_ALERT_PLUGIN_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete alert-plugin; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="alert-plugin.test",
        effects=REMOTE_WRITE,
        route=("alert-plugin", "test"),
        summary="Send one test alert using one existing alert-plugin instance.",
        arguments=(_ALERT_PLUGIN_ARGUMENT,),
    ),
    CommandContract(
        action="alert-plugin.definition.list",
        effects=REMOTE_READ,
        route=("alert-plugin", "definition", "list"),
        summary="List supported alert-plugin definitions, not configured instances.",
    ),
    CommandContract(
        action="alert-group.list",
        effects=REMOTE_READ,
        route=("alert-group", "list"),
        summary="List alert groups with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter alert groups by group name using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="alert-group.get",
        effects=REMOTE_READ,
        route=("alert-group", "get"),
        summary="Get one alert group by name or id.",
        arguments=(_ALERT_GROUP_ARGUMENT,),
    ),
    CommandContract(
        action="alert-group.create",
        effects=REMOTE_WRITE,
        route=("alert-group", "create"),
        summary="Create one alert group.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Alert-group name.",
                required=True,
            ),
            _ALERT_PLUGIN_INSTANCE_BINDINGS,
            InputContract(
                name="group-type",
                kind="option",
                value_type="string",
                description=(
                    "Legacy alert channel: EMAIL or SMS. Required by DS 1.3.9; omit"
                    " on DS 2.0.0 and newer."
                ),
                parse_default=None,
                choices=("EMAIL", "SMS"),
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional alert-group description.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="alert-group.update",
        effects=REMOTE_WRITE,
        route=("alert-group", "update"),
        summary="Update one alert group.",
        arguments=(_ALERT_GROUP_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated alert-group name. Omit to keep the current name.",
                parse_default=None,
            ),
            _ALERT_PLUGIN_INSTANCE_BINDINGS,
            InputContract(
                name="clear-instance-ids",
                kind="option",
                value_type="boolean",
                description="Clear all bound alert plugin instance ids.",
                parse_default=False,
            ),
            InputContract(
                name="group-type",
                kind="option",
                value_type="string",
                description=(
                    "Updated legacy alert channel: EMAIL or SMS. Supported by DS "
                    "1.3.9 only."
                ),
                parse_default=None,
                choices=("EMAIL", "SMS"),
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated alert-group description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the stored alert-group description.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="alert-group.delete",
        effects=REMOTE_WRITE,
        route=("alert-group", "delete"),
        summary="Delete one alert group.",
        arguments=(_ALERT_GROUP_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete alert-group; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="tenant.list",
        effects=REMOTE_READ,
        route=("tenant", "list"),
        summary="List tenants with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter tenants by tenant code using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="tenant.get",
        effects=REMOTE_READ,
        route=("tenant", "get"),
        summary="Get one tenant by code or id.",
        arguments=(_TENANT_ARGUMENT,),
    ),
    CommandContract(
        action="tenant.create",
        effects=REMOTE_WRITE,
        route=("tenant", "create"),
        summary="Create one tenant.",
        options=(
            InputContract(
                name="tenant-code",
                kind="option",
                value_type="string",
                description="Tenant code.",
                required=True,
            ),
            InputContract(
                name="queue",
                kind="option",
                value_type="string",
                description=(
                    "Queue name or numeric id to bind to this tenant. Run `dsctl "
                    "queue list` to discover values."
                ),
                required=True,
                discovery_command="dsctl queue list",
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional tenant description.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="tenant.update",
        effects=REMOTE_WRITE,
        route=("tenant", "update"),
        summary="Update a tenant's queue or description; its tenant code is immutable.",
        arguments=(_TENANT_ARGUMENT,),
        options=(
            InputContract(
                name="tenant-code",
                kind="option",
                value_type="string",
                description=(
                    "Deprecated compatibility option; the value must match the "
                    "tenant's immutable code."
                ),
                parse_default=None,
                hidden=True,
            ),
            InputContract(
                name="queue",
                kind="option",
                value_type="string",
                description=(
                    "Updated queue name or numeric id. Run `dsctl queue list` to "
                    "discover values; omit to keep the current queue."
                ),
                parse_default=None,
                discovery_command="dsctl queue list",
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated tenant description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the stored tenant description.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="tenant.delete",
        effects=REMOTE_WRITE,
        route=("tenant", "delete"),
        summary="Delete one tenant.",
        arguments=(_TENANT_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete tenant; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="user.list",
        effects=REMOTE_READ,
        route=("user", "list"),
        summary="List users with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter users by user name using the upstream search value."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="user.get",
        effects=REMOTE_READ,
        route=("user", "get"),
        summary="Get one user by name or id.",
        arguments=(_USER_ARGUMENT,),
    ),
    CommandContract(
        action="user.create",
        effects=REMOTE_WRITE,
        route=("user", "create"),
        summary="Create one user.",
        options=(
            InputContract(
                name="user-name",
                kind="option",
                value_type="string",
                description="User name.",
                required=True,
            ),
            InputContract(
                name="password",
                kind="option",
                value_type="string",
                description=(
                    "User password. Prefer --password-file to keep it out of "
                    "command arguments."
                ),
                parse_default=None,
            ),
            InputContract(
                name="password-file",
                kind="option",
                value_type="path",
                value_name="FILE",
                description=(
                    "Read the password from a UTF-8 file; use - for stdin. "
                    "Cannot combine with --password."
                ),
                parse_default=None,
            ),
            InputContract(
                name="email",
                kind="option",
                value_type="string",
                description="User email.",
                required=True,
            ),
            InputContract(
                name="tenant",
                kind="option",
                value_type="string",
                description=(
                    "Tenant code or numeric id. Run `dsctl tenant list` to discover"
                    " values."
                ),
                required=True,
                discovery_command="dsctl tenant list",
            ),
            InputContract(
                name="state",
                kind="option",
                value_type="integer",
                description="User state. Use 1 for enabled and 0 for disabled.",
                required=True,
                choices=(0, 1),
                parameter_name="state_value",
                minimum=0,
                maximum=1,
            ),
            InputContract(
                name="phone",
                kind="option",
                value_type="string",
                description="Optional user phone.",
                parse_default=None,
            ),
            InputContract(
                name="queue",
                kind="option",
                value_type="string",
                description=(
                    "Optional queue-name override stored on the user. Run `dsctl "
                    "queue list` to discover queue names."
                ),
                parse_default=None,
                discovery_command="dsctl queue list",
            ),
        ),
    ),
    CommandContract(
        action="user.update",
        effects=REMOTE_WRITE,
        route=("user", "update"),
        summary="Update one user.",
        arguments=(_USER_ARGUMENT,),
        options=(
            InputContract(
                name="user-name",
                kind="option",
                value_type="string",
                description="Updated user name.",
                parse_default=None,
            ),
            InputContract(
                name="password",
                kind="option",
                value_type="string",
                description=(
                    "Updated password. Prefer --password-file to keep it out of "
                    "command arguments."
                ),
                parse_default=None,
            ),
            InputContract(
                name="password-file",
                kind="option",
                value_type="path",
                value_name="FILE",
                description=(
                    "Read the updated password from a UTF-8 file; use - for stdin. "
                    "Omit both password options to preserve it."
                ),
                parse_default=None,
            ),
            InputContract(
                name="email",
                kind="option",
                value_type="string",
                description="Updated user email.",
                parse_default=None,
            ),
            InputContract(
                name="tenant",
                kind="option",
                value_type="string",
                description=(
                    "Updated tenant code or numeric id. Run `dsctl tenant list` to "
                    "discover values."
                ),
                parse_default=None,
                discovery_command="dsctl tenant list",
            ),
            InputContract(
                name="state",
                kind="option",
                value_type="integer",
                description="Updated user state. Use 1 for enabled and 0 for disabled.",
                parse_default=None,
                choices=(0, 1),
                parameter_name="state_value",
                minimum=0,
                maximum=1,
            ),
            InputContract(
                name="phone",
                kind="option",
                value_type="string",
                description="Updated user phone.",
                parse_default=None,
            ),
            InputContract(
                name="clear-phone",
                kind="option",
                value_type="boolean",
                description="Clear the stored user phone.",
                parse_default=False,
            ),
            InputContract(
                name="queue",
                kind="option",
                value_type="string",
                description=(
                    "Updated queue-name override stored on the user. Run `dsctl "
                    "queue list` to discover queue names."
                ),
                parse_default=None,
                discovery_command="dsctl queue list",
            ),
            InputContract(
                name="clear-queue",
                kind="option",
                value_type="boolean",
                description="Clear the stored queue-name override.",
                parse_default=False,
            ),
            InputContract(
                name="time-zone",
                kind="option",
                value_type="string",
                description="Updated IANA time zone.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="user.delete",
        effects=REMOTE_WRITE,
        route=("user", "delete"),
        summary="Delete one user.",
        arguments=(_USER_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete user; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="user.grant.project",
        effects=REMOTE_WRITE,
        route=("user", "grant", "project"),
        summary="Grant one project to one user with write permission.",
        arguments=(_USER_ARGUMENT, _PROJECT_PERMISSION_ARGUMENT),
    ),
    CommandContract(
        action="user.grant.datasource",
        effects=REMOTE_WRITE,
        route=("user", "grant", "datasource"),
        summary="Grant one or more datasources to one user.",
        arguments=(_USER_ARGUMENT,),
        options=(
            InputContract(
                name="datasource",
                kind="option",
                value_type="string",
                description=(
                    "Datasource name or numeric id. Repeat to grant multiple "
                    "datasources; run `dsctl datasource list` to discover values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl datasource list",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="user.grant.namespace",
        effects=REMOTE_WRITE,
        route=("user", "grant", "namespace"),
        summary="Grant one or more namespaces to one user.",
        arguments=(_USER_ARGUMENT,),
        options=(
            InputContract(
                name="namespace",
                kind="option",
                value_type="string",
                description=(
                    "Namespace name or numeric id. Repeat to grant multiple "
                    "namespaces; run `dsctl namespace list` to discover values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl namespace list",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="user.revoke.project",
        effects=REMOTE_WRITE,
        route=("user", "revoke", "project"),
        summary="Revoke one project from one user.",
        arguments=(_USER_ARGUMENT, _PROJECT_PERMISSION_ARGUMENT),
    ),
    CommandContract(
        action="user.revoke.datasource",
        effects=REMOTE_WRITE,
        route=("user", "revoke", "datasource"),
        summary="Revoke one or more datasources from one user.",
        arguments=(_USER_ARGUMENT,),
        options=(
            InputContract(
                name="datasource",
                kind="option",
                value_type="string",
                description=(
                    "Datasource name or numeric id. Repeat to revoke multiple "
                    "datasources; run `dsctl datasource list` to discover values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl datasource list",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="user.revoke.namespace",
        effects=REMOTE_WRITE,
        route=("user", "revoke", "namespace"),
        summary="Revoke one or more namespaces from one user.",
        arguments=(_USER_ARGUMENT,),
        options=(
            InputContract(
                name="namespace",
                kind="option",
                value_type="string",
                description=(
                    "Namespace name or numeric id. Repeat to revoke multiple "
                    "namespaces; run `dsctl namespace list` to discover values."
                ),
                required=True,
                selector="name_or_id",
                discovery_command="dsctl namespace list",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="access-token.list",
        effects=REMOTE_READ,
        route=("access-token", "list"),
        summary="List access tokens with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter access tokens using the upstream search value.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="access-token.get",
        effects=REMOTE_READ,
        route=("access-token", "get"),
        summary="Get one access token by numeric id.",
        arguments=(_ACCESS_TOKEN_ARGUMENT,),
    ),
    CommandContract(
        action="access-token.create",
        effects=REMOTE_WRITE,
        route=("access-token", "create"),
        summary="Create one access token.",
        options=(
            _ACCESS_TOKEN_USER,
            _ACCESS_TOKEN_EXPIRATION,
            InputContract(
                name="token",
                kind="option",
                value_type="string",
                description="Optional token string. Omit to let DS generate one.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="access-token.update",
        effects=REMOTE_WRITE,
        route=("access-token", "update"),
        summary="Update one access token by numeric id.",
        arguments=(_ACCESS_TOKEN_ARGUMENT,),
        options=(
            InputContract(
                name="user",
                kind="option",
                value_type="string",
                description=(
                    "Updated user name or numeric id. Run `dsctl user list` to "
                    "discover values."
                ),
                parse_default=None,
                selector="name_or_id",
                discovery_command="dsctl user list",
            ),
            InputContract(
                name="expire-time",
                kind="option",
                value_type="string",
                description=(
                    "Updated token expiration time, for example '2027-01-01 00:00:00'. "
                    "Required on DS 1.3.9 and 2.0.x."
                ),
                parse_default=None,
            ),
            InputContract(
                name="token",
                kind="option",
                value_type="string",
                description="Updated token string.",
                parse_default=None,
            ),
            InputContract(
                name="regenerate-token",
                kind="option",
                value_type="boolean",
                description="Ask DS to generate a fresh token string.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="access-token.delete",
        effects=REMOTE_WRITE,
        route=("access-token", "delete"),
        summary="Delete one access token by numeric id.",
        arguments=(_ACCESS_TOKEN_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete access-token; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="access-token.generate",
        effects=REMOTE_READ,
        route=("access-token", "generate"),
        summary="Generate one token string without persisting it.",
        options=(_ACCESS_TOKEN_USER, _ACCESS_TOKEN_EXPIRATION),
    ),
    CommandContract(
        action="monitor.health",
        effects=REMOTE_READ,
        route=("monitor", "health"),
        summary="Get the API server actuator health payload.",
    ),
    CommandContract(
        action="monitor.server",
        effects=REMOTE_READ,
        route=("monitor", "server"),
        summary="List registry-backed servers for one node type.",
        arguments=(
            InputContract(
                name="node_type",
                kind="argument",
                value_type="string",
                description="Server node type: master, worker, or alert-server.",
                required=True,
                value_name="TYPE",
            ),
        ),
    ),
    CommandContract(
        action="monitor.database",
        effects=REMOTE_READ,
        route=("monitor", "database"),
        summary="List database health metrics reported by the monitor API.",
    ),
    CommandContract(
        action="audit.list",
        effects=REMOTE_READ,
        route=("audit", "list"),
        summary="List audit-log rows with optional filters.",
        options=(
            InputContract(
                name="model-type",
                kind="option",
                value_type="string",
                description=(
                    "Audit model type filter. Repeat as needed; run `dsctl audit "
                    "model-types` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl audit model-types",
                parameter_name="model_types",
                multiple=True,
            ),
            InputContract(
                name="operation-type",
                kind="option",
                value_type="string",
                description=(
                    "Audit operation type filter. Repeat as needed; run `dsctl "
                    "audit operation-types` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl audit operation-types",
                parameter_name="operation_types",
                multiple=True,
            ),
            InputContract(
                name="start",
                kind="option",
                value_type="string",
                description="Start datetime in DS format 'YYYY-MM-DD HH:MM:SS'.",
                parse_default=None,
            ),
            InputContract(
                name="end",
                kind="option",
                value_type="string",
                description="End datetime in DS format 'YYYY-MM-DD HH:MM:SS'.",
                parse_default=None,
            ),
            InputContract(
                name="user-name",
                kind="option",
                value_type="string",
                description="Filter by audit actor user name.",
                parse_default=None,
            ),
            InputContract(
                name="model-name",
                kind="option",
                value_type="string",
                description="Filter by audited model name.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="audit.model-types",
        effects=REMOTE_READ,
        route=("audit", "model-types"),
        summary="List DS audit model types.",
    ),
    CommandContract(
        action="audit.operation-types",
        effects=REMOTE_READ,
        route=("audit", "operation-types"),
        summary="List DS audit operation types.",
    ),
    CommandContract(
        action="project.list",
        effects=REMOTE_READ,
        route=("project", "list"),
        summary="List projects with optional filtering and pagination controls.",
        options=(
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter projects by name using the upstream search value.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="project.get",
        effects=REMOTE_READ,
        route=("project", "get"),
        summary=f"Get one project by name or {_PROJECT_NATIVE_IDENTITY}.",
        arguments=(_PROJECT_ARGUMENT,),
    ),
    CommandContract(
        action="project.create",
        effects=REMOTE_WRITE,
        route=("project", "create"),
        summary="Create a project.",
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Project name.",
                required=True,
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Optional project description.",
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="project.update",
        effects=REMOTE_WRITE,
        route=("project", "update"),
        summary="Update a project.",
        arguments=(_PROJECT_ARGUMENT,),
        options=(
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated project name. Omit to keep the current name.",
                parse_default=None,
            ),
            InputContract(
                name="description",
                kind="option",
                value_type="string",
                description="Updated project description.",
                parse_default=None,
            ),
            InputContract(
                name="clear-description",
                kind="option",
                value_type="boolean",
                description="Clear the stored project description.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="project.delete",
        effects=REMOTE_WRITE,
        route=("project", "delete"),
        summary="Delete a project.",
        arguments=(_PROJECT_ARGUMENT,),
        options=(
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete project; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="project-parameter.list",
        effects=REMOTE_READ,
        route=("project-parameter", "list"),
        summary="List project parameters inside one selected project.",
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter project parameters by name using the upstream search value."
                ),
                parse_default=None,
            ),
            InputContract(
                name="data-type",
                kind="option",
                value_type="string",
                description=(
                    "Filter by DS projectParameterDataType. Run `dsctl enum list "
                    "data-type` to discover values. "
                    "Non-VARCHAR types require DS 3.3.1+."
                ),
                parse_default=None,
                discovery_command="dsctl enum list data-type",
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="project-parameter.get",
        effects=REMOTE_READ,
        route=("project-parameter", "get"),
        summary="Get one project parameter by name or code.",
        arguments=(_PROJECT_PARAMETER_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="project-parameter.create",
        effects=REMOTE_WRITE,
        route=("project-parameter", "create"),
        summary="Create one project parameter.",
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Project parameter name.",
                required=True,
            ),
            InputContract(
                name="value",
                kind="option",
                value_type="string",
                description="Project parameter value.",
                required=True,
            ),
            InputContract(
                name="data-type",
                kind="option",
                value_type="string",
                description=(
                    "DS projectParameterDataType value. Run `dsctl enum list "
                    "data-type` to discover values. "
                    "Non-VARCHAR types require DS 3.3.1+."
                ),
                parse_default="VARCHAR",
                discovery_command="dsctl enum list data-type",
            ),
        ),
    ),
    CommandContract(
        action="project-parameter.update",
        effects=REMOTE_WRITE,
        route=("project-parameter", "update"),
        summary="Update one project parameter.",
        arguments=(_PROJECT_PARAMETER_ARGUMENT,),
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="name",
                kind="option",
                value_type="string",
                description="Updated parameter name. Omit to keep the current name.",
                parse_default=None,
            ),
            InputContract(
                name="value",
                kind="option",
                value_type="string",
                description="Updated parameter value. Omit to keep the current value.",
                parse_default=None,
            ),
            InputContract(
                name="data-type",
                kind="option",
                value_type="string",
                description=(
                    "Updated DS projectParameterDataType value. Run `dsctl enum "
                    "list data-type` to discover values. "
                    "Non-VARCHAR types require DS 3.3.1+."
                ),
                parse_default=None,
                discovery_command="dsctl enum list data-type",
            ),
        ),
    ),
    CommandContract(
        action="project-parameter.delete",
        effects=REMOTE_WRITE,
        route=("project-parameter", "delete"),
        summary="Delete one project parameter.",
        arguments=(_PROJECT_PARAMETER_ARGUMENT,),
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete project parameter; omission fails without "
                    "prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="project-preference.get",
        effects=REMOTE_READ,
        route=("project-preference", "get"),
        summary=(
            "Get the singleton project preference default source for one "
            "selected project."
        ),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="project-preference.update",
        effects=REMOTE_WRITE,
        route=("project-preference", "update"),
        summary="Create or update the selected project-level default-value source.",
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="preferences-json",
                kind="option",
                value_type="string",
                description=(
                    "Inline JSON object used as the DS project preference payload."
                ),
                parse_default=None,
            ),
            InputContract(
                name="file",
                schema_value_name="PATH",
                kind="option",
                value_type="path",
                description=(
                    "Path to one JSON object file for the DS project preference "
                    "payload."
                ),
                parse_default=None,
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
        ),
    ),
    CommandContract(
        action="project-preference.enable",
        effects=REMOTE_WRITE,
        route=("project-preference", "enable"),
        summary="Enable the selected project preference default-value source.",
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="project-preference.disable",
        effects=REMOTE_WRITE,
        route=("project-preference", "disable"),
        summary="Disable the selected project preference default-value source.",
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="project-worker-group.list",
        effects=REMOTE_READ,
        route=("project-worker-group", "list"),
        summary="List the worker groups currently reported for one selected project.",
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="project-worker-group.set",
        effects=REMOTE_WRITE,
        route=("project-worker-group", "set"),
        summary=(
            "Replace the explicit worker-group assignment set for one selected project."
        ),
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="worker-group",
                kind="option",
                value_type="string",
                description=(
                    "Worker group to keep assigned to this project. Repeat as "
                    "needed; run `dsctl worker-group list` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl worker-group list",
                parameter_name="worker_groups",
                multiple=True,
            ),
        ),
    ),
    CommandContract(
        action="project-worker-group.clear",
        effects=REMOTE_WRITE,
        route=("project-worker-group", "clear"),
        summary=(
            "Clear the explicit worker-group assignment set for one selected project."
        ),
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Confirm removal of all explicit project worker-group assignments."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="schedule.list",
        effects=REMOTE_READ,
        route=("schedule", "list"),
        summary="List schedules inside one project.",
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="workflow",
                kind="option",
                value_type="string",
                description=(
                    "Exact workflow name or code to narrow the project schedule "
                    "list. Run `dsctl workflow list` in the selected project to "
                    "discover values."
                ),
                parse_default=None,
                selector="name_or_code",
                discovery_command="dsctl workflow list",
            ),
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter schedules by workflow name substring within the "
                    "selected project."
                ),
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="schedule.get",
        effects=REMOTE_READ,
        route=("schedule", "get"),
        summary="Get one schedule by id.",
        arguments=(_SCHEDULE_ID_ARGUMENT,),
        options=(_SCHEDULE_ID_PROJECT_OPTION,),
    ),
    CommandContract(
        action="schedule.preview",
        effects=REMOTE_READ,
        route=("schedule", "preview"),
        summary="Preview the next fire times for a schedule.",
        arguments=(
            InputContract(
                name="schedule_id",
                kind="argument",
                value_type="integer",
                description=(
                    "Existing schedule id to preview. Use `dsctl schedule list` to "
                    "discover values."
                ),
                parse_default=None,
                selector="id",
                discovery_command="dsctl schedule list",
            ),
        ),
        options=(
            InputContract(
                name="project",
                kind="option",
                value_type="string",
                description=(
                    f"Project name or {_PROJECT_NATIVE_IDENTITY}. For an existing "
                    "schedule id, constrains lookup to this project. For ad hoc "
                    "preview, selects the preview project. Falls back to the selected "
                    "context project; without either scope, id lookup searches the "
                    "bounded visible project inventory."
                ),
                parse_default=None,
                selector="name_or_native_identity",
                discovery_command="dsctl project list",
            ),
            InputContract(
                name="cron",
                kind="option",
                value_type="string",
                description=(
                    "Quartz cron expression for an ad hoc preview (6 or 7 fields, "
                    "seconds first)."
                ),
                parse_default=None,
            ),
            _SCHEDULE_START_TIME,
            _SCHEDULE_END_TIME,
            _SCHEDULE_PREVIEW_TIMEZONE,
        ),
    ),
    CommandContract(
        action="schedule.explain",
        effects=REMOTE_READ,
        route=("schedule", "explain"),
        summary="Explain one schedule create or update mutation.",
        arguments=(
            InputContract(
                name="schedule_id",
                kind="argument",
                value_type="integer",
                description=(
                    "Existing schedule id to explain as an update. Use `dsctl "
                    "schedule list` to discover values."
                ),
                parse_default=None,
                selector="id",
                discovery_command="dsctl schedule list",
            ),
        ),
        options=(
            InputContract(
                name="workflow",
                kind="option",
                value_type="string",
                description=(
                    "Workflow name or code for create explain only. Run `dsctl "
                    "workflow list` in the selected project to discover values. "
                    "When SCHEDULE_ID is omitted, --workflow is required; "
                    "do not pass --workflow with "
                    "SCHEDULE_ID."
                ),
                parse_default=None,
                selector="name_or_code",
                discovery_command="dsctl workflow list",
            ),
            InputContract(
                name="project",
                kind="option",
                value_type="string",
                description=(
                    f"Project name or {_PROJECT_NATIVE_IDENTITY}. For create explain, "
                    "selects the workflow project. With SCHEDULE_ID, constrains lookup "
                    "to this project. Run `dsctl project list` to discover values. "
                    "Falls back to the selected context project; without either scope, "
                    "id lookup searches the bounded visible project inventory."
                ),
                parse_default=None,
                selector="name_or_native_identity",
                discovery_command="dsctl project list",
            ),
            InputContract(
                name="cron",
                kind="option",
                value_type="string",
                description="Quartz cron expression (6 or 7 fields, seconds first).",
                parse_default=None,
            ),
            _SCHEDULE_START_TIME,
            _SCHEDULE_END_TIME,
            _SCHEDULE_PREVIEW_TIMEZONE,
            _SCHEDULE_MISSED_FIRE_POLICY,
            _SCHEDULE_FAILURE_STRATEGY,
            _SCHEDULE_WARNING_TYPE,
            InputContract(
                name="warning-group-id",
                kind="option",
                value_type="integer",
                description=(
                    "Warning group id for create explain or updated value for "
                    "update explain. Create explain can also inherit enabled "
                    "project preference when omitted; run `dsctl alert-group list` "
                    "to discover ids."
                ),
                parse_default=None,
                discovery_command="dsctl alert-group list",
                minimum=0,
            ),
            _SCHEDULE_PRIORITY,
            InputContract(
                name="worker-group",
                kind="option",
                value_type="string",
                description=(
                    "Worker group for create explain or updated value for update "
                    "explain. Create explain can also inherit enabled project "
                    "preference when omitted; run `dsctl worker-group list` to "
                    "discover values."
                ),
                parse_default=None,
                discovery_command="dsctl worker-group list",
            ),
            InputContract(
                name="tenant-code",
                kind="option",
                value_type="string",
                description=(
                    "Tenant code for create explain. Create explain can also "
                    "inherit enabled project preference when omitted; run `dsctl "
                    "tenant list` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl tenant list",
            ),
            InputContract(
                name="environment-code",
                kind="option",
                value_type="integer",
                description=(
                    "Environment selection for create or update explain. For "
                    "create, omit to allow enabled project preference and pass 0 to"
                    " explicitly use no environment. For update, omit to preserve "
                    "the current value and pass 0 to clear it. Run `dsctl "
                    "environment list` to discover positive codes (DS 3.2.2+). "
                    "On older DS, set task environment_code."
                ),
                parse_default=None,
                discovery_command="dsctl environment list",
                minimum=0,
            ),
        ),
    ),
    CommandContract(
        action="schedule.create",
        effects=REMOTE_WRITE,
        route=("schedule", "create"),
        summary="Create one schedule.",
        options=(
            _WORKFLOW_OPTION,
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="cron",
                kind="option",
                value_type="string",
                description="Quartz cron expression (6 or 7 fields, seconds first).",
                required=True,
            ),
            InputContract(
                name="start",
                kind="option",
                value_type="string",
                description="Schedule start time in DS datetime string format.",
                required=True,
            ),
            InputContract(
                name="end",
                kind="option",
                value_type="string",
                description="Schedule end time in DS datetime string format.",
                required=True,
            ),
            InputContract(
                name="timezone",
                kind="option",
                value_type="string",
                description=(
                    "Timezone id, for example Asia/Shanghai. Required on DS 2.0+; "
                    "omit on DS 1.3, whose schedule contract uses server-local "
                    "time."
                ),
                parse_default=None,
                required=True,
            ),
            _SCHEDULE_MISSED_FIRE_POLICY,
            _SCHEDULE_FAILURE_STRATEGY,
            _SCHEDULE_WARNING_TYPE,
            InputContract(
                name="warning-group-id",
                kind="option",
                value_type="integer",
                description=(
                    "Warning group id. Omit to keep the CLI fallback chain, "
                    "including enabled project preference; run `dsctl alert-group "
                    "list` to discover ids."
                ),
                parse_default=None,
                discovery_command="dsctl alert-group list",
                minimum=0,
            ),
            _SCHEDULE_PRIORITY,
            InputContract(
                name="worker-group",
                kind="option",
                value_type="string",
                description=(
                    "Worker group. Omit to allow enabled project preference; run "
                    "`dsctl worker-group list` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl worker-group list",
            ),
            InputContract(
                name="tenant-code",
                kind="option",
                value_type="string",
                description=(
                    "Tenant code. Omit to allow enabled project preference; run "
                    "`dsctl tenant list` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl tenant list",
            ),
            InputContract(
                name="environment-code",
                kind="option",
                value_type="integer",
                description=(
                    "Environment selection. Omit to allow enabled project "
                    "preference and otherwise create without an environment; pass 0"
                    " to explicitly use no environment and bypass project "
                    "preference. Run `dsctl environment list` to discover positive "
                    "codes (DS 3.2.2+). On older DS, set task environment_code."
                ),
                parse_default=None,
                discovery_command="dsctl environment list",
                minimum=0,
            ),
            _SCHEDULE_CONFIRM_RISK,
        ),
    ),
    CommandContract(
        action="schedule.update",
        effects=REMOTE_WRITE,
        route=("schedule", "update"),
        summary=(
            "Update one offline schedule. Use schedule explain to review inputs "
            "and schedule preview to inspect fire times; activate separately."
        ),
        arguments=(_SCHEDULE_ID_ARGUMENT,),
        options=(
            _SCHEDULE_ID_PROJECT_OPTION,
            InputContract(
                name="cron",
                kind="option",
                value_type="string",
                description=(
                    "Updated Quartz cron expression (6 or 7 fields, seconds first)."
                    " Omit to keep the current value."
                ),
                parse_default=None,
            ),
            InputContract(
                name="start",
                kind="option",
                value_type="string",
                description=(
                    "Updated schedule start time. Omit to keep the current value."
                ),
                parse_default=None,
            ),
            InputContract(
                name="end",
                kind="option",
                value_type="string",
                description=(
                    "Updated schedule end time. Omit to keep the current value."
                ),
                parse_default=None,
            ),
            InputContract(
                name="timezone",
                kind="option",
                value_type="string",
                description="Updated timezone id. Omit to keep the current value.",
                parse_default=None,
            ),
            _SCHEDULE_MISSED_FIRE_POLICY,
            _SCHEDULE_FAILURE_STRATEGY,
            _SCHEDULE_WARNING_TYPE,
            InputContract(
                name="warning-group-id",
                kind="option",
                value_type="integer",
                description=(
                    "Updated warning group id. Run `dsctl alert-group list` to "
                    "discover ids; omit to keep the current value."
                ),
                parse_default=None,
                discovery_command="dsctl alert-group list",
                minimum=0,
            ),
            _SCHEDULE_PRIORITY,
            InputContract(
                name="worker-group",
                kind="option",
                value_type="string",
                description=(
                    "Updated worker group. Run `dsctl worker-group list` to "
                    "discover values; omit to keep the current value."
                ),
                parse_default=None,
                discovery_command="dsctl worker-group list",
            ),
            InputContract(
                name="environment-code",
                kind="option",
                value_type="integer",
                description=(
                    "Updated environment selection. Omit to keep the current value;"
                    " pass 0 to clear the environment. Run `dsctl environment list`"
                    " to discover positive codes (DS 3.2.2+). On older DS, set "
                    "task environment_code."
                ),
                parse_default=None,
                discovery_command="dsctl environment list",
                minimum=0,
            ),
            _SCHEDULE_CONFIRM_RISK,
        ),
    ),
    CommandContract(
        action="schedule.delete",
        effects=REMOTE_WRITE,
        route=("schedule", "delete"),
        summary="Delete one schedule.",
        arguments=(_SCHEDULE_ID_ARGUMENT,),
        options=(
            _SCHEDULE_ID_PROJECT_OPTION,
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete schedule; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="schedule.online",
        effects=REMOTE_WRITE,
        route=("schedule", "online"),
        summary="Activate one schedule; its workflow definition must be online.",
        arguments=(_SCHEDULE_ID_ARGUMENT,),
        options=(_SCHEDULE_ID_PROJECT_OPTION,),
    ),
    CommandContract(
        action="schedule.offline",
        effects=REMOTE_WRITE,
        route=("schedule", "offline"),
        summary="Bring one schedule offline.",
        arguments=(_SCHEDULE_ID_ARGUMENT,),
        options=(_SCHEDULE_ID_PROJECT_OPTION,),
    ),
    CommandContract(
        action="template.workflow",
        effects=NO_EFFECTS,
        route=("template", "workflow"),
        summary="Emit a workflow YAML template for the selected DS version.",
        options=(
            InputContract(
                name="with-schedule",
                kind="option",
                value_type="boolean",
                description=(
                    "Include one optional schedule block in the emitted template."
                ),
                parse_default=None,
                fixed_default=False,
            ),
            InputContract(
                name="raw",
                kind="option",
                value_type="boolean",
                description=(
                    "Print only the workflow YAML template, without the JSON envelope."
                ),
                parse_default=None,
                fixed_default=False,
            ),
            InputContract(
                name="example",
                kind="option",
                value_type="string",
                description=(
                    "Complete workflow scenario: basic (default), output, branch, "
                    "child or dependent. Exact task support still applies."
                ),
                choices=("basic", "output", "branch", "child", "dependent"),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="template.workflow-patch",
        effects=NO_EFFECTS,
        route=("template", "workflow-patch"),
        summary="Emit the stable workflow edit patch YAML template.",
        options=(
            InputContract(
                name="raw",
                kind="option",
                value_type="boolean",
                description=(
                    "Print only the workflow patch YAML template, without the JSON "
                    "envelope."
                ),
                parse_default=None,
                fixed_default=False,
            ),
        ),
    ),
    CommandContract(
        action="template.workflow-instance-patch",
        effects=NO_EFFECTS,
        route=("template", "workflow-instance-patch"),
        summary="Emit the stable workflow-instance edit patch YAML template.",
        options=(
            InputContract(
                name="raw",
                kind="option",
                value_type="boolean",
                description=(
                    "Print only the workflow-instance patch YAML template, without "
                    "the JSON envelope."
                ),
                parse_default=None,
                fixed_default=False,
            ),
        ),
    ),
    CommandContract(
        action="template.params",
        effects=NO_EFFECTS,
        route=("template", "params"),
        summary="Emit stable DS parameter syntax metadata and examples.",
        options=(
            InputContract(
                name="topic",
                kind="option",
                value_type="string",
                description=(
                    "Parameter syntax topic. Run without --topic for compact "
                    "discovery. Supported: overview, property, built-in, time, "
                    "context, output, all."
                ),
                parse_default=None,
                discovery_command="dsctl template params",
            ),
        ),
    ),
    CommandContract(
        action="template.environment",
        effects=NO_EFFECTS,
        route=("template", "environment"),
        summary="Emit a DS environment shell/export config template.",
    ),
    CommandContract(
        action="template.cluster",
        effects=NO_EFFECTS,
        route=("template", "cluster"),
        summary="Emit a DS cluster config JSON template.",
    ),
    CommandContract(
        action="template.datasource",
        effects=NO_EFFECTS,
        route=("template", "datasource"),
        summary="Emit datasource JSON payload-template type discovery or one template.",
        options=(
            InputContract(
                name="type",
                kind="option",
                value_type="string",
                description=(
                    "Datasource type to template. Omit for compact type discovery. "
                    "Run `dsctl template datasource --ds-version VERSION` for "
                    "exact-version values. Current stable examples: MYSQL, "
                    "POSTGRESQL, HIVE, SPARK, CLICKHOUSE, ORACLE."
                ),
                parse_default=None,
                discovery_command="dsctl template datasource",
                parameter_name="datasource_type",
            ),
            InputContract(
                name="ds-version",
                kind="option",
                value_type="string",
                description=(
                    "Exact DolphinScheduler version for type and plugin-field "
                    "discovery."
                ),
                parse_default=None,
                discovery_command="dsctl version",
            ),
        ),
    ),
    CommandContract(
        action="template.task",
        effects=NO_EFFECTS,
        route=("template", "task"),
        summary="Emit one task YAML template or list supported task types.",
        arguments=(
            InputContract(
                name="task_type",
                kind="argument",
                value_type="string",
                description=(
                    "Task type to template. Omit for a compact template catalog. "
                    "Run `dsctl task-type get TYPE` for per-type guidance."
                ),
                parse_default=None,
                discovery_command="dsctl template task",
            ),
        ),
        options=(
            InputContract(
                name="variant",
                kind="option",
                value_type="string",
                description=(
                    "Task scenario listed by `dsctl task-type get TYPE`. "
                    "Omit to use the main template."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl task-type get TYPE",
            ),
            InputContract(
                name="raw",
                kind="option",
                value_type="boolean",
                description=(
                    "Print only the YAML task fragment, without the JSON envelope."
                ),
                parse_default=None,
                fixed_default=False,
            ),
        ),
    ),
    CommandContract(
        action="task-type.list",
        effects=REMOTE_READ,
        route=("task-type", "list"),
        summary=(
            "List live DS task types, categories, favourite flags, and CLI "
            "authoring coverage."
        ),
    ),
    CommandContract(
        action="task-type.get",
        effects=NO_EFFECTS,
        route=("task-type", "get"),
        summary="Summarize the local authoring contract for one task type.",
        arguments=(
            InputContract(
                name="task_type",
                kind="argument",
                value_type="string",
                description=(
                    "Task type to inspect. Discover values with `dsctl template "
                    "task` or the live catalog with `dsctl task-type list`."
                ),
                required=True,
                discovery_command="dsctl template task",
            ),
        ),
    ),
    CommandContract(
        action="task-type.schema",
        effects=NO_EFFECTS,
        route=("task-type", "schema"),
        summary=(
            "Print a bounded field contract for one task type; select "
            "detailed views explicitly."
        ),
        arguments=(
            InputContract(
                name="task_type",
                kind="argument",
                value_type="string",
                description=(
                    "Task type whose local authoring schema should be printed. "
                    "Discover values with `dsctl template task`."
                ),
                required=True,
                discovery_command="dsctl template task",
            ),
        ),
        options=(
            InputContract(
                name="field",
                kind="option",
                value_type="string",
                description=(
                    "Return one exact authoring field and its related state rules. "
                    "Discover paths with the default bounded field view; quote "
                    "paths containing []."
                ),
                parse_default=None,
                discovery_command_pattern="dsctl task-type schema TYPE",
            ),
            InputContract(
                name="json-schema",
                kind="option",
                value_type="boolean",
                description=(
                    "Return the nested JSON Schema without repeated authoring metadata."
                ),
                parse_default=False,
            ),
            InputContract(
                name="compile-mappings",
                kind="option",
                value_type="boolean",
                description="Return authoring-path to DS REST payload mappings.",
                parse_default=False,
            ),
            InputContract(
                name="full",
                kind="option",
                value_type="boolean",
                description=(
                    "Return the former expanded authoring contract for compatibility."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="workflow.list",
        effects=REMOTE_READ,
        route=("workflow", "list"),
        summary="List workflows with optional filtering and pagination controls.",
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter workflows by name using the upstream search value.",
                parse_default=None,
            ),
            _BOUNDED_PAGE_NO,
            _BOUNDED_PAGE_SIZE,
            _ALL_PAGES,
        ),
    ),
    CommandContract(
        action="workflow.get",
        effects=REMOTE_READ,
        route=("workflow", "get"),
        summary="Get one workflow by name or code.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.export",
        effects=REMOTE_READ,
        route=("workflow", "export"),
        raw_output_format="yaml",
        summary=("Export raw YAML for clone/create or read-only schedule-aware edit."),
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.describe",
        effects=REMOTE_READ,
        route=("workflow", "describe"),
        summary="Describe one workflow with tasks and relations.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.digest",
        effects=REMOTE_READ,
        route=("workflow", "digest"),
        summary="Return one compact workflow graph summary.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.create",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("workflow", "create"),
        summary="Create one workflow definition from a YAML file.",
        options=(
            InputContract(
                name="file",
                kind="option",
                value_type="path",
                description=(
                    "Path to one workflow YAML specification file. Start from "
                    "`dsctl template workflow --raw`; add task fragments with "
                    "`dsctl template task`, and inspect task fields with `dsctl "
                    "task-type schema TYPE`. Validate the authored DAG with `dsctl "
                    "lint workflow FILE`."
                ),
                required=True,
                discovery_command="dsctl template workflow --raw",
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="project",
                kind="option",
                value_type="string",
                description=(
                    f"Project name or {_PROJECT_NATIVE_IDENTITY}. Overrides "
                    "workflow.project from the YAML file; run `dsctl project list` "
                    "to discover values."
                ),
                parse_default=None,
                selector="name_or_native_identity",
                discovery_command="dsctl project list",
            ),
            InputContract(
                name="dry-run",
                kind="option",
                value_type="boolean",
                description=(
                    "Compile and print the full DS request without sending it. For "
                    "bounded DAG validation, use `dsctl lint workflow FILE`."
                ),
                parse_default=False,
            ),
            InputContract(
                name="confirm-risk",
                kind="option",
                value_type="string",
                description=(
                    "Explicit confirmation token returned by a previous high-risk "
                    "schedule validation failure."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="workflow.edit",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("workflow", "edit"),
        summary="Edit one workflow definition from a YAML patch or full YAML file.",
        arguments=(
            InputContract(
                name="workflow",
                kind="argument",
                value_type="string",
                description=(
                    "Workflow name or numeric code. Run `dsctl workflow list` in "
                    "the selected project to discover values. Pass WORKFLOW explicitly "
                    "with either --file or --patch."
                ),
                required=True,
                selector="name_or_code",
                discovery_command="dsctl workflow list",
            ),
        ),
        options=(
            InputContract(
                name="patch",
                kind="option",
                value_type="path",
                description=(
                    "Path to one workflow patch YAML file. Use exactly one of "
                    "--patch or --file. Inspect the current definition with `dsctl "
                    "workflow export WORKFLOW`, then write only the intended delta."
                    " Start from `dsctl template workflow-patch --raw`; use "
                    "--dry-run to inspect the compiled diff. `tasks.create[]` uses "
                    "full task fragments from `dsctl template task`; "
                    "`tasks.update[].set` uses partial task fields discovered with "
                    "`dsctl task-type schema TYPE`."
                ),
                parse_default=None,
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="file",
                kind="option",
                value_type="path",
                description=(
                    "Path to one full workflow YAML file describing the desired "
                    "definition state. Use exactly one of --patch or --file. Start "
                    "from `dsctl workflow export WORKFLOW` or `dsctl template "
                    "workflow --raw`; use --dry-run to inspect the compiled diff. "
                    "Full-file edits match task identity by exact task name and do "
                    "not infer renames. An exported `schedule:` block is verified "
                    "as a read-only snapshot and remains unchanged; use schedule "
                    "commands to modify it."
                ),
                parse_default=None,
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="dry-run",
                kind="option",
                value_type="boolean",
                description=(
                    "Compile the merged workflow edit payload without sending it. "
                    "When the full request is unnecessary, use `--columns "
                    "diff,no_change,workflow_state_constraints,schedule_impacts` "
                    "for a bounded review."
                ),
                parse_default=False,
            ),
            InputContract(
                name="confirm-risk",
                kind="option",
                value_type="string",
                description=(
                    "Explicit confirmation token returned by a previous high-risk "
                    "full-file edit validation failure."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="workflow.online",
        effects=REMOTE_WRITE,
        route=("workflow", "online"),
        summary=(
            "Bring one workflow definition online. Run it manually or configure "
            "its schedule separately; this does not reactivate an offline schedule."
        ),
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.offline",
        effects=REMOTE_WRITE,
        route=("workflow", "offline"),
        summary=(
            "Bring one workflow definition and its attached schedule offline. "
            "Bringing the definition online again does not reactivate the schedule."
        ),
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.run",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("workflow", "run"),
        summary=(
            "Trigger one workflow definition and return created workflow instance ids."
        ),
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(
            _PROJECT_CONTEXT_OPTION,
            _START_WORKER_GROUP,
            _START_TENANT,
            _START_FAILURE_STRATEGY,
            _START_PRIORITY,
            _START_WARNING_TYPE,
            _START_WARNING_GROUP_ID,
            _START_ENVIRONMENT_CODE,
            _START_PARAMETERS,
            _START_COMPILE_DRY_RUN,
            _START_EXECUTION_DRY_RUN,
        ),
    ),
    CommandContract(
        action="workflow.run-task",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("workflow", "run-task"),
        summary="Start one workflow definition from a selected task.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(
            InputContract(
                name="task",
                kind="option",
                value_type="string",
                description=(
                    "Task name or numeric code inside the selected workflow. Run "
                    "`dsctl task list --project PROJECT --workflow WORKFLOW` to "
                    "discover values."
                ),
                required=True,
                selector="name_or_code",
                discovery_command_pattern=(
                    "dsctl task list --project PROJECT --workflow WORKFLOW"
                ),
            ),
            _PROJECT_CONTEXT_OPTION,
            _TASK_EXECUTION_SCOPE,
            _START_WORKER_GROUP,
            _START_TENANT,
            _START_FAILURE_STRATEGY,
            _START_PRIORITY,
            _START_WARNING_TYPE,
            _START_WARNING_GROUP_ID,
            _START_ENVIRONMENT_CODE,
            _START_PARAMETERS,
            _START_COMPILE_DRY_RUN,
            _START_EXECUTION_DRY_RUN,
        ),
    ),
    CommandContract(
        action="workflow.backfill",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("workflow", "backfill"),
        summary=(
            "Backfill one workflow definition and return created workflow instance ids."
        ),
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="start",
                kind="option",
                value_type="string",
                description=(
                    "Complement start datetime, for example '2026-04-01 00:00:00'."
                ),
                parse_default=None,
            ),
            InputContract(
                name="end",
                kind="option",
                value_type="string",
                description=(
                    "Complement end datetime, for example '2026-04-10 00:00:00'."
                ),
                parse_default=None,
            ),
            InputContract(
                name="date",
                kind="option",
                value_type="string",
                description=(
                    "Explicit complement schedule datetime. Repeat for multiple "
                    "dates instead of using --start/--end. Available on DS 3.1.0 "
                    "and newer."
                ),
                parse_default=None,
                examples=("2026-04-01 00:00:00",),
                parameter_name="dates",
                multiple=True,
            ),
            _WORKFLOW_TASK_OPTION,
            InputContract(
                name="scope",
                kind="option",
                value_type="string",
                description=(
                    "Task execution scope when --task is set: self, pre, or post."
                ),
                parse_default="self",
                choices=("self", "pre", "post"),
            ),
            InputContract(
                name="run-mode",
                kind="option",
                value_type="string",
                description="Complement run mode: serial or parallel.",
                parse_default=None,
                fixed_default="serial",
                choices=("serial", "parallel"),
            ),
            InputContract(
                name="expected-parallelism-number",
                kind="option",
                value_type="integer",
                description=(
                    "Expected parallelism number when --run-mode parallel is used."
                ),
                parse_default=None,
                fixed_default=2,
            ),
            InputContract(
                name="complement-dependent-mode",
                kind="option",
                value_type="string",
                description="Complement dependent mode: off or all.",
                parse_default=None,
                fixed_default="off",
                choices=("off", "all"),
            ),
            InputContract(
                name="all-level-dependent",
                kind="option",
                value_type="boolean",
                description=(
                    "Enable all-level dependent complement when dependent mode is all."
                ),
                parse_default=False,
            ),
            InputContract(
                name="execution-order",
                kind="option",
                value_type="string",
                description="Complement execution order: desc or asc.",
                parse_default=None,
                fixed_default="desc",
                choices=("desc", "asc"),
            ),
            _START_WORKER_GROUP,
            _START_TENANT,
            _START_FAILURE_STRATEGY,
            _START_PRIORITY,
            _START_WARNING_TYPE,
            _START_WARNING_GROUP_ID,
            _START_ENVIRONMENT_CODE,
            _START_PARAMETERS,
            InputContract(
                name="dry-run",
                kind="option",
                value_type="boolean",
                description=(
                    "Resolve and compile the backfill request without sending it."
                ),
                parse_default=False,
            ),
            _START_EXECUTION_DRY_RUN,
        ),
    ),
    CommandContract(
        action="workflow.delete",
        effects=REMOTE_WRITE,
        route=("workflow", "delete"),
        summary="Delete one workflow definition.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(
            _PROJECT_CONTEXT_OPTION,
            InputContract(
                name="force",
                kind="option",
                value_type="boolean",
                description=(
                    "Required to delete workflow; omission fails without prompting."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="workflow.lineage.list",
        effects=REMOTE_READ,
        route=("workflow", "lineage", "list"),
        summary="Return the project-wide workflow lineage graph.",
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.lineage.get",
        effects=REMOTE_READ,
        route=("workflow", "lineage", "get"),
        summary="Return the lineage graph anchored on one workflow.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow.lineage.dependent-tasks",
        effects=REMOTE_READ,
        route=("workflow", "lineage", "dependent-tasks"),
        summary="Return workflows/tasks that depend on one workflow or task.",
        arguments=(_WORKFLOW_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION, _WORKFLOW_TASK_OPTION),
    ),
    CommandContract(
        action="workflow-instance.list",
        effects=REMOTE_READ,
        route=("workflow-instance", "list"),
        summary="List workflow instances using explicit runtime filters.",
        options=(
            _RUNTIME_PAGE_NO,
            _RUNTIME_PAGE_SIZE,
            _ALL_PAGES,
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            InputContract(
                name="workflow",
                kind="option",
                value_type="string",
                description=(
                    "Workflow name or code filter resolved inside the selected "
                    "project. Run `dsctl workflow list` to discover values."
                ),
                parse_default=None,
                selector="name_or_code",
                discovery_command="dsctl workflow list",
            ),
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description="Filter workflow instances by upstream searchVal.",
                parse_default=None,
            ),
            _RUNTIME_EXECUTOR_FILTER,
            InputContract(
                name="host",
                kind="option",
                value_type="string",
                description="Filter by workflow instance host.",
                parse_default=None,
            ),
            InputContract(
                name="start",
                kind="option",
                value_type="string",
                description=(
                    "Filter by start time lower bound, e.g. '2026-04-11 10:00:00'."
                ),
                parse_default=None,
            ),
            InputContract(
                name="end",
                kind="option",
                value_type="string",
                description=(
                    "Filter by start time upper bound, e.g. '2026-04-11 11:00:00'."
                ),
                parse_default=None,
            ),
            InputContract(
                name="state",
                kind="option",
                value_type="string",
                description=(
                    "Filter by DS workflow execution status name. Run `dsctl enum "
                    "list workflow-execution-status` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl enum list workflow-execution-status",
            ),
            InputContract(
                name="trigger-code",
                kind="option",
                value_type="integer",
                minimum=1,
                description=(
                    "Resolve an accepted trigger receipt to instances (DS 3.2.0+). "
                    "Use without workflow, time, state or pagination filters."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="workflow-instance.get",
        effects=REMOTE_READ,
        route=("workflow-instance", "get"),
        summary="Get one workflow instance by id.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.export",
        effects=REMOTE_READ,
        route=("workflow-instance", "export"),
        raw_output_format="yaml",
        summary="Export one workflow instance DAG as an editable YAML document.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.parent",
        effects=REMOTE_READ,
        route=("workflow-instance", "parent"),
        summary="Return the parent workflow instance for one sub-workflow instance.",
        arguments=(
            InputContract(
                name="sub_workflow_instance",
                kind="argument",
                value_type="integer",
                description=(
                    "Sub-workflow instance id. Resolve the child of a SUB_WORKFLOW "
                    "task with `dsctl task-instance sub-workflow`."
                ),
                required=True,
                selector="id",
                discovery_command_pattern=(
                    "dsctl task-instance sub-workflow TASK_INSTANCE --project PROJECT"
                ),
            ),
        ),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.digest",
        effects=REMOTE_READ,
        route=("workflow-instance", "digest"),
        summary="Return one compact workflow-instance runtime digest.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.edit",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("workflow-instance", "edit"),
        summary=(
            "Edit one finished workflow instance from a YAML patch or full YAML file."
        ),
        arguments=(
            InputContract(
                name="workflow_instance",
                kind="argument",
                value_type="integer",
                description=(
                    "Finished workflow instance id. Discover ids with `dsctl "
                    "workflow-instance list` scoped to the same project."
                ),
                required=True,
                selector="id",
                discovery_command_pattern=(
                    "dsctl workflow-instance list --project PROJECT"
                ),
            ),
        ),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            InputContract(
                name="patch",
                kind="option",
                value_type="path",
                description=(
                    "Path to one workflow-instance patch YAML file. Use exactly one"
                    " of --patch or --file. Inspect the current instance DAG with "
                    "`dsctl workflow-instance export` using the same instance id "
                    "and project, then write only the intended delta. Start from "
                    "`dsctl template workflow-instance-patch --raw`; "
                    "`tasks.create[]` uses full task fragments from `dsctl template"
                    " task`; `tasks.update[].set` uses partial task fields "
                    "discovered with `dsctl task-type schema TYPE`."
                ),
                parse_default=None,
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="file",
                kind="option",
                value_type="path",
                description=(
                    "Path to one full workflow-instance YAML file describing the "
                    "desired repaired DAG state. Use exactly one of --patch or "
                    "--file. Start from `dsctl workflow-instance export` using the "
                    "same instance id and project; use --dry-run to inspect the "
                    "compiled diff. Full-file edits match task identity by exact "
                    "task name and do not infer renames."
                ),
                parse_default=None,
                path_rules=PathRules(
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                    readable=True,
                    resolve_path=True,
                ),
            ),
            InputContract(
                name="sync-definition",
                kind="option",
                value_type="boolean",
                description=(
                    "Also write the edited DAG back to the current workflow "
                    "definition. "
                    "Required for task or edge changes on DS 2.0.0-2.0.2; those "
                    "versions require an already ONLINE definition because native "
                    "metadata synchronization may set it ONLINE."
                ),
                parse_default=False,
                flags=("--sync-definition/--no-sync-definition",),
            ),
            InputContract(
                name="dry-run",
                kind="option",
                value_type="boolean",
                description=(
                    "Compile the merged workflow-instance edit payload without "
                    "sending it."
                ),
                parse_default=False,
            ),
            InputContract(
                name="confirm-risk",
                kind="option",
                value_type="string",
                description=(
                    "Explicit confirmation token returned by a previous high-risk "
                    "full-file instance edit validation failure."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="workflow-instance.watch",
        effects=REMOTE_READ,
        route=("workflow-instance", "watch"),
        summary="Poll one workflow instance until it reaches a final state.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            _WATCH_INTERVAL_SECONDS,
            _WATCH_TIMEOUT_SECONDS,
            _WATCH_EXIT_STATUS,
            InputContract(
                name="after-run-times",
                kind="option",
                value_type="integer",
                minimum=0,
                description=(
                    "Wait for a run newer than this recovery receipt baseline "
                    "before accepting a final state."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="workflow-instance.stop",
        effects=REMOTE_WRITE,
        route=("workflow-instance", "stop"),
        summary="Request stop for one workflow instance.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.rerun",
        effects=REMOTE_WRITE,
        route=("workflow-instance", "rerun"),
        summary="Request rerun for one finished workflow instance.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.recover-failed",
        effects=REMOTE_WRITE,
        route=("workflow-instance", "recover-failed"),
        summary="Recover one failed workflow instance from failed tasks.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION,),
    ),
    CommandContract(
        action="workflow-instance.execute-task",
        effects=REMOTE_WRITE,
        route=("workflow-instance", "execute-task"),
        summary="Execute one task inside one finished workflow instance.",
        arguments=(_WORKFLOW_INSTANCE_ARGUMENT,),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            InputContract(
                name="task",
                kind="option",
                value_type="string",
                description=(
                    "Task name or task code within the workflow instance. Discover "
                    "values with `dsctl task-instance list` scoped to the same "
                    "project and workflow instance."
                ),
                required=True,
                selector="name_or_code",
                discovery_command_pattern=(
                    "dsctl task-instance list --project PROJECT --workflow-instance"
                    " WORKFLOW_INSTANCE"
                ),
            ),
            _TASK_EXECUTION_SCOPE,
        ),
    ),
    CommandContract(
        action="task.list",
        effects=REMOTE_READ,
        route=("task", "list"),
        summary="List tasks inside one workflow.",
        options=(
            _PROJECT_CONTEXT_OPTION,
            _WORKFLOW_OPTION,
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Filter tasks by name substring after fetching the workflow "
                    "task list."
                ),
                parse_default=None,
            ),
        ),
    ),
    CommandContract(
        action="task.get",
        effects=REMOTE_READ,
        route=("task", "get"),
        summary="Get one task definition by name or code.",
        arguments=(_TASK_ARGUMENT,),
        options=(_PROJECT_CONTEXT_OPTION, _WORKFLOW_OPTION),
    ),
    CommandContract(
        action="task.update",
        effects=REMOTE_WRITE_DRY_RUN_READ,
        route=("task", "update"),
        summary=(
            "Update one task and its incoming dependencies. Use workflow edit "
            "for changes spanning multiple tasks; workflow-instance edit repairs a run."
        ),
        arguments=(_TASK_ARGUMENT,),
        options=(
            _PROJECT_CONTEXT_OPTION,
            _WORKFLOW_OPTION,
            InputContract(
                name="set",
                kind="option",
                value_type="string",
                description=(
                    "Inline KEY=VALUE update for this single task. Repeat as "
                    "needed. Common keys: command, retry.times, timeout, "
                    "depends_on (comma-separated names or a YAML list). "
                    "Quote the whole KEY=VALUE argument. "
                    "Run `dsctl schema --command task.update` for all "
                    "supported keys."
                ),
                discovery_command="dsctl schema --command task.update",
                examples=(
                    "command=python v2.py",
                    "retry.times=5",
                    "task_group_id=12",
                    "timeout_notify_strategy=FAILED",
                ),
                supported_keys=(
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
                ),
                parameter_name="set_values",
                multiple=True,
                required=True,
            ),
            InputContract(
                name="dry-run",
                kind="option",
                value_type="boolean",
                description=(
                    "Compile the native task update request without sending it."
                ),
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="task-instance.list",
        effects=REMOTE_READ,
        route=("task-instance", "list"),
        summary="List task instances with project-scoped runtime filters.",
        options=(
            InputContract(
                name="workflow-instance",
                kind="option",
                value_type="integer",
                description=(
                    "Workflow instance id used to narrow the project-scoped "
                    "task-instance query. Discover ids with `dsctl "
                    "workflow-instance list` scoped to that project."
                ),
                parse_default=None,
                discovery_command_pattern=(
                    "dsctl workflow-instance list --project PROJECT"
                ),
            ),
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            InputContract(
                name="workflow",
                kind="option",
                value_type="string",
                description=(
                    "Reserved compatibility option. DS 3.4.1 task-instance list "
                    "does not reliably filter by workflow definition."
                ),
                parse_default=None,
                hidden=True,
            ),
            InputContract(
                name="workflow-instance-name",
                kind="option",
                value_type="string",
                description="Filter by workflow instance name.",
                parse_default=None,
            ),
            _RUNTIME_PAGE_NO,
            _RUNTIME_PAGE_SIZE,
            _ALL_PAGES,
            InputContract(
                name="search",
                kind="option",
                value_type="string",
                description=(
                    "Free-text upstream searchVal filter. Use --task for an exact "
                    "task instance name filter."
                ),
                parse_default=None,
            ),
            InputContract(
                name="task",
                kind="option",
                value_type="string",
                description="Filter by exact task instance name.",
                parse_default=None,
            ),
            InputContract(
                name="task-code",
                kind="option",
                value_type="integer",
                description=(
                    "Filter by task definition code. Run "
                    "`dsctl task list --project PROJECT --workflow WORKFLOW` to "
                    "discover values."
                ),
                parse_default=None,
                discovery_command_pattern=(
                    "dsctl task list --project PROJECT --workflow WORKFLOW"
                ),
            ),
            _RUNTIME_EXECUTOR_FILTER,
            InputContract(
                name="state",
                kind="option",
                value_type="string",
                description=(
                    "Filter by DS task execution status name. Run `dsctl enum list "
                    "task-execution-status` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl enum list task-execution-status",
            ),
            InputContract(
                name="host",
                kind="option",
                value_type="string",
                description="Filter by worker host.",
                parse_default=None,
            ),
            InputContract(
                name="start",
                kind="option",
                value_type="string",
                description="Task start-time lower bound, e.g. '2026-04-11 10:00:00'.",
                parse_default=None,
            ),
            InputContract(
                name="end",
                kind="option",
                value_type="string",
                description="Task start-time upper bound, e.g. '2026-04-11 11:00:00'.",
                parse_default=None,
            ),
            InputContract(
                name="execute-type",
                kind="option",
                value_type="string",
                description=(
                    "Filter by DS task execute type: BATCH or STREAM. Run `dsctl "
                    "enum list task-execute-type` to discover values."
                ),
                parse_default=None,
                discovery_command="dsctl enum list task-execute-type",
                choices=("BATCH", "STREAM"),
            ),
        ),
    ),
    CommandContract(
        action="task-instance.get",
        effects=REMOTE_READ,
        route=("task-instance", "get"),
        summary="Get one task instance by id within a project.",
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            _OPTIONAL_TASK_WORKFLOW_INSTANCE_OPTION,
        ),
    ),
    CommandContract(
        action="task-instance.watch",
        effects=REMOTE_READ,
        route=("task-instance", "watch"),
        summary="Poll one task instance until it reaches a finished state.",
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            _OPTIONAL_TASK_WORKFLOW_INSTANCE_OPTION,
            _WATCH_INTERVAL_SECONDS,
            _WATCH_TIMEOUT_SECONDS,
            _WATCH_EXIT_STATUS,
        ),
    ),
    CommandContract(
        action="task-instance.sub-workflow",
        effects=REMOTE_READ,
        route=("task-instance", "sub-workflow"),
        summary=(
            "Return the child workflow instance for one SUB_WORKFLOW task instance."
        ),
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION, _TASK_WORKFLOW_INSTANCE_OPTION),
    ),
    CommandContract(
        action="task-instance.log",
        effects=REMOTE_READ,
        route=("task-instance", "log"),
        summary="Read a task-instance log tail or a located line window.",
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(
            InputContract(
                name="tail",
                minimum=1,
                parser_minimum=None,
                kind="option",
                value_type="integer",
                description=(
                    "Return the last N log lines (default: 200). "
                    "Cannot combine with --start-line or --limit."
                ),
                parse_default=None,
            ),
            InputContract(
                name="start-line",
                kind="option",
                value_type="integer",
                minimum=1,
                description="Read a window starting at this 1-based line (default: 1).",
                parse_default=None,
            ),
            InputContract(
                name="limit",
                kind="option",
                value_type="integer",
                minimum=1,
                description="Maximum lines in a located window (default: 200).",
                parse_default=None,
            ),
            InputContract(
                name="raw",
                kind="option",
                value_type="boolean",
                description="Print only the log text, without the JSON envelope.",
                parse_default=False,
            ),
        ),
    ),
    CommandContract(
        action="task-instance.force-success",
        effects=REMOTE_WRITE,
        route=("task-instance", "force-success"),
        summary="Force one failed task instance into FORCED_SUCCESS.",
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(_RUNTIME_PROJECT_CONTEXT_OPTION, _TASK_WORKFLOW_INSTANCE_OPTION),
    ),
    CommandContract(
        action="task-instance.savepoint",
        effects=REMOTE_WRITE,
        route=("task-instance", "savepoint"),
        summary="Request one savepoint for a running task instance.",
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            _OPTIONAL_TASK_WORKFLOW_INSTANCE_OPTION,
        ),
    ),
    CommandContract(
        action="task-instance.stop",
        effects=REMOTE_WRITE,
        route=("task-instance", "stop"),
        summary="Request stop for one task instance.",
        arguments=(_TASK_INSTANCE_ARGUMENT,),
        options=(
            _RUNTIME_PROJECT_CONTEXT_OPTION,
            _OPTIONAL_TASK_WORKFLOW_INSTANCE_OPTION,
        ),
    ),
)
