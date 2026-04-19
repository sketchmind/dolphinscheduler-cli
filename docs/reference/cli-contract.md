# CLI Contract

## Status

This document describes the current stable CLI surface. If a command or field
is not described here, treat it as roadmap work rather than contract.

Current stable commands:

- global option `--env-file PATH`
- global option `--output-format {json,table,tsv}`
- global option `--columns CSV`
- `dsctl version`
- `dsctl context`
- `dsctl doctor`
- `dsctl schema`
- `dsctl capabilities`
- `dsctl enum names`
- `dsctl enum list ENUM`
- `dsctl lint workflow FILE`
- `dsctl task-type list|get|schema`
- `dsctl environment list|get|create|update|delete`
- `dsctl cluster list|get|create|update|delete`
- `dsctl datasource list|get|create|update|delete|test`
- `dsctl namespace list|get|available|create|delete`
- `dsctl resource list|view|upload|create|mkdir|download|delete`
- `dsctl queue list|get|create|update|delete`
- `dsctl worker-group list|get|create|update|delete`
- `dsctl task-group list|get|create|update|close|start`
- `dsctl task-group queue list|force-start|set-priority`
- `dsctl alert-plugin list|get|schema|create|update|delete|test`
- `dsctl alert-plugin definition list`
- `dsctl alert-group list|get|create|update|delete`
- `dsctl tenant list|get|create|update|delete`
- `dsctl user list|get|create|update|delete`
- `dsctl user grant project|datasource|namespace`
- `dsctl user revoke project|datasource|namespace`
- `dsctl access-token list|get|create|update|delete|generate`
- `dsctl monitor health|server|database`
- `dsctl audit list|model-types|operation-types`
- `dsctl use [--clear]`
- `dsctl use project NAME`
- `dsctl use workflow NAME`
- `dsctl project list|get|create|update|delete`
- `dsctl project-parameter list|get|create|update|delete`
- `dsctl project-preference get|update|enable|disable`
- `dsctl project-worker-group list|set|clear`
- `dsctl schedule list|get|preview|explain|create|update|delete|online|offline`
- `dsctl template workflow|workflow-patch|workflow-instance-patch|params|environment|cluster|datasource|task`
- `dsctl workflow list|get|describe|digest|create|edit|online|offline|run|run-task|backfill|delete`
- `dsctl workflow lineage list|get|dependent-tasks`
- `dsctl workflow-instance list|get|parent|digest|edit|watch|stop|rerun|recover-failed|execute-task`
- `dsctl task list|get|update`
- `dsctl task-instance list|get|watch|sub-workflow|log|force-success|savepoint|stop`

## Naming and Selection Rules

Names are opaque strings.

Rules:

- do not infer structure from delimiters inside names
- positional selectors mean raw names unless the command explicitly documents a
  numeric-code shortcut or a DS-native path selector
- `resource` is path-first and consumes the DS `fullName` path directly
- DS codes and ids remain the underlying stable server-side identity
- effective selection precedence is `flag > context`

Current examples:

```bash
dsctl project get etl-prod
dsctl project-parameter get warehouse_db --project etl-prod
dsctl environment get prod
dsctl cluster get k8s-prod
dsctl datasource get warehouse
dsctl lint workflow workflow.yaml
dsctl queue get default
dsctl alert-plugin get slack-ops
dsctl access-token get 12
dsctl audit list --model-type Workflow --operation-type Create
dsctl resource view /tenant/resources/demo.sql
dsctl workflow get daily-etl --project etl-prod
dsctl task get extract --project etl-prod --workflow daily-etl
dsctl workflow-instance get <workflow_instance_id>
dsctl task-instance get <task_instance_id> --workflow-instance <workflow_instance_id>
```

## Global Option

### `--env-file PATH`

Loads `DS_*` settings from a dotenv-style file, then lets process environment
variables override those values.

Only DS connection and version settings are accepted in profile config.
Resource selection and runtime-default keys are rejected. Use `dsctl use
project` for local project selection and `dsctl project-preference update` for
project-level runtime defaults instead of profile config.

Supported keys:

- `DS_VERSION`
- `DS_API_URL`
- `DS_API_TOKEN`
- `DS_API_RETRY_ATTEMPTS`
- `DS_API_RETRY_BACKOFF_MS`

Example:

```bash
dsctl --env-file cluster.env context
```

### `--output-format {json,table,tsv}`

Controls display rendering. The default is `json`.

Rules:

- `json` returns the standard JSON envelope and remains the stable machine
  contract; when `--columns` is present, only the command data payload at the
  canonical row/object path is projected
- `table` renders row/object-oriented data as a plain text table for terminal
  scanning
- `tsv` renders the same row model as tab-separated text for shell pipelines
- row-oriented formats use each command's `data_shape` metadata when present
  and fall back to runtime shape inference for simple list payloads
- global options are passed before the command group, for example:

```bash
dsctl --output-format table workflow-instance list --project etl-prod
```

### `--columns CSV`

Selects top-level row/object fields. For `json`, this narrows the standard
envelope data payload at the command's canonical row/object path. For `table`
and `tsv`, it selects rendered display columns.

Rules:

- comma-separated values keep the requested order
- only top-level row/object fields are selected
- `--columns '*'` selects all top-level row fields; quote `*` so the shell does
  not expand it as a filesystem glob
- unknown columns are a `user_input_error` when rows are available to validate
  against
- errors are never projected; failed commands keep the full structured error
  payload

Example:

```bash
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --output-format tsv --columns id,name,state,host task-instance list --workflow-instance 901
dsctl --output-format tsv --columns '*' task-instance list --workflow-instance 901
```

## Output Envelope

Every stable command returns the standard JSON envelope from `src/dsctl/output.py`.
This statement applies to the default `--output-format json` mode. Explicit
`--columns` projection keeps the envelope and narrows only the command `data`
payload. Row-oriented display formats are an alternate rendering layer over the
same command result.

Success shape:

```json
{
  "ok": true,
  "action": "version",
  "resolved": {},
  "data": {},
  "warnings": [],
  "warning_details": []
}
```

Error shape:

```json
{
  "ok": false,
  "action": "project.get",
  "resolved": {},
  "data": {},
  "warnings": [],
  "warning_details": [],
  "error": {
    "type": "not_found",
    "message": "Project 'etl-prod' was not found",
    "details": {
      "resource": "project",
      "name": "etl-prod"
    },
    "suggestion": "Retry with `project list` to inspect available values, or pass the numeric code if known."
  }
}
```

Error guarantees:

- `error.type` is machine-stable
- `error.details` is present when the command can expose structured context
- `error.source` is present when the CLI can preserve machine-readable origin
  facts from an underlying remote failure
- `error.suggestion` is present when the CLI can provide one concrete next step
  without guessing

When present, `error.source` currently uses this shape for remote DS failures:

```json
{
  "kind": "remote",
  "system": "dolphinscheduler",
  "layer": "result",
  "result_code": 30001,
  "result_message": "user has no operation privilege"
}
```

Field rules:

- `kind` is the broad origin class and is currently `remote`
- `system` identifies the remote system and is currently `dolphinscheduler`
- `layer` distinguishes remote failure layers such as `result` and `http`
- `result_code` and `result_message` are present for DS result-envelope failures
- `status_code` is present for HTTP-layer failures

### Field Naming Policy

- DS objects projected into `data` keep DS-native field names.
- CLI envelope fields such as `ok`, `action`, `resolved`, `warnings`, and
  selection metadata use CLI-owned naming.
- high-risk mutations may return `confirmation_required` and expect the same
  command to be retried with `--confirm-risk TOKEN`
- `warning_details` is a machine-readable list aligned positionally with
  `warnings`
- all stable warnings emitted by the CLI include aligned `warning_details`
- every dry-run result includes one standard warning detail with code
  `dry_run_no_request_sent`
- `resolved` records facts selected or adopted by this command invocation, such
  as resolved resource identities, normalized selectors, applied filters, or
  the active output view
- `resolved.view`, when present, is reserved for commands whose single stable
  action can return more than one `data` shape; it is not required for commands
  whose `action` already uniquely identifies the output shape
- discovery candidates and allowed values do not belong in `resolved`; expose
  them through command `data`, schema `choices`, `discovery_command`, enum
  commands, capabilities, or structured error `details`
- command schema entries may include `data_shape` metadata with a stable
  low-entropy row/object model for renderers, JSON projection, and AI agents

Current `data_shape` fields:

- `kind`: one of `page`, `collection`, `object`, or `summary`
- `row_path`: dot-path from the standard JSON envelope to the canonical row
  collection or object, such as `data.totalList` or `data`
- `default_columns`: suggested compact display columns
- `column_discovery`: currently `runtime_row_keys`, meaning full column
  discovery comes from the JSON row payload

## `dsctl version`

Returns CLI and selected DolphinScheduler version metadata. The selected
version comes from `DS_VERSION` when set, otherwise the default supported DS
version is used.

Example:

```json
{
  "ok": true,
  "action": "version",
  "resolved": {},
  "data": {
    "cli": "0.1.0",
    "ds": "3.4.1",
    "selected_ds_version": "3.4.1",
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "supported_ds_versions": ["3.3.2", "3.4.0", "3.4.1"]
  },
  "warnings": []
}
```

## `dsctl context`

Returns the effective config profile plus stored session context.

Current `data` fields:

- `api_url`
- `ds_version`
- `project`
- `workflow`

## `dsctl doctor`

Returns structured local and remote diagnostics for the current runtime.

Current `data` fields:

- `status`
- `summary`
- `checks`

Current guarantees:

- always returns one aggregated diagnostic payload instead of failing on the
  first broken dependency
- checks local profile loading, merged context loading, adapter resolution, API
  actuator health, and current-user runtime defaults
- preserves structured error details under `checks[].details.error`
- exposes `checks[].suggestion` for every check; successful checks use `null`
- emits top-level warnings for any check whose status is not `ok`
- when such a warning is present, the aligned `warning_details[]` item uses
  code `doctor_check_not_ok` and includes `check`, `status`, `message`, and
  `suggestion`

## `dsctl schema`

Returns the stable machine-readable command schema for the current CLI surface.
This is the authoritative self-description for command invocation: arguments,
options, choices, selectors, defaults, and supported composite keys.

Options:

- `--group GROUP`
- `--command ACTION`
- `--list-groups`
- `--list-commands`

Selection rules:

- omit all scope options to return the full schema, including `capabilities`
- `--group` returns one command-group schema by stable group name such as
  `task-instance`
- `--group` values come from `dsctl schema --list-groups`
- `--command` returns one command schema by stable action such as
  `task-instance.list` or `version`
- `--command` values come from `dsctl schema --list-commands`
- `--list-groups` returns compact rows with `name`, `summary`,
  `command_count`, and `schema_command`
- `--list-commands` returns compact rows with `action`, `group`, `name`,
  `summary`, and `schema_command`
- `--list-commands` uses `group: null` for root-level commands such as
  `version`
- `--group`, `--command`, `--list-groups`, and `--list-commands` are mutually
  exclusive
- scoped schema payloads keep the standard schema header and `commands` tree
  shape but omit `capabilities`; use `dsctl capabilities` for feature
  discovery
- scoped `--group` and `--command` payloads also include `rows` for compact
  table/tsv rendering; JSON callers that need the full contract should continue
  reading `commands`
- `--group` rows list commands in the group with `kind`, `action`, `name`,
  `summary`, and `schema_command`
- `--command` rows flatten the command contract into `command`, `argument`,
  `option`, `payload`, and `data_shape` rows so terminal output does not
  collapse nested contract data into one large value cell
- scoped schema `resolved.schema.view` is `group`, `command`, `groups`, or
  `commands`

Current `data` fields:

- `schema_version`
- `cli`
- `supported_ds_versions`
- `ds_versions`
- `global_options`
- `selection`
- `output`
- `errors`
- `confirmation`
- `capabilities`
- `commands`
- `rows` for scoped `--group` and `--command` views

Current guarantees:

- describes only the current stable surface
- uses `DS_VERSION` and `--env-file` when rendering embedded capability
  metadata, matching `dsctl capabilities`
- includes selector semantics for name-first, path-first, and id-first resources
- includes the standard success/error envelope contract
- includes the stable structured error envelope and `error.source` contract
- command arguments and options may include additive metadata such as
  `choices`, `examples`, `supported_keys`, and `discovery_command` when the
  CLI can expose a tighter contract for composite inputs
- command entries that accept file payloads may include compact `payload`
  metadata; when present, `payload.template_command` is the preferred
  progressive-discovery command for a concrete payload template
- includes task template type and variant discovery under
  `capabilities.templates.task`
- `--group`, `--command`, `--list-groups`, and `--list-commands` are additive
  scoped or discovery views over the same command tree, not a different schema
  mode
- `schema_version` changes for breaking schema changes; additive fields may
  appear within the same version
- is tested against the actual registered command tree
- command entries that expose row/object-oriented output include `data_shape`;
  this is the authoritative model for `--columns` and
  `--output-format table|tsv`
- schema and capabilities output metadata expose `json_column_projection` when
  JSON `--columns` projection is supported

## `dsctl capabilities`

Returns stable version and surface capability discovery for the current CLI and
selected DS version.
This payload is intentionally lighter than `dsctl schema`: it answers what
resource families and feature groups exist, not how to invoke every command.
Agents that need to construct commands should read `dsctl schema`.

Options:

- `--summary`
- `--section SECTION`

Selection rules:

- omit both options to return full capability discovery
- `--summary` returns lightweight capability discovery with `cli`, `ds`,
  `self_description`, `resources`, `planes`, `runtime`, `schedule`, `monitor`,
  `enums`, and a summarized `authoring` section
- `--section` returns one top-level section plus the standard `cli`, `ds`, and
  `self_description` header
- valid sections are `selection`, `output`, `errors`, `resources`, `planes`,
  `authoring`, `schedule`, `monitor`, `enums`, and `runtime`
- `--summary` and `--section` are mutually exclusive

Current `data` fields:

- `cli`
- `ds`
- `selection`
- `output`
- `errors`
- `self_description`
- `enums`
- `resources`
- `planes`
- `authoring`
- `schedule`
- `monitor`
- `runtime`

Current guarantees:

- `data.ds.selected_version` is the normalized target DS version
- `data.ds.contract_version` is the generated contract package version used by
  the selected adapter
- `data.ds.family` groups compatible server versions that share adapter
  semantics
- `data.ds.support_level` is one of `full`, `legacy_core`, or `experimental`

- summarizes only the current stable surface
- exposes name-first, path-first, and id-first selection rules
- exposes standard output-envelope support
- exposes structured error and `error.source` support
- exposes generated enum discovery names
- distinguishes CLI task-template coverage and variants from DS upstream
  default task types
- exposes untemplated upstream task types for authoring gap analysis
- keeps live runtime task-type discovery out of the static capability payload;
  use `dsctl task-type list` for cluster/user-visible DS task types
- does not describe command arguments or options; use `dsctl schema` for that
- exposes `data.self_description.command_invocation_source="schema"` and
  `data.self_description.capabilities_scope="feature_discovery"` so tools can
  distinguish feature discovery from command invocation metadata
- `--summary` and `--section` are additive scoped views over the same feature
  discovery data, not output-format modes
- is intended as the lightweight companion to `dsctl schema`

## `dsctl use`

Persists CLI context in the selected scope.

Supported forms:

- `dsctl use project NAME`
- `dsctl use workflow NAME`
- `dsctl use --clear`
- `dsctl use project --clear`
- `dsctl use workflow --clear`

Rules:

- `--scope` accepts `project` or `user`
- setting `project` clears any stored `workflow` beneath it
- clearing `project` also clears `workflow`

Successful output returns the merged effective context in:

- `data.project`
- `data.workflow`
- `data.set_at`

## `dsctl lint workflow FILE`

Runs local design-time checks for one workflow YAML file without contacting
DolphinScheduler.

Resolved fields:

- `kind`
- `file`

Rules:

- this command is local-only and does not require DS connectivity
- it validates the stable workflow YAML model
- it runs the same local compile path used by `workflow create`
- it warns when `workflow.project` is omitted because workflow selection then
  depends on `--project` or stored project context
- it warns on risky `$[...]` dynamic parameter time formats; uppercase
  `YYYY` emits `parameter_time_format_week_year_token`, and calendar-year plus
  week-number patterns such as `$[yyyyww]` emit
  `parameter_time_format_calendar_year_with_week`
- it rejects schedule blocks on offline workflows because DS only allows
  schedule creation for online workflows
- successful lint output includes `data.diagnostics[]`; pass diagnostics mirror
  completed local stages, and warning diagnostics mirror top-level
  `warning_details[]`
- blocking model or compile failures still use the standard error envelope and
  exit non-zero

Current `data` fields:

- `kind`
- `valid`
- `summary`
- `compilation`
- `checks`
- `diagnostics`

## `dsctl enum names`

Returns compact discovery rows for generated enum names supported by the
current selected DS contract.

Rules:

- this command is local-only and does not require DS connectivity
- each row includes the stable enum discovery name and the corresponding
  `dsctl enum list` command
- schema entries that require an enum discovery name should point to this
  command with `discovery_command`

Successful output returns a list of rows with:

- `name`
- `list_command`

## `dsctl enum list ENUM`

Returns one generated enum and its members for the current supported DS version.

Rules:

- `ENUM` uses the stable enum discovery names exposed by `dsctl enum names`
- class-name aliases such as `ReleaseState` are also accepted
- enum member metadata is projected from generated enum attributes and kept
  under `members[].attributes`

Successful output returns:

- `data.name`
- `data.module`
- `data.class_name`
- `data.ds_version`
- `data.value_type`
- `data.member_count`
- `data.members`

## `dsctl task-type list`

Returns the live DS task-type catalog for the configured cluster and current
user.

Rules:

- this is a live DS API call backed by `GET /favourite/taskTypes`
- the returned list is the DS default task type universe plus the current
  user's `isCollection` favourite flag
- unlike `capabilities`, this payload depends on the configured cluster and
  authenticated user
- unlike `template task`, this is not the local YAML template catalog
- `resolved.source` is always `favourite/taskTypes`

`data.taskTypes` projects the DS `FavTaskDto` records and keeps DS-native field
names:

- `taskType`
- `isCollection`
- `taskCategory`

The full stable payload also includes:

- `data.count`
- `data.taskTypesByCategory`
- `data.cliCoverage.taskTemplateTypes`
- `data.cliCoverage.typedTaskSpecs`
- `data.cliCoverage.genericTaskTemplateTypes`
- `data.cliCoverage.untemplatedTaskTypes`

## `dsctl task-type get TASK_TYPE`

Returns the local task authoring summary for one DS task type. This command is
local and does not call DolphinScheduler.

Rules:

- task type matching is case-insensitive and accepts supported local aliases
- `resolved.task_type` is the normalized DS-native task type
- `data.template_command` points to the default YAML fragment
- `data.raw_template_command` points to the copyable raw YAML fragment
- `data.schema_command` points to the full authoring contract
- `data.required_paths[]` lists fields required by the local authoring model
- `data.choice_sources[]` lists commands or local sources for discoverable
  values
- `data.rows[]` is the compact table/tsv view of next commands and variants
- generic task types emit warning code `generic_task_template`

## `dsctl task-type schema TASK_TYPE`

Returns the full local authoring contract for one DS task type. This command is
local and does not call DolphinScheduler.

Rules:

- `data.schema` is a JSON-Schema-style authoring contract with `x-dsctl`
  metadata
- `data.fields[]` is the canonical row model for table/tsv and `--columns`
- `data.fields[].choice_source` records the command or local source for
  discoverable values; `data.fields[].related_commands[]` records adjacent
  inspection or creation commands when useful
- `data.state_rules[]` describes task-type conditionals such as SQL
  `sqlType`
- `data.choice_sources[]` records how to discover valid external values
  and which returned field to use, such as resource `fullName`, datasource
  `id`, workflow `code`, or a task name in the same YAML file
- `data.compile_mappings[]` records how authoring YAML maps to DS REST form
  payload fields
- the schema is for authoring workflow YAML, not for representing every raw DS
  database column

## `dsctl project list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

When `--all` is used, the CLI materializes all fetched items into one
page-shaped response. `resolved.all` indicates the response was
client-aggregated.

Use `dsctl project list` to discover project names and numeric codes.

## `dsctl project get PROJECT`

Accepts a project name or a numeric project code, resolves the stable project
identity, then fetches the current project payload.

Use `dsctl project list` to discover project names and numeric codes.

`resolved.project` includes:

- `code`
- `name`
- `description`

## `dsctl project create`

Creates a project and returns the created project payload.

## `dsctl project update PROJECT`

Updates one project resolved by name or code.

Rules:

- use `dsctl project list` to discover project names and numeric codes
- omitting `--name` preserves the current name
- omitting both `--description` and `--clear-description` preserves the
  current description
- `--clear-description` sets the description to `null`
- `--description` and `--clear-description` are mutually exclusive

## `dsctl project delete PROJECT --force`

Deletes one resolved project.

Rules:

- `PROJECT` may be a project name or numeric code
- use `dsctl project list` to discover project names and numeric codes
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.project`

## `dsctl project-parameter list`

Lists project parameters inside one selected project.

Options:

- `--project PROJECT`
- `--search TEXT`
- `--data-type TYPE`
- `--page-no N`
- `--page-size N`
- `--all`

Use `dsctl project list` to discover projects. Use
`dsctl enum list data-type` to discover project-parameter data-type values.

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Each item keeps DS-native project-parameter fields:

- `id`
- `userId`
- `operator`
- `code`
- `projectCode`
- `paramName`
- `paramValue`
- `paramDataType`
- `createTime`
- `updateTime`
- `createUser`
- `modifyUser`

## `dsctl project-parameter get PROJECT_PARAMETER`

Accepts a project-parameter name or numeric code inside one selected project.

Selection rules:

- `--project` falls back to context, then config
- `PROJECT_PARAMETER` is name-first within that project, with a numeric `code`
  shortcut
- use `dsctl project-parameter list` inside the selected project to discover
  parameter names and codes

`resolved.projectParameter` includes:

- `code`
- `paramName`
- `paramDataType`

## `dsctl project-parameter create`

Creates one project parameter in the selected project.

Rules:

- `--name` is required
- `--value` is required
- `--data-type` defaults to `VARCHAR`
- use `dsctl enum list data-type` to discover data-type values

## `dsctl project-parameter update PROJECT_PARAMETER`

Updates one project parameter resolved by name or code inside one selected
project.

Rules:

- requires at least one of `--name`, `--value`, or `--data-type`
- omitting `--name`, `--value`, or `--data-type` preserves the current remote
  value for that field
- use `dsctl project-parameter list` inside the selected project to discover
  parameter names and codes
- use `dsctl enum list data-type` to discover data-type values

## `dsctl project-parameter delete PROJECT_PARAMETER --force`

Deletes one resolved project parameter.

Rules:

- `--project` falls back to context, then config
- `PROJECT_PARAMETER` may be a project-parameter name or numeric code
- use `dsctl project-parameter list` inside the selected project to discover
  parameter names and codes
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.projectParameter`

## `dsctl project-preference get`

Fetches the singleton project preference default-value source for one selected
project.

Rules:

- `--project` follows the standard `flag > context` selection rule
- use `dsctl project list` to discover project names and numeric codes
- DS may return `data: null` when the selected project has no stored preference
- `state=1` means the stored preference is enabled as a project-level
  default-value source for CLI and UI surfaces that explicitly support it
- `state=0` means the stored preference stays stored but should not be applied
  automatically
- enabling or disabling project preference does not rewrite existing workflow
  definitions, task definitions, or schedules

When a preference exists, the payload keeps DS-native fields:

- `id`
- `code`
- `projectCode`
- `preferences`
- `userId`
- `state`
- `createTime`
- `updateTime`

## `dsctl project-preference update`

Creates or updates the singleton project-level default-value source for one
selected project.

Options:

- `--project PROJECT`
- exactly one of `--preferences-json TEXT` or `--file PATH`

Rules:

- use `dsctl project list` to discover project names and numeric codes
- the input must decode to one JSON object
- the CLI normalizes that object into a compact JSON string before sending it
  as DS `projectPreferences`
- the stored object is DS-native preference data; DS does not automatically
  backfill it into existing workflow/task definitions
- successful output returns the refreshed project-preference projection

## `dsctl project-preference enable`

Enables the singleton project preference as a project-level default-value
source for one selected project.

Rules:

- `--project` follows the standard `flag > context` selection rule
- use `dsctl project list` to discover project names and numeric codes
- DS uses integer state `1` for enabled
- enabling project preference does not mutate existing workflow/task/schedule
  rows; it only affects clients that choose to consume it as a default source
- if DS accepts the request but no project-preference row exists, `data` stays
  `null` and the CLI emits one warning with aligned `warning_details` code
  `project_preference_missing`

## `dsctl project-preference disable`

Disables the singleton project preference as a project-level default-value
source for one selected project.

Rules:

- `--project` follows the standard `flag > context` selection rule
- use `dsctl project list` to discover project names and numeric codes
- DS uses integer state `0` for disabled
- disabling project preference does not delete the stored JSON payload
- the same missing-row warning semantics as `enable` apply

## `dsctl project-worker-group list`

Lists the worker groups currently reported for one selected project.

Rules:

- selection uses `--project`, then context, then config
- use `dsctl project list` to discover project names and numeric codes
- upstream `GET /projects/{projectCode}/worker-group` may return both explicitly
  assigned worker groups and worker groups still implied by tasks or schedules
- output is a JSON array, not a paging wrapper

Each item keeps DS-native fields:

- `id`
- `projectCode`
- `workerGroup`
- `createTime`
- `updateTime`

Successful resolution returns:

- `resolved.project`

## `dsctl project-worker-group set`

Replaces the explicit worker-group assignment set for one selected project.

Rules:

- selection uses `--project`, then context, then config
- use `dsctl project list` to discover project names and numeric codes
- repeat `--worker-group NAME` to keep multiple worker groups assigned
- use `dsctl worker-group list` to discover worker-group names
- the CLI normalizes duplicates after trimming whitespace
- the CLI rejects an empty assignment set; use `clear --force` instead
- successful output returns the current upstream-reported worker-group list after
  the mutation
- if upstream still reports worker groups not present in the requested set, the
  CLI emits a warning because those groups are still used by tasks or schedules

Successful resolution returns:

- `resolved.project`
- `resolved.requested_worker_groups`

## `dsctl project-worker-group clear --force`

Removes the explicit worker-group assignment set for one selected project.

Rules:

- use `dsctl project list` to discover project names and numeric codes
- `--force` is required
- successful output returns the current upstream-reported worker-group list after
  the mutation
- upstream may still report worker groups that remain in use by tasks or
  schedules; when that happens, the CLI emits a warning aligned with one
  `warning_details[]` item using code `project_worker_group_still_in_use`

## `dsctl environment list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

When `--all` is used, the CLI materializes all fetched items into one
page-shaped response. `resolved.all` indicates the response was
client-aggregated.

## `dsctl environment get ENVIRONMENT`

Accepts an environment name or a numeric environment code, resolves the stable
environment identity, then fetches the current environment payload.

`resolved.environment` includes:

- `code`
- `name`
- `description`

## `dsctl environment create`

Creates one environment.

Options:

- `--name TEXT` required
- exactly one of `--config TEXT` or `--config-file PATH`
- `--description TEXT`
- `--worker-group NAME` repeatable

Rules:

- `config` is DS environment shell/export text, not JSON
- prefer `--config-file` for multiline configs
- run `dsctl template environment` for a starter config file

Successful output returns the refreshed environment payload. `resolved.environment`
contains the created `code`, `name`, and `description`.

## `dsctl environment update ENVIRONMENT`

Updates one resolved environment while preserving omitted fields.

Options:

- `--name TEXT`
- `--config TEXT`
- `--config-file PATH`
- `--description TEXT`
- `--clear-description`
- `--worker-group NAME` repeatable
- `--clear-worker-groups`

Rules:

- `ENVIRONMENT` may be an environment name or numeric code
- at least one field change is required
- `--description` and `--clear-description` are mutually exclusive
- `--config` and `--config-file` are mutually exclusive
- `--worker-group` and `--clear-worker-groups` are mutually exclusive
- omitted `name`, `config`, `description`, and worker groups preserve the
  current remote values

## `dsctl environment delete ENVIRONMENT --force`

Deletes one resolved environment.

Rules:

- `ENVIRONMENT` may be an environment name or numeric code
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.environment`

## `dsctl cluster list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current cluster list item fields:

- `id`
- `code`
- `name`
- `config`
- `description`
- `workflowDefinitions`
- `operator`
- `createTime`
- `updateTime`

## `dsctl cluster get CLUSTER`

Accepts a cluster name or a numeric cluster code, resolves the stable cluster
identity, then fetches the current cluster payload.

`resolved.cluster` includes:

- `code`
- `name`
- `description`

## `dsctl cluster create`

Creates one cluster.

Options:

- `--name TEXT` required
- exactly one of `--config TEXT` or `--config-file PATH`
- `--description TEXT`

Rules:

- `config` is DS cluster config JSON text; in DS 3.4.1 the UI submits
  `{"k8s": "...", "yarn": ""}`
- prefer `--config-file` for multiline Kubernetes kubeconfigs
- run `dsctl template cluster` for a starter config file

Successful output returns the refreshed cluster payload. `resolved.cluster`
contains the created `code`, `name`, and `description`.

## `dsctl cluster update CLUSTER`

Updates one resolved cluster while preserving omitted fields.

Options:

- `--name TEXT`
- `--config TEXT`
- `--config-file PATH`
- `--description TEXT`
- `--clear-description`

Rules:

- `CLUSTER` may be a cluster name or numeric code
- at least one field change is required
- `--description` and `--clear-description` are mutually exclusive
- `--config` and `--config-file` are mutually exclusive
- omitted `name`, `config`, and `description` preserve the current remote
  values

## `dsctl cluster delete CLUSTER --force`

Deletes one resolved cluster.

Rules:

- `CLUSTER` may be a cluster name or numeric code
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.cluster`

## `dsctl datasource list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current datasource list item fields:

- `id`
- `name`
- `note`
- `type`
- `userId`
- `userName` — DS datasource owner/creator user, not the datasource
  connection username
- `createTime`
- `updateTime`

`datasource list` keeps DS-native field names. For connection credentials, use
`datasource get DATASOURCE`; in that detail payload, `userName` is the
datasource connection username accepted by datasource create/update payloads.

## `dsctl datasource get DATASOURCE`

Accepts a datasource name or a numeric datasource id, resolves the stable
datasource identity, then fetches the current datasource detail payload.

Rules:

- `data` keeps the DS-native detail fields returned by `/datasources/{id}`
- plugin-specific datasource fields are preserved as-is

`resolved.datasource` includes:

- `id`
- `name`
- `note`
- `type`

## `dsctl datasource create`

Creates one datasource from a DS-native JSON payload file.

Options:

- `--file PATH` required

Rules:

- the file must contain one JSON object
- the payload must include string fields `name` and `type`
- `type` is normalized against the generated DS `DbType` enum; discover values
  with `dsctl enum list db-type`
- the payload must not include `id`
- masked password placeholders such as `******` are rejected for create
- run `dsctl template datasource` to choose a supported type, then
  `dsctl template datasource --type TYPE` and write `data.json` to the file

## `dsctl datasource update DATASOURCE`

Updates one datasource from a DS-native JSON payload file.

Options:

- `--file PATH` required

Rules:

- `DATASOURCE` may be a datasource name or numeric id
- the file must contain one JSON object
- the payload must include string fields `name` and `type`
- `type` is normalized against the generated DS `DbType` enum; discover values
  with `dsctl enum list db-type`
- if the payload includes `id`, it must match the selected datasource id
- if the payload contains `password: "******"` from a prior `datasource get`,
  the CLI sends an empty password so DS preserves the existing secret
- start from `dsctl datasource get DATASOURCE` or
  `dsctl template datasource --type TYPE` when preparing an update file
- when that warning is present, the aligned `warning_details[]` item uses code
  `datasource_update_preserved_existing_password`

## `dsctl datasource delete DATASOURCE --force`

Deletes one resolved datasource.

Rules:

- `DATASOURCE` may be a datasource name or numeric id
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.datasource`

## `dsctl datasource test DATASOURCE`

Runs one datasource connection test.

Rules:

- `DATASOURCE` may be a datasource name or numeric id
- success returns `data.connected`

## `dsctl namespace list`

Lists namespaces with optional filtering and pagination controls.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

Selection and behavior:

- this command is admin-only because DS 3.4.1 namespace paging is admin-only
- `--search` is passed to the upstream `searchVal`
- without `--all`, the command returns one DS-style page payload
- with `--all`, the CLI fetches remaining pages up to the safety limit and
  materializes one DS-style page payload

Current namespace list item fields:

- `id`
- `code`
- `namespace`
- `clusterCode`
- `clusterName`
- `userId`
- `userName`
- `createTime`
- `updateTime`

## `dsctl namespace get NAMESPACE`

Accepts a namespace name or a numeric namespace id, resolves the stable
namespace identity, then returns the current namespace payload.

Selection rules:

- this command is admin-only because it resolves through the admin-only
  namespace paging endpoint
- `NAMESPACE` may be a namespace name or numeric id
- namespace names may be ambiguous across clusters; when that happens, the CLI
  returns a resolution error and expects a numeric namespace id

`resolved.namespace` includes:

- `id`
- `namespace`
- `clusterCode`
- `clusterName`

## `dsctl namespace available`

Returns the namespace list available to the current login user.

Selection and behavior:

- this command maps to DS `GET /k8s-namespace/available-list`
- admins receive all namespaces
- non-admin users receive the namespaces currently authorized to them
- the `data` payload is a plain JSON list, not a paging object, because the DS
  endpoint itself returns a list

Each list item keeps the DS-native namespace shape:

- `id`
- `code`
- `namespace`
- `clusterCode`
- `clusterName`
- `userId`
- `userName`
- `createTime`
- `updateTime`

`resolved` includes:

- `scope` with value `current_user`

## `dsctl namespace create`

Creates one namespace.

Options:

- `--namespace TEXT` required
- `--cluster-code N` required

Selection and behavior:

- this command is admin-only because DS 3.4.1 namespace create is admin-only
- `--cluster-code` is the DS cluster code stored in the namespace record
- run `dsctl cluster list` to discover cluster codes
- the returned `data` payload keeps the DS-native namespace shape
- `data.clusterName` may be `null` in the immediate create response because the
  DS create path does not always project the cluster name

## `dsctl namespace delete NAMESPACE --force`

Deletes one resolved namespace.

Rules:

- this command is admin-only because DS 3.4.1 namespace delete is admin-only
- `NAMESPACE` may be a namespace name or numeric id
- namespace names may be ambiguous across clusters; when that happens, the CLI
  returns a resolution error and expects a numeric namespace id
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.namespace`

## `dsctl resource list`

Returns a DS-style paging object for one DS resource directory.

Options:

- `--dir TEXT`
- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- when `--dir` is omitted, the CLI resolves the upstream resource base directory
- selectors are DS `fullName` paths rather than opaque names
- run `dsctl resource list` or `dsctl resource list --dir DIR` to discover
  resource paths
- the paging payload keeps DS field names

Current resource list item fields:

- `alias`
- `userName`
- `fileName`
- `fullName`
- `isDirectory`
- `type`
- `size`
- `createTime`
- `updateTime`

## `dsctl resource view RESOURCE`

Views one text content window for one resource file.

Options:

- `--skip-line-num N`
- `--limit N`

Rules:

- `RESOURCE` is a DS `fullName` path
- run `dsctl resource list --dir DIR` to discover resource paths
- `resolved.resource` returns the normalized path metadata
- `data.content` contains the returned text window

## `dsctl resource upload`

Uploads one local file into one DS directory.

Options:

- `--file PATH` required
- `--dir TEXT`
- `--name TEXT`

Rules:

- run `dsctl resource list` to discover destination directory paths
- when `--name` is omitted, the local leaf filename is reused remotely
- the returned `data` payload is a CLI projection because DS upload does not
  return an entity body
- `resolved.source_file` records the local upload path

## `dsctl resource create`

Creates one text resource from inline content.

Options:

- `--name TEXT` required
- `--content TEXT` required
- `--dir TEXT`

Rules:

- `--name` must include a file extension because DS online-create accepts
  `fileName` and `suffix` separately
- use `dsctl resource upload --file PATH` when the content already lives in a
  local file
- the returned `data` payload is a CLI projection because DS online-create does
  not return an entity body

## `dsctl resource mkdir NAME`

Creates one directory inside one DS resource directory.

Options:

- `--dir TEXT`

Rules:

- `NAME` is one leaf directory name, not a path
- run `dsctl resource list` to discover parent directory paths
- the returned `data` payload is a CLI projection because DS directory create
  does not return an entity body

## `dsctl resource download RESOURCE`

Downloads one remote resource into one local file path.

Options:

- `--output PATH`
- `--overwrite`

Rules:

- run `dsctl resource list --dir DIR` to discover resource paths

Successful output returns:

- `data.fullName`
- `data.saved_to`
- `data.size`
- `data.content_type`

## `dsctl resource delete RESOURCE --force`

Deletes one resource selected by DS `fullName` path.

Rules:

- run `dsctl resource list --dir DIR` to discover resource paths
- `RESOURCE` is path-first, not name-first
- `--force` is required
- `data.resource.isDirectory` may be `null` when the selector does not prove the
  remote kind ahead of deletion

## `dsctl queue list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current queue list item fields:

- `id`
- `queueName`
- `queue`
- `createTime`
- `updateTime`

## `dsctl queue get QUEUE`

Accepts a queue name or a numeric queue id, resolves the stable queue identity,
then returns the current queue payload.

Run `dsctl queue list` to discover queue names and ids.

`resolved.queue` includes:

- `id`
- `queueName`
- `queue`

## `dsctl queue create`

Creates one queue.

Options:

- `--queue-name TEXT` required
- `--queue TEXT` required

Rules:

- `queueName` is the human-facing DS queue name used as the selector label
- `queue` is the underlying YARN queue value stored in DS

## `dsctl queue update QUEUE`

Updates one resolved queue while preserving omitted fields.

Options:

- `--queue-name TEXT`
- `--queue TEXT`

Rules:

- `QUEUE` may be a queue name or numeric id
- run `dsctl queue list` to discover queue names and ids
- at least one field change is required
- omitted `queueName` and `queue` preserve the current remote values

## `dsctl queue delete QUEUE --force`

Deletes one resolved queue.

Rules:

- `QUEUE` may be a queue name or numeric id
- run `dsctl queue list` to discover queue names and ids
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.queue`

## `dsctl worker-group list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- `search` is passed through to the upstream UI worker-group filter
- config-derived worker-group rows may still appear on every page because DS
  3.4.1 appends them after paging UI rows
- `--all` deduplicates repeated config-derived rows by stable identity before
  materializing the final page

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current worker-group list item fields:

- `id`
- `name`
- `addrList`
- `createTime`
- `updateTime`
- `description`
- `systemDefault`

## `dsctl worker-group get WORKER_GROUP`

Accepts a worker-group name or a numeric worker-group id, resolves the stable
worker-group identity, then returns the current worker-group payload.

Run `dsctl worker-group list` to discover worker-group names and ids.

`resolved.workerGroup` includes:

- `id`
- `name`
- `addrList`
- `systemDefault`

## `dsctl worker-group create`

Creates one worker group.

Options:

- `--name TEXT` required
- `--addr TEXT` repeatable
- `--description TEXT`

Rules:

- repeated `--addr` values are joined into the upstream `addrList`
- run `dsctl monitor server worker` to discover worker server addresses
- omitting `--addr` creates the worker group with an empty `addrList`

## `dsctl worker-group update WORKER_GROUP`

Updates one resolved worker group while preserving omitted fields.

Options:

- `--name TEXT`
- `--addr TEXT` repeatable
- `--clear-addrs`
- `--description TEXT`
- `--clear-description`

Rules:

- `WORKER_GROUP` may be a worker-group name or numeric id
- run `dsctl worker-group list` to discover worker-group names and ids
- run `dsctl monitor server worker` to discover worker server addresses for
  `--addr`
- at least one field change is required
- omitted fields preserve the current remote values
- `--addr` and `--clear-addrs` are mutually exclusive
- `--description` and `--clear-description` are mutually exclusive
- config-derived worker-group rows cannot be updated through the CRUD endpoint

## `dsctl worker-group delete WORKER_GROUP --force`

Deletes one resolved worker group.

Rules:

- `WORKER_GROUP` may be a worker-group name or numeric id
- run `dsctl worker-group list` to discover worker-group names and ids
- `--force` is required
- config-derived worker-group rows cannot be deleted through the CRUD endpoint

Successful output returns:

- `data.deleted`
- `data.workerGroup`

## `dsctl task-group list`

Returns a DS-style paging object.

Options:

- `--project PROJECT`
- `--search TEXT`
- `--status TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- without `--project`, `--search` and `--status` are passed to the global
  task-group paging API
- with `--project`, the CLI resolves project selection and uses DS's
  project-scoped task-group list shape
- run `dsctl project list` to discover project names and codes for `--project`
- `--project` cannot be combined with `--search` or `--status` because
  DolphinScheduler 3.4.1 does not expose that filter shape
- `--status` accepts `open`, `closed`, `1`, or `0`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current task-group list item fields:

- `id`
- `name`
- `projectCode`
- `description`
- `groupSize`
- `useSize`
- `userId`
- `status`
- `createTime`
- `updateTime`

## `dsctl task-group get TASK_GROUP`

Accepts a task-group name or a numeric task-group id, resolves the stable
task-group identity, then returns the current task-group payload.

Run `dsctl task-group list` to discover task-group names and ids.

`resolved.taskGroup` includes:

- `id`
- `name`
- `projectCode`

## `dsctl task-group create`

Creates one task group inside a resolved project.

Options:

- `--project PROJECT`
- `--name TEXT` required
- `--group-size N` required
- `--description TEXT`

Rules:

- project selection uses `flag > context`
- run `dsctl project list` to discover project names and codes for `--project`
- `groupSize` must be greater than or equal to `1`
- omitted description is sent as an empty string

Successful output returns the created task-group payload.

## `dsctl task-group update TASK_GROUP`

Updates one resolved task group while preserving omitted fields.

Options:

- `--name TEXT`
- `--group-size N`
- `--description TEXT`
- `--clear-description`

Rules:

- `TASK_GROUP` may be a task-group name or numeric id
- run `dsctl task-group list` to discover task-group names and ids
- at least one field change is required
- omitted fields preserve the current remote values
- `--clear-description` sends an empty description
- closed task groups must be started before they can be updated

Successful output returns the updated task-group payload.

## `dsctl task-group close TASK_GROUP`

Closes one resolved task group and returns the refreshed task-group payload.

Rules:

- `TASK_GROUP` may be a task-group name or numeric id
- run `dsctl task-group list` to discover task-group names and ids
- closing an already closed task group returns `invalid_state` with a
  suggestion to run `task-group start`

## `dsctl task-group start TASK_GROUP`

Starts one resolved task group and returns the refreshed task-group payload.

Rules:

- `TASK_GROUP` may be a task-group name or numeric id
- run `dsctl task-group list` to discover task-group names and ids
- starting an already open task group returns `invalid_state` with a suggestion
  to keep it open or run `task-group close`

## `dsctl task-group queue list TASK_GROUP`

Lists task-group queue rows for one resolved task group.

Options:

- `--task-instance TEXT`
- `--workflow-instance TEXT`
- `--status TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- `TASK_GROUP` may be a task-group name or numeric id
- run `dsctl task-group list` to discover task-group names and ids
- `--task-instance` filters by task-instance name
- `--workflow-instance` filters by workflow-instance name
- `--status` accepts `WAIT_QUEUE`, `ACQUIRE_SUCCESS`, `RELEASE`, `-1`, `1`,
  or `2`

The payload keeps DS paging field names. Current queue item fields:

- `id`
- `taskId`
- `taskName`
- `projectName`
- `projectCode`
- `workflowInstanceName`
- `groupId`
- `workflowInstanceId`
- `priority`
- `forceStart`
- `inQueue`
- `status`
- `createTime`
- `updateTime`

## `dsctl task-group queue force-start QUEUE_ID`

Force-starts one waiting task-group queue row by numeric queue id.

Run `dsctl task-group queue list TASK_GROUP` to discover queue ids.

Successful output returns:

- `data.queueId`
- `data.forceStarted`

If the queue row has already acquired task-group resources, the CLI returns
`invalid_state`.

## `dsctl task-group queue set-priority QUEUE_ID`

Sets one task-group queue row priority by numeric queue id.

Options:

- `--priority N` required

Rules:

- `QUEUE_ID` is id-first and does not use context
- run `dsctl task-group queue list TASK_GROUP` to discover queue ids
- `--priority` must be greater than or equal to `0`

Successful output returns:

- `data.queueId`
- `data.priority`

## `dsctl alert-plugin list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current alert-plugin list item fields:

- `id`
- `pluginDefineId`
- `instanceName`
- `pluginInstanceParams`
- `createTime`
- `updateTime`
- `instanceType`
- `warningType`
- `alertPluginName`

Rules:

- `search` is passed through to the upstream alert-plugin instance-name filter

## `dsctl alert-plugin get ALERT_PLUGIN`

Accepts an alert-plugin instance name or a numeric alert-plugin id, resolves
the stable identity, then returns the current alert-plugin payload.

Run `dsctl alert-plugin list` to discover alert-plugin instance names and ids.

`resolved.alertPlugin` includes:

- `id`
- `instanceName`
- `pluginDefineId`
- `alertPluginName`

## `dsctl alert-plugin definition list`

Lists the alert-plugin definitions supported by the current DolphinScheduler
runtime. This command returns plugin definitions such as `Feishu`, `Email`, or
`Slack`; it does not return configured alert-plugin instances.

Current definition list payload fields:

- `definitions`
- `count`
- `schema_command`

Current definition row fields:

- `id`
- `pluginName`
- `pluginType`
- `createTime`
- `updateTime`

Rules:

- use this command to discover valid `--plugin` values for
  `alert-plugin create`
- use `alert-plugin schema PLUGIN` to fetch the full parameter schema for one
  returned definition

## `dsctl alert-plugin schema PLUGIN`

Accepts an alert UI plugin definition name or a numeric plugin-definition id,
then returns the current plugin definition payload.

Current plugin definition fields:

- `id`
- `pluginName`
- `pluginType`
- `pluginParams`
- `pluginParamFields`
- `createTime`
- `updateTime`

Rules:

- `PLUGIN` must resolve to an alert UI plugin definition; plugin-definition
  names are matched exactly first, then case-insensitively when unique
- run `dsctl alert-plugin definition list` to discover plugin definitions
- name resolution fetches the plugin-detail endpoint after locating the id
  because the upstream list endpoint returns only definition summaries
- `pluginParams` is the DS-native UI param-list schema used by create/update
  and test-send flows
- `pluginParamFields` is a compact derived summary of the same schema for
  field discovery; it includes `field`, `type`, `required`, `defaultValue`,
  and options when present

## `dsctl alert-plugin create`

Creates one alert-plugin instance.

Options:

- `--name TEXT` required
- `--plugin TEXT` required
- `--param KEY=VALUE`
- `--params-json JSON`
- `--file PATH`

Rules:

- `--plugin` accepts an alert UI plugin definition name or numeric id
- run `dsctl alert-plugin definition list` to discover `--plugin` values
- pass exactly one of `--param`, `--params-json`, or `--file`
- `--param` may be repeated; it overlays fields from the upstream plugin
  schema and then submits DS-native UI params to DolphinScheduler
- field names from `--param` are matched exactly first, then
  case-insensitively when unique
- `--params-json` and `--file` accept a DS-native JSON array of UI param
  objects, not a plain key/value JSON object
- use `dsctl alert-plugin schema PLUGIN` to fetch the upstream param template,
  fill each item's `value`, then submit it unchanged

Successful output returns the refreshed alert-plugin instance payload.

## `dsctl alert-plugin update ALERT_PLUGIN`

Updates one resolved alert-plugin instance while preserving omitted fields.

Options:

- `--name TEXT`
- `--param KEY=VALUE`
- `--params-json JSON`
- `--file PATH`

Rules:

- `ALERT_PLUGIN` may be an alert-plugin instance name or numeric id
- run `dsctl alert-plugin list` to discover alert-plugin instance names and ids
- at least one field change is required
- omitted params preserve the current upstream `pluginInstanceParams`
- when params are provided, pass exactly one of `--param`, `--params-json`, or
  `--file`
- `--param` overlays the current upstream UI params; omitted fields keep their
  current values
- `--params-json` and `--file` replace the full DS-native UI params array

## `dsctl alert-plugin delete ALERT_PLUGIN --force`

Deletes one resolved alert-plugin instance.

Rules:

- `ALERT_PLUGIN` may be an alert-plugin instance name or numeric id
- run `dsctl alert-plugin list` to discover alert-plugin instance names and ids
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.alertPlugin`

## `dsctl alert-plugin test ALERT_PLUGIN`

Sends one test alert using the resolved alert-plugin instance.

Rules:

- `ALERT_PLUGIN` may be an alert-plugin instance name or numeric id
- run `dsctl alert-plugin list` to discover alert-plugin instance names and ids
- the CLI reuses the current upstream `pluginDefineId` and
  `pluginInstanceParams` from the resolved instance

Successful output returns:

- `data.tested`
- `resolved.alertPlugin`

## `dsctl alert-group list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current alert-group list item fields:

- `id`
- `groupName`
- `alertInstanceIds`
- `description`
- `createTime`
- `updateTime`
- `createUserId`

Rules:

- `search` is passed through to the upstream alert-group name filter
- use `dsctl alert-group list` to discover alert-group names and ids

## `dsctl alert-group get ALERT_GROUP`

Accepts an alert-group name or a numeric alert-group id, resolves the stable
alert-group identity, then returns the current alert-group payload.

Use `dsctl alert-group list` to discover alert-group names and ids.

`resolved.alertGroup` includes:

- `id`
- `groupName`
- `description`

## `dsctl alert-group create`

Creates one alert group.

Options:

- `--name TEXT` required
- `--instance-id N` repeatable
- `--description TEXT`

Rules:

- use `dsctl alert-plugin list` to discover alert plugin instance ids
- repeated `--instance-id` values are deduplicated before the upstream request
- omitting `--instance-id` sends an empty upstream `alertInstanceIds` string

## `dsctl alert-group update ALERT_GROUP`

Updates one resolved alert group while preserving omitted fields.

Options:

- `--name TEXT`
- `--instance-id N` repeatable
- `--clear-instance-ids`
- `--description TEXT`
- `--clear-description`

Rules:

- `ALERT_GROUP` may be an alert-group name or numeric id
- use `dsctl alert-group list` to discover alert-group names and ids
- use `dsctl alert-plugin list` to discover alert plugin instance ids
- at least one field change is required
- omitted fields preserve the current remote values
- `--instance-id` and `--clear-instance-ids` are mutually exclusive
- `--description` and `--clear-description` are mutually exclusive

## `dsctl alert-group delete ALERT_GROUP --force`

Deletes one resolved alert group.

Rules:

- `ALERT_GROUP` may be an alert-group name or numeric id
- use `dsctl alert-group list` to discover alert-group names and ids
- `--force` is required
- DS 3.4.1 does not allow deleting the default alert group

Successful output returns:

- `data.deleted`
- `data.alertGroup`

## `dsctl tenant list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current tenant list item fields:

- `id`
- `tenantCode`
- `description`
- `queueId`
- `queueName`
- `queue`
- `createTime`
- `updateTime`

Rules:

- `search` is passed through to the upstream tenant-code filter
- use `dsctl tenant list` to discover tenant codes and ids
- `queueName` is usually present from the paging endpoint
- `queue` may be `null` because the DS tenant paging query does not always
  project the underlying queue value

## `dsctl tenant get TENANT`

Accepts a tenant code or a numeric tenant id, resolves the stable tenant
identity, then returns the current tenant payload.

`resolved.tenant` includes:

- `id`
- `tenantCode`
- `description`
- `queueId`
- `queueName`
- `queue`

Rules:

- use `dsctl tenant list` to discover tenant codes and ids
- `queueName` is the more reliable upstream tenant queue label
- `queue` may still be `null` on real clusters when the DS tenant detail query
  does not project the underlying queue value

## `dsctl tenant create`

Creates one tenant.

Options:

- `--tenant-code TEXT` required
- `--queue TEXT` required
- `--description TEXT`

Rules:

- `--queue` accepts a queue name or numeric id
- use `dsctl queue list` to discover queue names and ids
- the CLI resolves `--queue` to the upstream `queueId`

## `dsctl tenant update TENANT`

Updates one resolved tenant while preserving omitted fields.

Options:

- `--tenant-code TEXT`
- `--queue TEXT`
- `--description TEXT`
- `--clear-description`

Rules:

- `TENANT` may be a tenant code or numeric id
- `--queue` accepts a queue name or numeric id
- use `dsctl tenant list` to discover tenant codes and ids
- use `dsctl queue list` to discover queue names and ids
- at least one field change is required
- omitted fields preserve the current remote values
- `--description` and `--clear-description` are mutually exclusive

## `dsctl tenant delete TENANT --force`

Deletes one resolved tenant.

Rules:

- `TENANT` may be a tenant code or numeric id
- use `dsctl tenant list` to discover tenant codes and ids
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.tenant`

## `dsctl user list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Current user list item fields:

- `id`
- `userName`
- `email`
- `phone`
- `userType`
- `tenantId`
- `tenantCode`
- `queueName`
- `queue`
- `state`
- `createTime`
- `updateTime`

Rules:

- `search` is passed through to the upstream user-name filter
- use `dsctl user list` to discover user names and ids
- `queue` is the effective queue surfaced by the upstream paging view
- `queueName` is the tenant queue name joined by the upstream paging view

## `dsctl user get USER`

Accepts a user name or a numeric user id, resolves the stable user identity,
then returns the current user payload.

`resolved.user` includes:

- `id`
- `userName`
- `email`
- `tenantId`
- `tenantCode`
- `state`

Current get-only extra fields:

- `timeZone`

Rules:

- `USER` may be a user name or numeric id
- use `dsctl user list` to discover user names and ids
- `queue` remains the effective queue shown by the merged upstream user views

## `dsctl user create`

Creates one user.

Options:

- `--user-name TEXT` required
- `--password TEXT` required
- `--email TEXT` required
- `--tenant TEXT` required
- `--state {0,1}` required
- `--phone TEXT`
- `--queue TEXT`

Rules:

- `--tenant` accepts a tenant code or numeric id
- `--queue` is the raw queue-name override stored on the user record
- use `dsctl tenant list` to discover tenant codes and ids
- use `dsctl queue list` to discover queue names
- `--state 1` means enabled and `--state 0` means disabled

## `dsctl user update USER`

Updates one resolved user while preserving omitted fields.

Options:

- `--user-name TEXT`
- `--password TEXT`
- `--email TEXT`
- `--tenant TEXT`
- `--state {0,1}`
- `--phone TEXT`
- `--clear-phone`
- `--queue TEXT`
- `--clear-queue`
- `--time-zone TEXT`

Rules:

- `USER` may be a user name or numeric id
- use `dsctl user list` to discover user names and ids
- omitted fields preserve the current remote values
- `--tenant` accepts a tenant code or numeric id
- `--queue` is the raw queue-name override stored on the user record
- use `dsctl tenant list` to discover tenant codes and ids
- use `dsctl queue list` to discover queue names
- `--phone` and `--clear-phone` are mutually exclusive
- `--queue` and `--clear-queue` are mutually exclusive
- at least one field change is required

## `dsctl user delete USER --force`

Deletes one resolved user.

Rules:

- `USER` may be a user name or numeric id
- use `dsctl user list` to discover user names and ids
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.user`

## `dsctl user grant project USER PROJECT`

Grants one resolved project to one resolved user with write permission.

Rules:

- `USER` may be a user name or numeric id
- `PROJECT` may be a project name or numeric code
- use `dsctl user list` and `dsctl project list` to discover selectors
- the command uses the DS additive project grant path rather than replacing the
  full user grant set

Successful output returns:

- `data.granted`
- `data.permission`
- `data.user`
- `data.project`

## `dsctl user revoke project USER PROJECT`

Revokes one resolved project from one resolved user.

Rules:

- `USER` may be a user name or numeric id
- `PROJECT` may be a project name or numeric code
- use `dsctl user list` and `dsctl project list` to discover selectors

Successful output returns:

- `data.revoked`
- `data.user`
- `data.project`

## `dsctl user grant datasource USER --datasource DATASOURCE ...`

Grants one or more resolved datasources to one resolved user.

Options:

- `--datasource TEXT` repeatable and required

Rules:

- `USER` may be a user name or numeric id
- each `--datasource` accepts a datasource name or numeric id
- use `dsctl user list` and `dsctl datasource list` to discover selectors
- the CLI reads the user's currently authorized datasources, merges the
  requested datasources into that set, then writes the full set back through
  the DS datasource-grant endpoint
- repeated datasource selections are deduplicated by datasource id

Successful output returns:

- `data.granted`
- `data.user`
- `data.requested_datasources`
- `data.datasources`

## `dsctl user revoke datasource USER --datasource DATASOURCE ...`

Revokes one or more resolved datasources from one resolved user.

Options:

- `--datasource TEXT` repeatable and required

Rules:

- `USER` may be a user name or numeric id
- each `--datasource` accepts a datasource name or numeric id
- use `dsctl user list` and `dsctl datasource list` to discover selectors
- the CLI reads the user's currently authorized datasources, subtracts the
  requested datasources from that set, then writes the remaining full set back
  through the DS datasource-grant endpoint
- repeated datasource selections are deduplicated by datasource id

Successful output returns:

- `data.revoked`
- `data.user`
- `data.requested_datasources`
- `data.datasources`

## `dsctl user grant namespace USER --namespace NAMESPACE ...`

Grants one or more resolved namespaces to one resolved user.

Options:

- `--namespace TEXT` repeatable and required

Rules:

- `USER` may be a user name or numeric id
- each `--namespace` accepts a namespace name or numeric id
- use `dsctl user list` and `dsctl namespace list` to discover selectors
- namespace names may be ambiguous across clusters; when that happens, the CLI
  returns a resolution error and expects a numeric namespace id
- the CLI reads the user's currently authorized namespaces, merges the
  requested namespaces into that set, then writes the full set back through the
  DS namespace-grant endpoint
- repeated namespace selections are deduplicated by namespace id

Successful output returns:

- `data.granted`
- `data.user`
- `data.requested_namespaces`
- `data.namespaces`

## `dsctl user revoke namespace USER --namespace NAMESPACE ...`

Revokes one or more resolved namespaces from one resolved user.

Options:

- `--namespace TEXT` repeatable and required

Rules:

- `USER` may be a user name or numeric id
- each `--namespace` accepts a namespace name or numeric id
- use `dsctl user list` and `dsctl namespace list` to discover selectors
- namespace names may be ambiguous across clusters; when that happens, the CLI
  returns a resolution error and expects a numeric namespace id
- the CLI reads the user's currently authorized namespaces, subtracts the
  requested namespaces from that set, then writes the remaining full set back
  through the DS namespace-grant endpoint
- repeated namespace selections are deduplicated by namespace id

Successful output returns:

- `data.revoked`
- `data.user`
- `data.requested_namespaces`
- `data.namespaces`

## `dsctl access-token list`

Returns a DS-style paging object.

Options:

- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

The payload keeps DS paging field names:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`
- `pageNo`

Each item keeps DS-native access-token fields:

- `id`
- `userId`
- `token`
- `expireTime`
- `createTime`
- `updateTime`
- `userName`

## `dsctl access-token get ACCESS_TOKEN`

Accepts one numeric access-token id.

Selection rules:

- `ACCESS_TOKEN` is id-first
- use `dsctl access-token list` to discover token ids

`resolved.accessToken` includes:

- `id`
- `userId`
- `userName`

## `dsctl access-token create`

Creates one access token.

Rules:

- `--user` is required and accepts a user name or numeric id
- use `dsctl user list` to discover user names and ids
- `--expire-time` is required and follows the DS format
  `YYYY-MM-DD HH:MM:SS`
- `--token` is optional; omitting it lets DS generate one

## `dsctl access-token update ACCESS_TOKEN`

Updates one access token by numeric id.

Rules:

- requires at least one of `--user`, `--expire-time`, `--token`, or
  `--regenerate-token`
- `--user` accepts a user name or numeric id
- use `dsctl access-token list` to discover token ids
- use `dsctl user list` to discover user names and ids
- omitted `--user` and `--expire-time` preserve the current remote values
- omitted `--token` preserves the current token unless `--regenerate-token` is
  used
- `--token` and `--regenerate-token` are mutually exclusive

## `dsctl access-token delete ACCESS_TOKEN --force`

Deletes one access token by numeric id.

Rules:

- use `dsctl access-token list` to discover token ids
- `--force` is required

Successful output returns:

- `data.deleted`
- `data.accessToken`

## `dsctl access-token generate`

Generates one token string without persisting it.

Rules:

- `--user` is required and accepts a user name or numeric id
- use `dsctl user list` to discover user names and ids
- `--expire-time` is required and follows the DS format
  `YYYY-MM-DD HH:MM:SS`

Successful output returns:

- `data.token`
- `data.userId`
- `data.expireTime`

## `dsctl monitor health`

Returns the raw API server actuator health payload.

Resolved fields:

- `endpoint`
- `scope`

Rules:

- `resolved.scope` is currently always `api_server`
- `data` is the DS API server health object returned by `/actuator/health`

## `dsctl monitor server TYPE`

Lists registry-backed servers for one DS node type.

Arguments:

- `TYPE` must be one of `master`, `worker`, `alert-server`

Rules:

- the payload is a JSON array
- each item keeps DS-native field names
- `resolved.node_type` returns the normalized DS enum name

Current item fields:

- `id`
- `host`
- `port`
- `serverDirectory`
- `heartBeatInfo`
- `createTime`
- `lastHeartbeatTime`

## `dsctl monitor database`

Lists database health metrics reported by the DS monitor API.

Resolved fields:

- `endpoint`

Rules:

- the payload is a JSON array
- each item keeps DS-native field names
- unhealthy databases keep their raw `state` in `data` and also emit a warning
- when that warning is present, the aligned `warning_details[]` item uses code
  `monitor_database_degraded` and includes `db_type` plus `state`

Current item fields:

- `dbType`
- `state`
- `maxConnections`
- `maxUsedConnections`
- `threadsConnections`
- `threadsRunningConnections`
- `date`

## `dsctl audit list`

Lists audit-log rows with optional DS-native filters.

Resolved fields:

- `modelTypes`
- `operationTypes`
- `start`
- `end`
- `userName`
- `modelName`
- `page_no`
- `page_size`
- `all`

Rules:

- `--model-type` is repeatable and mapped to DS `modelTypes`
- `--operation-type` is repeatable and mapped to DS `operationTypes`
- use `dsctl audit model-types` to discover model-type filter values
- use `dsctl audit operation-types` to discover operation-type filter values
- `--start` and `--end` must use DS datetime format `YYYY-MM-DD HH:MM:SS`
- when both `--start` and `--end` are provided, `end` must be greater than or
  equal to `start`
- `--all` keeps the standard DS-style page payload and only auto-fetches more
  remote pages behind the scenes

The payload is the standard DS paging object:

- `totalList`
- `total`
- `totalPage`
- `pageSize`
- `currentPage`

Current audit-log item fields:

- `userName`
- `modelType`
- `modelName`
- `operation`
- `createTime`
- `description`
- `detail`
- `latency`

## `dsctl audit model-types`

Returns the DS audit model-type tree used by the audit filter UI.

Resolved fields:

- `source`

Rules:

- the payload is a JSON array
- each item keeps the DS-native tree shape

Current item fields:

- `name`
- `child`

## `dsctl audit operation-types`

Returns the DS audit operation-type list used by the audit filter UI.

Resolved fields:

- `source`

Rules:

- the payload is a JSON array
- each item keeps DS-native field names

Current item fields:

- `name`

## `dsctl workflow list`

Lists workflows inside one resolved project.

Selection rules:

- `--project` wins
- then stored context project
- use `dsctl project list` to discover project names and codes

The `data` payload is a JSON array of workflow summaries:

- `code`
- `name`
- `version`

## `dsctl workflow get`

Fetches one workflow by name or numeric code.

Selection rules:

- project selection: `flag > context`
- workflow selection: positional argument, then context workflow
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Formats:

- `--format json` returns the workflow payload
- `--format yaml` returns YAML inside the standard envelope as `data.yaml`

## `dsctl workflow describe`

Returns one workflow DAG as structured JSON:

Use `dsctl project list` and `dsctl workflow list` to discover selectors.

- `data.workflow`
- `data.tasks`
- `data.relations`

## `dsctl workflow digest`

Returns one compact workflow graph summary derived from the workflow DAG.

Use `dsctl project list` and `dsctl workflow list` to discover selectors.

Current `data` fields:

- `workflow`
- `taskCount`
- `relationCount`
- `taskTypeCounts`
- `globalParamNames`
- `rootTasks`
- `leafTasks`
- `isolatedTasks`
- `tasks`

Current guarantees:

- `data.tasks` is ordered for graph inspection rather than full raw DS detail
- each task entry keeps DS-native identity fields (`code`, `name`, `taskType`)
  and adds compact graph links (`upstreamTasks`, `downstreamTasks`)
- omits verbose task payload fields such as `taskParams` and retry/timeout
  details to reduce context size before a caller decides whether to fetch the
  full `workflow describe` or `workflow get --format yaml` view

## `dsctl workflow create`

Creates one workflow definition from a YAML file.

Options:

- `--file PATH` (required)
- `--project PROJECT`
- `--dry-run`
- `--confirm-risk TOKEN`

Rules:

- use `dsctl template workflow --raw` to start a workflow YAML file
- use `dsctl template task` to discover task template variants
- use `dsctl task-type schema TYPE` to inspect full task fields, choices,
  related discovery commands, and state rules before writing `task_params`
- use `dsctl project list` to discover project names and codes for `--project`
- project selection precedence is:
  - explicit `--project`
  - then `workflow.project` from the YAML file
  - then `context`
- `--dry-run` returns the compiled legacy DS form request inside the standard
  dry-run envelope; when additional lifecycle steps would run, `data.requests`
  contains the ordered request plan
- when a YAML `schedule:` block is present, `--dry-run` also returns
  `data.schedule_preview` and `data.schedule_confirmation`
- YAML `workflow.release_state: ONLINE` creates the workflow, then brings it
  online as a second step
- YAML `schedule:` blocks are supported
- `schedule:` requires `workflow.release_state: ONLINE`
- `schedule.cron` must be a DolphinScheduler Quartz cron expression with 6 or
  7 fields and seconds first
- if `schedule.release_state` or `schedule.enabled` requests an online
  schedule, the CLI creates the schedule, then brings it online as a final
  step
- high-frequency schedules reuse the standard `confirmation_required` flow and
  expect the same command to be retried with `--confirm-risk TOKEN`
- the same high-frequency confirmation rule applies to `--dry-run`
- when a confirmed high-frequency schedule warning is emitted, the aligned
  `warning_details[]` item uses code `confirmed_high_frequency_schedule`
- workflow dynamic parameter time-format warnings use the same
  `parameter_time_format_*` codes as `lint workflow`, both in dry-run and
  applied create results

The current stable YAML surface supports:

- `workflow.name`
- `workflow.project`
- `workflow.description`
- `workflow.timeout`
- `workflow.global_params`
- `workflow.execution_type`
- `workflow.release_state`
- optional `schedule` block with:
  - `cron`
  - `timezone`
  - `start`
  - `end`
  - `failure_strategy`
  - `priority`
  - `release_state`
  - `enabled`
- task entries with:
  - `name`
  - `type`
  - `description`
  - `task_params`
  - `command` for `SHELL`, `PYTHON`, and `REMOTESHELL`
  - `worker_group`
  - `priority`
  - `retry`
  - `timeout`
  - `delay`
  - `depends_on`
- task identity fields such as DS task `code` and `version` are system-managed
  and are not authored in workflow YAML

Current stable per-task-type validation is built in for:

- `SHELL`
- `PYTHON`
- `REMOTESHELL`
- `SQL`
- `HTTP`
- `SUB_WORKFLOW`
- `DEPENDENT`
- `SWITCH`
- `CONDITIONS`

Unknown task types still accept raw `task_params` mappings so exported YAML can
round-trip while task coverage grows.

Current dependency rules are intentionally narrow:

- `depends_on` expresses only basic in-workflow predecessor edges
- it does not encode DS logical task types such as `DEPENDENT`,
  `CONDITIONS`, or `SWITCH`
- those logical nodes belong in `type`-specific `task_params` models, not in a
  generic dependency DSL
- for `SWITCH` and `CONDITIONS`, branch targets are written as task names in
  YAML and compiled into DS task codes during `workflow create`

## `dsctl workflow edit`

Edits one existing workflow definition from a minimal YAML patch.

Options:

- positional `WORKFLOW` is optional and falls back to workflow context
- `--patch PATH` (required)
- `--project PROJECT`
- `--dry-run`

Rules:

- selection precedence matches other workflow commands:
  - explicit positional `WORKFLOW`
  - then workflow context
- project selection precedence remains `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors
- the patch is applied against the current live workflow YAML export, then
  compiled back into one legacy whole-definition update payload
- current stable patch operations are:
  - `patch.workflow.set`
  - `patch.tasks.create`
  - `patch.tasks.update`
  - `patch.tasks.rename`
  - `patch.tasks.delete`
- patch YAML is a CLI delta document rooted at `patch:`, not a REST `PATCH`
  request
- start new patch files with `dsctl template workflow-patch --raw`
- `patch.tasks.create[]` uses the same task item shape as full workflow YAML;
  use `dsctl template task TYPE --raw` and `dsctl task-type schema TYPE` to
  discover valid type-specific `task_params`
- `patch.tasks.update[].match.name` matches the live task name before the patch
  is applied
- `patch.tasks.update[].set` is a partial task object; omitted fields preserve
  the live value
- `patch.tasks.rename[]` is the only stable way to preserve task identity while
  changing a task name; the CLI does not guess rename intent from delete/create
  pairs
- `patch.tasks.delete[]` contains live task names to remove

Example:

```yaml
patch:
  workflow:
    set:
      description: "Updated workflow description"
      timeout: 3600
  tasks:
    create:
      - name: transform
        type: SHELL
        command: |
          echo transform
        depends_on:
          - extract
    update:
      - match:
          name: load
        set:
          depends_on:
            - transform
    rename:
      - from: old-load
        to: load
    delete:
      - obsolete
```
- current stable `patch.tasks.update[].set` fields are:
  - `type`
  - `description`
  - `task_params`
  - `command`
  - `flag`
  - `worker_group`
  - `environment_code`
  - `task_group_id`
  - `task_group_priority`
  - `priority`
  - `retry`
  - `timeout`
  - `timeout_notify_strategy`
  - `delay`
  - `cpu_quota`
  - `memory_max`
  - `depends_on`
- task matching is name-based
- workflow edit preserves the live DS task `code + version` identity for
  existing tasks and allocates new task codes only for newly created tasks
- task renames rewrite:
  - `depends_on`
  - `SWITCH` branch targets
  - `CONDITIONS` success and failure targets
- patch validation uses the same task-shape rules as workflow YAML:
  - `flag` accepts `YES` or `NO`
  - `task_group_priority` requires an effective `task_group_id`
  - `timeout_notify_strategy` requires an effective timeout greater than `0`
- workflow edit treats DS-semantic defaults as no-op equivalents when building
  `data.diff.updated_tasks`; for example:
  - `worker_group: default` and omitted `worker_group`
  - `timeout_notify_strategy: WARN` and omitted strategy on an open timeout
  - `cpu_quota: -1` or `memory_max: -1` and omitted resource limits
- `--dry-run` returns the merged legacy update request plus:
  - `data.diff`
  - `data.workflow_state_constraints`
  - `data.workflow_state_constraint_details`
  - `data.schedule_impacts`
  - `data.schedule_impact_details`
  - `data.no_change`
- apply requires the live workflow to already be offline
- applying a no-op patch returns the current workflow payload, emits one warning
  with aligned `warning_details[]` code `workflow_edit_no_persistent_change`,
  and sends no update request
- when apply emits attached-schedule impact warnings, the aligned
  `warning_details[]` items reuse the same codes as
  `data.schedule_impact_details[].code`
- workflow dynamic parameter time-format warnings use the same
  `parameter_time_format_*` codes as `lint workflow`, both in dry-run and
  applied edit results
- workflow edit does not mutate the attached schedule; schedule lifecycle stays
  on `schedule update|online|offline`

Current `data.diff` fields:

- `workflow_updated_fields`
- `added_tasks`
- `updated_tasks`
- `renamed_tasks`
- `deleted_tasks`
- `added_edges`
- `removed_edges`

Current `data.workflow_state_constraint_details[]` fields:

- `code`
- `message`
- `blocking`
- `current_release_state`
- `required_release_state`
- `current_schedule_release_state`

Current `data.schedule_impact_details[]` fields:

- `code`
- `message`
- `desired_workflow_release_state`
- `current_schedule_release_state`
- `dag_valid`

## `dsctl workflow delete WORKFLOW --force`

Deletes one workflow definition after explicit confirmation.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- positional `WORKFLOW` is optional and falls back to workflow context
- `--force` is required
- the CLI fetches the current workflow before deletion and returns that payload
  in `data.workflow`
- the workflow must be offline before deletion
- workflows with online schedules must have their schedule taken offline first
- workflows with running workflow instances cannot be deleted until those
  instances stop or finish
- workflows still referenced by other tasks return `conflict` and suggest
  `workflow lineage dependent-tasks`

Successful output returns:

- `data.deleted`
- `data.workflow`

## `dsctl workflow lineage list`

Returns the project-wide workflow lineage graph for one resolved project.

Selection rules:

- project selection: `flag > context`
- use `dsctl project list` to discover project names and codes

Current `data` fields:

- `workFlowRelationList`
- `workFlowRelationDetailList`

`data.workFlowRelationList[]` fields:

- `sourceWorkFlowCode`
- `targetWorkFlowCode`

`data.workFlowRelationDetailList[]` fields:

- `workFlowCode`
- `workFlowName`
- `workFlowPublishStatus`
- `scheduleStartTime`
- `scheduleEndTime`
- `crontab`
- `schedulePublishStatus`
- `sourceWorkFlowCode`

## `dsctl workflow lineage get WORKFLOW`

Returns the lineage graph anchored on one resolved workflow.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- positional `WORKFLOW` is optional and falls back to workflow context
- the payload shape matches `workflow lineage list`

## `dsctl workflow lineage dependent-tasks WORKFLOW`

Returns workflows and tasks that depend on one resolved workflow or task.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- optional task filter is explicit-only through `--task`
- use `dsctl project list`, `dsctl workflow list`, and `dsctl task list` to
  discover selectors

Options:

- `--project PROJECT`
- `--task TASK`

Rules:

- positional `WORKFLOW` is optional and falls back to workflow context
- `--task` accepts a task name or numeric task code inside the selected
  workflow

Current `data[]` item fields:

- `projectCode`
- `workflowDefinitionCode`
- `workflowDefinitionName`
- `taskDefinitionCode`
- `taskDefinitionName`

## `dsctl schedule list`

Lists schedules inside one resolved project.

Selection rules:

- project selection: `flag > context`
- workflow filter is explicit-only through `--workflow`

Options:

- `--project PROJECT`
- `--workflow WORKFLOW`
- `--search TEXT`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- `--workflow` and `--search` are mutually exclusive
- use `dsctl project list` to discover project names and codes for `--project`
- use `dsctl workflow list` inside the selected project to discover workflow
  names and codes for `--workflow`
- the payload keeps DS paging field names:
  - `totalList`
  - `total`
  - `totalPage`
  - `pageSize`
  - `currentPage`
  - `pageNo`
- when `--all` is used, the CLI materializes all fetched items into one
  page-shaped response and sets `resolved.all=true`

Each schedule item keeps DS-native field names such as:

- `id`
- `workflowDefinitionCode`
- `workflowDefinitionName`
- `projectName`
- `crontab`
- `timezoneId`
- `releaseState`

## `dsctl schedule get SCHEDULE_ID`

Fetches one schedule by numeric id.

Selection rules:

- `SCHEDULE_ID` is id-first and does not use context
- use `dsctl schedule list` inside the selected project to discover schedule ids

## `dsctl template workflow`

Returns the current stable workflow YAML template inside `data.yaml`.
`data.lines[]` provides the same template as row-oriented `line_no` and `line`
values for table and tsv output.

Use `--raw` when redirecting the template to a YAML file:

```bash
dsctl template workflow --raw > workflow.yaml
```

Options:

- `--with-schedule`
- `--raw`

Rules:

- the template matches the stable `workflow create --file ...` YAML surface
- it omits `schedule:` by default even though `workflow create` supports it
- this keeps the base template focused on the minimum full-spec workflow shape;
  schedule remains an optional add-on block
- each example task also includes commented optional runtime-control fields for
  `flag`, `environment_code`, `task_group_id`, `task_group_priority`,
  `timeout_notify_strategy`, `cpu_quota`, and `memory_max`
- `dsctl template workflow --with-schedule` includes one minimal optional
  `schedule:` block and returns `resolved.with_schedule=true`
- the optional `schedule.cron` example uses DolphinScheduler Quartz cron syntax
- `--raw` prints only the workflow YAML; it does not print the standard success
  envelope

## `dsctl template workflow-patch`

Returns a workflow edit patch YAML starting point inside `data.yaml`.
`data.lines[]` provides the same template as row-oriented `line_no` and `line`
values for table and tsv output.

Use `--raw` when redirecting the template to a YAML file:

```bash
dsctl template workflow-patch --raw > patch.yaml
dsctl workflow edit WORKFLOW --patch patch.yaml --dry-run
```

Options:

- `--raw`

Rules:

- the template matches the stable `workflow edit --patch ...` delta surface
- the YAML root is always `patch:`
- the active default patch changes workflow metadata only, so the file remains
  valid before task names are customized
- task create/update/rename/delete examples are included as comments; uncomment
  only the operations needed for the current edit
- `tasks.create[]` uses full task fragments from `dsctl template task TYPE --raw`
- `tasks.update[].set` uses partial task fields discoverable with
  `dsctl task-type schema TYPE`
- run `workflow edit --dry-run` before mutating the workflow definition

## `dsctl template workflow-instance-patch`

Returns a finished workflow-instance edit patch YAML starting point inside
`data.yaml`. `data.lines[]` provides the same template as row-oriented `line_no`
and `line` values for table and tsv output.

Use `--raw` when redirecting the template to a YAML file:

```bash
dsctl template workflow-instance-patch --raw > instance-patch.yaml
dsctl workflow-instance edit WORKFLOW_INSTANCE --patch instance-patch.yaml --dry-run
```

Options:

- `--raw`

Rules:

- the template matches the stable `workflow-instance edit --patch ...` delta
  surface
- the YAML root is always `patch:`
- `workflow-instance edit` only accepts `workflow.set.global_params` and
  `workflow.set.timeout` inside the workflow block
- task create/update/rename/delete examples are included as comments; uncomment
  only the operations needed for the repair
- `tasks.create[]` uses full task fragments from `dsctl template task TYPE --raw`
- `tasks.update[].set` uses partial task fields discoverable with
  `dsctl task-type schema TYPE`
- use `--sync-definition` only when the repaired instance DAG should also be
  written back to the current workflow definition

## `dsctl template params`

Returns progressive DS parameter syntax metadata.

Options:

- `--topic TOPIC`

Without `--topic`, the command returns only a compact topic index. Use a topic
when detailed syntax is needed.

Supported topics:

- `overview`
- `property`
- `built-in`
- `time`
- `context`
- `output`
- `all`

Default `data` fields:

- `default_topic`
- `topics`
- `recommended_flow`
- `rules`

Topic result fields:

- `topic`
- `summary`
- `next_topics`
- `details`

Rules:

- DS task parameters use the upstream `Property` shape
- `workflow.global_params` may use a mapping shorthand for IN VARCHAR values
- task-level parameters belong under `task_params.localParams`
- `task_params.varPool` is a runtime output pool and should normally stay empty
  in newly authored YAML
- `$[...]` time placeholders such as `$[yyyyMMdd-1]` are DS-native runtime
  expressions and are preserved as strings by the CLI
- DS uses Java-style date patterns inside `$[...]`: lowercase `yyyy` means
  calendar year, while uppercase `YYYY` means week-based year. The CLI emits
  warnings for risky expressions such as `$[YYYYMMdd]` or `$[yyyyww]`; use
  `year_week(...)` when week-of-year output is intended.
- script-like tasks can publish OUT parameters by writing
  `${setValue(name=value)}` or `#{setValue(name=value)}` to task logs
- SQL tasks can publish result columns whose names match OUT parameter `prop`
  values

## `dsctl template environment`

Returns one DS environment shell/export config template.

Default table and tsv output render `data.lines[]` so multiline config content
does not collapse into one large value cell.

Current `data` fields:

- `filename`
- `config`
- `lines`
- `target_commands`
- `source_options`
- `upstream_request_shape`
- `rules`

Rules:

- `data.config` is the file content accepted by
  `environment create --config-file` and `environment update --config-file`
- `data.lines[]` contains row-oriented `line` and `purpose` values for compact
  terminal scanning
- DS stores this value as the raw `EnvironmentController` form field `config`
- environment paths must exist on DolphinScheduler worker hosts

## `dsctl template cluster`

Returns one DS cluster config JSON template.

Default table and tsv output render `data.fields[]` so multiline kubeconfig
content does not collapse into one large value cell.

Current `data` fields:

- `filename`
- `config`
- `payload`
- `fields`
- `rows`
- `target_commands`
- `source_options`
- `upstream_request_shape`
- `upstream_ui_shape`
- `rules`

Rules:

- `data.config` is the file content accepted by
  `cluster create --config-file` and `cluster update --config-file`
- DS stores this value as the raw `ClusterController` form field `config`
- DS 3.4.1 reads the `k8s` JSON field as Kubernetes kubeconfig content
- keep `yarn` as an empty string unless your DS deployment uses it

## `dsctl template datasource`

Returns datasource JSON payload-template type discovery when `--type` is
omitted, or one DS-native datasource JSON payload template when `--type TYPE`
is passed.

Options:

- `--type TYPE`

Default index fields:

- `data.default_type`
- `data.template_command`
- `data.template_command_pattern`
- `data.target_commands`
- `data.type_enum`
- `data.type_discovery_command`
- `data.supported_types`
- `data.rows`

`resolved.view` is `list` for the default output. The supported type list lives
in `data.supported_types`; `resolved` does not duplicate it.

Typed template fields:

- `data.type`
- `data.target_commands`
- `data.source_option`
- `data.payload`
- `data.json`
- `data.fields`
- `data.rows`
- `data.rules`

`resolved.view` is `template` and `resolved.datasource_type` is the normalized
DS `DbType` value selected by `--type`.

Rules:

- `type` matching is case-insensitive and accepts common generated `DbType`
  aliases such as `mysql` and `aliyun-serverless-spark`
- `data.payload` is the object shape accepted by `datasource create --file`
- `data.json` is the same payload rendered as pretty JSON
- `data.fields` is grounded in generated `BaseDataSourceParamDTO` plus known
  plugin-specific JSON fields for the selected datasource type only
- `data.rows` is the row-oriented table/tsv view; index output lists
  datasource types, typed output lists payload fields
- typed template output does not repeat global `type` choices; the selected
  value is `data.type` and `resolved.datasource_type`, while full type
  discovery lives in the default index and `dsctl enum list db-type`

## `dsctl template task [TASK_TYPE]`

Returns a compact local task-template catalog when `TASK_TYPE` is omitted.
Returns one task YAML template inside `data.yaml` when `TASK_TYPE` is provided.
`data.rows[]` provides the row-oriented table/tsv view: task-type rows for the
catalog, and line rows for a concrete template.

Options:

- `--variant VARIANT`
- `--raw`

Current stable task template coverage includes every DS 3.4.1 upstream default
task type.

Typed task-params models currently exist for:

- `SHELL`
- `PYTHON`
- `REMOTESHELL`
- `SQL`
- `HTTP`
- `SUB_WORKFLOW`
- `DEPENDENT`
- `SWITCH`
- `CONDITIONS`

The remaining upstream default task types return generic templates with raw
`task_params: {}` placeholders.

Rules:

- omitting `TASK_TYPE` keeps the stable action `template.task` and returns
  `resolved.mode=index`
- task type matching is case-insensitive
- the normalized type is returned as `resolved.task_type`
- `resolved.task_category` reports the upstream DS category
- `resolved.template_kind` is `typed` or `generic`
- `resolved.variant` reports the selected template scenario
- `data.template.variants` lists valid scenarios for the selected task type
- every task template includes commented optional runtime-control fields for
  `flag`, `environment_code`, `task_group_id`, `task_group_priority`,
  `timeout_notify_strategy`, `cpu_quota`, and `memory_max`
- `--raw` prints only the YAML task fragment; it does not print the standard
  success envelope
- `dsctl template task` returns:
  - `data.task_types`
  - `data.typed_task_types`
  - `data.generic_task_types`
  - `data.task_types_by_category`
  - `data.default_task_type`
  - `data.next_command`
  - `data.rows`
- `data.rows[].next_command` points to `dsctl task-type get TYPE`
- detailed per-type fields, variants, state rules, choices, and compile
  mappings live in `dsctl task-type get TYPE` and
  `dsctl task-type schema TYPE`

Stable typed task variants include:

- `SHELL`: `minimal`, `params`, `resource`
- `PYTHON`: `minimal`, `params`, `resource`
- `REMOTESHELL`: `minimal`, `params`, `datasource`
- `SQL`: `minimal`, `params`, `pre-post-statements`
- `HTTP`: `minimal`, `params`, `post-json`
- `SUB_WORKFLOW`: `minimal`, `params`, `child-workflow`
- `DEPENDENT`: `minimal`, `params`, `workflow-dependency`
- `SWITCH`: `minimal`, `params`, `branching`
- `CONDITIONS`: `minimal`, `params`, `condition-routing`

For `SHELL` and `PYTHON`, the `resource` variant uses the DS-native
`task_params.resourceList[].resourceName` field. The `command` shorthand remains
the minimal inline script path and compiles to `taskParams.rawScript` with an
empty `resourceList`.

The `params` variants expose DS-native task dynamic parameter fields:
`task_params.localParams[]` and `task_params.varPool[]`. Parameter entries use
the DS `Property` shape: `prop`, `direct`, `type`, and optional `value`.
`direct` is `IN` or `OUT`; supported types are `VARCHAR`, `INTEGER`, `LONG`,
`FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`, `BOOLEAN`, `LIST`, and `FILE`.
Script-like tasks can emit output parameters through log lines matching
`${setValue(name=value)}` or `#{setValue(name=value)}`.

SQL templates and the SQL typed payload normalizer keep `localParams`,
`varPool`, `preStatements`, and `postStatements` as non-null lists when omitted
or empty, because DS SQL task execution expects list values for those fields.

For end-to-end YAML authoring guidance, see `docs/user/workflow-authoring.md`.

## `dsctl schedule preview`

Previews the next five trigger times for one existing or proposed schedule.

Supported forms:

- `dsctl schedule preview SCHEDULE_ID`
- `dsctl schedule preview --project PROJECT --cron CRON --start START --end END --timezone TZ`

Rules:

- use `dsctl schedule list` inside the selected project to discover schedule ids
- preview by id does not accept `--project`, `--cron`, `--start`, `--end`, or
  `--timezone`
- ad hoc preview requires all of `--cron`, `--start`, `--end`, and
  `--timezone`
- `--cron` must be a DolphinScheduler Quartz cron expression with 6 or 7
  fields and seconds first
- ad hoc preview resolves project selection with `flag > context`
- use `dsctl project list` to discover project names and codes for ad hoc
  `--project`

Successful output returns:

- `data.times`
- `data.count`
- `data.analysis.preview_count`
- `data.analysis.preview_limit`
- `data.analysis.min_interval_seconds`
- `data.analysis.risk_level`
- `data.analysis.risk_type`
- `data.analysis.requires_confirmation`
- `data.analysis.threshold_seconds`
- `data.analysis.reason`

## `dsctl schedule explain`

Explains one schedule create or update mutation without changing remote state.

Supported forms:

- `dsctl schedule explain --workflow WORKFLOW [--project PROJECT] --cron CRON --start START --end END --timezone TZ`
- `dsctl schedule explain SCHEDULE_ID [update options...]`

Rules:

- without `SCHEDULE_ID`, explain models `schedule.create` selection and risk
  rules
- use `dsctl schedule list` inside the selected project to discover schedule ids
  for update-form explain
- use `dsctl project list` and `dsctl workflow list` to discover create-form
  selectors
- use `dsctl alert-group list`, `dsctl worker-group list`,
  `dsctl tenant list`, and `dsctl environment list` to discover optional
  create/update selector values
- `--failure-strategy`, `--warning-type`, and `--priority` use generated DS
  enum values exposed by `dsctl enum list`
- create-form tenant selection:
  `flag > enabled project preference.tenant > current-user tenantCode > "default"`
- create-form omitted `warningType`, `warningGroupId`,
  `workflowInstancePriority`, `workerGroup`, and `environmentCode` may be
  supplied by enabled project preference before CLI built-in defaults
- with `SCHEDULE_ID`, explain models `schedule.update` and merges omitted
  fields from the current remote schedule before preview and risk analysis
- update-form explain also returns `data.currentSchedule`,
  `data.requestedFields`, `data.changedFields`, `data.inheritedFields`, and
  `data.unchangedRequestedFields`
- explain by id does not accept `--workflow`, `--project`, or `--tenant-code`
- explain by id requires at least one field option
- any provided `--cron` must be a DolphinScheduler Quartz cron expression with
  6 or 7 fields and seconds first
- explain never mutates remote state
- `data.confirmation.token` and `data.confirmation.confirmFlag` are `null`
  when confirmation is not required
- when confirmation is required, the returned token matches the later
  `schedule.create` or `schedule.update` mutation with the same effective input

Successful output returns:

- `data.mutationAction`
- `data.proposedSchedule.crontab`
- `data.proposedSchedule.startTime`
- `data.proposedSchedule.endTime`
- `data.proposedSchedule.timezoneId`
- `data.proposedSchedule.failureStrategy`
- `data.proposedSchedule.warningType`
- `data.proposedSchedule.warningGroupId`
- `data.proposedSchedule.workflowInstancePriority`
- `data.proposedSchedule.workerGroup`
- `data.proposedSchedule.tenantCode`
- `data.proposedSchedule.environmentCode`
- `data.currentSchedule` when explain models `schedule.update`
- `data.requestedFields` when explain models `schedule.update`
- `data.changedFields` when explain models `schedule.update`
- `data.inheritedFields` when explain models `schedule.update`
- `data.unchangedRequestedFields` when explain models `schedule.update`
- `data.preview`
- `data.confirmation.required`
- `data.confirmation.nextAction`
- `data.confirmation.token`
- `data.confirmation.confirmFlag`
- `data.confirmation.retryOption`
- `data.confirmation.riskType`
- `data.confirmation.riskLevel`
- `data.confirmation.reason`
- when explain models `schedule.create`, `resolved.tenant` also includes:
  - `value`
  - `source`
- when explain models `schedule.create` and enabled project preference supplied
  any omitted fields, `resolved.project_preference.used_fields` lists those
  destination field names

## `dsctl schedule create`

Creates one schedule bound to a resolved workflow.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- tenant selection:
  `flag > enabled project preference.tenant > current-user tenantCode > "default"`
- use `dsctl project list` and `dsctl workflow list` to discover project and
  workflow selectors
- use `dsctl alert-group list`, `dsctl worker-group list`,
  `dsctl tenant list`, and `dsctl environment list` to discover optional
  selector values
- `--failure-strategy`, `--warning-type`, and `--priority` use generated DS
  enum values exposed by `dsctl enum list`

Required options:

- `--cron`
- `--start`
- `--end`
- `--timezone`

Optional options:

- `--failure-strategy`
- `--warning-type`
- `--warning-group-id`
- `--priority`
- `--worker-group`
- `--tenant-code`
- `--environment-code`
- `--confirm-risk TOKEN`

Rules:

- create returns the created schedule payload
- omitted `warningType`, `warningGroupId`, `workflowInstancePriority`,
  `workerGroup`, `tenantCode`, and `environmentCode` may be supplied by
  enabled project preference before CLI built-in defaults
- `--cron` must be a DolphinScheduler Quartz cron expression with 6 or 7
  fields and seconds first
- the CLI does not expose `releaseState` as a create option; schedule
  activation uses explicit `online` / `offline`
- if the workflow is not online, the CLI returns `invalid_state`
- if the workflow already has a schedule, the CLI returns `conflict`
- if the computed preview interval is below the confirmation threshold, the
  CLI returns `confirmation_required` unless the matching `--confirm-risk`
  token is provided
- when a high-frequency schedule is explicitly confirmed, the success payload
  includes one warning
- when that warning is present, the aligned `warning_details[]` item uses code
  `confirmed_high_frequency_schedule`
- `resolved.tenant` includes:
  - `value`
  - `source`
- when enabled project preference supplied any omitted fields,
  `resolved.project_preference.used_fields` lists those destination field names

## `dsctl schedule update SCHEDULE_ID`

Updates one schedule by numeric id.

Rules:

- `SCHEDULE_ID` is id-first and does not use context
- use `dsctl schedule list` inside the selected project to discover schedule ids
- use `dsctl alert-group list`, `dsctl worker-group list`, and
  `dsctl environment list` to discover optional update selector values
- `--failure-strategy`, `--warning-type`, and `--priority` use generated DS
  enum values exposed by `dsctl enum list`
- omitted fields preserve current remote values
- at least one field change is required
- `--confirm-risk TOKEN` accepts a token previously returned in a
  `confirmation_required` error
- the CLI does not expose `releaseState` as an update option; schedule
  activation uses explicit `online` / `offline`
- updating an online schedule returns `invalid_state`
- if the computed preview interval is below the confirmation threshold, the
  CLI returns `confirmation_required` unless the matching `--confirm-risk`
  token is provided
- when a confirmed high-frequency schedule warning is present, the aligned
  `warning_details[]` item uses code `confirmed_high_frequency_schedule`

## `dsctl schedule delete SCHEDULE_ID --force`

Deletes one schedule by numeric id.

Rules:

- `SCHEDULE_ID` is id-first and does not use context
- use `dsctl schedule list` inside the selected project to discover schedule ids
- `--force` is required
- deleting an online schedule returns `invalid_state`

Successful output returns:

- `data.deleted`
- `data.schedule`

## `dsctl schedule online SCHEDULE_ID`

Brings one schedule online and returns the refreshed schedule payload.

Rules:

- `SCHEDULE_ID` is id-first and does not use context
- use `dsctl schedule list` inside the selected project to discover schedule ids
- the DS 3.4.1 adapter resolves the bound workflow to recover `projectCode`
  for the legacy online endpoint
- bringing a schedule online requires the bound workflow to already be online

## `dsctl schedule offline SCHEDULE_ID`

Brings one schedule offline and returns the refreshed schedule payload.

Rules:

- `SCHEDULE_ID` is id-first and does not use context
- use `dsctl schedule list` inside the selected project to discover schedule ids
- the DS 3.4.1 adapter resolves the bound workflow to recover `projectCode`
  for the legacy offline endpoint

## `dsctl task list`

Lists tasks inside one resolved workflow.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

The `data` payload is a JSON array of task summaries:

- `code`
- `name`
- `version`

## `dsctl task get`

Fetches one task definition inside one resolved workflow.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- task is resolved by name or numeric code within the selected workflow
- use `dsctl task list` inside the selected workflow to discover task names and
  codes

## `dsctl task update`

Updates one task definition inside one workflow using inline `--set` mutations.

Options:

- positional `TASK` accepts task name or numeric code
- `--project PROJECT`
- `--workflow WORKFLOW`
- `--set KEY=VALUE` (repeatable, required)
- `--dry-run`

Current stable `--set` keys:

- `description`
- `command`
- `flag`
- `worker_group`
- `environment_code`
- `priority`
- `retry.times`
- `retry.interval`
- `timeout`
- `timeout_notify_strategy`
- `delay`
- `task_group_id`
- `task_group_priority`
- `cpu_quota`
- `memory_max`
- `depends_on`

Rules:

- selection precedence matches `task get`
- use `dsctl task list` inside the selected workflow to discover task names and
  codes
- use `dsctl schema --command task.update` to discover supported `--set` keys,
  examples, and machine-readable metadata
- the CLI compiles the update into the DS native
  `updateTaskWithUpstream` form request
- `command` updates are supported only for `SHELL`, `PYTHON`, and
  `REMOTESHELL`
- `flag` accepts `YES` or `NO`
- `depends_on` accepts either a YAML list value or a comma-separated task-name
  string
- `worker_group`, `environment_code`, `task_group_id`, `cpu_quota`, and
  `memory_max` accept an empty value to reset back to the DS default
- setting `task_group_priority` requires an effective `task_group_id`
- setting `timeout_notify_strategy` requires an effective timeout greater than
  `0`
- when `timeout` changes from closed to open and no strategy is provided, the
  CLI uses DS's default `WARN` notify strategy
- task rename and task-type changes remain `workflow edit` operations, not
  inline task updates
- `--dry-run` returns the native request plus:
  - `data.updated_fields`
  - `data.no_change`
- applying a no-op update returns the current task payload with one warning and
  sends no request
- when that warning is present, the aligned `warning_details[]` item uses code
  `task_update_no_persistent_change`
- if DS reports that the task state does not support modification, the CLI
  returns `invalid_state`

## `dsctl workflow run`

Triggers one workflow definition and returns the created workflow instance ids.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors
- use `dsctl worker-group list`, `dsctl tenant list`,
  `dsctl alert-group list`, and `dsctl environment list` to discover optional
  runtime selectors
- worker group selection:
  `flag > enabled project preference.workerGroup > "default"`
- tenant selection:
  `flag > enabled project preference.tenant > "default"`
- runtime option defaults mirror the DS 3.4.1 UI start modal:
  `failureStrategy=CONTINUE`, `warningType=NONE`,
  `workflowInstancePriority=MEDIUM`, `dryRun=0`, and omitted
  `warningGroupId`, `environmentCode`, and `startParams`
- project preference overrides are used for `taskPriority`, `warningType`,
  `alertGroups`/`alertGroup`, `workerGroup`, `tenant`, and `environmentCode`
- `--dry-run` is a local CLI preview: it resolves inputs and emits the native
  `start-workflow-instance` request without sending that start request
- `--execution-dry-run` sends DS `dryRun=1`: DolphinScheduler creates dry-run
  workflow/task instances and skips task plugin trigger execution

The `data` payload is a JSON object:

- `workflowInstanceIds`

The `resolved` payload also includes:

- `project`
- `workflow`
- `worker_group.value`
- `worker_group.source`
- `tenant.value`
- `tenant.source`
- `failure_strategy`
- `warning_type`
- `workflow_instance_priority`
- `warning_group_id`
- `environment_code`
- `start_params.names`
- `start_params.count`
- `execution_dry_run`

Options:

- `--failure-strategy continue|end`, default `continue`
- `--priority highest|high|medium|low|lowest`, default `medium`
- `--warning-type none|success|failure|all`, default `none`
- `--warning-group-id ID`
- `--environment-code CODE`
- `--param KEY=VALUE`, repeatable, serialized to DS `startParams`
- `--dry-run`
- `--execution-dry-run`

## `dsctl workflow run-task`

Starts one workflow definition from a selected task and returns the created
workflow instance ids. This uses DolphinScheduler's workflow trigger endpoint
with `startNodeList` set to the selected task code and `taskDependType` mapped
from `--scope`.

Selection rules:

- project selection: `flag > context`
- workflow selection: `argument > context`
- task selection: `--task` name or code within the workflow definition
- use `dsctl project list`, `dsctl workflow list`, and `dsctl task list` to
  discover selectors
- worker group selection:
  `flag > enabled project preference.workerGroup > "default"`
- tenant selection:
  `flag > enabled project preference.tenant > "default"`
- runtime option defaults and project preference overrides match
  `dsctl workflow run`

Options:

- `--task TASK` is required
- `--scope self|pre|post`, default `self`
- the runtime options from `dsctl workflow run` are also supported, including
  `--dry-run`, `--execution-dry-run`, and repeatable `--param KEY=VALUE`

Scope mapping:

- `self` → DS `TASK_ONLY`
- `pre` → DS `TASK_PRE`
- `post` → DS `TASK_POST`

The `data` payload is a JSON object:

- `workflowInstanceIds`

The `resolved` payload also includes:

- `project`
- `workflow`
- `task`
- `scope`
- `worker_group.value`
- `worker_group.source`
- `tenant.value`
- `tenant.source`

Rules:

- the command emits a warning because downstream `DEPENDENT` tasks resolve
  dependency state from workflow/task instances in their dependency date
  interval; if the referenced task, whole workflow, or scheduled dependency
  instance has not produced a successful run, the downstream dependency can
  wait or fail
- the aligned `warning_details[]` item uses code
  `workflow_run_task_dependent_context`

## `dsctl workflow backfill`

Backfills one workflow definition and returns the created workflow instance ids.
This uses DolphinScheduler's workflow trigger endpoint with
`execType=COMPLEMENT_DATA`.

Selection rules:

- project selection: `flag > context`
- workflow selection: `argument > context`
- optional task selection: `--task` name or code within the workflow definition
- use `dsctl project list`, `dsctl workflow list`, and `dsctl task list` to
  discover selectors
- use `dsctl worker-group list`, `dsctl tenant list`,
  `dsctl alert-group list`, and `dsctl environment list` to discover optional
  runtime selectors
- worker group, tenant, warning, priority, environment, start params, and
  execution dry-run rules match `dsctl workflow run`

Backfill time selection:

- pass both `--start START` and `--end END` for a range, serialized as DS
  `complementStartDate` and `complementEndDate`
- or repeat `--date DATE` for explicit complement schedule dates, serialized
  as DS `complementScheduleDateList`
- `--date` is mutually exclusive with `--start` / `--end`

Options:

- `--task TASK`
- `--scope self|pre|post`, default `self`, applied when `--task` is set
- `--run-mode serial|parallel`, default `serial`
- `--expected-parallelism-number N`, default `2`
- `--complement-dependent-mode off|all`, default `off`
- `--all-level-dependent`
- `--execution-order desc|asc`, default `desc`
- the runtime options from `dsctl workflow run` are also supported, including
  `--dry-run`, `--execution-dry-run`, and repeatable `--param KEY=VALUE`

The `data` payload is a JSON object:

- `workflowInstanceIds`

The `resolved` payload also includes the `workflow run` resolved fields plus:

- `backfill.schedule_time_mode`
- `backfill.run_mode`
- `backfill.expected_parallelism_number`
- `backfill.complement_dependent_mode`
- `backfill.all_level_dependent`
- `backfill.execution_order`
- `task` and `scope` when `--task` is set

## `dsctl workflow online`

Brings one workflow definition online and returns the refreshed workflow
payload.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- if the workflow references sub-workflows, they must already be online
- if an attached schedule exists but remains offline, the command succeeds and
  adds one warning reminding the caller that `schedule online` is still
  required; the aligned `warning_details[]` item uses code
  `workflow_online_leaves_schedule_offline`

## `dsctl workflow offline`

Brings one workflow definition offline and returns the refreshed workflow
payload.

Selection rules:

- project selection: `flag > context`
- workflow selection: `flag > context`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- workflow offline is idempotent
- if the workflow currently has an online attached schedule, the command
  succeeds and adds one warning because DS also forces that schedule offline;
  the aligned `warning_details[]` item uses code
  `workflow_offline_also_offlines_schedule`

## `dsctl workflow-instance list`

Lists workflow instances using explicit runtime filters.

Options:

- `--page-no N`
- `--page-size N`
- `--all`
- `--project TEXT`
- `--workflow TEXT`
- `--search TEXT`
- `--executor TEXT`
- `--host TEXT`
- `--start TEXT`
- `--end TEXT`
- `--state TEXT`

Selection rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- discover `--project` values with `dsctl project list`
- discover `--workflow` values with `dsctl workflow list`
- discover `--state` values with
  `dsctl enum list workflow-execution-status`
- without `--project`, the CLI uses the DS v2 workflow-instance list API and
  supports global `--workflow`, `--host`, `--start`, `--end`, and `--state`
  filters
- with `--project`, the CLI resolves the project code and uses the
  project-scoped DS workflow-instance list API; `--workflow` is then resolved
  as a workflow definition name or code inside that project
- `--search` filters workflow-instance names through upstream `searchVal` and
  requires `--project`
- `--executor` filters by exact executor user name and requires `--project`
- `--host` filters by upstream host substring
- `--start` and `--end` filter workflow-instance `start_time` using DS datetime
  format `YYYY-MM-DD HH:MM:SS`; both are optional, and when both are present
  `--end` must be greater than or equal to `--start`
- `--state` accepts DS workflow execution status names such as
  `RUNNING_EXECUTION` and `SUCCESS`
- with `--all`, the CLI fetches remaining pages up to the standard safety limit
  and materializes one DS-style page payload

The `data` payload is a DS-style paging object whose `totalList` items expose
the current stable workflow-instance projection:

- `id`
- `workflowDefinitionCode`
- `workflowDefinitionVersion`
- `projectCode`
- `state`
- `recovery`
- `startTime`
- `endTime`
- `runTimes`
- `name`
- `host`
- `commandType`
- `taskDependType`
- `failureStrategy`
- `warningType`
- `scheduleTime`
- `executorId`
- `executorName`
- `tenantCode`
- `queue`
- `duration`
- `workflowInstancePriority`
- `workerGroup`
- `environmentCode`
- `timeout`
- `dryRun`
- `restartTime`

## `dsctl workflow-instance get`

Fetches one workflow instance by id.

Selection rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`

## `dsctl workflow-instance parent`

Returns the parent workflow instance for one sub-workflow instance.

Selection rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover sub-workflow instance ids with `dsctl workflow-instance list`
- the CLI first fetches the sub-workflow instance payload, then recovers its
  owning `projectCode` for the DS 3.4.1 relation endpoint
- the workflow instance must itself be a DS sub-workflow instance
- `resolved` includes `subWorkflowInstance`

The `data` payload is a JSON object:

- `parentWorkflowInstance`

## `dsctl workflow-instance digest`

Returns one compact runtime digest for a workflow instance.

Selection rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- the CLI fetches the owning workflow-instance payload, then auto-exhausts the
  task-instance list for that workflow instance inside the standard page safety
  limit

Current `data` fields:

- `workflowInstance`
- `taskCount`
- `taskStateCounts`
- `taskTypeCounts`
- `progress`
- `runningTasks`
- `queuedTasks`
- `failedTasks`
- `retriedTasks`

Current guarantees:

- `workflowInstance` reuses the stable `workflow-instance get` projection
- `taskStateCounts` preserves exact DS task execution status names
- `progress` is a CLI summary derived from task states and includes
  `running`, `queued`, `paused`, `failed`, `success`, `other`, `finished`,
  and `active`
- highlighted task lists are compact task-instance views rather than the full
  `task-instance get` payload

## `dsctl workflow-instance edit`

Edits one finished workflow instance DAG from a minimal YAML patch and then
returns the refreshed workflow-instance payload.

Options:

- `--patch PATH` (required)
- `--sync-definition`
- `--dry-run`

Rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- the workflow instance must already be in one DS final state
- the CLI requires `dagData` from the workflow-instance payload and rebuilds a
  live workflow spec snapshot from that instance DAG before applying the patch
- start new patch files with `dsctl template workflow-instance-patch --raw`
- patch grammar reuses the same stable task patch operations as `workflow edit`:
  `patch.tasks.create`, `patch.tasks.update`, `patch.tasks.rename`, and
  `patch.tasks.delete`
- current stable `patch.workflow.set` support is intentionally narrower than
  `workflow edit`; only `global_params` and `timeout` are accepted for
  workflow-instance edits
- definition-only workflow fields such as `name`, `description`,
  `execution_type`, and `release_state` are rejected as `user_input`
- `--sync-definition` forwards DS `syncDefine=true` so the saved DAG is also
  synchronized back to the current workflow definition
- without `--sync-definition`, the CLI still edits the finished workflow
  instance DAG but does not request current-definition synchronization
- `resolved` includes `workflowInstance`, `project`, `workflow`, `patch_file`,
  and `syncDefine`
- `--dry-run` returns the compiled DS form payload plus `diff`, `no_change`,
  and `syncDefine`
- applying a no-op patch returns the current workflow-instance payload, emits
  one warning, and the aligned `warning_details[]` item uses code
  `workflow_instance_edit_no_persistent_change`

## `dsctl workflow-instance stop`

Requests stop for one workflow instance and then returns the refreshed
workflow-instance payload.

Rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- the CLI checks the current DS workflow execution status before sending the
  stop request
- states that are not stoppable return `invalid_state`
- if DS accepts the stop request but the refreshed state is not yet `STOP`,
  the command succeeds and adds a warning describing the current state; the
  aligned `warning_details[]` item uses code
  `workflow_instance_action_state_after_request` with `action="stop"` and
  `target_state="STOP"`

## `dsctl workflow-instance watch`

Polls one workflow instance until it reaches a final DS execution state.

Options:

- `--interval-seconds N`
- `--timeout-seconds N`

Rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- default polling interval is `5` seconds
- default timeout is `600` seconds
- `--timeout-seconds 0` means wait indefinitely
- timeout returns a structured `timeout` error with the last observed state
- success returns the same workflow-instance projection as `workflow-instance get`

## `dsctl workflow-instance rerun`

Requests rerun for one finished workflow instance and then returns the refreshed
workflow-instance payload.

Rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- the workflow instance must already be in one DS final state
- if DS accepts the request but the refreshed state is still final, the command
  succeeds and adds a warning describing the current state; the aligned
  `warning_details[]` item uses code
  `workflow_instance_action_state_after_request` with `action="rerun"` and
  `expect_non_final=true`

## `dsctl workflow-instance recover-failed`

Requests recovery from failed tasks for one workflow instance and then returns
the refreshed workflow-instance payload.

Rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- the workflow instance must currently be in DS `FAILURE`
- if DS accepts the request but the refreshed state is still final, the command
  succeeds and adds a warning describing the current state; the aligned
  `warning_details[]` item uses code
  `workflow_instance_action_state_after_request` with
  `action="recover-failed"` and `expect_non_final=true`

## `dsctl workflow-instance execute-task`

Requests execution of one task inside one finished workflow instance and then
returns the refreshed workflow-instance payload.

Options:

- `--task NAME_OR_CODE`
- `--scope self|pre|post`

Rules:

- workflow-instance resources are id-first and do not consume project/workflow
  context
- discover workflow-instance ids with `dsctl workflow-instance list`
- discover `--task` values with
  `dsctl task-instance list --workflow-instance WORKFLOW_INSTANCE`
- the workflow instance must already be in one DS final state
- the CLI resolves `--task` against the owning workflow definition recovered
  from the workflow instance payload
- `--scope self|pre|post` maps to DS
  `TASK_ONLY|TASK_PRE|TASK_POST`
- `resolved` includes `workflowInstance`, `task`, and `scope`
- if DS accepts the request but the refreshed state is still final, the command
  succeeds and adds a warning describing the current state; the aligned
  `warning_details[]` item uses code
  `workflow_instance_action_state_after_request` with
  `action="execute-task"` and `expect_non_final=true`

## `dsctl task-instance list`

Lists task instances through the project-scoped DS task-instance paging query.

Options:

- `--workflow-instance ID`
- `--project TEXT`
- `--workflow-instance-name TEXT`
- `--page-no N`
- `--page-size N`
- `--all`
- `--search TEXT`
- `--task TEXT`
- `--task-code N`
- `--executor TEXT`
- `--state TEXT`
- `--host TEXT`
- `--start TEXT`
- `--end TEXT`
- `--execute-type TEXT`

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- discover `--project` values with `dsctl project list`
- discover `--task-code` values with `dsctl task list`
- discover `--state` values with `dsctl enum list task-execution-status`
- discover `--execute-type` values with
  `dsctl enum list task-execute-type`
- `--workflow-instance` narrows the project-scoped query to one workflow
  instance; when it is omitted, pass `--project` or set project context
- when `--workflow-instance` is present, the CLI resolves the owning project
  from the workflow instance; an explicit `--project` must match that project
- workflow-definition filtering is not part of the stable `task-instance list`
  contract for DS 3.4.1 because the upstream BATCH task-instance paging query
  does not reliably apply `workflowDefinitionName`; use
  `workflow-instance list --workflow ...` first, then pass the returned
  workflow-instance id to `task-instance list --workflow-instance`
- `--workflow-instance-name` filters by the upstream workflow-instance name
- `--state` accepts DS task execution status names such as
  `RUNNING_EXECUTION` and `SUCCESS`
- `--execute-type` accepts DS task execute type names such as `BATCH` and
  `STREAM`
- `--search` is the upstream free-text `searchVal` filter; use `--task` for an
  exact task instance name filter
- `--start` and `--end` filter task start time using DS datetime format
  `YYYY-MM-DD HH:MM:SS`; both are optional, and when both are present `--end`
  must be greater than or equal to `--start`
- with `--all`, the CLI fetches remaining pages up to the standard safety limit
  and materializes one DS-style page payload

The `data` payload is a DS-style paging object whose `totalList` items expose
the current stable task-instance projection:

- `id`
- `name`
- `taskType`
- `workflowInstanceId`
- `workflowInstanceName`
- `projectCode`
- `taskCode`
- `taskDefinitionVersion`
- `processDefinitionName`
- `state`
- `firstSubmitTime`
- `submitTime`
- `startTime`
- `endTime`
- `host`
- `logPath`
- `retryTimes`
- `duration`
- `executorName`
- `workerGroup`
- `environmentCode`
- `delayTime`
- `taskParams`
- `dryRun`
- `taskGroupId`
- `taskExecuteType`

## `dsctl task-instance get`

Fetches one task instance by id within one workflow instance.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- `--workflow-instance` is required because the DS 3.4.1 direct get endpoint is
  still project-scoped and the CLI recovers `projectCode` from the owning
  workflow instance

## `dsctl task-instance watch`

Polls one task instance until it reaches a finished DS task execution state.

Options:

- `--workflow-instance ID`
- `--interval-seconds N`
- `--timeout-seconds N`

Rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- `--workflow-instance` is required because the DS 3.4.1 direct get endpoint is
  still project-scoped and the CLI recovers `projectCode` from the owning
  workflow instance
- default polling interval is `5` seconds
- default timeout is `600` seconds
- `--timeout-seconds 0` means wait indefinitely
- timeout returns a structured `timeout` error with the last observed task state
- success returns the same task-instance projection as `task-instance get`
- finished states follow DS `TaskExecutionStatus.isFinished()`: `SUCCESS`,
  `FORCED_SUCCESS`, `KILL`, `FAILURE`, `NEED_FAULT_TOLERANCE`, and `PAUSE`

## `dsctl task-instance sub-workflow`

Returns the child workflow instance for one `SUB_WORKFLOW` task instance.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- `--workflow-instance` is required because the DS 3.4.1 relation endpoint is
  still project-scoped and the CLI recovers `projectCode` from the owning
  workflow instance
- the task instance must belong to the supplied workflow instance
- the task instance must be one DS `SUB_WORKFLOW` task instance
- `resolved` includes `workflowInstance` and `taskInstance`

The `data` payload is a JSON object:

- `subWorkflowInstanceId`

## `dsctl task-instance log`

Fetches the tail of one task-instance log.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- `--workflow-instance` is not required because the DS logger API reads log
  chunks by task-instance id
- `--tail` means “keep the last N lines” and is implemented by chunking the DS
  logger API until exhaustion
- `--raw` prints only `data.text`; errors still use the standard structured
  error envelope
- DS result code `10103` for an empty task log path is translated to stable
  error type `task_not_dispatched`

The `data` payload is a JSON object:

- `text`
- `lineCount`

## `dsctl task-instance force-success`

Forces one failed task instance into `FORCED_SUCCESS`.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- `--workflow-instance` is required because the DS 3.4.1 mutation endpoint is
  still project-scoped and the CLI recovers `projectCode` from the owning
  workflow instance
- the owning workflow instance must already be in one final state
- the task instance itself must currently be in `FAILURE`,
  `NEED_FAULT_TOLERANCE`, or `KILL`

The `data` payload is the refreshed task-instance projection.

## `dsctl task-instance savepoint`

Requests one savepoint for a running task instance.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- `--workflow-instance` is required because the DS 3.4.1 mutation endpoint is
  still project-scoped and the CLI recovers `projectCode` from the owning
  workflow instance
- the task instance must not already be in one finished state

The `data` payload is a JSON object:

- `requested`
- `taskInstance`

## `dsctl task-instance stop`

Requests stop for one task instance.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with `dsctl task-instance list`
- discover `--workflow-instance` values with `dsctl workflow-instance list`
- `--workflow-instance` is required because the DS 3.4.1 mutation endpoint is
  still project-scoped and the CLI recovers `projectCode` from the owning
  workflow instance
- the task instance must not already be in one finished state

The `data` payload is a JSON object:

- `requested`
- `taskInstance`

## Out of Scope

Everything else remains provisional until it lands in code and is added here.
