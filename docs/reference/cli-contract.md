# CLI Contract

## Status

This document describes the current stable CLI surface. If a command or field
is not described here, treat it as roadmap work rather than contract.

The installed surface contains 181 stable actions. Selecting one of the 37
exact DolphinScheduler profiles from `1.3.9` through `3.4.3` does not change
that command tree; it binds an exact action catalog and execution recipes. Every
action/version coordinate is terminal as supported, limited by upstream, or
absent upstream. Terminal compatibility, executable support, verification
evidence, and whole-profile promotion are separate facts. The
[support policy and verification guide](../user/version-compatibility.md#support-policy-and-verification)
explains the release labels and artifact-bound evidence scopes.

Current stable commands:

- global option `--context NAME`
- global option `--env-file PATH`
- global option `--format {json,json-compact,table,tsv}`
- global option `--columns FIELDS`
- `dsctl version`
- `dsctl context`
- `dsctl context create|update|list|get|delete`
- `dsctl config get|set|unset KEY` (`KEY` is `default-context`; `set` also takes a context name)
- `dsctl doctor`
- `dsctl schema`
- `dsctl capabilities`
- `dsctl enum names`
- `dsctl enum list ENUM`
- `dsctl lint workflow|workflow-patch|workflow-instance-patch FILE`
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
- `dsctl project list|get|create|update|delete`
- `dsctl project-parameter list|get|create|update|delete`
- `dsctl project-preference get|update|enable|disable`
- `dsctl project-worker-group list|set|clear`
- `dsctl schedule list|get|preview|explain|create|update|delete|online|offline`
- `dsctl template workflow|workflow-patch|workflow-instance-patch|params|environment|cluster|datasource|task`
- `dsctl workflow list|get|export|describe|digest|create|edit|online|offline|run|run-task|backfill|delete`
- `dsctl workflow lineage list|get|dependent-tasks`
- `dsctl workflow-instance list|get|export|parent|digest|edit|watch|stop|rerun|recover-failed|execute-task`
- `dsctl task list|get|update`
- `dsctl task-instance list|get|watch|sub-workflow|log|force-success|savepoint|stop`

## Discovery Routing

Concrete discovery commands returned by schema, capabilities, task metadata and
enum metadata retain the selected context or env file, with shell-safe quoting.
Static help links and explicitly unbound command patterns remain generic.

`dsctl --help` is the self-contained first-touch routing surface. Agents should
inspect only the command they will execute next, using its leaf help or
`dsctl schema --command ACTION`. When the action is unknown, they inspect one
relevant group rather than preloading unrelated groups or downstream lifecycle
actions. The root schema index is for discovering an unknown group.

`dsctl capabilities` answers feature and version questions. It is not required
to construct syntax for a known command. Use `--action ACTION` when the
question is whether one known action is available on the selected server; use a
bounded section for broader product inventory.

## Naming and Selection Rules

Names are opaque strings.

Rules:

- do not infer structure from delimiters inside names
- positional selectors mean raw names unless the command explicitly documents a
  native numeric identity shortcut or a DS-native path selector
- `resource` is path-first and consumes the DS `fullName` path directly
- DS codes and ids remain the underlying stable server-side identity
- project numeric selectors use `id` on DS `1.3.9` and `code` on newer
  releases; `project list` exposes the selected release's native identity.
  Cross-version project selectors use schema type `name_or_native_identity`
- project selection precedence is `flag > selected named context`; commands
  accepting project-bearing files document the file position separately
- workflow selection is explicit; there is no persistent workflow default
- positional `WORKFLOW` is parser-required for workflow `get`, `export`,
  `describe`, `digest`, `edit`, `online`, `offline`, `run`, `run-task`,
  `backfill`, `delete`, `lineage get`, and `lineage dependent-tasks`
- `--workflow WORKFLOW` is parser-required for task `list`, `get`, and `update`,
  and for `schedule create`; a missing required selector fails with a usage
  error before configuration resolution or network access
- `schedule explain` is intentionally conditional: create form requires
  `--workflow` when `SCHEDULE_ID` is absent, while update form takes the id and
  rejects `--workflow`; list and runtime filter options remain optional

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
dsctl workflow-instance get 901 --project etl-prod
dsctl task-instance get 902 --project etl-prod --workflow-instance 901
```

## Global Option

The profile and output options below may appear before or after the
command path. Root help explains this placement and the JSON versus field-view
tradeoff once. Leaf help keeps native arguments and options prominent, adds a
compact `Global options` panel from these canonical definitions, and ends with
`Schema: dsctl schema --command ACTION`. Applicable list and dry-run commands
also retain their pagination and prepared-effect hints. Framework options
`-h`/`--help`, `-v`/`--version`, `--show-completion SHELL` and
`--install-completion SHELL` are separate from that catalog. `-v`/`--version` eagerly
prints the installed CLI version as plain text, independently of display options,
without configuration or network access;
`version` remains the detailed profile report. Completion requires an explicit
shell (`bash`, `zsh`, `fish`, `powershell` or `pwsh`). Show writes a raw script;
install updates that shell's local configuration. Completion uses only the
static command tree. These framework options add no stable action or JSON
envelope.

### Target selectors

`--context NAME` selects a saved named context. `--env-file PATH` loads an
isolated dotenv profile for one invocation and supplies no saved project.
Both are global options and are mutually exclusive. An explicit selector
supersedes process selectors, including conflicting inherited selectors.

Without an explicit selector, use the first applicable source:

1. Process `DSCTL_CONTEXT` or `DSCTL_ENV_FILE`; presence of both is an error.
2. Process identity values when `DS_API_URL`, `DS_API_TOKEN` or `DS_VERSION`
   is present, even empty.
3. The user registry's `default-context`.
4. Unconfigured local operation.

A selected source supplies the URL, token and version. No missing identity is merged
from another source; selection, file, binding or completeness errors never
trigger fallback. Version-only environment input is valid for offline work,
but remote actions require a complete URL and token.

Supported profile keys are `DS_VERSION`, `DS_API_URL`, `DS_API_TOKEN`,
`DS_API_RETRY_ATTEMPTS`, `DS_API_RETRY_BACKOFF_MS` and `DS_API_TIMEOUT_SECONDS`.
Execution policies resolve per key from process, selected profile, then built-in
default; setting policy alone never selects a different connection. Defaults
are 3 attempts, 200 ms initial backoff and 10 seconds per request. Timeout must
be positive and finite. Version discovery also respects its 45-second total
budget. Resource and runtime-default keys are rejected; use a named context's
`--project` and `project-preference update`, respectively. An explicit file
ignores inherited URL, token and version. See configuration for examples.

Example:

```bash
dsctl --context production context
dsctl --env-file cluster.env context
```

No current-directory, parent-directory, Git-root or legacy context-file search
participates in source selection. See [Configuration](../user/configuration.md)
for setup and migration.

### `--format {json,json-compact,table,tsv}`

Controls display rendering. The default is `json`.

Migration: use `--format` in new versions; `--output-format` is no longer
accepted and has no compatibility alias. `resource download --output PATH`
continues to select the downloaded file’s destination.

Rules:

- `json` returns the standard JSON envelope and remains the stable machine
  contract; when `--columns` is present, only the command data payload at the
  canonical row/object path is projected
- `json-compact` encodes explicitly declared business row collections as shared
  `columns` and positional `rows`; other views keep their structures with
  compact whitespace. See [JSON layout](#json-layout)
- JSON is encoded as UTF-8 and ordinary non-ASCII text is not escaped
- `table` renders row data as a plain text table and nested detail objects as
  labeled sections; padding follows terminal display width, including CJK
- `tsv` renders the same row model as tab-separated text for shell pipelines
- table escapes control characters so a value cannot create a new displayed
  row or terminal escape sequence. TSV replaces tabs, CR and LF inside cells
  with spaces and escapes other controls; it is intentionally lossy. Nested
  values in TSV cells are compact JSON strings, and null becomes an empty cell. Use JSON
  when preserving types, null/empty distinctions or original text matters
- table and TSV use each command's `data_shape` metadata when present and fall
  back to runtime shape inference for simple list payloads. Compact JSON uses
  only explicit business-list metadata, without runtime eligibility inference
- table and TSV write only row data to stdout; partial/non-first-page summaries
  and warnings are written to stderr
- global options may appear before or after the command path; generated commands
  use the canonical prefix form, for example:

```bash
dsctl --format table workflow-instance list --project etl-prod
```

### `--columns FIELDS`

Selects row/object fields. For JSON, this narrows the standard envelope data
payload at the command's canonical row/object path. For `table` and `tsv`, it
selects rendered display columns. Compact business lists accept top-level
field names and `*`; other formats and non-tabular views retain dotted paths.

Rules:

- table/TSV columns and compact-list `columns` keep explicit request order;
  default compact-list columns use deterministic lexicographic order. JSON
  object members are encoded in sorted order and their order carries no meaning
- compact business lists reject dotted field selectors, including literal
  dotted keys, with `user_input_error` before service execution; select a
  parent field or use `json`. Nested values remain whole JSON values in the
  selected parent column
- elsewhere, dotted paths traverse objects and keep nesting in JSON; literal
  dotted keys take precedence. Array indexing is not supported. Missing fields
  in individual object rows remain absent in JSON and blank in text views
- `--columns '*'` selects all top-level row fields; quote `*` so the shell does
  not expand it as a filesystem glob
- unknown columns are a `user_input_error` when rows are available to validate
  against
- errors are never projected; failed commands keep the full structured error
  payload
- schema command views derive field rows before selecting columns. YAML
  templates keep their body once in `data.yaml`; their `line_source_path`
  declares a virtual line view. Table/TSV and explicit JSON `--columns`
  materialize `line_no,line` at the documented `row_path` (`data.rows` for a
  task, `data.lines` for workflow/patch templates). Explicit JSON line
  projection replaces the YAML body; unprojected JSON contains no duplicate
  line array. Raw YAML retains its body unchanged

Example:

```bash
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --format tsv --columns id,name,state,host task-instance list --project etl-prod --workflow-instance 901
dsctl --format tsv --columns '*' task-instance list --project etl-prod --workflow-instance 901
```

### JSON layout

`json` retains the standard object representation. `json-compact` combines
compact whitespace with shared column names for explicitly declared regular
business lists. The format is explicit and independent of redirection. The
former independent `--compact` option is removed; use `--format json-compact`.

For a business collection with `data_shape.compact_rows: true`, the renderer
replaces the object array at its existing `row_path`. This includes regular
`page`/`collection` views and three declared collections inside `summary`
views: `alert-plugin.definition.list` at `data.definitions`, `task-type.list`
at `data.taskTypes`, and `workflow.lineage.list` at
`data.workFlowRelationDetailList`. Sibling data remains unchanged.
The encoded collection has this shape:

```json
{"columns":["id","name","state"],"rows":[
[901,"daily-etl","SUCCESS"],
[902,"hourly-etl","FAILURE"]
]}
```

Decode each row by pairing its values with the column names at the same
positions. Every value array has the same length as `columns`. Row order and
native JSON values are retained: nested objects and arrays stay in their cells,
numbers stay numbers, and null stays null. Nested arrays are not recursively
encoded as tables. Each value array occupies one output line; compact output
does not promise to remove every newline.

The declared business-list contract owns its fields, including supported null
values. Field sets can differ between exact DolphinScheduler versions; a
field absent from the selected native recipe is not fabricated. The encoding
has no `missing` field, sparse-row bitmap, size threshold or data-dependent
fallback to object arrays. Empty, single-row and multi-row collections use the
same container. An empty unprojected list is `{"columns":[],"rows":[]}`;
explicit top-level columns can retain the requested header on empty results.

Without `--columns`, all returned fields are retained and column names are
sorted lexicographically. Table/TSV `default_columns` never remove compact JSON
fields. Explicit `--columns` determines the header order and accepts top-level
names or `*`. For dotted paths, select the whole parent field or use `json`.

Details, schemas, templates, diagnostics, documents and summaries without a
declared compact collection retain their object/document structures with
compact whitespace. Their existing explicit projection semantics remain
available. Undeclared row-like structures are not
inferred to be compact business lists. Errors retain the complete structured
payload. A broken declared row path or row shape produces
`output_contract_error`; use `--format json` to inspect the original fields.
It does not silently switch to object rows or classify the result as invalid
user input. Successful raw YAML and log bodies remain unchanged by either format.

Both JSON formats use UTF-8 and preserve envelope metadata, pagination,
resolved selections, warnings and navigation. `action_index.scope` and
`target.field` address the decoded logical collection and field, so
`targets: "all"` still means all returned rows. Encoding does not change index
coverage or remote pagination completeness.

Bound information with filters, columns and pagination before choosing its
encoding:

```bash
dsctl --format json-compact --columns id,name,state \
  task-instance list --project etl-prod --workflow-instance 901 --page-size 10
```

## Output Envelope

### Prepared mutation views

Dry-run preparation retains exactly one ordered `data.requests` array, including
single-request plans. The former `data.request` first-item alias is removed.
Default CLI output displays the semantic changes, constraints and impacts
provided by the service, plus `execution_order` (prepared method/path pairs),
the resolved target/effective inputs and an audit expansion hint. It omits raw
REST bodies. `--columns requests` selects the original request plan;
`--columns '*'` includes it with the other data. No additional CLI flag or
alternate preparation path is involved.

The prepared sequence excludes lookup/verification reads and later runtime
effects. A no-change preview has an empty mutation sequence. Apply prepares
against the state it observes at that later invocation; a dry-run neither locks
remote state nor guarantees an identical baseline. CLI `--dry-run` sends no
mutation; DS `--execution-dry-run` is a runtime execution setting and can create
instances. Successful acceptance of a backfill does not prove completion of
every requested date.

### Mutation review and confirmation

Workflow authoring and execution commands, workflow-instance edits and task
updates provide `--dry-run` because their compiled payloads or ordered stages
benefit from review. An unchanged edit reports `no_change: true`, empty
`requests` and empty `execution_order`, matching the apply path's lack of writes.

Direct resource setters and runtime controls apply the explicitly selected
operation and values immediately. Inspect their current state with the related
read command and use the action schema's `effects` to identify remote writes.
Deletion and clearing the project worker group require `--force`; workflow
authoring and schedule changes use `--confirm-risk` for their documented risks.
`schedule explain` prepares the create or update and its confirmation token.
An alert-plugin test sends a real notification and is a write even though its
name contains `test`.

Local context/configuration writes are serialized and atomic. Saving a default
reports both the saved value and the effective selection. Remote multi-stage
failures retain completed stages, resource identities and one recovery hint so
the caller can continue the unfinished work without repeating completed writes.

### Standard envelope

Every stable command returns the standard JSON envelope from `src/dsctl/output.py`.
This statement applies to both `json` and `json-compact`. Explicit
`--columns` projection keeps the envelope and narrows only the command `data`
payload. Row-oriented display formats are an alternate rendering layer over the
same command result.

Process-channel guarantees:

- successful standard results and raw artifacts are written to stdout
- structured command errors are written to stderr and exit with status 1;
  failed diagnostic or preview commands retain their complete report in `data`
- parser usage errors use the same structured JSON error envelope and exit
  with status 2; table/TSV errors use `Error:` and `Hint:` text on stderr
- SIGINT during a command exits with status 130 without a traceback
- an early-closed stdout pipe exits with status 1 without a traceback or
  additional diagnostics
- JSON keeps warnings and page metadata inside the envelope without duplicating
  them to stderr
- applicable successful JSON results may include bounded `next_actions` or an
  `action_index`; JSON `--columns` projection keeps these envelope fields and
  narrows only `data`
- table and TSV keep stdout row-only and write partial/non-first-page and
  warning diagnostics to stderr; they never append navigation metadata below
  rows
- raw artifacts keep their exact success body on stdout and write any warnings
  to stderr; they never append navigation metadata

The raw success modes are action-specific. `workflow export` and
`workflow-instance export` always write only their YAML documents. Concrete
workflow, workflow-patch, workflow-instance-patch, and typed task templates
write YAML only with `--raw`; otherwise they return their standard structured
template result. `task-instance log --raw` writes only log text, while the
default log result retains text and window metadata in the standard envelope.
Global display options do not transform any successful raw body. Failures
remain structured and follow the error channel rules above.

JSON object member order is not a semantic contract. Consumers must read fields
by name.

Warnings, when present, appear once as `warnings: [{"code": "...", "message":
"..."}]`, with additional diagnostic facts in that same object. An absent
`warnings` field means there are no warnings. Empty business data and unresolved
values remain unchanged; this rule only omits the empty optional warning field.

Success shape:

```json
{
  "ok": true,
  "action": "version",
  "resolved": {},
  "data": {}
}
```

Applicable workflow lifecycle results may also include this optional top-level
field:

```json
{
  "next_actions": [
    {
      "action": "workflow-instance.watch",
      "command": "dsctl --format json-compact --columns id,name,state,startTime,endTime,duration workflow-instance watch 242 --project 7",
      "mutates": false
    }
  ]
}
```

`next_actions` is an ordered, advisory list of at most three complete shell
invocations. It is derived locally from the current `action`, `resolved`, and
`data` plus the explicit invocation target; producing it sends no additional
request. Every item contains the stable target `action`, one placeholder-free
`command`, and `mutates`, which states whether executing that command changes
local or remote state according to its catalog effects. When the current invocation uses `--env-file`, every suggested
command preserves its resolved path with shell-safe quoting. A suggestion does
not grant permission to execute a mutation. When facts are missing, malformed,
or ambiguous, the field is omitted rather than populated with guessed values.

After independently checking current intent and authorization, agent callers
should preserve a selected provided command unchanged so its bounded page size,
projection, raw view, and explicit target are retained. Ordering communicates
recommendation priority, not a requirement to execute every item. A
`mutates: true` item must complete before a later read that depends on its new
state.

Applicable row-oriented list results may also include this optional top-level
field:

```json
{
  "action_index": {
    "scope": "data.totalList",
    "target": {"resource": "workflow", "field": "code"},
    "authorization": "not_evaluated",
    "eligibility": "row_facts_only",
    "groups": [
      {"targets": "all", "read": ["workflow.get"]},
      {"targets": [101], "mutate": ["workflow.online"]}
    ],
    "schema_command_pattern": "dsctl schema --command ACTION",
    "group_command": "dsctl schema --group workflow",
    "target_count": 2,
    "indexed_target_count": 2,
    "truncated": false
  }
}
```

Use `schema_command_pattern` when constructing per-action schema requests.
Fields ending in `_command` contain executable commands; fields ending in
`_command_pattern` contain placeholders. Schema 3 removes duplicate legacy aliases.

`action_index` is a compact positive-discovery index, not an authorization or
validation result. It is derived locally from the already returned rows and
sends no additional request. `target_count` is the number of returned rows;
`indexed_target_count` is the number of unique, valid selectors indexed, up to
100 in stable row order. `truncated` reports whether additional valid selectors
were omitted. A group's `targets` is either an explicit selector list or
`"all"`, meaning every returned row at `scope`, not every remote object.
`"all"` is emitted only when every returned row has a valid, unique selector
and the entire returned set is indexed. Truncation, duplicate selectors or
malformed rows require explicit selector lists, including for unconditional
read actions. `truncated: false` alone does not prove full indexing: invalid
or ambiguous identities can still be excluded.
Actions with exactly the same targets share one group so selector lists are not
repeated. Each action appears in one of four categories: `read`,
`read_needs_input`, `mutate`, or `mutate_needs_input`. The latter two require
mutation authorization; either `*_needs_input` category means the returned
selector and row facts are insufficient and additional input is required.

The index omits actions whose required row facts are missing or ambiguous.
Malformed selectors are ignored, and a selector appearing in more than one row
is excluded because its row facts are not unambiguous.
`authorization: "not_evaluated"` means permissions were not checked, and
`eligibility: "row_facts_only"` means other server-side facts may still reject
execution. `schema_command_pattern` is a substitution template: replace `ACTION` with
one selected grouped action to obtain its exact machine-readable contract.
The group command is the broader fallback when the desired action is still
unknown. Table, TSV, and raw output remain data-only rather than attempting to
represent an interactive UI dropdown in plain text.

JSON `--columns` only projects row fields; it does not change the indexed set
or remove explicit target ids. In compact lists, `scope` and `target.field`
refer to the decoded logical collection and field, not positions in the encoded
container. Keep the declared `target.field` in selected columns when
constructing invocations from rows covered by `"all"`.

Error shape:

```json
{
  "ok": false,
  "action": "project.get",
  "resolved": {},
  "data": {},
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
- `output_contract_error` reports a broken declared compact collection path or
  row shape at the rendering boundary. Its suggestion points to `--format json`
  for inspecting original fields. Dotted compact-list `--columns` selectors
  instead produce `user_input_error` before service execution
- `error.details` is present when the command can expose structured context
- when an operation returns successfully but its requested output cannot be
  rendered, the error preserves the complete returned `data`, `resolved`,
  warnings and navigation. Its details include `phase: output_render`,
  `result_available: true` and `operation_returned_success: true`. This does not
  assert `mutation_applied`; callers must inspect the preserved result or
  receipt and must not blindly repeat a mutation solely to change its output
- `error.source` is present when the CLI can preserve machine-readable origin
  facts from an underlying remote failure
- `error.suggestion` is present when the CLI can provide one concrete next step
  without guessing
- table and TSV errors preserve all `error.details` fields alongside type,
  message, suggestion and source, including confirmation impact, completed
  mutation stages and known resource identities

Transport diagnostics distinguish `error.details.retryable` (a transient failure)
from `request_replay_safe` (whether this request may safely be sent again).
A read rejected with HTTP 401 is replay-safe but not retryable; an unsafe mutation
rejected with HTTP 503 may be transient but is not automatically replayed.
Changing retry policy does not make an unsafe request safe.

Generated response-contract failures use `error.type: api_transport_error` and
may include these bounded diagnostic fields in `error.details`:

- `validation_message`
- `validation_error_count`
- `validation_errors[]`, with `field`, `type`, and `message`
- `validation_errors_truncated: true` when more than five validation errors
  were present

Validation diagnostics omit rejected values and use static messages. Field
paths have bounded depth and length; non-identifier dynamic keys are redacted.
When more than five errors exist, the CLI emits only the count and truncation
flag instead of expanding the remote error set.

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
- `next_actions` and `action_index`, when present, are CLI-owned navigation
  rather than DS-native resource data; they remain outside both `data` and
  `resolved`
- high-risk mutations may return `confirmation_required` and expect the same
  command to be retried with `--confirm-risk TOKEN`
- `warnings` is an optional array of structured objects, emitted only when
  nonempty. Each item contains a stable `code`, a readable `message`, and
  applicable facts. There is no separate `warning_details` array or repeated
  string-message list; consumers treat an absent `warnings` as no warnings
- every dry-run result includes one standard warning detail with code
  `dry_run_no_mutation_sent`
- `resolved` records facts selected or adopted by this command invocation, such
  as resolved resource identities, normalized selectors, applied filters, or
  the active output view
- `resolved.view`, when present, is reserved for commands whose single stable
  action can return more than one `data` shape; it is not required for commands
  whose `action` already uniquely identifies the output shape
- discovery candidates and allowed values do not belong in `resolved`; expose
  them through command `data`, schema `choices`, `discovery_command`, enum
  commands, capabilities, or structured error `details`
- a read admitted by API contract discovery records its execution evidence in
  `resolved.target`: `ds_version=null`, candidate versions, observation source
  and time, matched operation count, and `execution=read_only`. This describes
  the evidence adopted for this invocation, not an exact server identity.
  Successful reads include warning code `read_compatibility`; errors preserve
  the same unknown-version distinction.
- command schema entries may include `data_shape` metadata with a stable
  low-entropy row/object model for renderers, JSON projection, and AI agents

Current `data_shape` fields:

- `kind`: one of `page`, `collection`, `object`, `summary`, or `document`
- `row_path`: dot-path from the standard JSON envelope to the canonical row
  collection or object, such as `data.totalList` or `data`
- `compact_rows`: emitted as `true` for business collections encoded as
  `columns` and `rows` by `json-compact`, including explicitly declared nested
  collections in summary views; absence retains the standard JSON shape. It is
  a static shape declaration, not inferred from `kind` or returned values
- `value_path`: dot-path to a non-row document such as `data.schema`
- `default_columns`: suggested table/TSV display columns; not a JSON field filter
- `column_discovery`: normally `runtime_row_keys`, meaning full column discovery
  comes from the JSON row payload; document views use `not_applicable`
- `supported_output_formats`: present when a shape supports fewer than the
  standard `json`, `table`, and `tsv` set
- `column_projection`: present as `false` when `--columns` would destroy the
  document semantics and is therefore rejected

Current output metadata also exposes `format_option`, `compact_json`,
`compact_list_encoding: "columns_rows"`, `compact_list_contract`,
`json_encoding`, `default_json_layout`, `error_channel`, and
`row_diagnostics_channel`. `compact_list_contract` records
`data_shape_flag: "compact_rows"`, `fields: ["columns", "rows"]`,
`column_selection: "top_level_fields"`, and
`scope_paths: "decoded_logical_collections"`.
`compact_json: true` advertises format support; `data_shape.compact_rows`
decides whether a command's business list uses positional rows. Compact-list
projection accepts only top-level fields and `*`; navigation addresses decoded
logical rows and fields.

## `dsctl version`

Returns CLI and selected DolphinScheduler version metadata. The selected
version comes from an explicit `DS_VERSION`, or a valid local discovery cache
for an automatic target. This command never probes; a cold or expired target
cache returns `config_error` suggesting `doctor` or an explicit version.
Without any target settings, it uses the `3.4.1` offline authoring baseline.
`version_source` distinguishes `explicit`, `probe`, `cache`, and
`offline_default`; `version_checked_at` is the observation time in Unix seconds
(or null), and `version_evidence` identifies the REST probe (or null).

When discovery establishes only API contract candidates, `ds` and
`selected_ds_version` remain null, including for a single candidate. The payload
exposes `candidate_versions`, `reported_version`, and `automatic_read_actions`.
An internal compiled contract selected to execute an admitted read never becomes
the server's reported release. An unresolved cached observation can likewise be
inspected without selecting an exact profile.

Example:

```json
{
  "ok": true,
  "action": "version",
  "resolved": {},
  "data": {
    "cli": "0.4.0",
    "ds": "3.4.1",
    "selected_ds_version": "3.4.1",
    "version_source": "explicit",
    "version_checked_at": null,
    "version_evidence": null,
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "supported_ds_versions": [
      "1.3.9", "2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.4",
      "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.0.9",
      "3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6",
      "3.1.0", "3.1.1", "3.1.2", "3.1.3", "3.1.4", "3.1.5",
      "3.1.6", "3.1.7", "3.1.8", "3.1.9", "3.2.0", "3.2.1", "3.2.2",
      "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"
    ]
  },
  "warnings": []
}
```

`supported_ds_versions` lists selectable targets; it does not claim that every
listed version has stable support. The selected target's `support_level`
reports the inherited release policy: `3.4.1` is `full`, while the other 36
exact profiles are `experimental`. Artifact-bound verification coverage is
reported separately; see [support policy and verification](../user/version-compatibility.md#support-policy-and-verification).
Each profile reports its own exact
`contract_version` and one of the compatibility families
`process-definition-1.3`, `process-definition-2.0`,
`process-definition-3.0`, `process-definition-3.1`,
`process-definition-3.2`, or `workflow-3.3-plus`.

The exact catalog contains all 181 actions for every selected version. Its
availability counts may include upstream-limited or upstream-absent actions;
named live-smoke sets are evidence scopes rather than catalog limits. Current
generic exact-read promotion receipts use schema 2 and bind the semantic
profile, manifest, four accepted read recipes, their fingerprints, and
digest-bound `live_smoke` verification to one installed wheel without exposing
adapter class names. The read scope is exactly `project.list`, `project.get`,
`workflow.list`, and `workflow.get`. Schema-1 generic receipts remain auditable
only for their historical wheels and cannot satisfy the current promotion
checker. The separate `external-shell/v1` scenario on exact `3.4.2` requires
schema 7; its historical
schema-3/4/5/6 receipts likewise retain their original artifact and action scope.

Named conformance evidence does not itself change per-action verification,
action support, `tested`, or whole-profile promotion. A `live_smoke` task-update
receipt does not establish the deterministic stale-plan negative needed for
`task.update=live_full`. Successful behavior checks cannot replace verification
of the installed wheel's source, metadata and generated claims. Preliminary
checks and artifacts that fail release preflight cannot enter the governed
promotion corpus. See
[Live Testing](../development/live-testing.md#exact-version-profile-gates) for
the immutable identifiers and evidence policy.

## `dsctl context`

Returns the effective local target without a network request or remote project
validation. `data` contains `context` (name or `null`), `api_url`, `ds_version`
(requested exact version or `auto`) and `project`.

`resolved.selection` identifies the effective `source`, `context`, `env_file`
and `api_url`. `resolved.remote_validation` is `not_performed`. No token,
token fragment or credential hash appears in this output.

### Saved contexts

- `context create NAME --file FILE [--project PROJECT]`
- `context update NAME [--file FILE] [--project PROJECT | --clear-project]`
- `context list`
- `context get NAME`
- `context delete NAME`

The only store is `$XDG_CONFIG_HOME/dsctl/config.yaml`, otherwise
`~/.config/dsctl/config.yaml`. Each entry contains an absolute logical env-file
reference, bound normalized API URL and optional project. Credentials remain in
the caller-managed env file. Create and explicit file updates validate a
complete local connection, without contacting DS. An update requires at least
one change; `--project` and `--clear-project` conflict.

Token rotation at the same endpoint retains the project and is read on the
next invocation. An externally changed endpoint makes selection fail until an
explicit `update --file` rebinds it. Every file update clears the previous
project unless a replacement `--project` is supplied. Project-only updates
preserve the file binding. Selected contexts never borrow resource defaults
from another context or source. Workflows remain explicit.

List and get inspect the saved entries without reading their credential files.
Saved entry output contains `name`, `env_file`, `api_url` and `project`; list
and get also include `default`. Delete returns the removed entry with
`deleted: true`. These commands report `resolved.registry` and
`resolved.remote_validation: not_performed`. Registry updates are serialized
and atomic, and leave credential files unchanged.

### `dsctl config`

- `config get default-context`
- `config set default-context NAME`
- `config unset default-context`

`default-context` is the only supported key. Set requires an existing named
entry. Delete of that default entry is rejected until its default is unset.
These operations are local and do not validate the remote target.

Results contain `data.key` and `data.value` (name or `null`). Writes additionally
report `resolved.saved: true` and `resolved.effective`, the effective selection
under current invocation precedence. A saved default may be shadowed by an
explicit or environment source. If effective readback fails after a successful
write, the result stays successful with `effective: null` and warning detail
code `default_context_readback_failed`.

`dsctl use`, directory-scoped selection, workflow defaults and legacy context
file fallback have been removed. Old files are left untouched.

## `dsctl doctor`

Returns structured connection, version and authenticated API diagnostics.
It does not validate project permissions, contact master/worker RPC endpoints,
or execute tasks; `status: ok` applies only to the checks in the report.

Current `data` fields:

- `status`
- `summary`
- `checks`

Current guarantees:

- returns one aggregated diagnostic payload; any error check sets `ok: false`,
  returns `check_failed` and exits 1. Warning-only reports exit 0. Known health
  statuses `DOWN` and `OUT_OF_SERVICE` are errors; an unknown status is a warning
- resolves automatic targets afresh inside the aggregated profile check;
  failed discovery skips exact adapter binding and health requests. Contract
  candidates may admit a reviewed current-user read; otherwise it is skipped
- contract-only discovery reports profile and adapter warnings describing the
  bounded read scope. Successful authentication does not establish an exact
  version or a whole-profile health claim
- successful profile details include `version_source`, `version_checked_at`, and
  `version_evidence` with the same meanings as `version`; these describe the
  observation reused for adapter binding and do not trigger another probe
- checks selected profile loading, effective named-context binding, and exact
  profile/domain binding
- attempts API actuator health only when the exact `monitor.health` capability
  is supported; otherwise records the generated capability constraint as an
  `api` warning and uses the authenticated current-user check as the readiness
  fallback
- on exact `1.3.9`, `2.0.0`, and `2.0.9`, the unsupported health capability
  records reason `upstream_endpoint_absent`, sends no actuator request, and can
  still pass readiness when the authenticated current-user fallback succeeds;
  exact profiles from `3.0.0` use actuator health
- checks current-user runtime defaults and validates credentials through the
  exact current-user operation available on every supported profile
- exact `3.1.0` through `3.4.3` bind that current-user exchange through the
  reviewed `current-user-detail-v1` family; earlier profiles retain their
  distinct exact response epochs
- preserves structured error details under `checks[].details.error`
- exposes `checks[].suggestion` for every check; successful checks use `null`
- emits top-level warnings for any check whose status is not `ok`
- when such a warning is present, the `warnings[]` item uses
  code `doctor_check_not_ok` and includes `check`, `status`, `message`, and
  `suggestion`

## `dsctl schema`

Returns the stable machine-readable command schema for the current CLI surface.
Schema version 3 uses progressive discovery: the default response is a bounded
index, a group response is an action index, and a command response is the
complete action-local invocation contract. This is the authoritative
self-description for arguments, options, choices, selectors, defaults,
payload hints, supported composite keys, and command output shape.

For automatic targets with no exact version, schema remains available locally
as a catalog of the installed CLI's invocation syntax. It distinguishes unknown
availability from reads admitted by cached contract evidence. It does not expose
an exact-version authoring model or present candidate-specific constraints as
server facts. Use `doctor` to refresh the evidence or set an exact `DS_VERSION`
for version-specific schemas and templates.

Options:

- `--group GROUP`
- `--command ACTION`
- `--list-groups`
- `--list-commands`
- `--full`

Selection rules:

- omit all options to return the bounded root index; its `groups[].actions`
  and `root_actions[]` cover every stable action name without expanding action
  contracts
- `--group` returns a bounded action index for one stable group such as
  `task-instance`; each action includes its summary, help command, and exact
  schema command
- `--group` values come from `dsctl schema --list-groups`
- `--command` returns one complete action-local `data.command` object for an
  action such as `task-instance.list` or `version`; it does not wrap the action
  in the whole command-group tree
- action names come from the default index, a group index, or the compatibility
  inventory `dsctl schema --list-commands`
- `--list-groups` returns compact rows with `name`, `summary`,
  recursive `action_count`, and `schema_command`
- `--list-commands` retains compatibility rows with `action`, `group`, `name`,
  `summary`, and `schema_command`; the bounded default index is preferred when
  only action names are needed
- `--full` returns the expanded whole-surface representation, including the
  complete `commands` tree and embedded `capabilities`
- `--full` may be combined with `--group` or `--command` to retain the expanded
  scoped representation; it cannot be combined with list views
- `--group`, `--command`, `--list-groups`, and `--list-commands` are mutually
  exclusive
- unknown groups and actions return `available_count`, at most three
  deterministic `candidates`, and one `discovery_command`; errors never dump
  the entire action inventory

Bounded view shapes:

- index: `schema_version`, `view`, `cli`, `ds`, `global_options`,
  `action_count`, `groups`, `root_actions`, and `links`
- group: `schema_version`, `view`, `cli`, `ds`, `group`, `actions`, and
  `links`
- command: `schema_version`, `view`, `cli`, `ds`, `global_options`, optional
  `group`, canonical `command`, and `links`
- list views keep `data` as a row list for pipeline compatibility
- full: `schema_version`, `view`, `cli`, supported-version and global contract
  fields, `capabilities`, and the expanded `commands` tree

The canonical index, group, and command JSON views never contain renderer-only
`rows`. Table and TSV presentation derive rows from `groups`, `actions`, or the
canonical `command` object. For a command view, JSON `--columns` projects the
derived contract rows into `data.command`; without `--columns`,
`data.command` remains the canonical object.

Actions whose canonical row path changes by view expose
`data_shapes_by_view`. Their ordinary `data_shape` describes the default view;
renderers select the matching view-specific shape at runtime. This applies to
both progressive self-description and `task-type.schema`. Expanded top-level
schema shapes distinguish root `full` (`data.commands`) from `full_group` and
`full_command` (`data.rows`) using `resolved.schema.scope`.

`resolved.schema.view` is `index`, `group`, `command`, `groups`, `commands`, or
`full`. Full scoped responses also set `resolved.schema.scope` to `group` or
`command`.

`links[]` contains optional related navigation, not a sequence that callers
must follow. Exact, directly executable invocations use `command` or a more
specific `*_command` field. Placeholder-bearing forms use `command_pattern` or
the matching `*_command_pattern` field and uppercase semantic metavars such as
`PROJECT`, `WORKFLOW_INSTANCE`, and `FILE`. Angle-bracket placeholders are not
used in CLI notation because interactive shells parse `<` and `>` as
redirection operators. Schema v3 emits one canonical pattern field and removes
the old same-value `*_command` aliases. A concrete, already-bound command may
still be present alongside a different generic pattern. Bounded responses do
not link to the high-cost `--full` view.

Every action-local `command` includes an `invocation` usage string with the
exact CLI path, positional placeholders, and `[OPTIONS]` when applicable. This
is authoritative for actions whose stable action id is not the literal command
path, including bare group callbacks such as `context` (`dsctl context`).

Modeled static relationships between multiple inputs appear in
`command.constraints[]` instead of relying on prose. Current constraint kinds
are `exactly_one_of`, `at_most_one_of`, `at_least_one_of`, `all_or_none`,
`requires`, `requires_all`, `requires_any`, `requires_default`, and `forbids`.
`fields` names positional placeholders or option flags; `alternatives` groups
fields that form one mode; `if_present` and `if_absent` make a relationship
conditional. `requires_default` names the required parser defaults in `defaults`
when its condition is active. Constraint references are tested against the
corresponding action contract. Runtime validation remains authoritative for
dynamic conditions such as risk-confirmation tokens and for static validators
not yet represented in the registry; absence of `constraints` is not a claim
that no relationship can exist.

### Selected-version option projection

For actions with reviewed field-level compatibility evidence, the action-local
schema is a planning view for the selected DolphinScheduler version, not a dump
of every flag accepted by the installed parser. `command.options` contains only
options that can participate in a successful invocation on that version, and
`command.constraints` is recomputed over that same usable field set.

When an installed flag maps to a field that is absent upstream, the schema
removes it from `command.options` and records it in
`command.unavailable_options`. Each entry contains:

- `flag`: the omitted CLI flag
- `availability: "upstream_absent"`: the reason class
- `introduced_in`: the first reviewed DS version that exposes the field
- `instruction: "omit"`: the required planning behavior

For example, selected DS `1.3.9` schedule contracts mark `--timezone`,
`--environment-code`, and `--tenant-code` unavailable; schedule preview and
create-style explain therefore require only cron, start, and end. DS `2.0.0`
adds timezone and environment, while DS `3.2.0` adds tenant. These facts come
from the same exact schedule recipe used by the runtime domain.

Leaf Typer help has a different responsibility: the installed command parser
accepts the cross-version union of flags so it can return a typed compatibility
error, and help gives a concise conditional overview of that union. Agents
constructing a command for a configured target should treat selected-version
schema `options`, `constraints`, and `unavailable_options` as authoritative;
help remains the human-oriented routing and cross-version summary surface.

Every action declares `effects` from the shared command catalog:
`remote` is `none`, `read` or `write`; `local` is `none`, `configuration` or
`file`. Action details for dry-run-capable commands include a `dry_run` override
(`remote: read`, `local: none`). Group and action indexes expose the base effect
pair. Classification describes the operation's possible effects, independently
of HTTP method, permission or eligibility. For example, `alert-plugin test`
sends an alert and is a write, while `datasource test` only checks a connection.
The old schema-only `mutates`, `mutation_target` and `remote_requests` fields
are removed. Navigation keeps its existing `mutates` authorization signal.

Current guarantees:

- describes only the current stable surface
- bounded `global_options` repeat only the global flags required to build
  an invocation, and every entry declares `placement: "anywhere"`
- `--format` and `--columns` therefore remain discoverable
  even when an agent jumps directly to an action-local contract
- `json-compact` is a `--format` choice; declared compact business lists
  expose their encoding through `data_shape.compact_rows`
- uses `DS_VERSION` and `--env-file` for compact selected-version contract
  identity; `--full` retains all supported-version metadata
- command arguments and options may include additive metadata such as
  `choices`, `minimum`, `examples`, `supported_keys`, and
  `discovery_command` when the CLI can expose a tighter contract
- `normalization: "lowercase"` means the CLI lowercases the supplied value
  before choice validation; absence of `normalization` publishes no
  normalization guarantee for handwritten commands that have not migrated to
  the canonical command contract
- path inputs may expose `input_policy` with the parser's `exists`,
  `file_okay`, `dir_okay`, `readable`, and `resolve_path` rules
- `default` without `resolution` means omission produces one fixed value
  without consulting a higher-priority runtime source
- when `resolution` is also present, its source order is authoritative;
  `default` is retained only as a schema-version-compatible projection of the
  same terminal value, not as an eager parser default
- `resolution.precedence` is ordered from strongest to weakest; the current
  workflow runtime sources are `flag`, `project_preference`, and `default`;
  `resolution.fallback` carries the value used by that terminal `default`
  source
- command entries that accept file payloads may include compact `payload`
  metadata; when present, `payload.template_command` is the preferred
  progressive-discovery command for a concrete payload template
- `schema --command workflow.create` also exposes
  `command.payload.yaml_schema`: model-derived workflow metadata and schedule
  fields, exact enum constraints, and a bounded task-entry outline. Follow
  `payload.task_authoring` for task schemas and `payload.execution_context` for
  tenant/run/schedule settings. This file structure complements the command
  options; task semantics, graph references and cross-field validation still
  require `lint workflow` and the create preview
- feature discovery remains the responsibility of `dsctl capabilities`; only
  the explicit expanded `--full` view embeds it
- `schema_version` changes for breaking schema changes; additive fields may
  appear within the same version
- is tested against the actual registered command tree
- command entries that expose row/object-oriented output include `data_shape`;
  this is the authoritative model for `--columns` and
  `--format table|tsv`
- schema and capabilities output metadata expose `json_column_projection` when
  JSON `--columns` projection is supported
- compact output size is a review metric: root index 16 KiB, action-local
  contract 8 KiB, unknown-action recovery 2 KiB, and leaf help 10 KiB are
  review thresholds, not hard limits. Correctness, useful explanation and
  maintainable code take precedence. Tests record actual sizes in JUnit
  properties (`--junitxml=... -o junit_family=legacy`); structural and command-reference checks remain
  mandatory regardless of size
- an action-local view includes `data.capability`, sourced from the selected
  exact-version catalog, with `availability`, `verification`, and any bounded
  restriction; this describes whether the invocation may be attempted without
  changing the stable `data.command` contract. The same fact remains present
  when `--full` expands an action-local view
- a group view keeps the installed action surface discoverable while adding
  `availability` and `verification` to each action plus
  `available_action_count` to the group summary; default table and TSV columns
  retain these facts
- action-local table and TSV views emit capability rows before invocation rows,
  so changing output format cannot hide an unsupported selected-version action

## `dsctl capabilities`

Returns stable version and surface capability discovery for the current CLI and
selected DS version.
It answers which resource families and feature groups exist, while `schema`
answers how to invoke them. The default is a bounded summary; agents should
expand one `--section` unless they need the complete `--full` inventory, and
should request `capabilities --action ACTION` for one exact availability fact
and `schema --command ACTION` when constructing a command.

An automatic target without an exact version receives a catalog-only view with
null selected version. Cached contract evidence can mark individual reviewed
reads available; remaining remote actions and authoring require an exact version.
This view does not claim an exact profile's support level or release-gate status.
Candidate enum discovery labels its scope `candidate_generated_contracts` and
server membership `unverified`: equality across candidate sources does not prove
that the deployment exposes every member.
Local command and diagnostic availability remain separate from remote read
admission: `available_local`, `available_diagnostic`, `conditional_local`, and
`requires_discovery` identify those cases. In particular, invoking `doctor` does
not require a known release; its authenticated read is checked independently.
The exact-version fields and guarantees below apply after exact resolution.

Options:

- `--summary`
- `--section SECTION`
- `--full`
- `--action ACTION`

Selection rules:

- omit all options to return bounded summary capability discovery
- `--summary` explicitly requests the same default bounded view with `cli`, `ds`,
  `surface`, `self_description`, `resources`, `planes`, `runtime`, `schedule`,
  `monitor`, `enums`, and a summarized `authoring` section
- `--section` returns one top-level section plus the standard `cli`, `ds`, and
  `surface`, and `self_description` header
- `--full` returns the complete expanded capability inventory
- `--action` returns only the selected version metadata, one action's
  availability/verification/restrictions, and direct schema/help links
- valid sections are `selection`, `output`, `errors`, `resources`, `planes`,
  `authoring`, `schedule`, `monitor`, `enums`, and `runtime`
- `--summary`, `--section`, `--full`, and `--action` are mutually exclusive

Complete `--full` `data` fields:

- `cli`
- `ds`
- `surface`
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
- `action_catalog` (explicit `--full` audit only)

Current guarantees:

- `data.ds.selected_version` is the normalized target DS version
- `data.ds.contract_version` is the exact generated contract version selected
  for the server
- `data.ds.family` identifies the compatibility family used by the exact bound
  domains; family reuse is not itself a whole-profile support claim
- `data.ds.support_level` is one of `full`, `legacy_core`, or `experimental`
- `data.ds.tested` reports whether the selected profile's live release gate has
  passed; a narrower per-action live smoke does not necessarily set it
- `data.ds.supported_versions` lists selectable targets, while
  `data.ds.versions` reports each target's support level and test evidence
- `data.ds.catalog` is a bounded exact-version summary with action count plus
  availability and verification counts; it does not expand all action entries
- every exact catalog contains all 181 stable actions across 37 profiles;
  [Architecture](../development/architecture.md) records the current terminal
  coordinate inventory. Unsupported coordinates are upstream-absent; no
  upstream-present stable action remains limited
- explicit `--full` adds `data.action_catalog`, sorted by action and containing
  every stable action's exact availability, verification, and optional
  constraint; bounded summary and section views never pay this token cost
- resource and plane inventories describe the installed CLI surface, not an
  assertion that every listed remote action is available on the selected
  server; `data.surface.inventory_scope="installed_cli_surface"` makes this
  distinction explicit beside the selected-version metadata
- authoring keeps the two scopes structurally separate:
  `data.authoring.installed_cli_inventory` describes reusable CLI syntax and
  templates, while `data.authoring.selected_version_availability` reports
  whether those authoring paths are executable for the selected DS profile;
  `data.surface.selected_version_availability_source="action_catalog"` points
  to the authoritative per-action execution truth
- registry entries marked `full` or `legacy_core` always have `tested: true`;
  untested selectable targets use `experimental`
- bounded read-smoke evidence does not set `data.ds.tested`, change a support
  level, or expand an action catalog; current campaign status and receipt
  governance are maintained in
  [Live Testing](../development/live-testing.md#generic-exact-profile-installed-wheel-read-gate)
- a current `external-shell/v1` schema-7 receipt on exact `3.4.2` must bind its 15-action
  `live_smoke` bundle to the current canonical wheel, semantic profile, recipes,
  fingerprints, and generated manifest; schema-3 through schema-6 receipts
  remain auditable only for their historical artifacts, and neither current nor
  historical evidence changes `data.ds.tested=false`, the support level
  `experimental`, or whole-profile promotion
- DS-native local helpers are not automatically portable merely because they
  send no HTTP; whenever an experimental profile exposes an authoring helper,
  the version-selected task-authoring catalog governs schema, template, lint,
  compilation, and preservation, and a missing catalog/task type/facet fails
  closed without borrowing `3.4.1`

- summarizes only the current stable surface
- exposes name-first, path-first, and id-first selection rules
- exposes standard output-envelope support
- exposes structured error and `error.source` support
- exposes generated enum discovery names only when the selected exact-version
  catalog makes the enum actions available
- distinguishes CLI task-template coverage and variants from DS upstream
  default task types
- exposes untemplated upstream task types for authoring gap analysis
- keeps live runtime task-type discovery out of the static capability payload;
  use `dsctl task-type list` for cluster/user-visible DS task types only when
  that remote action is available for the selected version
- does not describe command arguments or options; use `dsctl schema` for that
- exact action availability is queried most narrowly with
  `dsctl capabilities --action ACTION`; action-local schema repeats this fact
  beside the invocation contract. Unsupported actions fail before transport
  and are omitted from result `next_actions` and `action_index` discovery
- exposes `data.self_description.command_invocation_source="schema"` and
  `data.self_description.capabilities_scope="feature_discovery"` so tools can
  distinguish feature discovery from command invocation metadata
- the default/`--summary` and `--section` views are projections over the same
  feature discovery data, not output-format modes
- the default summary and `--section` are the bounded feature-discovery
  companions to progressive `dsctl schema`; `--full` is explicit expansion
- compact default capability output is measured against the 8 KiB review
  reference; necessary compatibility facts take precedence over that size

### Version-selected task authoring

DS-native authoring behavior is selected by the exact `DS_VERSION`; it is not
implicitly the `3.4.1` model. One task-authoring catalog governs task schema,
templates, lint normalization, workflow compilation, and opaque preservation
for every command that consumes those views. Action preflight still decides
whether a command itself is available; catalog membership does not promote an
experimental action.

Mechanical task-plugin source facts cover all 37 exact releases. They describe
what upstream declares and are not by themselves executable typed-authoring or
preservation promises. Typed create/edit is enabled only by an exact positive
review membership. The reviewed matrix now contains forty-two distinct typed
task families and 902 exact version/family memberships:

| Exact profiles | Reviewed typed task families |
| --- | --- |
| `1.3.9` | 11: `CONDITIONS`, `DATAX/literal_custom_json_job`, `DEPENDENT`, `HTTP`, `MR/literal_java_jar_job`, `PROCEDURE`, `PYTHON`, `SHELL`, inline `SQL`, `SQOOP/literal_command`, canonical `SUB_WORKFLOW` |
| `2.0.0` | 13: the `1.3.9` families plus `PIGEON` and `SWITCH`; source-present `WATERDROP` is an exact runtime exclusion |
| `2.0.1` through `2.0.9` | 14: the `2.0.0` families plus `WATERDROP/literal_local_config_job` |
| `3.0.0` through `3.0.6` | 20: the common `2.0.x` 13-family set plus `BLOCKING/same_workflow_state_gate`, `DATA_QUALITY/local_mysql_table_row_count_equals`, `EMR`, `FLINK/inline_local_sql`, `SEATUNNEL/literal_local_config_job`, `SPARK/inline_local_sql`, and `ZEPPELIN/paragraph` |
| `3.1.0` | 27: the `3.0.x` families except FLINK and DATAX, plus `CHUNJUN/literal_local_json_job`, `DINKY/job_trigger`, inline `HIVECLI`, `DVC/operation`, `MLFLOW/model_serve`, `JUPYTER/preinstalled_notebook`, `OPENMLDB/literal_single_statement`, `SAGEMAKER/start_pipeline_execution`, and `PYTORCH/literal_resource_script` |
| `3.1.1` | 27: DATAX joins the `3.1.0` set; SAGEMAKER has a polling defect |
| `3.1.2` | 27: FLINK is repaired; OPENMLDB and SAGEMAKER have exact runtime exclusions |
| `3.1.3` | 29: OPENMLDB and SAGEMAKER are repaired; K8S and FLINK_STREAM remain excluded |
| `3.1.4` | 30: K8S is repaired; FLINK_STREAM remains excluded |
| `3.1.5` through `3.1.9` | 31: the `3.1.0` families plus DATAX, `FLINK/inline_local_sql`, `FLINK_STREAM/inline_local_sql`, and `K8S/literal_container_job` |
| `3.2.0` | 37: the `3.1.9` families plus `DATASYNC/create_and_execute`, `DATA_FACTORY/pipeline_trigger`, `DMS/resume_existing_full_load`, `REMOTESHELL`, `JAVA/literal_fat_jar`, and `KUBEFLOW/tfjob_manifest` |
| `3.2.1` | 36: the same additions except JAVA, whose absolute resource path is broken by an exact runtime double-prefix hole |
| `3.2.2` | 38: the `3.2.0` family set after the JAVA resource-path defect is repaired, plus `DYNAMIC/literal_single_dimension_fanout` |
| `3.3.1` and `3.3.2` | 34: the `3.2.2` families except upstream-absent `BLOCKING` and `PIGEON` and runtime-hole `FLINK_STREAM`, plus `ALIYUN_SERVERLESS_SPARK/literal_jar_submit`; `CHUNJUN/literal_local_json_job`, `DATASYNC/create_and_execute`, `DATAX/literal_custom_json_job`, `DATA_FACTORY/pipeline_trigger`, `DINKY/job_trigger`, `DMS/resume_existing_full_load`, `DVC`, `EMR`, `FLINK/inline_local_sql`, `HIVECLI`, `JAVA/literal_fat_jar`, `JUPYTER`, `K8S/literal_container_job`, `KUBEFLOW/tfjob_manifest`, `MLFLOW/model_serve`, `MR/literal_java_jar_job`, `OPENMLDB/literal_single_statement`, `PYTORCH/literal_resource_script`, `SAGEMAKER/start_pipeline_execution`, `SEATUNNEL/literal_local_config_job`, `SPARK/inline_local_sql`, `SQOOP/literal_command`, and `ZEPPELIN/paragraph` remain typed |
| `3.4.0` and `3.4.1` | 34: PYTORCH is removed upstream while `GRPC/literal_unary_string_record_call` joins the preceding family set |
| `3.4.2` | 35: the preceding families plus `EMR_SERVERLESS/start_job_run` |
| `3.4.3` | 35: the same positive families as `3.4.2`; DATAX additionally rejects an empty literal JSON object |

The fully unreviewed source-present remainder is zero.

The complete exact family memberships and parameter/execution transitions are
listed in [task authoring boundaries](../development/task-authoring-boundaries.md).
In particular, `2.0.8` first preserves arbitrary startup keys; child globals win
parent-name collisions through `2.0.2`, parent globals win from `2.0.3`, and
`2.0.7`–`2.0.9` child OUT copying occurs only on the cancel path, not normal
completion. Native CONDITIONS/SWITCH branch references change from strings to
longs in `2.0.8`.

`SEATUNNEL/literal_local_config_job` is typed on all twenty-six exact profiles from
`3.0.0` through `3.4.3`. Its closed canonical input owns only a literal ASCII
`rawScript` SeaTunnel configuration with LF/tab whitespace and no DS
placeholders. Projection has four exact wire epochs: `3.0.x` receives a
compiler-owned REST-only POSIX wrapper that writes the config and launches
`start-seatunnel-spark.sh` in client/local mode because the upstream UI emits
a broken Waterdrop launcher; exact `3.1.0`–`3.1.5` use `engine=SPARK`, custom config and `deployMode=local`
(the enum renders client). Exact `3.1.6` requires `deployMode=client` and
`master=LOCAL` to retain client/local execution; exact `3.1.7` onward uses `startupScript=seatunnel.sh`,
custom config, local deployment, and empty options. Resource mode, arbitrary
shell, remote engines, alternate launchers, parameters, outputs, and extra
state are excluded, including the resource-path runtime holes from `3.2.0`
through `3.2.2`. Exact `3.3.1` onward requires a parameter-free workflow
because upstream forwards workflow values outside the owned config. Raw opaque
create/edit is closed; richer state survives unchanged/export only with opaque
provenance. Workers require Unix-like execution, `SEATUNNEL_HOME`, Java,
compatible connectors/catalogs, network, tenant, and data access. HOCON/JSON
validity remains an operator prerequisite. Upstream INFO-logs task params,
config, and commands, publishes no structured output or durable application
id, cannot reattach after failover, cancels best-effort, and can replay the job
on retry. The review refreshes no live evidence and promotes no profile.

`DATA_QUALITY/local_mysql_table_row_count_equals` is typed on the twenty exact profiles from `3.0.0` through `3.2.2`. Legacy task input owns `datasource`,
`table`, and `expectedRowCount`; exact `3.2.x` also requires `database`.
Before typed create or a changed edit carrying this exact fixed-point
projection—including dry-run—the workflow service verifies the live stock-rule
page and rule-form fingerprint for rule 10 and a permission-visible MYSQL
datasource; the modern epoch also requires its database to equal the authored
value. No-op edits and unchanged richer opaque-preserve tasks skip this
authoring preflight. Projection fixes local Java Spark, the Data Quality main class,
fixed-value row-count equality, and blocking failure. Native `NE` preserves equality on exact `3.0.0`–`3.0.3` and `3.1.0`–`3.1.9`
because their master comparison is inverted; `3.0.4`–`3.0.6` and `3.2.x` use `EQ`. Legacy execution
derives database from the datasource, while `3.2.x` carries it on the wire. A
missing result row can leave the task falsely successful without comparison.
Upstream INFO logs parameters, commands, and resolved credentials, so fields
are not secret storage. Parameter-free workflows, shell-safe datasource
configuration, the Data Quality application, Spark, Java, MYSQL JDBC,
network/table `SELECT` access, and writable DS metadata are prerequisites.
The facet has no structured output, durable app id, or failover reattach;
cancellation is best-effort and retry can duplicate result rows or alerts.
Richer state is unchanged/export preserve-only. The review refreshes no live
evidence and promotes no profile.

`KUBEFLOW/tfjob_manifest` is typed in `MachineLearning` on the nine exact
profiles from `3.2.0` through `3.4.3`; it is upstream-absent earlier. The
closed canonical payload owns only `namespace`, `cluster`, and one unchanged
`yamlContent` string. Typed content is ASCII with LF line endings, parses as
one YAML mapping document, and must identify exactly
`apiVersion: kubeflow.org/v1` and `kind: TFJob`. It carries an explicit
`metadata.namespace` equal to canonical `namespace`, forbids root `status`, and
requires a nonempty `spec.tfReplicaSpecs` mapping. It rejects `generateName`
and server-owned metadata including `uid`, `resourceVersion`, `generation`,
`creationTimestamp`, `deletionTimestamp`, `deletionGracePeriodSeconds`,
`managedFields`, and `selfLink`. Its DNS-safe `metadata.name` prefix is at most
52 characters and ends with the sole allowed
`${system.workflow.instance.id}` placeholder. Only JSON-core scalar tags
(`string`, `null`, `bool`, `int`, and `float`) are accepted; YAML timestamp,
binary, set, and custom tags are rejected. All other placeholders,
controls, duplicate keys, aliases and merge keys, multiple
documents, `List` roots, and richer state are rejected.
Projection emits only the literal YAML and compact namespace JSON containing
`name` and `cluster`. Empty UI residue `localParams=[]` and `resourceList=[]`
canonicalizes on decode; nonempty or richer native params are unchanged/export
preserve-only. Raw opaque create/edit is closed.

Export remains read-only. A richer opaque KUBEFLOW baseline can be saved only
unchanged or through a description-only or task-name-only patch; a task rename
in such a patch is metadata-only. Any task/workflow execution-semantic or
topology edit fails closed.

The master uses `cluster` to resolve kubeconfig, but the worker's fixed
`kubectl apply/get/delete -f` commands ignore the outer namespace and target
the explicit manifest namespace. It substitutes prepared values before its
platform-default write and INFO-logs the complete task params, expanded YAML,
commands, status JSON, and complete resolved kubeconfig on all nine versions.
The task-log filter present on exact `3.3.1` and newer does not suppress that
worker-service INFO event. On a valid status shape, the tracker accepts only
`Succeeded`, `Available`, or `Bound` as success and `Failed` as failure. Missing
`status`/`conditions` or other nonterminal state can continue polling until the
task timeout. However, an existing empty `status.conditions` array makes all
nine exact `KubeflowHelper` implementations unconditionally access its last
element and fail at runtime instead of continuing to poll; a compatible
nonempty conditions protocol is a runtime prerequisite. No command timeout,
structured output, or durable resource id exists.
`appIds` is only an already-submitted sentinel: after callback persistence it
allows failover to resume manifest-derived polling because workflow-instance
identity is stable, but apply can succeed before persistence and be reapplied.
Generated authoring requires `retry.times=0`; retry within the same workflow
observes or reapplies the same terminal TFJob and does not guarantee a new run.
This closes only DolphinScheduler task retry; TFJob/Kubernetes-controller
reconciliation and Pod `restartPolicy` can still repeat training work.
The compiler rejects duplicate typed
`(cluster, namespace, metadata.name template)` identities within one workflow,
so sibling tasks cannot apply or delete the same TFJob. Workers need a
shell-safe task base path, `kubectl`, resolved kubeconfig,
network and RBAC, and the Kubeflow TFJob CRD with a compatible status protocol.
Typed workflow compilation requires `timeout > 0` in DolphinScheduler's native
minutes and `timeout_notify_strategy` equal to `FAILED` or `WARNFAILED`.
Omitted/default or explicit `WARN` only warns and leaves the watcher running.
The generated template sets `timeout_notify_strategy: FAILED` and `timeout: 60`
for one hour because neither kubectl commands nor status polling have an
internal deadline.

Recovery-style REPEAT/RERUN paths can reuse the same `workflowInstanceId`, so
the same `metadata.name` is rendered. Same-id recovery can reobserve an
already-successful TFJob without new training, and `REPEAT` does not guarantee
fresh training. Only creation of a new workflow instance provides a fresh
identity. `dsctl workflow-instance rerun`, `recover-failed`, and `execute-task`
now fail closed by default when the available instance `dagData` contains any
`KUBEFLOW` task. Use `dsctl workflow run WORKFLOW --project PROJECT` to create
a new workflow instance. This is a `dsctl` protection only: it cannot constrain
the DolphinScheduler UI or direct REST calls, and it makes no KUBEFLOW
detection claim when `dagData` is unavailable.

The independent `dsctl workflow online` path reads the exact workflow DAG
before its release REST call. When that DAG explicitly contains KUBEFLOW, it
reuses the applicable typed or opaque runtime preflight; a failed preflight
sends no release request. `dsctl workflow offline` does not read the DAG and is
not subject to this activation gate.

A server baseline whose `taskParams` decode canonically but whose outer retry,
timeout, or `WARN` strategy predates these typed gates remains unchanged for a
description-only patch or a file edit that leaves every execution-affecting task
field unchanged. Changing type, params, command, flag, worker/environment
selection, task group/priority, retry/timeout/strategy/delay, resource limits,
dependencies, or workflow-level execution settings—including `release_state`
transitions such as `OFFLINE` to `ONLINE`, workflow timeout, `execution_type`,
and global parameters—reruns the applicable gates. Standalone YAML has no server
provenance and cannot claim that preservation exception. No live preflight or
campaign is part of this claim; `3.4.1`
receipts become stale for the expanded authoring manifest, and no profile is
promoted.

`PYTORCH/literal_resource_script` is typed in `MachineLearning` on exact
`3.1.0` through `3.3.2`; it is upstream-absent on earlier profiles
and all exact `3.4.x` profiles. The canonical `task_params` object owns exactly
required `pythonExecutable`, required `scriptResource`, and optional
`scriptArgs` defaulting to `[]`. The executable is an absolute ASCII shell-safe
literal; `scriptResource` is a leading-slash ASCII shell-safe path relative to
the selected user's FILE root, identifies one `.py` DS FILE, and is not a
storage absolute fullName. Each ordered argument is one nonblank ASCII
shell-safe token. Whitespace,
shell expansion, controls, non-ASCII text, and DolphinScheduler placeholders
fail closed. Compilation fixes `localParams=[]`,
`isCreateEnvironment=false`, `pythonPath="."`, `pythonEnvTool="virtualenv"`,
`requirements="requirements.txt"`, and `condaPythonVersion="3.9"`; it removes
one leading slash for the staged script path and joins arguments with one
space. All seven typed profiles resolve the canonical path before mutation through
the exact generated resource API and require one permission-visible,
non-directory FILE. Exact `3.1.x` emits `pythonCommand` and sends the resolved
positive id. Exact `3.2.0` through `3.3.2` query the FILE base directory, prefix
and page-verify the canonical path, emit `pythonLauncher` and the resulting
storage absolute `resourceName`, and keep native `script` base-relative. Exact
`3.2.x` ignores the requested base type for admin sessions and returns one
ALL-resource root; because canonical intent carries no tenant selector,
resolution compares FILE/UDF responses and accepts only distinct sibling
`resources`/`udfs` tenant bases. Equal or structurally ambiguous roots fail
closed and require a non-admin tenant user. Modern reads canonicalize only when
the observed `resourceName` equals the freshly verified canonical-to-storage
identity; otherwise the original native package remains opaque. Cross-tenant
selection is outside the facet.

Git checkout, worker-local project paths, environment creation, custom
launcher fragments, local parameters, placeholders, output declarations,
additional resources, inherited fields, and future state are outside typed
create/edit. No raw opaque create/edit selector exists; richer existing state
is lossless only through unchanged or metadata-only edit provenance and export,
and any execution-affecting edit fails closed.
Exact `3.1.0` explicitly writes the command file as UTF-8. Exact `3.1.1`–`3.1.9`
and `3.2.0` through `3.3.2` instead encode it with the worker JVM's default
charset, so the shared contract remains ASCII and requires an eligible
Unix-like worker with the selected executable and
PyTorch dependencies already present. Exact `3.3.1` and `3.3.2` typed
compilation requires outer `timeout: 0`: their executor can block on output
before the timeout branch and plugin cancel is a no-op; `3.3.2` can also call
`exitValue()` on a live process. Upstream INFO-logs full params and commands,
does not transport task output or persist a durable app id, cannot reattach
after failover, and reruns the whole script on retry. Earlier cancellation is
outer-worker best effort. This review adds no live evidence and promotes no
profile.

`LINKIS` is source-present on exact `3.2.0` through `3.4.3`, but those nine
coordinates are reviewed preserve-only runtime exclusions. The stock plugin
submits `linkis-cli` asynchronously and parses `TaskResponse.resultString` for
the task id and status, although the command-executor path never populates that
field. It can fail after remote submission and before durable `appIds`
persistence; the shared remote-task lifecycle also performs only one status
check rather than polling to a terminal state. Consequently `dsctl` exposes
neither typed nor raw LINKIS create/edit. Existing native LINKIS task params
may survive an unchanged or metadata-only edit and export, but changing them
fails closed. Cancellation cannot reliably recover the missing id, and retry
or failover can duplicate or orphan remote jobs. This decision adds no live
evidence and promotes no profile.

`WATERDROP/literal_local_config_job` is typed on exact `2.0.1`–`2.0.9`. Canonical
create/edit owns one absolute ASCII shell-safe DS FILE `configResource`; the
workflow service resolves it to one visible, non-directory, positive resource
id. Projection fixes `localParams=[]`, emits that id in a one-entry
`resourceList`, and emits exactly one local/client/default launcher line. Its
`--config` argument removes exactly one leading `/` from the resource
`fullName`, and the launcher uses shell-expanded `$WATERDROP_HOME` rather than
a DolphinScheduler `${...}` placeholder. Multi-config and remote modes,
variables, parameters, output, custom queue/script state, and future fields are
outside typed and raw opaque create/edit; richer existing native state is
unchanged/export preserve-only.

Exact `2.0.0` is source-, model-, and UI-present but the stock worker does not
register `WATERDROP` as an alias of the `SHELL` task channel, so execution fails
before `ShellTask` construction. Typed and raw opaque create/edit therefore
fail closed there, while existing server state remains preservable. Exact
`2.0.1` adds the alias. Eligible workers require a POSIX shell,
`WATERDROP_HOME`, a compatible foreground Waterdrop/Spark launcher, tenant and
resource permissions, and target connectivity. Upstream INFO logs parameters,
script, paths, command, and child output; fields are not secret storage. The
task has no structured output, durable application id, or failover
reattachment; cancellation is worker-local best effort and retry/failover may
replay the whole job. This review refreshes no live evidence and promotes no
profile.

`DYNAMIC/literal_single_dimension_fanout` is typed only on exact `3.2.2`.
Canonical create/edit owns one literal same-project `childWorkflowName`, one
non-`system.*` literal `parameterName`, between
one and 1,024 ordered unique comma-free literal `values` whose complete
comma-joined native spelling is at most 256 UI/JavaScript UTF-16 code units,
and a positive
`degreeOfParallelism` no greater than the value count. Projection emits one
comma-delimited `listParameters` entry, fixes `filterCondition=""`, and derives
`maxNumOfSubWorkflowInstances` from the value count. Exact empty
`localParams=[]`, `resourceList=[]`, or `listParameters.disabled=true` UI
residue is accepted only while decoding; nonempty or richer native state remains unchanged/export
preserve-only, and raw opaque create/edit is closed.

Before live workflow create/edit, dsctl resolves the child name to a positive
native code in the selected project, proves it is not the containing workflow,
and proves that its reachable
exact `DYNAMIC`/`SUB_PROCESS` closure is acyclic. Declared parent globals may
not collide with `parameterName`, and ordinary task retry must remain zero;
runtime-supplied start parameters and post-authoring graph drift remain
operator prerequisites. Parent schedule time/timezone are not forwarded.
Upstream INFO-logs every generated group and `dynamic.out(taskName)`, including
child output/global values. Workflow recovery and failover can replay child
effects, partial persistence can orphan or duplicate work, cancellation is
best effort, and pause does not cascade. Exact `3.2.0` and `3.2.1` remain
reviewed runtime exclusions because child commands omit the tenant; `3.2.1`
also dereferences missing child start parameters. This review refreshes no
live evidence and promotes no profile.

`BLOCKING/same_workflow_state_gate` is typed only on the twenty exact profiles
from `3.0.0` through `3.2.2`; `BLOCKING` is upstream-absent from `3.3.1`.
Canonical create/edit owns `BlockingOnSuccess` or `BlockingOnFailed`, strict
`alertWhenBlocking`, and a nonempty two-level `AND`/`OR` tree of literal
same-workflow task-name `SUCCESS`/`FAILURE` predicates. The compiler resolves
each name to a positive `depTaskCode` and adds a real DAG predecessor edge.
The upstream tags expose no BLOCKING authoring form; this typed surface is
REST-only. Raw opaque create/edit is closed. Richer native state survives only
from an existing-server baseline during unchanged or metadata-only edits;
standalone YAML carries no opaque provenance and cannot independently reapply
it. Closed-looking native state is promoted to typed only when every predicate
already has a matching acyclic workflow relation; missing, self, or cyclic
relation evidence remains opaque. The task itself completes `SUCCESS`. A
matching gate moves the workflow
through `READY_BLOCK` to terminal `BLOCK`; a miss continues without ordinary
task retry, and `READY_BLOCK` normally waits for active and retry work to
drain. Exact `3.0.0` through `3.0.6` mark standby work `KILL`, while later reviewed
versions mark it `PAUSE`. `alertWhenBlocking` requests a blocking alert record
for the workflow `warningGroupId`; delivery requires a valid alert group and
alert infrastructure, which this facet does not validate. Upstream INFO logs
task codes, expected and actual states, aggregate results, and the blocking
opportunity, so fields are not secret storage. The master-local task has no
worker, output, application id, or dedicated failover resume. Pause/kill only
changes local task state through `3.1.9` and is warn/no-op on `3.2.x`; neither
epoch has a remote target. Infrastructure replay or workflow rerun may
reevaluate and repeat the alert. This review refreshes no live evidence and
promotes no profile.

- source-known task/facet coordinates outside that table retain opaque
  preservation and only their explicitly supported raw opaque-authoring
  intents; source discovery does not authorize typed create/edit or imply
  typed support for all discovered plugins;
- `SQL/resource_file` exists on `3.4.2` and `3.4.3` and remains
  opaque-preserve-only; public typed create/edit is unavailable;
- exact fingerprint reuse is evidence only when recorded in the review ledger;
  it is not an automatic nearest-version inheritance rule;
- canonical `SUB_WORKFLOW` compiles to native `SUB_PROCESS` through `3.2.2`;
  export maps representable native payloads back to the canonical name, while
  unrepresentable runtime evidence remains lossless opaque state;
- exact `1.3.9` is a separate service-resolved nested-workflow identity epoch.
  Canonical YAML accepts exactly one nonblank literal
  `task_params.childWorkflowName`; the service treats it as an explicit
  same-project name even when it is all digits, resolves it to a positive
  native id, and freezes that binding. The legacy graph compiler emits the
  exact one-field `SUB_PROCESS.params.processDefinitionId` package. Public raw
  opaque create/edit is closed. Modern workflow codes,
  caller-supplied native ids, placeholders, `localParams`, resources,
  `varPool`, richer state, and cross-project references are not typed input.
  After the final native graph is compiled, an iterative runtime-equivalent
  audit follows every task `params.processDefinitionId`, regardless of task
  type, through the descendant closure and rejects malformed runtime edges,
  missing definitions, permission failures, or cycles. Legacy detail codes
  `50001` and `50003` both normalize to not-found. The closure is limited to
  1,000 loaded descendants; overflow is a stable `user_input_error`, while a
  descendant permission failure remains `permission_denied`, and neither case
  mutates DS. Read-side hydration reverses only immediate exact one-field
  packages whose ids resolve through one same-project identity inventory; it
  does not inspect grandchildren. Placeholder-bearing or ambiguous names,
  unresolved ids, and richer packages stay opaque. Canonical describe/export
  emit `childWorkflowName` only for safely reversed tasks, while `workflow
  run-task` and task-scoped `workflow backfill` do not hydrate child names;
- exact `1.3.9` `SUB_PROCESS` execution is master-local. The parent/child map
  and child command are persisted transactionally, and the subsequent child
  link is persisted in its own transaction and lets recovery reuse the linked
  process. Parent workflow globals feed the child, while a same-name child
  global wins; task `localParams` are ignored. Pause and stop propagate to the
  child, but effective cancellation still depends on its tasks. No structured
  output or durable worker
  application id exists, and child retry or `REPEAT` can replay side effects.
  Descendants can mutate after authoring, so current existence, acyclicity,
  and release readiness remain operator prerequisites. This review adds no
  live evidence or profile promotion;
- `DEPENDENT` has reviewed typed membership on all 37 exact profiles. Exact
  `1.3.9` owns a service-resolved split-wire epoch: canonical YAML contains a
  nonempty two-level `AND`/`OR` tree of literal cross-project workflow or task
  names. Numeric-looking names remain names. The service resolves positive
  project/workflow database IDs and verifies task membership before the graph
  compiler emits `params={}` beside outer `TaskNode.dependence`.
  Workflow targets use `depTasks=ALL`; task targets use the literal task name.
  These references do not create workflow DAG edges. Placeholders, parameters,
  resources, datasource state, native IDs, richer fields, and public raw opaque
  create/edit are excluded. Workflow-definition reads reverse-bind only
  unambiguous closed packages. Unresolved or richer state remains opaque and
  requires a server baseline; the opaque task's name, type, task parameters,
  and command must stay unchanged, while description edits and unrelated
  topology additions are allowed. Standalone YAML is not a lossless
  preservation carrier. Workflow-instance edits resolve typed names, but
  instance read/export keeps this state opaque;
- exact `DEPENDENT` date projection is base-only on `1.3.9`, `2.0.0`–`2.0.9`,
  `3.0.0`–`3.0.1`, and `3.1.0`. `thisMonthBegin` and `thisMonthEnd` join on
  `3.0.2`–`3.0.6`, `3.1.1`–`3.1.9`, and `3.2.0` through `3.4.3`. Every `cycle`/`dateValue` pair must
  match the selected profile; the runtime selects the actual window from
  `dateValue`, not `cycle`. The task runs on the master, logs target identity,
  window, and state at INFO, publishes no output, and has no durable remote app
  ID or reconnectable cancel/resume target. Scheduled retry/failover retains
  the original `scheduleTime`; manual/current-time reinitialization can shift
  the effective window. Authoring checks target existence, but upstream does
  not attest target-project authorization, and later target rename, deletion,
  or state change remains a runtime prerequisite. This review adds no live
  evidence or profile promotion;
- canonical `PIGEON` authoring exists only from `2.0.0` through `3.2.2` and
  owns only a nonblank `task_params.targetJobName`; the worker must also
  resolve `p_host` at execution time, so schema and template output surface
  that runtime prerequisite without inventing it as a static task field;
  typed create/edit rejects inherited and future fields, while opaque
  preservation retains them losslessly;
- `CONDITIONS` has reviewed typed membership on all 37 exact profiles. Exact
  `1.3.9` is its own name-routed wire epoch: canonical `task_params` accepts
  only an `AND`/`OR` `dependence` tree with a nonempty `dependTaskList`, a
  nonempty `dependItemList` in every `AND`/`OR` group, and predicates containing
  a same-workflow `task` name plus `SUCCESS` or `FAILURE`, plus a
  `conditionResult` whose `successNode` and `failedNode` each contain exactly
  one same-workflow task name and the two names differ. The projector writes
  predicate names as native `depTasks` inside outer `TaskNode.dependence` and
  writes the branch names inside outer `TaskNode.conditionResult`; both remain
  siblings of `TaskNode.params`, and no task-code conversion or flattening is
  allowed.
  Exact `1.3.9` typed authoring has no placeholders, `localParams`, `varPool`,
  resources, datasource, or other task parameters; the native `params` object
  is exactly `{}`. Predicate and branch references also become the workflow's
  predecessor and successor edges, so unknown names, self-references, cycles,
  or extra successors fail before transport. The task runs entirely on the
  master, INFO-logs task names, expected/actual states, and the aggregate
  result, and selects a named branch from persisted process-instance state. It
  has no worker application id, credential/resource resolution, remote cancel,
  or remote resume target; failover and retry can only reevaluate persisted
  state from the same process instance. Richer or unknown native `params`,
  `dependence`, `conditionResult`, and future state are not typed-authoring
  input. They are retained only when an edit has an existing server baseline
  and leaves the `CONDITIONS` payload, task names, and topology unchanged,
  including a metadata-only edit. Standalone YAML export does not carry the
  split outer opaque fields and
  is not a lossless preservation carrier. No raw opaque create/edit selector is
  exposed, and invalid typed input does not downgrade to preservation. This
  review refreshes no live evidence or promotion;
- `HTTP` has reviewed typed membership on all 37 exact profiles. Exact `1.3.9`
  canonical authoring has no `httpBody`, `varPool`, or explicit
  `socketTimeout`; exact projection injects native `socketTimeout=60000`, while
  `connectTimeout` stays explicit. Exact `1.3.9` typed `localParams` are
  `IN`-only and use that release's nine native scalar data types; `OUT`
  parameters plus non-exact/future `LIST` and `FILE` raw shapes remain
  unchanged/export opaque state.
  Prepared-parameter substitution covers the URL and every property. INFO logs
  include the complete task params, substituted request params/properties,
  configured/original URL (not a
  claimed final or expanded URL), status, and complete response body. These
  fields are not secret storage, and the CLI does not redact them. Native
  `HEAD`, the `BODY` property kind, nondefault socket timeout, and future native
  members remain unchanged/export opaque preservation state; none belongs to
  typed authoring. There is no reliable cancel, durable id, failover resume,
  or structured output. Retry resends the
  whole request, so `POST`, `PUT`, and `DELETE` side effects can duplicate.
  This review supplies no live evidence or promotion;
- `PYTHON` has reviewed typed membership on all 37 exact profiles. Exact
  `1.3.9` owns `rawScript`, unique `IN`-only `localParams` using its nine
  scalar data types, plus an empty `resourceList`; it has no `varPool`, `LIST`,
  `FILE`, or structured output. Process-definition authorization checks only
  positive resource IDs, while the master's legacy `id=0`/`res` full-name
  branch resolves a tenant without that user permission context before the
  worker downloads the resource. Typed authoring
  therefore exposes no 1.3.9 resource attachment or `resource` variant. Every
  nonempty native resource shape, `OUT` parameter, inherited member, and future
  field is unchanged/export opaque preservation. The
  worker normalizes CRLF, applies prepared substitution without Python
  escaping, writes UTF-8, and resolves `PYTHON_HOME` with a `python` fallback.
  It INFO-logs complete params, original and substituted script, command, and
  stdout/stderr; these values are not secret storage and the CLI does not
  redact them. Cancel is local best-effort, no Python process can be durably
  resumed, and retry or worker failover reruns the entire script and may repeat
  side effects. This review adds no live evidence or promotion;
- `MR/literal_java_jar_job` has reviewed typed membership in `Universal` on all
  37 exact profiles. Canonical `task_params` owns only required `mainJar`, an
  absolute shell-safe DS resource `fullName` ending in `.jar`; required
  `mainClass`, a strict dotted ASCII Java class; and optional `mainArgs`, an
  ordered list of shell-safe tokens defaulting to `[]`;
- through `3.1.9`, the service resolves `mainJar` to an exact visible,
  non-directory `FILE` with a positive id before mutation and emits
  `mainJar={id}`. Safe reads reverse-resolve that id best-effort and preserve
  unresolved state opaquely. From `3.2.0`, compilation emits
  `mainJar={resourceName}` and `yarnQueue=""`. Every epoch fixes
  `programType=JAVA`, `appName=""`, `others=""`, `localParams=[]`, and
  `resourceList=[]`; it does not duplicate the JAR because the MR model adds
  `mainJar` to its runtime staging list. Queue selection remains
  runtime-owned;
- native `SCALA` is selector-restricted explicit opaque create/edit. Public
  PYTHON opaque authoring is closed despite its UI option because every
  reviewed executor builds `hadoop jar`. Richer JAVA state, legacy
  `id=0`/`res`, inherited fields, and future state remain unchanged/export
  opaque-preserve-only. Eligible workers need Hadoop/YARN, tenant execution
  permission, DS resource access, queue permission, and target-data access;
- upstream INFO-logs complete task parameters, command files, final commands,
  and child output. MR publishes no DS output or durable callback identity;
  cancellation is best-effort from an observed application id, failover cannot
  reattach, and retry/failover can rerun the entire JAR. Exact `3.3.1` onward
  also has a post-exit `appIds` context transport hole. This review adds no
  live evidence, changes no `tested` flag, and promotes no profile;
- `SQOOP/literal_command` has reviewed typed membership in `DataIntegration`
  on all 37 exact profiles. Canonical `task_params` owns only required
  `subcommand: import|export` and a nonempty ordered `args` list. Compilation
  fixes `jobType=CUSTOM`, `localParams=[]`, and one POSIX-quoted
  `customShell` beginning with `sqoop`;
- arguments reject edge whitespace, controls, surrogates, DS placeholders,
  interactive `-P`, standalone `--password`, and `--password=...`; use a
  worker-readable `--password-file`. Empty argument tokens are allowed and
  project reversibly as POSIX `''`; the ordered `args` list itself remains
  nonempty. Releases through `3.1.0` write UTF-8. Exact `3.1.1` and newer use
  the platform-default charset and therefore restrict typed arguments to
  ASCII. CRLF becomes LF through `3.1.9` and the worker OS line separator from
  `3.2.0`;
- native TEMPLATE jobs and arbitrary CUSTOM scripts are selector-restricted
  explicit opaque create/edit; richer state is unchanged/export preserve-only.
  Workers need POSIX shell, Sqoop, Hadoop/YARN, JDBC drivers, tenant permission,
  credentials, connectivity, and data access. Command-bearing state is logged
  at INFO and is not secret storage. SQOOP has no DS output, durable application
  id, or failover reattachment; cancellation is best-effort and retry/failover
  can duplicate the transfer. This review adds no live evidence, changes no
  `tested` flag, and promotes no profile;
- exact `1.3.9` `SQL` uses a dedicated closed inline contract. Typed
  `task_params` owns `MYSQL`, `POSTGRESQL`, `HIVE`, `SPARK`, `CLICKHOUSE`,
  `ORACLE`, `SQLSERVER`, or `DB2` as datasource `type`, positive `datasource`,
  literal nonblank `sql`, strict query/update `sqlType`, nonnegative
  `displayRows` and `limit`, unique `IN`-only `localParams` from the release's
  nine scalar types, and nonblank `preStatements`/`postStatements`.
  `connParams` must be empty except for HIVE, where it is a literal
  semicolon-separated `key=value` map with unique nonblank keys and nonblank
  values. Exact
  projection forces `sendEmail=false` because absent or null enables upstream
  mail, and emits empty UDF/mail state plus `showType=TABLE`; `groupId` and
  `varPool` do not exist on this wire. Mail, UDF, `OUT`, export-decoration,
  inherited, and future state remain selector-restricted explicit opaque
  authoring when supplied as one complete native parameter package, or
  unchanged/export preservation. Partial invalid typed input never selects
  opaque mode. Workflow create/update does not authorize
  the datasource before the master resolves it by id, so existence,
  authorization, and type agreement remain caller prerequisites. There is no
  transaction across pre/main/post SQL, durable application id, structured
  output, or reliable JDBC cancellation. Retry or worker failover replays the
  whole sequence, and INFO logs disclose full params, SQL, HIVE `connParams`,
  bound values, and result rows. The review adds no live evidence or
  promotion;
- `SPARK/inline_local_sql` is typed on twenty-six exact profiles from `3.0.0`
  through `3.4.3`. The SPARK plugin exists on profiles through `2.0.9`, but
  its upstream parameter model has no SQL program mode there, so that facet is
  absent and the plugin remains generic opaque;
- its canonical payload owns exactly one nonblank literal `rawScript` string
  and preserves its canonical and wire spelling. Typed create/edit rejects
  `${...}`, `$[...]`, unsafe control text, and every other field rather than
  accepting a partially understood native Spark payload;
- exact compilation emits `programType=SQL`, `sparkVersion=SPARK2`, and
  `deployMode=local` through `3.1.9`; exact `3.2.0` and `3.2.1` replace
  `sparkVersion` with `sqlExecutionType=SCRIPT`; and `3.2.2` through `3.4.3`
  additionally emit `master=local`;
- native `JAVA`, `SCALA`, and `PYTHON` programs select explicit opaque
  authoring on the reviewed profiles, including their complete native cluster
  and resource payloads. SQL `FILE` does the same from `3.2.0`. Cluster or
  resource fields added to inline SQL `SCRIPT`, inherited runtime members, and
  future fields outside a recognized opaque mode remain unchanged/export
  preservation only. Invalid inline SQL and unknown modes never downgrade to
  an opaque public intent;
- exact `3.0.x` writes the SQL file before final-command substitution, while
  `3.1.0` and newer expand SQL from the prepared parameter map first. The
  stable canonical subset rejects placeholders on every reviewed profile so
  one authored value cannot acquire a version-dependent meaning;
- SPARK workers use `SPARK_HOME2` through `3.1.9` and `SPARK_HOME` from
  `3.2.0`, and require Spark SQL, Java, Hadoop, Hive catalog configuration, and
  target-data permissions. Upstream logs the raw or expanded SQL at INFO,
  normalizes CRLF before writing the worker SQL file, and runs one synchronous
  local process. Runtime line endings therefore need not be byte-identical to
  the canonical wire. There is no durable application id or failover resume.
  Cancellation controls the local process, while retry reexecutes the complete
  SQL and may repeat side effects. This review adds no
  live evidence, changes no `tested` flag, and promotes no profile;
- `FLINK/inline_local_sql` is typed on twenty-four exact profiles:
  `3.0.0`–`3.0.6`, `3.1.2`–`3.1.9`, and `3.2.0` through `3.4.3`. The `1.3.9` and `2.0.x`
  models have no SQL `ProgramType`. Exact `3.1.0` is an explicit hole because
  its executor resolves `mainJar` unconditionally before SQL initialization;
  `3.1.1` adds the guard but reverses LOCAL/CLUSTER targets; `3.1.2` repairs
  target selection. The `3.1.1` hole allows only explicitly selected native
  opaque modes, while canonical local-inline input fails closed;
- its canonical payload owns exactly one nonblank literal `rawScript` string
  and preserves its spelling. Exact compilation supplies
  `programType=SQL`, `deployMode=local`, `initScript=""`, and the unchanged
  `rawScript` on every typed profile;
- exact `3.0.x` writes SQL using the platform-default charset, so its typed
  schema permits ASCII only. Exact `3.1.2` and newer write UTF-8. Typed
  create/edit rejects blank scripts, carriage returns, DEL/C0/C1 controls,
  `${...}`, and `$[...]`, while accepting multi-statement SQL, TAB/LF, quotes,
  semicolons, backslashes, backticks, and `$()` without normalization;
- recognized native `JAVA`, `SCALA`, and `PYTHON` programs and exact non-local
  SQL modes select explicit opaque create/edit. Cluster, resource,
  initialization, option, inherited, and future fields attached to local
  inline SQL, plus unrecognized native state, remain unchanged/export
  preservation only. Invalid local inline SQL never downgrades to opaque;
- workers through `3.2.2` resolve `sql-client.sh` from `PATH`; releases from
  `3.3.1` use `FLINK_HOME/bin/sql-client.sh`. Exact `3.4.2` and `3.4.3` substitute the
  prepared parameter map before writing SQL, but the portable typed contract
  rejects placeholders on every release. Workers need the Flink SQL client,
  Java, connector and catalog configuration, and target-data permissions;
- upstream logs FLINK task parameters, SQL content and file paths, and the
  command at INFO, so the task is not secret storage. Local SQL exposes no DS
  task output, durable application id, or failover resume. Retry reexecutes the
  whole SQL and may repeat side effects. This review adds no live evidence,
  changes no `tested` flag, and promotes no profile;
- `FLINK_STREAM/inline_local_sql` is typed only on exact `3.1.5`–`3.1.9` and `3.2.0`
  through `3.2.2`. The plugin is upstream-absent through `3.0.6`.
  Exact `3.1.0`–`3.1.4` retain the task-specific unconditional `mainJar`
  dereference before SQL initialization. They remain typed holes; `3.1.1`–`3.1.4`
  require an explicit native opaque selector. From `3.3.1` through `3.4.3`, the plugin remains
  registered but `ExecutorServiceImpl.execStreamTaskInstance` immediately
  throws `Not supported`; those runtime holes likewise retain generic opaque
  authoring but no typed facet;
- its canonical payload owns exactly one nonblank literal `rawScript`. Exact
  compilation supplies only the four native `taskParams` fields
  `programType=SQL`, `deployMode=local`, `initScript=""`, and unchanged
  `rawScript`. It separately emits top-level `taskExecuteType=STREAM`; that
  discriminator is compiler-owned rather than a fifth task-parameter or an
  authored YAML field. Every typed profile writes UTF-8;
- typed create/edit rejects blank scripts, carriage returns, DEL/C0/C1
  controls, unpaired Unicode surrogates, `${...}`, `$[...]`, and every extra
  field. Multi-statement SQL, TAB/LF, quotes, semicolons, backslashes,
  backticks, and `$()` retain their literal spelling;
- recognized native `JAVA`, `SCALA`, and `PYTHON` JAR programs and exact
  non-local SQL modes select explicit opaque create/edit. Local-inline extras,
  inherited or future fields, and unrecognized native modes remain
  unchanged/export opaque-preserve-only. Invalid local SQL never downgrades to
  opaque authoring;
- all eight typed releases resolve `sql-client.sh` from `PATH` and reject
  placeholders before worker execution. Later `FLINK_HOME` and prepared-map
  substitution plugin epochs do not widen typed membership because their
  STREAM execution entry is unsupported. Workers need the Flink SQL client,
  Java, connector and catalog configuration, and target-data permissions;
- upstream logs FLINK_STREAM task parameters, SQL content and file paths, and
  commands at INFO, so the task is not secret storage. Local SQL is expected
  to publish no application id and exposes no result output, durable submit
  identity, or failover resume; plugin cancel and savepoint require an
  application id. Stop order is plugin cancel then PID-tree kill on `3.1.x`,
  PID-tree kill then plugin cancel from `3.2.0` through `3.2.2`, and plugin
  cancel only from `3.3.1` through `3.4.3`; the final epoch returns on the
  missing id without a process fallback. Reliable stop is unsupported, so an
  unbounded stream may continue after the DS task stops. Retry reexecutes all
  SQL on typed releases and may repeat side effects. This review adds no
  live evidence, changes no `tested` flag, and promotes no profile;
- `K8S/literal_container_job` is typed in `Cloud` on fifteen exact profiles
  from `3.1.4` through `3.4.3`. Profiles through `3.0.6` are upstream-absent.
  Exact `3.1.0`–`3.1.3` remain runtime holes: its watcher counts down on a `RUNNING`
  event while the response exit status is still the default `-1`. Raw opaque
  create/edit is selector-restricted to a native payload with no canonical
  `connectionMode` or sibling `cluster`, a nonblank `image`, and compact
  `namespace` JSON containing nonblank `name` and `cluster`; canonical
  connection intent fails closed, and the default template is an explicit risk scaffold;
- the canonical payload always owns one literal image, finite nonnegative CPU
  and MiB-memory requests, and literal input environment entries with unique
  names. The outer task name must match
  `^[A-Za-z0-9][A-Za-z0-9-]{0,51}$`; uppercase is
  valid because runtime lowercases with `Locale.ROOT` before appending `-` and
  the decimal task-instance id. Exact
  `3.1.4` through `3.2.2` require `connectionMode=NAMESPACE`, a DNS-1123
  `namespace`, and a literal `cluster`, projected together as one compact
  native namespace JSON string. Exact `3.2.2` checks only the image and its
  upstream UI hides the namespace selector, but its backend, master, and
  runtime still require the legacy namespace wire. Exact `3.3.1` and newer
  instead require `connectionMode=DATASOURCE` plus a positive K8S datasource
  id; compilation fixes `type=K8S` and empty `namespace`/`kubeConfig` fields
  for worker-side overwrite;
- exact `3.2.0` and newer additionally own structured `command`, `args`, an
  optional pull-secret object name, explicit image pull policy, unique valid
  custom labels excluding upstream-owned keys, and structured node selectors.
  Custom-label values may be empty. Exact `3.2.0` applies custom labels to the
  Job only; `3.2.1` and newer apply them to both Job and Pod template. Repeated
  selector keys are valid AND clauses. `In`/`NotIn` require a nonempty list of
  unique nonempty Kubernetes label values;
  `Exists`/`DoesNotExist` require an empty list, while `Gt`/`Lt`
  require one decimal-integer string in the list, from zero through
  `9223372036854775807`.
  Exact `3.2.0` requires a nonempty custom-label list because its executor
  mutates an immutable empty label map. Exact `3.1.9` instead uses the image
  ENTRYPOINT/CMD, fixes `imagePullPolicy=Always`, and exposes none of these
  advanced fields;
- typed `outputs` exist only on `3.2.0`, `3.2.1`, `3.2.2`, `3.4.2`, and `3.4.3` and
  compile separately from input environment as `OUT`/`VARCHAR`/empty-value
  `localParams`. `3.2.0` parses the terminal `dsVal` marker; `3.2.1`, `3.2.2`,
  `3.4.2`, and `3.4.3` parse `setValue`. Published values must be nonempty on `3.2.0`
  and `3.2.1`. The `3.2.0` parser reserves `$VarPool$` as a delimiter and
  keeps only the first `=` segment; `3.2.1` preserves later `=` characters.
  Exact `3.2.2`, `3.4.2`, and `3.4.3` preserve `=` and accept empty published values.
  Exact `3.3.1` through `3.4.1` have a physical-executor transport hole and
  expose no typed output declarations. Exact `3.4.2` restores transport, but
  its OUT declarations also enter the prepared map and are injected into the
  Pod as empty-valued environment entries;
- the worker injects every prepared value into the Pod environment, so valid
  Kubernetes names and freedom from `taskInstanceId` collisions for global,
  built-in, and inherited keys are caller prerequisites. Job watch
  registration omits the namespace; the kubeconfig current context must match
  the selected target. Exact `3.3.1` through `3.4.1` additionally omit the
  resolved namespace from Pod-log/output lookup. From `3.3.1`, upstream writes
  datasource-resolved kubeconfig into task params and INFO-logs the resolved
  object. These values are not secret storage, and dsctl does not redact them;
- image tags are mutable; use a digest when immutability is required. The
  plugin publishes no durable application id or failover-resume identity.
  Cancel needs the same worker's in-memory Job, while retry or worker loss can
  duplicate execution and side effects. Typed coordinates expose no raw opaque
  create/edit selector; richer native state survives only through unchanged
  edit or export provenance. This review adds no live evidence, changes no
  `tested` flag, and promotes no profile;
- `KUBEFLOW/tfjob_manifest` is typed in `MachineLearning` on exact `3.2.0`
  through `3.4.3` and upstream-absent earlier. Canonical authoring owns only
  `namespace`, `cluster`, and literal `yamlContent`. It accepts one ASCII/LF,
  single-mapping-document `kubeflow.org/v1` `TFJob` with explicit matching
  `metadata.namespace`, no root `status`, and a nonempty
  `spec.tfReplicaSpecs` mapping. `generateName`, server-owned metadata (`uid`,
  `resourceVersion`, `generation`, creation/deletion timestamps and grace,
  `managedFields`, and `selfLink`). Only JSON-core scalar tags (`string`,
  `null`, `bool`, `int`, and `float`) are accepted; YAML timestamp, binary,
  set, and custom tags are rejected. Duplicate keys, aliases, merge keys,
  controls, other placeholders, multi-document and `List` input, and
  extras are rejected. The DNS-safe name prefix is at most 52 characters and
  ends with the sole allowed `${system.workflow.instance.id}` placeholder;
- projection emits only unchanged `yamlContent` and compact
  `namespace={name,cluster}` JSON. Exact empty UI `localParams=[]` and
  `resourceList=[]` residue canonicalizes on decode; richer or nonempty state
  is unchanged/export preserve-only. Typed coordinates expose no raw opaque
  create/edit;
- export is read-only. Saving richer opaque KUBEFLOW state is allowed only
  unchanged or through a description-only or task-name-only patch; a task
  rename in such a patch is metadata-only. Any task/workflow
  execution-semantic or topology edit fails closed;
- the master resolves `cluster` to kubeconfig, while fixed
  `kubectl apply/get/delete -f` execution ignores the outer namespace and uses
  the manifest's namespace. The platform-default writer substitutes prepared
  values and INFO logging exposes complete params, expanded YAML, commands,
  status JSON, and complete resolved kubeconfig on all nine versions. The
  task-log filter present on exact `3.3.1` and newer does not suppress that
  worker-service INFO event.
  On a valid status shape, success is limited to `Succeeded`, `Available`, or
  `Bound`, and failure to `Failed`. Missing `status`/`conditions` or other
  nonterminal state can continue polling until the task timeout. An existing
  empty `status.conditions` array instead makes all nine exact
  `KubeflowHelper` implementations unconditionally access its last element and
  fail at runtime; a compatible nonempty conditions protocol is a runtime
  prerequisite. There is no
  command timeout, structured output, or durable resource id. `appIds` is only
  a post-callback submission sentinel; failover can resume manifest polling
  after persistence because workflow-instance identity is stable, while the
  apply-before-callback gap can reapply. The template requires `retry.times=0`;
  retry in the same workflow observes or reapplies the same terminal TFJob and
  does not guarantee a rerun. It disables only DS task retry; the
  TFJob/Kubernetes controller and Pod `restartPolicy` can still repeat training
  work. Duplicate typed
  `(cluster, namespace, metadata.name template)` identities in one workflow are
  rejected so sibling tasks cannot apply/delete the same TFJob. Typed workflow
  compilation requires a positive native-minute `timeout` and
  `timeout_notify_strategy=FAILED` or `WARNFAILED`; omitted/default or explicit
  `WARN` only warns and does not terminate the watcher. The template sets
  `timeout_notify_strategy: FAILED` and `timeout: 60` for one hour. Kubectl,
  kubeconfig, network/RBAC, TFJob CRD/status
  compatibility, and a shell-safe task path are prerequisites. A
  canonical-decoding server baseline with legacy outer retry, timeout, or
  `WARN` remains unchanged only through a description-only patch or a file edit
  that leaves every execution-affecting task field untouched. Changing type,
  params, command, flag, worker/environment selection, task group/priority,
  retry/timeout/strategy/delay, resource limits, dependencies, or workflow-level
  execution settings—including `release_state` transitions such as `OFFLINE`
  to `ONLINE`, workflow timeout, `execution_type`, and global parameters—reruns
  the applicable gates;
  standalone YAML has no provenance exception. No live preflight, receipt
  refresh, tested-state change, or profile promotion is
  claimed;
- Recovery-style REPEAT/RERUN paths can reuse the same `workflowInstanceId`
  and rendered `metadata.name`; same-id recovery may reobserve an already
  successful TFJob without new training. `REPEAT` does not guarantee fresh
  training. Only creation of a new workflow instance provides a fresh identity.
  `dsctl workflow-instance rerun`, `recover-failed`, and `execute-task` now fail
  closed by default when the available instance `dagData` contains any
  `KUBEFLOW` task; use `dsctl workflow run WORKFLOW --project PROJECT` to create
  a new instance. This protection cannot constrain the UI or direct REST, and
  makes no detection claim without `dagData`;
- `JAVA/literal_fat_jar` is typed in `Universal` on exact `3.2.0`, `3.2.2`,
  and `3.3.1` through `3.4.3`. Earlier profiles through `3.1.9` do
  not register JAVA. Exact `3.2.1` remains an explicit runtime hole: resource
  staging records an absolute local JAR path, then `JavaTask` prefixes both
  `mainJar` and `resourceList` paths with `executePath` again; `3.2.2`
  removes those duplicate prefixes. A materialized fail-closed policy allows
  opaque create/edit there only for a nonblank native `runType=JAVA` source
  payload. Broken `JAR` state is opaque-preserve-only, and canonical
  `mainJar`/`mainArgs` input never downgrades to opaque;
- canonical typed `task_params` owns only required `mainJar`, an absolute
  shell-safe DS resource `fullName` ending in `.jar`, and optional `mainArgs`,
  a list of shell-safe tokens defaulting to `[]`. Compilation duplicates the
  same `ResourceInfo` into `mainJar` and `resourceList` because the worker
  downloads only `resourceList`, joins `mainArgs` with one space, and fixes
  `jvmArgs=""`, `isModulePath=false`, and `localParams=[]`. Exact `3.2.0` and
  `3.2.2` additionally emit `runType=JAR` and `rawScript=""`; exact `3.3.1`
  onward emit `runType=FAT_JAR` and `mainClass=""`;
- the legacy `runType=JAVA` source mode and modern `NORMAL_JAR` mode remain
  selector-restricted opaque authoring. Module path, JVM arguments,
  parameters/output, extra dependencies, placeholders, and future fields are
  outside typed ownership and survive only through unchanged/export
  preservation. Native `jvmArgs` is placed after the target and application
  arguments through `3.4.0`; `3.4.1` repairs that order and substitutes the
  final command, but the shared facet still fixes it empty. Eligible workers
  need `JAVA_HOME`, a compatible JDK, tenant execution permission, and DS
  resource access. Upstream INFO-logs the complete params, final command, and
  child output. Cancellation on `3.2.x` destroys only the direct process and
  can leave child processes behind; `3.3.1` onward kills the local process
  tree and attempts generic application cancellation. JAVA publishes no typed
  DS output or durable application id, cannot reattach after failover, and
  retry reruns the whole JAR with possible duplicate side effects. This review
  adds no live evidence, changes no
  `tested` flag, and promotes no profile;
- `DATA_FACTORY/pipeline_trigger` is upstream-absent through `3.1.9` and typed
  in `Cloud` on the nine exact profiles from `3.2.0` through `3.4.3`. Its
  canonical and native wire payloads contain exactly the same three required
  literal strings: `factoryName`, `resourceGroupName`, and `pipelineName`;
- those identity values are preserved without renaming or normalization and
  reject edge whitespace, control or surrogate text, `${...}`, and `$[...]`.
  Typed create/edit rejects Azure-derived runtime `runId`, `localParams`,
  `varPool`, `resourceList`, inherited state, and future fields. No public
  opaque create/edit selector exists; excluded or unrecognized native state is
  retained only by unchanged/export opaque-preserve provenance;
- every eligible worker must configure `resource.azure.client.id`,
  `resource.azure.client.secret`, `resource.azure.subId`, and
  `resource.azure.tenant.id`. Upstream logs task parameters, including the
  complete three-field identity, at INFO but does not log those credential
  values. The plugin performs no placeholder substitution, accepts no Azure
  pipeline runtime parameters or resource files, and publishes no DS task
  output;
- status polling uses worker property `resource.query.interval`, defaulting to
  `10000` ms. A post-submit callback stores Azure `runId` in task-instance
  `appIds`; once durable, failover skips resubmission and resumes polling or
  cancellation against the same Azure run. If `createRun` succeeds before the
  callback persists `appIds`, failover or a later DS retry can submit a
  duplicate pipeline run. The plugin itself does not retry `createRun`. This
  review adds no live evidence, changes no `tested` flag, and promotes no
  profile;
- `CHUNJUN/literal_local_json_job` is upstream-absent through `3.0.6` and typed
  in `Other` on all nineteen exact profiles from `3.1.0` through `3.4.3`. Its closed
  canonical model owns only required `task_params.json`, preserves accepted
  source spelling, and requires one literal JSON object;
- typed CHUNJUN rejects blank or malformed JSON, array/scalar roots,
  nonstandard numeric constants, duplicate keys at any depth, placeholders,
  CR, decoded controls or unpaired surrogates, and every additional canonical
  field. Exact projection always emits only strict integer `customConfig=1`,
  unchanged `json`, and `deployMode=local`. All nineteen upstream executors support
  `localParams` and prepared-map placeholder substitution; typed canonical
  authoring deliberately excludes them because replacement is not
  JSON-escaped, not because substitution is runtime-unsupported;
- native nonlocal opaque create/edit is selector-restricted to strict integer
  `customConfig=1` plus exact `standalone`, `yarn-session`, or `yarn-per-job`.
  Built-in `customConfig=0`, the UI typo `standlone`, local `others` or
  parameter/resource state, dormant datasource-generation fields, and future
  state remain unchanged/export opaque-preserve-only. Built-in mode is a
  runtime hole because `ChunJunTask.buildChunJunJsonFile` never constructs its
  JSON. Exact `3.1.0`–`3.1.6` UI default `customConfig=false` is a UI defect; the
  compiler/REST strict integer `1` local wire remains runnable, and UI defaults
  are not used to infer one uniform exact wire;
- every executor epoch normalizes CRLF to LF, performs unsafe prepared-value
  substitution without JSON escaping, and writes UTF-8. Exact `3.1.0` uses a
  legacy shell file and nondurable post-exit application-id log discovery;
  `3.1.1`–`3.1.9` keep the legacy path but write empty `appIds`; `3.2.0` through
  `3.3.2` use the shell interceptor with empty `appIds`; `3.4.0` through
  `3.4.3` use `taskRequest` with empty `appIds`. Every exact upstream
  `chunjun.md` requires removing the trailing background `&` from the `nohup`
  command in `${CHUNJUN_HOME}/bin/start-chunjun`; workers must use that
  foreground launcher or DS status and cancellation semantics are
  untrustworthy;
- upstream INFO-logs complete task parameters and DEBUG-logs expanded JSON.
  These fields are not secret storage. CHUNJUN publishes no structured output
  or durable id and cannot resume after failover. Cancellation is worker-local
  and best-effort: `3.1.0` through `3.1.9` use legacy wrapper soft/hard kill,
  `3.2.0` through `3.2.2` direct-process destroy/force, and `3.3.1` through
  `3.4.3` process-tree kill plus generic application cancel. Retry replays the
  whole job. This review adds no live evidence, changes no `tested` flag, and
  promotes no profile;
- `DATAX/literal_custom_json_job` is typed in `DataIntegration` on 36 exact profiles: every target except exact `3.1.0`. Its closed canonical model owns
  only required `task_params.json`, preserves accepted source spelling and LF
  formatting, and requires one JSON object root;
- typed DATAX rejects blank/malformed JSON, array or scalar roots,
  nonstandard `NaN`/`Infinity` constants, duplicate keys at any depth,
  `${...}`/`$[...]` in source or decoded keys/values, CR, decoded C0/C1/DEL
  controls, decoded unpaired Unicode surrogates, and every additional field.
  Invalid typed input never downgrades to opaque authoring;
- exact `3.4.3` additionally rejects a literal empty JSON object, including
  whitespace-only object forms. Its worker treats that value as absent and
  falls back to a JSON resource, outside this literal facet. `3.4.2` still
  accepts empty object literals. Nonempty literal jobs remain typed; no file
  fallback is introduced;
- exact `1.3.9` projection sends only integer `customConfig=1` and `json`.
  Exact `2.0.0` and newer also send compiler-owned integer `xms=1` and
  `xmx=1`. Typed projection emits neither `localParams` nor `resourceList`;
- safe DATAX decode strips only exact `localParams=[]` on every profile and
  the generic UI formatter's `resourceList=[]` from `3.0.0`. Empty
  `resourceList` on `1.3.9`/`2.0.x`, any nonempty parameter/resource state,
  generated datasource mode, changed memory values, inherited state, and
  future fields remain opaque. Positive typed profiles close raw opaque
  create/edit while retaining unchanged/export preservation;
- exact `3.1.0` records exclusion
  `null-empty-prepare-params-map-breaks-custom-command`: when the global,
  local, and `varPool` sources are empty, its curing path returns a null
  prepared map and `DataxTask.addCustomParameters` dereferences it. Exact
  `3.1.1` adds the null/empty guard. Raw opaque create/edit is restricted to
  strict integer native `customConfig=0/1`; canonical JSON-only input fails
  closed, and the generic default template is a non-executable built-in-mode scaffold
  until its exact native fields are supplied; no typed default template exists there;
- workers through `3.1.9` use `PYTHON_HOME` or `python2.7` with
  `DATAX_HOME/bin/datax.py` and replace CRLF with LF. Workers from `3.2.0` use
  `PYTHON_LAUNCHER` plus `DATAX_LAUNCHER` and replace CRLF with the worker OS
  separator. All releases then substitute prepared values in the JSON and
  write UTF-8; `-p -D` prepared-map forwarding exists only from `3.1.0`.
  Upstream shell-builds that option without safely quoting workflow, startup,
  local, or `varPool` values, so positive profiles from `3.1.1` require a parameter-free workflow for the safe typed claim. Typed input forbids
  placeholders/task parameters and authors no resources;
- eligible workers need matching DataX plugins/drivers, source/target
  connectivity, and data permissions. Complete task params, the generic final
  command, and child output are INFO-logged, while DataX job/command
  construction is also DEBUG-logged. Task fields are not secret storage and
  dsctl does not redact them. DATAX publishes no structured output or durable
  id, cannot resume after failover, and replays the entire transfer on retry.
  Cancellation is worker-local: wrapper kill through `3.1.9`, direct-process
  destroy through `3.2.2`, then process-tree plus generic application cancel.
  This review refreshes no live evidence, changes no `tested` flag, and
  promotes no profile;
- `DATASYNC/create_and_execute` is upstream-absent through `3.1.9` and has
  reviewed `Other` membership on the nine exact profiles from `3.2.0` through
  `3.4.3`. Its closed public wrapper model is `DatasyncTaskParamsSpec`;
- normal typed create/edit owns exactly literal `name`, `sourceLocationArn`,
  `destinationLocationArn`, optional `cloudWatchLogGroupArn`, and strict
  `jsonFormat=false`, which is emitted even when omitted. The three identity
  fields are required. Normal values must be nonblank and reject control text,
  Unicode surrogates, `${...}`, and `$[...]`; additional fields fail closed;
- the discoverable `raw-json` variant is selector-restricted opaque create/edit.
  It requires explicit `jsonFormat=true` and exactly one nonblank syntactically
  valid JSON-object string, with no normal-mode sibling or additional wrapper
  field. The projector preserves that string. At runtime, the UpperCamelCase
  mapper deserializes only known `DatasyncParameters` and ignores unknown
  fields. Only unknown enum values in inherited `LocalParams`/`VarPool`
  `Property.Direct/Type` become `null`. DataSync-specific enum-like strings such
  as `FilterType` are neither enum-validated nor null-converted; `FilterType`
  reaches the SDK builder unchanged for AWS validation. The mode is not
  arbitrary AWS `CreateTask` passthrough. Richer normal or unrecognized native
  state remains unchanged/export opaque-preserve-only;
- the upstream normal UI shares one `model.name` between the outer DS task and
  inner DataSync task. Create writes the same value to both and edit folds them
  back together. REST and dsctl can preserve distinct outer and inner names;
  equality is not a wire-level invariant;
- known raw members keep upstream behavior and defects. `Tags`, `Excludes`,
  and `Schedule` reach AWS task creation. BeanUtils cannot apply authored
  `Options` values to the immutable AWS SDK model, so they are ineffective.
  `Includes` is passed to `builder.excludes`: alone it becomes the excludes
  filter and, when both are present, it overwrites authored `Excludes`.
  `Schedule` leaves a persistent recurring AWS Task;
- DATASYNC never consumes the prepared parameter map. `localParams`,
  `varPool`, DS placeholders, task resources, and resource files are therefore
  outside the reviewed runtime contract, and no structured DS output is
  returned;
- exact `3.2.0` through `3.2.2` workers use static basic credentials from
  `resource.aws.access.key.id`, `resource.aws.secret.access.key`, and
  `resource.aws.region`. Exact `3.3.1` and newer instead use
  `aws.datasync.access.key.id`, `aws.datasync.access.key.secret`, and
  `aws.datasync.region`; their upstream operator guide still documents the
  obsolete `resource.aws.*` keys. No datasource, endpoint, session token,
  instance profile, or default credential chain is supported;
- upstream INFO-logs the complete original task parameters and converted
  parameter object without logging the configured credential values in task
  code. Authored fields are not secret storage, and dsctl neither detects nor
  redacts secrets;
- each fresh attempt calls `CreateTask` and then `StartTaskExecution`. A
  callback persists only `taskExecutionArn` in task-instance `appIds`; after
  persistence, failover resumes polling and cancellation of that execution.
  The persistent AWS Task ARN is not stored, `DeleteTask` is never called, and
  the per-task client is never closed. Failure before callback persistence or
  retry without durable `appIds` can create duplicate executions and leak
  persistent or scheduled Tasks. Polling has no internal deadline and returns
  no structured output. This review adds no live evidence, changes no `tested`
  flag, and promotes no profile;
- `SAGEMAKER/start_pipeline_execution` is upstream-absent through `3.0.6` and
  typed in `MachineLearning` on seventeen exact profiles from `3.1.0` through
  `3.4.3`, excluding `3.1.1`–`3.1.2`. Those two keep a stale local polling
  status and may never observe completion after initial EXECUTING. Exact `3.1.0`
  updates an instance field correctly; `3.1.3` repairs the local assignment.
  The two holes permit only unchanged/export preservation. Its closed model is
  `SagemakerStartPipelineExecutionTaskParamsSpec`. Raw opaque create/edit is
  closed; unrecognized native state remains available only through
  unchanged/export opaque preservation;
- canonical authoring owns one preserved nonblank `sagemakerRequestJson`,
  ordered `localParams` with unique nonblank `prop` names and exact-profile
  `Direct`/`DataType` values, and, from `3.2.1`, a strict positive
  `datasource`. Literal request text without `${...}` or `$[...]` must parse as
  one JSON object. When a placeholder is present, the pre-substitution text may
  be invalid JSON because upstream performs whole-text substitution without
  JSON escaping before parsing. Each resolved value must therefore be valid in
  its exact JSON position. The resulting UpperCamelCase object is read only as
  AWS `StartPipelineExecutionRequest`. The exact known public keys are
  `PipelineName`, `PipelineExecutionDisplayName`,
  `PipelineParameters[{Name,Value}]`, `PipelineExecutionDescription`,
  `ClientRequestToken`, and
  `ParallelismConfiguration.MaxParallelExecutionSteps`. Unknown or wrongly
  cased fields are ignored rather than passed through to AWS. The mapper
  enables unknown-enum-to-null handling, but this request shape has no enum
  member, so that setting is inert here;
- projection always sends `localParams` and compiler-owned `resourceList=[]`.
  Exact `3.1.0`, `3.1.3`–`3.1.9`, and `3.2.0` forbid `datasource` and have no native
  `type` field. From `3.2.1`, `datasource` is required and projection fixes
  `type=SAGEMAKER`. Authored `username`, `password`, `awsRegion`, `varPool`,
  resources, runtime ids, and future fields fail closed. Datasource-era decode
  may strip only absent, empty, or null UI credential residue; nonempty values
  remain opaque-preserve-only;
- exact `3.1.0` through `3.4.0` put only `IN` local parameters into the prepared
  substitution map. From `3.4.1`, `OUT` declarations can also supply
  substitution values. SAGEMAKER never derives or publishes DS task output, so
  `OUT` does not become an output contract in either epoch;
- exact `3.1.0` through `3.2.0` workers use static
  `resource.aws.access.key.id`, `resource.aws.secret.access.key`, and
  `resource.aws.region`. Exact `3.2.1` and `3.2.2` require a SAGEMAKER
  datasource and build a static provider from its username/password/region.
  From `3.3.1`, the datasource remains required and initialized, but the AWS
  client ignores those values and instead reads
  `aws.sagemaker.credentials.provider.type`, optional
  `aws.sagemaker.endpoint`, `aws.sagemaker.region`, and either static
  `aws.sagemaker.access.key.id`/`aws.sagemaker.access.key.secret` credentials
  or an instance profile;
- upstream INFO-logs complete task parameters, the resolved request, pipeline
  identifiers, statuses, and pipeline steps. Datasource-era initialization
  also logs the resolved parameter object containing datasource credentials,
  including on later releases where the AWS client ignores them. Task fields
  and datasource values are not secret storage, and dsctl neither detects nor
  redacts arbitrary secrets;
- exact `3.1.0` writes a null application id before start and never backfills
  the returned ARN, so it has no durable failover-resume identity. From
  typed `3.1.3`, a post-submit callback persists the pipeline execution identity into
  `appIds`, so later failover can resume polling and cancellation. Callers that
  require AWS idempotency should author one stable `ClientRequestToken`; when
  omitted, the SDK may generate only a transient wire token while the plugin's
  persisted or reread token remains null. Failure after
  `StartPipelineExecution` but before callback persistence, or a retry without
  durable `appIds`, can start a duplicate execution. Polling sleeps `5000` ms
  only while status is `Executing`, treats only `Succeeded` as success, has no
  internal deadline, and publishes no DS output. The AWS client is not closed.
  This review adds no live evidence, changes no `tested` flag, and promotes no
  profile;
- `DMS/resume_existing_full_load` is upstream-absent through `3.1.9` and typed
  in `Cloud` on the nine exact profiles from `3.2.0` through `3.4.3`;
- its canonical payload and native wire require all five explicit fields:
  `isRestartTask=true`, `isJsonFormat=false`, `migrationType=full-load`,
  `startReplicationTaskType=resume-processing`, and one literal
  `replicationTaskArn`. Typed create/edit rejects DS placeholders, local
  parameters, resources, start/create/JSON/CDC configuration, destructive
  `reload-target`, and every other native or future field. There is no public
  opaque create/edit selector; excluded state is unchanged/export
  opaque-preserve-only;
- the caller must attest that the ARN identifies an actual stopped, previously
  executed full-load task. Restart validation checks only the ARN, so DS cannot
  verify the remote migration type or current state. `resume-processing` may
  reload partly loaded or not-yet-loaded tables. A mismatched CDC ARN without
  `cdcStopPosition` can take upstream's early-success path after start while
  remote CDC continues. `reload-target` can truncate or drop/reload target
  tables and remains deliberately excluded;
- through `3.2.2`, workers use static `resource.aws.access.key.id`,
  `resource.aws.secret.access.key`, and `resource.aws.region`. From `3.3.1`,
  `aws.dms.credentials.provider.type` selects static
  `aws.dms.access.key.id`/`aws.dms.access.key.secret` credentials or an instance
  profile; `aws.dms.region` is required and `aws.dms.endpoint` is optional;
- credentials remain worker configuration. Upstream logs full task parameters
  and remote identifiers at INFO without logging credential values, polls
  every `1000` ms without an explicit internal deadline, and publishes no DS
  task output. A callback stores `replicationTaskArn` in task-instance
  `appIds`; once durable, failover resumes tracking and cancellation stops the
  same remote task. Failure before callback persistence, or retry without
  durable `appIds`, can resend `resume-processing`. This review adds no live
  evidence, changes no `tested` flag, and promotes no profile;
- `ALIYUN_SERVERLESS_SPARK/literal_jar_submit` is upstream-absent through exact `3.2.2` and typed in `Cloud` on exact `3.3.1`, `3.3.2`, `3.4.0`,
  `3.4.1`, `3.4.2`, and `3.4.3`;
- its closed canonical payload owns eight fields: positive integer
  `datasource`; literal `workspaceId`, `resourceQueueId`, `jobName`, absolute
  object URI `entryPoint` under `oss://`, and `sparkSubmitParameters`;
  one nonempty literal string list `entryPointArguments`; and strict-boolean
  `isProduction`, defaulting to and always emitting `false` when omitted.
  Exact projection joins the argument list with `#` and adds fixed native
  `type=ALIYUN_SERVERLESS_SPARK` and `codeType=JAR`, producing ten wire fields;
- owned literals reject edge whitespace, controls, Unicode surrogates, and DS
  placeholders. Argument items also reject `#`, which would be ambiguous on
  the native list wire. Native `PYTHON` and `SQL` are explicit opaque
  create/edit modes. `engineReleaseVersion`, `templateId`, local parameters,
  variable pools, resources, richer JAR state, and future fields remain
  unchanged/export opaque-preserve-only;
- every execution fetches the optional Aliyun template and may prepend its
  Spark configuration, so the final submitted parameters can differ from the
  canonical and projected task wire. Because typed authoring omits
  `engineReleaseVersion`, the returned template may also derive display release
  and fusion values for the SDK request. Exact `3.3.1` retries status calls only,
  starts once without a client token, maps Aliyun `Failed` to DS `KILL`, and
  logs then swallows cancel failures;
- exact `3.3.2` through `3.4.1` retry template, start, status, and cancel calls
  for up to 11 attempts at `1000` ms intervals. A client token is created and
  reused within one DS attempt, but a DS retry receives a new token; `Failed`
  maps to `FAILURE`, cancel failure propagates, and start/cancel exception causes
  are not preserved. Exact `3.4.2` keeps that epoch and preserves start/cancel
  exception causes;
- upstream logs the complete task parameters, `jobRunId`, and state at INFO, so
  every authored value can appear in logs. Typed fields are not secret storage,
  and the CLI does not detect or redact secrets. Datasource-managed access keys
  are not task parameters, and task code does not explicitly log them through
  that object. The selected datasource supplies
  static `accessKeyId`, `accessKeySecret`, and `regionId` plus an optional
  custom endpoint; otherwise the client uses
  `emr-serverless-spark.%s.aliyuncs.com`. Its connectivity check returns
  without a remote call, so credentials, endpoint and OSS reachability, and
  Aliyun permissions remain caller prerequisites;
- the executor has no durable callback-backed application id or failover
  resume. A DS retry can submit a duplicate, cancel requires the in-memory
  `jobRunId`, status polling runs every 10 seconds without an internal
  deadline, and no DS task output is published. This review adds no live
  evidence, changes no `tested` flag, and promotes no profile;
- `GRPC/literal_unary_string_record_call` is upstream-absent through `3.3.2`
  and typed in `Universal` only on exact `3.4.0`, `3.4.1`, `3.4.2`, and `3.4.3`. Its
  closed model is `GrpcLiteralUnaryStringRecordTaskParamsSpec`;
- the canonical payload requires exactly eight fields: literal `url`, exact
  `channelCredentialType`, proto identifier `serviceName` and `methodName`,
  flat `requestFields` and `responseFields`, exact-key literal-string
  `message`, and positive integer `grpcConnectTimeoutMs`. The URL is a literal
  `host:port` without userinfo. Each record may be empty; otherwise each item
  is exactly a unique proto field `name` and unique valid protobuf field
  `number`; protobuf JSON camel-case names are also unique under proto3's
  ASCII-case-insensitive collision rule, and message keys must exactly equal
  the request field names. The case-insensitive exact
  request-name denylist is `apiKey`, `authToken`, `clientSecret`, `credential`,
  `password`, `passwd`, `privateKey`, `secret`, and `token`; response-only names
  use the proto-identifier rule without that denylist. Placeholders, controls,
  and non-string message values are rejected. JSON Schema publishes the
  expressible structural constraints and lists uniqueness/key-set checks that
  require lint under `x-dsctl-runtime-validations`;
- compilation generates matching package-free proto3
  `grpcServiceDefinition` and protobufjs `grpcServiceDefinitionJSON` for fixed
  `Request` and `Response` string records and one blocking unary method. It
  emits native `methodName=serviceName/methodName`, stable compact message JSON,
  `grpcCheckCondition=STATUS_CODE_DEFAULT`, `condition=""`, the positive RPC
  deadline, and correct Java `channelCredentialType`, plus compiler-owned
  top-level `taskExecuteType=BATCH`. Packages, streaming, nested/repeated or
  custom types, arbitrary proto/descriptor input, and custom status conditions
  are not supported; `grpcServiceDefinition` is runtime-dead but generated for
  upstream UI display;
- typed create/edit is enabled, while public raw opaque create/edit is closed.
  Complex native definitions, local/runtime state, and future fields remain
  available only through unchanged/export opaque preservation. Invalid typed
  input never falls back to that path. The task accepts no DS placeholders,
  `localParams`, `varPool`, or resources because the worker never consumes the
  master's `prepareParamsMap`;
- `channelCredentialType` accepts only `INSECURE` or `TLS_DEFAULT`.
  `TLS_DEFAULT` uses system trust and hostname verification; there is no custom
  CA, mTLS, token, or request-auth field. The upstream UI writes
  `grpcCredentialType` rather than the Java wire member, so a later UI edit can
  silently downgrade TLS to `INSECURE`;
- upstream logs the complete GRPC params at INFO, so authored fields are not
  secret storage. The response is not transported as DS output, cancel is a
  no-op, and there is no durable id or failover resume. Retry or failover can
  resend the unary RPC and duplicate side effects. The channel and
  `NioEventLoopGroup` are not closed, so repeated tasks can accumulate worker
  resources. This review adds no live evidence, changes no `tested` flag, and
  promotes no profile;
- `OPENMLDB/literal_single_statement` is absent through `3.0.6` and typed in
  `MachineLearning` on eighteen exact profiles from `3.1.0` through `3.4.3`,
  excluding `3.1.2`. Its inherited Python output handling dereferences null
  parameters after SQL execution; that exact hole permits only unchanged/export
  preservation, and retries can repeat SQL effects. Orphaned OPENMLDB UI files on `3.0.x` do not establish a
  registered task plugin;
- its canonical payload requires exactly four identity-projected native
  fields: `zk`, `zkPath`, `executeMode`, and `sql`. The mode is lowercase
  `offline` or `online`; `zk` is a comma-separated DNS/IPv4 `host:port`
  ensemble, `zkPath` is an absolute conservative ZooKeeper znode path, and
  `sql` is one nonblank literal Python-source-safe statement. Semicolon,
  double quote, backslash, carriage return, unsafe control text, `${...}`,
  and `$[...]` are rejected;
- typed OPENMLDB rejects `localParams`, `varPool`, `resourceList`, inherited
  `rawScript`, runtime state, and future fields. It exposes no public opaque
  create/edit selector; excluded and unrecognized existing native state is
  retained only on export and unchanged edits whose recorded projection
  provenance selects opaque preservation;
- exact `3.1.x` workers select `PYTHON_HOME`, while `3.2.0` and newer select
  `PYTHON_LAUNCHER`. Workers require Python 3, `openmldb`, SQLAlchemy plus the
  OpenMLDB driver, ZooKeeper reachability, and target-data permissions.
  Offline mode enables synchronous jobs with a fixed `1800000` ms timeout;
- upstream logs complete OPENMLDB task parameters, raw SQL, rendered Python,
  and the final generated Python file at INFO, so no task field is safe for
  secrets. The SQLAlchemy result is discarded rather than published as a DS
  output. The local process exposes no durable application id or failover
  resume, and retry reexecutes the statement and can repeat side effects. This
  review adds no live evidence, changes no `tested` flag, and promotes no
  profile;
- `DINKY/job_trigger` is absent upstream through `3.0.6` and typed on all nineteen
  exact profiles from `3.1.0` through `3.4.3`. Its closed native payload owns
  only required literal HTTP(S) `address`, required nonblank literal `taskId`,
  and optional strict-boolean `online`, which defaults to `false`;
- `address` must have a host and may have a literal port and path; URI userinfo,
  query, fragment, whitespace, control text, `${...}`, and `$[...]` are
  rejected. `taskId` likewise rejects whitespace, control text, placeholders,
  and shell-control syntax. `localParams`, `varPool`, and future native fields
  are rejected by typed create/edit, including when explicitly `null`;
- DINKY exposes no public raw opaque-authoring selector. Existing excluded
  native state remains lossless only on export and unchanged edits, and invalid
  canonical input never selects or downgrades to opaque create/edit;
- DINKY's legacy request path forwards no workflow or task variables through
  exact `3.2.0`. From `3.2.1`, the worker negotiates the remote Dinky version;
  only a negotiated Dinky 1.x `submitApplicationV1` branch sends variables,
  while the negotiated legacy/v0 branch still sends none;
- on that v1 branch, exact `3.2.1` and `3.2.2` send workflow globals and task
  `localParams`; exact `3.3.1` through `3.4.1` additionally expand local
  placeholders from the prepared parameter context; and exact `3.4.2` and `3.4.3` send
  the complete prepared parameter map—built-in, project, workflow, task,
  command, `varPool`, and business values—to Dinky and logs it at INFO;
- upstream DINKY execution also logs task parameters plus request URL and
  response content. It supplies no request authentication or explicit HTTP
  timeout, exposes no durable submitted-job id or failover resume, and may
  resubmit on retry. Cancellation targets `taskId`, not a unique submitted-run
  handle. This reviewed facet makes no confidentiality, runtime-success,
  live-evidence, `tested`, or profile-promotion claim;
- canonical `HIVECLI` authoring exists from `3.1.0` through `3.4.3` and owns
  only inline `SCRIPT`: a nonblank `hiveSqlScript`, optional literal
  `hiveCliOptions`, and unique `IN`/`VARCHAR` `localParams`. It is absent
  upstream on profiles through `3.0.6`;
- exact `3.1.x` workers substitute DS placeholders across the assembled
  `hive -e` command, while releases from `3.2.0` substitute only SQL text,
  write a temporary file, and execute `hive -f`. Typed `hiveCliOptions`
  therefore forbids `${...}` and `$[...]` on every reviewed release, while
  SQL placeholders remain supported;
- HIVECLI execution requires the worker Hive CLI in `PATH`, Hive/HDFS client
  configuration, and access to HDFS and the Hive Metastore. The task does not
  use a DS datasource, and the CLI does not provision these runtime services;
- HIVECLI `FILE`, `resourceList`, `varPool`, runtime output, and future native
  fields remain outside typed create/edit and round-trip only through opaque
  preservation;
- `DVC/operation` is absent upstream through `3.0.6` and typed on all nineteen exact
  profiles from `3.1.0` through `3.4.3`. Its native `dvcTaskType` selects
  `Upload`, `Download`, or `Init DVC`; each mode requires exactly its active
  repository, location, worker path, revision, message, or store URL fields,
  and typed input rejects fields inactive for that mode;
- upstream interpolates the repository, location, worker path, revision, and
  store URL into unquoted POSIX shell positions and places the message inside
  double quotes. Typed DVC therefore accepts only one conservative shell-safe
  token per unquoted field, rejects URI userinfo and non-portable Git refs,
  and rejects double quote, backslash, shell expansion, or control text in messages.
  `${...}` and `$[...]` are forbidden in every typed DVC field;
- DVC typed authoring exposes no `localParams`, `varPool`, resources,
  placeholders, or runtime output. Opaque create/edit and export preserve
  inherited, unsafe, inactive, and future native fields losslessly;
- DVC execution requires a POSIX worker with `git` and `dvc` in `PATH`, a Git
  identity, repository and DVC-remote permissions, and worker-managed SSH,
  credential-helper, or provider credentials. Credentials must not be put in
  task URLs because upstream logs both the parameters and generated command;
- DVC has only a local shell process and no resumable remote job or failover
  state. Its upstream script has no `set -e` and leaves several clone,
  directory, remote, commit, tag, and push steps unguarded, so an intermediate
  failure can be masked by a later successful command. The UI's persistent
  `taskType: MLFLOW` initialization typo is an upstream UI limitation; exact
  registration and wire authoring use `DVC`;
- `MLFLOW/model_serve` is absent upstream through `3.0.6` and typed on all nineteen
  exact profiles from `3.1.0` through `3.4.3`. This narrow facet is not full
  MLFLOW support. It owns exactly five required native fields:
  `mlflowTaskType: "MLflow Models"`, `deployType: "MLFLOW"`,
  `mlflowTrackingUri`, `deployModelKey`, and decimal-string `deployPort`;
- `mlflowTrackingUri` must be absolute HTTP(S) with a host and may contain only
  a safe path; userinfo, query, fragment, whitespace, control text,
  placeholders, quotes, backslash, globbing, and shell metacharacters are
  rejected. `deployModelKey` accepts only conservative
  `models:/name/version-or-stage` or `runs:/run-id/artifact[/subpath...]`
  forms with nonempty safe components and no `.` or `..` component.
  `deployPort` is serialized as a decimal string in the range `1` through
  `65535`;
- MLFLOW Projects mode, Docker deployment, `localParams`, `varPool`,
  `resourceList`, `registerModel`, runtime state, and future native fields are
  rejected by typed create/edit even when supplied as `null`; opaque
  create/edit and export preserve them losslessly;
- MLFLOW model serving requires a POSIX worker with the `mlflow` CLI in `PATH`,
  tracking-server and artifact-store access, access to the selected model or
  run artifact, an available port, and credentials supplied through worker
  environment or configuration rather than URI userinfo. Upstream runs a
  foreground service bound to `0.0.0.0`; cancellation controls only the local
  process, with no remote deployment ID, reconnect, or failover. A retry can
  collide with the port when an earlier process survives. This review adds no
  live evidence and promotes no profile;
- `JUPYTER/preinstalled_notebook` is absent upstream through `3.0.6` and typed
  on all nineteen exact profiles from `3.1.0` through `3.4.3`. It owns required
  shell-safe `condaEnvName`, `inputNotePath`, and `outputNotePath` fields. The
  environment must already be installed; the two paths must be distinct,
  absolute, POSIX-safe `.ipynb` paths;
- its optional `parameters` field is a literal shell-safe string-to-string
  map, `kernel` and `engine` are optional single safe tokens, and
  `executionTimeout` plus `startTimeout` are optional strict positive integer
  values. Exact projection omits an empty map, sorts and compact-serializes a
  nonempty map to the native JSON string, and emits timeout integers as native
  decimal strings;
- typed Jupyter authoring rejects `.tar.gz` and `.txt` environment bootstrap,
  `resourceList`, `others`, `localParams`, `varPool`, placeholders, runtime
  state, and future native members, including explicit `null` members. An
  exact lowercase `.txt` or `.tar.gz` `condaEnvName` is the only public raw
  selector and preserves that bootstrap payload plus its native companion
  fields through opaque create/edit. Other excluded native state is preserved
  only on export and unchanged edits; invalid preinstalled canonical input
  does not downgrade to opaque. The raw selector follows upstream's
  case-sensitive suffix check, while typed validation rejects suffix-like
  names case-insensitively; uppercase forms do not select raw mode;
- the parameter-map boundary rejects URI userinfo and a case-insensitive
  exact-name denylist consisting of `password`, `passwd`, `secret`, `token`,
  `credential`, `api_key`, `access_key`, and `private_key`. It is not
  comprehensive secret detection or redaction. All authored values and the
  assembled command may be logged upstream, so callers must not use task
  parameters as secret storage;
- eligible Jupyter workers must be POSIX hosts with `conda.path` configured,
  the selected conda environment plus Papermill, Jupyter, kernel, and engine
  already installed, and the input/output notebook paths readable/writable as
  applicable. The output notebook is a worker filesystem artifact, not a DS
  task output. The plugin exposes no remote application id or failover-resume
  protocol, and retry executes the notebook again. This review adds no live
  evidence, changes no `tested` flag, and promotes no profile;
- `ZEPPELIN/paragraph` is absent upstream through `2.0.9` and typed on all
  twenty-six exact profiles from `3.0.0` through `3.4.3`. It owns safe
  `noteId`/`paragraphId`, the exact selected `connectionMode`, and an optional
  literal string-to-string `parameters` map;
- the canonical discriminator is not sent. Exact `3.0.x` requires
  `WORKER_CONFIG`, emits only the two ids, and relies on worker
  `zeppelin.rest.url`; `3.1.x` through `3.2.0` requires `REST_ENDPOINT` plus an
  anonymous literal `restEndpoint`; `3.2.1` and newer requires `DATASOURCE`
  plus a positive datasource id and injects native `type: ZEPPELIN`;
- canonical `parameters` compile to one compact native JSON string from
  `3.1.0`; the empty map is omitted. On `3.0.x`, the same canonical field has
  `maxProperties: 0` and no native compile path. Typed keys/values must be
  literal strings without control text, `${...}`, or `$[...]`;
- datasource anonymity is a user runtime prerequisite, not a CLI-attested
  property. The CLI checks only the positive id and does not inspect the
  datasource's authentication mode. Exact `3.2.0` logs raw task parameters
  containing native inline credentials, `3.2.1` logs the resolved datasource
  username after login, and `3.2.2` and newer log the resolved parameter object
  including datasource credentials. Typed authoring exposes neither credential
  form. The CLI does not detect or redact secret-like paragraph values, so
  callers must not use this map as secret storage;
- where upstream owns them, whole-note execution, cloning through
  `productionNoteDirectory`, inline or datasource authentication state,
  `localParams`, `varPool`, `resourceList`, runtime output, and future native
  fields remain outside typed create/edit and round-trip only through explicit
  opaque authoring/preservation;
- exact `3.4.1` creates `taskName.result` in task-local parameter state without
  reliable downstream var-pool publication. Exact `3.4.2` installs that value
  into the task execution context's var pool, but output is runtime-only on
  both profiles;
- Zeppelin execution is one synchronous paragraph call with no reliable remote
  application id, reconnect, or failover resume. A retry may execute the same
  paragraph again. This source review adds no live evidence, changes no
  `tested` flag, and promotes no profile;
- canonical `EMR` authoring exists from `3.0.0` through `3.4.3`; it is absent
  upstream on `1.3.9`, `2.0.0`, and `2.0.9`. It keeps AWS request JSON in raw
  string fields rather than parsing it into a CLI-owned request object;
  `3.0.x` supports only `RUN_JOB_FLOW` and omits the canonical `programType`
  discriminator on its implied-mode wire, while `3.1.0` and newer support both
  `RUN_JOB_FLOW` and `ADD_JOB_FLOW_STEPS` with the native discriminator;
- DolphinScheduler substitutes `${...}` and `$[...]` in EMR request text only
  from `3.2.2`; earlier profiles send the text unchanged and typed authoring
  rejects placeholders bound by EMR `localParams`; literal JSON must be an
  object, and a statically parseable ADD request must contain exactly one
  `Steps` entry;
- EMR `localParams` accept unique `IN`/`VARCHAR` properties only and its typed
  surface does not expose `varPool`; opaque export/edit preserves additional
  native and future state losslessly;
- EMR AWS credentials are an exact server-side runtime prerequisite, not task
  YAML or CLI-managed state: `3.0.0` workers use `aws.access.key.id`,
  `aws.secret.access.key`, and `aws.region`; `3.0.6` through `3.2.2` workers use
  `resource.aws.access.key.id`, `resource.aws.secret.access.key`, and
  `resource.aws.region`; `3.3.1` and newer use AWS authentication with the
  `aws.emr.*` entries in `aws.yaml`;
- upstream EMR task failover is not implemented; typed authoring does not claim
  or add EMR failover or high availability;
- `EMR_SERVERLESS/start_job_run` is typed on exact `3.4.2` and `3.4.3` and absent
  upstream through exact `3.4.1`. It owns literal `applicationId`,
  `executionRoleArn`, optional `jobName`, raw `startJobRunRequestJson`, and
  unique `IN`/`VARCHAR` `localParams`. Placeholder substitution applies only
  inside the request JSON. Unresolved runtime placeholders must be quoted JSON
  string content; bound local parameters may supply raw fragments only when the
  exact substituted request remains a JSON object. Top-level values override
  matching JSON members;
- eligible workers need `aws.emr.*` credentials and region or an AWS SDK
  default credential chain, access to a pre-existing `STARTED` or `CREATED`
  application, and the required service/job-data permissions. Custom endpoints
  use `emr.serverless.endpoint` or `EMR_SERVERLESS_ENDPOINT`, not
  `aws.emr.endpoint`;
- EMR Serverless persists `jobRunId` through task-instance `appIds` and resumes
  polling after failover. Runtime and future native fields remain
  opaque-preserve-only;
- canonical `PROCEDURE` authoring uses JDBC call syntax such as
  `{call schema.refresh_daily(?,?)}`, with each positional `?` matched to one
  `localParams` entry in source order; exact compilation emits a bare qualified
  procedure name on `1.3.9`, a full positional JDBC call on `2.0.0`, and a full
  `${prop}` call from `2.0.9` through `3.4.3`;
- `PROCEDURE` local parameters accept only JDBC scalar types: `VARCHAR`,
  `INTEGER`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`, and
  `BOOLEAN`; `LIST` and `FILE` are not procedure-call parameter types, and
  although `1.3.9` accepts both `IN` and `OUT`, downstream `varPool`
  publication begins only on `2.0.9`;
- `PROCEDURE.outProperty` is derived runtime state on `2.0.9` through `3.4.0`;
  typed create/edit normalization removes known exported state and never emits
  it, while opaque export/edit keeps both `outProperty` and `method` in their
  exact native shape because a positional method is otherwise ambiguous;
- a missing exact catalog, compatibility membership, task type, facet, or
  requested authoring intent returns `unsupported_feature` before transport
  instead of silently falling back to another version.

## `dsctl lint workflow FILE`

Runs local design-time checks for one workflow YAML file without contacting
DolphinScheduler.

Resolved fields:

- `kind`
- `file`

Rules:

- this command is local-only and does not require DS connectivity
- it validates the stable workflow YAML model
- when references can be bound locally, it compiles the selected profile's task
  catalog into the CLI canonical local authoring plan; remote `workflow.create`
  availability and exact wire support remain independently version-gated
- datasource names retain local model, graph and task execution-constraint
  checks, including workflow parameter and task retry/timeout restrictions;
  only wire compilation waits for a create/edit dry run to resolve real IDs.
  Diagnostics explicitly report
  `workflow_datasource_binding_deferred` and `workflow_compilation_deferred`;
  no placeholder datasource IDs are fabricated
- compiled task codes are deterministic preview values only; lint never
  requests or reserves persistent task codes from DolphinScheduler
- it warns when `workflow.project` is omitted because workflow selection then
  depends on `--project` or stored project context
- it warns on risky `$[...]` dynamic parameter time formats; uppercase
  `YYYY` emits `parameter_time_format_week_year_token`, and calendar-year plus
  week-number patterns such as `$[yyyyww]` emit
  `parameter_time_format_calendar_year_with_week`
- it warns when a local parameter value references its own property with
  `parameter_local_self_reference`
- it warns when a workflow global references itself with
  `parameter_global_self_reference`
- it uses the selected exact parameter profile for task-output syntax and
  nested-workflow semantics; incompatible `${setValue(...)}` or
  `#{setValue(...)}` placement/markers emit
  `task_output_set_value_syntax_incompatible`, and populated nested-task
  `localParams` emit `sub_workflow_local_params_not_child_inputs` only in
  epochs where upstream ignores them as child inputs
- it rejects schedule blocks on offline workflows because DS only allows
  schedule creation for online workflows
- `data.diagnostics[]` records severity, stable code, path and message for
  completed checks, warnings and errors. Independent findings are aggregated;
  stages requiring invalid inputs are explicitly skipped
- invalid lint preserves all collected diagnostics in `data`, returns
  `ok: false` and exits 1. Local validation never proves live resource existence
  or worker readiness

Current `data` fields:

- `kind`
- `valid`
- `summary` when a workflow model is available
- `compilation` only when local compilation completed
- `diagnostics`

## `dsctl lint workflow-patch FILE` / `dsctl lint workflow-instance-patch FILE`

Validate a patch locally before edit preview. These commands check document
shape, independently knowable task payloads and intrinsic operation conflicts.
They share patch validation rules with the corresponding edit service.

`data` contains `kind`, `valid`, `summary`, `diagnostics` and `baseline`.
`baseline.requiredForCompleteValidation` is true; `baseline.unchecked` names
checks deferred until a live baseline is selected, including existing identities,
merged task fields, graph consistency and preserved-state constraints. A valid
patch lint result covers only those locally knowable checks. Run the matching
`workflow edit --dry-run` or `workflow-instance edit --dry-run` for the full
baseline-aware plan. Invalid results retain the report and exit 1. No REST
request or persistent task-code allocation occurs.

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

The action is available on exact profiles `3.1.0` through `3.4.3`.
Generated contracts retain each version's native `FavTaskDto` shape; the
task-type adapter maps its reviewed version recipe into one CLI view.
Exact source provenance and availability remain independent of shared code.

`data.taskTypes` projects the records using the current DS catalog field names:

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

On `3.1.0` through `3.1.9`, the native catalog stores the task name in
`taskName` and its category in `taskType`. The exact adapter projects these as
CLI `taskType` and `taskCategory`, matching the catalog fields used from `3.2.0`.
For example, `SHELL` remains the task type and `Universal` its category on both
sides of that version boundary.

## `dsctl task-type get TASK_TYPE`

Returns the local task authoring summary for one DS task type. This command is
local and does not call DolphinScheduler.

Rules:

- task type matching is case-insensitive and accepts supported local aliases
- `resolved.task_type` is the normalized DS-native task type
- `data.template_command` points to the default YAML fragment
- `data.raw_template_command` points to the copyable raw YAML fragment
- `data.schema_command` points directly to the bounded field contract; it is
  not necessary to call `get` before it
- `data.payload_modes[]` lists the accepted payload forms for the selected task
  type
- `data.required_paths[]` lists fields required independently of the selected
  payload mode
- `data.required_paths_by_payload_mode` lists the additional leaf fields needed
  by each payload form; `SHELL` and `PYTHON` expose `command` and `task_params`
  alternatives, while `REMOTESHELL` exposes only its datasource-backed
  `task_params` form
- `data.choice_sources[]` lists commands or local sources for discoverable
  values
- `data.rows[]` is the compact table/tsv view of next commands and variants
- generic task types emit warning code `generic_task_template`

## `dsctl task-type schema TASK_TYPE`

Returns one local authoring view for a DS task type. This command is local and
does not call DolphinScheduler. With no selector it returns the bounded field
contract needed for ordinary YAML authoring.

Options:

- `--field FIELD`
- `--json-schema`
- `--compile-mappings`
- `--full`

Rules:

- the four selectors are mutually exclusive; no selector means the `fields`
  view
- `resolved.task_type` is the normalized type and `resolved.view` is one of
  `fields`, `field`, `json_schema`, `compile_mappings`, or `full`
- bounded views use `data.schema_version: 2` and include small `data.links`
  commands for direct progressive navigation
- the default `data.fields[]` is the canonical row model for table/tsv and
  `--columns`; `data.state_rules[]` keeps the conditionals needed to use those
  fields correctly, and `condition_paths[]` identifies the fields controlling
  each rule without parsing its human-readable `when` text
- dotted authoring paths are nested JSON Schema objects and `[]` paths are
  represented as arrays; `data.fields[]` is not duplicated as `data.rows[]`
- `data.fields[].choice_source` records the command or local source for
  discoverable values, `data.fields[].choice_value` identifies which returned
  field to use, and `data.fields[].related_commands[]` records adjacent
  inspection or creation commands when useful
- `--field FIELD` returns one element in `data.fields[]` plus only state rules
  that mention that path; unknown paths return at most three executable typo
  candidates rather than the whole field catalog
- `--json-schema` returns the nested structural validation contract in
  `data.schema`. Constraints expressible in JSON Schema are enforced there;
  model-specific cross-field relationships that JSON Schema cannot encode are
  named in `x-dsctl-runtime-validations`, and the advertised lint command is
  authoritative for them. Its `x-dsctl` navigation metadata does not repeat
  state rules, choice sources, or compile mappings.
  `x-dsctl.lint_command_pattern` is the canonical lint invocation pattern;
  `x-dsctl.lint_command` is its deprecated compatibility alias. This document
  view is JSON-only, so table/tsv and `--columns` fail rather than silently
  returning an incomplete schema
- `--compile-mappings` returns one `data.compile_mapping_policy` plus compact
  `data.compile_mappings[]` rows, whose table/tsv row source is distinct from
  the field view
- `--full` retains the former expanded top-level paths and repeated
  `schema.x-dsctl` metadata for compatibility, audits, and generators; it is
  not the recommended LLM authoring path
- supported table/tsv output is always a standard single table with no metadata
  footer: fields/field/full render `data.fields[]`, and compile mappings render
  their own rows
- compact success envelopes are budgeted below 12 KiB for the default view,
  10 KiB for JSON Schema, 5 KiB for compile mappings, and 3 KiB for one field
  across the supported task-type catalog
- the schema is for authoring workflow YAML, not for representing every raw DS
  database column

## `dsctl project list`

Returns a DS-style paging object.

Options:

- `--search SEARCH`
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

With `--all`, `total` counts the collected items. Without `--all`, `total`
retains the upstream reported count for the requested search. Both forms
include `coverage`: original totals, requested page range, per-page observations,
changes in reported totals, and observation times. `scope_complete` describes
that requested range, not a complete or atomic server snapshot. A failure
during auto-fetch retains partial coverage in the error.

Use `dsctl project list` to discover project names and native numeric identifiers:
`id` on DS `1.3.9`, `code` on DS `2.0.0`–`3.4.3`. On code-based profiles a
returned database `id` is not the project's numeric CLI selector.

## `dsctl project get PROJECT`

Accepts a project name or its native numeric identifier, resolves the stable project
identity, then fetches the current project payload.

Use `dsctl project list` to discover project names and native identifiers.

`resolved.project` includes:

- `id` on DS `1.3.9`, or `code` on DS `2.0.0`–`3.4.3`
- `name`
- `description`

## `dsctl project create`

Creates a project and returns the created project payload.

## `dsctl project update PROJECT`

Updates one project resolved by name or native numeric identifier.

Rules:

- use `dsctl project list` to discover project names and native identifiers
- omitting `--name` preserves the current name
- omitting both `--description` and `--clear-description` preserves the
  current description
- `--clear-description` sets the description to `null`
- `--description` and `--clear-description` are mutually exclusive

## `dsctl project delete PROJECT --force`

Deletes one resolved project.

Rules:

- `PROJECT` may be a project name or its native numeric identifier
- use `dsctl project list` to discover project names and native identifiers
- `--force` is required

On reviewed DS `3.2.0`–`3.4.3` profiles, native project deletion also deletes its
task groups and their queue records. DS `3.0.0`–`3.1.9` does not perform that
cascade; deleting the project or its user does not remove those task groups.
See [task-group cleanup boundaries](../user/version-compatibility.md#task-group-cleanup-boundaries).

Successful output returns:

- `data.deleted`
- `data.project`

## `dsctl project-parameter list`

Lists project parameters inside one selected project.

Options:

- `--project PROJECT`
- `--search SEARCH`
- `--data-type TYPE`
- `--page-no N`
- `--page-size N`
- `--all`

Use `dsctl project list` to discover projects. Use
`dsctl enum list data-type` to discover project-parameter data-type values.

On exact DS `3.2.0` through `3.2.2`, project parameters support only `VARCHAR`;
selected-version schema restricts `--data-type` accordingly for list, create and
update. Other values fail before any parameter mutation. Create defaults to
`VARCHAR`; update preserves the stored type when the option is omitted, and list
omits the type filter by default. DS `3.3.1` and newer use the native typed
parameter field. Schema for an unresolved target does not infer this restriction.

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

- `--project` falls back to the selected named context project
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

- `--project` falls back to the selected named context project
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
- exactly one of `--preferences-json PREFERENCES_JSON` or `--file FILE`

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
  `null` and the CLI emits one warning with `warnings[]` code
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

- selection uses `--project`, then the selected named context project
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

- selection uses `--project`, then the selected named context project
- use `dsctl project list` to discover project names and numeric codes
- repeat `--worker-group NAME` to keep multiple worker groups assigned
- use `dsctl worker-group list` to discover worker-group names
- the CLI normalizes duplicates after trimming whitespace
- the CLI rejects an empty assignment set; use `clear --force` on releases that
  support clearing (`3.3.1` and newer)
- successful output returns the current upstream-reported worker-group list after
  the mutation
- if upstream still reports worker groups not present in the requested set, the
  CLI emits a warning because those groups are still used by tasks or schedules

Successful resolution returns:

- `resolved.project`
- `resolved.requested_worker_groups`

## `dsctl project-worker-group clear --force`

Removes the explicit worker-group assignment set for one selected project.

The upstream clear operation requires DS `3.3.1` or newer. DS `3.2.2` supports
reading and assigning nonempty worker-group sets, but rejects an empty set with
native `1402003` (`WORKER_GROUP_TO_PROJECT_IS_EMPTY`). Deleting the project does
not clear these assignment rows and is not a substitute for clearing them.
The exact `3.2.2` capability and schema mark only `project-worker-group.clear`
as `limited`, and CLI preflight returns `unsupported_feature` before transport.
Selecting a different profile does not change the server's capability; the
profile must describe the actual deployed release. If a server still reports
`1402003`, the CLI preserves that remote source and reports the nonempty
assignment requirement as `unsupported_feature`.

Rules:

- use `dsctl project list` to discover project names and numeric codes
- `--force` is required
- successful output returns the current upstream-reported worker-group list after
  the mutation
- upstream may still report worker groups that remain in use by tasks or
  schedules; when that happens, the CLI emits a warning aligned with one
  `warnings[]` item using code `project_worker_group_still_in_use`

## `dsctl environment list`

Returns a DS-style paging object.

Options:

- `--search SEARCH`
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

- `--name NAME` required
- exactly one of `--config CONFIG` or `--config-file CONFIG_FILE`
- `--description DESCRIPTION`
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

- `--name NAME`
- `--config CONFIG`
- `--config-file CONFIG_FILE`
- `--description DESCRIPTION`
- `--clear-description`

For backward compatibility, the parser still accepts the hidden deprecated
option `--tenant-code TENANT_CODE`. Its value must equal the resolved tenant's
current code; a different value returns a structured `user_input_error` because
tenant identity is immutable. The hidden option is omitted from help and
machine-readable command schemas.
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

- `--search SEARCH`
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

- `--name NAME` required
- exactly one of `--config CONFIG` or `--config-file CONFIG_FILE`
- `--description DESCRIPTION`

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

- `--name NAME`
- `--config CONFIG`
- `--config-file CONFIG_FILE`
- `--description DESCRIPTION`
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

- `--search SEARCH`
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

`datasource list` keeps DS-native field names. In `datasource get DATASOURCE`,
`userName` is the datasource connection username accepted by datasource
create/update payloads; secret values remain redacted.

## `dsctl datasource get DATASOURCE`

Accepts a datasource name or a numeric datasource id, resolves the stable
datasource identity, then fetches the current datasource detail payload.

Rules:

- `data` keeps the DS-native detail fields returned by the exact-version
  datasource detail route
- non-sensitive plugin-specific datasource fields are preserved as-is
- sensitive fields are recursively replaced with `******` before the result
  crosses the CLI output boundary

`resolved.datasource` includes:

- `id`
- `name`
- `note`
- `type`

## `dsctl datasource create`

Creates one datasource from a DS-native JSON payload file.

Options:

- `--file FILE` required

Rules:

- the file must contain one JSON object
- the payload must include string fields `name` and `type`
- `type` and the accepted plugin-specific fields are validated against the
  configured cluster's exact DS version before any request is sent
- the payload must not include `id`
- masked values such as `******` are rejected for every sensitive field,
  including `password`, `accessKeySecret`, `kubeConfig`, and the version-specific
  SSH key field
- run `dsctl template datasource --ds-version VERSION` to choose a type, then
  `dsctl template datasource --ds-version VERSION --type TYPE` and write
  `data.json` to the file; omitting `--ds-version` uses the configured
  `DS_VERSION` from process or `--env-file` inputs, falling back to the stable
  `3.4.1` contract only when no version is configured

## `dsctl datasource update DATASOURCE`

Updates one datasource from a DS-native JSON payload file.

Options:

- `--file FILE` required

Rules:

- `DATASOURCE` may be a datasource name or numeric id
- the file must contain one JSON object
- the payload must include string fields `name` and `type`
- the datasource type cannot be changed by an update
- `type` and plugin-specific fields are validated against the configured
  cluster's exact DS version before any request is sent
- if the payload includes `id`, it must match the selected datasource id
- omitted sensitive fields are preserved from an internal unredacted read
- a masked sensitive field copied from `datasource get` also means preserve;
  the CLI substitutes the existing value when upstream exposes it, and uses
  DS 2.0.0+'s reviewed blank-password preservation behavior when those
  versions mask only `password`; DS 1.3.9 requires an explicit recoverable
  value and fails closed otherwise
- if upstream does not expose a masked non-password value safely, the CLI
  rejects the update and asks for the real value instead of guessing
- start from `dsctl datasource get DATASOURCE` or
  `dsctl template datasource --ds-version VERSION --type TYPE` when preparing
  an update file
- when a supplied masked field is preserved, its aligned
  `warnings[]` code is `datasource_update_preserved_existing_FIELD`

All datasource detail results redact sensitive fields before they reach a
`CommandResult`, including nested occurrences. The redaction boundary covers
`password`, `accessKeySecret`, `kubeConfig`, and the applicable SSH
public/private key field; internal update merging may use the unredacted
upstream payload.

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

- `--search SEARCH`
- `--page-no N`
- `--page-size N`
- `--all`

Selection and behavior:

- namespace management is absent before DS 3.0.0; those exact profiles return
  a terminal `unsupported_feature` without sending a request
- this command is admin-only because namespace paging is admin-only
- `--search` is passed to the upstream `searchVal`
- without `--all`, the command returns one DS-style page payload
- with `--all`, the CLI fetches remaining pages up to the safety limit and
  materializes one DS-style page payload

Namespace row fields follow the selected exact generated native model.
Depending on the release, these include `id`, `code`, `namespace`,
`clusterCode`, `clusterName`, `userId`, `userName`, `createTime`, `updateTime`,
and legacy fields such as `k8s`, `limitsCpu`, `limitsMemory`, `podRequestCpu`,
`podRequestMemory`, `podReplicas`, and `onlineJobNum`.

Supported fields retain null values; fields absent from that exact model are
omitted, including code/cluster fields on releases that do not define them.
This projection applies to both JSON formats. `json-compact` encodes the list
at `data.totalList` as `columns` and positional `rows`; the field set can vary
by release while the container remains fixed.

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
- the logical `data` payload is a collection because the DS endpoint returns a
  list. `json` uses an object array; `json-compact` uses `columns` and `rows`
  directly at `data`
- row fields follow the selected exact native model, as for `namespace list`;
  supported null fields remain present and unavailable fields are omitted
- user-permission namespace identity summaries retain their separate fixed
  summary contract

`resolved` includes:

- `scope` with value `current_user`

## `dsctl namespace create`

Creates the real Kubernetes namespace and registers it in DolphinScheduler.

Options:

- `--namespace NAMESPACE` required
- `--k8s K8S`
- `--cluster-code N`
- `--limits-cpu NUMBER`
- `--limits-memory N`

Selection and behavior:

- this command is admin-only
- DS 3.0.0 through 3.0.6 require `--k8s` and reject `--cluster-code`
- DS 3.1.0 and newer require `--cluster-code` and reject `--k8s`
- `--limits-cpu` and `--limits-memory` are accepted through DS 3.2.0 and
  rejected by DS 3.2.1 and newer
- invalid version-specific option combinations are rejected before a request
- run `dsctl cluster list` to discover cluster codes
- the returned `data` payload keeps the DS-native namespace shape
- all versions use bounded namespace paging to read back and verify the mutation;
  DS 3.2.2 and newer also return an entity whose id narrows that verification
- `data.clusterName` may be `null` when the exact upstream shape does not
  project a cluster name

## `dsctl namespace delete NAMESPACE --force`

Deletes one resolved namespace registration. On DS 3.0.0 through 3.1.9 this
operation also deletes the real Kubernetes namespace.

Rules:

- this command is admin-only
- `NAMESPACE` may be a namespace name or numeric id
- namespace names may be ambiguous across clusters; when that happens, the CLI
  returns a resolution error and expects a numeric namespace id
- `--force` is required
- DS 3.0.0 through 3.1.9 return a warning because deletion removes both the DS
  registration and the real Kubernetes namespace
- DS 3.2.0 and newer delete only the DS registration

Successful output returns:

- `data.deleted`
- `data.deletesKubernetesNamespace`
- `data.namespace`
- `resolved.deletes_kubernetes_namespace`

## `dsctl resource list`

Returns a DS-style paging object for one DS resource directory.

Options:

- `--dir DIR`
- `--search SEARCH`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- when `--dir` is omitted, the CLI resolves the upstream resource base directory
- selectors are DS `fullName` paths rather than opaque names
- run `dsctl resource list` or `dsctl resource list --dir DIR` to discover
  resource paths
- the paging payload keeps DS field names

Across resource operations, an explicit native `STORAGE_NOT_STARTUP` (`60002`)
returns `invalid_state`, preserves the upstream code in `error.source`, and
directs an administrator to check the server's effective resource storage
configuration. This also applies when the CLI resolves the default directory
or verifies a resource attachment before a workflow/task mutation. It is a
server prerequisite, not an invalid path, missing permission or unsupported
version. The CLI does not enable storage or retry the operation automatically.
Generic errors such as `10057` and `10061` retain their original classification;
their codes or messages alone do not establish that storage is disabled.

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
- exact `3.4.3` uses the repaired native preview endpoint with the requested
  `skipLineNum` and `limit`. Exact `3.3.1` through `3.4.2` retain the reviewed
  download-and-slice workaround for their native limit bug

## `dsctl resource upload`

Uploads one local file into one DS directory.

Options:

- `--file FILE` required
- `--dir DIR`
- `--name NAME`

Rules:

- run `dsctl resource list` to discover destination directory paths
- when `--name` is omitted, the local leaf filename is reused remotely
- the returned `data` payload is a CLI projection because DS upload does not
  return an entity body
- `resolved.source_file` records the local upload path

## `dsctl resource create`

Creates one text resource from inline content.

Options:

- `--name NAME` required
- `--content CONTENT` required
- `--dir DIR`

Rules:

- `--name` must include a file extension because DS online-create accepts
  `fileName` and `suffix` separately
- use `dsctl resource upload --file FILE` when the content already lives in a
  local file
- the returned `data` payload is a CLI projection because DS online-create does
  not return an entity body

## `dsctl resource mkdir NAME`

Creates one directory inside one DS resource directory.

Options:

- `--dir DIR`

Rules:

- `NAME` is one leaf directory name, not a path
- run `dsctl resource list` to discover parent directory paths
- the returned `data` payload is a CLI projection because DS directory create
  does not return an entity body

## `dsctl resource download RESOURCE`

Downloads one remote resource into one local file path.

Options:

- `--output OUTPUT`
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

- `--search SEARCH`
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

- `--queue-name QUEUE_NAME` required
- `--queue QUEUE` required

Rules:

- `queueName` is the human-facing DS queue name used as the selector label
- `queue` is the underlying YARN queue value stored in DS

## `dsctl queue update QUEUE`

Updates one resolved queue while preserving omitted fields.

Options:

- `--queue-name QUEUE_NAME`
- `--queue QUEUE`

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

- `--search SEARCH`
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

- `--name NAME` required
- `--addr ADDR` repeatable
- `--description DESCRIPTION`

Rules:

- repeated `--addr` values are joined into the upstream `addrList`
- run `dsctl monitor server worker` to discover worker server addresses
- omitting `--addr` creates the worker group with an empty `addrList`
- descriptions are available on DolphinScheduler 3.1.0 and newer; those
  controllers persist their native empty-string default when `--description`
  is omitted
- earlier versions reject a non-empty `--description` before mutation

## `dsctl worker-group update WORKER_GROUP`

Updates one resolved worker group while preserving omitted fields.

Options:

- `--name NAME`
- `--addr ADDR` repeatable
- `--clear-addrs`
- `--description DESCRIPTION`
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
- descriptions are available on DolphinScheduler 3.1.0 and newer;
  `--clear-description` persists the native empty string
- earlier versions reject a non-empty description before mutation
- config-derived worker-group rows cannot be updated through the CRUD endpoint
- on exact `3.1.0` and `3.1.1`, upstream incorrectly treats a group's own
  unchanged name as a duplicate. An update must explicitly change `--name` to
  a new unique name; changing only addresses or description is rejected before
  mutation. Exact `3.1.2` and newer correct this behavior

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
- `--search SEARCH`
- `--status STATUS`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- without `--project`, `--search` and `--status` are passed to the global
  task-group paging API
- with `--project`, the CLI resolves project selection and uses DS's
  project-scoped task-group list shape
- run `dsctl project list` to discover project names and native identifiers for `--project`
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
- `--name NAME` required
- `--group-size N` required
- `--description DESCRIPTION`

Rules:

- project selection uses `flag > context`
- run `dsctl project list` to discover project names and native identifiers for `--project`
- `groupSize` must be greater than or equal to `1`
- omitted description is sent as an empty string

Successful output returns the created task-group payload.

## `dsctl task-group update TASK_GROUP`

Updates one resolved task group while preserving omitted fields.

Options:

- `--name NAME`
- `--group-size N`
- `--description DESCRIPTION`
- `--clear-description`

Rules:

- `TASK_GROUP` may be a task-group name or numeric id
- run `dsctl task-group list` to discover task-group names and ids
- at least one field change is required
- omitted fields preserve the current remote values
- `--clear-description` sends an empty description; it cannot be combined with
  `--description`
- closed task groups must be started before they can be updated

Successful output returns the updated task-group payload.

## `dsctl task-group close TASK_GROUP`

Closes one resolved task group and returns the refreshed task-group payload.
Closing changes its enabled state; it does not delete the group or its history.

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

- `--task-instance TASK_INSTANCE`
- `--workflow-instance WORKFLOW_INSTANCE`
- `--status STATUS`
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

`taskId` is `null` when the native paging query does not supply a task-instance
ID; a returned positive ID is retained. The Java entity's default `0` is not an
instance identity. To locate the task, list task instances with the returned
project and `workflowInstanceId`, then match `taskName`. A single queue row with
these identities can suggest that scoped list command.

`inQueue` is `null` on `3.0.0`–`3.2.0`, whose paging query omits that column.
Later profiles retain its native integer value, including `0`. Use `status`
for the queue state; an unavailable flag is not evidence that a task has left
the queue.

## `dsctl task-group queue force-start QUEUE_ID`

Requests force-start for one waiting task-group queue row by numeric queue id.

Run `dsctl task-group queue list TASK_GROUP` to discover queue ids.

Successful output returns:

- `data.queueId`
- `data.accepted`: the force-start request was accepted

Acceptance confirms the server accepted the request, not that a worker started
or completed the task. Inspect task instances in the owning workflow to verify
actual execution. Queue rows can change state or disappear after force-start,
depending on the DS release; their absence alone does not prove execution.
The receipt does not invent those identities or issue additional discovery reads.

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

- `--search SEARCH`
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

- `search` uses the upstream instance-name filter when the selected DS release
  exposes it; exact DS `2.0.0` performs the same case-insensitive substring
  projection locally over a bounded page scan because that controller has no
  `searchVal` parameter
- source-proven null item collections become empty `totalList` arrays only
  when the native count allows an empty requested page; a null collection on
  a page that should contain records raises `api_transport_error`, preserving
  the distinction between inconsistent upstream data and resource absence
- alert-plugin definitions and instances are absent in DS `1.3.9`; all
  `alert-plugin` actions fail before transport on that profile

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

- `--name NAME` required
- `--plugin PLUGIN` required
- `--param KEY=VALUE`
- `--params-json JSON`
- `--file FILE`

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
- UI param objects retain the upstream `field`, `type` and `title` metadata.
  Mutation verification compares the values DS persists, allowing the upstream
  template to reconstruct display metadata and JSON formatting. Empty params may
  be returned as the native JSON text `null`; later updates preserve that empty
  state without submitting invalid input.
- use `dsctl alert-plugin schema PLUGIN` to fetch the upstream param template,
  fill each item's `value`, then submit it unchanged

Successful output returns the refreshed alert-plugin instance payload.

## `dsctl alert-plugin update ALERT_PLUGIN`

Updates one resolved alert-plugin instance while preserving omitted fields.

Options:

- `--name NAME`
- `--param KEY=VALUE`
- `--params-json JSON`
- `--file FILE`

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
- a native associated-group rejection remains `conflict`, preserving its
  status in `error.source`. Use `dsctl alert-group list --all` and
  `dsctl alert-group get ALERT_GROUP` to inspect references before changing
  associations. An omitted or `null` association field in an older list
  projection does not establish that a plugin is unused.

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
- test-send is available from DS `3.2.1`; older exact profiles fail before
  transport because their controller has no test-send operation

Successful output returns:

- `data.tested`
- `resolved.alertPlugin`

## `dsctl alert-group list`

Returns a DS-style paging object.

Options:

- `--search SEARCH`
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
- `groupType`
- `description`
- `createTime`
- `updateTime`
- `createUserId`

Rules:

- `search` is passed through to the upstream alert-group name filter
- use `dsctl alert-group list` to discover alert-group names and ids
- `groupType` is the legacy `EMAIL`/`SMS` value on DS `1.3.9` and `null` on
  plugin-instance-based profiles; fields unavailable in an older list
  projection are returned as `null`, not guessed
- use `alert-group get` for association details when the list projection
  omits them; a missing `alertInstanceIds` value is not an absence check

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

- `--name NAME` required
- `--instance-id N` repeatable on plugin-instance-based profiles
- `--group-type EMAIL|SMS` on DS `1.3.9`
- `--description DESCRIPTION`

Rules:

- use `dsctl schema --command alert-group.create` to obtain the selected
  version's association input
- on DS `1.3.9`, `--group-type` is required and `--instance-id` is unavailable
- from DS `2.0.0`, use `dsctl alert-plugin list` to discover alert plugin
  instance ids; `--group-type` is unavailable, repeated `--instance-id` values
  are deduplicated, and omission sends an empty upstream `alertInstanceIds`
  string

## `dsctl alert-group update ALERT_GROUP`

Updates one resolved alert group while preserving omitted fields.

Options:

- `--name NAME`
- `--instance-id N` repeatable on plugin-instance-based profiles
- `--clear-instance-ids` on plugin-instance-based profiles
- `--group-type EMAIL|SMS` on DS `1.3.9`
- `--description DESCRIPTION`
- `--clear-description`

Rules:

- `ALERT_GROUP` may be an alert-group name or numeric id
- use `dsctl alert-group list` to discover alert-group names and ids
- use `dsctl schema --command alert-group.update` to obtain the selected
  version's association input
- on DS `1.3.9`, `--group-type` selects the native association and alert-plugin
  instance options are unavailable
- from DS `2.0.0`, use `dsctl alert-plugin list` to discover alert plugin
  instance ids; `--group-type` is unavailable
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

- `--search SEARCH`
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
- DS `3.2.0` through `3.4.3` may return their native `default` tenant with id
  `-1`. This exact identity is retained; other tenant identities remain positive.
- That default tenant may have `queueId=0` after a PostgreSQL upgrade, or a
  positive queue id after a fresh install. An update that omits `--queue`
  preserves this native value. Other tenants require a positive queue id;
  negative, boolean and missing queue identities are rejected.

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

- `--tenant-code TENANT_CODE` required
- `--queue QUEUE` required
- `--description DESCRIPTION`

Rules:

- `--queue` accepts a queue name or numeric id
- use `dsctl queue list` to discover queue names and ids
- the CLI resolves `--queue` to the upstream `queueId`

## `dsctl tenant update TENANT`

Updates one resolved tenant's mutable fields while preserving its tenant code.

Options:

- `--queue QUEUE`
- `--description DESCRIPTION`
- `--clear-description`

Rules:

- `TENANT` may be a tenant code or numeric id
- `--queue` accepts a queue name or numeric id
- use `dsctl tenant list` to discover tenant codes and ids
- use `dsctl queue list` to discover queue names and ids
- `tenantCode` is the tenant's immutable DS identity: choose it during
  `tenant create`; `tenant update` never changes it
- at least one field change is required
- omitted fields preserve the current remote values
- `--description` and `--clear-description` are mutually exclusive

The immutable identity rule matches the edit behavior in every supported
DolphinScheduler UI. It also prevents DolphinScheduler `1.3.9`'s unsafe
tenant-code update path: that upstream service rejects a new unused code and
can accept an already-used code because its existence check is inverted. This
is an upstream `1.3.9` defect, not a missing CLI implementation.

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

- `--search SEARCH`
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

- `--user-name USER_NAME` required
- exactly one of `--password PASSWORD` or `--password-file FILE` required
- `--email EMAIL` required
- `--tenant TENANT` required
- `--state {0,1}` required
- `--phone PHONE`
- `--queue QUEUE`

Rules:

- `--password-file FILE` reads one UTF-8 password; `--password-file -` explicitly
  reads stdin. It accepts one optional final line ending and preserves the
  password's other characters. File and literal password are mutually exclusive;
  missing/malformed input fails before mutation without echoing its contents

- `--tenant` accepts a tenant code or numeric id
- `--queue` is the raw queue-name override stored on the user record
- use `dsctl tenant list` to discover tenant codes and ids
- use `dsctl queue list` to discover queue names
- `--state 1` means enabled and `--state 0` means disabled
- DS 1.3.9 has no user-state field; `--state 1` is accepted as the compatible
  enabled default, while `--state 0` is rejected before a request is sent

## `dsctl user update USER`

Updates one resolved user while preserving omitted fields.

Options:

- `--user-name USER_NAME`
- `--password PASSWORD`
- `--password-file FILE`
- `--email EMAIL`
- `--tenant TENANT`
- `--state {0,1}`
- `--phone PHONE`
- `--clear-phone`
- `--queue QUEUE`
- `--clear-queue`
- `--time-zone TIME_ZONE`

Rules:

- `--password-file FILE` reads one UTF-8 password; `--password-file -` explicitly
  reads stdin. It accepts one optional final line ending and preserves the
  password's other characters. File and literal password are mutually exclusive;
  missing/malformed input fails before mutation without echoing its contents

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
- user state is unavailable on DS 1.3.9, and time-zone updates are unavailable
  before DS 3.0.0; unsupported field changes fail before a request is sent

## `dsctl user delete USER --force`

Deletes one resolved user.

Rules:

- `USER` may be a user name or numeric id
- use `dsctl user list` to discover user names and ids
- `--force` is required
- all 37 exact profiles bind the same reviewed `user-delete-form-v1` POST-form
  exchange; it remains a once-only mutation and is not retried after dispatch

Successful output returns:

- `data.deleted`
- `data.user`

## `dsctl user grant project USER PROJECT`

Grants one resolved project to one resolved user with write permission.

Rules:

- `USER` may be a user name or numeric id
- `PROJECT` may be a project name or the native numeric identity: project id
  on DS 1.3.9, project code on newer versions
- use `dsctl user list` and `dsctl project list` to discover selectors
- the CLI preserves existing project membership and applies one logical add.
  DS 1.3.9 and 3.0.0 through 3.1.9 replace the complete relation set, so their
  exact recipes submit and verify the merged set. These releases' public REST
  grant paths only assign write permission, so existing membership is a no-op.
- DS 2.0.0 through 2.0.9 use an insert-only grant path. Existing membership is
  a no-op to avoid duplicate relations; this release group has no public
  read-only grant path.
- DS 3.2.0 through 3.4.3 replace the selected project's relation with write
  permission. Existing membership still requires that write, because it can
  represent read-only permission.
- readback verifies project membership. It does not expose the relation's
  permission level or test execution as the target user; successful output
  states `verification: "membership_only"` alongside the requested write grant.
  Whole-set APIs do not provide a concurrency lock; serialize permission
  changes for one user to avoid competing read/modify/write updates.

Successful output returns:

- `data.granted`
- `data.permission`
- `data.verification`
- `data.user`
- `data.project`

## `dsctl user revoke project USER PROJECT`

Revokes one resolved project from one resolved user.

Rules:

- `USER` may be a user name or numeric id
- `PROJECT` may be a project name or the native numeric identity: project id
  on DS 1.3.9, project code on newer versions
- use `dsctl user list` and `dsctl project list` to discover selectors
- DS 2.0.0 and 2.0.1 have no safe single-project revoke operation, so the command reports
  the capability as unavailable before sending a request

Successful output returns:

- `data.revoked`
- `data.user`
- `data.project`

## `dsctl user grant datasource USER --datasource DATASOURCE ...`

Grants one or more resolved datasources to one resolved user.

Options:

- `--datasource DATASOURCE` repeatable and required

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

- `--datasource DATASOURCE` repeatable and required

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

- `--namespace NAMESPACE` repeatable and required

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
- namespace permissions require DS 3.0.0 or newer; older profiles reject the
  action before user resolution or any HTTP request

Successful output returns:

- `data.granted`
- `data.user`
- `data.requested_namespaces`
- `data.namespaces`

## `dsctl user revoke namespace USER --namespace NAMESPACE ...`

Revokes one or more resolved namespaces from one resolved user.

Options:

- `--namespace NAMESPACE` repeatable and required

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
- namespace permissions require DS 3.0.0 or newer; older profiles reject the
  action before user resolution or any HTTP request

Successful output returns:

- `data.revoked`
- `data.user`
- `data.requested_namespaces`
- `data.namespaces`

## `dsctl access-token list`

Returns a DS-style paging object.

Options:

- `--search SEARCH`
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

All 37 exact versions bind this exchange through the compiled access-token
domain, preserving each reviewed response epoch. The stable page projection
uses the requested page size and the server's validated current page.

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
- `--token` is optional; DS 1.3.9 and 2.0.0 require a concrete token on create,
  so the CLI first calls the main token-generation route and then creates it;
  newer profiles let the create route generate it

## `dsctl access-token update ACCESS_TOKEN`

Updates one access token by numeric id.

Rules:

- requires at least one of `--user`, `--expire-time`, `--token`, or
  `--regenerate-token`
- `--user` accepts a user name or numeric id
- use `dsctl access-token list` to discover token ids
- use `dsctl user list` to discover user names and ids
- omitted `--user` preserves the current remote value
- on DS `1.3.9` and `2.0.x`, supply `--expire-time` explicitly in the server's
  local `YYYY-MM-DD HH:MM:SS` format. These releases return a serialized Date
  with an offset, which cannot safely be copied back as server-local input
  without knowing the server timezone; the CLI does not guess it
- legacy Date readback retains the native representation. Verification checks
  the token identity and a valid date, but cannot prove expiry equality across
  the two representations without the server timezone
- from DS `3.0.0`, omitted `--expire-time` preserves the current remote value
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

Returns the raw API server actuator health payload when the selected exact
profile exposes that upstream endpoint.

Resolved fields:

- `endpoint`
- `scope`

Rules:

- `resolved.scope` is currently always `api_server`
- `data` is the DS API server health object returned by `/actuator/health`
- exact `1.3.9`, `2.0.0`, and `2.0.9` classify this action as
  upstream-absent and fail the capability check before sending an actuator
  request; exact profiles from `3.0.0` support it
- `dsctl doctor` handles that old-version boundary separately by recording an
  API warning with reason `upstream_endpoint_absent` and checking the
  authenticated current user as its readiness fallback

## `dsctl monitor server TYPE`

Lists registry-backed servers for one DS node type.

Arguments:

- on DS `1.3.9` through `3.2.1`, `TYPE` must be `master` or `worker`
- on DS `3.2.2` and newer, `TYPE` may also be `alert-server`

Rules:

- the payload is a JSON array
- each item keeps DS-native field names
- `resolved.node_type` returns the normalized DS enum name

Current item fields:

- `id`
- `host`
- `port`
- `serverDirectories`
- `serverDirectory`
- `heartBeatInfo`
- `createTime`
- `lastHeartbeatTime`

`serverDirectories` is the canonical, sorted, de-duplicated directory list.
It preserves every directory returned by the legacy Worker payload.
`serverDirectory` remains a compatibility scalar: scalar upstream payloads
retain their exact value, while a legacy Worker payload sets it only when
exactly one directory exists. It is `null` when multiple worker directories
make a scalar ambiguous.

## `dsctl monitor database`

Lists database health metrics reported by the DS monitor API.

Resolved fields:

- `endpoint`

Rules:

- the payload is a JSON array
- each item keeps DS-native field names
- unhealthy databases keep their raw `state` in `data` and also emit a warning
- when that warning is present, the `warnings[]` item uses code
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

- on DS `3.0.0` through `3.2.1`, `--model-type` and `--operation-type`
  each accept at most one value; schema exposes the exact upstream enum
  choices, and `--model-name` is unavailable
- on DS `3.2.2` and newer, `--model-type` and `--operation-type` are repeatable
  and mapped to DS CSV filter text; use `dsctl audit model-types` and
  `dsctl audit operation-types` to discover values
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

The stable item shape is also used for DS `3.0.0` through `3.2.1`:
`resource`, `resourceName`, and `time` project to `modelType`, `modelName`, and
`createTime`; fields that did not yet exist (`description`, `detail`, and
`latency`) are `null`.

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
- use `dsctl project list` to discover project names and native identifiers
- `--search` is passed to the upstream paged list operation
- `--page-no` and `--page-size` select one page
- `--all` exhausts pages up to the shared safety limit

The `data` payload is standard page data. Rows live at `data.totalList`; page
metadata includes `total`, `totalPage`, `pageNo`, `pageSize`, and
`currentPage`. Like `project list`, single-page and `--all` responses retain
`data.coverage`. Aggregated `total` counts collected rows; the original totals
and requested range remain in coverage, without claiming an atomic snapshot.
Current row fields are:

- `id` on `1.3.9`, or `code` on newer profiles
- `name`
- `version`
- `releaseState`
- `scheduleReleaseState`
- `scheduleId`

The public list uses the rich paged workflow endpoint. Name/code resolution
uses a separate lightweight reference endpoint, so improving public discovery
does not make every selector resolution fetch a rich page.

## `dsctl workflow get`

Fetches one workflow by name or numeric native identity (`id` on `1.3.9`,
`code` on later profiles).

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit positional argument
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Output:

- default output returns the workflow payload in the standard JSON envelope
- the service resolves the independently persisted attached schedule instead
  of trusting the DS workflow-detail payload; `data.schedule=null` therefore
  means the authoritative lookup confirmed that no schedule exists
- every non-null nested workflow `schedule` summary includes a positive numeric
  `id`; workflow YAML export intentionally omits that persisted identity
- attached-schedule lookup errors fail the command with a structured error;
  they are never downgraded to `schedule:null`

## `dsctl workflow export`

Exports one workflow by name or numeric native identity as raw YAML for clone/create
or read-only schedule-aware definition editing.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit positional argument
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Output:

- writes only the workflow YAML document
- global display options do not change successful raw YAML; they still shape
  structured errors
- an attached schedule is preserved as a `schedule:` block from the
  authoritative schedule resource; lookup failure does not emit partial YAML
- the same document can be passed directly to `workflow create --file` or
  `workflow edit --file`: create treats `schedule:` as desired state, while edit
  verifies it as a read-only snapshot and never changes the schedule

## `dsctl workflow describe`

Returns one workflow DAG as structured JSON:

Use `dsctl project list` and `dsctl workflow list` to discover selectors.

- `data.workflow`
- `data.tasks`
- `data.relations`
- `data.workflow.schedule` and `scheduleReleaseState` use the same authoritative
  zero-or-one schedule lookup as `workflow get`

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
- each task entry keeps DS-native identity fields (`id` plus `name` on `1.3.9`,
  or `code` plus `name` on code-native profiles) and `taskType`
  and adds compact graph links (`upstreamTasks`, `downstreamTasks`)
- omits verbose task payload fields such as `taskParams` and retry/timeout
  details to reduce context size before a caller decides whether to fetch the
  full `workflow describe` or `workflow export` view
- `data.workflow.schedule` uses the authoritative attached-schedule lookup

## `dsctl workflow create`

Creates one workflow definition from a YAML file.

Options:

- `--file FILE` (required)
- `--project PROJECT`
- `--dry-run`
- `--confirm-risk TOKEN`

Rules:

- preview and apply reject a same-project workflow name already present in the
  visible identity inventory before task-code allocation or mutation. This read
  does not reserve the name; the server still detects races during create

- use `dsctl template workflow --raw` to start a workflow YAML file
- use `dsctl schema --command workflow.create` for workflow metadata and schedule
  fields under `command.payload.yaml_schema`, independently of CLI options
- use `dsctl template task` to discover task template variants
- use `dsctl task-type schema TYPE` to inspect bounded task fields, returned
  value selectors, related discovery commands, and state rules before writing
  `task_params`; request JSON Schema or compile mappings only when needed
- use `dsctl project list` to discover project names and native identifiers for `--project`
- project selection precedence is:
  - explicit `--project`
  - then `workflow.project` from the YAML file
  - then `context`
- `--dry-run` returns the compiled selected-version DS form request inside the standard
  dry-run envelope; when additional lifecycle steps would run, `data.requests`
  contains the ordered request plan
- dry-run and apply prepare through the same profile-bound generated definition
  program; apply executes its detached encoded request instead of reconstructing
  the selected-version route or form from rendered preview JSON
- exact `1.3.9` compiles the canonical YAML into its native string-id/name
  whole-definition graph and preserves that identity on edit/export; the CLI
  does not fabricate task or workflow codes
- an exact `1.3.9` canonical `SUB_WORKFLOW` task selects its child with
  one nonblank literal `task_params.childWorkflowName`. Create/edit resolves
  that value by name in the selected project and freezes its positive id; a
  numeric-looking name remains a name. The legacy graph compiler alone emits
  `SUB_PROCESS.params.processDefinitionId`. The service then iteratively audits
  the final compiled graph, following every task `params.processDefinitionId`
  exactly as the legacy runtime does, and rejects malformed runtime edges,
  missing definitions, permission failures, cycles, or a closure beyond 1,000
  loaded descendants. Either legacy detail code `50001` or `50003` is a
  not-found result. These failures are translated to stable CLI errors before
  mutation. Raw opaque create/edit is closed. Graph-read hydration performs one
  same-project identity inventory and reverses only safe immediate exact-id
  packages; placeholder-bearing or ambiguous names, unresolved ids, and richer
  native tasks remain opaque, and grandchildren are never audited
- prepared requests use one ordered `data.requests` array without a first-item
  alias. Default dry-run shows `changes` (name, release state and task names),
  stage order and any schedule impacts; `--columns requests` expands the audit
- a successful create dry-run includes a complete, mutating
  `next_actions[].command` that applies the same file and resolved project; a
  required schedule confirmation token is preserved in that command
- dry-run task codes are deterministic preview values and are never sent or
  persisted; applied create first completes the same local compile preflight,
  then obtains persistent task codes from DolphinScheduler's REST API
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
- an applied create returns the final workflow payload; when the YAML contains
  a `schedule:` block, `data.schedule` is the created attached-schedule summary
  (including `id`) and `data.scheduleReleaseState` reflects its final
  `OFFLINE` or `ONLINE` state even when the upstream workflow detail response
  does not embed schedules
- high-frequency schedules reuse the standard `confirmation_required` flow and
  expect the same command to be retried with `--confirm-risk TOKEN`
- the same high-frequency confirmation rule applies to `--dry-run`
- the schedule preview, confirmation and dry-run request use resolved project
  preferences. A change to effective settings invalidates the previous
  confirmation. If settings change after workflow creation but before schedule
  creation, that stage stops and reports the completed workflow mutation;
  inspect the workflow before creating its schedule separately.
- when a confirmed high-frequency schedule warning is emitted, the aligned
  `warnings[]` item uses code `confirmed_high_frequency_schedule`
- workflow dynamic parameter time-format warnings use the same
  `parameter_time_format_*` codes as `lint workflow`, both in dry-run and
  applied create results
- parameter semantic warnings use the same
  `parameter_global_self_reference`,
  `parameter_local_self_reference` and
  `sub_workflow_local_params_not_child_inputs` codes as `lint workflow`
- the exact definition recipe sends the DS-native `tenantCode` form field only
  from `2.0.0` through `3.1.9`; `1.3.9` and `3.2.0` or newer omit it. This is an
  internal wire difference, not a workflow YAML field

Workflow YAML is a stable `dsctl` semantic contract. Templates may improve
their prose, comments, examples, ordering, and formatting, but accepted fields,
omission rules, meanings, and documented round-trip guarantees remain
compatible for the advertised create, edit, export, and preservation facets on
the selected exact profile unless the CLI deliberately revises the authoring
contract. This does not promise that a facet is portable to a profile where it
is unavailable or make arbitrary native server state part of the YAML surface.
Typed create, typed edit, and opaque preservation are independent promises. An
unavailable facet fails before HTTP and is never silently dropped. The current
YAML dialect does not require a file-level contract version; a future
incompatible dialect must not silently reinterpret existing YAML and requires
an explicit version or migration diagnostic.

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
  - version-selected `timezone` (unavailable and omitted on `1.3.9`)
  - version-selected `missed_fire_policy` (available on exact `3.4.3`)
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
  - `command` for `SHELL` and `PYTHON`
  - `worker_group`
  - `priority`
  - `retry`
  - `timeout`
  - `delay`
  - `depends_on`
- task identity fields such as DS task `code` and `version` are system-managed
  and are not authored in workflow YAML
- `REMOTESHELL` requires `task_params.rawScript` and
  `task_params.datasource`; `task_params.type` defaults to the DS-native `SSH`
  value when omitted

Per-task validation uses the single
[version-selected task authoring catalog](#version-selected-task-authoring)
and its exact [reviewed boundaries](../development/task-authoring-boundaries.md).
Use `dsctl task-type schema TYPE` for the selected fields and executable facet.
Typed input fails closed; existing opaque state can round-trip only under its
reviewed preservation policy. Catalog membership does not make every native
mode authorable.

Current dependency rules are intentionally narrow:

- `depends_on` expresses only basic in-workflow predecessor edges
- it does not encode DS logical task types such as `DEPENDENT`,
  `CONDITIONS`, or `SWITCH`
- those logical nodes belong in `type`-specific `task_params` models, not in a
  generic dependency DSL
- for `SWITCH` and `CONDITIONS`, branch targets are written as task names in
  YAML; exact `1.3.9` `CONDITIONS` preserves those names in its native outer
  wire, while code-routed profiles compile them into DS task codes during
  `workflow create`

## `dsctl workflow edit`

Edits one existing workflow definition from either a minimal YAML patch or a
full desired-state workflow YAML file.

Options:

- positional `WORKFLOW` is required for both `--file` and `--patch`
- exactly one of `--patch PATCH` or `--file FILE` is required
- `--project PROJECT`
- `--dry-run`
- `--confirm-risk TOKEN`

Rules:

- on exact DS `2.0.3`, relation changes return `unsupported_feature` before
  mutation because the upstream definition service can silently discard
  dependency rewires; this conservative restriction applies to patch and
  full-file edits, while ordinary fields and identity-preserving task renames
  remain available
- `--patch` is a delta edit:
  - the patch is applied against the current live workflow YAML export, then
    compiled back into one selected-version whole-definition update payload
  - patch YAML is a CLI delta document rooted at `patch:`, not a REST `PATCH`
    request
  - start new patch files with `dsctl template workflow-patch --raw`
- `--file` is a full desired-state edit:
  - the YAML uses the same full workflow shape as `workflow create --file`
  - the full file's `tasks:` list is the desired complete task set
  - same-name tasks preserve live DS `code + version`
  - tasks present in the YAML but absent from the live workflow are created
  - live tasks absent from the YAML are deleted and require `--confirm-risk`
  - full-file edit does not infer task renames; use `--patch` with
    `tasks.rename[]` when task identity must survive a name change
  - workflow rename and same-name task type changes also require
    `--confirm-risk`
  - an exported `schedule:` is a read-only concurrency snapshot: matching
    values are stripped before definition compile, while changed or missing
    attached schedule state is a pre-mutation `conflict`
  - deleting or nulling one exported schedule field is also a snapshot mismatch;
    remove the complete `schedule:` block to preserve the schedule without
    snapshot validation
  - use schedule commands for intentional schedule lifecycle or configuration
    changes
- target workflow selection for `--patch`:
  - explicit positional `WORKFLOW` is required
- target workflow selection for `--file`:
  - explicit positional `WORKFLOW` is required, so workflow rename intent is
    explicit
- project selection for `--patch` is `flag > context`
- project selection for `--file` is `flag > workflow.project > context`; if
  `--project` and `workflow.project` both exist and disagree, the command fails
- use `dsctl project list` and `dsctl workflow list` to discover selectors
- for a bounded dry-run review that omits the large compiled request, use
  `--columns diff,no_change,workflow_state_constraints,schedule_impacts`.
  Failed previews keep the complete semantic report; native request bodies
  still require `--columns requests` or `--columns '*'`
- current stable patch operations are:
  - `patch.workflow.set`
  - `patch.tasks.create`
  - `patch.tasks.update`
  - `patch.tasks.rename`
  - `patch.tasks.delete`
- patch YAML is a CLI delta document rooted at `patch:`, not a REST `PATCH`
  request
- exact `1.3.9` applies the prepared mutation as one whole-definition graph,
  preserving unknown graph fields and native string ids
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

Full-file example:

```yaml
workflow:
  name: daily-sync
  project: etl-prod
  description: Daily ETL workflow
  timeout: 3600
  global_params:
    bizdate: "${system.biz.date}"
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: |
      echo extract
  - name: load
    type: SHELL
    command: |
      echo load
    depends_on:
      - extract
```

Patch example:

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
  existing tasks and allocates new task codes from DolphinScheduler only for
  newly created tasks; the CLI completes local compile validation before that
  allocation request, and no-op edits request no codes
- exact `3.1.0` additionally reads each retained task's current main-table
  detail before a changed whole-workflow edit and sends that task's main-table
  `id` with its code. New tasks carry no `id`. The CLI rejects missing or
  mismatched task identities before writing and checks changed tasks against
  both the saved DAG and main-table detail after writing. This works around
  the `3.1.0` definition update's missing main-table id assignment; other
  versions do not make these task-detail reads
- task renames rewrite:
  - `depends_on`
  - `SWITCH` branch targets
  - `CONDITIONS` success and failure targets
- patch validation uses the same task-shape rules as workflow YAML:
  - `flag` accepts `YES` or `NO`
  - `task_group_priority` requires an effective `task_group_id`
  - `timeout_notify_strategy` requires an effective timeout greater than `0`
- workflow edit treats DS-semantic defaults as no-op equivalents when building
  `data.diff.task_changes`; for example:
  - `worker_group: default` and omitted `worker_group`
  - `timeout_notify_strategy: WARN` and omitted strategy on an open timeout
  - `cpu_quota: -1` or `memory_max: -1` and omitted resource limits
- `--dry-run` returns the merged selected-version exact update request plus:
  - `data.diff`
  - `data.workflow_state_constraints`
  - `data.workflow_state_constraint_details`
  - `data.schedule_impacts`
  - `data.schedule_impact_details`
  - `data.no_change`
- dry-run and apply prepare through the same profile-bound generated definition
  program; apply executes its detached encoded request instead of reconstructing
  the selected-version route or form from rendered preview JSON
- a changed edit requires the live workflow to already be offline. A known
  blocking online state makes dry-run return `ok: false`, `invalid_state` and
  exit 1, while retaining its diff, constraints and plan in `data`. A no-change
  preview remains successful. Apply repeats the same state checks
- applying a no-op patch or full file returns the current workflow payload,
  emits one warning with `warnings[]` code
  `workflow_edit_no_persistent_change`, and sends no update request
- when apply emits attached-schedule impact warnings, the aligned
  `warnings[]` items reuse the same codes as
  `data.schedule_impact_details[].code`
- workflow dynamic parameter time-format warnings use the same
  `parameter_time_format_*` codes as `lint workflow`, both in dry-run and
  applied edit results
- parameter semantic warnings use the same
  `parameter_global_self_reference`,
  `parameter_local_self_reference` and
  `sub_workflow_local_params_not_child_inputs` codes as `lint workflow`
- workflow edit does not mutate the attached schedule; schedule lifecycle stays
  on `schedule update|online|offline`
- a `schedule:` block from workflow export is accepted as a read-only snapshot;
  edit verifies its fields against the authoritative attached schedule, removes
  it from the definition compiler input, and fails before mutation when it is
  missing or changed
- omitted and explicit-null fields inside that block compare as null rather than
  acting as wildcards; only omitting the complete block opts out of validation
- the `ONLINE` schedule state in an older export may match a current `OFFLINE`
  schedule only when the workflow is offline and the document still requests
  the workflow itself `ONLINE`, because DS workflow offline cascades that exact
  schedule transition; a schedule-only lifecycle change remains a conflict
- schedule constraints, impacts, and returned workflow payloads use one
  authoritative lookup performed before any edit mutation

Current `data.diff` fields:

- `workflow_changes`: `{field,before,after}` entries for workflow fields
- `added_tasks`
- `task_changes`: `{task,changes}` entries; each change has `field`, `before`, `after`
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
- workflow selection: required positional `WORKFLOW`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- positional `WORKFLOW` is required
- `--force` is required
- the CLI fetches the current workflow before deletion and returns that payload
  in `data.workflow`
- deletion resolves the attached schedule before sending the mutation; lookup
  failure is fail-closed and sends no delete request
- the workflow must be offline before deletion
- workflows with online schedules must have their schedule taken offline first
- workflows with running workflow instances cannot be deleted until those
  instances stop or finish
- workflows still referenced by other tasks return `conflict` and suggest
  `workflow lineage dependent-tasks`

On exact DS `3.3.1` and `3.3.2`, whole-workflow deletion omits cleanup of owned
DEPENDENT lineage. The CLI reads the workflow's lineage before deletion and
returns `conflict` without sending DELETE when it finds owned lineage records,
including historical rows left after a task edit. The suggestion provides scoped
lineage inspection and offline commands. A failed or unverifiable lineage read
also stops deletion; `--force` does not bypass the check. Incoming references
remain subject to the server's normal deletion checks. Other versions retain
their native deletion path; DS `3.4.0` adds whole-workflow lineage cleanup.

This preflight observes a snapshot. The upstream REST API provides no atomic
read/delete operation, so prevent concurrent edits to the workflow while deleting
it. Keep affected definitions offline until an administrator addresses the
upstream cleanup defect. Removing DEPENDENT tasks does not reliably remove old
lineage, and deleting an old workflow version is unsafe as an automatic repair:
it clears all owned lineage before whole-definition deletion has succeeded.

The CLI cannot repair orphan rows left by earlier deletions. Such rows can cause
a later deletion of the referenced workflow to fail with upstream code `50022`,
but that generic code alone does not identify the cause. More specific diagnosis
of existing orphan rows remains follow-up work. Separately recorded acceptance
fixture cleanup does not establish defect-free native deletion.

Successful output returns:

- `data.deleted`
- `data.workflow`

## `dsctl workflow lineage list`

Returns the project-wide workflow lineage graph for one resolved project.

Selection rules:

- project selection: `flag > context`
- use `dsctl project list` to discover project names and native identifiers

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
- workflow selection: required positional `WORKFLOW`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- positional `WORKFLOW` is required
- the payload shape matches `workflow lineage list`
- on `2.0.0` through `2.0.9`, native lookup follows the selected workflow's
  ancestors only. To inspect an edge from an upstream workflow to a dependent
  workflow, select the dependent workflow. Selecting the upstream workflow does
  not discover its downstream consumers; use project-wide `workflow lineage
  list` for that graph.
- on `3.0.0` through `3.2.2`, native direct lookup can omit a dependency when
  its `DEPENDENT` task has no successor: its SQL joins predecessor task codes.
  Project-wide `workflow lineage list` uses a different lookup. A missing edge
  from `get` alone therefore does not prove that no dependency exists.

## `dsctl workflow lineage dependent-tasks WORKFLOW`

Returns workflows and tasks that depend on one resolved workflow or task.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit selector
- optional task filter is explicit-only through `--task`
- use `dsctl project list`, `dsctl workflow list`, and `dsctl task list` to
  discover selectors

Options:

- `--project PROJECT`
- `--task TASK`

Rules:

- positional `WORKFLOW` is required
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
- on `1.3.9` through `3.1.9`, upstream requires a workflow filter. An unfiltered
  CLI request enumerates visible workflows, reads their schedules, and pages
  the merged result in schedule-ID order. This requires more requests than a
  scoped `--workflow` read. Releases `3.2.0+` use project-wide server paging

Options:

- `--project PROJECT`
- `--workflow WORKFLOW`
- `--search SEARCH`
- `--page-no N`
- `--page-size N`
- `--all`

Rules:

- `--workflow` and `--search` are mutually exclusive
- use `dsctl project list` to discover project names and native identifiers for `--project`
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

- `SCHEDULE_ID` is id-first; optional `--project` takes precedence over the
  named-context project. A selected project bounds lookup with no global fallback
- use `dsctl schedule list` inside the selected project to discover schedule ids
- without project selection, bounded global ID lookup searches visible projects; on `1.3.9` through `3.1.9`
  it enumerates their workflows before requesting scoped schedule pages.
  Permission and missing-workflow errors propagate instead of silently
  presenting an incomplete search as a missing schedule

## `dsctl template workflow`

Returns the selected-version workflow YAML template inside `data.yaml`.
Table and tsv derive `line_no` and `line` from `data.yaml`; the JSON result
does not duplicate the artifact as line rows.

Use `--raw` when redirecting the template to a YAML file:

```bash
dsctl template workflow --raw > workflow.yaml
```

Options:

- `--with-schedule`
- `--example basic|output|branch|child|dependent`
- `--raw`

Rules:

- the template matches the selected version's `workflow create --file ...` YAML
  surface; `resolved.ds_version` identifies that exact version
- version selection follows local task templates and schemas: explicit
  `DS_VERSION`, otherwise a valid discovery cache for the configured URL/token;
  an untargeted invocation uses the `3.4.1` baseline
- `--env-file` selects the same isolated profile for JSON and raw output; a
  configured automatic target without a valid cache returns `config_error`
  with guidance to run `dsctl doctor` or set `DS_VERSION`, without probing
- `data.artifact.raw_command` preserves an explicit `--env-file` with shell-safe
  quoting so the suggested invocation emits the same target's YAML
- `data.artifact.target_command`, `data.related_command_patterns` and YAML
  guidance preserve that same explicit profile
- optional `workflow.description`, `timeout` (minutes) and `execution_type`
  are comments; the execution mode is shown only where the exact definition
  API supports it, and omitted values retain the authoring model's defaults
- it omits `schedule:` by default even though `workflow create` supports it
- this keeps the base template focused on the minimum full-spec workflow shape;
  schedule remains an optional add-on block
- omitting `--example` and selecting `--example basic` are identical, including
  metadata; this skeleton contains two SHELL nodes, `extract` then `load`, both referencing
  the example `workflow.global_params.bizdate`; comments explain task-local
  parameter ownership and replacing `command` with native `task_params`
- task nodes omit routine defaults and advanced runtime-control blocks; the
  selected task template and task schema own those fields and graph rules
- YAML comments point to the workflow file schema, task fragments, parameter
  precedence, local lint and the create preview. The workflow schema links to
  run and schedule settings; tenant is not an authored YAML field
- `dsctl template workflow --with-schedule` includes one minimal optional
  `schedule:` block and returns `resolved.with_schedule=true`
- that scheduled template sets `workflow.release_state: ONLINE`, which is
  required before DS can create the schedule; the schedule itself remains
  offline through `schedule.enabled: false`
- the optional `schedule.cron` example uses DolphinScheduler Quartz cron syntax
- the schedule includes `timezone` only when the exact schedule contract supports
  it; `1.3.9` instead explains the server-local timezone in a YAML comment
- `--raw` prints only the workflow YAML; it does not print the standard success
  envelope

The installed CLI provides complete, single-workflow compositions on demand:

| Example | Active graph and parameter source |
| --- | --- |
| `basic` | Two SHELL tasks using workflow globals; unchanged default. |
| `output` | SHELL producer OUT → consumer with `depends_on`; the selected exact profile determines implicit binding versus declared consumer IN. |
| `branch` | SWITCH, three named branch targets and an explicit join; `route` comes from workflow globals on every supported profile. |
| `child` | One parent with a SUB_WORKFLOW node; first create/release a separate child with `basic`, then discover and bind its same-project identity. |
| `dependent` | DEPENDENT followed by a SHELL task; replace definition identities and review the exact date window before use. |

`output` and `branch` are unavailable on `1.3.9`; requesting them returns
`unsupported_feature` with local discovery guidance. Every supported example
can combine with `--with-schedule`, with the same offline schedule default.
Non-basic examples report `resolved.example`. Their YAML and adjacent schema
navigation preserve an explicit `--env-file`. The templates do not create
external resources or resolve placeholders on the server. See
[Task composition examples](../user/task-examples.md) for the CLI retrieval and
review sequence; those instructions reuse these generated artifacts.

## `dsctl template workflow-patch`

Returns a workflow edit patch YAML starting point inside `data.yaml`.
Table and tsv derive `line_no` and `line` from `data.yaml`; the JSON result
does not duplicate the artifact as line rows.

Use `--raw` when redirecting the template to a YAML file:

```bash
dsctl template workflow-patch --raw > patch.yaml
dsctl workflow edit daily-etl --project etl-prod --patch patch.yaml --dry-run
```

Options:

- `--raw`

Rules:

- the template matches the stable `workflow edit --patch ...` delta surface
- the YAML root is always `patch:`
- the active default patch changes only `workflow.set.description`, so the file
  remains valid before task names are customized; timeout is an optional
  commented change in minutes, not an active default
- task create/update/rename/delete examples are included as comments; uncomment
  only the operations needed for the current edit
- `tasks.create[]` uses full task fragments from `dsctl template task TYPE --raw`
- `tasks.update[].set` uses partial task fields discoverable with
  `dsctl task-type schema TYPE`
- run `workflow edit --dry-run` before mutating the workflow definition

## `dsctl template workflow-instance-patch`

Returns a finished workflow-instance edit patch YAML starting point inside
`data.yaml`. Table and tsv derive `line_no` and `line` from that artifact;
the JSON result does not duplicate the YAML as line rows.

Use `--raw` when redirecting the template to a YAML file:

```bash
dsctl template workflow-instance-patch --raw > instance-patch.yaml
dsctl workflow-instance edit 901 --project etl-prod --patch instance-patch.yaml --dry-run
```

Options:

- `--raw`

Rules:

- the template matches the stable `workflow-instance edit --patch ...` delta
  surface
- the YAML root is always `patch:`
- `workflow-instance edit` only accepts `workflow.set.global_params` and
  `workflow.set.timeout` inside the workflow block
- the active default updates the command of the placeholder `failed-step`;
  replace that selector with the intended task before preview
- workflow timeout and global parameters are commented optional changes, so
  generating a repair patch does not reset them
- additional task create/update/rename/delete examples are comments; retain
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

- all topic details are selected by the exact `DS_VERSION`; they do not assume
  the offline-default `3.4.1` runtime
- DS task parameters use the upstream `Property` shape
- `Property.type` values come from the exact generated DS `DataType` enum;
  enum discovery and parameter templates report those values, while workflow
  compilation rejects values absent from that release
- `workflow.global_params` may use a mapping shorthand for IN VARCHAR values
- task-level parameters belong under `task_params.localParams`
- `task_params.varPool` is a runtime output pool and should normally stay empty
  in newly authored YAML
- the `context` topic returns the selected profile's effective precedence;
  precedence changes at the `2.0.0`, `3.0.0`, `3.2.0`, `3.2.1`, and `3.3.1`
  epochs and must not be inferred from the stable profile
- a same-name local self-reference such as `prop: label` plus `value: ${label}`
  shadows the workflow global and can become circular
- nested workflow task type, parent input sources, collision precedence, and
  child output contract are exact-version facts: releases through `3.2.2` use
  `SUB_PROCESS`, while `3.3.1` and newer use `SUB_WORKFLOW`; do not treat either
  task's `localParams` as a portable child-input map. Exact `1.3.9` accepts a
  same-project `childWorkflowName`; the parent workflow globals feed the child,
  a same-name child global wins, and task-level `localParams` are ignored
- `$[...]` time placeholders such as `$[yyyyMMdd-1]` are DS-native runtime
  expressions and are preserved as strings by the CLI
- DS uses Java-style date patterns inside `$[...]`: lowercase `yyyy` means
  calendar year, while uppercase `YYYY` means week-based year. The CLI emits
  warnings for risky expressions such as `$[YYYYMMdd]` or `$[yyyyww]`; use
  `year_week(...)` when week-of-year output is intended.
- task-output syntax is exact-versioned: `1.3.9` has no `setValue` output
  parser; `2.0.x` recognizes `${setValue(...)}` only at the start of a line;
  `3.0.0` through `3.2.0` also recognize `#{setValue(...)}` at line start; and
  `3.2.1` and newer scan the stream. From `3.3.1`, downstream binding also
  requires a declared same-name IN parameter and the latest non-empty upstream
  value wins
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
- `target_command_patterns`
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
- `target_command_patterns`
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
- `data.target_command_patterns`
- `data.type_enum`
- `data.type_discovery_command`
- `data.supported_types`
- `data.rows`

`resolved.view` is `list` for the default output. The supported type list lives
in `data.supported_types`; `resolved` does not duplicate it.

Typed template fields:

- `data.type`
- `data.target_command_patterns`
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
The index retains real task-type rows in `data.rows[]`. For a concrete
template, table/tsv derive line rows from `data.yaml`; JSON keeps only one copy
of the YAML artifact.

Options:

- `--variant VARIANT`
- `--raw`

Task template coverage for exact DS 3.4.1 includes every authorable
upstream default task type. Preserve-only runtime exclusions remain visible in
the upstream inventory but do not receive create/edit templates.

Typed versus generic coverage comes from the same
[version-selected task authoring catalog](#version-selected-task-authoring).
The selected type's schema, template and lint share its exact reviewed facet;
source-present preserve-only exclusions do not receive executable templates.
Use `dsctl task-type get TYPE` for current membership and scenario discovery.
A generic placeholder requires the operator to supply a reviewed native mode;
it is not an executable default.

Rules:

- omitting `TASK_TYPE` keeps the stable action `template.task` and returns
  `resolved.mode=index`
- task type matching is case-insensitive
- the normalized type is returned as `resolved.task_type`
- `resolved.task_category` reports the upstream DS category
- `resolved.template_kind` is `typed` or `generic`
- `resolved.variant` reports an explicitly selected scenario and is absent for default retrieval
- `data.template.variants` lists only complete, independently useful scenarios:
  coupled output code/declarations, resources/script invocation, reference target
  kinds, or distinct business operations. It excludes generic `minimal`/`params`
  selectors, field-only examples and aliases without execution semantics. A
  named upstream business operation can remain public even when selected by
  default, as with DVC `upload`
- omit `--variant` for the default fragment; its raw command does not add a
  selector. `minimal`, `params`, and pure alias selectors are rejected with
  current scenario discovery guidance; there is no compatibility variant list
- each task has one default fragment (DVC executes upload). Ordinary optional
  input, literal-map and SQL lifecycle fields use short commented hints derived
  from field examples. For SHELL/PYTHON, move `command` to `task_params.rawScript`
  before adding `localParams`. Complete resource and output scenarios stay
  separate rather than duplicating their bodies in default comments
- commented runtime controls are projected to the selected exact profile;
  unsupported environment, delay, task-group and resource-limit fields are not
  shown even in generic templates
- outer task `timeout` is in minutes; `0` disables it. Plugin-specific HTTP/gRPC
  timeout/deadline units remain independent. An open timeout defaults to WARN;
  choose FAILED/WARNFAILED intentionally and inspect the task's cancellation
  boundary. KUBEFLOW/PYTORCH retain their additional exact constraints
- `--raw` prints only the YAML task fragment, including comments linking to the
  exact task schema, reference discovery, relevant parameter topics and one
  complete `template workflow --example ...` composition; it does not print
  the standard success envelope. Navigation preserves an explicit `--env-file`
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

Scenario discovery is projected to the exact selected profile. It retains
DVC `upload`/`download`/`init`, EMR `add-steps`, HTTP `post-json`, DATASYNC
`raw-json`, DEPENDENT `task-dependency`, resource scripts, and supported output
scenarios. Use `dsctl task-type get TYPE` for the current list rather than a
second static version-by-variant catalog. DATASYNC `raw-json` is explicitly
selector-restricted opaque authoring; a family-level `template_kind=typed`
does not promise typed validation of every alternate mode.

On exact `1.3.9`, the `CONDITIONS` default template uses a closed
name-routing facet. Each predicate has only `task` and
`status`, each task name must resolve in the same workflow, both relation levels
are `AND` or `OR`, each group/list is nonempty, status is `SUCCESS` or
`FAILURE`, and `successNode` and `failedNode` each contain exactly one distinct
task name. `localParams`, `varPool`, resources, datasource fields,
and placeholders are rejected. The compiled definition keeps `dependence` and
`conditionResult` as outer `TaskNode` members beside exact `params={}`, using
native `depTasks` and branch names rather than task codes.

SWITCH is absent on `1.3.9`. On the 29 exact profiles from `2.0.0` through
`3.2.1`, its evaluator reads workflow globals and incoming runtime varPool,
not the SWITCH node's own `localParams`. The main template therefore directs `route` to `workflow.global_params`, or to a predecessor OUT
with an explicit `depends_on` edge. From `3.2.2`, the evaluator uses the prepared
map including local parameters. The complete `branch` workflow example uses a
global route on every supported profile. CONDITIONS instead tests upstream
execution states: the compiler adds its routing/dependency edges; a commented
`bizdate` parameter does not become part of its state predicate.

The `PIGEON` default template authors only
`task_params.targetJobName`, trims it, and requires it to remain nonblank. Its
template calls out that `p_host` belongs in the enclosing workflow globals
and must be available to the DS worker at execution time; a task-local
`localParams` entry does not supply this field.
Inherited task-parameter members and future native fields are not part of the
typed surface; opaque preservation keeps them losslessly when round-tripping
existing definitions.

The `DINKY` default template maps only to `DINKY/job_trigger` on exact
profiles from `3.1.0` through `3.4.3`; profiles through `3.0.6` do not contain
the plugin. It emits exactly required literal `address`, required nonblank
literal `taskId`, and optional strict-boolean `online`, defaulting to `false`.
The address must be an absolute HTTP(S) endpoint with a host and optional
literal port/path, but no URI userinfo, query, fragment, whitespace, control
text, or DS placeholder. The task id similarly rejects whitespace, control
text, placeholders, and shell-control syntax.

Typed DINKY owns no `localParams`, `varPool`, or future native field, including
when supplied as `null`. It has no public raw opaque-authoring selector;
excluded state remains lossless only on export and unchanged edits, and invalid
canonical input never downgrades to opaque. Runtime semantics are exact: the
legacy path forwards no variables through `3.2.0`; from `3.2.1`, the worker
negotiates the remote version, and only the Dinky 1.x `submitApplicationV1`
branch forwards variables. The negotiated legacy/v0 branch still forwards
none. On the v1 branch, `3.2.1` and `3.2.2` send workflow globals plus task
`localParams`; `3.3.1` through `3.4.1` additionally expand local placeholders
from the prepared context; and `3.4.2` and `3.4.3` send the full built-in, project,
workflow, task, command, `varPool`, and business prepared map and logs it at
INFO. Upstream also logs task parameters, request URLs, and response content.
Requests have no authentication or explicit HTTP timeout. There is no durable
submitted-job id or failover resume; retry may submit again, and cancellation
addresses `taskId`. This facet makes no confidentiality, live-evidence, or
profile-promotion claim.

The `HIVECLI` default template authors only inline `SCRIPT`. `hiveSqlScript` must be
nonblank and may contain DS `${...}` or `$[...]` placeholders;
`hiveCliOptions` is optional literal text and rejects either placeholder form.
This portable restriction absorbs an exact upstream behavior change: `3.1.x`
substitutes the complete assembled `hive -e` command, whereas `3.2.0` and
newer substitute only SQL text, write it to a temporary file, and invoke
`hive -f`. Authored HIVECLI `localParams` are unique `IN`/`VARCHAR`
properties. `FILE`, `resourceList`, `varPool`, runtime output, and future
native fields are opaque-preserve-only. Each eligible worker must already have
the Hive CLI in `PATH`, Hive and HDFS client configuration, and access to HDFS
and the Hive Metastore; HIVECLI does not use a DS datasource, and the CLI does
not install or configure that runtime.

The `DVC` variants map directly to `Upload`, `Download`, and `Init DVC` under
the `DVC/operation` facet on exact `3.1.0` through `3.4.3`; profiles through `3.0.6`
do not contain the plugin. Mode-specific fields must be present only
when active. Because upstream builds and logs a POSIX shell script without
quoting most values, repository, location, worker path, version, and store URL
accept only a conservative single-token subset, URI userinfo is rejected, and
version accepts only a portable Git tag, branch, or SHA. Messages may contain
spaces, single quotes, and Unicode but reject double quote, backslash, shell
expansion, and control text. No typed field accepts `${...}` or `$[...]`, and typed DVC exposes no
`localParams`, `varPool`, resources, or output parameters; opaque authoring
preserves all additional native state losslessly.

Before execution, route DVC tasks to a POSIX worker with `git` and `dvc` in
`PATH`, configured Git identity, repository permissions, network access, DVC
remote support, and credentials supplied through the worker's SSH agent,
credential helper, or provider configuration. Do not embed credentials in
task URLs. The plugin has no resumable remote job identity or failover, and
its script lacks `set -e` and does not guard every intermediate Git/DVC
operation; a successful final command can mask an earlier failure. The
upstream UI's `MLFLOW` initialization typo does not alter the exact `DVC` wire
identity.

The `MLFLOW` default template maps only to `MLFLOW/model_serve` on exact
profiles from `3.1.0` through `3.4.3`; profiles through `3.0.6` do not contain
the plugin. It emits exactly the five required native fields
`mlflowTaskType: "MLflow Models"`, `deployType: "MLFLOW"`,
`mlflowTrackingUri`, `deployModelKey`, and `deployPort`. The URI must be
absolute HTTP(S) with a host and an optional safe path, but no userinfo, query,
fragment, whitespace, control text, placeholder, quote, backslash, glob, or
shell metacharacter. Model keys accept only
`models:/name/version-or-stage` or `runs:/run-id/artifact[/subpath...]` with
nonempty conservative components and no `.` or `..` component. The port is a
wire string containing a decimal integer from `1` through `65535`.

These bounds are part of the public typed contract because upstream constructs
an unquoted POSIX script equivalent to exporting the tracking URI and invoking
`mlflow models serve -m MODEL --port PORT -h 0.0.0.0`. Projects mode, Docker
deployment, `localParams`, `varPool`, `resourceList`, `registerModel`, runtime
state, and future native members are not typed fields, including when supplied
as `null`; opaque authoring and export preserve them losslessly. This facet is
therefore not a claim of full MLFLOW support.

Route model-serving tasks only to POSIX workers with the `mlflow` CLI in
`PATH`, network access to the tracking server and artifact store, access to the
selected registered-model version or run artifact, an available listening
port, and tracking/artifact credentials configured in the worker environment
or runtime configuration. Credentials must not appear in URI userinfo. The
service is a foreground local process bound to `0.0.0.0`; DolphinScheduler can
cancel that process but has no remote deployment ID, reconnect, or failover
protocol. Retries can fail on an occupied port if an earlier process survives.
This typed review adds no live evidence and promotes no profile.

The `JUPYTER` default template maps only to `JUPYTER/preinstalled_notebook` on exact
profiles from `3.1.0` through `3.4.3`; profiles through `3.0.6` do not contain
the plugin. Both require a
shell-safe preinstalled `condaEnvName` plus distinct absolute POSIX-safe
`.ipynb` `inputNotePath` and `outputNotePath` values. Optional `parameters`
are a literal shell-safe string map; optional `kernel` and `engine` values are
single safe tokens; optional `executionTimeout` and `startTimeout` values are
strict positive YAML integers. The projector omits an empty parameter map,
sorts and compact-serializes a nonempty map to the native JSON string, and
converts each timeout integer to a native decimal string.

This typed facet does not create environments. `.tar.gz` and `.txt`
environment bootstrap, `resourceList`, `others`, `localParams`, `varPool`, DS
placeholders, runtime state, and future native members are rejected by typed
create/edit even when supplied as `null`. Only an exact lowercase `.txt` or
`.tar.gz` `condaEnvName` selects public opaque bootstrap authoring; export and
unchanged edits preserve other existing native forms losslessly. Parameter
keys block only the
case-insensitive exact names `password`, `passwd`, `secret`, `token`,
`credential`, `api_key`, `access_key`, and `private_key`, and parameter values
block URI userinfo. Those checks are not comprehensive secret detection or
redaction. Upstream may log all authored values and the assembled command, so
do not use task parameters as secret storage.

Route the task only to POSIX workers with `conda.path` configured, the selected
environment plus Papermill, Jupyter, kernel, and engine already installed, and
the input/output paths readable/writable as applicable. The output notebook is
a worker filesystem artifact, not a DS task output. The plugin has no remote
application id or failover-resume protocol, and a retry executes the notebook
again. This source review adds no live evidence and does not alter profile
promotion.

The `SPARK` default template maps only to `SPARK/inline_local_sql` on exact
profiles from `3.0.0` through `3.4.3`. The SPARK plugin exists on profiles through `2.0.9` but has no SQL program mode there, so those profiles expose
only their generic opaque template. Typed YAML contains exactly one field:

```yaml
name: run-inline-local-sql
type: SPARK
task_params:
  rawScript: SELECT 1 AS answer
```

`rawScript` must be nonblank literal SQL. Its canonical and wire spelling is
preserved, while `${...}`, `$[...]`, NUL, DEL, unsupported control text, and
any additional typed field are rejected. The exact projector adds the native
mode fields:
through `3.1.9` it emits `programType: SQL`, `sparkVersion: SPARK2`, and
`deployMode: local`; `3.2.0` and `3.2.1` emit `programType: SQL`,
`sqlExecutionType: SCRIPT`, and `deployMode: local`; from `3.2.2` it also emits
`master: local`.

Native `JAVA`, `SCALA`, and `PYTHON` programs remain explicit opaque modes on
the reviewed profiles, including their complete native cluster and resource
payloads. Native SQL `FILE` is also opaque from `3.2.0`. Those fields are not
part of the typed facet; cluster or resource fields added to inline SQL and
unrecognized inherited or future state remain unchanged preservation only.
Exact `3.0.x` writes the SQL file before final-command substitution; `3.1.0`
and newer expand SQL from the prepared parameter map
before writing it, but the stable typed contract forbids placeholders on every
profile. Workers use `SPARK_HOME2` through `3.1.9` and `SPARK_HOME` from
`3.2.0`, with Spark SQL, Java, Hadoop, Hive catalog configuration, and target
data permissions already available. Upstream logs raw or expanded SQL at INFO,
normalizes CRLF before writing the worker SQL file, and runs it as a synchronous
local process. Runtime line endings therefore need not be byte-identical to the
canonical wire. There is no durable application id or failover resume;
cancellation controls the local process and retry executes the whole SQL again,
potentially repeating side effects. This review refreshes no live receipt,
changes no `tested` flag, and promotes no profile.

The `FLINK_STREAM` default template maps only to
`FLINK_STREAM/inline_local_sql` in `Universal` on exact `3.1.5`–`3.1.9` and `3.2.0`
through `3.2.2`:

```yaml
name: run-inline-local-sql
type: FLINK_STREAM
task_params:
  rawScript: SELECT 1 AS answer
```

The plugin is upstream-absent through `3.0.6`. Exact `3.1.0`–`3.1.4` resolve `mainJar` before SQL initialization;
`3.1.1`–`3.1.4` expose explicitly native JAR scaffolds, without a typed SQL claim. Releases from `3.3.1`
through `3.4.3` also expose only generic opaque authoring because
`ExecutorServiceImpl.execStreamTaskInstance` immediately throws
`Not supported`; source-known task parameters do not make that runtime entry
executable. Typed YAML owns exactly the nonblank literal `rawScript` above.
The projector preserves its spelling and emits exactly four `taskParams`
fields: `programType: SQL`, `deployMode: local`, `initScript: ""`, and
`rawScript`. It also supplies top-level `taskExecuteType: STREAM`; do not add
that compiler-owned discriminator to `task_params` or task YAML. Every typed
profile writes UTF-8.

Typed authoring rejects blank scripts, carriage returns, DEL/C0/C1 controls,
unpaired Unicode surrogates, `${...}`, `$[...]`, and every extra field.
Multi-statement SQL, TAB/LF, quotes, semicolons, backslashes, backticks, and
`$()` remain literal. Recognized native `JAVA`, `SCALA`, and `PYTHON` JAR
programs and exact non-local SQL modes are explicit opaque create/edit.
Local-inline extras and unrecognized inherited or future state remain
unchanged/export opaque-preserve-only; invalid local SQL never falls open to
opaque authoring.

Route all eight typed releases to a worker whose `PATH` resolves
`sql-client.sh`. Every eligible worker needs the Flink SQL client, Java,
connector and catalog configuration, and target-data permissions. Later
`FLINK_HOME` and prepared-substitution plugin behavior stays outside typed
membership because the corresponding STREAM entry is unsupported. Upstream
logs task parameters, SQL content and file paths, and commands at INFO, so do
not place secrets in the task.

Local SQL is expected to publish no application id and exposes no result
output, durable submit identity, or failover resume. Plugin cancel and
savepoint both require an application id. Stop order is plugin cancel then
PID-tree kill on `3.1.x`, PID-tree kill then plugin cancel from `3.2.0` through
`3.2.2`, and plugin cancel only from `3.3.1` through `3.4.3`; the last epoch
returns on the missing id with no process fallback. Reliable stop is
unsupported, so an unbounded stream may continue after the DS task stops.
Retry reexecutes the whole SQL on typed releases and can repeat side effects.
Keep retries at zero unless replay and continued-stream risks are acceptable.
This review adds no live evidence, changes no `tested` flag, and promotes no
profile.

The typed `K8S` default template maps only to
`K8S/literal_container_job` in `Cloud` on exact profiles from `3.1.4` through
`3.4.3`. Exact `3.1.0`–`3.1.3` instead generate explicitly risky native opaque
scaffold: it omits canonical `connectionMode`/`cluster`, stores nonblank
`name`/`cluster` in compact `namespace` JSON, and requires a nonblank `image`.
That selector prevents canonical connection intent from downgrading to opaque;
the watcher defect still makes the scaffold non-attested for execution. The
selected typed version determines its connection fields. Exact `3.1.4`–`3.1.9` use:

```yaml
name: run-container-job
type: K8S
task_params:
  connectionMode: NAMESPACE
  namespace: analytics
  cluster: production
  image: busybox:1.36
  minCpuCores: 0.0
  minMemorySpace: 0.0
  environment: []
```

Exact `3.2.0` through `3.2.2` keep the same three connection fields but add the
advanced fields described below; use the selected-version template rather
than copying the `3.1.x` block unchanged. The projector encodes `namespace`
and `cluster` as one compact native `namespace` JSON string. Exact `3.2.2` is
an upstream mismatch: its UI hides the namespace selector and
`checkParameters` validates only `image`, while the backend, master, and
runtime continue to require the legacy namespace wire. The CLI therefore
still requires both canonical connection values. Exact `3.3.1` and newer
instead use:

```yaml
name: run-container-job
type: K8S
task_params:
  connectionMode: DATASOURCE
  datasource: 7
  image: registry.example.com/jobs/report:2026-08-20
  minCpuCores: 0.5
  minMemorySpace: 256
  environment: []
  command: []
  args: []
  imagePullPolicy: IfNotPresent
  customizedLabels: []
  nodeSelectors: []
```

`datasource` is a strict positive K8S datasource id. Compilation fixes native
`type: K8S` and empty `namespace` and `kubeConfig` fields for worker-side
datasource resolution. The image is a nonblank literal tag or digest; a tag
can move, so use a digest when immutable image identity is required. CPU and
MiB-memory values must be finite and nonnegative. `environment` entries are
unique literal name/value pairs compiled to `IN`/`VARCHAR` `localParams`; the
name `taskInstanceId` is reserved. The outer task name must match
`^[A-Za-z0-9][A-Za-z0-9-]{0,51}$`. Uppercase is valid: runtime lowercases it
with `Locale.ROOT`, then appends `-` and the decimal task-instance id.

From exact `3.2.0`, the selected-version template and schema also expose
ordered string-array `command` and `args`, an optional `pullSecret` Kubernetes
Secret object name, `imagePullPolicy`, `customizedLabels`, and
`nodeSelectors`. The compiler serializes those structured canonical values to
the exact compact native JSON-string fields. Custom-label keys must be unique
and Kubernetes-valid and cannot be `k8s.cn/layer`, `k8s.cn/name`, or
`dolphinscheduler-label`. Exact `3.2.0` requires at least one custom label
because its executor mutates an immutable empty map; later exact profiles
permit `customizedLabels: []`. Custom-label values may be empty. Exact `3.2.0`
attaches custom labels to the Job only; `3.2.1` and newer attach them to both
Job and Pod template. Node-selector keys may repeat because separate
expressions are ANDed. `In`/`NotIn` require a nonempty list of unique nonempty
Kubernetes label values; `Exists`/`DoesNotExist` require an empty list, while
`Gt`/`Lt` take one decimal-integer string in the list, from zero through
`9223372036854775807`, for example `values: ["1"]`.

Exact `3.1.4`–`3.1.9` expose none of those advanced fields: the worker uses the image
ENTRYPOINT/CMD and fixes `imagePullPolicy=Always`.

The optional `outputs` list is present only on exact `3.2.0`, `3.2.1`,
`3.2.2`, `3.4.2`, and `3.4.3`. Each unique Kubernetes-style name excludes
`taskInstanceId`, is disjoint from input environment, and compiles to an
`OUT`/`VARCHAR` native `localParams` entry with `value: ""`.
The container must write a terminal marker that matches its exact runtime:
`${(name=value)dsVal}` or `#{(name=value)dsVal}` on `3.2.0`, and
`${setValue(name=value)}` or `#{setValue(name=value)}` on `3.2.1`, `3.2.2`,
`3.4.2`, and `3.4.3`. A published marker value must be nonempty on `3.2.0` and `3.2.1`.
The `3.2.0` legacy parser reserves `$VarPool$` as a delimiter and keeps only
the first segment after `name=` when the value contains another `=`; `3.2.1`
preserves the rest of such a value. Exact `3.2.2`, `3.4.2`, and `3.4.3` preserve
`=` and support an empty published value. Exact `3.3.1` through `3.4.1` expose no
typed outputs because the physical executor does not return its mutated
`varPool`. On `3.4.2` and `3.4.3`, each OUT declaration also enters the prepared map and
appears in the Pod as an empty-valued environment entry.

The worker injects the full prepared parameter map into the Pod, not just
`environment`. Every global, built-in, and inherited key must therefore be a
valid Kubernetes environment name and must not collide with
`taskInstanceId`; dsctl can statically check only the authored task entries.
Job watch registration omits the target namespace, so the selected namespace
must match the kubeconfig current-context namespace. Exact `3.3.1` through
`3.4.1` also fail to propagate the datasource namespace to Pod-log/output
lookup; `3.4.2` repairs that context path.

Do not place secrets in these fields or prepared values. Upstream logs Pod
output; on `3.2.x`, full task/context logging can expose configuration outside
the task-log converter. From `3.3.1` it writes datasource-resolved kubeconfig
into task parameters and INFO-logs the resolved object without reliable
masking. `pullSecret` is only an object name, never secret content. The plugin
publishes no durable Kubernetes job id and has no failover-resume protocol.
Cancel requires the same worker's in-memory Job; retry or worker loss can
duplicate execution and side effects. Typed coordinates have no raw opaque
create/edit selector. Richer native, runtime, and future state remains
lossless only through unchanged edit or export provenance. Profiles through `3.0.6`
are upstream-absent; exact `3.1.0`–`3.1.3` permit only the
selector-restricted native opaque risk scaffold described above because its
watcher can finish on `RUNNING` with exit status `-1`. Canonical connection
intent still fails closed. This source review adds no live evidence and
promotes no profile.

The `KUBEFLOW` default template maps only to
`KUBEFLOW/tfjob_manifest` in `MachineLearning` on exact profiles from `3.2.0`
through `3.4.3`:

```yaml
name: train-mnist
type: KUBEFLOW
task_params:
  namespace: kubeflow-team
  cluster: production
  yamlContent: |
    apiVersion: kubeflow.org/v1
    kind: TFJob
    metadata:
      name: train-mnist-${system.workflow.instance.id}
      namespace: kubeflow-team
    spec:
      tfReplicaSpecs:
        Worker:
          replicas: 1
          restartPolicy: OnFailure
          template:
            spec:
              containers:
                - name: tensorflow
                  image: registry.example/tf-mnist:1.0
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout_notify_strategy: FAILED
timeout: 60
```

The three canonical values are required literals. `yamlContent` is preserved
byte-for-byte after validation and must be ASCII with LF line endings. It must
parse as one mapping document for exactly `kubeflow.org/v1` `TFJob`; its
explicit `metadata.namespace` must equal canonical `namespace`, and
root `status` is forbidden. `spec.tfReplicaSpecs` must be a nonempty mapping.
`generateName` and server-owned `uid`, `resourceVersion`, `generation`,
`creationTimestamp`, `deletionTimestamp`, `deletionGracePeriodSeconds`,
`managedFields`, and `selfLink` are forbidden. `metadata.name` consists of a
DNS-safe prefix no longer than 52 characters followed by exactly the terminal
`${system.workflow.instance.id}` placeholder. No other `${...}` or `$[...]`
placeholder is allowed. Only JSON-core scalar tags (`string`, `null`, `bool`,
`int`, and `float`) are accepted; YAML timestamp, binary, set, and custom tags
are rejected. Duplicate keys anywhere, aliases, merge keys, control characters,
multiple documents, and `List` roots fail closed.

Compilation emits only unchanged `yamlContent` and compact native namespace
JSON with `name` and `cluster`; it does not emit `localParams` or
`resourceList`. Exact empty UI residue in those two fields canonicalizes when
reading a server baseline. Nonempty or additional native state remains
unchanged/export opaque-preserve-only, and no raw opaque create/edit selector
exists.

Export remains read-only. Saving that richer opaque KUBEFLOW state is allowed
only unchanged or through a description-only or task-name-only patch; a task
rename in such a patch is metadata-only. Any task/workflow execution-semantic
or topology edit fails closed.

The master uses canonical `cluster` to obtain kubeconfig. The worker ignores
the outer namespace when it invokes fixed `kubectl apply/get/delete -f`
commands, making the manifest namespace the effective runtime target. It
substitutes prepared values before writing the manifest with the worker's
platform-default charset, which is why the typed model is ASCII-only. INFO
logs include complete params, expanded YAML, commands, status JSON, and the
complete resolved kubeconfig on all nine versions. The task-log filter present
on exact `3.3.1` and newer does not suppress that worker-service INFO event. Do not
put secrets in this task.

On a valid status shape, polling succeeds only when the root phase or final
condition type is `Succeeded`, `Available`, or `Bound`, and fails only for
`Failed`; missing `status`/`conditions` or other nonterminal state can poll
until the task timeout. An existing empty `status.conditions` array instead
makes all nine exact `KubeflowHelper` implementations unconditionally access
its last element and fail at runtime rather than continue polling. A compatible
nonempty conditions protocol is a runtime prerequisite. `appIds` stores only an
already-submitted sentinel, not a Kubernetes object identity. A persisted
sentinel lets failover derive status again from the same manifest because its
workflow-instance-scoped identity is stable, but apply can succeed before that
callback and be reapplied. The template requires `retry.times=0`: retry in the
same workflow observes or reapplies the same terminal TFJob and does not
guarantee a fresh run. That field disables only DolphinScheduler task retry;
the TFJob/Kubernetes controller and Pod `restartPolicy` can still repeat
training work. Typed compilation also rejects duplicate
`(cluster, namespace, metadata.name template)` identities within one workflow,
preventing sibling tasks from applying or deleting the same TFJob. There is no
structured output or command-level timeout. Provide
`kubectl`, the resolved kubeconfig, network/RBAC, the Kubeflow TFJob CRD and
compatible status semantics, and a shell-safe task base path. Typed workflow
compilation requires `timeout > 0` in native minutes and
`timeout_notify_strategy` set to `FAILED` or `WARNFAILED`; omitted/default or
explicit `WARN` emits a warning but does not terminate the watcher. The
template uses `timeout_notify_strategy: FAILED` and `timeout: 60` for one hour
because unknown status can poll indefinitely.

Recovery-style REPEAT/RERUN paths can reuse the same `workflowInstanceId` and
rendered `metadata.name`; same-id recovery may reobserve an already-successful
TFJob without new training. `REPEAT` does not guarantee fresh training. Only
creation of a new workflow instance provides a fresh identity.
`dsctl workflow-instance rerun`, `recover-failed`, and `execute-task` now fail
closed by default when the available instance `dagData` contains any
`KUBEFLOW` task. Use `dsctl workflow run WORKFLOW --project PROJECT` to create
a new workflow instance. This is a `dsctl` protection only: it cannot constrain
the DolphinScheduler UI or direct REST calls, and it makes no KUBEFLOW detection
claim when `dagData` is unavailable.

When an existing server task's `taskParams` decode to this canonical facet but
its outer retry, timeout, or `WARN` strategy does not meet the new gates, a
description-only patch or a file edit that leaves every execution-affecting task
field unchanged preserves that baseline exactly. Changing type, params,
command, flag, worker/environment selection, task group/priority,
retry/timeout/strategy/delay, resource limits, dependencies, or workflow-level
execution settings—including `release_state` transitions such as `OFFLINE` to
`ONLINE`, workflow timeout, `execution_type`, and global parameters—reruns the corresponding
gates. Standalone YAML lacks that provenance and must pass them immediately.
This claim performs no live preflight or campaign, refreshes no receipt, and
promotes no profile; prior `3.4.1`
receipts are stale for
the expanded manifest.

The `JAVA` default template maps only to `JAVA/literal_fat_jar` on exact
`3.2.0`, `3.2.2`, and `3.3.1` through `3.4.3`:

```yaml
name: run-java-fat-jar
type: JAVA
task_params:
  mainJar: /jobs/daily-orders.jar
  mainArgs:
    - --date
    - "2026-08-30"
```

`mainJar` is the `fullName` of an already uploaded DolphinScheduler resource,
not a worker-local path. It must be absolute, shell-safe, and end in `.jar`.
Use `dsctl resource list` to discover it. `mainArgs` is an array because the
compiler owns the exact single-space join; each item must be one nonblank
shell-safe token without whitespace, expansion syntax, or DS placeholders.
The compiler writes the same resource to native `mainJar` and `resourceList`
so the worker downloads it, fixes empty parameter/JVM state and a false module
path flag, then selects `JAR` plus empty `rawScript` on `3.2.0`/`3.2.2` or
`FAT_JAR` plus empty `mainClass` from `3.3.1`.

Do not use typed JAVA on `3.2.1`: its executor double-prefixes the already
absolute staged JAR path. On that exact version, only a nonblank native
`runType=JAVA` raw-source payload may use selector-restricted opaque
create/edit; broken `JAR` state is preserve-only, and canonical
`mainJar`/`mainArgs` input fails closed instead of downgrading. Raw Java source
on the other legacy typed wires and `NORMAL_JAR` on the modern wire are also
explicit opaque-authoring modes. JVM arguments, module path,
parameters/output, extra resources, and richer native state are not part of
this typed facet. Workers need `JAVA_HOME`, a compatible JDK, tenant permission,
and resource-storage access. Upstream logs complete
params, the final shell command, and child output at INFO, so authored values
are not secret storage. Exact `3.2.x` cancellation destroys only the direct
process and can leave children; `3.3.1` onward kills the process tree and
attempts generic application cancellation. There is no durable JAVA
application id or failover reattachment; a retry reruns the whole JAR and can
duplicate side effects.
This review refreshes no live evidence and promotes no profile.

The `MR` default template maps to `MR/literal_java_jar_job` in `Universal` on
all 37 exact profiles:

```yaml
name: run-mapreduce-jar
type: MR
task_params:
  mainJar: /jobs/wordcount.jar
  mainClass: com.example.WordCount
  mainArgs:
    - hdfs:///input
    - hdfs:///output
```

`mainJar` is an absolute shell-safe DS resource `fullName` ending in `.jar`,
not a worker-local path. `mainClass` must be a strict dotted ASCII Java class,
and each `mainArgs` item must be one shell-safe token; the compiler joins the
items with one space. Through `3.1.9`, the service first resolves the resource
to an exact visible non-directory file with a positive id and sends
`mainJar={id}`. From `3.2.0`, it sends `mainJar={resourceName}` and
`yarnQueue=""`. Every version fixes `programType=JAVA`, empty
`appName`/`others`, and empty `localParams`/`resourceList`. Do not duplicate the
JAR in `resourceList`: upstream MR adds `mainJar` to the runtime resource list.
Queue selection remains runtime-owned.

Native `SCALA` is the only selector-restricted explicit opaque create/edit
mode. Do not author native `PYTHON`: the UI advertises it, but the executor
still invokes `hadoop jar`. Richer or legacy resource state remains
unchanged/export preserve-only. Workers need Hadoop/YARN configuration,
tenant and DS resource permission, queue access, and data access. Upstream
INFO-logs full params, command files, final commands, and child output. MR has
no DS output, durable callback identity, or failover reattachment;
cancellation is best-effort from an observed application id, and retry or
failover can rerun the whole JAR. Exact `3.3.1` onward also has a post-exit
`appIds` context transport hole. This review refreshes no live evidence and
promotes no profile.

The `SQOOP` default template maps to `SQOOP/literal_command` in
`DataIntegration` on all 37 exact profiles:

```yaml
name: import-orders
type: SQOOP
task_params:
  subcommand: import
  args:
    - --connect
    - jdbc:mysql://db.example.invalid:3306/source
    - --username
    - sqoop_reader
    - --password-file
    - file:///run/secrets/sqoop-password
    - --table
    - orders
    - --target-dir
    - hdfs:///warehouse/orders
```

The compiler emits exactly one POSIX-quoted `sqoop import` or `sqoop export`
as native `customShell`, together with `jobType=CUSTOM` and `localParams=[]`.
Arguments are literal and ordered; unsafe text, placeholders, inline or
interactive passwords, and non-ASCII text on `3.1.1+` fail closed. Native
TEMPLATE jobs and arbitrary CUSTOM scripts remain selector-restricted opaque
authoring. SQOOP has no DS output, durable application id, or failover
reattachment; retry or failover can duplicate the transfer. This review
refreshes no live evidence and promotes no profile.

The `CHUNJUN` default template maps to
`CHUNJUN/literal_local_json_job` in `Other` on all nineteen exact profiles from
`3.1.0` through `3.4.3`:

```yaml
name: run-chunjun-job
type: CHUNJUN
task_params:
  json: |-
    {
      "job": {
        "setting": {"speed": {"channel": 1}},
        "content": []
      }
    }
```

`json` is the only canonical field. It must remain one unambiguous literal
JSON-object string without placeholders, CR, controls, or unpaired surrogates.
Compilation emits exactly strict integer `customConfig=1`, unchanged `json`,
and `deployMode=local`. Upstream supports `localParams` and prepared-map
substitution on every one of these releases; the typed template deliberately
omits `localParams` and rejects all DS placeholders because replacement is not
JSON-escaped. Explicit opaque nonlocal authoring requires strict
integer `customConfig=1` and exact `standalone`, `yarn-session`, or
`yarn-per-job`; built-in `customConfig=0`, UI typo `standlone`, local extras,
and richer state are preserve-only. Exact `3.1.0`–`3.1.6` UI default
`customConfig=false` is a UI defect; the fixed strict integer `1` REST wire
emitted by the compiler remains runnable.

All epochs normalize CRLF, substitute prepared values without JSON escaping,
and write UTF-8. Exact `3.1.0` uses nondurable post-exit log discovery;
`3.1.1` and later store no application id across the legacy shell,
shell-interceptor, and `taskRequest` epochs. Before running this template,
follow every exact upstream `chunjun.md`: remove the trailing background `&`
from the `nohup` command in `${CHUNJUN_HOME}/bin/start-chunjun` and route the
task only to a worker using that foreground launcher. Otherwise DS status and
cancellation are untrustworthy. Complete parameters and expanded JSON enter
INFO and DEBUG logs respectively. There is no structured output, durable id,
or failover resume. Cancellation remains best-effort: `3.1.0` through `3.1.9`
use legacy wrapper soft/hard kill, `3.2.0` through `3.2.2` direct-process
destroy/force, and `3.3.1` through `3.4.3` process-tree kill plus generic
application cancel. Retry replays the job. This review adds no live evidence
or profile promotion.

The `DATAX` default template maps to
`DATAX/literal_custom_json_job` in `DataIntegration` on every exact profile
except `3.1.0`:

```yaml
name: run-datax-job
type: DATAX
task_params:
  json: |-
    {
      "job": {
        "setting": {"speed": {"channel": 1}},
        "content": []
      }
    }
```

`json` is the only typed field. It must be a nonblank JSON object string;
arrays/scalars, nonstandard `NaN`/`Infinity` constants, duplicate keys at any
depth, CR, decoded C0/C1/DEL controls or decoded unpaired Unicode surrogates,
and `${...}`/`$[...]` in source or decoded keys/values are rejected. Accepted
spelling and LF formatting
are preserved. This is a bounded literal custom-job facet, not typed support
for datasource-generated jobs, arbitrary DataX plugin configuration, DS
parameters, or resource files.

The compiler sends only `customConfig=1` and `json` on `1.3.9`; from `2.0.0`
it also fixes integer `xms=1` and `xmx=1`. It never sends `localParams` or
`resourceList`. Reads canonicalize only exact empty UI residue:
`localParams=[]` on every release and `resourceList=[]` from `3.0.0`. Empty
resource state on earlier releases, any nonempty parameter/resource state,
generated mode, changed JVM memory, inherited state, and future fields remain
opaque and can survive unchanged/export preservation. Raw opaque create/edit
is closed on positive typed profiles.

On exact `3.4.3`, the literal JSON job must be a nonempty object. Empty object
forms such as `{}` select the native worker's JSON-resource fallback and are
rejected by this typed literal facet. Exact `3.4.2` still accepts them; the
CLI does not silently author a resource-backed job.

Do not use typed DATAX on exact `3.1.0`. When global, local, and `varPool`
inputs are empty, that runtime returns a null prepared map and dereferences it
while adding custom parameters. Under reviewed reason
`null-empty-prepare-params-map-breaks-custom-command`, raw opaque create/edit
requires strict integer native `customConfig=0` or `1`; canonical JSON-only
input fails closed. Its generic default template uses `customConfig=0` but is
deliberately non-executable until the operator supplies all exact native
fields for the chosen built-in mode; it is not a typed task. `3.1.1` adds the null/empty guard.

Through `3.1.9`, route DATAX to a worker with `PYTHON_HOME` or `python2.7` and
`DATAX_HOME/bin/datax.py`; from `3.2.0`, configure `PYTHON_LAUNCHER` and
`DATAX_LAUNCHER`. Matching DataX plugins/drivers, source/target connectivity,
and data permissions are caller prerequisites. All versions replace CRLF,
substitute prepared values into JSON, and write UTF-8; the replacement is LF
through `3.1.9` and the worker OS separator from `3.2.0`. The DataX CLI `-p -D`
map is forwarded only from `3.1.0`, behind the typed placeholder/parameter ban.
Upstream does not safely quote shell metacharacters in prepared workflow,
startup, local, or `varPool` values, so use a parameter-free workflow for this
safe typed facet on positive releases from `3.1.1`. Compilation blocks visible
workflow globals on create, task/runtime edits and global edits; online activation
checks existing globals too. Metadata-only preservation is allowed. Empty local
globals do not prove that startup or worker-prepared parameters will be absent.

Upstream logs complete task params, the final command, and child output at
INFO, and logs job/command construction at DEBUG. Do not embed credentials:
task fields are not secret storage and dsctl does not redact them. DATAX has no
structured output, durable application id, or failover resume. Cancellation is
worker-local—wrapper kill through `3.1.9`, direct-process destroy through
`3.2.2`, and process-tree plus generic application cancel thereafter. Retry
reruns the entire transfer and can duplicate writes. This review adds no live
evidence or profile promotion.

The `DATASYNC` default template and `raw-json` scenario map to
`DATASYNC/create_and_execute` in `Other` on exact profiles from `3.2.0` through
`3.4.3`; the plugin is upstream-absent on profiles through `3.1.9`. The
normal default wire is:

```yaml
name: nightly-transfer
type: DATASYNC
task_params:
  jsonFormat: false
  name: nightly-transfer
  sourceLocationArn: arn:aws:datasync:cn-north-1:123456789012:location/source
  destinationLocationArn: arn:aws:datasync:cn-north-1:123456789012:location/destination
  cloudWatchLogGroupArn: arn:aws:logs:cn-north-1:123456789012:log-group:datasync
```

Normal authoring requires `name`, `sourceLocationArn`, and
`destinationLocationArn`, allows optional `cloudWatchLogGroupArn`, and always
emits strict `jsonFormat=false`. No additional normal fields are typed. The
upstream UI uses one model value for the outer and inner names, making them
equal on create and collapsing them again on edit, but the REST wire and CLI
can represent distinct values.

The `raw-json` variant is not a generic fallback. Its only wrapper shape is:

```yaml
name: raw-datasync
type: DATASYNC
task_params:
  jsonFormat: true
  json: |
    {
      "Name": "raw-transfer",
      "SourceLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/source",
      "DestinationLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/destination"
    }
```

Explicit `jsonFormat=true` selects selector-restricted opaque create/edit only
when `json` is one valid JSON-object string and there are no siblings. The
string is retained unchanged, but worker deserialization accepts only
UpperCamelCase known `DatasyncParameters` and ignores unknown fields. Its
`READ_UNKNOWN_ENUM_VALUES_AS_NULL` setting turns unknown enum values into `null`
only in inherited `LocalParams`/`VarPool` `Property.Direct/Type`. Enum-like
strings such as `FilterType` are not enum-validated or null-converted and reach
the SDK unchanged for AWS validation. It does not pass arbitrary JSON to AWS.
The whole `Options` copy is ineffective, including its string fields;
`Includes` is misrouted to and overwrites `Excludes`, and `Schedule` creates a
persistent recurring Task. `localParams`, placeholders, resources, and result
output are runtime-unsupported.

Exact `3.2.x` workers use static `resource.aws.access.key.id`,
`resource.aws.secret.access.key`, and `resource.aws.region`; exact `3.3.1` and
newer use static `aws.datasync.access.key.id`,
`aws.datasync.access.key.secret`, and `aws.datasync.region`, despite the later
upstream guide's stale keys. Upstream INFO-logs original and converted task
parameters; task fields are not secret storage and dsctl does not redact them.
Each fresh attempt calls `CreateTask` then `StartTaskExecution`, never deletes
the persistent Task, and never closes the client. Only `taskExecutionArn` is
callback-persisted in `appIds`; failover and cancel reuse it after persistence,
while the pre-callback/retry window can duplicate executions and leak Tasks.
Polling has no internal deadline or structured output. This review adds no live
evidence or promotion.

The `SAGEMAKER` default template and its short IN field hint map only to
`SAGEMAKER/start_pipeline_execution` in `MachineLearning` on exact profiles
from `3.1.0` through `3.4.3`, except the `3.1.1`–`3.1.2` polling holes.
The plugin is absent through `3.0.6`. The parameterized canonical shape is:

```yaml
name: start-parameterized-sagemaker-pipeline
type: SAGEMAKER
task_params:
  sagemakerRequestJson: |-
    {
      "PipelineName": "nightly-training",
      "PipelineParameters": [
        {"Name": "training_job_name", "Value": "${training_job_name}"}
      ]
    }
  datasource: 1
  localParams:
    - prop: training_job_name
      direct: IN
      type: VARCHAR
      value: nightly-training
```

The request example omits `ClientRequestToken`; add one stable token for each
intended logical submission when idempotency is needed. Reuse that token for
retries of that submission and choose a new token for a new intended run.

Omit `datasource` on exact `3.1.0`, `3.1.3`–`3.1.9`, and `3.2.0`; it is required from
`3.2.1`. The authoring field and JSON Schema agree on that positive required
datasource id; generic optional connection guidance does not override it.
Compilation always emits `localParams` and `resourceList=[]`, and adds
native `type=SAGEMAKER` only with the datasource epoch. The default template
uses one literal JSON object and an empty parameter list. Canonical authoring
permits pre-substitution non-JSON only when DS placeholder syntax is present;
substitution is whole-text and does not JSON-escape values. Only `IN` values
enter the prepared map through `3.4.0`; from `3.4.1`, `OUT` values can also be
substituted, but this task still publishes no output. A stable explicit
`ClientRequestToken` is recommended when AWS idempotency is required because
the SDK-generated token may be transient while the plugin's persisted/read
token remains null.

Exact `3.1.0` writes a null application id before start and never backfills the
returned ARN. From typed `3.1.3`, callback-persisted `appIds` supports later polling
and cancellation, but the pre-callback/retry window can still submit another
execution. Credential and logging epochs, the ignored datasource credentials
from `3.3.1`, absent output/deadline, and unclosed client are specified under
[Version-selected task authoring](#version-selected-task-authoring). This
review adds no live evidence or promotion.

The `DMS` default template maps only to
`DMS/resume_existing_full_load` in `Cloud` on exact profiles from `3.2.0`
through `3.4.3`; the plugin is upstream-absent on profiles through `3.1.9`.
The five canonical fields are also the complete native wire:

```yaml
name: resume-existing-full-load
type: DMS
task_params:
  isRestartTask: true
  isJsonFormat: false
  migrationType: full-load
  startReplicationTaskType: resume-processing
  replicationTaskArn: arn:aws:dms:us-east-1:123456789012:task:REPLACE-ME
```

Every field is required. The first four are strict constants rather than
hidden defaults. `replicationTaskArn` is a literal AWS DMS replication-task ARN
without edge whitespace, controls, surrogates, or DS placeholders. Typed
create/edit rejects local parameters, resource files, start/create/JSON/CDC
configuration, destructive `reload-target`, and every additional native or
future field. DMS exposes no public opaque create/edit selector; excluded or
unrecognized existing state remains lossless only through unchanged/export
opaque-preserve provenance. Invalid canonical input never downgrades to opaque
authoring.

The user must attest that the ARN is an actual stopped, previously executed
full-load task. DolphinScheduler's restart validation checks only the ARN and
cannot verify the remote migration type or state. AWS DMS
`resume-processing` may reload partly loaded or not-yet-loaded tables. If the
ARN instead identifies a CDC task without `cdcStopPosition`, upstream can
report early DS success after start while CDC continues remotely. The excluded
`reload-target` mode can truncate or drop/reload target tables and must remain
an explicitly preserved native risk, not typed intent.

Workers through `3.2.2` use static `resource.aws.access.key.id`,
`resource.aws.secret.access.key`, and `resource.aws.region`. From `3.3.1`,
`aws.dms.credentials.provider.type` selects static
`aws.dms.access.key.id`/`aws.dms.access.key.secret` credentials or an instance
profile; `aws.dms.region` is required and `aws.dms.endpoint` is optional.
Credentials remain worker configuration. Upstream logs complete task
parameters and remote identifiers at INFO without logging credential values,
polls at `1000` ms with no explicit internal deadline, and returns no DS task
output.

After AWS accepts the request, a callback stores `replicationTaskArn` in
task-instance `appIds`. Once durable, `AbstractRemoteTask` resumes tracking
after failover and cancellation stops the same remote task. A crash before the
callback persists `appIds`, or a retry with no durable value, can submit
`resume-processing` again. Keep retries at zero unless this duplicate-submit
window is acceptable. This source review adds no live evidence, changes no
`tested` flag, and promotes no profile.

The `OPENMLDB` default template maps only to
`OPENMLDB/literal_single_statement` in the `MachineLearning` category on exact
profiles from `3.1.0` through `3.4.3`, except `3.1.2`, which fails in inherited
Python output handling after SQL execution. Profiles through `3.0.6` do not have a
registered OPENMLDB plugin; orphaned task UI files on `3.0.x` do not make it
available. Typed YAML contains the four required identity-projected fields:

```yaml
name: openmldb-single-statement
type: OPENMLDB
task_params:
  zk: zk-1.example.com:2181,zk-2.example.com:2181
  zkPath: /openmldb/production
  executeMode: online
  sql: SELECT 1 AS answer
```

`executeMode` is exactly lowercase `offline` or `online`. `zk` accepts a
comma-separated DNS/IPv4 `host:port` ensemble with each port in the range 1
through 65535. `zkPath` accepts an absolute conservative ZooKeeper znode path,
including `/`. `sql` is one nonblank literal statement inserted into
upstream-generated Python; semicolons, double quotes, backslashes, carriage
returns, unsafe control text, `${...}`, and `$[...]` are rejected. Accepted
values and the four field names retain their canonical and wire spelling.

Typed OPENMLDB has no `localParams`, `varPool`, `resourceList`, inherited
`rawScript`, runtime, or future field. It exposes no public opaque create/edit
selector. Existing excluded or unrecognized native payloads remain lossless
only on export and unchanged edits whose projection provenance stays
opaque-preserve; invalid typed input never downgrades to opaque authoring.

Exact `3.1.x` workers use `PYTHON_HOME`; exact `3.2.0` and newer use
`PYTHON_LAUNCHER`. Route tasks only to workers with Python 3, `openmldb`,
SQLAlchemy and the OpenMLDB driver, ZooKeeper reachability, and target-data
permissions. Offline mode enables synchronous jobs with a fixed `1800000` ms
job timeout. Upstream logs the complete task parameters, raw SQL, rendered
Python, and final generated Python file at INFO, so no OPENMLDB field may
contain secrets. SQLAlchemy execution results are discarded and are not DS
output parameters. The synchronous local process has no durable application
id or failover resume; a retry executes the statement again and can repeat
side effects. This source review adds no live evidence, changes no `tested`
flag, and promotes no profile.

The `ZEPPELIN` default template and its optional field hints map only to `ZEPPELIN/paragraph` on exact profiles
from `3.0.0` through `3.4.3`; profiles through `2.0.9` do not contain the
plugin. Typed authoring requires URL-segment-safe `noteId` and `paragraphId`.
The selected exact version fixes `connectionMode`: `WORKER_CONFIG` on `3.0.x`,
`REST_ENDPOINT` from `3.1.0` through `3.2.0`, and `DATASOURCE` from `3.2.1`.
The first mode relies on worker `zeppelin.rest.url`, the second requires an
absolute anonymous HTTP(S) `restEndpoint` without whitespace, credentials,
query, fragment, or placeholders, and the third requires a positive ZEPPELIN
datasource id. The projector removes `connectionMode`; datasource wire output
also injects `type: ZEPPELIN`.

The short literal-map hint starts at `3.1.0`. Canonical YAML always exposes
`parameters` as a mapping: on `3.0.x` it defaults to `{}`, has
`maxProperties: 0`, and has no native compile path; from `3.1.0`, exact
projection sorts and serializes literal string keys and values to the native
JSON string. An empty map is omitted. Control text and DS `${...}` or `$[...]`
placeholders are rejected so substitution introduced in the later server epoch
cannot change the portable typed meaning. The CLI does not detect or redact
arbitrary secret-like values; do not use this map as secret storage.

Typed REST execution is anonymous. For `3.2.1` and newer, selecting a
passwordless/anonymous datasource is a caller-owned runtime prerequisite:
the CLI validates the positive id but neither reads nor certifies the stored
authentication mode. Credentialed datasource execution is therefore an
explicit opaque/risk-managed path. This matters because `3.2.0` logs raw task
parameters containing its native inline username/password, `3.2.1` logs the
resolved datasource username after login, and `3.2.2` and newer log the
resolved parameter object including datasource credentials. Where upstream owns them,
whole-note execution, cloning, authentication, inherited parameters, output,
and future fields are also opaque-only.

Exact `3.4.1` creates local result evidence but does not reliably publish it
to downstream tasks; `3.4.2` and `3.4.3` publish runtime `taskName.result` through the
var pool. Neither output is authored YAML. The executor performs one
synchronous remote call and has no resumable application id or failover
protocol, so a retry can execute a side-effecting paragraph more than once.
This reviewed facet adds no live evidence and does not alter profile promotion.

The `EMR` variants keep `task_params.jobFlowDefineJson` and
`task_params.stepsDefineJson` as raw JSON strings; the CLI validates literal
JSON but does not parse it into a canonical AWS request object or reformat the
text. Canonical `task_params.programType` is `RUN_JOB_FLOW` or
`ADD_JOB_FLOW_STEPS`. Exact `3.0.x` accepts only `RUN_JOB_FLOW` and compiles it
to the upstream implied-mode shape without `programType`; releases from
`3.1.0` send the native discriminator and support both modes. A statically
parseable ADD request must contain exactly one `Steps` entry. DS substitutes
`${...}` and `$[...]` inside EMR request text only from `3.2.2`, which is why
the short request-binding hint starts there; earlier exact profiles send the raw text
unchanged and reject placeholders bound by task-local parameters. Authored EMR
`localParams` are unique `IN`/`VARCHAR` properties, and typed EMR does not
expose `varPool`. Opaque export/edit round-trips additional native and future
fields losslessly. The templates intentionally contain no AWS credentials:
configure the exact server-side worker or AWS-authentication prerequisite under
[Version-selected task authoring](#version-selected-task-authoring). The CLI
does not manage those credentials, and upstream EMR task failover remains
unimplemented rather than becoming a CLI high-availability claim.

The `EMR_SERVERLESS` default template and its short IN hint use `EMR_SERVERLESS/start_job_run` on exact
`3.4.2` and `3.4.3`. They keep `startJobRunRequestJson` as unformatted raw JSON and allow
unique `IN`/`VARCHAR` local parameters; `${...}` and `$[...]` substitution is
request-only. Unresolved runtime placeholders must stay in quoted JSON strings;
bound values are checked after exact raw substitution. `applicationId`,
`executionRoleArn`, and optional `jobName` are
literal top-level values that override matching JSON members; `ClientToken` is
runtime-owned. Workers require `aws.emr.*` or a usable AWS SDK default chain,
a region, application access, and service/job-data permissions. Custom
endpoints use `emr.serverless.endpoint` or `EMR_SERVERLESS_ENDPOINT`; this
executor does not read `aws.emr.endpoint`. Upstream stores `jobRunId` in
task-instance `appIds` for failover. Runtime and future fields remain
opaque-preserve-only, and the review carries no live-evidence or promotion
claim.

For `SHELL` and `PYTHON`, the `resource` variant uses the DS-native
`task_params.resourceList[].resourceName` field with a canonical path relative
to the selected FILE root. The CLI resolves and verifies each FILE before
mutation. Exact `2.0.0` through `3.1.9` encode the positive resource id;
`3.2.0` and newer encode the verified storage absolute `resourceName`.
The `command` shorthand remains
the minimal inline script path and compiles to `taskParams.rawScript` with an
empty `resourceList`.

Exact `1.3.9` is the exception: typed `PYTHON` and `SHELL` require
`resourceList: []` and do not publish the `resource` variant. Upstream
process-definition authorization checks only positive resource IDs, while the
legacy `id=0`/`res` full-name branch resolves a tenant without that user
permission context before the worker downloads the resource. The CLI therefore
never manufactures that old-name wire. Any existing
nonempty native resource payload remains unchanged/export opaque state; use a
newer exact profile for typed resource attachment.

Input field hints and coupled `output` scenarios expose the reviewed DS-native
task dynamic parameter fields: `task_params.localParams[]` and
`task_params.varPool[]`. `PROCEDURE` authors `localParams`; its `varPool` is
runtime state and must stay empty when the field exists, while exact `1.3.9`
omits that field entirely. Parameter entries use the DS `Property` shape:
`prop`, `direct`, `type`, and optional `value`. `direct` is `IN` or `OUT`. On
exact `3.4.1`, the general generated parameter types are `VARCHAR`,
`INTEGER`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`, `BOOLEAN`,
`LIST`, and `FILE`; exact `1.3.9` omits `LIST` and `FILE`, releases from `2.0.0`
through `3.1.9` omit `FILE`, and releases from `3.2.0` expose the stable set.
Use `dsctl enum list data-type` for the selected profile. Script-like task
output must follow the selected parser epoch documented by
`dsctl template params --topic output`.

Exact `1.3.9` `PYTHON` and `SHELL` typed parameters are unique `IN` entries
using only the nine scalar types listed above. They have no `varPool`, `LIST`,
`FILE`, or structured output; richer native parameter state remains opaque on
unchanged/export paths.

`PROCEDURE` narrows that general enum to the JDBC scalar types `VARCHAR`,
`INTEGER`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`, and
`BOOLEAN`. Its canonical `method` is a JDBC call with positional `?`
placeholders, for example `{call reporting.refresh_daily(?,?)}`, and the
placeholder count must equal `localParams` length. Compilation maps that one
surface to the three exact native syntax epochs documented under
[Version-selected task authoring](#version-selected-task-authoring).

The exact `1.3.9` `SUB_WORKFLOW` default identifies the child only with
`childWorkflowName`; they do not emit `localParams`, resources, `varPool`, or a
workflow code. On later profiles, the `SUB_WORKFLOW` default documents
parent-to-child parameter inheritance without adding unused `localParams`. In
`3.3.1` and `3.3.2`, child inputs come from parent startup parameters; from
`3.4.0`, they come from parent workflow globals, startup parameters, and
varPool. The `SUB_WORKFLOW` task's own `localParams` are not child inputs in
either later epoch.

SQL templates and the SQL typed payload normalizer keep `localParams`,
`varPool`, `preStatements`, and `postStatements` as non-null lists when omitted
or empty, because DS SQL task execution expects list values for those fields.

For end-to-end YAML authoring guidance, see `docs/user/workflow-authoring.md`.

## `dsctl schedule preview`

Previews the next five trigger times for one existing or proposed schedule.

Supported forms:

- `dsctl schedule preview SCHEDULE_ID [--project PROJECT]`
- DS `2.0.0` and later: `dsctl schedule preview --project PROJECT --cron CRON --start START --end END --timezone TIMEZONE`
- DS `1.3.9`: `dsctl schedule preview --project PROJECT --cron CRON --start START --end END`

Rules:

- use `dsctl schedule list` inside the selected project to discover schedule ids
- preview by id accepts optional `--project`; it rejects `--cron`, `--start`,
  `--end` and `--timezone`
- ad hoc preview requires `--cron`, `--start`, and `--end`; `--timezone` is
  required from DS `2.0.0`, while selected DS `1.3.9` marks it unavailable
- `--cron` must be a DolphinScheduler Quartz cron expression with 6 or 7
  fields and seconds first
- ad hoc preview resolves project selection with `flag > context`
- use `dsctl project list` to discover project names and native identifiers for ad hoc
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

On the exact `3.4.3` profile, create, update and explain accept
`--missed-fire-policy`. Discover native values with
`dsctl enum list schedule-missed-fire-policy`: `SKIP_MISSED`, `FIRE_ONCE_NOW`,
or `FIRE_ALL_MISSED`. Create omission uses the native `FIRE_ALL_MISSED` default;
update omission preserves the stored value, including historical null state.
Explicit null or an unknown enum is rejected. Older profiles mark the option
unavailable. YAML uses `schedule.missed_fire_policy`; export and definition-edit
validation retain it as part of the read-only schedule snapshot. The native
field is `missedFirePolicy` inside the schedule JSON, not a top-level form field.

Supported forms:

- `dsctl schedule explain --workflow WORKFLOW [--project PROJECT] --cron CRON --start START --end END [--timezone TIMEZONE]`
- `dsctl schedule explain SCHEDULE_ID [--project PROJECT] [update options...]`

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
- an absent/null project preference means no defaults are supplied; malformed
  preference payloads still fail validation
- create-form omitted `warningType`, `warningGroupId`,
  `workflowInstancePriority`, `workerGroup`, and `environmentCode` may be
  supplied by enabled project preference before CLI built-in defaults
- create-form `--environment-code 0` explicitly selects no environment and
  bypasses an enabled project environment preference; update-form zero clears
  the current environment, while omission preserves it
- on DS `2.0.0`–`3.2.1`, positive environment selections are rejected with
  `unsupported_feature`, including effective create preferences. The native
  scheduler/task factory does not pass that setting to default tasks. Use an
  explicit task `environment_code` (which also applies to manual runs). An
  unrelated update to an existing positive value remains explainable with a
  structured runtime warning. Selected-version schema and schedule capabilities
  expose this distinction; DS `3.2.2` onward supports inheritance.
- with `SCHEDULE_ID`, explain models `schedule.update` and merges omitted
  fields from the current remote schedule before preview and risk analysis
- update-form explain also returns `data.currentSchedule`,
  `data.requestedFields`, `data.changedFields`, `data.inheritedFields`, and
  `data.unchangedRequestedFields`
- explain by id accepts `--project` to constrain schedule resolution; it does not
  accept `--workflow` or `--tenant-code`
- explain by id requires at least one field option
- any provided `--cron` must be a DolphinScheduler Quartz cron expression with
  6 or 7 fields and seconds first
- explain never mutates remote state
- `data.confirmation.token` and `data.confirmation.confirmFlag` are `null`
  when confirmation is not required
- when confirmation is required, the returned token matches the later
  `schedule.create` or `schedule.update` mutation with the same effective input
- confirmation binds the action, resolved target, effective schedule fields and
  high-frequency risk policy. Refreshing preview times or their sampled interval
  does not invalidate it. Changed inputs or risk policy require fresh confirmation.
  Workflow creation with an embedded schedule uses the same rule.

Successful output returns:

- `data.mutationAction`
- `data.proposedSchedule.crontab`
- `data.proposedSchedule.startTime`
- `data.proposedSchedule.endTime`
- `data.proposedSchedule.timezoneId`
- `data.proposedSchedule.missedFirePolicy` when supported by the exact profile
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
- workflow selection: required `--workflow WORKFLOW`
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

- `--workflow WORKFLOW`
- `--cron`
- `--start`
- `--end`

Version-selected option:

- `--timezone` is required from DS `2.0.0`; DS `1.3.9` marks it unavailable

Optional options:

- `--failure-strategy`
- `--warning-type`
- `--warning-group-id`
- `--priority`
- `--worker-group`
- `--tenant-code`
- `--environment-code`
- `--missed-fire-policy` on exact `3.4.3`
- `--confirm-risk TOKEN`

Rules:

- create returns the created schedule payload
- omitted `warningType`, `warningGroupId`, `workflowInstancePriority`,
  `workerGroup`, `tenantCode`, and `environmentCode` may be supplied by
  enabled project preference before CLI built-in defaults
- when no environment code is selected after preference resolution, the
  schedule is created without an environment; the selected schedule domain chooses
  the compatible generated endpoint rather than sending a synthetic code `0`
- explicit `--environment-code 0` selects that no-environment state and
  bypasses an enabled project environment preference; positive values select a
  concrete DS environment
- positive selections require schedule environment inheritance in the selected
  profile; on DS `2.0.0`–`3.2.1`, explicit and preference-derived positive
  values fail before mutation. See `schedule explain` above for the task-level
  alternative. Embedded workflow schedule creation applies the same check
  before creating the workflow.
- `--cron` must be a DolphinScheduler Quartz cron expression with 6 or 7
  fields and seconds first
- the CLI does not expose `releaseState` as a create option; schedule
  activation uses explicit `online` / `offline`
- if the workflow is not online, the CLI returns `invalid_state`
- if the workflow already has a schedule, the CLI returns `conflict`
- if the server rejects a past start time, the CLI returns `user_input_error`
  with a suggestion to choose a future `--start` and a later `--end`; the same
  translation applies to schedule update and activation
- DS `1.3.9` has no schedule timezone field. Pass `--start` and `--end` with
  explicit UTC offsets when the server accepts that ISO form. Mutation readback
  can then compare offset-bearing values by their exact instant even when the
  server renders a different offset. A naive or invalid value is never assigned
  an inferred server timezone; if it differs on readback, the CLI requires
  inspection before retry and recommends explicit offsets for a later mutation
- if the computed preview interval is below the confirmation threshold, the
  CLI returns `confirmation_required` unless the matching `--confirm-risk`
  token is provided
- when a high-frequency schedule is explicitly confirmed, the success payload
  includes one warning
- when that warning is present, the `warnings[]` item uses code
  `confirmed_high_frequency_schedule`
- `resolved.tenant` includes:
  - `value`
  - `source`
- when enabled project preference supplied any omitted fields,
  `resolved.project_preference.used_fields` lists those destination field names

## `dsctl schedule update SCHEDULE_ID`

Updates one schedule by numeric id.

Rules:

- `SCHEDULE_ID` is id-first; optional `--project` takes precedence over the
  named-context project. A selected project bounds lookup with no global fallback
- use `dsctl schedule list` inside the selected project to discover schedule ids
- use `dsctl alert-group list`, `dsctl worker-group list`, and
  `dsctl environment list` to discover optional update selector values
- `--failure-strategy`, `--warning-type`, and `--priority` use generated DS
  enum values exposed by `dsctl enum list`
- omitted fields preserve current remote values
- exact `3.4.3` also accepts `--missed-fire-policy`; omission preserves the
  current strategy. See [Schedule explain](#dsctl-schedule-explain) for native
  choices, null rejection and read-only YAML snapshot semantics
- changing only execution or warning settings preserves the stored calendar,
  even when its original start time has passed. Explicit changes to cron,
  start/end, timezone or missed-fire policy send the merged calendar and remain
  subject to the selected DS version's native time validation
- `--environment-code 0` clears the current environment; omitting the option
  preserves the current environment instead
- on DS `2.0.0`–`3.2.1`, a positive selection fails before mutation; an unrelated
  update preserves a stored positive value with a runtime warning. Read and
  lifecycle commands also disclose this limitation without blocking cleanup.
- an existing schedule without an environment remains environment-free when
  other fields are updated; the selected schedule domain uses the compatible
  project-scoped update contract without exposing that version detail
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
  `warnings[]` item uses code `confirmed_high_frequency_schedule`

## `dsctl schedule delete SCHEDULE_ID --force`

Deletes one schedule by numeric id.

Rules:

- `SCHEDULE_ID` is id-first; optional `--project` takes precedence over the
  named-context project. A selected project bounds lookup with no global fallback
- use `dsctl schedule list` inside the selected project to discover schedule ids
- `--force` is required
- deleting an online schedule returns `invalid_state`

Successful output returns:

- `data.deleted`
- `data.schedule`

## `dsctl schedule online SCHEDULE_ID`

Brings one schedule online and returns the refreshed schedule payload.

Rules:

- `SCHEDULE_ID` is id-first; optional `--project` takes precedence over the
  named-context project. A selected project bounds lookup with no global fallback
- use `dsctl schedule list` inside the selected project to discover schedule ids
- the selected exact schedule recipe resolves the bound workflow when its
  online route requires `projectCode`
- bringing a schedule online requires the bound workflow to already be online

## `dsctl schedule offline SCHEDULE_ID`

Brings one schedule offline and returns the refreshed schedule payload.

Rules:

- `SCHEDULE_ID` is id-first; optional `--project` takes precedence over the
  named-context project. A selected project bounds lookup with no global fallback
- use `dsctl schedule list` inside the selected project to discover schedule ids
- the selected exact schedule recipe resolves the bound workflow when its
  offline route requires `projectCode`

### Task-definition version boundaries

The stable task selector accepts a task name and, where the selected upstream
model has one, a numeric task code. Exact profiles retain native identity rather
than fabricating code/version values or silently corrupting workflow relations:

- `1.3.9` identifies task nodes with strings. Task `list` and `get` project the
  native id and exact task name; task `update` replaces the containing legacy
  definition graph while preserving unknown fields.
- `2.0.0` through `2.0.2` update tasks through the complete owning workflow,
  preserving task and relation-bound versions; dependency edits use that same
  whole-workflow path.
- `2.0.3` also uses whole-workflow task updates for ordinary fields, but rejects
  explicit `depends_on` changes. Its definition service can silently ignore a
  relation rewire when the number of relation rows is unchanged. The CLI
  conservatively rejects workflow-definition relation changes on this exact
  profile; ordinary fields and task renames that preserve native identity
  remain available. Workflow creation and instance editing use different
  upstream save paths and retain their own contracts.
- `2.0.4` through `3.2.0` use the standalone task endpoint for ordinary fields
  and reject explicit `depends_on` changes before I/O. Before preparing and
  again before applying the mutation, the CLI inventories all project
  workflows and requires the task to belong only to the selected workflow.
  Use whole-workflow edit for dependency changes on these profiles.
- `3.2.1` through `3.4.2` use the standalone dependency-update route.
  `3.4.1` and `3.4.2` use the project-scoped main route rather than the removed
  V2 route.
- `3.4.3` removes standalone task update. The CLI uses the reviewed atomic
  whole-workflow update for ordinary fields and dependency changes, including
  removing the final incoming edge. It requires the workflow to be offline,
  retains native task/relation identities and preserves unselected tasks,
  workflow settings and relation metadata. This strategy is selected from the
  exact generated task policy, not from a generic newer-version fallback.

## `dsctl task list`

Lists tasks inside one resolved workflow.

Selection rules:

- project selection: `flag > context`
- workflow selection: required `--workflow WORKFLOW`
- use `dsctl project list` and `dsctl workflow list` to discover selectors

The `data` payload is a JSON array of task summaries. Code-native profiles
return:

- `code`
- `name`
- `version`

Exact `1.3.9` returns native `id` and `name` instead, without invented `code` or
`version` values. Every task-definition recipe resolves the selected workflow
and projects this list from its DAG. `task list` does not imply a separate
global task-list endpoint or permission scope.

## `dsctl task get`

Fetches one task definition inside one resolved workflow.

Selection rules:

- project selection: `flag > context`
- workflow selection: required `--workflow WORKFLOW`
- task is resolved by exact name, or by numeric code when the selected profile
  has code-native task identity
- use `dsctl task list` inside the selected workflow to discover the selected
  profile's native task names and identities
- the selected exact task domain resolves project/workflow/task identity and
  uses its reviewed scoped detail route; it never scans all visible projects

## `dsctl task update`

Updates one existing task definition inside one workflow using inline `--set`
mutations. This is the lightweight path for small single-task definition edits,
not a workflow patch replacement.

Options:

- positional `TASK` accepts an exact task name or, on code-native profiles, a
  numeric code
- `--project PROJECT`
- `--workflow WORKFLOW` (required)
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
- inspect the current task with
  `dsctl task get extract --project etl-prod --workflow daily-etl` before
  editing when the current value matters
- use `dsctl schema --command task.update` to discover supported `--set` keys,
  examples, and machine-readable metadata for the selected profile; unavailable
  keys are omitted rather than accepted and silently dropped
- use `workflow edit --patch|--file` for structural definition changes such as
  create, delete, rename, task type changes, or multi-task DAG edits
- use `workflow-instance edit --patch|--file` for finished instance repair
- the CLI compiles the update into the selected exact native form; see
  [Task-definition version boundaries](#task-definition-version-boundaries).
  The legacy definition and reviewed whole-workflow strategies remain separate
  from standalone `updateTaskWithUpstream` routes
- exact `3.4.1` and `3.4.2` both use the project-scoped main
  `updateTaskWithUpstream` operation; the `3.4.1` path does not retain the V2
  operation removed upstream in `3.4.2`
- `command` updates are supported for `SHELL`, `PYTHON`, and `REMOTESHELL`;
  existing `REMOTESHELL` connection type and datasource fields are preserved
- `flag` accepts `YES` or `NO`
- `depends_on` accepts either a YAML list value or a comma-separated task-name
  string
- the standalone project-scoped main route through `3.4.2` cannot express
  removing the final upstream relation: when a task currently has dependencies,
  `--set 'depends_on=[]'` returns `unsupported_feature` before mutation and
  directs the caller to `dsctl workflow edit`. The `3.4.3` whole-workflow
  strategy can clear that dependency in the same coherent task/relation write
- `worker_group`, `environment_code`, `task_group_id`, `cpu_quota`, and
  `memory_max` accept an empty value to reset back to the DS default
- setting `task_group_priority` requires an effective `task_group_id`
- clearing `task_group_id` also clears its priority; changing the group without
  an explicit priority uses `0`, while selecting the same group retains its
  current priority. This applies to the `3.4.3` whole-workflow strategy too
- setting `timeout_notify_strategy` requires an effective timeout greater than
  `0`
- when `timeout` changes from closed to open and no strategy is provided, the
  CLI uses DS's default `WARN` notify strategy
- task rename and task-type changes remain `workflow edit` operations, not
  inline task updates
- task-specific complex `task_params` authoring remains under the workflow YAML
  and `task-type schema` path; `task update` intentionally exposes only the
  stable common inline keys above
- the update compiler reconstructs only its reviewed top-level DS form and
  preserves unknown members nested inside the existing `taskParams` object;
  if task detail contains an unreviewed top-level field, the command returns
  `unsupported_feature` before mutation instead of risking a lossy write
- `--dry-run` returns prepared stage order and:
  - `data.changes`: `{field,before,after}` entries for changed authoring fields;
    explicit null resets and effective defaults remain visible
  - `data.no_change`
- use `--columns requests` to inspect the prepared native request
- dry-run and apply use the same profile-bound `CompiledWireProgram` and
  preparation path. A later apply prepares from fresh state and executes that
  detached encoded request; it never reconstructs a request from rendered dry-run
  JSON
- initial prepare and the fresh read immediately before apply both require a
  coherent snapshot: task detail and the selected DAG must agree on workflow
  identity and task version, and every relation that references the task must
  carry that same task version
- for `3.1.9` and `3.2.0`, those two boundaries also exhaust the complete
  project workflow-reference inventory and describe every DAG. Wire/contract
  read failures and inconsistent inventory are `api_transport_error`;
  permission denial and a project that disappears during project re-resolution
  retain `permission_denied` and `not_found`; an initially valid task bound
  outside the selected workflow is `unsupported_feature`; a binding change
  before apply is `conflict`. Each result has `mutation_applied: false`
- before sending a non-no-op update, apply re-reads task detail and the DAG and
  returns `conflict` with `mutation_applied: false` if the task version,
  normalized upstream dependency codes, or any requested-field projection has
  changed since prepare
- whole-workflow task updates also fingerprint the complete native graph;
  a concurrent edit to an unselected task or relation blocks the write before
  mutation instead of being overwritten by the prepared graph
- this is an optimistic stale-plan guard, not an atomic compare-and-swap;
  DolphinScheduler's PUT route has no version precondition, so a concurrent
  write can still occur after the fresh read and before or during the PUT
- applying a no-op update returns the current task payload with one warning and
  sends no request
- when that warning is present, the `warnings[]` item uses code
  `task_update_no_persistent_change`
- if DS reports that the task state does not support modification, the CLI
  returns `invalid_state`
- the exact `3.4.1` generated response accepts an integer task code or `null`
  because its successful no-op server branch can omit data; exact `3.4.2`
  requires the declared integer. The command does not expose that difference:
  a non-no-op apply always returns the canonical project-scoped readback
- after a successful response, one immediate detail/DAG readback must show the
  requested field projection, exact expected upstream dependency codes, a task
  version greater than the prepared version, and consistent detail/DAG/relation
  versions; otherwise the error records `mutation_applied: true`
- readback currently has no eventual-consistency retry loop. A stale or failed
  immediate read therefore becomes an explicit reconciliation error instead of
  causing another mutation request
- generated response-decode and post-success readback failures record
  `details.mutation_applied: true`; a transport failure after dispatch records
  `details.mutation_may_have_applied: true`. Both direct the caller to inspect
  the scoped task before deciding what happened
- the mutation wire call disables automatic transport retry. Ambiguous failure
  handling is inspect/reconcile only; `dsctl` never resends the PUT
  automatically, and callers must not blindly repeat it

The archived `3.4.2` schema-6 installed-wheel receipt
`history/external-shell/3.4.2/2026-08-10-f328d3e2d6ba.json` proves dry-run non-mutation,
apply/readback, preservation, and exact external `SHELL` task restoration for
the then-current wheel SHA-256
`f328d3e2d6ba261c26f9f74decb12a9229b5d471b6f2efdd2a07d55b116af06f`
and its generated profile manifest. Its 48-operation trace contains 45
successful operations and three expected negative paths, while its digest-bound
bundle fixes the 15 `live_smoke` actions attested for that artifact. Unit and
contract tests prove
the optimistic stale-plan conflict path; the live receipt does not interleave
an external write between prepare and apply. Cleanup does refuse to overwrite
an observed concurrent fixture change, which is a different guarantee. The
generic read-only campaign cannot substitute for this mutating gate, and
neither receipt promotes unrelated typed-authoring facets or the whole
`3.4.2` profile. The later artifact-bound receipt
`live-evidence/external-shell/3.4.2/2026-08-12-e8eacee57af9.json` binds the same contract to
wheel SHA-256
`e8eacee57af9a9d2659194b05bccbbd7680eb10c310c4189d9cfa3ae8b5c9152`
and passed the checker for its recorded manifest. Expanded fingerprints now make it
historical; its `task.update` claim remains `live_smoke` for that artifact.

## `dsctl workflow run`

Triggers one workflow definition and returns an acceptance receipt with any
resolved workflow instance IDs.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit selector
- use `dsctl project list` and `dsctl workflow list` to discover selectors
- use `dsctl worker-group list`, `dsctl tenant list`,
  `dsctl alert-group list`, and `dsctl environment list` to discover optional
  runtime selectors
- worker group selection:
  `flag > enabled project preference.workerGroup > "default"`
- tenant selection:
  `flag > enabled project preference.tenant > "default"`
- when no enabled project preference provides a value, runtime fallbacks mirror
  the DS 3.4.1 UI start modal: `failureStrategy=CONTINUE`,
  `warningType=NONE`, `workflowInstancePriority=MEDIUM`, `dryRun=0`, and omitted
  `warningGroupId`, `environmentCode`, and `startParams`
- priority selection is
  `flag > enabled project preference.taskPriority > MEDIUM`
- warning-type selection is
  `flag > enabled project preference.warningType > NONE`
- project preference resolution also applies to `alertGroups`/`alertGroup`,
  `workerGroup`, `tenant`, and `environmentCode`
- `--dry-run` is a local CLI preview: it resolves inputs and emits the native
  prepared effects without sending the start request; `--columns requests`
  expands the native REST plan
- `--execution-dry-run` sends DS `dryRun=1`: DolphinScheduler creates dry-run
  workflow/task instances and skips task plugin trigger execution
- the exact execution recipe sends `tenantCode` from `3.2.0`; profiles from
  `1.3.9` through `3.1.9` omit that wire field even though the stable runtime
  selection model remains unchanged
- if DS cannot select an available master, the CLI returns `invalid_state`,
  preserves the upstream result source, and suggests checking
  `dsctl monitor server master`; the CLI does not automatically retry the
  trigger
- a recognized API-to-master RPC failure returns `api_transport_error` with
  the upstream result source, `mutation_may_have_applied: true` and
  `request_replay_safe: false`. A failed response does not prove that the
  master did not dispatch execution. Inspect the scoped instance list and
  verify API-to-master connectivity before deciding whether to retry. This
  applies to run-task and backfill too; parallel backfill also retains
  `partial_dispatch_possible: true`

The `data` payload includes `workflowInstanceIds` (real IDs only),
`accepted: true`, and `instanceResolution: resolved|pending|unavailable`.
On 3.2.0/3.2.1/3.2.2, the scalar reply is retained as `triggerCode` and queried
once for actual IDs. An empty result remains pending. A failed lookup preserves
the accepted receipt in `error.details.execution` and `mutation_applied: true`.
It never resubmits the start request. Older empty replies remain unavailable.
Use the returned identity-resolution navigation before watching an instance.
The same receipt applies to run-task and backfill. See
[Execution outcomes](../development/execution-outcomes.md) for exact evidence.

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
- `--priority highest|high|medium|low|lowest`; omitted values use the priority
  resolution order above
- `--warning-type none|success|failure|all`; omitted values use the warning-type
  resolution order above
- `--warning-group-id ID`
- `--environment-code CODE`
- `--param KEY=VALUE`, repeatable, serialized to DS `startParams`
- `--dry-run`
- `--execution-dry-run`

Positive workflow environment selections also require native task inheritance.
Through DS `3.2.1`, explicit or preference-derived positive codes return
`unsupported_feature` before dispatch, including `--dry-run`. Use an explicit
task `environment_code` for tasks needing the environment; it applies to both
scheduled and manual runs. Explicit `--environment-code 0` bypasses a project
environment preference. DS `3.2.2` onward supports workflow-to-task inheritance.

Exact startup-parameter rules apply equally to `workflow run`, `run-task`, and
`backfill`:

- `1.3.9` through `3.0.6` consume executor `scheduleTime` as a comma-separated
  date range, including for ordinary `START_PROCESS`; `3.1.0` and newer consume
  the JSON schedule object. The CLI selects this wire shape from the exact
  profile;
- `1.3.9` has no `startParams` wire field, so any non-empty `--param` returns
  `unsupported_feature` before transport;
- `2.0.0` accepts only keys already declared in `workflow.global_params`; the
  CLI rejects undeclared keys instead of letting upstream silently ignore them;
- `2.0.9` and newer accept arbitrary startup keys as VARCHAR values.

On exact `1.3.9`, `workflow run-task` and task-scoped `workflow backfill`
decode the containing native graph only to select the task name. They do not
hydrate `SUB_PROCESS` ids to child names or read any child/grandchild workflow;
unscoped backfill does not load the graph.

## `dsctl workflow run-task`

Starts one workflow definition from a selected task and returns the created
workflow instance ids. This uses DolphinScheduler's workflow trigger endpoint
with `startNodeList` set to the selected task code and `taskDependType` mapped
from `--scope`.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit selector
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
- the `warnings[]` item uses code
  `workflow_run_task_dependent_context`
- missing-master failures follow the same `invalid_state` and no-automatic-retry
  contract as `workflow run`

## `dsctl workflow backfill`

Backfills one workflow definition and returns the created workflow instance ids.
This uses DolphinScheduler's workflow trigger endpoint with
`execType=COMPLEMENT_DATA`.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit selector
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
  `scheduleTime` in the exact executor's comma-range or JSON shape
- or repeat `--date DATE` for explicit complement schedule dates, serialized
  as DS `complementScheduleDateList`
- `--date` is mutually exclusive with `--start` / `--end`
- repeated `--date` is unavailable through `3.0.6`, whose executor accepts only
  the comma-separated range; use `--start` and `--end` on those profiles

Options:

- `--task TASK`
- `--scope self|pre|post`, default `self`, applied when `--task` is set
- `--run-mode serial|parallel`, default `serial`
- `--expected-parallelism-number N`; from `2.0.0`, omission normalizes to `2`
- `--complement-dependent-mode off|all`, default `off`
- `--all-level-dependent`
- `--execution-order desc|asc`, default `desc`
- the runtime options from `dsctl workflow run` are also supported, including
  `--dry-run`, `--execution-dry-run`, and repeatable `--param KEY=VALUE`
- exact `1.3.9` has no `expectedParallelismNumber` field: omission sends
  nothing, selected-version schema marks the option unavailable, and an
  explicit value fails before I/O

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

Failure safety:

- serial backfill missing-master failures return `invalid_state` without an
  automatic retry
- a parallel backfill selects a master once per partition, so a later
  missing-master failure can occur after earlier partitions were dispatched;
  its error includes `partial_dispatch_possible=true` and tells callers to
  inspect workflow instances and compare `scheduleTime` values before deciding
  whether to retry

## `dsctl workflow online`

Brings one workflow definition online and returns the refreshed workflow
payload.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit selector
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- before the release REST mutation, the command reads the exact workflow DAG
- when that DAG explicitly contains any `KUBEFLOW` task, the command reuses the
  applicable typed or opaque KUBEFLOW runtime preflight; preflight failure is
  fail-closed and no release request is sent
- this activation claim depends on the exact DAG revealing KUBEFLOW; it does
  not claim detection from missing DAG data
- publish referenced sub-workflows before running their parent. Upstream
  publication-time enforcement varies by version; exact `3.3.1` checks the
  old child-code field and can miss offline children. Successful parent
  publication alone does not prove child readiness; see
  [Workflow boundaries](../user/version-compatibility.md#workflow-boundaries)
- if an attached schedule exists but remains offline, the command succeeds and
  adds one warning reminding the caller that `schedule online` is still
  required; the `warnings[]` item uses code
  `workflow_online_leaves_schedule_offline`
- the returned workflow payload includes the authoritative attached schedule;
  bringing a workflow online does not bring that schedule online
- if the mutation succeeds but workflow or schedule refresh fails, the
  structured error reports `mutation_applied=true` and tells callers to verify
  state before retrying

## `dsctl workflow offline`

Brings one workflow definition offline and returns the refreshed workflow
payload.

Selection rules:

- project selection: `flag > context`
- workflow selection: explicit selector
- use `dsctl project list` and `dsctl workflow list` to discover selectors

Rules:

- the offline entry point does not read the workflow DAG and does not run the
  KUBEFLOW activation preflight
- workflow offline is idempotent
- if the workflow currently has an online attached schedule, the command
  succeeds and adds one warning because DS also forces that schedule offline;
  the `warnings[]` item uses code
  `workflow_offline_also_offlines_schedule`
- when a schedule exists, the CLI refreshes it after the workflow mutation so
  the returned payload reflects DS's cascading `OFFLINE` state; if that refresh
  fails, the structured error reports `mutation_applied=true`

## `dsctl workflow-instance list`

Lists workflow instances using explicit runtime filters.

Options:

- `--page-no N`
- `--page-size N`
- `--all`
- `--project PROJECT`
- `--workflow WORKFLOW`
- `--search SEARCH`
- `--executor EXECUTOR`
- `--host HOST`
- `--start START`
- `--end END`
- `--state STATE`

Selection rules:

- every workflow-instance query is project-scoped; effective project selection
  is explicit `--project` first, then current project context
- if neither source supplies a project, the command fails as `user_input_error`
  before any DolphinScheduler request
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- discover `--project` values with `dsctl project list`
- discover `--workflow` values with `dsctl workflow list`
- discover `--state` values with
  `dsctl enum list workflow-execution-status`
- the CLI resolves one project and calls its project-scoped workflow-instance
  paging API; it does not scan visible projects or use a global V2 list API
- `--workflow` is resolved as a workflow definition name or code inside the
  selected project
- `--search` filters workflow-instance names through upstream `searchVal`
- `--executor` filters by exact executor user name
- `--host` filters by upstream host substring
- `--start` and `--end` filter workflow-instance `start_time` using DS datetime
  format `YYYY-MM-DD HH:MM:SS`; when present, `--end` must be greater than or
  equal to `--start`. Before 3.1.0 use both bounds: single-bound filters are
  rejected. `resolved.time_filter` exposes exact interval and current-state
  semantics; see [Operational investigation](../user/operational-investigation.md)
- `--state` accepts DS workflow execution status names such as
  `RUNNING_EXECUTION` and `SUCCESS`
- with `--all`, the CLI fetches remaining pages up to the standard safety limit
  and materializes one DS-style page payload
- `data.coverage` preserves the original totals, page observations, read range,
  changes and timestamps. Completing the initial range does not imply an atomic
  snapshot. A mid-read failure retains partial coverage in the error
- `--trigger-code CODE` on exact 3.2.0+ selects the native trigger lookup.
  It returns `totalList` identity rows, `triggerCode` and `instanceResolution`
  without invented pagination. It cannot combine with ordinary filters,
  non-default pagination or `--all`; it is outside candidate-only read admission

The `data` payload is a DS-style paging object whose `totalList` items expose
the current stable workflow-instance projection:

- `id`
- `workflowDefinitionCode` or `workflowDefinitionId`, selected by the exact recipe
- `workflowDefinitionVersion`
- `projectCode` or `projectId`, preserving the selected native project identity
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
- `queue` on entity-backed pages; absent from exact `3.4.3` summary pages
- `duration`
- `workflowInstancePriority`
- `workerGroup`
- `environmentCode`
- `timeout`
- `dryRun`
- `restartTime`

Exact `3.4.3` pages decode native `WorkflowInstanceSummaryVO` rows. They retain
real instance/definition identities and statuses but omit `queue`, which is
absent from that summary; the CLI does not fetch each detail to fill it. Its
trigger-code lookup uses the same native summary VO to resolve IDs. Heavy
entity fields such as `commandParam`, `globalParams` and `dagData` are not
required by those reads. `workflow-instance get` continues to decode the
native entity and can expose detail-only values.

The exact recipe fixes the definition-identity field even when its value is
null; IDs are never relabeled as codes. This projection applies to both JSON
formats. Compact output encodes ordinary list rows at `data.totalList` using
the same selected fields and values.

## `dsctl workflow-instance get`

Fetches one workflow instance by id.

Selection rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- the CLI calls the selected project's exact workflow-instance detail route;
  it does not page through instances or scan projects to locate the id
- default output returns the stable DS runtime projection in the standard JSON
  envelope; definition-identity fields follow the exact recipe and retain null
  values as described for `workflow-instance list`

## `dsctl workflow-instance export`

Exports one workflow instance DAG as an editable YAML document.

Selection rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`

Output:

- writes only the workflow-instance YAML document
- exports the instance DAG as the same workflow YAML authoring
  shape used by `workflow create` and `workflow edit --file`; use it as the
  starting point for `workflow-instance edit --file`
- requires `dagData` on the workflow-instance payload
- exact `1.3.9` obtains the native instance directly by id and projects its
  legacy process-instance graph into the same YAML shape

## `dsctl workflow-instance parent`

Returns the parent workflow instance for one sub-workflow instance.

Selection rules:

- select a project with `--project` or current project context
- discover a child instance through its SUB_WORKFLOW task using
  `dsctl task-instance sub-workflow TASK --project PROJECT --workflow-instance PARENT`
- the CLI fetches the sub-workflow instance directly inside the selected
  project before calling the project-scoped relation endpoint
- the workflow instance must itself be a DS sub-workflow instance
- `resolved` includes `subWorkflowInstance`

The `data` payload is a JSON object:

- `parentWorkflowInstance`

## `dsctl workflow-instance digest`

Returns one compact runtime digest for a workflow instance.

Selection rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
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
- each highlighted task includes `logAvailable`, derived from whether DS
  returned a non-empty log path; lifecycle navigation only suggests `log` when
  this value is true

## `dsctl workflow-instance edit`

Edits one finished workflow instance DAG from a YAML patch or full workflow YAML
file and then returns the refreshed workflow-instance payload.

Options:

- `--project PROJECT`
- exactly one of `--patch PATCH` or `--file FILE`
- `--sync-definition`
- `--dry-run`
- `--confirm-risk TOKEN`

Rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- the workflow instance must already be in one DS final state
- the CLI requires `dagData` from the workflow-instance payload and rebuilds a
  live workflow spec snapshot from that instance DAG before applying the edit
- on all modern profiles, the instance's current `globalParams` and `timeout`
  override the definition-backed DAG metadata for export and edit. Sequential
  scalar edits preserve the other runtime scalar; empty instance globals never
  fall back to definition globals. Malformed or unrepresentable instance globals
  fail at the projection boundary before authoring
- exact `1.3.9` compiles the prepared edit into its native
  `processInstanceJson` whole-graph update and preserves native string ids
- exact `1.3.9` instance edits that add or replace a canonical `SUB_WORKFLOW`
  resolve the literal `childWorkflowName` in the current containing
  definition's project and compile the frozen binding to
  `SUB_PROCESS.params.processDefinitionId`. The iterative post-compile audit
  then follows every final task `params.processDefinitionId`, including an
  unchanged preserved native task, and rejects missing definitions or cycles.
  Raw opaque authoring remains closed; opaque native packages may only remain
  unchanged
- start new patch files with `dsctl template workflow-instance-patch --raw`
- start full-file repairs with
  `dsctl workflow-instance export 901 --project etl-prod`
- patch grammar reuses the same stable task patch operations as `workflow edit`:
  `patch.tasks.create`, `patch.tasks.update`, `patch.tasks.rename`, and
  `patch.tasks.delete`
- full-file edit treats the YAML as the desired complete instance DAG:
  same-name tasks preserve DS task identity, YAML-only tasks are created, and
  live-only tasks are deleted after `--confirm-risk`
- full-file edit does not infer renames; use `--patch` with `tasks.rename[]`
  when the task name changes and the existing task identity must be preserved
- current stable `patch.workflow.set` support is intentionally narrower than
  `workflow edit`; only `global_params` and `timeout` are accepted for
  workflow-instance edits
- definition-only workflow fields such as `name`, `description`,
  `execution_type`, and `release_state` are rejected as `user_input`
- full-file edit follows the same field rule: only workflow-level
  `global_params` and `timeout` may change; `workflow.project`, when present,
  must match the instance project
- full-file edit rejects `schedule:` blocks; schedule lifecycle remains under
  `dsctl schedule`
- `--sync-definition` forwards DS `syncDefine=true` so the saved DAG is also
  synchronized back to the current workflow definition
- exact `3.1.0` uses current main-table task ids for a changed
  `--sync-definition` request and checks changed tasks in the saved instance
  DAG and main-table detail afterward. An older instance DAG may differ from
  current task detail; the CLI binds by project and task code without requiring
  those historical fields to match before the edit. Edits without
  `--sync-definition` do not read main-table task ids
- exact `2.0.0`, `2.0.1`, and `2.0.2` require `--sync-definition` for task
  or edge changes: native `syncDefine=false` ignores those inputs. Scalar-only
  instance edits remain supported without synchronization and may return a null
  native payload; the CLI reads the updated instance before reporting success
- on those three versions, synchronization requires an already ONLINE current
  definition. Native metadata synchronization may set the definition ONLINE.
  Use `workflow edit` for an OFFLINE definition; explicitly publish first only
  when publication is intended. Dry-run retains the prepared request and diff,
  reports the same blocker as a failed preview, and explains the native effect. The CLI never enables synchronization or publishes for you
- on the other supported versions, without `--sync-definition`, the CLI edits
  the finished instance DAG without requesting current-definition synchronization
- `resolved` includes `workflowInstance`, `project`, `workflow`, `input_mode`,
  one of `patch_file` or `file`, and `syncDefine`
- `--dry-run` returns `diff`, `no_change`, `syncDefine` and prepared stage order;
  add `--columns requests` to inspect the compiled DS form
- dry-run uses deterministic preview task codes; applied edits obtain
  persistent codes from DolphinScheduler only for newly created tasks and only
  after local compile validation and confirmation checks succeed
- applying a no-op edit returns the current workflow-instance payload, emits one
  warning, and the `warnings[]` item uses code
  `workflow_instance_edit_no_persistent_change`

## `dsctl workflow-instance stop`

Requests stop for one workflow instance and then returns the refreshed
workflow-instance payload.

Rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- the CLI checks the current DS workflow execution status before sending the
  stop request
- states that are not stoppable return `invalid_state`
- on exact releases `3.2.0` through `3.4.3`, where upstream code `50015` can
  follow either an applied stop path or an earlier failure, the CLI returns
  `mutation_outcome_unknown`. The error preserves the upstream result source,
  instance/project scope, `mutation_may_have_applied: true`, and
  `request_replay_safe: false`. Its `next_actions` contain scoped `get` and
  `watch` commands and retain the selected `--context` or `--env-file`; use
  those reads before deciding the next action, and do not blindly repeat stop
- if DS accepts the stop request but the refreshed state is not yet `STOP`,
  the command succeeds and adds a warning describing the current state; the
  `warnings[]` item uses code
  `workflow_instance_action_state_after_request` with `action="stop"` and
  `target_state="STOP"`

## `dsctl workflow-instance watch`

Polls one workflow instance until it reaches a final DS execution state.

Options:

- `--project PROJECT`
- `--interval-seconds N`
- `--timeout-seconds N`
- `--exit-status`: require a successful terminal execution result
- `--after-run-times N`: require a greater observed `runTimes` before accepting
  a final state. Carry the baseline returned by an authorized rerun/recovery;
  a fast new run may finish between polls. The marker identifies a later round,
  not a unique concurrent command. Without it, watch observes the current round

Rules:

- ordinary watch exits 0 for any observed terminal state. With `--exit-status`,
  only `SUCCESS` exits 0; other terminal states return
  `execution_failed`, retain the final instance in `data` and exit 1

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- default polling interval is `5` seconds
- default timeout is `600` seconds
- `--timeout-seconds 0` means wait indefinitely
- timeout returns a structured `timeout` error with the last observed state
- success returns the same workflow-instance projection as `workflow-instance get`

## `dsctl workflow-instance rerun`

Requests rerun for one finished workflow instance and then returns the refreshed
workflow-instance payload.

Rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- the workflow instance must already be in one DS final state
- before sending the request, `dsctl` inspects the available instance `dagData`;
  when that DAG contains any `KUBEFLOW` task, rerun fails closed by default and
  directs the caller to
  `dsctl workflow run WORKFLOW --project PROJECT` for a new workflow instance
- this guard is `dsctl`-local: it cannot constrain the DolphinScheduler UI or
  direct REST calls, and it makes no KUBEFLOW detection claim when `dagData` is
  unavailable
- if DS accepts the request but the refreshed state is still final, the command
  succeeds and adds a warning describing the current state; the aligned
  `warnings[]` item uses code
  `workflow_instance_action_state_after_request` with `action="rerun"` and
  `expect_non_final=true`
- if DS cannot select an available master, the command returns `invalid_state`
  and suggests checking `dsctl monitor server master` before retrying; the CLI
  does not automatically retry

## `dsctl workflow-instance recover-failed`

Requests recovery from failed tasks for one workflow instance and then returns
the refreshed workflow-instance payload.

Rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- the workflow instance must currently be in DS `FAILURE`
- before sending the request, `dsctl` inspects the available instance `dagData`;
  when that DAG contains any `KUBEFLOW` task, recovery fails closed by default
  and directs the caller to
  `dsctl workflow run WORKFLOW --project PROJECT` for a new workflow instance
- this guard is `dsctl`-local: it cannot constrain the DolphinScheduler UI or
  direct REST calls, and it makes no KUBEFLOW detection claim when `dagData` is
  unavailable
- if DS accepts the request but the refreshed state is still final, the command
  succeeds and adds a warning describing the current state; the aligned
  `warnings[]` item uses code
  `workflow_instance_action_state_after_request` with
  `action="recover-failed"` and `expect_non_final=true`
- missing-master failures follow the same `invalid_state` and no-automatic-retry
  contract as `workflow-instance rerun`

## `dsctl workflow-instance execute-task`

Requests execution of one task inside one finished workflow instance and then
returns the refreshed workflow-instance payload.

Supported on `3.2.0`–`3.2.2` and `3.4.2`–`3.4.3`. On `3.3.1`, `3.3.2`,
`3.4.0` and `3.4.1`, the API exists but the master lacks an `EXECUTE_TASK`
handler. The action is `limited`; CLI preflight returns `unsupported_feature`
before dispatch. `capabilities --action workflow-instance.execute-task` and
`schema --command workflow-instance.execute-task` report the same constraint.

Options:

- `--project PROJECT`
- `--task NAME_OR_CODE`
- `--scope self|pre|post`

Rules:

- select a project with `--project` or current project context
- discover workflow-instance ids with
  `dsctl workflow-instance list --project etl-prod`
- discover `--task` values with
  `dsctl task-instance list --project etl-prod --workflow-instance 901`
- the workflow instance must already be in one DS final state
- before sending the request, `dsctl` inspects the available instance `dagData`;
  when that DAG contains any `KUBEFLOW` task, task execution fails closed by
  default and directs the caller to
  `dsctl workflow run WORKFLOW --project PROJECT` for a new workflow instance
- this guard is `dsctl`-local: it cannot constrain the DolphinScheduler UI or
  direct REST calls, and it makes no KUBEFLOW detection claim when `dagData` is
  unavailable
- the CLI resolves `--task` against the owning workflow definition recovered
  from the workflow instance payload
- `--scope self|pre|post` maps to DS
  `TASK_ONLY|TASK_PRE|TASK_POST`
- `resolved` includes `workflowInstance`, `task`, and `scope`
- if DS accepts the request but the refreshed state is still final, the command
  succeeds and adds a warning describing the current state; the aligned
  `warnings[]` item uses code
  `workflow_instance_action_state_after_request` with
  `action="execute-task"` and `expect_non_final=true`

## `dsctl task-instance list`

Lists task instances through the project-scoped DS task-instance paging query.

Options:

- `--workflow-instance ID`
- `--project PROJECT`
- `--workflow-instance-name WORKFLOW_INSTANCE_NAME`
- `--page-no N`
- `--page-size N`
- `--all`
- `--search SEARCH`
- `--task TASK`
- `--task-code N`
- `--executor EXECUTOR`
- `--state STATE`
- `--host HOST`
- `--start START`
- `--end END`
- `--execute-type EXECUTE_TYPE`

Selection rules:

- task-instance queries are project-scoped; effective project selection is
  explicit `--project` first, then current project context
- if neither source supplies a project, the command fails as `user_input_error`
  before any DolphinScheduler request, including when `--workflow-instance` is
  present
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- discover `--workflow-instance` values with
  `dsctl workflow-instance list --project etl-prod`
- discover `--project` values with `dsctl project list`
- discover `--task-code` values with `dsctl task list`
- discover `--state` values with `dsctl enum list task-execution-status`
- discover `--execute-type` values with
  `dsctl enum list task-execute-type`
- `--workflow-instance` narrows the selected project's query to one workflow
  instance; the CLI validates it through the selected project's exact
  workflow-instance detail route and never scans projects to infer ownership
- workflow-definition filtering is not part of the stable `task-instance list`
  contract for DS 3.4.1 because the upstream BATCH task-instance paging query
  does not reliably apply `workflowDefinitionName`; use
  `workflow-instance list --workflow ...` first, then pass the returned
  workflow-instance id to `task-instance list` with the same project and
  `--workflow-instance`
- `--workflow-instance-name` filters by the upstream workflow-instance name
- `--state` accepts DS task execution status names such as
  `RUNNING_EXECUTION` and `SUCCESS`
- `--execute-type` accepts DS task execute type names such as `BATCH` and
  `STREAM`
- `--search` is the upstream free-text `searchVal` filter; use `--task` for an
  exact task instance name filter
- `--start` and `--end` filter task start time using DS datetime format
  `YYYY-MM-DD HH:MM:SS`; before 3.1.0 use both bounds, otherwise a single bound
  is supported. `--end` must be greater than or equal to `--start`.
  `resolved.time_filter` records exact interval and current-state semantics
- with `--all`, the CLI fetches remaining pages up to the standard safety limit
  and materializes one DS-style page payload

The `data` payload is a DS-style paging object whose `totalList` items expose
the current stable task-instance projection:

The enclosing `coverage` records the requested/observed pages, initial totals,
changes and observation times; `scope_complete` applies only to its stated
range. Offset pagination is not an atomic snapshot. Partial failures retain
coverage in `error.details.coverage`.

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

Fetches one task instance by id within one project.

Selection rules:

- select a project with `--project` or current project context
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- `--workflow-instance` is optional. When supplied, the CLI fetches that
  workflow instance directly inside the selected project and resolves the task
  within that narrow scope
- when it is omitted, the CLI searches the selected project's bounded
  task-instance pages by exact id, including the separate DS `BATCH` and
  `STREAM` query branches when the selected version supports them. This admits
  standalone STREAM instances whose native `workflowInstanceId` is `0`
- `resolved.workflowInstance` is present only for an explicitly supplied
  workflow scope; project-only selection never invents a workflow-instance id

## `dsctl task-instance watch`

Polls one task instance until it reaches a finished DS task execution state.

Options:

- `--project PROJECT`
- `--workflow-instance ID` (optional)
- `--interval-seconds N`
- `--timeout-seconds N`
- `--exit-status`: require a successful terminal execution result

Rules:

- ordinary watch exits 0 for any observed terminal state. With `--exit-status`,
  only `SUCCESS` or `FORCED_SUCCESS` exits 0; other terminal states return
  `execution_failed`, retain the final instance in `data` and exit 1

- select a project with `--project` or current project context
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- when supplied, `--workflow-instance` is resolved directly inside the selected
  project; when omitted, each poll repeats the project-only exact-id selection
  described by `task-instance get`
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

- select a project with `--project` or current project context
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- discover `--workflow-instance` values with
  `dsctl workflow-instance list --project etl-prod`
- `--workflow-instance` is required and is resolved directly inside the
  selected project before the relation query
- the task instance must belong to the supplied workflow instance
- the task instance must be one DS `SUB_WORKFLOW` task instance
- `resolved` includes `workflowInstance` and `taskInstance`

The `data` payload is a JSON object:

- `subWorkflowInstanceId`

## `dsctl task-instance log`

Reads the tail or a located source-line window of one task-instance log.

Selection rules:

- task-instance resources are id-first
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- `task-instance log` is the sole runtime-instance exception that does not
  require project selection
- `--workflow-instance` is not required because the DS logger API reads log
  chunks by task-instance id
- `--tail` means “keep the last N lines” and is implemented by chunking the DS
  logger API until exhaustion (default 200, within the existing scan budget)
- `--start-line N --limit N` selects a one-based source window (defaults 1/200
  when either window flag is set). It cannot combine with explicit `--tail`.
  `data.window` records source positions, scanned/returned lines, clipping,
  observation times and `next_start_line`; use its continuation instead of
  repeatedly expanding tail. `has_more: null` means EOF was not established
- `--raw` prints only `data.text`; errors still use the standard structured
  error envelope
- DS result code `10103` for an empty task log path is translated to stable
  error type `task_not_dispatched`

The `data` payload is a JSON object:

- `text`
- `lineCount`
- `window`: source-line coverage and continuation; the DS path preamble is
  counted separately. Raw output contains only text and no location metadata

## `dsctl task-instance force-success`

Forces one failed task instance into `FORCED_SUCCESS`.

Selection rules:

- select a project with `--project` or current project context
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- discover `--workflow-instance` values with
  `dsctl workflow-instance list --project etl-prod`
- `--workflow-instance` is required and is resolved directly inside the
  selected project before mutation
- the owning workflow instance must already be in one final state
- the task instance itself must currently be in `FAILURE`,
  `NEED_FAULT_TOLERANCE`, or `KILL`

The `data` payload is the refreshed task-instance projection.

## `dsctl task-instance savepoint`

Requests one savepoint for a running task instance.

Selection rules:

- select a project with `--project` or current project context
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- `--workflow-instance` is optional. Supply it for a workflow-owned task to
  keep the mutation in that narrow scope; omit it for a standalone STREAM task
  and the CLI first resolves the exact id inside the selected project
- the task instance must not already be in one finished state

The `data` payload is a JSON object:

- `requested`
- `taskInstance`

## `dsctl task-instance stop`

Requests stop for one task instance.

Selection rules:

- select a project with `--project` or current project context
- discover task-instance ids with
  `dsctl task-instance list --project etl-prod`
- `--workflow-instance` is optional. Supply it for a workflow-owned task to
  keep the mutation in that narrow scope; omit it for a standalone STREAM task
  and the CLI first resolves the exact id inside the selected project
- the task instance must not already be in one finished state

The `data` payload is a JSON object:

- `requested`
- `taskInstance`

## Out of Scope

Everything else remains provisional until it lands in code and is added here.
