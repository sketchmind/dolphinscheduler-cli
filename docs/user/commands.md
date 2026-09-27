# Commands

The [CLI Contract](../reference/cli-contract.md) documents stable command names,
output envelopes, errors, warnings, and dry-run behavior. Use
`dsctl schema --command ACTION` for the machine-readable invocation contract.

Command notation uses uppercase semantic metavars: `PROJECT`, `WORKFLOW`,
`WORKFLOW_INSTANCE`, `FILE`, and similar tokens mean “replace this value”.
Literal examples use concrete values such as `etl-prod`. Do not type a metavar
literally. The docs and help output do not use `<project>`-style placeholders,
because shells interpret angle brackets as redirection. In JSON metadata,
directly executable values use `*_command`; values that still need substitution
use `*_command_pattern`.

When the command path is known, start with its leaf `--help`; this is the
task-oriented projection for constructing the common next invocation. Use the
bounded `dsctl schema` index to find unknown actions, `schema --group GROUP`
to browse one unknown family, and `schema --command ACTION` when exact
arguments, options, choices, selector rules, resolution precedence, payload
hints, or output shape are required. Use
`dsctl capabilities --action ACTION` for one selected-version availability
fact and a bounded capability section for broader feature discovery; neither is
an argument schema.

For agent or scripted discovery, inspect only the immediate action and expand
only the contract that is needed:

```bash
dsctl capabilities
dsctl capabilities --action workflow.create
dsctl capabilities --section runtime
dsctl capabilities --full
dsctl schema
dsctl schema --list-groups
dsctl schema --group task-instance
dsctl schema --command task-instance.list
dsctl enum names
```

`schema --group` values come from `dsctl schema --list-groups`.
Action names are present in the default index and group views;
`schema --list-commands` retains the detailed compatibility inventory.
In an action-local response, use `data.command.invocation` for the exact CLI
path and placeholders, and obey `data.command.constraints[]` before executing.
An option with `resolution` is not a static parser default: its `precedence`
lists the value sources consulted when the option is omitted. The current
runtime source order is `flag`, `project_preference`, then `default`, and the
`fallback` field carries the value used by that terminal source. If `default`
also appears beside `resolution`, it is a schema-version-compatible projection
of the same terminal value; `resolution` remains authoritative.
`enum list ENUM` values come from `dsctl enum names`.

## Discovery

```bash
dsctl version
dsctl context
dsctl doctor
dsctl schema
dsctl schema --list-groups
dsctl schema --list-commands
dsctl schema --command task-instance.list
dsctl schema --full
dsctl capabilities
dsctl capabilities --action workflow.create
dsctl capabilities --section runtime
dsctl capabilities --full
dsctl enum names
dsctl enum list WorkflowExecutionStatus
```

## Governance And Project Resources

```bash
dsctl project list
dsctl environment list
dsctl template environment
dsctl environment create --name stock-etl --config-file env.sh
dsctl template cluster
dsctl cluster create --name k8s-prod --config-file cluster-config.json
dsctl datasource list
dsctl schema --command datasource.create
dsctl template datasource --ds-version 3.4.2 --type MYSQL
dsctl namespace create --namespace etl-prod --cluster-code 9001
dsctl namespace delete etl-prod --force
dsctl resource list
dsctl worker-group list
dsctl alert-group list
dsctl user list
dsctl tenant create --tenant-code analytics --queue default
dsctl tenant update analytics --queue batch
```

Datasource templates are exact-version payload catalogs. Match
`--ds-version` to the target cluster when authoring a file; create/update also
validate the file against the configured cluster version before sending it.
Datasource detail output masks passwords, cloud secrets, kubeconfigs, and SSH
keys at the CLI boundary.

Namespace management exists in DolphinScheduler 3.0.0 and newer. Creation uses
`--k8s` on DS 3.0.x and `--cluster-code` on DS 3.1.0+; quota flags are accepted
only through DS 3.2.0. Treat namespace deletion on DS 3.0.0-3.1.9 as destructive
to the real Kubernetes namespace as well as its DolphinScheduler registration.

Tenant codes are DS identities, not mutable labels. Select the code when
creating a tenant; `tenant update` changes only its queue or description. This
matches the edit behavior of the DolphinScheduler UIs across supported
versions and avoids an unsafe tenant-code update defect in DolphinScheduler
`1.3.9`. Older scripts may continue to pass the hidden deprecated
`--tenant-code` update option when its value matches the current code; an
attempted rename returns a structured input error.

## Workflow Authoring

```bash
dsctl template task
dsctl task-type get SQL
dsctl task-type schema SQL
dsctl template task SQL --raw
dsctl template workflow --raw > workflow.yaml
dsctl template workflow-patch --raw > patch.yaml
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
dsctl workflow create --file workflow.yaml --project etl-prod
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml --dry-run
dsctl workflow edit daily-etl --project etl-prod --patch patch.yaml --dry-run
```

## Runtime

```bash
dsctl workflow run daily-etl --project etl-prod
dsctl workflow run-task daily-etl --task load --project etl-prod
dsctl workflow-instance export 901 --project etl-prod > instance.yaml
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml --dry-run
dsctl workflow-instance digest 901 --project etl-prod
dsctl workflow-instance watch 901 --project etl-prod
dsctl task-instance list --project etl-prod --workflow-instance 901
dsctl task-instance list --project etl-prod --state FAILURE
dsctl task-instance log 902 --raw
```

## Output Contract

Structured commands return the standard JSON envelope by default:

```json
{
  "ok": true,
  "action": "version",
  "resolved": {},
  "data": {}
}
```

Errors use a stable `error.type` and include structured details when the CLI can
derive them without guessing.

Warnings appear only when present, as `warnings: [{"code": "...", "message":
"..."}]` with any applicable diagnostic facts in each object. Both JSON
formats keep that envelope. `json` retains ordinary object rows;
`json-compact` encodes declared business lists using shared column names and
positional value arrays. Migrate the
old `--compact` flag to `--format json-compact` and read warning objects from
`warnings` instead of `warning_details`.

Raw artifact operations such as workflow exports, templates with `--raw`, and
raw task logs keep their native success body. Global display options do not
change that body; failures remain structured.

For scan-friendly terminal output, pass a global output renderer before or after
the command path. Examples use the canonical prefix form:

```bash
dsctl --format json-compact --columns id,name,state workflow-instance list --project etl-prod --page-size 10
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --format table workflow-instance list --project etl-prod
dsctl --format tsv --columns id,name,state task-instance list --project etl-prod --workflow-instance 901
dsctl --format tsv --columns '*' task-instance list --project etl-prod --workflow-instance 901
```

For agents and scripts reading command results, use filters, explicit columns
and a small `--page-size` to bound the information. `json-compact` puts
`{"columns":[...],"rows":[...]}` at the declared business-list path. Each value
array is one row; match values to columns by position. It retains all returned
fields by default, including native nested values and numbers. Empty and
single-row lists use the same container. Details, schemas, templates and
diagnostics retain their structures with compact whitespace. Compact lists
accept top-level `--columns` and `*`; select a parent field or use `json` when
you need dotted-path projection. See [JSON layout](../reference/cli-contract.md#json-layout)
for encoding boundaries and decoding rules.

Successful data and raw artifacts use stdout. Structured command errors use
stderr. Table and TSV keep stdout row-only; partial/non-first-page summaries
and warnings use stderr so redirection and simple pipelines remain valid. Raw
artifact warnings also use stderr without changing the artifact body.

Use `dsctl schema --command ACTION` and inspect `data.command.data_shape` to
discover the canonical row/object path and default display columns for
row-oriented commands. For quick terminal inspection of one command contract,
use table output. The renderer derives compact argument, option, payload, and
data-shape rows from the canonical `data.command` object; it does not append a
non-standard footer or duplicate those rows in JSON:

```bash
dsctl --format table schema --command datasource.create
dsctl --format table --columns flag,description,discovery_command schema --command environment.create
```
