# Commands

The stable CLI surface is documented in the
[CLI Contract](../reference/cli-contract.md). Use that document as the
machine-readable behavior contract for command names, output envelopes, error
shape, warnings, and dry-run behavior.

When the command path is known, start with its leaf `--help`; this is the
task-oriented projection for constructing the common next invocation. Use the
bounded `dsctl schema` index to find unknown actions, `schema --group GROUP` to
browse one unknown family, and `schema --command ACTION` when exact arguments,
options, choices, selector rules, resolution precedence, payload hints, or
output shape are required. Use `dsctl capabilities` for feature discovery; it
is not an argument schema.

For agent or scripted discovery, inspect only the immediate action and expand
only the contract that is needed:

```bash
dsctl capabilities
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
dsctl template datasource --type MYSQL
dsctl resource list /
dsctl worker-group list
dsctl alert-group list
dsctl user list
```

## Workflow Authoring

```bash
dsctl template task
dsctl task-type get SQL
dsctl task-type schema SQL
dsctl template task SQL --variant pre-post-statements --raw
dsctl template workflow --raw > workflow.yaml
dsctl template workflow-patch --raw > patch.yaml
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
dsctl workflow create --file workflow.yaml --project etl-prod
dsctl workflow edit WORKFLOW --file workflow.yaml --dry-run
dsctl workflow edit WORKFLOW --patch patch.yaml --dry-run
```

## Runtime

```bash
dsctl workflow run daily-etl --project etl-prod
dsctl workflow run-task daily-etl --task load --project etl-prod
dsctl workflow-instance export <workflow_instance_id> > instance.yaml
dsctl workflow-instance edit <workflow_instance_id> --file instance.yaml --dry-run
dsctl workflow-instance digest <workflow_instance_id>
dsctl workflow-instance watch <workflow_instance_id>
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance list --project etl-prod --state FAILURE
dsctl task-instance log <task_instance_id> --raw
```

## Output Contract

All stable commands return the standard JSON envelope by default:

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

Errors use a stable `error.type` and include structured details when the CLI can
derive them without guessing.

For scan-friendly terminal output, pass a global output renderer before the
command group:

```bash
dsctl --compact --columns id,name,state workflow-instance list --project etl-prod --page-size 10
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --output-format table workflow-instance list --project etl-prod
dsctl --output-format tsv --columns id,name,state task-instance list --workflow-instance <workflow_instance_id>
dsctl --output-format tsv --columns '*' task-instance list --workflow-instance <workflow_instance_id>
```

For agents and scripts reading command results, prefer
`--compact --columns ...` plus a small `--page-size`. Compact JSON is UTF-8,
keeps the standard envelope, and changes only insignificant whitespace.
Column projection and page size provide the largest reductions; compact JSON
is a secondary, lossless whitespace optimization.

Successful data and raw artifacts use stdout. Structured command errors use
stderr. Table and TSV keep stdout row-only; partial/non-first-page summaries
and warnings use stderr so redirection and simple pipelines remain valid. Raw
artifact warnings also use stderr without changing the artifact body.

Use `dsctl schema --command <ACTION>` and inspect `data.command.data_shape` to
discover the canonical row/object path and default display columns for
row-oriented commands. For quick terminal inspection of one command contract,
use table output. The renderer derives compact argument, option, payload, and
data-shape rows from the canonical `data.command` object; it does not append a
non-standard footer or duplicate those rows in JSON:

```bash
dsctl --output-format table schema --command datasource.create
dsctl --output-format table --columns flag,description,discovery_command schema --command environment.create
```
