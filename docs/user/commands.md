# Commands

The stable CLI surface is documented in the
[CLI Contract](../reference/cli-contract.md). Use that document as the
machine-readable behavior contract for command names, output envelopes, error
shape, warnings, and dry-run behavior.

Use `dsctl schema` for exact command arguments, options, choices, and selector
rules. Use `dsctl capabilities` for lightweight feature discovery; it is not an
argument schema.

For agent or scripted discovery, prefer scoped self-description calls when the
full payload is unnecessary:

```bash
dsctl capabilities --summary
dsctl capabilities --section runtime
dsctl schema --list-groups
dsctl schema --list-commands
dsctl schema --group task-instance
dsctl schema --command task-instance.list
dsctl enum names
```

`schema --group` values come from `dsctl schema --list-groups`.
`schema --command` values come from `dsctl schema --list-commands`.
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
dsctl capabilities
dsctl capabilities --summary
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
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --output-format table workflow-instance list --project etl-prod
dsctl --output-format tsv --columns id,name,state task-instance list --workflow-instance <workflow_instance_id>
dsctl --output-format tsv --columns '*' task-instance list --workflow-instance <workflow_instance_id>
```

Use `dsctl schema --command <ACTION>` and inspect `data_shape` to discover the
canonical row/object path and default display columns for row-oriented
commands.
For quick terminal inspection of one command contract, use table output; scoped
schema views include compact rows for arguments, options, payload hints, and
data-shape metadata:

```bash
dsctl --output-format table schema --command datasource.create
dsctl --output-format table --columns flag,description,discovery_command schema --command environment.create
```
