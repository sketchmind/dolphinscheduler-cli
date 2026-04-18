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
dsctl schema --group task-instance
dsctl schema --command task-instance.list
```

`schema --group` values are the command group names exposed by
`dsctl capabilities --summary` at `data.resources.groups` and by full schema
`data.commands[].name`.

## Discovery

```bash
dsctl version
dsctl context
dsctl doctor
dsctl schema
dsctl schema --command task-instance.list
dsctl capabilities
dsctl capabilities --summary
dsctl enum list WorkflowExecutionStatus
```

## Governance And Project Resources

```bash
dsctl project list
dsctl env list
dsctl datasource list
dsctl resource list /
dsctl worker-group list
dsctl alert-group list
dsctl user list
```

## Workflow Authoring

```bash
dsctl template workflow > workflow.yaml
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
dsctl workflow create --file workflow.yaml --project etl-prod
```

## Runtime

```bash
dsctl workflow run daily-etl --project etl-prod
dsctl workflow run-task daily-etl --task load --project etl-prod
dsctl workflow-instance digest <workflow_instance_id>
dsctl workflow-instance watch <workflow_instance_id>
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance list --project etl-prod --state FAILURE
dsctl task-instance log <task_instance_id>
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
