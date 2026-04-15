# Commands

The stable CLI surface is documented in the
[CLI Contract](../reference/cli-contract.md). Use that document as the
machine-readable behavior contract for command names, output envelopes, error
shape, warnings, and dry-run behavior.

## Discovery

```bash
dsctl version
dsctl context
dsctl doctor
dsctl schema
dsctl capabilities
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
dsctl workflow-instance digest 901
dsctl workflow-instance watch 901
dsctl task-instance log 3001 --workflow-instance 901
```

## Output Contract

All stable commands return the standard JSON envelope:

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
