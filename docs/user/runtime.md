# Runtime Operations

Runtime commands inspect or control DolphinScheduler workflow instances and
task instances through REST endpoints.

## Workflow Runs

Run a workflow definition:

```bash
dsctl workflow run daily-etl --project etl-prod
```

Run one task from a workflow definition:

```bash
dsctl workflow run-task daily-etl --task load --project etl-prod
```

When a selected task has downstream dependency nodes that are not included in
the run scope, DolphinScheduler may reject the command at execution time. The
CLI surfaces this as a warning or translated user-facing error when upstream
returns enough structured detail.

## Backfill

Backfill uses DolphinScheduler complement-data semantics:

```bash
dsctl workflow backfill daily-etl \
  --project etl-prod \
  --start "2026-04-01 00:00:00" \
  --end "2026-04-02 00:00:00"
```

Use `--dry-run` to inspect the request without sending it.

## Instances

Inspect progress:

```bash
dsctl workflow-instance digest <workflow_instance_id>
dsctl workflow-instance watch <workflow_instance_id>
```

Inspect task logs:

```bash
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance log <task_instance_id>
```

Control runtime state:

```bash
dsctl workflow-instance stop <workflow_instance_id>
dsctl workflow-instance rerun <workflow_instance_id>
dsctl workflow-instance recover-failed <workflow_instance_id>
dsctl task-instance force-success <task_instance_id> --workflow-instance <workflow_instance_id>
```

High-impact mutations may require an explicit confirmation token.
