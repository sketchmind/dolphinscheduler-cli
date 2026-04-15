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
dsctl workflow-instance digest 901
dsctl workflow-instance watch 901
```

Inspect task logs:

```bash
dsctl task-instance log 3001 --workflow-instance 901
```

Control runtime state:

```bash
dsctl workflow-instance stop 901
dsctl workflow-instance rerun 901
dsctl workflow-instance recover-failed 901
dsctl task-instance force-success 3001 --workflow-instance 901
```

High-impact mutations may require an explicit confirmation token.
