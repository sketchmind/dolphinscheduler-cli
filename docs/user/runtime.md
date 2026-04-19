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
dsctl workflow-instance list --project etl-prod --start "2026-04-11 00:00:00" --end "2026-04-11 23:59:59"
dsctl workflow-instance digest <workflow_instance_id>
dsctl workflow-instance watch <workflow_instance_id>
```

`workflow-instance list` is the primary runtime query surface. Use
`--project` for project-scoped filters such as `--search` and `--executor`.
Without `--project`, global filters are limited to `--workflow`, `--host`,
`--start`, `--end`, and `--state`.

Inspect task logs:

```bash
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance list --project etl-prod --state FAILURE --start "2026-04-11 00:00:00" --end "2026-04-11 23:59:59"
dsctl task-instance log <task_instance_id>
```

`task-instance list` uses the project-scoped DS task-instance paging query. Use
`--workflow-instance` for the common per-run inspection path, or use `--project`
plus filters such as `--task`, `--executor`, `--host`, `--state`, `--start`,
and `--end` for runtime triage across workflow instances. To inspect task
instances for one workflow definition, first run
`dsctl workflow-instance list --project etl-prod --workflow daily-etl`, then
pass the returned instance id to `task-instance list --workflow-instance`. Use
`--search` only for the upstream free-text `searchVal` filter; use `--task` for
an exact task instance name filter.

Control runtime state:

```bash
dsctl workflow-instance stop <workflow_instance_id>
dsctl workflow-instance rerun <workflow_instance_id>
dsctl workflow-instance recover-failed <workflow_instance_id>
dsctl task-instance force-success <task_instance_id> --workflow-instance <workflow_instance_id>
```

High-impact mutations may require an explicit confirmation token.
