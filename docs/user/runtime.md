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

In the default JSON format, common create/online/run/watch/list results may
include up to three `next_actions`. Each entry contains a complete command with
resolved numeric identities and a `mutates` flag, so callers can continue the
common lifecycle without another help or schema lookup. The entries are
advisory; callers must still decide whether a mutating command matches their
intent. An explicit `--env-file` target is carried into every suggested
command. Table, TSV, and raw output remain pure rows or artifact text and do
not append navigation below the output.

List JSON may additionally contain `action_index`. It groups available stable
actions over returned instance ids and marks state-dependent target subsets.
This is a low-cost discovery aid: inspect `dsctl schema --command ACTION` only
for the action you choose. The index does not evaluate permissions or replace
the execution-time checks performed by DolphinScheduler.

When a selected task has downstream dependency nodes that are not included in
the run scope, DolphinScheduler may reject the command at execution time. The
CLI surfaces this as a warning or translated user-facing error when upstream
returns enough structured detail.

## Schedule Environments

Use `dsctl capabilities --section schedule` to check `environment_inheritance`
for the selected server. DS `3.2.2`–`3.4.3` pass a schedule environment to tasks
without their own environment. An explicit task environment takes precedence.

On DS `2.0.0`–`3.2.1`, native schedule storage accepts an environment but the
scheduler or task factory does not reliably pass it to default tasks. The CLI
rejects new positive schedule environment selections, including values supplied
by project preferences. Set `environment_code` on each task that needs it;
that task setting applies to both scheduled and manual runs. Use
`--environment-code 0` to create without a schedule environment or clear it.
DS `1.3.9` has no environment option.

The old task factory also affects `workflow run`, `run-task`, and `backfill`:
through `3.2.1`, new positive workflow environment selections are rejected,
including enabled project preferences. Explicit task environments remain the
alternative for these runs.

Updating another field preserves a preexisting schedule environment and reports
the runtime limitation. Viewing or activating such a schedule also warns;
clearing, taking offline, and deleting remain available. A successful readback
proves storage, not environment execution: verify the expected task output.

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
dsctl workflow-instance digest 901 --project etl-prod
dsctl workflow-instance watch 901 --project etl-prod
```

`workflow-instance list` is the primary runtime query surface. Every
workflow-instance query selects exactly one project from explicit `--project`
or current project context; explicit input wins. The CLI does not scan visible
projects to infer which project owns an instance id.

Inspect task logs:

```bash
dsctl task-instance list --project etl-prod --workflow-instance 901
dsctl task-instance list --project etl-prod --state FAILURE --start "2026-04-11 00:00:00" --end "2026-04-11 23:59:59"
dsctl task-instance log 902 --raw
```

`task-instance list` uses the project-scoped DS task-instance paging query.
Select a project with `--project` or current context even when narrowing with
`--workflow-instance`; this keeps requests bounded and unambiguous. Use filters
such as `--task`, `--executor`, `--host`, `--state`, `--start`, and `--end` for
runtime triage across workflow instances. To inspect task
instances for one workflow definition, first run
`dsctl workflow-instance list --project etl-prod --workflow daily-etl`, then
pass the returned instance id to `task-instance list` with the same project and
`--workflow-instance`. Use
`--search` only for the upstream free-text `searchVal` filter; use `--task` for
an exact task instance name filter.

Control runtime state:

```bash
dsctl workflow-instance stop 901 --project etl-prod
dsctl workflow-instance rerun 901 --project etl-prod
dsctl workflow-instance recover-failed 901 --project etl-prod
dsctl task-instance force-success 902 --project etl-prod --workflow-instance 901
```

Repair a finished instance DAG:

```bash
dsctl workflow-instance export 901 --project etl-prod > instance.yaml
# edit the task graph, then inspect the diff
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml --dry-run
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml
```

Use `--patch` with `dsctl template workflow-instance-patch --raw` for small
targeted changes, especially task renames. Use `--file` when the exported
instance DAG should be reconciled as the desired complete DAG. Instance edits
only allow workflow-level `global_params` and `timeout`; definition metadata and
schedule lifecycle belong to `workflow` and `schedule` commands. On modern
profiles, export and edit use the instance's current globals and timeout, so
editing one scalar preserves the other even when the attached definition differs.

On DS `2.0.0`, `2.0.1`, and `2.0.2`, task or edge changes require explicit
`--sync-definition`; native instance-only updates ignore the DAG. Synchronizing
persistent changes on these versions requires the current definition to be
ONLINE, because native metadata synchronization may set it ONLINE. Use
`workflow edit` to change an OFFLINE definition. Run `workflow online` first only
if publication is intended, then retry with `--sync-definition`. Dry-run retains
the compiled edit and reports the same blocker without writing. Scalar-only
instance changes remain supported without synchronization and are read back
after the native update, even when its response is null.

High-impact mutations may require an explicit confirmation token.
