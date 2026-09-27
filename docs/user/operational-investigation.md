# Operational investigation

Use an explicit project and retain the target and selection metadata alongside the
rows you inspect. A successful request describes the scope actually read; it does
not establish that every instance, execution attempt or historical state was read.
These routes use REST reads. Starting, recovering, stopping or editing work is a
separate decision.

## Fix the selection window first

`workflow-instance list` and `task-instance list` filter **start_time**, using DS
`YYYY-MM-DD HH:MM:SS` values. They do not select failure time or schedule time.
Use both `--start` and `--end` when investigating older targets:

| Read action | Exact targets | Interval | Single bound |
| --- | --- | --- | --- |
| workflow-instance list | 1.3.9 through 3.0.0 | `(start,end]` | rejected |
| workflow-instance list | 3.0.1 through 3.0.6 | `[start,end]` | rejected |
| task-instance list | 1.3.9 through 3.0.6 | `(start,end]` | rejected |
| both lists | 3.1.0 through 3.4.2 | `[start,end]` | supported |

These are reviewed final-release memberships, not a promise about an arbitrary
intermediate or custom version. `resolved.time_filter` carries the applicable
semantics. The CLI rejects legacy one-sided filters because the upstream mapper
would ignore the filter or compare against a missing bound. It does not subtract
a second to disguise an exclusive lower boundary.

`--state FAILURE` selects the state observed now. It cannot prove that a task never
failed earlier. `runTimes` identifies execution rounds and may increase for
operations beyond rerunning; `commandType` can change when an instance is rerun.
Neither field alone proves the original scheduling source.

## Read coverage and revisit only the needed range

For list requests, `data.coverage` records the requested page size/start page,
initial server totals, each requested and reported page, rows read, changes in
reported totals, and observation start/end timestamps. `--all` reads the initial
page range within the safety limit. `scope_complete=true` means that range was
read; `scope=initial_page_range` does not mean a changing server was frozen.
`atomic_snapshot` is always false. A growing total may require a later explicitly
scoped check. Offset paging can also move rows between pages without changing its
total; unchanged totals do not prove absence of duplicates or omissions.

Existing materialized pagination fields describe the returned collection; consult
`coverage.initial_total` and the per-page observations for the original server
counts. A mid-read API or transport failure remains an error and carries partial
coverage in `error.details.coverage`. Already read rows are not presented as a
successful complete list, and permission failure is never treated as zero rows.
`workflow-instance digest` includes coverage for the task pages it inspected.

## Follow an instance to its actual failing task

1. Inspect `workflow-instance get INSTANCE --project PROJECT`, then
   `workflow-instance digest INSTANCE --project PROJECT` for the observed task
   states and their read coverage.
2. Read the relevant `task-instance get TASK --project PROJECT --workflow-instance INSTANCE`. A waiting or
   undispatched task can have no worker log yet; `task_not_dispatched` is useful
   evidence, not proof that a worker executed and failed.
3. For a `SUB_WORKFLOW` task, run
   `task-instance sub-workflow TASK --project PROJECT --workflow-instance PARENT`,
   then inspect the returned child instance directly. For the reverse relation,
   use `workflow-instance parent CHILD --project PROJECT`.

Parent and child instances can belong to different projects. A relationship
response containing only the target instance id does not establish its project;
resolve that target scope before the next project-scoped read.

Do not depend on the ordinary workflow-instance list to discover every child:
1.3.9 explicitly excludes subprocess instances from that list. A missing child
relation can mean it has not been created, is absent, or cannot be read; retain
the actual error rather than inferring a child failure from the parent exit code.

## Read logs by source position

```sh
dsctl task-instance log TASK --start-line 1 --limit 200
dsctl task-instance log TASK --start-line 201 --limit 200
```

Line coordinates are one-based source log lines. The DS `[LOG-PATH]` preamble is
not a source line in window mode. Read the returned `data.window.next_start_line`
rather than assuming the second example is always the right continuation. The
window includes actual start/end lines, scanned and returned line counts,
clipping, timestamps and continuation evidence. One extra source line can be read
to establish `has_more=true`; it is retained for the next request, not skipped.
At the existing read-budget boundary, `has_more=null` means EOF was not observed.
This is an observation of the log available during that read, not an immutable
snapshot; rotation or replacement can invalidate old coordinates.

Without window flags, the command retains the existing default tail of 200 lines.
`--tail N` still scans from the beginning to find the available end, so repeatedly
increasing it rescans earlier chunks. Tail metadata records that scan and the
retained source range. The existing path preamble can remain in a short tail;
`window.header_lines` distinguishes it from source lines. `--tail` cannot be
combined with `--start-line` or `--limit`. `--raw` emits only text; retain a
structured response separately when line references and coverage are needed.

A window is not a regex search. If the selected task only reports a generic
nonzero exit, continue around the referenced source line or inspect the nested
child/resource identified by the log. Do not turn the upper-level exit into an
unsupported root-cause claim.

## No instance, waiting, recovery and migration

For a suspected missed schedule with no instance, inspect the workflow and its
schedule (`workflow get`, `schedule list`, `schedule get`) before widening an
instance scan. `schedule preview`/`schedule explain` describe cron and timezone
expectations; they do not prove that a historical trigger was dispatched. Inspect
`monitor health`, `monitor server TYPE` and, when permitted, `monitor database` for
current infrastructure evidence. Waiting tasks may need the appropriate
`task-group get` and `task-group queue list` read; queue force-start and priority
changes are separate mutations. Check command availability for the exact target.

For a queue investigation, retain the queue row's `id`, `taskId`,
`workflowInstanceId`, and project identity: they select different resources.
After a force-start request, `accepted: true` means DS accepted the request.
Re-read the group queue and the actual task/instance to establish dispatch and
execution; the acceptance receipt alone cannot establish either. A queue row
with insufficient identity facts requires another scoped read, not a guessed
task id.

After a start/backfill request, preserve the returned receipt. When it provides a
trigger selector, follow its offered read route to discover the resulting
instances; do not equate a receipt with completion of all requested dates. Inspect
each relevant instance/task round. Rechecking one instance updates only that
instance's evidence, not the coverage of a previous multi-page scan.

Before `workflow backfill`, `workflow-instance rerun` or `recover-failed`, inspect
the concrete command schema and the current definition/instance parameters and
dependencies. A date range does not establish that historical inputs, upstream
partitions or task side effects are safe to repeat. Read the post-action round
and task results when an action is explicitly authorized.

For cross-environment migration, export and inspect semantic YAML, discover the
destination's projects, tenants, worker groups, environments and referenced
resources, and validate the destination plan before applying it. Semantic YAML is
not a lossless backup of every native execution state. Reverting a workflow
definition cannot undo data already written by its tasks; newly started work does
not automatically inherit an old instance's scheduling environment.

## Source evidence for time predicates

The interval rules above come from the reviewed exact upstream mappers and the
legacy service handling of absent bounds. Upstream behavior for a single bound
explains why the CLI rejects those inputs before a request:

| Upstream read | Exact targets | Start only | End only |
| --- | --- | --- | --- |
| workflow instances | 1.3.9 through 3.0.0 | compares against a missing upper bound | no time filter |
| workflow instances | 3.0.1 through 3.0.6 | no time filter | no time filter |
| task instances | 1.3.9 through 3.0.6 | compares against a missing upper bound | no time filter |
| both reads | 3.1.0 through 3.4.2 | independent bound | independent bound |

Representative immutable source references retain the native predicates and
absent-bound handling:

- DS 1.3.9 [workflow mapper](https://github.com/apache/dolphinscheduler/blob/174c78c4a90a53fdfe7131e9b065edaa38b7936f/dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/dao/mapper/ProcessInstanceMapper.xml#L68),
  [task mapper](https://github.com/apache/dolphinscheduler/blob/174c78c4a90a53fdfe7131e9b065edaa38b7936f/dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/dao/mapper/TaskInstanceMapper.xml#L100)
  and [default time parsing](https://github.com/apache/dolphinscheduler/blob/174c78c4a90a53fdfe7131e9b065edaa38b7936f/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/BaseService.java#L144).
- DS 3.0.1 [workflow mapper](https://github.com/apache/dolphinscheduler/blob/cb03ca8166cb9d6d8e63db43664b4a2126ea43b4/dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/dao/mapper/ProcessInstanceMapper.xml#L117).
- DS 3.1.0 [task mapper](https://github.com/apache/dolphinscheduler/blob/ae33ba594754425a2ac72100450adffd3b5e3971/dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/dao/mapper/TaskInstanceMapper.xml#L186).

The selected exact recipe retains its own membership and interval; these source
examples do not authorize neighboring or custom releases. Filtering start_time
does not create a historical snapshot of task state or select failure time.
