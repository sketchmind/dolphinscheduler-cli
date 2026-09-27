# Runtime Operations

Use this reference for launching and verifying a new execution, backfill,
failure diagnosis, recovery, force-success, or finished-instance DAG repair.

For a launch, select the workflow and requested runtime parameters, preview with
the action's CLI `--dry-run`, then send one authorized trigger. DS
`--execution-dry-run` creates instances while skipping plugin execution; choose
it only for that requested outcome. Continue below using the trigger receipt.
`doctor` verifies configuration and API access; it does not prove that the API,
master and workers can execute a workflow together.

1. **Locate.** Obtain the workflow-instance id from run output, a complete
   authorized suggested command, or an authoritative list. Continue when the
   exact instance and project are resolved. Carry that project through
   project-scoped workflow-instance and task-instance commands with `--project`,
   unless a deliberate current project context supplies it. `task-instance log`
   uses only the task-instance ID and its log options; it has no project selector.
   An accepted trigger still awaiting identities is a receipt; follow its
   identity-resolution command before watching an instance. `triggerCode` is
   not a workflow-instance ID. If resolution is pending or unavailable, bind
   only real IDs from authoritative discovery evidence; preserve the receipt
   and report an unresolved execution identity when it cannot be established.
   A parent/child relationship can cross projects; resolve the target instance's
   project before carrying that new id into a project-scoped read.
2. **Narrow when needed.** For diagnosis or a task operation, move from instance
   digest to the task-instance list, then the relevant log window. Preserve
   source positions and follow returned continuation when more evidence is
   needed. Continue when the failing or targeted task and state are identified;
   otherwise go directly to execution verification.
3. **Choose recovery intent.** Treat rerun, recover-failed, execute-task, stop, and
   force-success as distinct business operations. Select force-success only
   when the user explicitly intends to override task state. Continue when the
   operation exactly represents the requested recovery.
4. **Repair when needed.** Start finished-instance DAG repair from
   `workflow-instance export`, use the matching patch or full-file edit, and
   lint and preview the instance edit before applying it. `--sync-definition`
   adds a write to the saved definition and requires that change to be in scope.
   Continue when the preview proves the intended graph change.
5. **Verify.** Bound watch and polling waits to the requested monitoring window,
   then verify the requested instance/task state after each recovery mutation.
   Carry the receipt's execution baseline into its suggested watch command so
   an old final state cannot complete the new run. Use `watch --exit-status` when
   success is required and a finite `--timeout-seconds` for bounded observation.
   An observation timeout retains the last facts; it does not mean execution
   stopped or failed.

For an inventory investigation, fix the time window and requested population
first. Read the returned filter semantics and page coverage before claiming
completeness. A current failure filter establishes current state; earlier
attempts and the original scheduling source require their own evidence.
Report generic exit errors as unresolved causes until a task or child log
provides the underlying failure.

Complete runtime work when the requested instance and task states are
authoritative, or when a precise blocker identifies the required authority or
external change.
