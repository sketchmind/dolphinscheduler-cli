# Runtime Operations

Use this reference for backfill, failure diagnosis, recovery, force-success, or
finished workflow-instance DAG repair.

1. **Locate.** Obtain the workflow-instance id from run output, a complete
   authorized suggested command, or an authoritative list. Continue when the
   exact instance and project are resolved.
2. **Narrow.** Move from instance digest or watch to the task-instance list,
   then read the relevant raw task log. Continue when the failing or targeted
   task and its current state are identified.
3. **Choose intent.** Treat rerun, recover-failed, execute-task, stop, and
   force-success as distinct business operations. Select force-success only
   when the user explicitly intends to override task state. Continue when the
   operation exactly represents the requested recovery.
4. **Repair when needed.** Start finished-instance DAG repair from
   `workflow-instance export`, use the matching patch or full-file edit, and
   preview the instance edit before applying it. Continue when the preview
   proves the intended graph change.
5. **Verify.** Bound watch and polling waits to the requested monitoring window,
   then read instance and task state after each recovery mutation.

Complete runtime work when the requested instance and task states are
authoritative, or when a precise blocker identifies the required authority or
external change.
