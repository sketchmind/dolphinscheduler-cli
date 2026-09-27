# Schedule Lifecycle

Use this reference for schedule configuration or lifecycle mutations.

1. **Resolve.** For an existing schedule, select its ID and read the workflow
   binding and current release state. For creation, select the existing workflow
   and project, then check its publication state and attached schedule. Continue
   when the binding is authoritative and the requested operation fits the
   current state.
2. **Assess.** For configuration changes, use preview for proposed run times and
   explain when mutation risk or lifecycle constraints affect the decision.
   Continue when cron, timezone, time range and relevant risk are understood.
   When missed executions are in scope, discover the selected profile's missed
   fire policy choices. An omitted
   create policy uses the native default; an omitted update preserves the stored
   policy. Select a different policy only when it fits the requested outcome.
   When selecting an environment, check `capabilities --section schedule` for
   `environment_inheritance`. DS `1.3.9` has no schedule or task environment
   fields; report a requested environment as unsupported on that release.
   On DS `2.0.0`–`3.2.1`, use explicit task `environment_code` when that definition
   change fits the authorized outcome; it also affects manual runs. On releases
   with an environment field, schedule `--environment-code 0` bypasses a project
   environment preference or clears a stored value. Continue when the selected
   environment can reach the tasks.
3. **Apply.** Treat configuration and activation as separate mutations.
   Create/update changes configuration; execute online only when activation is
   in the requested outcome. Updating requires an offline schedule. Taking its
   workflow offline also deactivates the schedule; bringing the definition
   online leaves schedule activation as a separate decision. Refresh schedule
   state after lifecycle changes. Restore a previously active schedule after an
   edit only when preserving that activation is part of the requested outcome.
   For an authorized deletion, ensure the schedule is OFFLINE, then use
   `schedule delete ID --force` within the selected project scope.
   On snapshot conflict, refresh the baseline and retain the intended delta
   before rebuilding the mutation. Continue when the mutation result is available.
4. **Verify.** For deletion, confirm absence within the bound project scope.
   For other mutations, read the schedule back and compare its workflow binding,
   cron, timezone, time range, any requested missed fire policy and release state
   with the request.
   When environment execution matters, verify its effect in task output;
   environment readback alone proves only storage.

Complete schedule work when the requested fields are authoritative and match
the outcome, or when an authoritative list/get confirms deletion.
