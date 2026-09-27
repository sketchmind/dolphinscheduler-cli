# Execution receipts and mutation outcomes

This document explains execution identities, mutation uncertainty and recovery
observations. The public [CLI contract](../reference/cli-contract.md) owns command
syntax and output fields. These rules do not promote an exact profile or
establish live-server coverage.

## Acceptance and identity

`workflow run`, `run-task`, and `backfill` retain `data.workflowInstanceIds`, containing only actual workflow instance IDs. They also return `accepted: true` and `instanceResolution`:

- `resolved`: this response or its immediate trigger query supplied real instance IDs.
- `pending`: the server supplied a trigger code, but the immediate trigger query has not supplied IDs yet.
- `unavailable`: the exact executor reply does not identify instances and has no supported trigger receipt to resolve.

DS 3.2.0, 3.2.1 and 3.2.2 return a scalar **triggerCode**, not a workflow instance ID. The CLI preserves it separately and performs the compiled `instance_trigger` read once. An empty result remains pending; multiple IDs remain multiple IDs. A failed query raises the existing post-mutation error with `mutation_applied: true`, `phase: instance_resolution`, the accepted receipt in `details.execution`, and its project. It does not resubmit the executor request.

The command `workflow-instance list --trigger-code CODE --project PROJECT` repeats the query. The exact source endpoint exists from 3.2.0 through 3.4.3. It returns `totalList` identity rows plus trigger and resolution facts; `resolved.query` identifies a non-paginated identity projection. Ordinary list filters, non-default pagination and `--all` cannot be combined with this mode. This primitive is not automatically added to the separately reviewed candidate-read policy.

The trigger query is an observation, not a guarantee that a backfill has produced every expected instance or completed every requested date. `resolved` describes available identities, not backfill coverage. Before 3.2.0 the CLI cannot infer an ID from the successful empty reply by matching workflow name or time.

Source evidence:

- 3.2.0 `ExecutorServiceImpl.java:267–286` generates/returns the trigger; `ProcessInstanceController.java:445–449` and `ProcessInstanceServiceImpl.java:1094–1107` implement its read.
- The compiler owns both native controller names and their exact request/model closure in `compiled_workflow_instances.py`; workflow execution sources include this dependency only for the three trigger-code releases.

## Partial mutations

For workflow run, run-task and backfill, a recognized upstream
`START_WORKFLOW_INSTANCE_ERROR` containing the Netty client's `Call method to
... failed` reports `api_transport_error`, preserves the upstream result source
and records `mutation_may_have_applied: true` and `request_replay_safe: false`.
The wrapper covers both send and response failures, so it cannot prove that
dispatch did not happen. The suggestion names the scoped instance-list command
and master connectivity check; no automatic resend occurs. Parallel backfill
also preserves its partial-dispatch warning. Other start errors retain their
existing classification. Source: exact 3.4.1 `ExecutorController`,
`ExecutorServiceImpl.triggerWorkflowDefinition` and
`NettyRemotingClient.sendSync`.

Workflow create/edit use the existing `mutation_call` and `verify_mutation` outcome classification. A small workflow progress record preserves `completed_stages`, `failed_stage`, `known_resources`, and uncertain stages on errors. It never retries or rolls back a previous write. Success retains progress in `resolved.mutation`.

If output rendering fails after a command returns, its error retains the result
data, resolved identities and receipts with `phase: output_render` and
`result_available: true`. This is distinct from a failed request. The caller
must not repeat a mutation merely to change display options; a retained preview
still does not imply that a write occurred.

Creation can separately complete workflow creation, identity readback, workflow publication, schedule creation, schedule publication, and final workflow readback. If a later stage fails, prior IDs/names and successful stages remain visible. Edit preserves the updated workflow identity and, for legacy edit followed by publication, the preceding update. First-stage transport uncertainty retains `mutation_may_have_applied`; it does not introduce a contradictory `mutation_applied: false`.

Instance stop, rerun, recover-failed and execute-task retain the known instance/project when dispatch is uncertain or readback fails. A successful control request followed by failed readback is reported as applied with `failed_stage: instance_readback`. This describes the accepted control request, not proof that the requested runtime state has been reached.

DS `3.2.0` through `3.4.3` can also return the generic execute-instance code
`50015` from a stop path whose outcome is ambiguous. In `3.2.x`, stoppable
running states are written as `READY_STOP` before the API calls the master. From
`3.3.1`, non-direct stop states call the master while `SERIAL_WAIT` is handled by
a direct database transaction. A lost or failed master reply does not prove
that stop was not applied, while the same generic result can also cover a
precondition or database failure. The stop request carries no state
compare-and-swap condition, so the CLI's pre-request state read cannot prove
which branch the server later executed. For these exact releases, the CLI
therefore returns `mutation_outcome_unknown`, preserves the upstream result and
known instance/project, and sets `mutation_may_have_applied: true` and
`request_replay_safe: false`. It does not label the result as a transport or
master failure, read back automatically, or resend stop. Rerun,
recover-failed, and execute-task keep their separate command-insert semantics.

## Recovery and waiting

Rerun and recover-failed return `resolved.execution_baseline.run_times` when the pre-dispatch instance contains a positive marker. `workflow-instance watch --after-run-times N` requires both `runTimes > N` and a terminal state. It need not observe RUNNING: a short new execution may finish between polls. Timeouts retain `last_run_times`, `after_run_times`, and `new_execution_observed`; ordinary watch retains its existing current-instance semantics.

Reviewed source membership is explicit in `upstream/replay_baselines.py`:

- All 37 exact versions increment `runTimes` for rerun and recover-failed. Through 3.2.2 this is in `ProcessService[Impl]`; from 3.3.1 it is in `ReRunWorkflowCommandHandler` and `WorkflowInstanceRecoverFailureTaskTrigger`.
- 1.3.9 `ProcessService.java:638–653`; 3.2.0 `ProcessServiceImpl.java:827–845`; 3.2.1/3.2.2 `ProcessServiceImpl.java:765–788` share the failed/suspended recovery case.
- Execute-task has a reviewed increment in the three 3.2.x releases only. `3.4.2`/`3.4.3` reuse the counter, so their results do not advertise an execution baseline. `3.3.1`–`3.4.1` have no master handler and are blocked by the action capability preflight.
- A missing/non-positive marker never becomes an inferred baseline.

A greater marker proves a later execution of the same instance. Concurrent controls can also advance it; this is not server-side command correlation or an atomic compare-and-swap guarantee.

## Preview and parameter limits

Dry-run captures the prepared request before mutation. Dependencies and server state can change between preview and a later invocation; applying the same CLI input is not an atomic replay of a reviewed server snapshot. Creation supplies workflow name, desired release state, task count and task names as semantic preview facts.

Fresh run/backfill options follow their existing CLI/context/default resolution. They do not automatically inherit worker, tenant or environment from an old schedule/instance. Backfill acceptance, instance discovery and completion of the expected dates are distinct observations. These operations do not roll back external data effects or promise exact restore of native runtime state.

## Regression coverage

Execution-round and trigger-resolution tests cover real identities, pending
receipts, failed post-mutation reads and later-round completion.
`tests/services/workflow/test_mutation_progress.py` covers completed and uncertain
stages; execution-preview and workflow-instance tests cover prepared effects,
known identities and actionable recovery guidance. Generated workflow tests
retain exact native request and response expectations.

These tests must preserve the distinction between request acceptance, known
mutation effects and observed runtime completion. Passing development tests or
regenerating exact packages does not replace artifact-bound server acceptance;
historical receipts remain attached to their original wheel.
