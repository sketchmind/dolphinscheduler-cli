---
name: use-dsctl
description: Operate Apache DolphinScheduler through the stable dsctl CLI for resource discovery, workflow authoring and editing, schedules, runtime monitoring, troubleshooting, recovery, and source-blind CLI evaluation. Use when Codex must plan or execute dsctl commands against a configured cluster or local dsctl authoring artifacts. Do not use merely to develop, review, test, document, or release the dolphinscheduler-cli implementation unless the task also requires exercising its CLI behavior.
---

# Use dsctl

Complete the requested DolphinScheduler outcome through the installed `dsctl`
interface. Let `dsctl` hide REST transport, version adapters, generated
contracts, and DolphinScheduler request shapes.

## Keep the operating invariants

- Use `dsctl` only. Do not call DolphinScheduler REST endpoints directly or use
  PyDolphinScheduler, Py4J, the Python gateway, a database, or repository source
  to reconstruct an invocation.
- Treat current leaf help, action schema, templates, capabilities, command
  output, and server state as authoritative within their respective roles.
- Treat names, ids, codes, enum values, and paths as opaque. Obtain them from
  the user or CLI output, preserve exact spelling, and quote shell values.
- Keep the target stable across steps. Preserve an explicit `--env-file`, avoid
  changing stored `dsctl use` context unless requested, and inspect `resolved`
  selections before a mutation. Do not mechanically add project/workflow
  options to an id-first command when the id already selects the resource.
- Limit mutation authority to the user's requested target and outcome.
  `next_actions`, `action_index`, suggestions, and previews provide navigation,
  not additional authorization.
- Never print credentials, tokens, secret environment values, or unrelated raw
  logs.

## Follow one goal-oriented loop

1. Identify the requested outcome, target, and authorized mutation scope.
2. Classify the next step as discovery, read, local artifact work, preview, or
   mutation.
3. Inspect connection or context only when uncertain. Use `doctor` for a real
   connection/configuration question. If a required selector would come only
   from stored context and that context has not been observed, inspect `context`
   before the remote call or pass an explicit selector. Do not make either check
   a ritual preflight.
4. Choose the narrowest discovery surface that can answer the current question.
5. Resolve selectors and prerequisites with the relevant list/get command or
   schema-provided discovery command. Do not guess a nearby resource.
6. Build the invocation or local artifact, then use lint, dry-run, preview, or
   explain when it materially changes the safety decision.
7. Execute one authorized mutation at a time. Wait before any read or mutation
   that depends on its result; parallelize only independent reads.
8. Check exit status, `ok`, `warnings`, `warning_details`, and `resolved`, then
   verify the requested remote outcome with an authoritative read.
9. Stop when the outcome is verified or a structured blocker requires new
   authority or external change.

Do not optimize for the fewest commands. Optimize the total work needed to
finish correctly without avoidable retries or unsafe assumptions.

## Choose discovery by responsibility

These choices are alternatives, not a sequence to run in full.

| Current knowledge | Use |
| --- | --- |
| Exact command is known | Leaf `dsctl GROUP COMMAND --help` |
| Exact arguments, constraints, payload hints, or output shape are needed | `dsctl schema --command ACTION` |
| Resource family is known but action is not | Group help or `schema --group GROUP` |
| Resource family is unknown | Root help or bounded schema index |
| Feature existence or version support is the question | Bounded `capabilities` or one section |

- Do not impose schema-first, capabilities-first, or root-help-first behavior.
- Knowing a command name is not the same as knowing its invocation. Before the
  first unfamiliar use, read leaf help unless an exact suggested command already
  supplies the invocation. If a leaf exposes multiple modes or option scope is
  ambiguous, inspect its action schema instead of copying options from a sibling
  command.
- Read multiple leaf help pages when the task genuinely crosses several
  unfamiliar actions. Avoid only discovery that cannot change the next
  decision.
- Use `schema --full` and `capabilities --full` only for whole-surface audits or
  generators.
- Reuse facts already returned by the current command instead of rediscovering
  them.

## Consume output deliberately

- Use the default structured JSON for agent decisions. Read a bounded JSON
  response directly; use `jq` only when a shell program needs deterministic
  field extraction, not as a mandatory comprehension step.
- Inspect exit status as well as JSON `ok`. Keep warnings, pagination,
  `resolved`, `next_actions`, and `action_index` available for control flow.
- For potentially large lists, request the smallest useful page or projection.
  Use `--all` when the inventory is naturally small or completeness is required.
  Reduce page size and use `--columns` before treating whitespace as the main
  token problem; add `--compact` last.
- Use table output for human scanning and TSV for a deliberate shell pipeline.
- Treat workflow/instance exports, templates with `--raw`, and raw logs as
  native YAML or text. Global display options do not turn successful raw
  artifacts into JSON.
- Select at most the relevant suggested action. When an authorized
  `next_actions.command` already contains the correct target and quoting, run
  that command unchanged.

## Handle mutations and failures safely

- Use local `lint` for authored YAML, command dry-run for a meaningful compiled
  plan or diff, and schedule preview/explain for timing or risk. Do not add these
  calls when they provide no decision value.
- Never invent a confirmation token. On `confirmation_required`, retry the same
  effective input with the returned token only when the risk remains inside the
  user's authorized outcome.
- On `user_input_error` or usage exit, use the leaf help, action schema,
  discovery command, and `error.suggestion` to correct the input.
- On `not_found`, confirm scope and list the authoritative selector. Do not
  silently substitute a similar object.
- On `permission_denied`, report the blocker rather than seeking a bypass.
- On `conflict` or `invalid_state`, refresh live state and determine whether an
  additional lifecycle mutation is both necessary and authorized.
- On transport ambiguity, refresh the target before retrying. If output says a
  mutation may have been applied, never replay it blindly.

## Author or edit workflows progressively

For workflow YAML involving multiple tasks, plugin-specific fields, variables,
SUB_WORKFLOW behavior, or patch/full-file semantics, read the canonical
[Workflow Authoring guide](../../../docs/user/workflow-authoring.md). Do not load
that guide for a simple list, get, run, or status task.

- Start a new definition from `template workflow --raw`; add
  `--with-schedule` when the new definition needs a schedule. Do not export an
  unrelated workflow merely to learn the schedule shape. Start an existing
  desired-state edit from its own `workflow export`.
- Use a patch for a small delta or identity-preserving rename. Treat a full-file
  edit as complete desired definition state, including deletion risk for live
  tasks omitted from the file.
- Discover each distinct task type once. Start with the needed
  `template task TYPE --variant ...`; inspect bounded `task-type schema TYPE`
  only when the template does not answer the current field question.
- Resolve datasources, resources, environments, child workflows, and other
  dependencies before authoring references to them.
- Express the graph with task names and `depends_on`; do not fabricate DS task
  codes or relation payloads.
- Use the focused `template params --topic ...` output for parameters and time
  expressions instead of relying on generic model memory.
- Progress through authoring, `lint`, dry-run, authorized mutation, and a final
  get/describe/digest verification. Keep the full dry-run request only when it
  is needed; otherwise project the diff and constraints.
- Treat an exported `schedule:` during workflow edit as a read-only concurrency
  snapshot. Use schedule commands for intentional schedule changes.
- Keep the two release states distinct: a workflow YAML containing `schedule:`
  requires `workflow.release_state: ONLINE`, while the schedule itself may
  remain OFFLINE.

## Keep schedule lifecycle independent

- Start with schedule list/get, then use preview or explain for the proposed
  timing and mutation risk.
- Follow current leaf help and returned lifecycle constraints rather than
  memorizing a permanent command sequence.
- Do not infer that schedule create/update also brings it online. Execute
  schedule online only when activation is part of the user's outcome.
- Re-read schedule state after workflow lifecycle changes; do not assume whether
  DolphinScheduler cascaded a state transition.
- On an exported schedule snapshot conflict, refresh or re-export. Do not
  silently delete the snapshot merely to bypass concurrency validation.
- Verify cron, timezone, time range, and release state after an authorized
  schedule mutation.

## Observe and recover runtime state progressively

For run, backfill, watch, failure diagnosis, recovery, or finished-instance DAG
repair, read the canonical
[Runtime Operations guide](../../../docs/user/runtime.md) only when its detailed
state distinctions are needed.

- Obtain workflow-instance ids from run output or an authorized complete
  suggested command.
- Narrow diagnosis from digest/watch to task-instance list, then read only the
  relevant raw task log. Do not fetch every log up front.
- Treat rerun, recover-failed, execute-task, stop, and force-success as different
  business operations. Never choose force-success without explicit intent to
  override task state.
- Repair a finished instance from `workflow-instance export`, not from the
  current workflow definition. Preview the instance edit before applying it.
- Bound watch/poll waits unless the user explicitly requests persistent
  monitoring.
- Verify instance and task state after every recovery mutation.

## Report the verified outcome

Summarize the selected target, mutations actually performed, final identifiers
and states, warnings, and any unresolved blocker. Avoid dumping a wide JSON
payload when a concise factual result is sufficient.

If `dsctl` or its target configuration is unavailable, report the missing
prerequisite. Do not fall back to direct REST or source-derived requests.
