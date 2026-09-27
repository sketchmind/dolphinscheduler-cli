---
name: dsctl
description: dsctl operations for Apache DolphinScheduler cluster resources, workflow authoring, schedule lifecycle, and workflow-run monitoring, diagnosis, or recovery.
---

# Use dsctl

## Run a closed loop

1. **Frame.** Resolve the requested outcome, target connection, and authorized
   mutation scope. Keep the selected target stable; carry any supplied
   `--context` or `--env-file` through later commands and preserve stored
   context unless changing it is part of the request. Treat suggested actions
   as navigation within that authority. Continue when the desired state, target,
   and permitted mutations are explicit.
2. **Ground.** Resolve names, ids, codes, enum values, and paths from the user or
   current CLI output, preserving exact spelling and shell quoting. Treat
   commands from branch references, complete returned commands, current
   installed help/schema, and successful prior invocations as grounded. Use
   just-in-time discovery for a concrete unknown that blocks the next command.
   Load the matching branch reference below. Continue when the invocation has
   no unresolved syntax, selector, or constraint.
3. **Act once.** Execute the grounded read, local validation, preview, or
   authorized mutation. Parallelize independent reads; serialize mutations and
   dependent work. Continue when the process exit status and complete result
   are available.
4. **Observe.** Interpret the result and compare authoritative state with the
   requested outcome. Use returned verified readback when it proves that state;
   otherwise perform the narrowest required read. Acceptance of a trigger is
   not proof of completed execution. Finish when the outcome is
   verified or a structured blocker requires new authority or external change;
   otherwise carry the new facts back to **Ground**.

Prefer the lowest-total-work route that closes the loop with authoritative
verification.

## Use just-in-time discovery

Enter this table when a concrete unknown blocks the next invocation. Choose the
matching row and carry the returned facts into later iterations of the closed
loop.

| Current unknown | Use |
| --- | --- |
| Syntax or options for a known action | Leaf `dsctl GROUP COMMAND --help` |
| Machine-readable constraints, modes, payload, or output contract | `dsctl schema --command ACTION` |
| Available actions in a known resource family | `dsctl schema --group GROUP`; group help for prose |
| Relevant resource family | Root help or bounded schema index |
| Known action version availability | `dsctl capabilities --action ACTION` |
| Product/version inventory | Default capabilities or one bounded section |

- Inspect `context` when an unobserved stored selector would determine the
  target. Use `doctor` when connection or configuration state is the unknown.
- Automatic API-contract matching can admit specific reads without identifying
  the exact server release. Writes, exports and authoring need an exact target;
  inspect [version selection](references/configuration.md#version-selection)
  when that distinction blocks the next operation.
- Prefer explicit project/workflow selectors or complete returned commands
  within one task. For independent tasks sharing a directory, retain each
  task's `DSCTL_CONTEXT` or absolute `DSCTL_ENV_FILE` in every child process.
  When setup, source precedence, or persistent defaults are the unknown, read
  [Connection selection](references/configuration.md). Complete that branch
  when `context` reports the intended target and project; use `doctor` only
  when remote readiness must also be established.
- For id-first commands, let the id select the resource and add project or
  workflow options when leaf help or action schema requires them.
- Treat a complete returned command as grounded when its resolved target and
  action fit the authorized scope.

## Consume output as control flow

- Read bounded default JSON directly for agent decisions. Introduce `jq` when
  an extracted field feeds another shell command.
- Bound lists with filters, the smallest sufficient page and selected columns;
  use `--all` for naturally small inventories or required completeness.
  Select column names already observed in that action's result or schema.
  Keep short detail and mutation replies intact instead of guessing projections.
  `json-compact` encodes declared business lists at their existing row path as
  `columns` plus positional `rows`, including empty/single-row results. Match
  each value to its column; nested values retain JSON types. Default compact
  output keeps all returned fields. Its list `--columns` accepts top-level
  fields or `*`; select a parent or use `json` for dotted paths. Other views
  retain their structures. Navigation `scope` and `target.field` refer to
  decoded logical rows and fields; `targets: "all"` covers returned rows.
  Check page coverage before claiming that a resource is absent or an inventory
  complete; index coverage does not establish remote population coverage.
- Use table output for human scanning and TSV for an intentional shell
  pipeline.
- Parse workflow and instance exports, `--raw` templates, and raw logs as
  native YAML or text.
- Inspect exit status together with `ok`, optional structured `warnings`, and
  `resolved`. Failed doctor/lint/preview results can retain a full report in
  `data`. For a CI success condition, use `watch --exit-status`; ordinary watch
  only waits for a terminal state. Preserve relevant `next_actions` and
  `action_index` for navigation.
- Redact credentials, tokens, and secret environment values; report only
  task-relevant log content.

## Load branch guidance when needed

- Before authoring or editing workflow YAML, read
  [Workflow authoring](references/workflows.md). Complete that branch after the
  exact artifact passes lint and dry-run and any applied mutation is read back.
- Before creating, updating, activating, deactivating, or deleting a schedule,
  read [Schedule lifecycle](references/schedules.md). Complete that branch after
  the requested configuration and release state, or absence after deletion, are
  read back.
- When the task needs a new execution's identity or completion, or before
  backfill, recovery, force-success or finished-instance DAG repair, read
  [Runtime operations](references/runtime.md). For conditional
  diagnosis, load it when the observed instance state indicates failure.
  Complete that branch after the requested instance and task states are read
  back. A trigger-only request, standalone list/get, and observation of an
  already identified instance use the closed loop and leaf help.
- After a non-success result or an ambiguous mutation outcome, read
  [Structured errors](references/errors.md) before choosing the next command.

## Report the verified outcome

Report concise verified facts: target, performed mutations, final identifiers
and states, warnings, and blockers.
