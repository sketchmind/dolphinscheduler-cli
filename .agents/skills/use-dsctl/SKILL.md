---
name: use-dsctl
description: dsctl operations for Apache DolphinScheduler cluster resources, workflow authoring, schedule lifecycle, and workflow-run monitoring, diagnosis, or recovery.
---

# Use dsctl

## Run a closed loop

1. **Frame.** Resolve the requested outcome, exact target, and authorized
   mutation scope. Keep the selected target stable; carry any supplied
   `--env-file` through later commands and preserve stored context unless
   changing it is part of the request. Treat suggested actions as navigation
   within that authority. Continue when the desired state, target, and permitted
   mutations are explicit.
2. **Ground.** Resolve names, ids, codes, enum values, and paths from the user or
   current CLI output, preserving exact spelling and shell quoting. Treat
   commands from branch references, complete returned commands, successful
   prior invocations, and unambiguous familiar leaf syntax as grounded. Use
   just-in-time discovery for a concrete unknown that blocks the next command.
   Load the matching branch reference below. Continue when the invocation has
   no unresolved syntax, selector, or constraint.
3. **Act once.** Execute the grounded read, local validation, preview, or
   authorized mutation. Parallelize independent reads; serialize mutations and
   dependent work. Continue when the process exit status and complete result
   are available.
4. **Observe.** Interpret the result, read authoritative state after a mutation,
   and compare it with the requested outcome. Finish when the outcome is
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
| Available actions in a known resource family | Group help or bounded group schema |
| Relevant resource family | Root help or bounded schema index |
| Feature or version support | Bounded capability section |

- Inspect `context` when an unobserved stored selector would determine the
  target. Use `doctor` when connection or configuration state is the unknown.
- For id-first commands, let the id select the resource and add project or
  workflow options when leaf help or action schema requires them.
- Treat a complete returned command as grounded when its resolved target and
  action fit the authorized scope.

## Consume output as control flow

- Read bounded default JSON directly for agent decisions. Introduce `jq` when
  an extracted field feeds another shell command.
- Bound list content with filters, the smallest sufficient page, and selected
  columns. Use `--all` for naturally small inventories or required
  completeness; apply `--compact` to bounded JSON.
- Use table output for human scanning and TSV for an intentional shell
  pipeline.
- Parse workflow and instance exports, `--raw` templates, and raw logs as
  native YAML or text.
- Inspect exit status together with `ok`, `warnings`, `warning_details`, and
  `resolved`. Preserve relevant `next_actions` and `action_index` for navigation.
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
- Before backfill, recovery, force-success, or finished workflow-instance DAG
  repair, read [Runtime operations](references/runtime.md). For conditional
  diagnosis, load it when the observed instance state indicates failure.
  Complete that branch after the requested instance and task states are read
  back. Plain run, list, get, and watch operations use the closed loop and leaf
  help.
- After a non-success result or an ambiguous mutation outcome, read
  [Structured errors](references/errors.md) before choosing the next command.

## Report the verified outcome

Report concise verified facts: target, performed mutations, final identifiers
and states, warnings, and blockers.
