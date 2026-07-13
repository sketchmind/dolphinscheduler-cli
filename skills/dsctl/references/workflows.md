# Workflow Authoring

Use this reference for workflow creation, desired-state editing, patching, and
multi-task YAML.

## Select the artifact

1. Start a new definition with `template workflow --raw`. Use
   `template workflow --with-schedule --raw` when it includes a schedule.
2. Start an existing desired-state edit from that workflow's own
   `workflow export`.
3. Use a patch for a small delta or identity-preserving rename. Treat a
   full-file edit as the complete desired definition; omitted live tasks are
   deletion candidates.

Continue when the artifact source and edit mode match the requested outcome.

## Ground the model

- Resolve datasources, resources, environments, child workflows, and other
  dependencies before placing their identifiers in YAML.
- For each distinct task type, run the needed
  `template task TYPE --variant ...` directly. Its successful output grounds the
  task type and base shape. Inspect a bounded `task-type schema TYPE` when the
  template leaves a required field unresolved.
- Use the focused `template params --topic ...` output for parameters and time
  expressions.
- Express the graph with task names and `depends_on`; let dsctl generate native
  task codes and relations.
- Keep schedule states distinct: a YAML definition containing `schedule:` uses
  `workflow.release_state: ONLINE`, while the attached schedule may remain
  OFFLINE.
- Treat an exported `schedule:` block as a read-only concurrency snapshot during
  workflow editing. Apply intentional schedule changes through schedule
  commands.

Continue when every dependency and plugin-specific value has CLI evidence and
the intended DAG, parameters, and release states are explicit.

## Validate, apply, and verify

1. Run `lint workflow` against the exact artifact that will be applied.
2. Run the matching workflow command with `--dry-run` and inspect its diff,
   constraints, and schedule preview when present.
3. When a mutation is authorized, apply it once after lint and dry-run succeed,
   then read the workflow back with the narrowest get, describe, or digest view
   that proves the requested definition and state.

Complete workflow authoring when lint and dry-run cover the intended artifact
and any applied mutation has an authoritative read matching the requested DAG,
parameters, and release state.
