# Workflow Authoring

Use this reference for workflow creation, desired-state editing, patching, and
multi-task YAML.

## Select the artifact

1. Start a new definition with `template workflow --raw`, saving raw YAML rather
   than the JSON success envelope. Use
   `template workflow --with-schedule --raw` when it includes a schedule.
2. Start an existing desired-state edit from that workflow's own
   `workflow export`.
3. Use a patch for a small delta or identity-preserving rename. Treat a
   full-file edit as the complete desired definition; omitted live tasks are
   deletion candidates.
4. For an existing definition, record the original workflow/schedule release
   states and the requested final states before any offline transition.

Continue when the artifact source and edit mode match the requested outcome.

## Ground the model

- Ground dependency names, IDs/codes, paths and authored values in the user's
  input or exact CLI facts. Use `task-type schema TYPE --field FIELD_PATH` and
  its choice source when a specific dependency field is unknown. Datasource
  fields accept the selected schema's ID or exact name; dry-run resolves names
  and checks type/access. CLI validation does not certify worker prerequisites.
- For an ordinary new task, start with `template task TYPE --raw`; its default
  is one active fragment with routine optional settings in comments. For an
  unfamiliar mode, `template task TYPE` returns the body and selected-profile
  scenarios in `data.template.variants`, plus an executable raw-artifact command.
  Choose an advertised `--variant NAME` when its coupled fields or side effects
  match the request; follow the schema pointer only for unresolved constraints.
- An advertised `output` scenario couples publication code/result columns with
  the OUT declaration; also validate the downstream IN binding against that
  exact profile. SHELL/PYTHON `resource` couples a script with its attached FILE
  reference, and DEPENDENT `task-dependency` selects a task target rather than a
  whole definition. Availability comes from current template metadata. Ordinary
  IN parameters use the main comments and parameter topic, rather than a second
  parameter template. On a removed selector error, select a current advertised
  scenario rather than retrying an old alias.
- Use the focused `template params --topic ...` output for parameters and time
  expressions. Task-local parameters belong in `task_params.localParams`, not
  at the task root. Keep example workflow globals only when the composed tasks use
  them and permit them; a task's parameter-free prerequisite applies to the
  whole workflow.
- Express the graph with task names and `depends_on`; let dsctl generate native
  task codes and relations.
- For expression branching, start with `template workflow --example branch`
  (SWITCH). For success/failure branching, use `template task CONDITIONS --raw`
  and its task-name and edge guidance. Child workflows and OUT-to-IN parameters
  have `child` and `output` workflow examples; verify the producer and consumer
  against the selected task schema and parameter topic.
- Retain native fields from the selected workflow's existing baseline when
  editing. A typed task template owns only its reviewed mode; invalid typed
  input is an error, while explicit opaque authoring needs an advertised mode.
  Exported YAML alone cannot recreate preservation that requires a live baseline.
- Keep schedule states distinct: a YAML definition containing `schedule:` uses
  `workflow.release_state: ONLINE`, while the attached schedule may remain
  OFFLINE.
- Treat an exported `schedule:` block as a read-only concurrency snapshot during
  workflow editing. Apply intentional schedule changes through schedule
  commands.

Continue when dependencies and plugin-specific values are grounded and
the intended DAG, parameters, and release states are explicit.

## Validate, apply, and verify

1. Run `lint workflow` for a desired-state file, `lint workflow-patch` for a
   definition patch, or `lint workflow-instance-patch` for an instance patch.
   Resolve every error. Deferred datasource binding still requires dry-run;
   local model, graph and execution constraints must already pass lint. Valid
   patch lint covers locally knowable checks; its deferred baseline checks
   remain obligations of the matching edit preview.
2. Run the matching workflow command with `--dry-run` and inspect its diff,
   constraints, and schedule preview when present. Recheck if the artifact or
   remote baseline changes before apply; preview is an observation, not a lock.
3. When a mutation is authorized, apply it once after lint and dry-run succeed,
   then verify returned authoritative readback or use the narrowest get,
   describe, digest or export view that proves the requested definition/state.

If preview reports an ONLINE state constraint, it can still contain a useful
diff. Take the definition offline within the intended change scope and refresh
an exported schedule snapshot without losing the authored delta. Preview and
apply against that baseline, then restore only the lifecycle states required
by the requested final outcome. Workflow online does not reactivate a schedule.
For scheduled deployment, continue through the [schedule lifecycle](schedules.md)
after definition verification; manual running and schedule activation are
separate outcome branches.

Complete workflow authoring when lint and dry-run cover the intended artifact
and any applied mutation has an authoritative read matching the requested DAG,
parameters, and release state.
