# Compose workflows from main templates

First select the target `DS_VERSION`, then run `dsctl template task TYPE --raw`.
Each task type's default template provides one active configuration, with brief
comments for ordinary IN parameters or optional fields. Replace existing fields
instead of appending duplicate YAML keys. When adding `localParams` to
SHELL/PYTHON, move `command` to `task_params.rawScript`.

Only scenarios that need a complete combination of related settings appear in
`data.template.variants`: for example, `output` shows producer code and an OUT
declaration, `resource` shows an attachment name and script invocation, and
DEPENDENT's `task-dependency` changes the reference target. DVC's
upload/download/init, EMR's add-steps, and HTTP's post-json also represent
specific business operations. The default needs no `--variant`; `minimal`,
`params`, and selectors that were only aliases have been removed. SQL pre/post
fields use brief hints in the default template without adding a scenario
selector. DATASYNC `raw-json` is opaque input restricted by a selector; it does
not use normal typed field validation or pass through arbitrary AWS requests.

Use the installed CLI below to obtain **complete workflows projected for the
selected DS_VERSION**, without copying a separate YAML file from the repository.
`basic` is identical to omitting `--example`. All four compositions support
`--with-schedule`; an attached schedule remains inactive by default. The
output/branch examples are explicitly rejected on 1.3.9. These examples have
passed local parsing, model, and graph compilation checks; they do not prove
that workers, plugins, credentials, resources, or referenced workflows exist.
Replace the example identities and check fields against the selected version's
schema. Carry an explicitly selected `--env-file FILE` into discovery, lint, and
dry-run commands as well; navigation supplied by the template preserves it.

## OUT producers and downstream IN parameters

Save as `output-flow.yaml`:

```bash
dsctl template workflow --example output --raw > output-flow.yaml
```

On 3.3.1+, downstream tasks must declare IN parameters; writing `${row_count}`
in a successor script alone does not establish the same input contract. This
output composition is unsupported on 1.3.9, and older versions also have
different log output markers and parameter override rules. Use
`dsctl template params --topic output` to check the current version's markers
and `--topic context` to check precedence. Put shared values in
`workflow.global_params` and values used by only one task in that task's
`localParams`. Do not assume that values with the same name in both locations
override each other in the same order across all versions.

## Branches and joins

Save as `branch-flow.yaml`:

```bash
dsctl template workflow --example branch --raw > branch-flow.yaml
```

SWITCH automatically generates edges to matching/default targets. Targets must
appear in the same `tasks[]`, and join nodes must explicitly list their
predecessors. On 2.0.0–3.2.1, SWITCH reads only workflow globals and the
incoming runtime varPool; **it does not read that node's localParams**. The CLI
branch example deliberately uses a global parameter so it does not suggest that
older versions can route using local parameters. Only 3.2.2+ uses the prepared
map that includes localParams. When using dynamic upstream output as a condition
input, also establish a predecessor edge to SWITCH with `depends_on`. SWITCH is
absent on 1.3.9.

If a branch depends on a task's SUCCESS/FAILURE within the same instance, use
`dsctl template task CONDITIONS --raw`. Its template explains that incoming
edges from predicate tasks and outgoing edges to successNode/failedNode are
added automatically; do not duplicate them with the same `depends_on`. The
template's optional compatibility field `bizdate` does not participate in state
predicate evaluation.

## Call a child workflow in the same project

First prepare a separate `child.yaml`, give the child its own name, and adjust
its tasks as needed. Then create it and bring it online:

```bash
dsctl template workflow --example basic --raw > child.yaml
```

Parent workflow `parent.yaml`:

```bash
dsctl template workflow --example child --raw > parent.yaml
```

The child name or number in the parent file is a placeholder. Run
`dsctl workflow list --project PROJECT` /
`dsctl workflow get WORKFLOW --project PROJECT` to obtain the real child code
and check the child workflow, its reference closure, and its online status. On
1.3.9, use `childWorkflowName` from the same project instead; the CLI resolves
its id. Do not put a code in the name field. The current authoring checks for
existence and cycles cannot guarantee that a child graph remains ready after
later changes.

SUB_WORKFLOW local parameters do not form a uniform child input map across
versions. 3.3.x forwards only parent startup parameters; 3.4.x forwards parent
globals/startup/varPool, and localParams are not child inputs. 2.0.x also has
differences in same-name overrides and outputs on normal completion versus
cancellation. Run `dsctl template params --topic context` and consult the child
task schema for the exact rules instead of mechanically copying every IN/OUT
declaration into the parent template. Tenant, worker group, and environment
belong to runtime/schedule or task-level configuration; select them through the
corresponding schema. A child workflow's existence does not prove that its
worker environment is installed.

## Wait for a workflow or task in another project

Save as `dependency-flow.yaml`:

```bash
dsctl template workflow --example dependent --raw > dependency-flow.yaml
```

Run `dsctl project list`, `dsctl workflow list --project PROJECT`, and
`dsctl task list --project PROJECT --workflow WORKFLOW` in order. Modern
templates take **definition codes**, not instance ids; use `depTaskCode: 0` for
the whole workflow. To select a specific task, use its task code and
`DEPENDENT_ON_TASK`. 1.3.9 uses `projectName` / `workflowName` / `taskName`, and
the compiler converts the whole-workflow scope to ALL. The main template
provides replacement examples for both scopes.

Choose `cycle` / `dateValue` as a pair from the exact enums listed in the
schema; their values do not undergo parameter substitution. Date windows are
interpreted in the DS schedule/runtime context, which does not mean they always
query the current moment. Cross-workflow references do not automatically
generate DAG edges in this file. You must still confirm the target identities,
target project permissions, historical instances, and expected states. Use the
failure controls available from 3.2.0+ and parameterPassing available from
3.2.1+ only as shown in the version-specific templates.

## Validation, dependencies, and changes

```bash
dsctl lint workflow output-flow.yaml
dsctl workflow create --file output-flow.yaml --project PROJECT --dry-run
```

For the other files, likewise run lint first, then a dry-run against the target.
For resources, use `resource list` without --dir to obtain the FILE root, then
follow the schema to convert the file's fullName into a root-relative name that
retains the leading /. Check the script path in the worker's download directory.
For datasources, use a positive id and matching type; a connection test does not
replace checks of business tables and permissions. Obtain external objects with
no CLI discovery command, such as ARNs, Pigeon jobs, and Zeppelin paragraphs,
from their own systems. Put PIGEON's p_host in the parent
workflow.global_params.

The outer task `timeout` is measured in minutes; 0 disables it. When enabled,
the default WARN behavior only emits a warning. The effects of FAILED/WARNFAILED
on termination and remote cancellation still depend on the task. HTTP
connectTimeout and gRPC deadline remain measured in milliseconds, and other
plugins retain their own units in seconds; the outer timeout unit does not
change them. The exact schema remains authoritative for special constraints such
as those for KUBEFLOW/PYTORCH.

For an existing workflow, first read/export its baseline, then use
`dsctl template workflow-patch --raw` to edit explicit fields. By default, it
changes only description. An instance patch defaults to changing only the
selected task's script and does not set workflow timeout/global_params. Remove
unneeded operation blocks, run the corresponding edit with `--dry-run`, then
apply. Objects may change between preview and apply; reverting a definition does
not roll back external side effects that have already occurred.
