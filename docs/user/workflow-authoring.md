# Workflow Authoring

This guide describes the stable YAML authoring surface used by
`dsctl workflow create`, `dsctl workflow edit`, `dsctl lint workflow`, and
`dsctl template`.

Use the narrowest command output needed for the current authoring decision.
Start a new workflow without a schedule using:

```bash
dsctl template workflow --raw > workflow.yaml
```

Or start one with an attached schedule using:

```bash
dsctl template workflow --with-schedule --raw > workflow.yaml
```

Then validate the selected artifact before applying it:

```bash
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project PROJECT --dry-run
```

Replace `PROJECT` with the target project. A saved context can supply the
project when `--project` is omitted. For complete producer/consumer,
branch, child-workflow, and dependency compositions, see
[Task examples](task-examples.md).

Inspect task and parameter details only when the selected workflow needs them:

```bash
dsctl template task SHELL --variant resource --raw
dsctl task-type schema SHELL --field 'task_params.resourceList[].resourceName'
dsctl template params --topic output
```

For an existing workflow, choose the edit mode that matches the intended
change:

```bash
dsctl workflow export daily-etl --project etl-prod > workflow.yaml
dsctl template workflow-patch --raw > patch.yaml
dsctl lint workflow-patch patch.yaml
dsctl workflow edit daily-etl --project etl-prod --patch patch.yaml --dry-run
```

These are decision branches, not a preflight list to run in full. Use leaf help
for a known command, action schema only when exact constraints or payload facts
are still needed, and capabilities only when feature or version support is the
question.

## Edit a scheduled workflow

Export the current definition and preview the intended edit before taking it
offline. A preview can return a useful diff together with an ONLINE state
constraint; resolve that constraint before applying. Taking the definition
offline also takes its attached schedule offline. Exported schedule fields are
a concurrency snapshot, so refresh the export after that state change before
reapplying a full-file edit.

After applying and verifying the definition, choose the requested outcome:
leave it as a draft, bring it online for manual runs, or resume scheduled
execution. Bringing the definition online does not reactivate its schedule.
Inspect the attached schedule and explicitly activate it only when resuming
scheduled execution is intended. `schedule explain` reviews configuration and
constraints; `schedule preview` shows expected fire times. Neither proves an
actual run occurred.

Lint reports independent local findings together, including task parameters,
duplicate names and invalid graph references. Patch lint checks the artifact
without reading the remote baseline; use `lint workflow-instance-patch` for an
instance patch. Remote identities, permissions and state are checked when the
matching edit command prepares its preview. A successful lint does not prove
that a mutation will be accepted.

## Task datasource references

In reviewed datasource-backed task parameters, YAML integers select native
datasource IDs and strings select exact datasource names. For example:

```yaml
task_params:
  type: MYSQL
  datasource: warehouse
  sql: SELECT 1
  sqlType: 0
```

Use the selected task template for the complete fragment. The reference rule
applies to SQL, PROCEDURE, DATA_QUALITY, ALIYUN_SERVERLESS_SPARK and REMOTESHELL,
and to the datasource modes of K8S, SAGEMAKER and ZEPPELIN where the selected
version supports them. `dsctl task-type schema TYPE --field task_params.datasource`
shows the applicable contract and discovery command.

Creation and edits resolve names to real IDs and verify datasource type before
building requests. A quoted numeric value such as `"123"` means the name `123`;
unquoted `123` means ID 123. Missing, ambiguous or incompatible references fail
with actionable errors. Repeated references share lookups within that operation.
Local lint defers remote binding and omits `data.compilation` when names need
resolution, while still checking models, the graph and local execution rules
such as task retry/timeout and workflow parameter restrictions.
Existing unchanged datasource usage does not
acquire a new lookup permission requirement, and export retains native IDs.
Name resolution does not attest worker configuration or datasource connectivity.

## Discovery Flow

Use `dsctl task-type list` when you need the live DS task-type catalog for the
configured cluster and current user. Use `dsctl template task` when you need
the compact local template catalog. Then use `dsctl task-type get TYPE` for the
optional per-type authoring summary and `dsctl task-type schema TYPE` directly
for bounded fields, state rules, discovery commands, and returned value names.
Use `--field FIELD`, `--json-schema`, or `--compile-mappings` only for the detail
needed by the current task; quote field paths containing `[]`. The JSON Schema
view is a complete JSON document and therefore does not support table/TSV or
`--columns`. `--full` is the expanded compatibility view.

`dsctl template workflow` returns a minimal full workflow inside the standard
JSON envelope. Use `dsctl template workflow --raw > workflow.yaml` when you want
only the YAML file content. The template is intentionally small so generated
files do not include unrelated optional fields.

Workflow and task templates use the same selected target as local lint and task
schemas: an explicit `DS_VERSION`, or a valid discovery cache for the configured
URL and token. With no target configuration they use the `3.4.1` baseline. If an
automatic target has no valid cache, run `dsctl doctor` or set `DS_VERSION`;
template commands do not probe the server. An explicit `--env-file` selects an
isolated profile for both JSON and raw YAML output.

The workflow skeleton retains the same authoring structure across versions but
omits unsupported execution modes. JSON reports its
target in `resolved.ds_version`; agents can use the emitted fields without
selecting version-specific YAML formats themselves.

The two SHELL tasks, `extract` and `load`, both use the example `bizdate` global.
Keep globals for values shared across tasks; remove the example global if it is
not needed. A value used by one task belongs in that task's `localParams`.
The skeleton keeps `release_state` explicit and shows optional `description`,
workflow `timeout` (minutes), and supported `execution_type` as comments. Task
runtime controls and task-specific graph rules remain in the task templates and
schemas, so each workflow node does not repeat a block of unused settings.

Use `dsctl schema --command workflow.create` to inspect the YAML contract as well
as the command arguments. Its `command.payload.yaml_schema` describes all
workflow metadata and schedule fields, including defaults and exact enum values.
The task entry is a structural outline: follow `task_authoring` for task fields
and rules, then lint the complete graph. JSON Schema alone does not validate task
semantics, name references, or cross-field rules.

Choose the entry point for the information being authored:

| Information | Owner and discovery |
| --- | --- |
| Name, project, description, timeout, globals, execution and release state | `workflow:`; `dsctl schema --command workflow.create` |
| One task's parameters, runtime controls and graph rules | `tasks[]`; `dsctl task-type schema TYPE` or `dsctl template task TYPE --raw` |
| Parameter property shape, precedence or output | `dsctl template params --topic property`, `context` or `output`, respectively |
| Attached cron schedule, date window and schedule settings | `schedule:`; `dsctl template workflow --with-schedule --raw` |
| Tenant and other settings for a manual run | `dsctl schema --command workflow.run` |
| Tenant and other settings for a separately created schedule | `dsctl schema --command schedule.create` |

Neither `workflow:` nor `schedule:` accepts a tenant field. Use `workflow run
--tenant CODE` or, where supported, `schedule create --tenant-code CODE`; enabled
project preferences can supply defaults. The create schema explains this boundary
and links to the selected run and schedule contracts.
Template navigation commands preserve an explicit `--env-file`, including paths
with spaces, so following them retains the selected target.

When a new workflow needs an attached schedule, start from
`dsctl template workflow --with-schedule --raw`. That template sets
`workflow.release_state: ONLINE`, which DolphinScheduler requires before the
schedule can be created, while leaving the schedule itself offline. Do not
export an unrelated workflow merely to discover the schedule YAML shape.
For `1.3.9`, this block omits `timezone` and explains that DS uses the server's
local timezone. Later versions include the explicit `timezone` field.

Use `dsctl template workflow-patch --raw > patch.yaml` before
`workflow edit --patch`. Use
`dsctl template workflow-instance-patch --raw > instance-patch.yaml` before
`workflow-instance edit --patch`. These patch templates keep the active YAML
valid and put optional task operations in comments, so agents can discover the
shape without accidentally targeting placeholder task names.

`dsctl template params` returns a compact parameter-topic index. Expand only the
topic needed for the current authoring task:

```bash
dsctl template params --topic property
dsctl template params --topic built-in
dsctl template params --topic time
dsctl template params --topic context
dsctl template params --topic output
```

`dsctl template task` returns compact machine-readable task-template rows:

- `rows[].task_type`
- `rows[].kind`
- `rows[].category`
- `rows[].variants`
- `rows[].next_command`

`dsctl template task TYPE` returns the default task-level YAML fragment.
Use `--variant VARIANT` only for a complete scenario advertised by
`data.template.variants`; defaults and pure aliases are not choices. Add `--raw`
when you want only the YAML fragment without the JSON envelope. Copy the
fragment under `tasks:` in a workflow YAML file, then run `dsctl lint workflow`
and `workflow create --dry-run`.

## Payload Modes

`SHELL` and `PYTHON` tasks can use either a CLI shorthand or the DS-native task
params shape:

```yaml
name: inline-shell
type: SHELL
command: |
  echo "inline command"
```

The `command` shorthand is only accepted for `SHELL` and `PYTHON`. It compiles
to DS `taskParams.rawScript`.

`REMOTESHELL` only accepts `task_params` because DS also needs an SSH
datasource. Its required leaf fields are `task_params.rawScript` and
`task_params.datasource`; `task_params.type` defaults to `SSH` when omitted.

Use `task_params` when a task needs DS-native plugin fields:

```yaml
name: shell-resource-task
type: SHELL
task_params:
  rawScript: |
    bash scripts/job.sh
  resourceList:
    - resourceName: /scripts/job.sh
  localParams: []
  varPool: []
```

For `SHELL` and `PYTHON`, attached DS resources use
`task_params.resourceList[].resourceName`. Upload or inspect resources with:

```bash
dsctl resource upload --file job.sh --dir /tenant/resources/scripts
dsctl resource list --dir /tenant/resources/scripts
```

The directory commands use native storage paths: first run `dsctl resource list`
without `--dir` to obtain this target's FILE root in `resolved.directory`.
`/tenant/resources` above is an example root. Task `resourceName` is relative to
that root with one leading slash, so the selected native
`/tenant/resources/scripts/job.sh` becomes `/scripts/job.sh`; the worker downloads
it to `scripts/job.sh` under the task directory. When the FILE root is `/`, the
listed full name is already the task-relative path.

The CLI verifies each typed FILE before mutation. Exact `2.0.0` through
`3.1.9` compile script attachments to their positive resource IDs; newer profiles
compile them to the verified storage absolute path. Offline previews use
preview identities and cannot substitute for this persistent verification.

Exact `1.3.9` does not provide typed resource attachment for `PYTHON` or
`SHELL`: keep `resourceList: []`, and do not expect a `resource` template
variant. Upstream authoring permission checks cover positive resource IDs, but
the master's old `id=0`/`res` full-name path resolves a tenant without that user
permission context before the worker downloads the resource. The CLI never
manufactures that wire. Existing nonempty
native resource payloads remain unchanged/export opaque state; use a newer
exact profile for typed resource attachment.

## Dynamic Parameters

Choose the smallest useful scope: workflow globals for shared values, task
`localParams` for values owned by one task. Adding local parameters to a SHELL
task requires replacing the `command` shorthand with native `task_params`;
these two forms are mutually exclusive. Follow the short IN field hint in
`dsctl template task SHELL --raw`: move `command` to `task_params.rawScript`
and declare the input in `localParams`. A global can be referenced directly as `${name}` without declaring
a same-name local parameter; a local parameter can shadow it. Use
`dsctl template params --topic context` for exact precedence and
`--topic property` for parameter field types and defaults.

Workflow-level parameters are written under `workflow.global_params`:

```yaml
workflow:
  name: parameterized-workflow
  global_params:
    bizdate: "${system.biz.date}"
```

Mapping shorthand values are strings. Quote date-like and timestamp-like values,
such as `"2026-09-20"`, so YAML does not resolve them to date/time types.

Task-level parameters use the DS `Property` shape under
`task_params.localParams`:

```yaml
name: shell-params-task
type: SHELL
task_params:
  rawScript: |
    echo "bizdate=${bizdate}"
    echo '${setValue(row_count=42)}'
  localParams:
    - prop: bizdate
      direct: IN
      type: VARCHAR
      value: ${system.biz.date}
    - prop: row_count
      direct: OUT
      type: INTEGER
      value: "0"
  resourceList: []
  varPool: []
```

`direct: IN` declares input values used by `${name}` placeholders. `direct: OUT`
declares values that can be published into the downstream var pool. Output
syntax is exact-versioned: `1.3.9` has no `setValue` parser; `2.0.x` recognizes
`${setValue(name=value)}` only at line start; `3.0.0` through `3.2.0` also
recognize `#{setValue(name=value)}` at line start; and `3.2.1` and newer scan the
stream. From `3.3.1`, downstream binding requires a declared same-name IN
parameter. SQL tasks can publish result columns whose names match OUT parameter
`prop` values.

Runtime `--param KEY=VALUE` follows the selected exact profile: `1.3.9` rejects
it because the executor has no `startParams`; `2.0.0` through `2.0.7` accept only
keys declared in `workflow.global_params`; and `2.0.8` and newer accept arbitrary
keys as VARCHAR startup parameters. The same rule applies to `workflow run`,
`run-task`, and `backfill`.

For ordinary tasks, an upstream OUT value in the var pool overrides the local
fallback of a same-name downstream IN parameter. Do not use a same-name
self-reference such as `prop: run_label` with `value: ${run_label}` to forward a
workflow global: the local value shadows that global and can become a circular
placeholder. Omit the local entry to consume the workflow global directly, or
use a different property name for an intentional local alias. `lint workflow`
reports this self-reference.

Nested-workflow parameter inheritance is exact-versioned. On exact DS 3.4.1,
a `SUB_WORKFLOW` task's `localParams` do not become child inputs. The child
automatically inherits the parent workflow's globals, startup parameters, and
workflow-instance var pool as startup parameters, which override matching
child global defaults. Define a reusable standalone fallback on the child
itself:

```yaml
workflow:
  name: child
  global_params:
    run_label: CHILD_DEFAULT
```

Set the value supplied by a particular parent under that parent's
`workflow.global_params`, or pass it when starting the parent:

```yaml
workflow:
  name: parent
  global_params:
    run_label: FROM_PARENT
tasks:
  - name: invoke-child
    type: SUB_WORKFLOW
    task_params:
      workflowDefinitionCode: 123456789
      localParams: []
```

`lint workflow` warns when a SUB_WORKFLOW task has non-empty `localParams`,
because that shape does not configure the child in DS 3.4.1.

Exact `1.3.9` uses a different identity and parameter epoch. Select the child
only by its same-project workflow name:

```yaml
workflow:
  name: legacy-parent
  global_params:
    run_label: FROM_PARENT
tasks:
  - name: invoke-child
    type: SUB_WORKFLOW
    task_params:
      childWorkflowName: legacy-child
```

`childWorkflowName` must be one nonblank literal same-project name. The CLI
does not reinterpret an all-digit name as an id; it resolves the name and
compiles it to native `SUB_PROCESS` with one positive
`processDefinitionId`. Do not supply a workflow code, native id, placeholder,
`localParams`, resource, `varPool`, or cross-project selector. Raw opaque
create/edit is closed.

After compiling the final native graph, the CLI performs an iterative audit
equivalent to the legacy runtime traversal. It follows every task
`params.processDefinitionId`, including preserved native tasks of another
type, and rejects a malformed runtime edge, missing definition, permission
failure, direct/indirect cycle, or a closure beyond 1,000 loaded descendants.
These fail as stable CLI errors before mutation; descendant permission remains
`permission_denied`, and legacy result codes `50001` and `50003` are both
treated as not-found. Read-side hydration performs one same-project identity
inventory and maps only a safe immediate `SUB_PROCESS` one-id package back to
`childWorkflowName`; it neither reads nor audits grandchildren. A
placeholder-bearing or ambiguous name, unresolved id, or richer native package
remains opaque. `workflow describe` and export show the child name only after
that safe reversal. `workflow run-task` and task-scoped `workflow backfill` do
not perform child-name hydration.

At runtime, parent workflow globals feed the child, but a same-name child
global wins; task-level `localParams` are ignored. The task runs on the master,
persists its parent/child mapping and command transactionally, then persists
the child link in its own transaction for recovery. Pause and stop propagate
to the child subject to that child's task cancellation behavior. It
has no structured output or durable worker application id, and a child task
retry or `REPEAT` can replay side effects. Descendants may change after the
authoring audit, so verify their current existence, acyclicity, and release
readiness before execution. This typed addition does not provide new live
evidence or promote the experimental `1.3.9` profile.

DS also supports runtime time placeholders using `$[...]`, such as
`$[yyyyMMdd-1]`, `$[add_months(yyyyMMdd,-1)]`, and
`$[month_first_day(yyyy-MM-dd,-1)]`. The CLI preserves these expressions as
strings; DS evaluates them at runtime. Run `dsctl template params --topic time`
for the focused syntax list.

Inside `$[...]`, DS uses Java-style date patterns. Lowercase `yyyy` is calendar
year; uppercase `YYYY` is week-based year. `dsctl lint workflow` and
`workflow create/edit` warn on risky expressions such as `$[YYYYMMdd]` or
`$[yyyyww]` so callers can choose calendar-year, week-year, or
`year_week(...)` deliberately.

For SQL tasks, `sqlType: 0` means query SQL that returns rows; `sqlType: 1`
means non-query SQL such as DDL or DML. Run `dsctl task-type schema SQL` under
the selected exact version to see its state rules. SQL task payloads keep
`localParams`, `preStatements`, and `postStatements` as lists. `varPool` is a
later-version field and is intentionally absent from the exact `1.3.9` typed
contract.

Use the default template's short input hints or an advertised output scenario:

```bash
dsctl template params
dsctl template params --topic time
dsctl template task SHELL --variant output
dsctl template task PYTHON --variant output
dsctl template task SQL --variant output
dsctl template task HTTP
dsctl template task SWITCH
DS_VERSION=3.1.0 dsctl template task HIVECLI
DS_VERSION=3.1.0 dsctl template task MLFLOW
DS_VERSION=3.1.0 dsctl template task JUPYTER
DS_VERSION=3.1.0 dsctl template task OPENMLDB
DS_VERSION=3.1.0 dsctl template task ZEPPELIN
DS_VERSION=3.1.0 dsctl template task DINKY
DS_VERSION=3.1.9 dsctl template task FLINK
DS_VERSION=3.1.9 dsctl template task FLINK_STREAM
DS_VERSION=3.2.0 dsctl template task JAVA
DS_VERSION=3.2.0 dsctl template task KUBEFLOW
DS_VERSION=3.3.2 dsctl template task PYTORCH
DS_VERSION=2.0.9 dsctl template task WATERDROP
DS_VERSION=3.4.1 dsctl template task MR
DS_VERSION=3.4.1 dsctl template task DATAX
DS_VERSION=3.4.1 dsctl template task CHUNJUN
DS_VERSION=3.2.0 dsctl template task DATA_FACTORY
DS_VERSION=3.1.0 dsctl template task SAGEMAKER
DS_VERSION=3.2.1 dsctl template task SAGEMAKER
DS_VERSION=3.2.0 dsctl template task DMS
DS_VERSION=3.3.1 dsctl template task ALIYUN_SERVERLESS_SPARK
DS_VERSION=3.4.0 dsctl template task GRPC
dsctl task-type schema SQL
```

## Typed Task Templates

Typed template availability is selected by the exact DS version. Use
`dsctl template task` for the selected profile's compact catalog, then inspect
`dsctl task-type schema TYPE` before authoring a fragment. The authoritative
release counts, family membership matrix, exact holes and closed facets live in
[Reviewed task authoring boundaries](../development/task-authoring-boundaries.md#exact-task-coverage);
the commands project those reviewed decisions from the generated task catalog.
Do not infer support from a neighboring release, a shared Python model, or a
task type reported by the live cluster. In particular, selecting `3.4.3` does
not extend a family whose row in that matrix ends on an earlier exact release.

Typed availability also does not certify worker, plugin, credential, resource,
datasource or external-system readiness. Follow the applicable family section
below and its boundary entry for runtime prerequisites, excluded modes and any
allowed unchanged/export preservation path.

`SEATUNNEL/literal_local_config_job` authors one literal ASCII/LF
`task_params.rawScript` configuration on exact `3.0.0` through `3.4.3`.
Placeholders, resources, parameters, outputs, arbitrary shell, remote engines,
and alternate startup options are rejected. On `3.0.x`, compilation emits a
REST-only POSIX wrapper that writes the configuration and starts the Spark
launcher in client/local mode; this bypasses the upstream UI defect that emits
a Waterdrop command. Exact `3.1.0`–`3.1.5` use the native SPARK/custom-config
wire with `deployMode=local`, whose enum renders client mode. Exact `3.1.6`
uses `deployMode=client` and `master=LOCAL`; `3.1.7` onward uses
`seatunnel.sh` with custom config and local deploy.
Resource mode remains outside the facet, including the staged-path runtime
holes on `3.2.0` through `3.2.2`. From `3.3.1`, the workflow must have no
global parameters because upstream forwards them as command arguments outside
the owned config. Raw opaque create/edit is closed; richer server state can be
preserved unchanged or exported with provenance. Route the task to a Unix-like
worker with `SEATUNNEL_HOME`, Java, compatible connectors/catalogs, tenant,
network, and data access. Validate HOCON/JSON and connector behavior outside
dsctl. Upstream INFO-logs params, config, and commands, provides no structured
output or durable application id, cannot resume after failover, cancels
best-effort, and can rerun the complete job on retry. This review refreshes no
live evidence and promotes no profile.

`DATA_QUALITY/local_mysql_table_row_count_equals` authors a closed table-count
equality check. Exact `3.0.0` through `3.1.9` accept `datasource`, `table`, and
`expectedRowCount`; exact `3.2.0` through `3.2.2` additionally require
`database`. Before typed create or a changed edit carrying this exact
fixed-point projection—including dry-run—dsctl verifies the live stock rule
page and rule-form fingerprint for rule 10 plus a permission-visible MYSQL
datasource; the modern epoch also verifies that its database matches the
authored value. No-op edits and unchanged richer opaque-preserve tasks skip
this authoring preflight. Compilation fixes local Java Spark, the Data Quality main
class, a fixed-value row-count comparison, and blocking failure. Equality uses
native `NE` on `3.0.0`–`3.0.3` and `3.1.0`–`3.1.9` because those master
result checks are inverted, and native `EQ` on `3.0.4`–`3.0.6` plus `3.2.x`.
Older workers derive the database from the datasource; newer task wire carries it. A missing
result row can leave the task falsely successful without comparison. INFO logs
include parameters, commands, and resolved credentials, so fields are not
secret storage. Use a parameter-free workflow and shell-safe datasource
configuration, and provide the Data Quality application, Spark, Java, MYSQL
JDBC, network/table `SELECT` access, and writable DS metadata. The task has no
structured output, durable app id, or failover reattach; cancellation is
best-effort and retry can duplicate result rows or alerts. Richer existing
state is unchanged/export preserve-only. The review refreshes no live evidence
and promotes no profile.

`KUBEFLOW/tfjob_manifest` is typed in `MachineLearning` on exact `3.2.0`
through `3.4.3` and upstream-absent earlier. Canonical authoring owns only
`namespace`, `cluster`, and one literal `yamlContent`. The manifest must be one
ASCII/LF mapping document for `apiVersion: kubeflow.org/v1` and `kind: TFJob`,
with explicit `metadata.namespace` equal to canonical `namespace`, no root
`status`, and a nonempty `spec.tfReplicaSpecs` mapping. `generateName` and
server-owned `uid`, resource version/generation, creation/deletion timestamps
and grace, `managedFields`, and `selfLink` are forbidden. Its DNS-safe
`metadata.name` prefix is at most 52 characters and ends with the sole allowed
`${system.workflow.instance.id}` placeholder. Only JSON-core scalar tags
(`string`, `null`, `bool`, `int`, and `float`) are accepted; YAML timestamp,
binary, set, and custom tags are rejected. Other placeholders, controls,
duplicate keys, aliases and merges, multiple documents, `List`
roots, and extras fail closed. Compilation emits
only unchanged YAML and compact `{name,cluster}` namespace JSON. Exact empty UI
`localParams=[]` and `resourceList=[]` residue canonicalizes on decode;
nonempty or richer state is unchanged/export preserve-only. Raw opaque
create/edit is closed.

Export remains read-only. A richer opaque KUBEFLOW baseline can be saved only
unchanged or through a description-only or task-name-only patch; a task rename
in such a patch is metadata-only. Any task/workflow execution-semantic or
topology edit fails closed.

The master resolves `cluster` to kubeconfig, but fixed
`kubectl apply/get/delete -f` execution ignores the outer namespace and uses
the manifest namespace. The platform-default writer substitutes prepared
values and INFO logs expose complete params, expanded YAML, commands, status
JSON, and the complete resolved kubeconfig on every reviewed version. The task-log
filter present on exact `3.3.1` and newer does not suppress that worker-service
INFO event. On a valid status shape, the tracker recognizes only `Succeeded`,
`Available`, or `Bound` as success and `Failed` as failure. Missing
`status`/`conditions` or other nonterminal state can continue polling until the
task timeout. However, an existing empty `status.conditions` array makes all
reviewed exact `KubeflowHelper` implementations unconditionally access its last
element and fail at runtime instead of continuing to poll; a compatible
nonempty conditions protocol is a runtime prerequisite.
`appIds` is only a persisted submission sentinel, not a durable object id: it
can resume manifest-derived failover polling after the callback because the
workflow-instance-scoped resource name remains stable, while the
apply-before-callback gap can reapply. Keep `retry.times=0`; a retry in the same
workflow observes or reapplies the same terminal TFJob and does not guarantee
a new run. This disables only DolphinScheduler task retry; TFJob/Kubernetes
controller reconciliation and Pod `restartPolicy` can still repeat training
work. Duplicate typed `(cluster, namespace, metadata.name template)`
identities in one workflow are rejected so sibling tasks cannot apply or delete
the same TFJob. There is no command timeout or structured output. Provide a
shell-safe task path, `kubectl`, kubeconfig,
network/RBAC, and the TFJob CRD/status protocol. Typed workflow compilation
requires `timeout > 0` in native minutes and `timeout_notify_strategy` equal to
`FAILED` or `WARNFAILED`; omitted/default or explicit `WARN` warns but does not
terminate the watcher. The template sets `timeout_notify_strategy: FAILED` and
`timeout: 60` for one hour because neither the commands nor polling have an
internal deadline.

Recovery-style REPEAT/RERUN paths can reuse the same `workflowInstanceId` and
therefore the same rendered `metadata.name`. Same-id recovery may simply
reobserve an already-successful TFJob without new training; `REPEAT` does not
guarantee fresh training. Only creation of a new workflow instance provides a
fresh identity. `dsctl workflow-instance rerun`, `recover-failed`, and
`execute-task` now fail closed by default when the available instance `dagData`
contains any `KUBEFLOW` task. Use
`dsctl workflow run WORKFLOW --project PROJECT` to create a new workflow
instance. This is a `dsctl` protection only: it cannot constrain the
DolphinScheduler UI or direct REST calls, and it makes no KUBEFLOW detection
claim when `dagData` is unavailable.

The independent `dsctl workflow online` path reads the exact workflow DAG
before its release REST call. When that DAG explicitly contains KUBEFLOW, it
reuses the applicable typed or opaque runtime preflight; a failed preflight
sends no release request. `dsctl workflow offline` does not read the DAG and is
not subject to this activation gate.

A canonical-decoding server baseline whose outer retry, timeout, or `WARN`
predates these gates remains unchanged only through a description-only patch or a
file edit that leaves every execution-affecting task field untouched. Changing
type, params, command, flag, worker/environment selection, task group/priority,
retry/timeout/strategy/delay, resource limits, dependencies, or workflow-level
execution settings—including `release_state` transitions such as `OFFLINE` to
`ONLINE`, workflow timeout, `execution_type`, and global parameters—reruns the
applicable gates. Standalone YAML carries no server
provenance and gets no exception. No
live preflight or campaign is claimed; no profile is promoted and existing stable
`3.4.1` receipts become stale for this expanded authoring manifest.

`PYTORCH/literal_resource_script` is authorable on exact `3.1.0` through
`3.3.2` and upstream-absent on earlier profiles plus all exact
`3.4.x` profiles. Its closed `MachineLearning` payload owns only
`pythonExecutable`, one `.py` `scriptResource`, and ordered `scriptArgs`:

```yaml
- name: train-pytorch-model
  type: PYTORCH
  task_params:
    pythonExecutable: /usr/bin/python3
    scriptResource: /ml/train.py
    scriptArgs:
      - --epochs
      - "10"
  timeout: 0
```

`pythonExecutable` must be an absolute ASCII shell-safe literal.
`scriptResource` is a leading-slash ASCII shell-safe path relative to the
selected user's FILE root—not the storage absolute `fullName` shown on modern
profiles. Run `dsctl resource list`, remove its `resolved.directory` prefix
from the selected `.py` file's `fullName`, and retain one leading slash. Each
argument is one nonblank ASCII shell-safe token. Placeholders,
whitespace-bearing or shell-expanding tokens, Git checkout, environment
creation, custom launcher fragments, parameters, output declarations,
additional resources, and future fields fail closed for typed create/edit.
All typed profiles resolve that canonical path before mutation through the exact
generated resource API and require one permission-visible, non-directory FILE.
Exact `3.1.x` sends `pythonCommand` and the resolved positive id. Exact `3.2.0`
through `3.3.2` query the FILE base, prefix and page-verify the canonical path,
send `pythonLauncher` plus the resulting storage absolute `resourceName`, and
keep native `script` base-relative. The `3.2.x` base-dir endpoint returns an
ALL-resource root to admins regardless of the requested type; PYTORCH therefore
compares FILE and UDF responses and accepts only distinct sibling
`resources`/`udfs` tenant bases. Equal or ambiguous roots fail closed and require
a non-admin tenant user. Existing modern tasks are rendered canonically only
when their `resourceName` matches the freshly verified FILE identity; otherwise
their native task parameters remain opaque. Compilation also fixes environment
creation off, `pythonPath="."`, empty `localParams`, dormant environment
defaults, the staged relative script path, and a one-space argument join.
There is no raw opaque create/edit selector; richer existing native state is
metadata-only or unchanged/export preserve-only, and execution-affecting edits
fail closed.

Run the task only on Unix-like workers that already provide the selected
executable and PyTorch dependencies. Exact `3.1.0` writes UTF-8; later
reviewed releases write with the worker platform charset, which is why the
shared typed subset is ASCII. Keep outer `timeout: 0` on exact `3.3.1` and
`3.3.2`: those executors can block on output before timeout handling, plugin
cancellation is a no-op, and `3.3.2` can also call `exitValue()` on a live
process. Earlier cancellation is outer-worker best effort. INFO logs expose
the complete params and command; the task transports no output, has no durable
application id or failover reattachment, and a retry can rerun the complete
script. This review adds no live evidence and promotes no profile.

`LINKIS` is a reviewed runtime exclusion on every source-present release,
exact `3.2.0` through `3.4.3`. The stock worker submits `linkis-cli`
asynchronously and then parses a result field that its command executor never
fills; it can lose the remote task id after submission. Its remote-task base
also checks status only once rather than polling a terminal state. Therefore a
new LINKIS task, a LINKIS task-parameter edit, and raw opaque LINKIS authoring
all fail closed. A live LINKIS baseline may still be exported or carried
through an unchanged or metadata-only workflow edit with its native
`task_params` value unchanged. Retry/failover may duplicate or orphan the remote job,
and missing identity prevents reliable cancellation. No live evidence or
profile promotion is implied.

`WATERDROP/literal_local_config_job` is authorable on exact `2.0.1`–`2.0.9` and
accepts one absolute ASCII shell-safe DS FILE `configResource`. Before the
workflow mutation, dsctl resolves it to one visible, non-directory, positive
resource id. Compilation fixes `localParams=[]`, emits a one-entry
`resourceList`, and constructs one local/client/default launcher line. The
`--config` path removes exactly one leading `/` from the resource `fullName`,
while `$WATERDROP_HOME` is expanded by the worker shell rather than treated as
a DolphinScheduler `${...}` placeholder. Multi-config and remote modes,
variables, parameters, output, custom queue/script state, and future fields are
not authorable through typed or raw opaque create/edit. Richer existing native
state can survive only through unchanged/export preservation.

Do not author new `WATERDROP` tasks against exact `2.0.0`. That release exposes
the task type, model, and UI but its stock worker does not register the literal
channel as a `SHELL` alias, so execution fails before task construction. Typed
and raw opaque create/edit fail closed; existing server state remains
preservable. Exact `2.0.1` adds the alias. Eligible workers need a POSIX shell,
`WATERDROP_HOME`, a compatible foreground Waterdrop/Spark launcher, tenant and
resource permissions, and target connectivity. Upstream INFO logs parameters,
script, paths, command, and child output, so these fields are not secret
storage. The task has no structured output, durable application id, or
failover reattachment; cancellation is worker-local best effort and retry or
failover may replay the whole job.

`DYNAMIC/literal_single_dimension_fanout` is available only on exact `3.2.2`.
Use one literal same-project `childWorkflowName`, one literal
non-`system.*` `parameterName`, one to 1,024 ordered unique comma-free literal
`values`, and a positive `degreeOfParallelism` no greater than the value
count. The complete comma-joined `values` spelling must be at most 256
UI/JavaScript UTF-16 code units. Compilation emits one native dimension, an empty filter, and an
exact derived maximum. Empty UI `localParams`, `resourceList`, and
`disabled=true` residue are decode-only; richer state is unchanged/export
preserve-only and raw opaque create/edit is closed.

```yaml
- name: fanout-region
  type: DYNAMIC
  task_params:
    childWorkflowName: child-orders
    parameterName: region
    values: [east, west]
    degreeOfParallelism: 2
```

Before a live mutation, dsctl resolves that exact child name to a positive
native code in the selected project and
proves non-self, same-project, acyclic reachability across exact `DYNAMIC` and
`SUB_PROCESS` descendants. Keep the task retry count at zero and keep
`parameterName` distinct from declared parent globals; runtime start-parameter
collisions and later child-graph drift remain operator responsibilities.
Parent schedule time/timezone is not forwarded. INFO logs include all groups
and `dynamic.out(taskName)` data. Recovery/failover can replay effects, partial
persistence can orphan or duplicate children, cancellation is best effort,
and pause does not cascade. Exact `3.2.0` and `3.2.1` are reviewed runtime
exclusions because they omit the child tenant; `3.2.1` also has an unguarded
missing-start-parameter dereference. This review adds no live verification or
profile promotion.

`BLOCKING/same_workflow_state_gate` is available only on the exact
profiles from `3.0.0` through `3.2.2`; `BLOCKING` is upstream-absent from
`3.3.1`. Typed create/edit owns either blocking opportunity, strict
`alertWhenBlocking`, and nonempty grouped
same-workflow task-name `SUCCESS`/`FAILURE` predicates. The compiler resolves
the names to positive task codes and adds their DAG predecessor edges. Raw
opaque create/edit is closed. Those upstream tags expose no BLOCKING authoring
form, so this helper is REST-only. Richer native state survives only when an
unchanged or metadata-only edit starts from the existing server baseline;
standalone exported YAML has no opaque provenance and cannot independently
reapply it. Closed-looking native state is promoted to typed only when every
predicate already has a matching acyclic workflow relation; otherwise it stays
opaque. The task itself completes `SUCCESS`. A match moves the workflow
through `READY_BLOCK` to terminal `BLOCK`; a miss continues without ordinary
task retry, and active/retry work normally drains first. Exact `3.0.0` through
`3.0.6` mark standby tasks `KILL`; later reviewed versions use `PAUSE`.
`alertWhenBlocking` requests an alert record for the workflow
`warningGroupId`; actual delivery requires a valid alert group and alert
infrastructure that this task facet does not validate. The master-local task
INFO logs task codes, expected/actual states, aggregate results, and its
opportunity, so fields are not secret storage. It has no worker, structured
output, application id, or dedicated failover resume. Pause/kill changes only
task-local state through `3.1.9` and is warn/no-op on `3.2.x`; neither epoch has
a remote target. Infrastructure replay or workflow rerun may reevaluate and
repeat the alert. No live evidence is refreshed and no profile is promoted by
this review.

On releases through `3.2.2`, the user-facing `SUB_WORKFLOW` template compiles
to the native `SUB_PROCESS` task and safe native payloads export back to the
canonical name. Exact `1.3.9` uses the name-to-id service resolution described
above; later profiles use their exact code field. HTTP, DEPENDENT, logic-task
reference, parameter, child-workflow, and plugin payload fields are projected
by the exact selected version; inspect
`dsctl task-type schema TYPE` and `dsctl template task TYPE` under that same
`DS_VERSION` before authoring.

Exact `1.3.9` `DEPENDENT` uses literal names in YAML and database IDs only on
the wire:

```yaml
name: wait-for-publish
type: DEPENDENT
task_params:
  dependence:
    relation: AND
    dependTaskList:
      - relation: OR
        dependItemList:
          - dependentType: DEPENDENT_ON_TASK
            projectName: analytics
            workflowName: upstream-daily
            taskName: publish
            cycle: day
            dateValue: last1Days
```

`DEPENDENT_ON_WORKFLOW` omits `taskName`; its native leaf uses `depTasks=ALL`.
`DEPENDENT_ON_TASK` requires one literal task name other than `ALL`. Project and
workflow names may be all digits and are never reinterpreted as IDs. The CLI
resolves each project/workflow pair and verifies task membership before
transport, then emits `params={}` beside the outer legacy `dependence` object.
A DEPENDENT reference is a runtime state predicate, not a workflow DAG edge.
Do not add placeholders, parameters, resources, datasource fields, native IDs,
or future fields. Raw opaque create/edit is closed.

The base date values are exact: hour accepts `currentHour`, `last1Hour`,
`last2Hours`, `last3Hours`, and `last24Hours`; day accepts `today`,
`last1Days`, `last2Days`, `last3Days`, and `last7Days`; week accepts
`thisWeek`, `lastWeek`, and `lastMonday` through `lastSunday`; month accepts
`thisMonth`, `lastMonth`, `lastMonthBegin`, and `lastMonthEnd`. Exact `1.3.9`,
`2.0.0`–`2.0.9`, `3.0.0`–`3.0.1`, and `3.1.0` expose only that base set.
`thisMonthBegin` and `thisMonthEnd` join on `3.0.2`–`3.0.6`, `3.1.1`–`3.1.9`,
and `3.2.0` through `3.4.3`. Keep `cycle` paired with the selected value:
upstream runtime uses `dateValue` to calculate the window and does not use
`cycle` to correct it.

Workflow create/edit/export and workflow-instance edit support the exact
name-resolution path. Workflow-definition reads reverse-bind names best-effort.
Unresolved or richer split outer state remains opaque and requires a server
baseline. Its name, type, task parameters, and command must remain unchanged;
description edits and unrelated topology additions are allowed. Standalone
YAML and workflow-instance export are not lossless carriers. The task runs on
the master, publishes no output, and has no durable remote app ID or
reconnectable cancel/resume target. Scheduled retry/failover retains the
original `scheduleTime`; only manual/current-time reinitialization can shift
the window. Authoring verifies target existence, but upstream does not attest
target-project authorization. A later target rename, deletion, or state change
remains a runtime prerequisite.

CONDITIONS templates use placeholders for existing task names. Define the
predicate and branch targets in the same workflow's `tasks[]`. The CLI builds
incoming edges from predicate references and outgoing edges to `successNode`
and `failedNode`; do not duplicate these edges in `depends_on`. Inspect the
selected task template or schema for its exact branch constraints.

Exact `1.3.9` `CONDITIONS` uses task names throughout:

```yaml
name: route-by-state
type: CONDITIONS
task_params:
  dependence:
    relation: AND
    dependTaskList:
      - relation: AND
        dependItemList:
          - task: upstream-task
            status: SUCCESS
  conditionResult:
    successNode: [on-success]
    failedNode: [on-failure]
```

Every referenced task must resolve in the same workflow. Predicate status is
strictly `SUCCESS` or `FAILURE`; both relation levels accept only `AND` or `OR`,
and every group/list must be nonempty. `successNode` and `failedNode` must each
hold exactly one task name, and the two names must differ. Do not add placeholders,
`localParams`, `varPool`, resources, datasource fields, or other parameters.
The exact wire keeps native `dependence` and `conditionResult` outside `params`, writes the
predicate as `depTasks: upstream-task`, keeps both branch targets as names, and
emits `params: {}`. Predicate and branch references form the workflow edges;
unknown names, self-references, cycles, and extra successors are rejected
before transport. The default template owns this boundary; there is no
parameter or alias selector.

This task runs on the master, not a worker. INFO logs include referenced task
names, expected and actual states, and the aggregate result. Retry or master
failover reevaluates persisted state from the same process instance; there is
no remote application id, cancel target, or remote resume. Existing richer
native state is not typed create/edit input. It survives only when editing from
an existing server baseline without changing the task payload, task names, or
topology, including a metadata-only edit. Standalone YAML export omits the
split outer opaque fields, so do not use it as a lossless backup or migration
format for such a task.
There is no raw opaque create/edit selector, and invalid typed input fails
instead of silently downgrading to baseline preservation.

`PIGEON` has a default template from `2.0.0` through `3.2.2`. It authors only
the nonblank `task_params.targetJobName`. The DS worker also needs `p_host` at
execution time, but that value may come from the wider resolved parameter
context and is therefore documented rather than invented as a static task
field.

`HTTP` is typed on every exact profile. The default template includes a short
IN binding hint; the coupled method/body scenario `post-json` begins at `3.2.1`. Exact `1.3.9`
canonical YAML has no `httpBody`, `varPool`, or explicit `socketTimeout`; the
exact wire injects `socketTimeout: 60000`, while `connectTimeout` remains an
explicit typed field. Exact `1.3.9` typed `localParams` are `IN`-only and use
that release's nine native scalar data types; `OUT` parameters plus
non-exact/future `LIST` and `FILE` raw shapes remain unchanged/export opaque
state. DolphinScheduler applies prepared-parameter substitution to the URL and
every property. At INFO it logs complete task params, substituted request
params/properties, the configured/original URL (not a
claimed final or expanded URL), status, and the complete response body. Do not
use these fields for secret storage; the CLI does not redact them. Native
`HEAD`, the `BODY` property kind, nondefault socket timeout, and future native
members remain unchanged/export opaque preservation state and are not typed
authoring. There is no reliable cancel, durable id, failover resume, or
structured output. Retry resends the whole request, so
`POST`, `PUT`, and `DELETE` side effects can duplicate. This review adds no live
evidence or promotion.

`PYTHON` is typed on every exact profile. On exact `1.3.9`, use
`rawScript`, an empty `resourceList`, and unique `IN`-only `localParams` with
`VARCHAR`, `INTEGER`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`,
or `BOOLEAN`. That release has no `varPool`, `LIST`, `FILE`, `OUT` publication,
or structured output. As described above, every nonempty native resource
shape, richer parameter state, inherited member, and future field remains
unchanged/export opaque rather than being guessed as canonical input.

The `1.3.9` worker normalizes CRLF, expands prepared parameters without Python
escaping, writes the generated script as UTF-8, and selects `PYTHON_HOME` with
a `python` fallback. It INFO-logs complete task params, original and substituted
script, command, and stdout/stderr. Do not put secrets in these values; the CLI
does not detect or redact them. Cancellation is local best-effort, there is no
durable Python-process resume, and retry or worker failover runs the whole
script again. Design Python tasks so repeated database, file, or remote calls
are safe. This review adds no live receipt or profile promotion.

Exact `1.3.9` also has a dedicated typed SQL subset. Supported datasource types
are `MYSQL`, `POSTGRESQL`, `HIVE`, `SPARK`, `CLICKHOUSE`, `ORACLE`,
`SQLSERVER`, and `DB2`; `H2` is not part of the executable typed claim. A
minimal task supplies one of those types, a positive datasource id, literal
SQL, and strict query/update `sqlType`:

```yaml
name: legacy-query
type: SQL
task_params:
  type: MYSQL
  datasource: 7
  sql: SELECT 1 AS answer
  sqlType: 0
```

Optional `displayRows` and `limit` are nonnegative. `localParams` are unique,
`IN`-only, and limited to `VARCHAR`, `INTEGER`, `LONG`, `FLOAT`, `DOUBLE`,
`DATE`, `TIME`, `TIMESTAMP`, or `BOOLEAN`; every pre/post statement must be
nonblank. `connParams` must be empty for non-HIVE datasources. HIVE alone
accepts a literal semicolon-separated `key=value` string with unique nonblank
keys and nonblank values. Do not add `sendEmail`, UDFs, mail fields, `OUT` parameters,
`groupId`, or `varPool` to typed YAML. The exact projector emits
`sendEmail: false` because absent or null enables mail in upstream `1.3.9`,
plus empty UDF/mail fields and `showType: TABLE`. Richer native state remains
selector-restricted explicit opaque authoring only when supplied as one
complete native parameter package, or unchanged/export preservation. Partial
invalid typed input never selects opaque mode.

Workflow creation and update do not authorize the referenced datasource before
the master loads it, so verify that the id exists, the submitting user may use
it, and its type matches `task_params.type`. Pre/main/post SQL does not run in
one explicit transaction. Cancellation can finish while JDBC continues, and a
retry or worker failover runs the whole sequence again. Upstream INFO logs
include full parameters, SQL, HIVE `connParams`, bound values, and result rows;
do not put secrets in these fields. The task publishes no structured output or
durable application id. This review adds no live receipt or profile promotion.

`SPARK/inline_local_sql` has one default template from `3.0.0` through
`3.4.3`:

```yaml
name: run-inline-local-sql
type: SPARK
task_params:
  rawScript: SELECT 1 AS answer
```

The SPARK plugin also exists on `1.3.9` and `2.0.0`–`2.0.9`, but those
upstream parameter models have no SQL program mode. The typed facet is absent
there, so use only an intentional native opaque payload on those profiles.
Typed authoring owns exactly the nonblank literal `rawScript` shown above,
preserves its canonical and wire spelling, and rejects `${...}`, `$[...]`,
unsafe control text, and all other fields.

The CLI supplies the exact native mode fields. Through `3.1.9` it emits
`programType: SQL`, `sparkVersion: SPARK2`, and `deployMode: local`. Exact
`3.2.0` and `3.2.1` instead emit `programType: SQL`,
`sqlExecutionType: SCRIPT`, and `deployMode: local`; from `3.2.2` the wire also
contains `master: local`. Native `JAVA`, `SCALA`, and `PYTHON` applications are
explicit opaque modes on reviewed profiles, including their complete native
cluster and resource payloads. Native SQL `FILE` is likewise opaque from
`3.2.0`. Cluster or resource fields added to inline SQL, and unrecognized
inherited or future state, remain unchanged preservation only. Invalid inline
SQL never unlocks opaque mode.

Exact `3.0.x` writes SQL to the worker file before final-command substitution.
From `3.1.0`, upstream expands SQL from the prepared parameter map before
writing it, but the stable typed contract forbids placeholders on every
profile. Route the task to a worker with `SPARK_HOME2` through `3.1.9` or
`SPARK_HOME` from `3.2.0`, plus Spark SQL, Java, Hadoop, Hive catalog
configuration, and target-data permissions. Upstream logs raw or expanded SQL
at INFO and normalizes CRLF before writing the worker SQL file, so runtime line
endings need not be byte-identical to the canonical wire. Execution is one
synchronous local process with no durable application id or failover resume;
cancellation controls that process and a retry runs the complete SQL again.
Keep retries at zero unless repeated side effects are safe. This source review
refreshes no live receipt, changes no `tested` flag, and promotes no profile.

`FLINK/inline_local_sql` has one default template on exact `3.0.0`–`3.0.6`,
`3.1.2`–`3.1.9`, and `3.2.0` through `3.4.3`:

```yaml
name: run-inline-local-sql
type: FLINK
task_params:
  rawScript: SELECT 1 AS answer
```

The `1.3.9` and `2.0.x` parameter models have no SQL `ProgramType`. Exact
`3.1.0` has the SQL shape but no typed facet because its executor resolves
`mainJar` unconditionally before SQL initialization. Exact `3.1.1` fixes that
guard but reverses LOCAL/CLUSTER SQL targets; `3.1.2` fixes target selection.
Both `3.1.0` and `3.1.1` remain typed holes. Untyped registered profiles retain
their exact opaque policies; `3.1.1` requires a recognized native selector and
rejects canonical inline SQL.
Typed authoring owns only the nonblank literal `rawScript`, keeps its spelling,
and supplies `programType: SQL`, `deployMode: local`, and `initScript: ""` on
the wire.

Exact `3.0.x` workers write the SQL file with their platform-default charset,
so those typed schemas accept ASCII only. Exact `3.1.2` and newer write
UTF-8. All typed profiles reject blank scripts, carriage returns, DEL/C0/C1
controls, `${...}`, and `$[...]`. Multi-statement SQL, TAB/LF, quotes,
semicolons, backslashes, backticks, and `$()` remain literal and are not
rewritten.

Recognized native `JAVA`, `SCALA`, and `PYTHON` programs and exact non-local
SQL modes use explicit opaque create/edit. Do not attach cluster, resource,
initialization, option, inherited, or future fields to typed local inline SQL;
those fields and unrecognized native state round-trip only on unchanged/export
opaque-preservation paths. Invalid local inline SQL does not unlock opaque
mode.

Route tasks through `3.2.2` to a worker whose `PATH` resolves
`sql-client.sh`; releases from `3.3.1` use
`FLINK_HOME/bin/sql-client.sh`. Every eligible worker needs the Flink SQL
client, Java, connector and catalog configuration, and target-data permissions.
Exact `3.4.2`–`3.4.3` substitute prepared parameters before writing SQL, but the
typed contract still rejects placeholders for one portable literal meaning.
Upstream logs task parameters, SQL content and file paths, and the command at
INFO, so do not put secrets in the script. The local client exposes no DS task
output, durable application id, or failover resume; retry runs the whole SQL
again and may repeat side effects. This source review refreshes no live
evidence, changes no `tested` flag, and promotes no profile.

`FLINK_STREAM/inline_local_sql` has one default template only on exact
`3.1.5`–`3.1.9` and `3.2.0` through `3.2.2`:

```yaml
name: run-inline-local-sql
type: FLINK_STREAM
task_params:
  rawScript: SELECT 1 AS answer
```

The plugin is upstream-absent through `3.0.6`. Exact `3.1.0`–`3.1.4` register
`FLINK_STREAM`, but its executor resolves `mainJar` before SQL initialization.
Those releases retain their exact opaque policies; `3.1.1`–`3.1.4` require a
recognized native selector. From `3.3.1` through `3.4.3`,
`ExecutorServiceImpl.execStreamTaskInstance` immediately throws
`Not supported`; those registered runtime holes likewise expose no typed
variant. Typed authoring owns only the nonblank literal `rawScript`, preserves
its spelling, and supplies exactly four `taskParams` fields:
`programType: SQL`, `deployMode: local`, `initScript: ""`, and `rawScript`.
The compiler separately supplies top-level `taskExecuteType: STREAM`; do not
author it in `task_params` or task YAML. Every typed profile writes UTF-8.

Blank scripts, carriage returns, DEL/C0/C1 controls, unpaired Unicode
surrogates, `${...}`, `$[...]`, and extra fields are rejected. Multi-statement
SQL, TAB/LF, quotes, semicolons, backslashes, backticks, and `$()` stay literal.
Recognized `JAVA`, `SCALA`, and `PYTHON` JAR programs and exact non-local SQL
modes use explicit opaque create/edit. Local-inline extras and unrecognized
inherited or future state survive only through unchanged/export
opaque-preservation; invalid local SQL never unlocks opaque mode.

Route every typed release to a worker whose `PATH` resolves
`sql-client.sh`. Workers also need Java, connector and catalog configuration,
and target-data permissions. Later `FLINK_HOME` and prepared-substitution
plugin behavior cannot widen typed membership because the corresponding STREAM
entry is unsupported. Upstream logs parameters, SQL content and file paths,
and commands at INFO, so do not put secrets in this task.

Local SQL is expected to publish no application id and exposes no result
output, durable submit identity, or failover resume. Plugin cancel and
savepoint require an application id. Stop order is plugin cancel then PID-tree
kill on `3.1.x`, PID-tree kill then plugin cancel from `3.2.0` through `3.2.2`,
and plugin cancel only from `3.3.1` through `3.4.3`; the last epoch returns on
the missing id without a process fallback. Reliable stop is unsupported, so an
unbounded SQL stream may continue after the DS task stops. Retry reexecutes all
SQL on typed releases. Keep retries at zero unless replay and continued-stream
risks are acceptable. This review refreshes no live evidence, changes no
`tested` flag, and promotes no profile.

`K8S/literal_container_job` has one default template in `Cloud` on exact
`3.1.4` through `3.4.3`. The connection shape changes with the selected version.
Exact `3.1.0`–`3.1.3` are not typed: their default template is an explicit
native opaque risk scaffold under the broken watcher.
Raw create/edit requires canonical `connectionMode` and sibling `cluster` to
be absent, a nonblank image, and `namespace` containing compact JSON with
nonblank `name` and `cluster`. Canonical connection intent fails closed rather
than downgrading, and the scaffold is not an executable-runtime attestation.
Exact `3.1.4`–`3.1.9` use namespace intent:

```yaml
name: run-container-job
type: K8S
task_params:
  connectionMode: NAMESPACE
  namespace: analytics
  cluster: production
  image: busybox:1.36
  minCpuCores: 0.0
  minMemorySpace: 0.0
  environment:
    - name: REPORT_DATE
      value: "2026-08-20"
```

Exact `3.2.0` through `3.2.2` retain those connection fields but add the
advanced fields described below; generate the selected-version template
instead of copying the `3.1.9` block unchanged. The CLI compiles `namespace`
and `cluster` to one compact native namespace JSON string. Exact `3.2.2` has a
misleading upstream surface: its UI hides the namespace selector and its
parameter check validates only `image`, but the backend, master, and runtime
still require the legacy namespace wire. Keep both canonical fields. Exact
`3.3.1` and newer instead use a positive K8S datasource id:

```yaml
name: run-container-job
type: K8S
task_params:
  connectionMode: DATASOURCE
  datasource: 7
  image: registry.example.com/jobs/report:2026-08-20
  minCpuCores: 0.5
  minMemorySpace: 256
  environment: []
  command: []
  args: []
  imagePullPolicy: IfNotPresent
  customizedLabels: []
  nodeSelectors: []
```

Compilation fixes native `type: K8S` and empty `namespace` and `kubeConfig`
fields, which the worker replaces from the datasource. The image is literal;
tags may move, so use a digest when immutable identity matters. CPU and
MiB-memory quantities must be finite and nonnegative. Environment names must
be unique Kubernetes environment identifiers, cannot be `taskInstanceId`, and
compile to `IN`/`VARCHAR` `localParams`.

The task's outer `name` must match
`^[A-Za-z0-9][A-Za-z0-9-]{0,51}$`. Uppercase is allowed: the runtime
lowercases it with `Locale.ROOT`, then appends `-` and the decimal task-instance
id to derive the Kubernetes Job name.

Exact `3.2.0` and newer additionally support structured `command` and `args`
lists, an optional `pullSecret` object name, `imagePullPolicy`, unique valid
`customizedLabels`, and structured `nodeSelectors`. Exact `3.2.0` requires at
least one custom label because its executor cannot mutate an empty label map;
later profiles allow the empty list, and custom-label values may be empty.
Exact `3.2.0` attaches custom labels to the Job only; `3.2.1` and newer attach
them to both Job and Pod template. Exact `3.1.4`–`3.1.9` reject all of these advanced
fields. Multiple node-selector expressions may repeat the same key; Kubernetes
ANDs them, which permits bounds such as `rank Gt 1` plus `rank Lt 10`.
`In`/`NotIn` require a nonempty list of unique nonempty Kubernetes label
values; `Exists`/`DoesNotExist` require `values: []`, while `Gt`/`Lt` take one
decimal-integer string in the list, from zero through `9223372036854775807`,
for example `values: ["1"]`.

On exact `3.1.4`–`3.1.9`, the worker instead uses the image ENTRYPOINT/CMD and fixes
`imagePullPolicy=Always`.

`outputs` is available only on `3.2.0`, `3.2.1`, `3.2.2`, and `3.4.2`–`3.4.3`:

```yaml
  outputs:
    - name: result_path
```

Output names must be unique Kubernetes-style names other than
`taskInstanceId` and disjoint from environment names. The compiler emits
`OUT`/`VARCHAR` entries with empty values. The container must write
`${(result_path=value)dsVal}` or `#{(result_path=value)dsVal}` on `3.2.0`, and
`${setValue(result_path=value)}` or `#{setValue(result_path=value)}` on
`3.2.1`, `3.2.2`, or `3.4.2`–`3.4.3`. Marker values must be nonempty on `3.2.0` and
`3.2.1`. On `3.2.0`, `$VarPool$` is a reserved delimiter and a value such as
`a=b` publishes only `a`; `3.2.1` preserves `a=b`. Exact `3.2.2` and `3.4.2`–`3.4.3`
also preserve `=` and allow an empty value. Do not author outputs on `3.3.1`
through `3.4.1`; those releases
parse into a local `varPool` but fail to transport it from the physical
executor. Exact `3.4.2` restores transport, although every OUT declaration
also enters the prepared map and is injected into the Pod as an empty-valued
environment entry.

DolphinScheduler injects the complete prepared map—not only the authored
`environment` list—into the Pod. Global, built-in, and inherited keys must also
be valid Kubernetes environment names and must not collide with
`taskInstanceId`. Job watch registration omits namespace, so the selected
target must match the kubeconfig current-context namespace. Exact `3.3.1`
through `3.4.1` additionally omit the datasource namespace from Pod-log/output
lookup. Upstream logs Pod output; on `3.2.x`, full task/context logging can
expose configuration outside the task-log converter. From `3.3.1` it puts the
resolved kubeconfig into task params and INFO-logs the object. Do not use
authored or prepared values as secret storage; dsctl does not redact them.

K8S publishes no durable application id and cannot resume after worker
failover. Cancel requires the same worker's in-memory Job, and retry or worker
loss can duplicate the container's side effects. Typed K8S has no raw opaque
create/edit selector; richer native state is retained only by unchanged edit or
export provenance. Releases before `3.1.0` are upstream-absent. Exact
`3.1.0`–`3.1.3` have only selector-restricted native opaque authoring because
the watcher can finish on `RUNNING` with exit status `-1`; canonical connection fields fail
closed and its default payload is an explicit risk scaffold. This
review adds no live evidence and promotes no profile.

`KUBEFLOW/tfjob_manifest` has one default template in `MachineLearning` on
exact `3.2.0` through `3.4.3`:

```yaml
name: train-mnist
type: KUBEFLOW
description: Run one workflow-instance-scoped Kubeflow TFJob
task_params:
  namespace: kubeflow-team
  cluster: production
  yamlContent: |
    apiVersion: "kubeflow.org/v1"
    kind: TFJob
    metadata:
      name: train-mnist-${system.workflow.instance.id}
      namespace: kubeflow-team
    spec:
      tfReplicaSpecs:
        Worker:
          replicas: 1
          restartPolicy: OnFailure
          template:
            spec:
              containers:
                - name: tensorflow
                  image: registry.example/tf-mnist:1.0
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout_notify_strategy: FAILED
timeout: 60
```

Keep the workflow-instance placeholder exactly once as the terminal suffix of
the TFJob name. It remains stable when DolphinScheduler creates a replacement
task instance during failover. Recovery-style REPEAT/RERUN can reuse the same
`workflowInstanceId`, which renders the same name and may reobserve an already
successful TFJob without new training. Only creation of a new workflow instance
provides a fresh identity; `REPEAT` does not guarantee fresh training.
`dsctl workflow-instance rerun`, `recover-failed`, and `execute-task` now fail
closed by default when the available instance `dagData` contains any
`KUBEFLOW` task. Use `dsctl workflow run WORKFLOW --project PROJECT` to create
a new workflow instance. This is a `dsctl` protection only: it cannot constrain
the DolphinScheduler UI or direct REST calls, and it makes no KUBEFLOW detection
claim when `dagData` is unavailable. A task-instance placeholder would break
the failover reattachment property.
Keep task retries at zero: `kubectl apply` uses the same name inside one
workflow instance, so a retry can observe or reapply the already-terminal
TFJob and does not guarantee a new training run. The compiler rejects duplicate
typed `(cluster, namespace, metadata.name template)` identities in one workflow
so sibling tasks cannot apply or delete the same TFJob. `retry.times=0` closes
only the DolphinScheduler task retry; TFJob/Kubernetes-controller reconciliation
and the Pod `restartPolicy` may still repeat training work. Typed workflow
compilation requires `timeout > 0` in native minutes and
`timeout_notify_strategy=FAILED` or `WARNFAILED`; omitted/default or explicit
`WARN` only warns and leaves the watcher running. The template uses
`timeout_notify_strategy: FAILED` and `timeout: 60` for one hour because the
kubectl commands and status loop have no internal deadline.

For an existing server baseline that decodes to canonical `taskParams`, a
description-only patch or a file edit preserves legacy outer retry, timeout, or
`WARN` only when every execution-affecting task field is unchanged. Changing
type, params, command, flag, worker/environment selection, task group/priority,
retry/timeout/strategy/delay, resource limits, dependencies, or workflow-level
execution settings—including `release_state` transitions such as `OFFLINE` to
`ONLINE`, workflow timeout, `execution_type`, and global parameters—reruns the
relevant typed gates. A standalone YAML file has no baseline
provenance and
must satisfy the gates directly.

The typed validator preserves accepted YAML spelling but accepts only ASCII/LF,
one YAML mapping document for a `kubeflow.org/v1` `TFJob`, and an explicit
manifest namespace equal to `task_params.namespace`. Root `status` is
forbidden, `spec.tfReplicaSpecs` must be a nonempty mapping, and
`generateName`, `uid`, `resourceVersion`, `generation`, `creationTimestamp`,
`deletionTimestamp`, `deletionGracePeriodSeconds`, `managedFields`, and
`selfLink` are rejected. Only JSON-core scalar tags (`string`, `null`, `bool`,
`int`, and `float`) are accepted; YAML timestamp, binary, set, and custom tags
are rejected. Other placeholders, duplicate keys, aliases/merges, controls,
multi-document input, and `List` roots also fail.
Compilation sends only
the YAML plus compact native `{name,cluster}` namespace JSON. Empty
`localParams=[]` and `resourceList=[]` left by the upstream UI are accepted
when decoding a server baseline, but neither is emitted by compilation.

The selected DolphinScheduler cluster supplies kubeconfig; the worker does not
pass the outer namespace to kubectl, so the manifest namespace is
authoritative. Route the task to a worker with `kubectl`, that kubeconfig,
network/RBAC, and a compatible Kubeflow TFJob CRD/status implementation. Do not
place secrets in the manifest: upstream logs the expanded YAML, commands,
status responses, and the complete resolved kubeconfig on every reviewed version.
The task-log filter present on exact `3.3.1` and newer does not suppress that
worker-service INFO event.
On a valid status shape, only `Succeeded`, `Available`, or `Bound` ends tracking
successfully and only `Failed` ends it unsuccessfully. Missing
`status`/`conditions` or other nonterminal state can continue polling until the
task timeout. An existing empty `status.conditions` array is different: all
reviewed exact `KubeflowHelper` implementations unconditionally access its last
element and fail at runtime instead of continuing to poll. A compatible
nonempty conditions protocol is therefore a runtime prerequisite.
There is no structured output or durable Kubernetes id; persisted `appIds` is
only an already-submitted sentinel. Raw opaque creation and parameter edits
are closed, while richer existing state can survive unchanged/export
preservation. No live preflight or current live receipt attests this facet.

Export remains read-only. Saving that richer opaque KUBEFLOW state is allowed
only unchanged or through a description-only or task-name-only patch; a task
rename in such a patch is metadata-only. Any task/workflow execution-semantic
or topology edit fails closed.

`JAVA/literal_fat_jar` has one default template in `Universal` on exact
`3.2.0`, `3.2.2`, and `3.3.1` through `3.4.3`:

```yaml
name: run-java-fat-jar
type: JAVA
task_params:
  mainJar: /jobs/daily-orders.jar
  mainArgs:
    - --date
    - "2026-08-30"
```

`mainJar` is an already uploaded DolphinScheduler resource `fullName`, not a
local workstation or worker path. Use `dsctl resource list` to choose it. The
value must be absolute, shell-safe, and end in `.jar`. `mainArgs` must be an
array of separate shell-safe tokens; do not combine words with spaces or put
shell expansion, credentials, `${...}`, or `$[...]` in them. The compiler
joins the tokens with one space and writes the same resource into native
`mainJar` and `resourceList`, because the worker downloads only
`resourceList`.

On `3.2.0` and `3.2.2`, compilation selects `runType=JAR` and sends empty
`rawScript`. From `3.3.1`, it selects `runType=FAT_JAR` and sends empty
`mainClass`. Every typed version fixes empty `jvmArgs`, false
`isModulePath`, and empty `localParams`. Do not use typed JAVA on `3.2.1`:
that exact executor prefixes the already absolute staged path again for both
the main JAR and resource list. Its materialized fail-closed policy allows
opaque create/edit only for a nonblank native `runType=JAVA` raw-source
payload; broken `JAR` state is preserve-only, and canonical `mainJar`/
`mainArgs` input never downgrades to opaque. Exact `3.2.2` fixes the path
defect. JAVA is upstream-absent on earlier profiles through `3.1.9`.

Legacy raw Java source on its reviewed exact wires and modern `NORMAL_JAR` are
selector-restricted explicit opaque-authoring modes. JVM arguments, module
path, task parameters/output, additional dependency resources, and richer
native state remain outside typed ownership.
Eligible workers need `JAVA_HOME`, a compatible JDK, tenant execution
permission, and DS resource-storage access. Upstream logs the complete task
params, final shell command, and child output at INFO, so these values are not
secret storage. On `3.2.x`, cancellation destroys only the direct process and
can leave child processes behind; `3.3.1` onward kills the process tree and
attempts generic application cancellation. JAVA has no durable application id
or failover reattachment; retry runs the whole JAR again and can duplicate
side effects. This review refreshes no live evidence and promotes no profile.

`MR/literal_java_jar_job` has one default template in `Universal` on every
exact profile:

```yaml
name: run-mapreduce-jar
type: MR
task_params:
  mainJar: /jobs/wordcount.jar
  mainClass: com.example.WordCount
  mainArgs:
    - hdfs:///input
    - hdfs:///output
```

Choose `mainJar` from `dsctl resource list`. It must be an absolute shell-safe
resource `fullName` ending in `.jar`; it is not a worker-local path.
`mainClass` must be a strict dotted ASCII Java class, and each `mainArgs` item
must be one shell-safe token. The compiler joins arguments with one space.
Through `3.1.9`, the service resolves `mainJar` to an exact visible
non-directory file with a positive id before mutation and sends
`mainJar={id}`. From `3.2.0`, it sends `mainJar={resourceName}` and
`yarnQueue=""`.

Every version fixes `programType=JAVA`, empty `appName`/`others`, and empty
`localParams`/`resourceList`. Do not add the JAR to `resourceList`: the native
MR model adds `mainJar` to the runtime resource list, so a duplicate can stage
it twice. Queue selection stays runtime-owned. Native `SCALA` is the only
selector-restricted explicit opaque create/edit mode. Do not use native
`PYTHON` even though the upstream UI lists it; the executor still runs
`hadoop jar`. Richer and legacy resource state remains unchanged/export
preserve-only.

Workers need Hadoop/YARN configuration, tenant and resource-storage
permission, queue access, and target-data access. Upstream INFO-logs full task
parameters, command files, final commands, and child output, so these fields
are not secret storage. MR publishes no DS output or durable callback identity
and cannot reattach after failover. Cancellation is best-effort from an
observed application id; retry or failover can rerun the whole JAR. Exact
`3.3.1` onward also has a post-exit `appIds` context transport hole. This
review refreshes no live evidence and promotes no profile.

`SQOOP/literal_command` has one default template in `DataIntegration` on every
exact profile:

```yaml
name: import-orders
type: SQOOP
task_params:
  subcommand: import
  args:
    - --connect
    - jdbc:mysql://db.example.invalid:3306/source
    - --username
    - sqoop_reader
    - --password-file
    - file:///run/secrets/sqoop-password
    - --table
    - orders
    - --target-dir
    - hdfs:///warehouse/orders
```

The closed payload owns only `subcommand: import|export` and one nonempty,
ordered `args` list. The compiler emits exactly `jobType=CUSTOM`,
`localParams=[]`, and one POSIX-quoted `customShell` beginning with `sqoop`.
Arguments reject edge whitespace, controls, surrogates, DS placeholders,
interactive `-P`, standalone `--password`, and `--password=...`; a
worker-readable `--password-file` is allowed. Empty argument tokens are valid
and round-trip through POSIX `''`; only the ordered list itself must be
nonempty. Releases through `3.1.0` write the script as UTF-8. Exact `3.1.1`
and newer use the platform-default charset, so typed arguments there are
ASCII-only. CRLF becomes LF through `3.1.9` and the worker OS line separator
from `3.2.0`.

Native TEMPLATE jobs and arbitrary CUSTOM scripts are selector-restricted
explicit opaque create/edit; richer native state is unchanged/export
preserve-only. Workers need a POSIX shell, Sqoop, Hadoop/YARN, JDBC drivers,
tenant execution permission, credentials, connectivity, and data access.
Upstream logging exposes command-bearing state, so task fields are not secret
storage. There is no DS output, durable application id, or failover reattach;
cancellation is best-effort and retry/failover can duplicate the transfer.
This review refreshes no live evidence and promotes no profile.

`DATA_FACTORY/pipeline_trigger` has one default template in the `Cloud`
category from exact `3.2.0` through `3.4.3`:

```yaml
name: trigger-data-factory-pipeline
type: DATA_FACTORY
task_params:
  factoryName: analytics-factory
  resourceGroupName: analytics-rg
  pipelineName: daily-copy
```

The plugin is upstream-absent through `3.1.9`. On all reviewed typed
coordinates, create/edit owns exactly the three required literal identity
fields shown above and preserves their names and values on the wire. Values
must be nonblank and reject edge whitespace, control or surrogate text,
`${...}`, and `$[...]`. Do not add Azure-derived `runId`, `localParams`,
`varPool`, `resourceList`, inherited state, or future fields. There is no public
opaque create/edit selector; excluded existing state is retained only by
unchanged/export operations carrying opaque-preserve provenance.

Route the task to a worker configured with `resource.azure.client.id`,
`resource.azure.client.secret`, `resource.azure.subId`, and
`resource.azure.tenant.id`. Upstream logs task parameters and identity at INFO
but does not log those worker credential values. DATA_FACTORY performs no DS
placeholder substitution, accepts no Azure pipeline runtime parameters or
resource files, and publishes no DS task output. Polling uses worker property
`resource.query.interval`, default `10000` ms.

After Azure `createRun`, a callback stores `runId` in task-instance `appIds`.
Once durable, failover resumes polling and cancellation of the same Azure run
without submitting again. A failure after `createRun` but before callback
persistence leaves no `appIds`; failover or a later DS retry in that window can
submit a duplicate pipeline run. The plugin itself does not retry `createRun`.
Keep task retries at zero unless duplicate Azure runs are acceptable. This
source review refreshes no live evidence, changes no `tested` flag, and
promotes no profile.

`CHUNJUN/literal_local_json_job` has one default template in `Other` on exact
`3.1.0` through `3.4.3`:

```yaml
name: run-chunjun-job
type: CHUNJUN
task_params:
  json: |-
    {
      "job": {
        "setting": {"speed": {"channel": 1}},
        "content": []
      }
    }
```

The only canonical field is the literal JSON-object string `json`. Valid
spelling is preserved; blank or malformed JSON, array/scalar roots,
nonstandard numeric constants, duplicate keys, placeholders, CR, controls,
unpaired surrogates, and additional canonical fields fail closed. Compilation
always emits exactly strict integer `customConfig=1`, unchanged `json`, and
`deployMode=local`. Every upstream executor in this range supports
`localParams` and prepared-map placeholder substitution; the typed template
deliberately omits `localParams` and rejects DS placeholders because the
replacement is not JSON-escaped.

Explicit nonlocal opaque create/edit requires strict integer
`customConfig=1` and exact `standalone`, `yarn-session`, or `yarn-per-job`.
Built-in `customConfig=0`, the upstream UI typo `standlone`, local extras,
parameters/resources, dormant datasource-generation fields, and future state
are unchanged/export preserve-only. Built-in execution is a runtime hole:
`ChunJunTask.buildChunJunJsonFile` never constructs its JSON.
Exact `3.1.0`–`3.1.6` UI default `customConfig=false` is a UI defect; the compiler's
strict integer `1` REST wire remains runnable. The UI default becomes true
in `3.1.7`. Do not derive exact native wire from UI defaults.

Every exact worker normalizes CRLF to LF, substitutes prepared values without
JSON escaping, and writes UTF-8; typed authoring therefore rejects
placeholders. Exact `3.1.0` uses a legacy shell file and nondurable post-exit
application-id log discovery; `3.1.1`–`3.1.9` keep that path but store empty
`appIds`; `3.2.0` through `3.3.2` use the shell interceptor with empty
`appIds`; `3.4.0` through `3.4.3` use `taskRequest` with empty `appIds`.
All reviewed exact upstream `chunjun.md` files require removing the trailing
background `&` from the `nohup` command in
`${CHUNJUN_HOME}/bin/start-chunjun`. Route this template only to a worker using
that foreground launcher; otherwise DS status and cancellation are
untrustworthy. Complete parameters enter INFO logs and expanded JSON enters
DEBUG logs, so do not store secrets here. There is no structured output,
durable id, or failover resume. Cancellation remains worker-local and
best-effort: `3.1.0` through `3.1.9` use legacy wrapper soft/hard kill, `3.2.0`
through `3.2.2` direct-process destroy/force, and `3.3.1` through `3.4.3`
process-tree kill plus generic application cancel. Keep retries at zero unless
replaying the whole job is acceptable.

`DATAX/literal_custom_json_job` has one default template in
`DataIntegration` on every exact profile except `3.1.0`:

```yaml
name: run-datax-job
type: DATAX
task_params:
  json: |-
    {
      "job": {
        "setting": {"speed": {"channel": 1}},
        "content": []
      }
    }
```

The only typed field is `task_params.json`. It must be a nonblank JSON object
string. Arrays/scalars, malformed JSON, nonstandard `NaN`/`Infinity`
constants, duplicate keys at any depth, CR, decoded C0/C1/DEL controls or
decoded unpaired Unicode surrogates, and `${...}`/`$[...]` in source or
decoded keys/values are invalid. Valid spelling and LF formatting
are preserved. Exact `3.4.3` also rejects `{}` and whitespace-only empty
objects: upstream treats them as absent inline JSON and selects resource-file
fallback, which is outside this typed facet. Nonempty literal objects retain
their exact spelling. This facet represents one literal custom job; it does not
promise typed coverage for generated datasource jobs, arbitrary DataX plugins,
DS parameters, or resource files.

Exact `1.3.9` compiles to only `customConfig=1` and `json`. From `2.0.0`, the
compiler also fixes integer `xms=1` and `xmx=1`. It never emits `localParams`
or `resourceList`. Read/import removes exact `localParams=[]` on all profiles
and the UI's generic empty `resourceList` from `3.0.0`. Empty resource state on
`1.3.9`/`2.0.x`, any nonempty parameter/resource state, generated mode,
changed JVM memory, inherited state, and future fields stay opaque and are
preserved only on unchanged/export paths. Positive typed coordinates expose no
raw opaque create/edit selector.

Exact `3.1.0` is an explicit runtime hole. With no global, local, or `varPool`
values, its curing path returns a null prepared map and its custom-command path
dereferences it. Exact `3.1.0` has no typed DATAX default. Its generic
default template is instead a clearly non-executable native scaffold with
strict integer `customConfig=0`; add every exact field required by the chosen
built-in mode before treating it as a raw payload. Raw opaque create/edit
accepts only strict native `customConfig=0` or `1`, while canonical JSON-only
input fails closed. The reason is
`null-empty-prepare-params-map-breaks-custom-command`; exact `3.1.1` adds the
null/empty guard.

Through `3.1.9`, route DATAX to a worker configured with `PYTHON_HOME` or
`python2.7` and `DATAX_HOME/bin/datax.py`. From `3.2.0`, configure
`PYTHON_LAUNCHER` and `DATAX_LAUNCHER`. Matching plugins and JDBC drivers,
source/target connectivity, and data permissions remain operator
prerequisites. Every version replaces CRLF, substitutes prepared values into
the JSON, and writes UTF-8; the replacement is LF through `3.1.9` and the
worker OS separator from `3.2.0`. DataX `-p -D` prepared-map forwarding begins
at `3.1.0`, but the typed contract forbids task parameters and placeholders.
Upstream builds that option without safely quoting shell metacharacters in
workflow, startup, local, or `varPool` values. Use a parameter-free workflow
for this safe typed facet on positive releases from `3.1.1`.

Complete task params, the final command, and child output enter INFO logs, and
DataX job/command construction also appears at DEBUG. Do not put credentials
in this JSON: task fields are not secret storage and dsctl does not redact
them. The task publishes no structured output or durable DataX id and cannot
resume after failover. Cancellation is worker-local—wrapper kill through
`3.1.9`, direct-process destroy through `3.2.2`, then process-tree plus generic
application cancellation. Keep retries at zero unless replaying the entire
transfer and possibly duplicating writes is acceptable. This review adds no
live evidence or profile promotion.

`DATASYNC/create_and_execute` has a default template and the `raw-json` scenario in `Other`
on exact `3.2.0` through `3.4.3`; earlier profiles do not contain the
plugin. The normal default template is typed:

```yaml
name: nightly-transfer
type: DATASYNC
task_params:
  jsonFormat: false
  name: nightly-transfer
  sourceLocationArn: arn:aws:datasync:cn-north-1:123456789012:location/source
  destinationLocationArn: arn:aws:datasync:cn-north-1:123456789012:location/destination
  cloudWatchLogGroupArn: arn:aws:logs:cn-north-1:123456789012:log-group:datasync
```

Normal create/edit owns exactly the four mode fields shown after `jsonFormat`;
`cloudWatchLogGroupArn` is optional and the other three are required. The
selector is a strict boolean and compiles as `false` even when omitted. Values
must be nonblank literals without control text, Unicode surrogates, `${...}`,
or `$[...]`. Do not add `localParams`, `varPool`, resources, raw AWS options,
or future fields to normal typed authoring. Richer existing normal state is
lossless only through unchanged edit or export opaque-preserve provenance.

The upstream normal UI uses one `model.name` for the outer DolphinScheduler
task name and the inner AWS DataSync task name. Create writes both names equal,
and edit merges them back into that one model value. The REST/backend wire can
represent different values; dsctl keeps both fields explicit and does not
impose a UI-only equality rule. Keep them equal only when the task must remain
editable without name loss in the upstream UI.

The discoverable `raw-json` variant is a restricted opaque public mode:

```yaml
name: raw-datasync
type: DATASYNC
task_params:
  jsonFormat: true
  json: |
    {
      "Name": "raw-transfer",
      "SourceLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/source",
      "DestinationLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/destination"
    }
```

Use `dsctl template task DATASYNC --variant raw-json` to discover it. Selection
requires explicit `jsonFormat: true` plus exactly one nonblank, syntactically
valid JSON-object string; normal siblings and additional wrapper fields fail
closed. The CLI preserves the JSON string rather than normalizing its spelling.
At runtime, however, an UpperCamelCase mapper converts only fields known to
upstream `DatasyncParameters` and ignores unknown fields. Only unknown enum
values in inherited `LocalParams`/`VarPool` `Property.Direct/Type` become
`null`. DataSync-specific enum-like strings such as `FilterType` are neither
enum-validated nor null-converted and reach the SDK unchanged for AWS
validation. This is not arbitrary AWS `CreateTask` JSON passthrough.

Known raw fields carry upstream defects. The whole `Options` object is copied
into an immutable AWS SDK value and has no effect, including its string fields.
`Includes` is passed to the excludes builder:
by itself it behaves as `Excludes`, and with both fields it overwrites the real
`Excludes`. A `Schedule` creates a persistent recurring AWS Task. DATASYNC does
not consume the prepared parameter map, so the UI's `localParams` field is
runtime-dead and DS placeholders are unsupported. It accepts no resource files
and publishes no DS task output.

Configure every eligible worker with static basic credentials. Exact `3.2.0`
through `3.2.2` read `resource.aws.access.key.id`,
`resource.aws.secret.access.key`, and `resource.aws.region`. Exact `3.3.1` and
newer instead read `aws.datasync.access.key.id`,
`aws.datasync.access.key.secret`, and `aws.datasync.region`; the later upstream
guide still lists the obsolete `resource.aws.*` keys. There is no datasource,
endpoint, session-token, instance-profile, or default-chain option. Upstream
INFO-logs the complete task parameters and converted parameter object. Task
fields are not secret storage, and dsctl neither detects nor redacts secrets.

Each fresh DS attempt calls `CreateTask` and then `StartTaskExecution`. Only the
resulting `taskExecutionArn` is stored in task-instance `appIds` by a callback;
once durable, failover resumes polling and cancel stops that same execution.
The AWS Task ARN is not stored, the persistent Task is never deleted, and the
client is never closed. Failure before callback persistence, or retry without
durable `appIds`, can create duplicate executions and leak additional
persistent or scheduled Tasks. Polling has no internal deadline. Keep DS
retries at zero unless that duplicate-and-leak window is acceptable. This
review adds no live evidence, changes no `tested` flag, and promotes no profile.

`SAGEMAKER/start_pipeline_execution` has one default template with a short IN hint in
`MachineLearning` on exact `3.1.0` through `3.4.3`, except `3.1.1` and
`3.1.2`; earlier profiles do not contain the plugin. The two excluded releases
discard refreshed status and can poll forever after an initial EXECUTING
state. Typed and raw opaque create/edit are closed there; existing native
state remains unchanged/export preserve-only. Discover the selected exact form
with:

```bash
DS_VERSION=3.1.0 dsctl template task SAGEMAKER
DS_VERSION=3.2.1 dsctl template task SAGEMAKER
```

On datasource-era profiles (`3.2.1` and newer), the default template owns one
literal AWS request object and no parameters:

```yaml
name: start-sagemaker-pipeline
type: SAGEMAKER
task_params:
  sagemakerRequestJson: |-
    {
      "PipelineName": "nightly-training",
      "ClientRequestToken": "0123456789abcdef0123456789abcdef"
    }
  datasource: 1
  localParams: []
```

The `params` form permits DS substitution inside the preserved request text:

```yaml
name: start-parameterized-sagemaker-pipeline
type: SAGEMAKER
task_params:
  sagemakerRequestJson: |-
    {
      "PipelineName": "nightly-training",
      "ClientRequestToken": "0123456789abcdef0123456789abcdef",
      "PipelineParameters": [
        {"Name": "training_job_name", "Value": "${training_job_name}"}
      ]
    }
  datasource: 1
  localParams:
    - prop: training_job_name
      direct: IN
      type: VARCHAR
      value: nightly-training
```

Omit `datasource` on typed releases through `3.2.0`; it is required and
must be positive from `3.2.1`. Compilation always emits `localParams` and
compiler-owned `resourceList=[]`, and adds native `type=SAGEMAKER` only in the
datasource epoch. Without `${...}` or `$[...]`, `sagemakerRequestJson` must be
one valid JSON object. With a placeholder, it may be invalid before execution
because upstream performs whole-text substitution without JSON escaping and
then parses the result. Ensure every resolved value is valid JSON in its exact
position.

The public request shape is limited to `PipelineName`,
`PipelineExecutionDisplayName`, `PipelineParameters[{Name,Value}]`,
`PipelineExecutionDescription`, `ClientRequestToken`, and
`ParallelismConfiguration.MaxParallelExecutionSteps`. Unknown or wrongly
cased keys are ignored; this shape has no enum, so the mapper's
unknown-enum-to-null setting has no effect. Only `IN` local parameters enter
the prepared map through `3.4.0`; from `3.4.1`, `OUT` declarations can also
substitute, but this task never publishes DS output. Raw opaque create/edit is
closed, and richer native state is unchanged/export opaque-preserve-only.

Workers through `3.2.0` use static `resource.aws.*` credentials. Exact `3.2.1`
and `3.2.2` use the required SAGEMAKER datasource. From `3.3.1`, that
datasource remains required and initialized, but the AWS client ignores its
values and instead uses `aws.sagemaker.*` static or instance-profile
configuration. Upstream INFO-logs the full parameters and resolved request;
datasource initialization may also log resolved credentials. Exact `3.1.0`
stores a null application id before start and never backfills the returned ARN.
From `3.1.9`, callback-persisted `appIds` enables later polling and cancel, but
failure before persistence or a retry without `appIds` can submit again. Keep
one stable explicit `ClientRequestToken` for AWS idempotency because an omitted
SDK token may be transient while the plugin's persisted/read token remains
null. Polling has no internal deadline or output, and the client is not closed.
This review adds no live evidence or promotion.

`DMS/resume_existing_full_load` has one default template in the `Cloud`
category from exact `3.2.0` through `3.4.3`:

```yaml
name: resume-existing-full-load
type: DMS
task_params:
  isRestartTask: true
  isJsonFormat: false
  migrationType: full-load
  startReplicationTaskType: resume-processing
  replicationTaskArn: arn:aws:dms:us-east-1:123456789012:task:REPLACE-ME
```

Releases before `3.2.0` do not contain DMS. All five fields are required
and remain explicit on the native wire; the first four accept only the shown
constants. `replicationTaskArn` must be a literal AWS DMS replication-task ARN
without edge whitespace, controls, surrogates, `${...}`, or `$[...]`. Do not
add local parameters, resources, start/create/JSON/CDC configuration, or other
native and future state. There is no public opaque create/edit selector;
excluded state is lossless only on export and unchanged edits carrying
opaque-preserve provenance.

This operation requires a caller-attested actual stopped, previously executed
full-load task. DolphinScheduler validates only the ARN on restart; it cannot
verify the remote migration type or current state. `resume-processing` may
reload target tables that were partially loaded or not yet loaded. If the ARN
instead identifies a CDC task with no `cdcStopPosition`, upstream can report
the DS task successful after start while remote CDC continues. Do not treat
the constants as remote proof. Destructive `reload-target` can truncate or
drop/reload target tables and is deliberately excluded from typed authoring;
existing forms remain opaque-preserve-only.

Workers through `3.2.2` use static `resource.aws.access.key.id`,
`resource.aws.secret.access.key`, and `resource.aws.region`. From `3.3.1`,
configure `aws.dms.credentials.provider.type`, `aws.dms.region`, optional
`aws.dms.endpoint`, and either static
`aws.dms.access.key.id`/`aws.dms.access.key.secret` credentials or the instance
profile. Credentials are worker configuration, not task fields. Upstream logs
full task parameters and remote identifiers at INFO without logging credential
values, polls every `1000` ms with no explicit internal deadline, and exposes
no DS task output.

After AWS accepts the request, a callback persists `replicationTaskArn` into
task-instance `appIds`. Durable `appIds` lets failover resume polling and lets
cancellation stop the same remote task. Failure before callback persistence,
or retry without durable `appIds`, can submit `resume-processing` again. Keep
retries at zero unless that duplicate-resume window is acceptable. This source
review adds no live evidence, changes no `tested` flag, and promotes no
profile.

`ALIYUN_SERVERLESS_SPARK/literal_jar_submit` has one default template in the
`Cloud` category on exact `3.3.1` through `3.4.3`:

```yaml
name: submit-aliyun-serverless-spark-jar
type: ALIYUN_SERVERLESS_SPARK
task_params:
  datasource: 17
  workspaceId: w-analytics-prod
  resourceQueueId: root.analytics
  jobName: daily-orders
  entryPoint: oss://analytics-jobs/jars/orders.jar
  entryPointArguments:
    - --date=2026-08-20
    - --mode=batch
  sparkSubmitParameters: --class com.example.Orders --conf spark.executor.instances=4
  isProduction: false
```

Releases before `3.3.1` do not contain this plugin. Typed create/edit owns only
the eight fields above; `datasource` is a positive integer and
`isProduction` is a strict boolean that defaults to and still emits `false`
when omitted. The other scalar values must remain nonblank literals, and
`entryPoint` must be an absolute `oss://` object URI. The
argument list must contain at least one literal string. Edge whitespace,
controls, Unicode surrogates, `${...}`, `$[...]`, and `#` inside an argument
are rejected.

Projection preserves the owned values, joins `entryPointArguments` with `#`,
and adds fixed native `type: ALIYUN_SERVERLESS_SPARK` and `codeType: JAR`, for
an exact ten-field wire. Native `PYTHON` and `SQL` use explicit opaque
create/edit. Do not add `engineReleaseVersion`, `templateId`, `localParams`,
`varPool`, `resourceList`, richer JAR state, or future fields to typed YAML;
existing values survive only through unchanged/export opaque preservation.

Every execution fetches the optional Aliyun template and may prepend its Spark
configuration, so the final submitted Spark parameters can differ from the
task wire. Because typed authoring omits `engineReleaseVersion`, the returned
template may also derive display release and fusion values for the SDK request.
Exact `3.3.1` retries status only, starts once without a client
token, maps Aliyun `Failed` to DS `KILL`, and logs then swallows cancel
failures. Exact `3.3.2` through `3.4.1` retry template, start, status, and
cancel calls for up to 11 attempts at `1000` ms intervals; they reuse one
client token inside one DS attempt, map `Failed` to `FAILURE`, and do not
preserve start/cancel exception causes. Exact `3.4.2` additionally preserves
start/cancel causes. A DS retry gets a new token and can submit another job.

Select an Aliyun Serverless Spark datasource containing static `accessKeyId`,
`accessKeySecret`, and `regionId`, plus a custom endpoint when needed; the
default endpoint template is `emr-serverless-spark.%s.aliyuncs.com`. The
datasource connectivity check does not make a remote call, so verify the
credentials, endpoint, OSS object access, and Aliyun permissions yourself.
Upstream logs task parameters, `jobRunId`, and state at INFO, so every authored
value can appear in logs. Typed fields are not secret storage, and the CLI does
not detect or redact secrets; never place a secret in `sparkSubmitParameters`,
`entryPointArguments`, or another typed field. Datasource-managed access keys
are not task parameters, and task code does not explicitly log them through
that object.

The submitted id is not persisted by a durable callback. Failover cannot
resume the job, cancellation requires the current worker's in-memory
`jobRunId`, and retry may duplicate submission. Polling runs every 10 seconds
without an internal deadline and produces no DS task output. Keep DS retries
at zero unless duplicate Aliyun jobs are acceptable. This source review adds
no live evidence, changes no `tested` flag, and promotes no profile.

`GRPC/literal_unary_string_record_call` has one default template in the
`Universal` category only on exact `3.4.0` through `3.4.3`:

```yaml
name: call-grpc-echo
type: GRPC
task_params:
  url: grpc.example.internal:7443
  channelCredentialType: TLS_DEFAULT
  serviceName: EchoService
  methodName: Echo
  requestFields:
    - name: account
      number: 1
    - name: region
      number: 2
  responseFields:
    - name: result
      number: 1
  message:
    account: alice
    region: cn
  grpcConnectTimeoutMs: 10000
```

Releases before `3.4.0` do not contain GRPC. On the reviewed releases,
`GrpcLiteralUnaryStringRecordTaskParamsSpec` owns exactly the eight fields
shown above. `url` must be a literal `host:port` without userinfo. Service,
method, and field names are strict package-free proto identifiers. Request and
response records may be empty; otherwise each record requires unique raw names
and valid protobuf field numbers. Protobuf JSON camel-case names must be unique
under proto3's ASCII-case-insensitive collision rule. Every field is a scalar
string, and `message` must contain exactly the
request-field keys with literal-string values. The case-insensitive exact
request-name denylist is `apiKey`, `authToken`, `clientSecret`, `credential`,
`password`, `passwd`, `privateKey`, `secret`, and `token`; response-only names
do not use that denylist. Controls, placeholders, missing or extra message keys,
non-string values, and nonpositive timeouts are rejected. JSON Schema exposes
all structural rules it can express and names runtime-only uniqueness/key-set
checks in `x-dsctl-runtime-validations`; run workflow lint for the final
cross-field decision.

The CLI generates matching package-free proto3 and protobufjs definitions for
fixed `Request` and `Response` records and one blocking unary call. Projection
combines `serviceName/methodName` on the native wire, uses compact message JSON,
fixes `grpcCheckCondition: STATUS_CODE_DEFAULT` and `condition: ""`, emits the
correct Java `channelCredentialType`, and supplies top-level
`taskExecuteType: BATCH`. Do not add a package, streaming method, nested,
repeated or custom type, custom status condition, arbitrary proto/descriptor,
`localParams`, `varPool`, resource, runtime, or future field. Raw opaque
create/edit is unavailable. Existing complex native state survives only on an
unchanged edit or export carrying opaque-preserve provenance.

Choose only `INSECURE` or `TLS_DEFAULT`. `TLS_DEFAULT` uses system trust and
hostname verification; custom CA, mTLS, bearer token, and request
authentication are not supported. Avoid editing a TLS task in the upstream UI:
that UI writes `grpcCredentialType` instead of the Java
`channelCredentialType` field and can silently downgrade the task to
`INSECURE`.

The worker does not substitute DS placeholders and logs the complete parameter
object at INFO, so never put secrets in GRPC fields. It does not transport the
response as DS task output. Cancel is a no-op, and neither a durable id nor
failover resume exists; retry or failover can resend the RPC and duplicate its
side effects. The channel and `NioEventLoopGroup` are not closed, so frequent
calls can accumulate worker resources. Keep retries at zero unless the RPC is
idempotent. This review adds no live evidence, changes no `tested` flag, and
promotes no profile.

`OPENMLDB/literal_single_statement` has one default template in the
`MachineLearning` category from `3.1.0` through `3.4.3`, except `3.1.2`:

```yaml
name: openmldb-single-statement
type: OPENMLDB
task_params:
  zk: zk-1.example.com:2181,zk-2.example.com:2181
  zkPath: /openmldb/production
  executeMode: online
  sql: SELECT 1 AS answer
```

Releases before `3.1.0` do not contain a registered OPENMLDB plugin; orphaned
UI files on `3.0.x` do not make the task available. Exact `3.1.2` leaves the
inherited Python parameters null, so output handling fails after SQL has run.
Typed and raw opaque create/edit are closed on that release; existing native
state remains unchanged/export preserve-only. Retrying can repeat SQL effects.
The typed profiles project exactly the four required fields above without wire
renaming.
`executeMode` accepts only lowercase `offline` or `online`. `zk` must be a
comma-separated DNS/IPv4 `host:port` ensemble with ports from 1 through 65535,
and `zkPath` must be an absolute conservative ZooKeeper znode path. `sql` must
be one nonblank literal statement safe inside the Python source generated by
upstream: do not use a semicolon, double quote, backslash, carriage return,
unsafe control text, `${...}`, or `$[...]`.

Do not add `localParams`, `varPool`, `resourceList`, inherited `rawScript`,
runtime state, or future fields to typed OPENMLDB. There is no public opaque
create/edit selector; existing excluded or unrecognized native state is
lossless only on export and unchanged edits that retain its opaque-preserve
provenance. Route exact `3.1.x` tasks to workers configured through
`PYTHON_HOME`; releases from `3.2.0` use `PYTHON_LAUNCHER`. The worker also
needs Python 3, `openmldb`, SQLAlchemy and its OpenMLDB driver, ZooKeeper
reachability, and target-data permissions. `offline` enables synchronous jobs
with a fixed `1800000` ms timeout.

Upstream logs the complete task parameters, raw SQL, rendered Python, and
final generated Python file at INFO. Do not put secrets in any OPENMLDB task
field. The SQLAlchemy result is discarded rather than published as a DS task
output. Execution is a synchronous local Python process with no durable
application id or failover resume; retry runs the statement again and may
repeat side effects. This source review refreshes no live evidence, changes no
`tested` flag, and promotes no profile.

`DINKY/job_trigger` has one default template from `3.1.0` through `3.4.3` and
is absent upstream on earlier profiles. Supply only its exact native
core fields:

```yaml
name: trigger-dinky-job
type: DINKY
task_params:
  address: https://dinky.example.com
  taskId: daily-job_7
  online: false
```

`address` is a required literal HTTP(S) endpoint with a host and optional
literal port/path. It rejects URI userinfo, query, fragment, whitespace,
control text, `${...}`, and `$[...]`. `taskId` is a required nonblank literal
identifier and rejects whitespace, control text, placeholders, and
shell-control syntax. `online` is optional, defaults to `false`, and accepts
only a real YAML boolean rather than `0`, `1`, or string coercions.

Do not add `localParams`, `varPool`, or future native fields to typed DINKY,
even as `null`. There is no public raw opaque-authoring selector. Existing
excluded state remains lossless only through export and an unchanged edit;
invalid canonical input never downgrades to opaque create/edit.

Parameter forwarding changes by exact worker version:

- through `3.2.0`, the legacy path forwards no workflow or task variables;
- from `3.2.1`, the worker negotiates the remote Dinky version; only a
  negotiated Dinky 1.x `submitApplicationV1` branch forwards variables, while
  the negotiated legacy/v0 branch still forwards none;
- on that v1 branch, `3.2.1` and `3.2.2` send workflow globals and task
  `localParams`;
- on that v1 branch, `3.3.1` through `3.4.1` additionally expand local
  placeholders from the prepared parameter context;
- on that v1 branch, `3.4.2` sends the complete prepared map—built-in, project,
  workflow, task, command, `varPool`, and business values—to Dinky and logs
  that map at INFO.

Upstream also logs task parameters, request URLs, and response content. The
request has no authentication or explicit HTTP timeout, so expose the Dinky
endpoint only through an appropriately protected network path and never put
credentials or secret values in this task or its forwarded parameters. The
plugin keeps no durable submitted-job id and cannot resume after failover.
Retry can submit the same job again; cancellation addresses `taskId`, not a
unique submitted-run handle. This source-reviewed facet is not a
confidentiality, live-execution, or profile-promotion claim.

`HIVECLI` has one default template with a short IN binding hint from `3.1.0`
through `3.4.3` and is absent upstream on earlier profiles. Typed authoring owns only
inline `SCRIPT`: a nonblank `hiveSqlScript`, optional literal
`hiveCliOptions`, and unique `IN`/`VARCHAR` `localParams`. DS placeholders are
supported in SQL but forbidden in options because upstream behavior changes:
`3.1.x` substitutes the assembled `hive -e` command, whereas `3.2.0` and newer
substitute SQL only, write a temporary file, and invoke `hive -f`. Before
execution, install the Hive CLI on every eligible worker and provide Hive/HDFS
client configuration plus access to HDFS and the Hive Metastore. HIVECLI does
not select a DS datasource, and the CLI does not provision this runtime.
`FILE`, `resourceList`, `varPool`, runtime output, and future native fields are
available only through lossless opaque preservation.

`DVC/operation` has `upload`, `download`, and `init` variants from `3.1.0`
through `3.4.3` and is absent upstream on earlier profiles. Use the
exact native `dvcTaskType` values `Upload`, `Download`, and `Init DVC`. Upload
requires repository, DVC location, worker path, version, and message; Download
requires the same fields except message; Init DVC requires repository and
remote store URL. Omit fields inactive for the chosen mode.

Upstream builds and logs a POSIX shell script and leaves repository, location,
worker path, version, and store URL unquoted. Typed values must therefore be
one conservative shell-safe token: whitespace, globbing, option prefixes,
shell expansion, control text, and URI userinfo are rejected. Version accepts
only a portable Git tag, branch, or SHA. Messages may contain spaces, single
quotes, and Unicode, but double quote, backslash, shell expansion, and control
text are rejected.
No typed DVC field accepts `${...}` or `$[...]`; `localParams`, `varPool`,
resources, runtime output, inactive values, and future native fields remain
available only through lossless opaque authoring.

Route DVC tasks to POSIX workers with `git` and `dvc` in `PATH`, configured Git
identity, repository permissions, DVC remote support, network access, and
credentials supplied through the worker's SSH agent, credential helper, or
provider configuration. Never embed credentials in task URLs. DVC has no
resumable remote job identity or failover protocol, and its upstream script
does not use `set -e` or guard every intermediate Git/DVC operation; inspect
logs because a successful final command can mask an earlier failure. The
upstream UI's `taskType: MLFLOW` initialization typo does not change the exact
wire type `DVC`.

`MLFLOW/model_serve` has one default template from `3.1.0` through `3.4.3`
and is absent upstream on earlier profiles. This narrow tracer is not
full MLFLOW authoring. Supply exactly these five required native fields:

- `mlflowTaskType: "MLflow Models"`
- `deployType: "MLFLOW"`
- `mlflowTrackingUri`: absolute HTTP(S), with a host and optional safe path
- `deployModelKey`: `models:/name/version-or-stage` or
  `runs:/run-id/artifact[/subpath...]`
- `deployPort`: a quoted decimal string from `"1"` through `"65535"`

The URI must not contain userinfo, query, fragment, whitespace, control text,
placeholders, quotes, backslash, globbing, or shell metacharacters. Every model
key component must be nonempty and conservative; empty, `.`, and `..`
components are rejected. These bounds are intentional because upstream
interpolates the URI, model key, and port into a POSIX shell script without
quoting them.

Do not add Projects or Docker mode fields, `localParams`, `varPool`,
`resourceList`, `registerModel`, runtime state, or future native members to a
typed MLFLOW task, even as `null`; existing values remain lossless only through
opaque authoring and export. Route the task to a POSIX worker with the `mlflow`
CLI in `PATH`, tracking-server and artifact-store access, access to the selected
registered-model version or run artifact, an available port, and credentials
configured through the worker environment or runtime configuration. Never put
credentials in URI userinfo. The service is a foreground local process bound
to `0.0.0.0`; cancellation has no remote deployment ID, reconnect, or failover
protocol. A retry can collide with a surviving process or occupied port. This
typed review adds no live evidence and promotes no profile.

`JUPYTER/preinstalled_notebook` has one default template with a short literal-map hint from
`3.1.0` through `3.4.3` and is absent upstream on earlier profiles.
It runs one notebook in an already installed conda environment. Required
fields are a shell-safe `condaEnvName` plus distinct absolute POSIX-safe
`.ipynb` `inputNotePath` and `outputNotePath` values. The optional `parameters`
mapping contains literal shell-safe string keys and values. `kernel` and
`engine` are optional single safe tokens; `executionTimeout` and `startTimeout`
are optional strict positive YAML integers.

```yaml
name: execute-daily-notebook
type: JUPYTER
task_params:
  condaEnvName: analytics-py310
  inputNotePath: /opt/notebooks/daily-input.ipynb
  outputNotePath: /opt/notebooks/daily-output.ipynb
  parameters:
    business_date: "2026-08-20"
  kernel: python3
  engine: nbclient
  executionTimeout: 900
  startTimeout: 60
```

The CLI omits an empty parameter map from the wire, serializes a nonempty map
as sorted compact native JSON, and converts timeout integers to native decimal
strings. This typed facet does not bootstrap an environment: `.tar.gz` and
`.txt` `condaEnvName` forms, `resourceList`, `others`, `localParams`,
`varPool`, placeholders, runtime state, and future native fields are rejected
by typed create/edit. An exact lowercase `.txt` or `.tar.gz` environment name
is the only public raw selector; it preserves the bootstrap payload and its
native companion fields through opaque create/edit. Other excluded native
values remain lossless only during export and unchanged edits. Invalid
preinstalled canonical input does not downgrade to opaque. The raw selector
uses upstream's case-sensitive suffix check; typed validation rejects
suffix-like names case-insensitively, so uppercase forms do not select raw
mode.

The parameter map rejects URI userinfo and the case-insensitive exact key names
`password`, `passwd`, `secret`, `token`, `credential`, `api_key`, `access_key`,
and `private_key`. This bounded denylist is not comprehensive secret detection,
and the CLI does not redact authored values. Upstream may log every task value
and the assembled command, so never use Jupyter task parameters as secret
storage.

Route the task to a POSIX worker with `conda.path` configured, the selected
environment plus Papermill, Jupyter, kernel, and engine already installed, and
the input/output notebook paths readable/writable as applicable. The output
notebook is a worker filesystem artifact, not a DS task output. The plugin has
no remote application id or failover-resume protocol, and a retry executes the
notebook again. This reviewed facet adds no live evidence and does not promote
a profile.

`ZEPPELIN/paragraph` is available from `3.0.0` through `3.4.3` and absent on
earlier profiles. The default template adds a short literal parameter-map
hint from `3.1.0`. It executes exactly one paragraph and requires
URL-segment-safe `noteId` and `paragraphId`. The exact selected version fixes
the only valid connection mode:

| Exact profiles | Canonical input | Native connection |
| --- | --- | --- |
| `3.0.0`–`3.0.6` | `connectionMode: WORKER_CONFIG` | The wire carries only the ids; configure worker `zeppelin.rest.url`. |
| `3.1.0` through `3.2.0` | `connectionMode: REST_ENDPOINT` plus `restEndpoint` | The wire carries the literal anonymous HTTP(S) endpoint. |
| `3.2.1` through `3.4.3` | `connectionMode: DATASOURCE` plus positive `datasource` | The wire carries the id and injected `type: ZEPPELIN`. |

`connectionMode` is a canonical selector and is removed from the DS wire. A
REST endpoint must be absolute HTTP(S) without whitespace, credentials, query,
fragment, control text, or DS placeholders. For datasource mode, use a
passwordless/anonymous ZEPPELIN datasource. The CLI validates the positive id
but does not inspect or certify the datasource's authentication mode, so this
is a user runtime prerequisite rather than a verified CLI property.

The canonical `parameters` value is a YAML mapping of literal string keys to
literal string values:

```yaml
name: zeppelin-paragraph-params
type: ZEPPELIN
task_params:
  noteId: 2FZ4VC2MX
  paragraphId: paragraph-1
  connectionMode: DATASOURCE
  datasource: 17
  parameters:
    business_date: "2026-08-20"
```

The CLI serializes that map to the native JSON string on `3.1.0` and newer;
an empty map is omitted. On `3.0.x`, `parameters: {}` remains the stable
canonical field but the schema requires it to stay empty and the projector
omits it. Typed keys and values reject control text, `${...}`, and `$[...]`.
The CLI does not detect or redact arbitrary secret-like values; do not use this
map as secret storage.

Where upstream owns them, whole-note execution, note cloning through
`productionNoteDirectory`, native username/password, datasource authentication
state, `localParams`, `varPool`, `resourceList`, runtime output, and future
native fields are not typed. Existing native forms remain lossless only through
opaque authoring and preservation. This boundary is also a logging precaution:
exact `3.2.0` logs raw task parameters containing inline credentials, `3.2.1`
logs the resolved datasource username after login, and `3.2.2` and newer log
the resolved parameter object including datasource credentials.

The executor performs one synchronous paragraph call and exposes no reliable
remote application id, reconnect, or failover-resume protocol. Keep retries at
zero unless the paragraph is safe to run more than once. Exact `3.4.1`
constructs `taskName.result` without reliable downstream publication; `3.4.2`
publishes that runtime value through the var pool. Neither result is authored
in typed YAML. This source-reviewed facet adds no live evidence and does not
promote any profile.

`PROCEDURE` has one default template with a short positional binding hint. Author one
canonical positional JDBC call such as `{call reporting.refresh_daily(?,?)}`;
the CLI projects it to the selected release's native method syntax. Each `?`
matches one `localParams` entry in source order. Procedure parameters accept
the JDBC scalar types documented by `dsctl task-type schema PROCEDURE`, not
general `LIST` or `FILE` values.

`EMR` has reviewed typed authoring from `3.0.0` through `3.4.3` and is absent
upstream on earlier profiles. Its default creates a job flow from
`3.0.0`; `add-steps` begins at `3.1.0`; a short IN request binding hint begins at
`3.2.2`. Canonical `jobFlowDefineJson` and `stepsDefineJson` values remain raw
JSON strings: the CLI validates literal JSON without converting it into a
CLI-owned AWS request object or reformatting the text. Exact `3.0.x` accepts
only `RUN_JOB_FLOW` and compiles it without a native `programType` field.
Releases from `3.1.0` support both `RUN_JOB_FLOW` and
`ADD_JOB_FLOW_STEPS`; a statically parseable ADD request must contain exactly
one `Steps` entry. DolphinScheduler substitutes `${...}` and `$[...]` in the
raw JSON text only from `3.2.2`. EMR `localParams` must be unique
`IN`/`VARCHAR` properties, and typed EMR does not expose `varPool`.

Before running EMR tasks, configure AWS access in DolphinScheduler for the
selected exact release:

- `3.0.0` worker configuration: `aws.access.key.id`,
  `aws.secret.access.key`, and `aws.region`
- `3.0.6` through `3.2.2` worker configuration:
  `resource.aws.access.key.id`, `resource.aws.secret.access.key`, and
  `resource.aws.region`
- `3.3.1` and newer: AWS authentication through `aws.yaml` `aws.emr.*`

Do not put those credentials in workflow YAML; the CLI does not provision or
store them. Upstream EMR task failover is not implemented, and typed authoring
does not supply a separate EMR failover or high-availability layer.

`EMR_SERVERLESS/start_job_run` has one default template with a short IN hint on
exact `3.4.2`–`3.4.3`; earlier profiles do not contain the plugin. Its typed
params are literal `applicationId`, `executionRoleArn`, optional `jobName`, raw
`startJobRunRequestJson`, and unique `IN`/`VARCHAR` `localParams`. DS
substitutes `${...}` and `$[...]` only in that raw request. Keep unresolved
runtime placeholders inside quoted JSON strings; exact local-parameter
substitution must still leave a JSON object. Top-level values stay literal and
override matching JSON members.

Eligible workers need `aws.emr.*` credentials and region or a usable AWS SDK
default credential chain, access to a pre-existing `STARTED` or `CREATED`
application, and the required service and job-data permissions. Custom
endpoints use `emr.serverless.endpoint` or `EMR_SERVERLESS_ENDPOINT`, not
`aws.emr.endpoint`. The plugin persists `jobRunId` through task-instance
`appIds` and resumes polling after failover. Do not author those runtime
values; runtime and future native fields remain opaque-preserve-only. This
typed review adds no live evidence and does not promote either profile.

Useful variants:

```bash
dsctl template task SHELL --variant resource
dsctl template task SHELL --variant output
dsctl template task PYTHON --variant resource
dsctl template task PYTHON --variant output
dsctl template task SQL
dsctl template task SQL --variant output
DS_VERSION=3.2.1 dsctl template task HTTP --variant post-json
dsctl template task HTTP
dsctl template task SWITCH
dsctl template task CONDITIONS
dsctl template task DEPENDENT
dsctl template task DEPENDENT --variant task-dependency
DS_VERSION=1.3.9 dsctl template task SUB_WORKFLOW
dsctl template task SUB_WORKFLOW
dsctl template task REMOTESHELL
dsctl template task REMOTESHELL --variant output
DS_VERSION=3.2.2 dsctl template task PIGEON
DS_VERSION=3.1.0 dsctl template task DINKY
DS_VERSION=3.1.0 dsctl template task HIVECLI
DS_VERSION=3.1.0 dsctl template task DVC --variant upload
DS_VERSION=3.1.0 dsctl template task DVC --variant download
DS_VERSION=3.1.0 dsctl template task DVC --variant init
DS_VERSION=3.1.0 dsctl template task MLFLOW
DS_VERSION=3.1.0 dsctl template task JUPYTER
DS_VERSION=3.1.0 dsctl template task OPENMLDB
DS_VERSION=3.1.9 dsctl template task FLINK
DS_VERSION=3.1.9 dsctl template task FLINK_STREAM
DS_VERSION=3.2.0 dsctl template task JAVA
DS_VERSION=3.4.1 dsctl template task MR
DS_VERSION=3.4.1 dsctl template task SQOOP
DS_VERSION=3.4.1 dsctl template task DATAX
DS_VERSION=3.4.1 dsctl template task CHUNJUN
DS_VERSION=3.2.0 dsctl template task DATA_FACTORY
DS_VERSION=3.1.0 dsctl template task SAGEMAKER
DS_VERSION=3.2.1 dsctl template task SAGEMAKER
DS_VERSION=3.2.0 dsctl template task DMS
DS_VERSION=3.3.1 dsctl template task ALIYUN_SERVERLESS_SPARK
DS_VERSION=3.4.0 dsctl template task GRPC
DS_VERSION=3.0.0 dsctl template task SPARK
DS_VERSION=3.0.0 dsctl template task ZEPPELIN
DS_VERSION=3.1.0 dsctl template task ZEPPELIN
dsctl template task PROCEDURE
dsctl template task EMR
dsctl template task EMR --variant add-steps
DS_VERSION=3.4.2 dsctl template task EMR_SERVERLESS
```

Source-known but not-yet-typed DS task types retain raw `task_params: {}`
templates where the exact profile permits opaque authoring. Export/edit also
preserves unrepresentable DS-native runtime fields opaquely so unrelated
workflow changes remain lossless. That opaque preservation also retains
additional or future native `PIGEON`, `HIVECLI`, `DVC`, `MLFLOW`, `JUPYTER`,
`OPENMLDB`, `DINKY`, `FLINK`, `FLINK_STREAM`, `JAVA`, `MR`, `SQOOP`, `DATAX`,
`CHUNJUN`,
`DATA_FACTORY`, `SAGEMAKER`,
`DMS`,
`ALIYUN_SERVERLESS_SPARK`, `GRPC`, `SPARK`, `ZEPPELIN`,
`PROCEDURE`, `EMR`, and `EMR_SERVERLESS` fields without claiming they belong
to typed authoring. Typed membership is a static reviewed contract; it does not
by itself add live evidence, change `tested`, or promote the selected profile.

## Workflow Patch YAML

`workflow edit` accepts two input modes:

- `--patch`: a small delta document rooted at `patch:`
- `--file`: a full desired-state workflow YAML document

Patch YAML is not a REST `PATCH` request; the CLI applies the delta to a live
DAG snapshot, validates the merged workflow, then compiles the whole DS-native
form payload. Use patch YAML for precise changes, especially task rename/delete
flows and finished-instance repair.

Full-file edit treats the YAML as the desired complete workflow definition:
same-name tasks preserve DS task identity, YAML-only tasks are created, and
live-only tasks are deleted after `--confirm-risk`. Full-file edit does not
infer task renames. Use patch `tasks.rename[]` when a task name changes and the
DS task `code + version` must be preserved.

`workflow edit --file` accepts the `schedule:` block emitted by workflow export
as a read-only snapshot. It verifies that snapshot against the authoritative
attached schedule, removes it before compiling the workflow definition update,
and never changes schedule state or configuration. Changed or missing schedule
state fails before any workflow mutation. Use `schedule update|online|offline`
for intentional schedule changes. Deleting or nulling a field inside an exported
schedule is a mismatch, not a request to ignore it; remove the complete block
only when snapshot validation is intentionally unnecessary.

Start from the matching patch template:

```bash
dsctl template workflow-patch --raw > patch.yaml
dsctl template workflow-instance-patch --raw > instance-patch.yaml
```

Use full-file edit when the entire definition should be reconciled:

```bash
dsctl workflow export daily-etl --project etl-prod > workflow.yaml
# edit workflow definition and tasks; leave schedule unchanged
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml --dry-run
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml
```

For a finished workflow instance repair, export the instance DAG instead of the
current workflow definition:

```bash
dsctl workflow-instance export 901 --project etl-prod > instance.yaml
# edit the failed instance DAG
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml --dry-run
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml
```

`workflow-instance edit --file` uses the same task YAML shape as workflow
authoring, but it is intentionally narrower at the workflow level: only
`workflow.global_params` and `workflow.timeout` may change. Keep
`workflow.project` matching the instance project. Remove `schedule:` blocks;
instance repair does not manage schedule lifecycle.

On exact `1.3.9`, a newly authored canonical `SUB_WORKFLOW` in an instance
repair uses the same `childWorkflowName` contract as workflow authoring. The
service resolves that literal name in the current containing definition's
project and freezes its positive id; it never treats a workflow code as the
legacy database id. The legacy graph compiler emits the final instance graph,
after which the service iteratively audits every task
`params.processDefinitionId`, including preserved native tasks.

```yaml
patch:
  workflow:
    set:
      description: "Updated workflow description"
      timeout: 3600
  tasks:
    create:
      - name: transform
        type: SHELL
        command: |
          echo transform
        depends_on:
          - extract
    update:
      - match:
          name: load
        set:
          depends_on:
            - transform
    rename:
      - from: old-load
        to: load
    delete:
      - obsolete
```

`tasks.create[]` uses the same task item shape as full workflow YAML. Start
from `dsctl template task TYPE --raw` and inspect bounded task fields with
`dsctl task-type schema TYPE`. `tasks.update[].match.name` matches the live
task name before the patch is applied; `tasks.update[].set` is a partial task
object and omitted fields keep their live values.

Use `tasks.rename[]` when a task name changes and task identity should be
preserved. The CLI does not guess rename intent from delete/create pairs.

`workflow-instance edit` intentionally accepts only instance-safe workflow
fields: `global_params` and `timeout`. Definition fields such as name,
description, execution type, and release state belong to `workflow edit`.

## Validation

Always validate generated YAML locally before sending it to DolphinScheduler:

```bash
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --dry-run
```

`lint workflow` checks the stable YAML model, task semantics and graph. It also
checks the local authoring plan when references can be bound without remote
reads; datasource names explicitly defer that step to preview. Lint does not
imply that remote workflow creation has passed that version's compatibility gate.
`--dry-run` shows the selected-version exact request plan, schedule preview
when present, and any risk confirmation token required before mutation. Apply
uses the same generated definition-plan path and executes the request it
prepares for that invocation.
