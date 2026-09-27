# Version Compatibility

Omitting `DS_VERSION` or setting it to `auto` enables discovery for a configured
target. Reviewed metadata can identify an exact release; otherwise public API
contracts may admit bounded reads without identifying the server's release.
An explicit exact `DS_VERSION` overrides discovery; common spellings such as
`v3.4.1` and `ds_3_4_1` are normalized. Automatic selection does not change a
profile's support level.

Reviewed releases before `3.2.0` need an explicit version for writes and
authoring. Stock `3.2.2` likewise needs `DS_VERSION=3.2.2` for those operations
because its database version metadata reports `3.3.0`. Reviewed reads can still
be admitted when the API documents satisfy their contracts. See
[Version Selection](configuration.md#version-selection) for exact metadata,
read admission, offline discovery, cache behavior, and failures.

## Current Support Matrix

The CLI includes exact profiles for all 37 final releases from `1.3.9` through
`3.4.3`. The prerelease `3.3.0-alpha` is excluded. Each release has its own
source identity, contracts and reviewed task boundaries; no nearest-version
selection is used. The [intermediate source audit](../development/intermediate-release-source-audit.md)
and [admission record](../development/stable-release-admission.md) describe the
21 added profiles and their differences.

The [current action inventory](../development/architecture.md#current-stable-surface)
records the installed CLI surface and aggregate availability decisions. Each
selectable exact profile gives every action a terminal decision. Use
`dsctl capabilities` for the selected profile's supported, limited and
unsupported counts; these do not represent live-test counts.

| Exact server releases | Profiles | Contract family |
| --- | --- | --- |
| `1.3.9` | 1 | `process-definition-1.3` |
| `2.0.0`–`2.0.9` | 10 | `process-definition-2.0` |
| `3.0.0`–`3.0.6` | 7 | `process-definition-3.0` |
| `3.1.0`–`3.1.9` | 10 | `process-definition-3.1` |
| `3.2.0`–`3.2.2` | 3 | `process-definition-3.2` |
| `3.3.1`, `3.3.2`, `3.4.0`–`3.4.3` | 6 | `workflow-3.3-plus` |

Family names group related contracts. Select the actual server version to use
its exact fields, enums and runtime behavior.

## Support Policy and Verification

These are separate facts:

- **Action availability** records whether a specific operation is supported,
  limited or absent upstream on the selected release. Check
  `dsctl capabilities --action ACTION` for that operation's constraints.
- **Live verification** records a tested scenario against a specific wheel,
  source and environment. Each receipt identifies its actions and artifact.
- **Profile support policy** is the recorded release designation. The current
  metadata retains `3.4.1` as `full` with `tested=true`, historically called the
  stable target; the other 36 profiles retain `experimental` with `tested=false`.
  These labels record release-policy decisions. The profile-level `tested`
  flag belongs to that policy; per-action verification and receipts describe
  the behaviors exercised by tests.
- **Offline default** selects `3.4.1` only when no URL, token or version is
  configured. Configured targets use explicit version selection or discovery.
  See [Version selection](configuration.md#version-selection).

All 37 profiles passed the shared read and core scopes listed below. Support
labels remain separate metadata used by diagnostics and release checks;
changes follow an explicit policy review with matching evidence.

### Recorded Live Coverage

The historical `0.4.0` development candidate has these receipts on one wheel:

| Scenario | Exact releases | Verified scope |
| --- | --- | --- |
| [Exact reads](../development/live-evidence/exact-read/) | All 37 | Four read actions |
| [Core conformance](../development/live-evidence/conformance-bundles/) | All 37 | The same 18-action `full_core/v1` bundle |
| [External SHELL preservation](../development/live-evidence/external-shell/3.4.2/) | `3.4.2` | A 15-action bundle including mutation and restoration of a pre-existing task |

The additional SHELL scenario verifies restoration of the original command,
non-owned fields, dependencies and DAG topology. Its reviewed test target is
`3.4.2`; the suite establishes that restoration contract. Action counts overlap
across bundles, so assess coverage using each suite's named operations. See the
[live gate contracts](../development/live-testing.md#exact-version-profile-gates)
for detailed obligations and other scenario-specific evidence.

These receipts attest the listed scenarios on a historical candidate built
before subsequent runtime fixes. Each new release candidate completes its own
artifact-bound acceptance.
Historical admission evidence is recorded in the
[admission decisions](../development/stable-release-admission.md).

The current generic exact-read promotion format is semantic schema 2; generic
schema-1 receipts remain historical-audit-only. The `external-shell/v1` format,
currently reviewed for `3.4.2`, is semantic schema 7; its schema-3 through
schema-6 receipts remain historical-audit-only. These receipt schema numbers are
separate from the cluster, fixture, and image-inspection input manifest schemas.

## PostgreSQL resource prerequisite

The upstream DS `2.0.0`–`2.0.2` fresh-install PostgreSQL schemas declare
`t_ds_resources.is_directory` as `int`, while their Java resource entity writes a
`boolean`. Resource creation can consequently fail with generic upstream code
`10061`; confirm the column-type mismatch in the server log before attributing
that code to this defect.

DS `2.0.3` corrects the column to `boolean DEFAULT FALSE` in both its fresh-install
schema and `sql/upgrade/2.0.3_schema/postgresql/dolphinscheduler_ddl.sql` under
`dolphinscheduler-dao/src/main/resources/`. A server administrator can apply that
specific upstream schema correction to an affected installation after checking
existing data. The CLI uses the native REST contract and does not migrate the
database. Record the correction alongside live acceptance evidence; it does not
by itself change the CLI profile's support or verification level.

## Local resource storage in DS 3.3.1–3.4.3

These releases have an upstream resource-listing defect with the documented
`HDFS` backend configured with `file:///` as its default filesystem. Hadoop
returns file paths as `file:/...`, but the server compares that string against
its `file:///...` storage prefix. Listing nonempty directories can therefore
fail with native `10057` and `Invalid resource path`, even after successful
file creation. The CLI cannot correct this server-side comparison by changing
the request path.

The native `LOCAL` backend avoids this HDFS URI conversion. Switching backends
requires checking existing resource identities, shared storage and consistent
API/worker configuration, followed by the appropriate service restart. A test
with the URI defect remains a failed scenario until the chosen backend passes
the complete lifecycle; successful individual mutations do not establish list
or download coverage.

## Task-group cleanup boundaries

Reviewed DS `3.0.0`–`3.4.3` profiles support task-group operations but expose no
individual task-group DELETE endpoint. `task-group close` disables a group;
it does not remove it.

On DS `3.2.0`–`3.4.3`, deleting the owning project cascades to its task groups
and their queue records. On DS `3.0.0`–`3.1.9`, deleting either the project or
the creator user leaves the task groups behind. Do not use project deletion
as cleanup for temporary task-group experiments on those older releases.
Such live tests need a disposable environment or an explicitly reviewed
state-restoration procedure. A closed or no-longer-visible group is not proof
of deletion.

## Reading the Matrix

Terminal does not mean supported, supported does not mean live-tested, and a
live-smoked action does not promote its entire profile. `dsctl capabilities
--action ACTION` returns the selected version's public availability
(`supported`, `limited`, or `unsupported`), its strongest verification level,
and a concrete constraint for non-supported actions. The bounded default
capabilities view reports aggregate counts; use `--full` only for an explicit
all-action audit. `dsctl schema --command ACTION` describes invocation
syntax and repeats the same action fact as a convenience.

For example, `3.2.2` supports `project-worker-group.list` and nonempty `set`,
but `clear` is limited: its API rejects empty assignments with `1402003`.
The CLI blocks clear before transport. Clearing is supported from `3.3.1`;
changing the selected profile cannot add that capability to an older server.

`workflow-instance.execute-task` is limited on `3.3.1`, `3.3.2`, `3.4.0` and
`3.4.1`: the API can insert an `EXECUTE_TASK` command, but the master has no
handler to execute it. The CLI rejects it before dispatch and retains its wire
contract for source auditing. The action is supported on `3.2.0`–`3.2.2` and
`3.4.2`–`3.4.3`; earlier profiles lack the endpoint. Use a server with a working
implementation rather than selecting a different profile for the same server.

Named support-tier conformance bundles add a separate installed-wheel release
gate over that action catalog:

| Bundle | Required actions | Static readiness | Terminal blockers |
| --- | ---: | ---: | --- |
| `legacy_core/v1` | 9 | 37 of 37 profiles | none |
| `full_core/v1` | 18 | 37 of 37 profiles | none |

`full_core/v1` inherits all nine legacy actions and adds six workflow
definition actions plus `task.list`, `task.get`, and `task.update`.
Static readiness means every required action has an executable accepted recipe
in the generated profile. Release acceptance additionally needs installed-wheel
scenarios, strict receipt validation, a complete same-wheel corpus and the
quality and release gates. A passing bundle does not promote an entire profile
or verify every authoring facet.

The [live evidence policy](../development/live-testing.md#exact-version-profile-gates)
defines current gate schemas and their fixed action scopes. Prior receipts are
retained under [live-evidence](../development/live-evidence/), including historical
read and mutation formats. Each attests only its recorded wheel, source,
manifest, action scope and cleanup. Historical schema-3 through schema-6 task
mutation receipts do not satisfy the current schema-7 stale-plan requirement.
Source review, a successful development check or an older receipt cannot refresh
those bindings for a new package.

Version capability, cluster/plugin configuration, current-user permission, and
runtime state are separate facts. A permission or cluster-state failure is not
evidence that a server capability is absent.

### Exact 3.4.3 changes

`3.4.3` keeps the existing command tree and 35 reviewed typed task families.
User and datasource authorization lists now return identity-only records.
Permission and token operations consume their native id/name fields; resolving
the current user needs only `users/get-user-info`. Other users are selected
through the admin-only enabled-user list, with paging only when an identity is
absent from that list. User management retains full user reads and their native
permission requirements.

The removed cluster query-by-code route is replaced by exact-code selection
across all native cluster pages, including mutation readback. Audit requests
retain their filters; the server returns all actors to admins and only the
current actor to other users. These adapters do not infer broader authorization.

The DEPENDENT `ALL` key roundtrip is repaired without changing date windows or
adding task facets. DataX treats an empty JSON object as absent inline content
and reads a resource file instead. The typed literal facet therefore rejects
`{}` on `3.4.3`; resource-file jobs remain opaque preservation. The shared task
API recognizes SIGINT exit code 130 as killed, which adds no durable identity or
failover resume. These source changes add no live-verification or promotion
claim.

## Exact Compatibility Model

The CLI keeps one stable command tree and one workflow vocabulary while exact
generated contracts and bound domains own version-specific routes, request
fields, result shapes, and execution recipes. An older irregular API may be
normalized only when it preserves the stable meaning. The CLI fails before
transport when an action would require guessing, silently dropping data, or
changing promised semantics.

Reviewed semantic operations are projected into the complete current action
catalog for each of the 37 exact versions. Mechanical source facts, terminal
action decisions, executable runtime support, verification evidence, and profile
promotion remain separate layers. This prevents broad generated-package reuse
or a passing narrow smoke test from becoming an accidental whole-version
compatibility claim.

### Task-definition boundaries

The task-definition contract follows upstream identity and mutation epochs:

- `1.3.9` task identity remains string-native. Task `list` and `get` project
  native ids and exact-name selection from the containing definition graph;
  task `update` preserves the whole graph and unknown fields instead of
  fabricating code/version identity.
- `2.0.0` through `2.0.2` update tasks through the complete owning workflow,
  preserving task and relation-bound versions; dependency edits use that same
  whole-workflow path.
- `2.0.3` also uses whole-workflow task updates for ordinary fields, but rejects
  explicit `depends_on` changes. Its definition service can silently ignore a
  relation rewire when the number of relation rows is unchanged. The CLI
  conservatively rejects workflow-definition relation changes on this exact
  profile; ordinary fields and task renames that preserve native identity
  remain available. Workflow creation and instance editing use different
  upstream save paths and retain their own contracts.
- `2.0.4` through `3.2.0` use the standalone task endpoint for ordinary fields
  and reject explicit `depends_on` changes before I/O. Before preparing and
  again before applying the mutation, the CLI inventories all project
  workflows and requires the task to belong only to the selected workflow.
  Use whole-workflow edit for dependency changes on these profiles.
- `3.2.1` and newer support the reviewed dependency-update route. `3.4.1` through
  `3.4.3` use the project-scoped main route rather than the removed V2 route.
- Removing the final upstream relation is not representable through the main
  task update path on affected profiles; use workflow edit.

Task-plugin source facts cover all 37 releases, but source extraction is not an
executable typed-authoring claim. The exact positive matrix contains
42 distinct reviewed typed task families and 902 exact version/family
memberships:

| Exact profiles | Reviewed typed task families |
| --- | --- |
| `1.3.9` | 11: `CONDITIONS`, `DATAX/literal_custom_json_job`, `DEPENDENT`, `HTTP`, `MR/literal_java_jar_job`, `PROCEDURE`, `PYTHON`, `SHELL`, inline `SQL`, `SQOOP/literal_command`, canonical `SUB_WORKFLOW` |
| `2.0.0` | 13: the `1.3.9` families plus `PIGEON` and `SWITCH`; source-present `WATERDROP` is an exact runtime exclusion |
| `2.0.1` through `2.0.9` | 14: the `2.0.0` families plus `WATERDROP/literal_local_config_job` |
| `3.0.0` through `3.0.6` | 20: the common `2.0.x` 13-family set plus `BLOCKING/same_workflow_state_gate`, `DATA_QUALITY/local_mysql_table_row_count_equals`, `EMR`, `FLINK/inline_local_sql`, `SEATUNNEL/literal_local_config_job`, `SPARK/inline_local_sql`, and `ZEPPELIN/paragraph` |
| `3.1.0` | 27: the `3.0.x` families except FLINK and DATAX, plus `CHUNJUN/literal_local_json_job`, `DINKY/job_trigger`, inline `HIVECLI`, `DVC/operation`, `MLFLOW/model_serve`, `JUPYTER/preinstalled_notebook`, `OPENMLDB/literal_single_statement`, `SAGEMAKER/start_pipeline_execution`, and `PYTORCH/literal_resource_script` |
| `3.1.1` | 27: DATAX joins the `3.1.0` set; SAGEMAKER has a polling defect |
| `3.1.2` | 27: FLINK is repaired; OPENMLDB and SAGEMAKER have exact runtime exclusions |
| `3.1.3` | 29: OPENMLDB and SAGEMAKER are repaired; K8S and FLINK_STREAM remain excluded |
| `3.1.4` | 30: K8S is repaired; FLINK_STREAM remains excluded |
| `3.1.5` through `3.1.9` | 31: the `3.1.0` families plus DATAX, `FLINK/inline_local_sql`, `FLINK_STREAM/inline_local_sql`, and `K8S/literal_container_job` |
| `3.2.0` | 37: the `3.1.9` families plus `DATASYNC/create_and_execute`, `DATA_FACTORY/pipeline_trigger`, `DMS/resume_existing_full_load`, `REMOTESHELL`, `JAVA/literal_fat_jar`, and `KUBEFLOW/tfjob_manifest` |
| `3.2.1` | 36: the same additions except JAVA, whose absolute resource path is broken by an exact runtime double-prefix hole |
| `3.2.2` | 38: the `3.2.0` family set after the JAVA resource-path defect is repaired, plus `DYNAMIC/literal_single_dimension_fanout` |
| `3.3.1` and `3.3.2` | 34: the `3.2.2` families except upstream-absent `BLOCKING` and `PIGEON` and runtime-hole `FLINK_STREAM`, plus `ALIYUN_SERVERLESS_SPARK/literal_jar_submit`; `CHUNJUN/literal_local_json_job`, `DATASYNC/create_and_execute`, `DATAX/literal_custom_json_job`, `DATA_FACTORY/pipeline_trigger`, `DINKY/job_trigger`, `DMS/resume_existing_full_load`, `DVC`, `EMR`, `FLINK/inline_local_sql`, `HIVECLI`, `JAVA/literal_fat_jar`, `JUPYTER`, `K8S/literal_container_job`, `KUBEFLOW/tfjob_manifest`, `MLFLOW/model_serve`, `MR/literal_java_jar_job`, `OPENMLDB/literal_single_statement`, `PYTORCH/literal_resource_script`, `SAGEMAKER/start_pipeline_execution`, `SEATUNNEL/literal_local_config_job`, `SPARK/inline_local_sql`, `SQOOP/literal_command`, and `ZEPPELIN/paragraph` remain typed |
| `3.4.0` and `3.4.1` | 34: PYTORCH is removed upstream while `GRPC/literal_unary_string_record_call` joins the preceding family set |
| `3.4.2` and `3.4.3` | 35: the preceding families plus `EMR_SERVERLESS/start_job_run` |

The fully unreviewed source-present implementation gap is zero.
This count excludes coordinates with exact negative reviews as well as task
families that already have a positive membership but retain an exact runtime
hole.

The complete exact family membership table and parameter/execution transitions
are maintained in [task authoring boundaries](../development/task-authoring-boundaries.md).
Typed SCRIPT resources and MR/JAVA JARs must resolve to a visible FILE before
mutation. Canonical names are FILE-root paths such as `/jobs/app.jar`; native
wire through `3.1.9` uses the verified resource id. From `3.2.0`, the
`resourceName` field carries the verified storage absolute path, such as
`/tenant/resources/jobs/app.jar`. Unverified reads remain opaque; this does not
repair the `3.2.1` JAVA runtime hole or add `1.3.9` PYTHON resource authoring.

On `2.0.0`–`2.0.2`, child-defined globals win same-name parent values; parent
globals win from `2.0.3`. Child output is absent through `2.0.6`, and
`2.0.7`–`2.0.9` copy task-local OUT values only on the cancel path, not normal
completion. Arbitrary startup keys first survive in `2.0.8`; native
CONDITIONS/SWITCH branch references change from strings to longs there too.

`SEATUNNEL/literal_local_config_job` contributes twenty-six memberships from exact
`3.0.0` through `3.4.3`. YAML owns only a literal ASCII/LF `rawScript`
SeaTunnel configuration and rejects placeholders. Exact `3.0.x` uses a
compiler-owned REST-only POSIX launcher wrapper because its upstream UI emits
a broken Waterdrop command; exact `3.1.0`–`3.1.5` use native SPARK/custom-config `deployMode=local`
(the enum renders client). Exact `3.1.6` requires `deployMode=client` and
`master=LOCAL` to retain client/local execution; exact `3.1.7` onward uses the native `seatunnel.sh` custom-config/local
wire. Resource mode, arbitrary shell, alternate engines or launchers,
parameters, outputs, and the `3.2.0`-through-`3.2.2` staged-resource path holes
remain outside typed authoring. Exact `3.3.1` onward requires a parameter-free
workflow because upstream otherwise forwards workflow values outside the
facet. Raw opaque create/edit is closed, while richer existing state is
unchanged/export preserve-only. Eligible Unix-like workers require
`SEATUNNEL_HOME`, Java, compatible connectors/catalogs, network, tenant, and
data permissions; configuration validity remains an operator prerequisite.
Upstream INFO-logs params, config, and commands, exposes no structured output
or durable application id, cannot resume after failover, cancels best-effort,
and may replay the complete job on retry. The review adds no live evidence and
promotes no profile.

`DATA_QUALITY/local_mysql_table_row_count_equals` is available on exact
`3.0.0` through `3.2.2`. Its legacy YAML owns `datasource`, `table`, and
`expectedRowCount`; exact `3.2.x` additionally requires `database`. Before typed
create or a changed edit carrying this exact fixed-point projection, including
dry-run, dsctl verifies the live stock-rule page and rule-form fingerprint for
rule 10 plus a permission-visible MYSQL datasource; the modern profiles also
require its database to match the authored value. No-op edits and unchanged
richer opaque-preserve tasks skip this authoring preflight.
Projection fixes local Java Spark, the Data Quality main class, fixed-value
row-count equality, and blocking failure. Because master evaluation is
inverted on `3.0.0`–`3.0.3` and `3.1.0`–`3.1.9`, those releases use native
`NE`; `3.0.4`–`3.0.6` and `3.2.x` use `EQ`. The legacy executor derives database
from the datasource, whereas the modern wire carries it explicitly. If the
result row is missing, upstream can report false success without performing
the comparison. INFO logs disclose parameters, commands, and resolved
credentials, so task fields are not secret storage. Parameter-free workflows,
shell-safe datasource configuration, the Data Quality application, Spark,
Java, MYSQL JDBC, network/table `SELECT` access, and writable DS metadata are
operator prerequisites. There is no structured output, durable app id, or
failover reattach; cancellation is best-effort and retry can duplicate result
rows or alerts. Richer state is unchanged/export preserve-only. The review
refreshes no live evidence and promotes no profile.

`KUBEFLOW/tfjob_manifest` is available in `MachineLearning` on exact `3.2.0`
through `3.4.3`; it is upstream-absent on earlier profiles. Its
canonical YAML owns only literal `namespace`, literal `cluster`, and one
unchanged `yamlContent`. The typed manifest is one ASCII/LF mapping document
for exactly `apiVersion: kubeflow.org/v1` and `kind: TFJob`. It must include
`metadata.namespace` equal to canonical `namespace`, omit root `status`, and
carry a nonempty `spec.tfReplicaSpecs` mapping. `generateName` and server-owned
`uid`, resource version/generation, creation/deletion timestamps and grace,
`managedFields`, and `selfLink` are rejected. Its DNS-safe `metadata.name`
prefix is no longer than 52 characters and ends with the sole allowed
`${system.workflow.instance.id}` placeholder. Only JSON-core scalar tags
(`string`, `null`, `bool`, `int`, and `float`) are accepted; YAML timestamp,
binary, set, and custom tags are rejected. Other placeholders, controls,
duplicate keys, aliases and merge keys, multiple documents,
`List` roots, and richer state are rejected. Projection emits only
literal YAML and compact native namespace JSON with `name` and `cluster`.
Exact empty UI `localParams=[]` and `resourceList=[]` residue canonicalizes on
decode; nonempty or richer native state is unchanged/export preserve-only.
There is no raw opaque create/edit selector.

Export remains read-only. A richer opaque KUBEFLOW baseline can be saved only
unchanged or through a description-only or task-name-only patch; a task rename
in such a patch is metadata-only. Any task/workflow execution-semantic or
topology edit fails closed.

The master resolves `cluster` to kubeconfig, but stock
`kubectl apply/get/delete -f` execution ignores the outer namespace and uses
the manifest's namespace. Prepared substitution occurs before a
platform-default write. Upstream INFO logs complete task params, expanded YAML,
commands, status JSON, and complete resolved kubeconfig on all nine versions.
The task-log filter present on exact `3.3.1` and newer does not suppress that
worker-service INFO event; do not store secrets in these fields. On a valid
status shape, polling recognizes only `Succeeded`, `Available`, or `Bound` as
success and `Failed` as failure. Missing `status`/`conditions` or other
nonterminal state can continue until the task timeout. However, an existing
empty `status.conditions` array makes all nine exact `KubeflowHelper`
implementations unconditionally access its last element and fail at runtime
instead of continuing to poll; a compatible nonempty conditions protocol is a
runtime prerequisite. It has no command
timeout, structured output, or durable Kubernetes identity. `appIds` is only a
persisted already-submitted sentinel for manifest-derived failover polling;
workflow-instance identity remains stable across failover, while apply can
succeed before the callback and be reapplied. The template requires
`retry.times=0`; retry within the same workflow observes or reapplies the same
terminal TFJob and does not guarantee another run. It disables only the DS task
retry; TFJob/Kubernetes-controller reconciliation and Pod `restartPolicy` can
still repeat training work. The compiler rejects
duplicate typed `(cluster, namespace, metadata.name template)` identities in
one workflow so sibling tasks cannot apply or delete the same TFJob.

Recovery-style REPEAT/RERUN paths can reuse the same `workflowInstanceId`, so
the rendered `metadata.name` is also reused. Same-id recovery may reobserve an
already-successful TFJob without starting new training, and `REPEAT` does not
guarantee fresh training. A fresh identity means creation of a new workflow
instance. `dsctl workflow-instance rerun`, `recover-failed`, and `execute-task`
now fail closed by default when the available instance `dagData` contains any
`KUBEFLOW` task. Use `dsctl workflow run WORKFLOW --project PROJECT` to create
a new workflow instance. This is a `dsctl` protection only: it cannot constrain
the DolphinScheduler UI or direct REST calls, and it makes no KUBEFLOW
detection claim when `dagData` is unavailable.

The independent `dsctl workflow online` path reads the exact workflow DAG
before its release REST call. When that DAG explicitly contains KUBEFLOW, it
reuses the applicable typed or opaque runtime preflight; a failed preflight
sends no release request. `dsctl workflow offline` does not read the DAG and is
not subject to this activation gate.
Workers need a shell-safe task path, `kubectl`, the resolved kubeconfig,
network/RBAC, and the TFJob CRD/status protocol. Typed workflow compilation
requires `timeout > 0` in native minutes and `timeout_notify_strategy` set to
`FAILED` or `WARNFAILED`; omitted/default or explicit `WARN` warns without
terminating the watcher. The template sets `timeout_notify_strategy: FAILED`
and `timeout: 60` for one hour because polling has no internal deadline. This
gate does not retroactively block preservation: a canonical-decoding server
baseline with legacy outer retry, timeout, or `WARN` survives only a
description-only patch or a file edit that leaves every execution-affecting task
field unchanged. Changing type, params, command, flag, worker/environment
selection, task group/priority, retry/timeout/strategy/delay, resource limits,
dependencies, or workflow-level execution settings—including `release_state`
transitions such as `OFFLINE` to `ONLINE`, workflow timeout, `execution_type`,
and global parameters—reruns the applicable gates. Standalone YAML has
no server provenance and gets no exception. This review performs no live
preflight or campaign and promotes no profile. Earlier receipts remain bound to
their original authoring manifest and do not attest this expansion.

`PYTORCH/literal_resource_script` is available in `MachineLearning` on exact
`3.1.0` through `3.3.2`. It is upstream-absent on earlier profiles
and exact `3.4.0`–`3.4.3`. Canonical authoring owns only one
absolute ASCII shell-safe worker `pythonExecutable`, one leading-slash ASCII
shell-safe `.py` `scriptResource` relative to the selected user's FILE root,
and ordered ASCII shell-safe `scriptArgs`. Every typed profile resolves that
canonical path before mutation and
requires one permission-visible, non-directory FILE. Exact `3.1.x` sends native
`pythonCommand` and the resolved positive id. Exact `3.2.0` through `3.3.2`
query the FILE base directory, page-verify the prefixed storage path, send
`pythonLauncher` plus that storage absolute `resourceName`, and keep the staged
script base-relative. Exact `3.2.x` admin sessions receive an ALL-resource base,
so resolution compares FILE and UDF base responses and accepts only distinct
sibling `resources`/`udfs` tenant roots; ambiguous results fail closed and
require a non-admin tenant user. On reads, a modern native package becomes
canonical only when its `resourceName` matches that freshly verified FILE
identity; unresolved or mismatched state stays opaque.
The compiler fixes
environment creation off, `pythonPath="."`, empty `localParams`, dormant
environment defaults, the staged relative script path, and the one-space
argument join. Placeholders, shell fragments, Git/environment modes,
parameters, output declarations, additional resources, and future fields are
outside typed and raw opaque create/edit. Richer existing state is
unchanged/export preserve-only.

Workers must be Unix-like and already provide the selected executable plus its
PyTorch dependencies. Exact `3.1.0` explicitly writes the command file as UTF-8.
Exact `3.1.1`–`3.1.9` and `3.2.0` through `3.3.2` instead encode it with the
worker JVM's default charset, so the portable typed boundary remains ASCII.
Exact `3.3.1` and `3.3.2` typed workflow compilation
requires `timeout: 0`: their executor can block on output before timeout
handling and plugin cancellation is a no-op, while `3.3.2` can additionally
call `exitValue()` on a live process. Cancellation before `3.3.x` remains an
outer-worker best-effort operation. Upstream INFO-logs full params and the
command, does not transport task output or persist a durable app id, cannot
reattach after failover, and can replay the whole script on retry. This review
adds no live evidence and promotes no profile.

Do not author new `LINKIS` tasks on exact `3.2.0` through `3.4.3`. All nine
source-present coordinates are reviewed runtime exclusions: the plugin submits
`linkis-cli` asynchronously, then tries to parse its task id and status from a
command-result field that the executor never populates. A remote job may have
started before that parse fails and before its id is persisted. The runtime
also checks status only once instead of polling to completion. Typed and raw
create/edit therefore fail closed; an existing LINKIS task can survive only
through unchanged/export preservation or a metadata-only workflow edit.
Without the captured id cancellation is unreliable, while retry or failover
can submit another remote job. This review does not refresh live evidence or
promote any profile.

`WATERDROP/literal_local_config_job` contributes nine positive memberships on
exact `2.0.1`–`2.0.9`. It owns one absolute ASCII shell-safe DS FILE
`configResource`, resolved before mutation to a visible, non-directory,
positive resource id. Exact projection fixes `localParams=[]`, emits one
`resourceList` id, and constructs one local/client/default launcher line whose
`--config` path removes exactly one leading `/`; `$WATERDROP_HOME` is left for
the worker shell instead of being written as a DolphinScheduler `${...}`
placeholder. Multi-config and remote modes, variables, parameters, output,
custom queue/script state, and future fields are excluded from typed and raw
opaque create/edit. Richer existing native state remains unchanged/export
preserve-only.

Exact `2.0.0` remains a reviewed runtime exclusion, not an unreviewed or
upstream-absent coordinate: its stock worker never registers the literal
`WATERDROP` channel as a `SHELL` alias, so execution fails before task
construction. Typed and raw opaque create/edit fail closed there, while
existing server state remains preservable; exact `2.0.1` adds the missing alias. Eligible workers require a POSIX shell, `WATERDROP_HOME`, a compatible
foreground Waterdrop/Spark launcher, tenant and resource permissions, and
target connectivity. Upstream INFO logs task parameters, script, paths,
command, and child output. There is no structured output, durable application
id, or failover reattachment; cancellation is worker-local best effort and a
retry or failover may replay the whole job. The review adds no live evidence
or profile promotion.

`DYNAMIC/literal_single_dimension_fanout` contributes one positive membership
on exact `3.2.2`. Its canonical payload owns one literal same-project
`childWorkflowName`, one literal non-`system.*`
`parameterName`, one to 1,024 ordered unique comma-free literal `values` whose
complete comma-joined native spelling is at most 256 UI/JavaScript UTF-16 code
units, and a
positive `degreeOfParallelism` no greater than the value count. Projection
fixes an empty filter, one comma-delimited native dimension, and a maximum
equal to the value count. Empty `localParams`/`resourceList` and exact
`disabled=true` UI residue are decode-only; nonempty or richer native state is
unchanged/export preserve-only, and raw opaque create/edit is closed.

Live mutations resolve the child name to a positive native code, then prove
same-project existence, non-self identity, and an acyclic
reachable exact `DYNAMIC`/`SUB_PROCESS` closure before writing. Declared parent
globals cannot collide with `parameterName`; ordinary task retry is fixed at
zero. Runtime-supplied start parameters, child permissions/runtime/resources,
and graph drift after authoring remain operator prerequisites. The parent
schedule time/timezone is not forwarded. INFO logs expose generated groups and
`dynamic.out(taskName)` values. Recovery/failover can replay effects, partial
persistence can orphan or duplicate children, cancellation is best effort,
and pause does not cascade. Exact `3.2.0` and `3.2.1` remain reviewed runtime
exclusions because the child tenant is not forwarded; `3.2.1` also has an
unguarded missing-start-parameter dereference. No live evidence or profile
promotion is added.

`BLOCKING/same_workflow_state_gate` contributes the twenty exact memberships
from `3.0.0` through `3.2.2`; it is upstream-absent from `3.3.1`. Typed
create/edit owns `BlockingOnSuccess` or `BlockingOnFailed`, strict
`alertWhenBlocking`, and a nonempty two-level `AND`/`OR` tree of literal local
task-name `SUCCESS`/`FAILURE` predicates, which compile to positive task codes
and real DAG edges. Raw opaque create/edit is closed; richer/future state is
preservable only from an existing-server baseline during unchanged or
metadata-only edits. The upstream tags expose no BLOCKING authoring form, so
the facet is REST-only; standalone YAML has no opaque provenance and cannot
independently reapply richer state. Closed-looking native state becomes typed
only when matching acyclic workflow relations already encode every predicate;
otherwise it stays opaque. The task itself completes `SUCCESS`. A
match moves the workflow through drain-sensitive `READY_BLOCK` to terminal
`BLOCK`; a miss continues without ordinary task retry. Exact `3.0.0` through
`3.0.6` mark standby work `KILL`, while later reviewed versions use `PAUSE`.
`alertWhenBlocking` requests a record for the workflow `warningGroupId`, but
delivery requires a valid alert group and infrastructure that the facet does
not validate. Upstream INFO logs identities, expected/actual states, aggregate
results, and the opportunity, so fields are not secret storage. The
master-local task has no worker, output, durable app id, or dedicated failover
resume. Pause/kill changes only task-local state through `3.1.9` and is
warn/no-op on `3.2.x`; neither epoch has a remote target. Infrastructure replay
or workflow rerun can reevaluate and repeat the alert. The review refreshes no
live evidence and promotes no profile.

Through `3.2.2`, canonical `SUB_WORKFLOW` projects to native `SUB_PROCESS`.
Logic-task references, DEPENDENT fields, HTTP body/socket fields, parameter
behavior, and task-plugin payloads are projected using exact-version semantic
profiles rather than inherited from `3.4.1`.

Exact `1.3.9` owns a distinct nested-workflow identity contract. Canonical
YAML supplies only one nonblank literal `task_params.childWorkflowName`. The
CLI resolves that value explicitly as a same-project name—even if it contains
only digits—then emits native `SUB_PROCESS` with exactly one positive
`processDefinitionId`. Public raw opaque create/edit is closed. Workflow codes,
caller-supplied ids, placeholders, task `localParams`, resources, `varPool`,
richer native state, and cross-project references are excluded from typed
authoring.

Once the final native graph is compiled, an iterative runtime-equivalent audit
follows every task `params.processDefinitionId`, regardless of task type,
through the descendant closure and rejects missing definitions or direct and
indirect cycles. Legacy detail result codes `50001` and `50003` both normalize
to not-found. Read-side hydration maps only safe immediate exact one-id
packages back to child names and does not inspect grandchildren; unresolved ids
and richer packages stay opaque. `workflow run-task` and task-scoped
`workflow backfill` use the native graph without child-name hydration.

The exact executor is master-local. It transactionally persists the
parent/child map together with the child command and later persists the child
link in its own transaction so recovery can reuse it. Parent workflow globals
feed the child and a same-name child global wins; task-local parameters are
ignored. Pause and stop propagate to the child, subject to its tasks'
cancellation behavior. There is
no structured output or durable worker application id, while child retry or
`REPEAT` may replay side effects. Descendants can change after authoring, so
their continued existence, acyclicity, and release readiness remain runtime
operator prerequisites. This static review adds no live evidence, changes no
`tested` flag, and does not promote `1.3.9`.

`DEPENDENT` is typed on all 37 profiles. Exact `1.3.9` is a distinct
service-resolved split-wire epoch: canonical leaves contain literal
cross-project `projectName` and `workflowName`, plus a literal `taskName` for
`DEPENDENT_ON_TASK`; numeric-looking names remain names. The CLI resolves
positive database IDs and verifies target task membership before emitting
`params={}` beside outer `TaskNode.dependence`. Workflow dependencies use
native `depTasks=ALL`, task dependencies use the literal task name, and neither
creates a workflow DAG edge. Placeholders, parameters, resources, datasource
state, native IDs, future fields, and public raw opaque create/edit are closed.
Workflow-definition reads reverse-bind only unambiguous exact packages.
Unresolved or richer state remains opaque and requires a server baseline; its
name, type, task parameters, and command must remain unchanged, while
description edits and unrelated topology additions are allowed. Standalone
YAML plus workflow-instance export are not lossless carriers.
Workflow-instance edit does support typed name resolution.

The exact base hour/day/week/month date set applies everywhere.
`thisMonthBegin` and `thisMonthEnd` are additionally executable on
`3.0.2`–`3.0.6`, `3.1.1`–`3.1.9`, and `3.2.0` through `3.4.3`; they are
rejected on `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.1`, and `3.1.0`. Runtime window calculation
uses `dateValue`, while `cycle` is retained only in the dependency key, so the
exact projector requires a matching pair rather than trusting UI choices. The
task is master-local, logs target/window/state at INFO, returns no output, and
has no durable remote app ID or reconnectable cancel/resume target. Scheduled
retry/failover retains the original `scheduleTime`; only manual/current-time
reinitialization can shift relative windows. Authoring verifies target
existence, but upstream does not attest target-project authorization. Later
target rename, deletion, or state change remains a runtime prerequisite. This
static review adds no live evidence or promotion.

`CONDITIONS` is now typed on all 37 profiles, with an exact legacy contract on
`1.3.9`. That profile accepts grouped `SUCCESS`/`FAILURE` predicates and one
task name in each success and failure branch; the two names must differ. All
references must resolve inside the same workflow. Its native `TaskNode` keeps
`dependence` and `conditionResult` beside `params`; the projector emits native
`depTasks` and branch names there instead of task codes. It accepts no placeholders,
`localParams`, `varPool`, resources, datasource, or other parameter fields.
Its native `params` is exactly `{}`. The task executes and reevaluates
persisted state on the master, logs task names and expected/actual states at
INFO, and has no remote id, cancel target, or failover-resume path. Richer
native state is not typed input and survives only when an edit starts from an
existing server baseline and leaves the task payload, task names, and topology
unchanged, including a metadata-only edit. Standalone YAML export does not
carry the split outer opaque fields and is not lossless. No raw opaque
create/edit selector is
available, and invalid typed input fails rather than downgrading. The review
adds no live evidence or profile promotion.

`PROCEDURE` is typed on all 37 profiles. It keeps one canonical positional JDBC
call shape while exact projection absorbs the native bare-name, positional, and
named-placeholder method epochs. `PIGEON` is typed only from `2.0.0` through
`3.2.2`, owns only nonblank `targetJobName`, and documents the worker's runtime
`p_host` dependency; it is absent upstream on `1.3.9` and from `3.3.1` through
`3.4.3`.

`HTTP` has reviewed typed membership on all 37 exact profiles. Exact `1.3.9`
canonical authoring has no `httpBody`, `varPool`, or explicit `socketTimeout`;
the exact wire projector injects `socketTimeout=60000`, while `connectTimeout`
remains explicit. Exact `1.3.9` typed `localParams` are `IN`-only and use that
release's nine native scalar data types; `OUT` parameters plus non-exact/future
`LIST` and `FILE` raw shapes remain unchanged/export opaque state.
Prepared-parameter substitution covers the URL and every property. INFO logs
include the complete task params, substituted request params/properties,
configured/original URL (not a claimed final or expanded
URL), status, and complete response body. These task fields are not secret
storage, and the CLI does not redact them. Native `HEAD`, the `BODY` property
kind, nondefault socket timeout, and future native members remain unchanged/
export opaque preservation state; none belongs to typed authoring. There is no
reliable cancel, durable id, failover resume, or structured output. Retry
resends the whole request, so `POST`, `PUT`, and
`DELETE` side effects can duplicate. This review adds no live evidence or profile promotion.

`PYTHON` is typed on all 37 exact profiles. Exact `1.3.9` canonical YAML owns
`rawScript`, unique `IN`-only `localParams` using its nine scalar data types,
and an empty `resourceList`; it has no `varPool`, `LIST`, `FILE`, or structured
output. Process-definition authorization checks only positive resource IDs,
while the master's old `id=0`/`res` full-name branch resolves a tenant without
that user permission context before the worker downloads the resource. Typed
authoring therefore provides no 1.3.9 resource
attachment or `resource` variant. Every nonempty native resource shape, `OUT`
parameter, inherited member, and future field remains unchanged/export opaque
preservation. The
worker normalizes CRLF, performs prepared substitution without Python escaping,
writes UTF-8, and uses `PYTHON_HOME` with a `python` fallback. INFO logs include
full params, original and substituted script, command, and stdout/stderr; do
not place secrets there because the CLI does not redact them. Cancellation is
local best-effort, there is no durable Python-process resume, and retry or
worker failover reruns the complete script and can duplicate side effects.
This review changes no live evidence, `tested` flag, or profile promotion.

`MR/literal_java_jar_job` is typed in `Universal` on all 37 exact profiles.
The canonical payload owns one absolute shell-safe DS resource `fullName`
ending in `.jar`, a strict dotted ASCII Java `mainClass`, and ordered
shell-safe `mainArgs`. Through `3.1.9`, the CLI resolves the resource to an
exact visible, non-directory file with a positive id before mutation and sends
`mainJar={id}`. From `3.2.0`, it sends `mainJar={resourceName}` and
`yarnQueue=""`. Every version fixes `programType=JAVA`, `appName=""`,
`others=""`, `localParams=[]`, and `resourceList=[]`. The main JAR is not
duplicated in `resourceList` because the native MR model adds it to the runtime
resource list; queue selection remains runtime-owned.

Only native `SCALA` selects public opaque create/edit. Do not author the
UI-advertised native `PYTHON` mode: the executor still constructs
`hadoop jar`. Richer state and legacy `id=0`/`res` resource forms remain
unchanged/export preserve-only. Workers need Hadoop/YARN configuration,
tenant and resource-storage permission, queue access, and data access.
Upstream INFO-logs full params, command files, commands, and child output. MR
publishes no DS output or durable callback identity, cannot reattach after
failover, and can rerun the whole JAR on retry/failover. Cancellation is
best-effort from an observed application id; `3.3.1` onward also has a
post-exit `appIds` context transport hole. This review changes no live
evidence, `tested` flag, or profile promotion.

`SQOOP/literal_command` is typed in `DataIntegration` on all 37 exact
profiles. Its closed payload owns only `subcommand: import|export` plus a
nonempty ordered argument list and projects to compiler-owned `CUSTOM`, empty
`localParams`, and one POSIX-quoted `sqoop` command. Empty argument tokens
round-trip as POSIX `''`; only the list itself must be nonempty. Arguments
reject controls, surrogates, DS placeholders, edge whitespace, interactive or
inline passwords, and, from exact `3.1.1`, all non-ASCII text because the
upstream script writer uses the worker platform-default charset. Releases
through `3.1.0` write UTF-8; CRLF becomes LF through `3.1.9` and the worker OS
line separator from `3.2.0`.

Native TEMPLATE jobs and arbitrary CUSTOM scripts are selector-restricted
opaque create/edit; richer state is unchanged/export preserve-only. Workers
need POSIX shell, Sqoop, Hadoop/YARN, JDBC drivers, tenant permission,
credentials, connectivity, and data access. Upstream logs command-bearing
state, so task fields are not secret storage. SQOOP publishes no DS output or
durable application id, cannot reattach after failover, and may duplicate a
transfer on retry/failover. This review changes no live evidence, `tested`
flag, or profile promotion.

Exact `1.3.9` `SQL` uses a separate closed inline contract. It owns `MYSQL`,
`POSTGRESQL`, `HIVE`, `SPARK`, `CLICKHOUSE`, `ORACLE`, `SQLSERVER`, or `DB2` as
datasource `type`, positive `datasource`, literal nonblank `sql`, strict
query/update `sqlType`, nonnegative `displayRows`/`limit`, unique `IN`-only
nine-scalar `localParams`, and nonblank pre/post statements. `connParams` must
be empty except for HIVE, where it is a literal semicolon-separated
`key=value` map with unique nonblank keys and nonblank values. Projection forces
`sendEmail=false` because absent or null enables mail upstream, and supplies
empty UDF/mail fields plus `showType=TABLE`; newer `groupId` and `varPool`
fields are absent. Mail, UDF, `OUT`, export-decoration, inherited, and future
state stay on selector-restricted explicit opaque authoring when supplied as
one complete native parameter package, or unchanged/export preservation.
Partial invalid typed input never selects opaque mode.

The process-definition API checks project and resource permission but does not
authorize the datasource before the master resolves its id, so datasource
existence, access, and type agreement remain caller prerequisites. The worker
has no explicit transaction spanning pre/main/post SQL, durable application
id, or structured output. Cancellation may report success while JDBC
continues; retry or worker failover replays the complete sequence. INFO logs
disclose full params, SQL, HIVE `connParams`, bound values, and result rows, so
typed fields are not secret storage. This review refreshes no live evidence,
changes no `tested` flag, and promotes no profile.

`SPARK/inline_local_sql` has reviewed typed membership from `3.0.0` through
`3.4.3`. The SPARK plugin exists on `1.3.9` and `2.0.0`–`2.0.9`, but its
upstream parameter model has no SQL program mode there; those three profiles
therefore keep the plugin on its generic opaque path rather than pretending
the typed SQL facet exists. The stable canonical payload is only a nonblank
literal `rawScript`. It preserves canonical and wire spelling and rejects
`${...}`, `$[...]`, unsafe control text, and every other field.

Projection absorbs three wire epochs. Releases through `3.1.9` emit
`programType=SQL`, `sparkVersion=SPARK2`, and `deployMode=local`. Exact
`3.2.0` and `3.2.1` emit `programType=SQL`,
`sqlExecutionType=SCRIPT`, and `deployMode=local`; releases from `3.2.2` also
emit `master=local`. Native `JAVA`, `SCALA`, and `PYTHON` programs are explicit
opaque modes on the reviewed profiles, including their complete native cluster
and resource payloads, and SQL `FILE` joins them from `3.2.0`. Cluster or
resource fields added to inline SQL, and unrecognized inherited or future
state, remain unchanged preservation only. Invalid inline SQL does not
downgrade to opaque.

Exact `3.0.x` writes the SQL file before final-command parameter substitution;
`3.1.0` and newer expand SQL from the prepared parameter map before writing
the file, although typed authoring rejects placeholders everywhere for one
portable meaning. Workers use `SPARK_HOME2` through `3.1.9` and `SPARK_HOME`
from `3.2.0`, and need Spark SQL, Java, Hadoop, Hive catalog configuration, and
target-data permissions. Upstream logs raw or expanded SQL at INFO, normalizes
CRLF before writing the worker SQL file, and runs a synchronous local process.
Runtime line endings therefore need not be byte-identical to the canonical
wire. There is no durable application id or failover resume. Cancellation
controls that process; retry reexecutes the complete SQL and can repeat side
effects. This review refreshes no live evidence, changes no `tested` flag, and
promotes no profile.

`FLINK/inline_local_sql` has reviewed typed membership on `3.0.0`–`3.0.6`,
`3.1.2`–`3.1.9`, and every profile from `3.2.0` through `3.4.3`. The
`1.3.9` and `2.0.x` models have no SQL `ProgramType`. Exact `3.1.0` remains a
deliberate typed hole because its executor resolves `mainJar` unconditionally
before SQL initialization. `3.1.1` repairs that guard but reverses LOCAL/CLUSTER
SQL targets; `3.1.2` repairs target selection. Exact `3.1.1` accepts only
explicitly selected native opaque modes, while canonical inline SQL fails closed.

The canonical payload owns only a nonblank literal `rawScript`, preserving its
spelling. Every typed profile emits the exact native wire
`programType=SQL`, `deployMode=local`, `initScript=""`, and the unchanged
`rawScript`. Exact `3.0.x` writes with the platform-default charset and thus
accepts ASCII only; `3.1.2` and newer write UTF-8. Typed validation rejects
blank scripts, carriage returns, DEL/C0/C1 controls, `${...}`, and `$[...]`,
but accepts multiple SQL statements, TAB/LF, quotes, semicolons, backslashes,
backticks, and `$()` without normalization.

Recognized native `JAVA`, `SCALA`, and `PYTHON` programs and exact non-local
SQL modes use explicit opaque create/edit. Cluster, resource, initialization,
option, inherited, or future fields attached to local inline SQL, plus
unrecognized state, remain unchanged/export preservation only. Invalid local
inline SQL never selects opaque mode. Workers through `3.2.2` resolve
`sql-client.sh` from `PATH`; releases from `3.3.1` use
`FLINK_HOME/bin/sql-client.sh`. Exact `3.4.2`–`3.4.3` substitute prepared parameters
before writing SQL, but typed authoring still rejects placeholders everywhere.

Eligible workers need the Flink SQL client, Java, connector and catalog
configuration, and target-data permissions. Upstream logs task parameters,
SQL content and file paths, and the execution command at INFO; do not place
secrets in FLINK SQL. The local SQL client exposes no DS task output, durable
application id, or failover resume. Retry reexecutes the whole SQL and may
repeat side effects. This review refreshes no live evidence, changes no
`tested` flag, and promotes no profile.

`FLINK_STREAM/inline_local_sql` has reviewed typed membership only on exact
`3.1.5`–`3.1.9` and `3.2.0` through `3.2.2`. The plugin is upstream-absent through
`3.0.6`. Exact `3.1.0`–`3.1.4` retain the plugin-specific unconditional `mainJar`
dereference before SQL initialization. They remain typed holes independently
of ordinary FLINK fixes. From `3.3.1` through `3.4.3`,
`ExecutorServiceImpl.execStreamTaskInstance` immediately throws
`Not supported`; those registered runtime holes are also untyped. Untyped registered coordinates retain their separate opaque policies without
a typed execution claim; `3.1.1`–`3.1.4` require explicit native mode selectors.

The canonical payload owns only one nonblank literal `rawScript`. Projection
preserves its spelling and emits exactly four `taskParams` fields:
`programType=SQL`, `deployMode=local`, `initScript=""`, and `rawScript`.
Compilation separately supplies top-level `taskExecuteType=STREAM`; users do
not author that discriminator in task YAML. Every typed release writes UTF-8.
Typed
validation rejects blanks, carriage returns, DEL/C0/C1 controls, unpaired
Unicode surrogates, `${...}`, `$[...]`, and any additional field, while
allowing multi-statement SQL, TAB/LF, quotes, semicolons, backslashes,
backticks, and `$()` unchanged.

Recognized native `JAVA`, `SCALA`, and `PYTHON` JAR programs and exact
non-local SQL modes use explicit opaque create/edit. Local-inline extras,
inherited or future fields, and unrecognized modes remain unchanged/export
opaque-preserve-only; invalid local SQL never downgrades to opaque authoring.
All eight typed releases resolve `sql-client.sh` from `PATH` and reject
placeholders. Later `FLINK_HOME` and prepared-substitution plugin behavior does
not widen typed membership because the corresponding STREAM entry is
unsupported.

Workers need the Flink SQL client, Java, connector and catalog configuration,
and target-data permissions. Upstream logs parameters, SQL content and file
paths, and commands at INFO, so do not put secrets in this task. Local SQL is
expected to publish no application id and exposes no result output, durable
submit identity, or failover resume. Cancel and savepoint require an
application id. Stop order is plugin cancel then PID-tree kill on `3.1.x`,
PID-tree kill then plugin cancel from `3.2.0` through `3.2.2`, and plugin cancel
only from `3.3.1` through `3.4.3`; the last epoch returns on the missing id with
no process fallback. Reliable stop is unsupported, so an unbounded stream may
continue after the DS task stops. Retry reexecutes all SQL on typed releases
and may repeat side effects. This review refreshes no live evidence, changes no
`tested` flag, and promotes no profile.

`K8S/literal_container_job` has reviewed typed `Cloud` membership on fifteen
exact profiles from `3.1.4` through `3.4.3`. Profiles through `3.0.6` are
upstream-absent. Exact `3.1.0`–`3.1.3` register the plugin but remain formal
runtime exclusions: its watcher counts down on a
`RUNNING` update while the response exit status is still `-1`. Raw opaque
create/edit is selector-restricted to a nonblank image and native compact
`namespace` JSON with nonblank `name` and `cluster`, with canonical
`connectionMode` and sibling `cluster` absent. Canonical connection input
fails closed, and the default template is an explicit risk scaffold
rather than an attested executable task.

The canonical contract owns a literal image tag or digest, finite nonnegative
CPU and MiB-memory values, and literal input environment entries with unique
names that project to `IN`/`VARCHAR` native `localParams`. The outer task name
must match
`^[A-Za-z0-9][A-Za-z0-9-]{0,51}$`; uppercase is accepted because runtime
lowercases with `Locale.ROOT`, then appends `-` and the decimal task-instance
id. Connection selection is exact.
`3.1.4` through `3.2.2` require `connectionMode=NAMESPACE`, `namespace`, and
`cluster`; projection encodes the latter two as one compact native namespace
JSON string. Exact `3.2.2` weakens `checkParameters` to an image-only check and
its upstream UI hides the namespace selector, but the backend, master, and
runtime still require the legacy namespace wire. From `3.3.1`,
`connectionMode=DATASOURCE` instead requires a positive K8S datasource id and
projects compiler-owned `type=K8S` plus empty `namespace` and `kubeConfig`
fields for worker-side overwrite.

From `3.2.0`, typed authoring also owns structured command and argument arrays,
an optional pull-secret object name, explicit image pull policy, unique valid
custom labels, and structured node selectors. Custom-label values may be
empty; `3.2.0` applies them only to the Job, while `3.2.1` and newer apply them
to both Job and Pod template. Repeated selector keys are legal AND clauses.
`In`/`NotIn` require a nonempty list of unique nonempty Kubernetes label
values; `Exists`/`DoesNotExist` require an empty list, while `Gt`/`Lt` accept
one decimal-integer string in the list, from zero through the signed 64-bit
maximum. Exact `3.2.0` requires a nonempty custom-label list because its
executor mutates an immutable empty map. Exact `3.1.4`–`3.1.9` do not expose those
fields; it uses the image
ENTRYPOINT/CMD and fixes `imagePullPolicy=Always`.

Output declarations are typed only on `3.2.0`, `3.2.1`, `3.2.2`, and `3.4.2`–`3.4.3`.
Their unique Kubernetes-style names exclude `taskInstanceId`, stay disjoint
from input environment, and compile as `OUT`/`VARCHAR`/empty-value
`localParams`. Exact `3.2.0` parses the legacy terminal `dsVal` marker;
`3.2.1`, `3.2.2`, and `3.4.2`–`3.4.3` use `setValue`. Published values must be
nonempty on `3.2.0` and `3.2.1`. The `3.2.0` parser reserves `$VarPool$` as a
delimiter and keeps only the first `=` segment, while `3.2.1` preserves later
`=` characters. Exact `3.2.2` and `3.4.2`–`3.4.3` preserve `=` and support empty
published values. Exact `3.3.1` through `3.4.1` have a physical-executor
transport hole and expose no typed output field. Exact `3.4.2`–`3.4.3` restore
transport, but its OUT declarations also enter the prepared map and are
injected into the Pod as empty-valued environment entries.

The worker injects the full prepared parameter map into the Pod. Kubernetes
environment-name validity and freedom from `taskInstanceId` collisions for
global, built-in, and inherited keys are therefore caller prerequisites. Job
watch registration omits namespace, so the selected target must match the
kubeconfig current-context namespace. Exact `3.3.1` through `3.4.1` also omit
the resolved namespace from Pod-log/output lookup. On `3.2.x`, full
task/context logging can expose configuration outside the task-log converter.
From `3.3.1`, upstream writes datasource-resolved kubeconfig into task params
and INFO-logs the resolved object. Authored and prepared values are not secret
storage, and the CLI does not redact them.

Image tags are mutable; use a digest when immutability matters. The plugin has
no durable application id or failover resume, cancel requires the same
worker's in-memory Job, and retry or worker loss can duplicate execution and
side effects. Typed coordinates expose no raw opaque create/edit selector;
richer native state survives only through unchanged edit or export provenance.
This source review adds no live evidence, changes no `tested` flag, and
promotes no profile.

`JAVA/literal_fat_jar` has reviewed typed `Universal` membership on exact
`3.2.0`, `3.2.2`, and `3.3.1` through `3.4.3`. The task type is upstream-absent
through `3.1.9`. Exact `3.2.1` remains an explicit
runtime hole: resource staging records an absolute local path, but `JavaTask`
prefixes that value again with `executePath` for both `mainJar` and
`resourceList`. Exact `3.2.2` removes both duplicate prefixes. Because the main
JAR must also appear in `resourceList` for download, typed authoring cannot
avoid the defect. Exact `3.2.1` therefore materializes a fail-closed policy:
only a nonblank native `runType=JAVA` source payload may use opaque create/edit,
broken `JAR` state is opaque-preserve-only, and canonical `mainJar`/`mainArgs`
input never downgrades to opaque.

The closed canonical payload owns a required absolute shell-safe DS resource
`fullName` ending in `.jar` as `mainJar`, plus an ordered `mainArgs` list of
shell-safe tokens defaulting to empty. The exact projector duplicates the same
`ResourceInfo` into `mainJar` and `resourceList` because the worker downloads
only the latter, and joins `mainArgs` with one native space. Exact `3.2.0` and
`3.2.2` emit `runType=JAR` and `rawScript=""`; exact `3.3.1` onward emits
`runType=FAT_JAR` and `mainClass=""`. Every typed coordinate fixes
`jvmArgs=""`, `isModulePath=false`, and `localParams=[]`.

Legacy `runType=JAVA` source mode on its reviewed exact wires and modern
`runType=NORMAL_JAR` remain selector-restricted explicit opaque authoring. JVM
arguments, placeholders, module path, parameters, output declarations,
additional dependency resources, and future state stay outside typed authoring
and survive only on their applicable opaque or unchanged/export preservation
paths. Through `3.4.0`, native `jvmArgs`
follows the target and application arguments instead of acting as JVM options.
Exact `3.4.1` through `3.4.3` move it before the target and substitute the final
command, but the shared portable subset still fixes it empty and rejects
placeholders.

Eligible workers need `JAVA_HOME`, a compatible JDK, tenant execution
permission, and access to DolphinScheduler resource storage. Upstream INFO-logs
the complete task parameters, final unquoted shell command, and child-process
output; these fields are not secret storage and the CLI does not redact them.
Cancellation on `3.2.0` through `3.2.2` destroys only the direct process and
can leave child processes behind; `3.3.1` onward kills the process tree and
attempts generic application cancellation. The subset publishes no DS output,
has no durable application id or failover reattachment, and retry reruns the
whole JAR with possible duplicate side effects. This review adds no live
evidence, changes no `tested` flag, and promotes no profile.

`DATA_FACTORY/pipeline_trigger` is upstream-absent through `3.1.9` and has reviewed typed `Cloud` membership on all nine exact
profiles from `3.2.0` through `3.4.3`. Its canonical payload and native wire
both contain only required literal `factoryName`, `resourceGroupName`, and
`pipelineName`; projection preserves the three values exactly. The identities
must be nonblank and reject edge whitespace, control or surrogate text,
`${...}`, and `$[...]`.

Azure-derived `runId` is runtime-only. Typed create/edit also rejects
`localParams`, `varPool`, `resourceList`, inherited state, and future fields.
DATA_FACTORY exposes no public opaque create/edit selector; existing excluded
or unrecognized state round-trips only through unchanged/export operations
whose recorded provenance selects opaque preservation. The plugin performs no
DS placeholder substitution and cannot pass Azure pipeline runtime parameters
or resource files.

Eligible workers provide `resource.azure.client.id`,
`resource.azure.client.secret`, `resource.azure.subId`, and
`resource.azure.tenant.id`. Upstream logs task parameters and the complete
identity at INFO but does not log those worker credential values. Status
polling reads `resource.query.interval`, whose default is `10000` ms, and
pipeline results are not exposed as DS task output.

After Azure `createRun`, a callback persists `runId` in task-instance `appIds`.
Once `appIds` is durable, worker failover skips submission and resumes polling
or cancellation of the same Azure run. If the worker fails after `createRun`
but before callback persistence, or a DS retry begins without `appIds`, the
pipeline can be submitted again. The plugin itself does not retry `createRun`.
This review adds no live evidence, changes no `tested` flag, and promotes no
profile.

`CHUNJUN/literal_local_json_job` has reviewed typed membership in `Other` on
all nineteen exact profiles from `3.1.0` through `3.4.3`; CHUNJUN is upstream-absent
on the five earlier releases. Canonical authoring owns only one preserved
literal JSON-object string. Projection emits exactly strict integer
`customConfig=1`, unchanged `json`, and `deployMode=local`. Typed validation
rejects placeholders, CR, controls, surrogates, ambiguous JSON, parameters,
resources, native `others`, dormant datasource-generation fields, and future
state. All nineteen upstream executors support `localParams` and prepared-map
placeholder substitution; typed canonical authoring excludes them deliberately
because replacement is not JSON-escaped, not because the runtimes lack it.

Selector-restricted nonlocal opaque create/edit requires strict integer
`customConfig=1` and exact `standalone`, `yarn-session`, or `yarn-per-job`.
Built-in `customConfig=0`, the upstream UI typo `standlone`, and richer local or
unrecognized state are unchanged/export preserve-only. Built-in mode is a
runtime hole because `ChunJunTask.buildChunJunJsonFile` never constructs the
job JSON.
Exact `3.1.0`–`3.1.6` UI default `customConfig=false` is a UI defect. The
REST/compiler strict integer `1` local path remains runnable, and no shared
exact wire is inferred from UI defaults.

All exact executors normalize CRLF to LF, substitute prepared values without
JSON escaping, and write UTF-8. Exact `3.1.0` uses the legacy shell-file path
with nondurable post-exit application-id log discovery; `3.1.1`–`3.1.9` retain that
path but stores empty `appIds`; `3.2.0` through `3.3.2` use the shell
interceptor with empty `appIds`; `3.4.0` through `3.4.3` use `taskRequest` with
empty `appIds`. Every exact upstream `chunjun.md` requires removing the
trailing background `&` from the `nohup` command in
`${CHUNJUN_HOME}/bin/start-chunjun`; a worker must use that foreground launcher
or DS status and cancellation semantics are untrustworthy. Complete parameters
are INFO-logged and expanded JSON is DEBUG-logged. There is no structured
output, durable id, or failover resume. Cancellation remains best-effort:
`3.1.0` through `3.1.9` use legacy wrapper soft/hard kill, `3.2.0` through
`3.2.2` direct-process destroy/force, and `3.3.1` through `3.4.3` process-tree
kill plus generic application cancel. Retries replay the complete job. This
review adds no live evidence, changes no `tested` flag, and promotes no profile.

`DATAX/literal_custom_json_job` has reviewed `DataIntegration` membership on
36 exact profiles: all except `3.1.0`. Typed canonical authoring owns only a
required `json` string and preserves valid source spelling and LF formatting.
The string must decode to one object. Blank/malformed JSON, arrays/scalars,
nonstandard `NaN`/`Infinity` constants, duplicate keys at any depth,
placeholders in the source or decoded keys/values, CR, decoded C0/C1/DEL
controls, decoded unpaired Unicode surrogates, and additional fields are
rejected.

On exact `3.4.3`, the object must also be nonempty. Upstream treats empty
objects, including whitespace-formatted `{}`, as absent inline JSON and requires
exactly one `.json` resource file. Typed authoring does not own that fallback;
it rejects the empty literal and preserves richer existing native jobs.

Projection sends only `customConfig=1` plus `json` on `1.3.9`, and fixed
integer `xms=1`/`xmx=1` from `2.0.0`. It never emits `localParams` or
`resourceList`. Decode strips exact `localParams=[]` on every profile and the
UI formatter's empty `resourceList` only from `3.0.0`. Earlier empty resource
state, all nonempty parameters/resources, changed memory, datasource-generated
mode, inherited state, and future fields stay opaque; positive typed profiles
close raw opaque create/edit but retain unchanged/export preservation.

Exact `3.1.0` retains only selector-restricted native opaque create/edit with
reviewed reason
`null-empty-prepare-params-map-breaks-custom-command`. Its curing path returns
a null prepared map when global, local, and `varPool` sources are all empty,
then the DataX custom-command path dereferences it. Exact `3.1.1` adds a null/empty guard. The selector requires strict integer native
`customConfig=0/1`; canonical JSON-only input fails closed. Its
default template is a deliberately non-executable built-in-mode scaffold.
This is a runtime exclusion, not an unreviewed gap.

Through `3.1.9`, workers use `PYTHON_HOME` or `python2.7` plus
`DATAX_HOME/bin/datax.py` and replace CRLF with LF. From `3.2.0`, they use
`PYTHON_LAUNCHER` plus `DATAX_LAUNCHER` and replace CRLF with the worker OS
separator. Every release then substitutes prepared values into JSON and writes
UTF-8; prepared-map forwarding to DataX as `-p -D` begins at `3.1.0`. Typed
input nevertheless rejects task parameters/placeholders and authors no
resources. Because upstream shell-builds `-p -D` without safely quoting
workflow, startup, local, or `varPool` values, a parameter-free workflow is a
prerequisite for the safe typed claim from `3.1.1`. Compilation rejects visible
globals when creating a task, changing execution content/settings, or changing
workflow globals; online activation rechecks the baseline. Metadata-only edits
preserve existing state. Empty local globals cannot establish that startup or
worker-prepared values will be absent when the task runs.
Workers need matching DataX plugins/drivers, connectivity, and source/target
permissions. Complete task params, the final command, and child output enter
INFO logs, with job/command construction also at DEBUG; fields are not secret
storage and dsctl does not redact them. There is no structured output, durable
DataX id, or failover resume. Cancellation stays worker-local across wrapper,
direct-process, and process-tree/application epochs, and retry reruns the whole
transfer with possible duplicate writes. This review refreshes no live
evidence, changes no `tested` flag, and promotes no profile; `3.4.1` remains
stable.

`DATASYNC/create_and_execute` is upstream-absent through `3.1.9` and has
reviewed `Other` membership on all nine exact profiles from `3.2.0` through
`3.4.3`. Normal typed create/edit owns exactly literal `name`,
`sourceLocationArn`, `destinationLocationArn`, optional
`cloudWatchLogGroupArn`, and fixed `jsonFormat=false`. A separate discoverable
`raw-json` variant permits restricted opaque create/edit only when explicit
`jsonFormat=true` selects one valid JSON-object string with no siblings. The
string is preserved, but the worker maps only UpperCamelCase known
`DatasyncParameters` and ignores unknown fields. Only unknown enum values in
inherited `LocalParams`/`VarPool` `Property.Direct/Type` become `null`.
DataSync-specific enum-like strings such as `FilterType` are neither
enum-validated nor null-converted and reach the SDK unchanged for AWS
validation. The mode is not arbitrary AWS `CreateTask` passthrough. Richer
normal state remains unchanged/export opaque-preserve-only.

The upstream normal UI uses one model name for the outer DS task and inner AWS
DataSync Task: create makes them equal and edit folds them together. The REST
wire and CLI can represent different values. Known raw fields also expose exact
upstream bugs: `Options` are ineffective; `Includes` is sent as `Excludes` and
overwrites a real exclude filter; and `Schedule` creates a persistent recurring
AWS Task. DATASYNC never consumes the prepared parameter map, so `localParams`
and placeholders are runtime-dead, and it accepts no resources or DS output.

Exact `3.2.0` through `3.2.2` workers read static
`resource.aws.access.key.id`, `resource.aws.secret.access.key`, and
`resource.aws.region`. Exact `3.3.1` and newer instead read static
`aws.datasync.access.key.id`, `aws.datasync.access.key.secret`, and
`aws.datasync.region`, even though the later upstream guide still documents
the obsolete keys. Complete original and converted task parameters are
INFO-logged; authored values are not secret storage, and dsctl performs no
secret detection or redaction.

Every fresh attempt calls `CreateTask` then `StartTaskExecution`. A callback
persists only `taskExecutionArn` in `appIds`, enabling failover polling and
cancel of that execution after persistence. The persistent AWS Task is never
deleted, its ARN is not stored, and the client is not closed. A pre-callback
failure or retry without durable `appIds` can duplicate executions and leak
persistent or scheduled Tasks. Polling has no internal deadline and produces
no structured output. This review refreshes no live evidence, changes no
`tested` flag, and promotes no profile.

`SAGEMAKER/start_pipeline_execution` is upstream-absent through `3.0.6` and
has reviewed `MachineLearning` membership on sixteen exact profiles from
`3.1.0` through `3.4.3`, excluding `3.1.1`–`3.1.2`. Those two keep a stale
local polling status and can wait indefinitely after initial EXECUTING. Exact
`3.1.0` refreshes an instance field correctly; `3.1.3` repairs the local
assignment. The two holes permit only unchanged/export native preservation. Typed authoring owns one preserved nonblank
`sagemakerRequestJson`, exact-profile `localParams` with unique property names,
and, from `3.2.1`, a positive `datasource`. Without a DS `${...}` or `$[...]`
placeholder, the request must already be one JSON object. With a placeholder,
pre-substitution text may be invalid JSON because upstream substitutes the
whole string without JSON escaping before parsing it. The known public request
shape is `PipelineName`, `PipelineExecutionDisplayName`,
`PipelineParameters[{Name,Value}]`, `PipelineExecutionDescription`,
`ClientRequestToken`, and
`ParallelismConfiguration.MaxParallelExecutionSteps`; unknown or wrongly
cased keys are ignored. The mapper's unknown-enum-to-null option is inert
because this public shape has no enum member.

Projection always emits `localParams` and compiler-owned `resourceList=[]`.
Exact `3.1.0`, `3.1.3`–`3.1.9`, and `3.2.0` forbid `datasource`; from `3.2.1` it is
required and native `type=SAGEMAKER` is fixed. Only `IN` local parameters enter
the prepared substitution map through `3.4.0`; from `3.4.1`, `OUT` declarations
can also substitute, but SAGEMAKER never publishes DS output. Workers through
`3.2.0` use static `resource.aws.*` credentials. Exact `3.2.1` and `3.2.2` use
the required datasource credentials. From `3.3.1`, the datasource remains a
required initialized prerequisite, but the AWS client ignores its values and
uses `aws.sagemaker.*` static or instance-profile configuration instead.

Upstream INFO-logs complete parameters, the resolved request, identifiers,
statuses, and pipeline steps; datasource initialization can also log resolved
credentials. Exact `3.1.0` stores a null application id before start and never
backfills the returned ARN. From typed `3.1.3`, a callback persists the execution
identity for later failover polling and cancellation, but failure before that
callback or retry without durable `appIds` can submit again. Author a stable
`ClientRequestToken` when AWS idempotency matters: an omitted SDK token may be
transient while the plugin's persisted or reread token remains null. Polling
has no internal deadline or task output, and the client is not closed. Richer
native state is unchanged/export opaque-preserve-only. This review adds no
live evidence, changes no `tested` flag, and promotes no profile.

`DMS/resume_existing_full_load` is upstream-absent through `3.1.9` and has
reviewed typed `Cloud` membership on the nine exact profiles from `3.2.0`
through `3.4.3`. Its exact wire contains all five required fields:
`isRestartTask: true`, `isJsonFormat: false`, `migrationType: full-load`,
`startReplicationTaskType: resume-processing`, and a literal AWS DMS
`replicationTaskArn`. Typed create/edit accepts no placeholders, local
parameters, resources, start/create/JSON/CDC configuration, or other native
and future fields. It exposes no public opaque create/edit selector; excluded
state is retained only on export and unchanged edits with opaque-preserve
provenance.

The caller must attest that the ARN is an actual stopped, previously executed
full-load replication task. DolphinScheduler checks only the ARN on the restart
path and cannot verify the remote migration type or current state before the
request. AWS DMS `resume-processing` may reload tables that were partially or
not yet loaded. If the ARN instead refers to a CDC task without
`cdcStopPosition`, upstream can mark the DS task successful soon after start
while remote CDC continues. Destructive `reload-target`, which can truncate or
drop/reload target tables, remains excluded from typed authoring and
opaque-preserve-only.

Workers through `3.2.2` use static `resource.aws.access.key.id`,
`resource.aws.secret.access.key`, and `resource.aws.region`. From `3.3.1`,
`aws.dms.credentials.provider.type` selects static
`aws.dms.access.key.id`/`aws.dms.access.key.secret` credentials or the instance
profile; `aws.dms.region` is required and `aws.dms.endpoint` is optional.
Credentials stay in worker configuration. Upstream logs full task parameters
and remote identifiers at INFO without logging credential values, polls every
`1000` ms without an explicit internal deadline, and returns no DS task output.

After AWS accepts the resume request, a callback persists
`replicationTaskArn` in task-instance `appIds`. Durable `appIds` lets
`AbstractRemoteTask` resume tracking after failover and lets cancellation stop
the same remote task. A crash before callback persistence, or a retry without
durable `appIds`, can send `resume-processing` again. This review refreshes no
live evidence, changes no `tested` flag, and promotes no profile.

`ALIYUN_SERVERLESS_SPARK/literal_jar_submit` is absent before `3.3.1` and has reviewed typed `Cloud` membership on exact `3.3.1`, `3.3.2`,
`3.4.0`–`3.4.3`. Its closed canonical payload owns positive
integer `datasource`; literal `workspaceId`, `resourceQueueId`, `jobName`,
absolute object URI `entryPoint` under `oss://`, and
`sparkSubmitParameters`; a nonempty literal string list
`entryPointArguments`; and optional strict-boolean
`isProduction`, which defaults to `false`. Projection joins arguments with `#`
and emits fixed `type=ALIYUN_SERVERLESS_SPARK` and `codeType=JAR`, so the exact
native wire has ten fields. Edge whitespace, controls, Unicode surrogates, DS
placeholders, and `#` inside an argument are rejected without rewriting other
literal spelling.

Native `PYTHON` and `SQL` select explicit opaque create/edit.
`engineReleaseVersion`, `templateId`, local parameters, variable pools,
resources, richer JAR state, and future fields remain unchanged/export
opaque-preserve-only. Every execution fetches the optional template and may
prepend its Spark configuration, so the final submitted parameters can differ
from the projected task wire. Because typed authoring omits
`engineReleaseVersion`, the template may also derive display release and fusion
values for the SDK request.

Exact `3.3.1` retries only status calls, starts once without a client token,
maps Aliyun `Failed` to DS `KILL`, and logs then swallows cancel failures. Exact
`3.3.2` through `3.4.1` retry template, start, status, and cancel calls for up
to 11 attempts at `1000` ms intervals, reuse one client token within a DS
attempt, map `Failed` to `FAILURE`, and do not preserve remote exception
causes for start/cancel. A DS retry gets a new token and can therefore submit a duplicate.
Exact `3.4.2` keeps that behavior and preserves start/cancel exception causes.
Upstream logs the full task parameters, `jobRunId`, and state at INFO, so every
authored value can appear in logs. Typed fields are not secret storage, and the
CLI does not detect or redact secrets. Datasource-managed access keys are not
task parameters, and task code does not explicitly log them through that object.

The selected datasource supplies static `accessKeyId`, `accessKeySecret`, and
`regionId`, with an optional custom endpoint and default
`emr-serverless-spark.%s.aliyuncs.com`. Its connectivity check makes no remote
call, so credential validity, endpoint and OSS reachability, and Aliyun
permissions remain caller prerequisites. No callback durably stores the
submitted id: failover cannot resume, cancellation needs the in-memory
`jobRunId`, status polling runs every 10 seconds without an internal deadline,
and no DS task output is published. This review adds no live evidence, changes
no `tested` flag, and promotes no profile.

`GRPC/literal_unary_string_record_call` is upstream-absent through `3.3.2` and
has reviewed typed `Universal` membership only on exact `3.4.0`, `3.4.1`, and
`3.4.2`. `GrpcLiteralUnaryStringRecordTaskParamsSpec` requires exactly eight
canonical fields: literal `url`, `channelCredentialType`, `serviceName`,
`methodName`, flat string-record `requestFields` and `responseFields`, exact-key
literal-string `message`, and positive `grpcConnectTimeoutMs`. Each record may
be empty. Otherwise its raw proto field names and valid field numbers must be
unique, protobuf JSON camel-case names must be unique under proto3's
ASCII-case-insensitive collision rule, and message keys must exactly equal the
request field names. The literal `host:port` URL, proto identifiers, and message
strings reject placeholders and controls. Request names also use the
case-insensitive exact denylist `apiKey`, `authToken`, `clientSecret`,
`credential`, `password`, `passwd`, `privateKey`, `secret`, and `token`; that
denylist does not apply to response-only names. The full parameter object is
INFO-logged. JSON Schema names runtime-only uniqueness/key-set checks in
`x-dsctl-runtime-validations`; lint enforces them.

Exact projection generates matching package-free proto3 and protobufjs
definitions with fixed `Request` and `Response` flat string messages and one
blocking unary method. The native wire carries combined
`methodName=serviceName/methodName`, compact message JSON,
`grpcCheckCondition=STATUS_CODE_DEFAULT`, `condition=""`, the timeout, and the
correct Java `channelCredentialType`; top-level `taskExecuteType` is `BATCH`.
Arbitrary proto/descriptor input, packages, streaming, complex field types,
and custom status conditions are excluded. Typed create/edit is available;
raw opaque create/edit is not. Complex native and future state survives only
through unchanged/export opaque preservation.

`TLS_DEFAULT` uses system trust and hostname verification and offers no custom
CA, mTLS, token, or request authentication. The upstream UI writes the wrong
field `grpcCredentialType`, so editing a TLS task there can silently downgrade
it to `INSECURE`. The plugin consumes no `prepareParamsMap`, local/global
parameters, variable pool, or resources. It publishes no DS output, cancel is
a no-op, and it has no durable id or failover resume; retry or failover can
repeat the RPC. Its channel and `NioEventLoopGroup` are never closed. This
review adds no live evidence, changes no `tested` flag, and promotes no
profile.

`OPENMLDB/literal_single_statement` is absent through `3.0.6` and has reviewed
typed `MachineLearning` membership on eighteen exact profiles from `3.1.0`
through `3.4.3`, excluding `3.1.2`. Its inherited Python output handling
dereferences null parameters after SQL execution; that exact hole permits only
unchanged/export preservation, and retries may repeat SQL side effects. Orphaned OPENMLDB UI files in `3.0.x` do not establish a
registered plugin. All eighteen supported profiles keep the same four required
native field names: `zk`, `zkPath`, `executeMode`, and `sql`. Modes are exact
lowercase `offline` or `online`. `zk` accepts a comma-separated DNS/IPv4
`host:port` ensemble with ports from 1 through 65535; `zkPath` accepts an
absolute conservative ZooKeeper znode path; and `sql` accepts one nonblank
literal statement safe in upstream's generated Python. Semicolon, double
quote, backslash, carriage return, unsafe control text, and `${...}` or `$[...]`
are rejected.

Typed OPENMLDB excludes `localParams`, `varPool`, `resourceList`, inherited
`rawScript`, runtime state, and future fields. It has no public opaque
create/edit selector. Existing excluded or otherwise unrecognized native state
round-trips only through export and unchanged edits whose projection
provenance remains opaque-preserve. Workers select `PYTHON_HOME` through
`3.1.9` and `PYTHON_LAUNCHER` from `3.2.0`; they need Python 3, `openmldb`,
SQLAlchemy with its OpenMLDB driver, ZooKeeper reachability, and target-data
permissions. Offline mode enables synchronous jobs with a fixed `1800000` ms
timeout. Upstream logs complete task parameters, raw SQL, rendered Python, and
the final generated Python file at INFO, so none of these fields may contain
secrets. The SQLAlchemy result is discarded. There is no durable application
id or worker-failover resume, and retry can replay the statement and repeat
side effects. This review adds no live evidence, changes no `tested` flag, and
promotes no profile.

`DINKY/job_trigger` is absent upstream through `3.0.6`, then has reviewed typed
membership on all nineteen exact profiles from `3.1.0` through `3.4.3`. Its closed
payload owns only required literal HTTP(S) `address`, required nonblank literal
`taskId`, and optional strict-boolean `online`, which defaults to `false`.
Address values need a host and may include a literal port/path, but reject URI
userinfo, query, fragment, whitespace, control text, and DS placeholders. Task
ids reject whitespace, control text, placeholders, and shell-control syntax.
`localParams`, `varPool`, and future native fields are rejected by typed
create/edit. There is no public raw DINKY selector; excluded state remains
lossless only on export and unchanged edits, and invalid canonical input never
downgrades to opaque.

The legacy request path forwards no workflow or task variables through
`3.2.0`. From `3.2.1`, the worker negotiates the remote Dinky version; variable
forwarding occurs only when it selects the Dinky 1.x `submitApplicationV1`
branch, while the negotiated legacy/v0 branch still forwards none. On that v1
branch, exact `3.2.1` and `3.2.2` send workflow globals and task `localParams`;
exact `3.3.1` through `3.4.1` additionally expand local placeholders from the
prepared context; and exact `3.4.2`–`3.4.3` send the full prepared map—built-in,
project, workflow, task, command, `varPool`, and business values—to Dinky and
logs it at INFO. Upstream also logs task parameters, request URLs, and response
content. It supplies no request authentication or explicit HTTP timeout, no
durable submitted-job id, and no failover resume. Retry may submit the same job
again, while cancellation targets `taskId`. Do not place credentials or secret
values in this path. The source review makes no confidentiality or successful
execution claim, refreshes no live evidence, changes no `tested` flag, and
promotes no profile.

`DVC/operation` is absent upstream through `3.0.6`, then has reviewed typed
membership on all nineteen exact profiles from `3.1.0` through `3.4.3`. Its three
native modes are `Upload`, `Download`, and `Init DVC`; each accepts exactly its
active repository, DVC location, worker path, Git revision, message, or remote
store fields. The upstream plugin interpolates most values into unquoted POSIX
shell positions and logs both parameters and the generated command. Typed DVC
therefore uses a conservative single-shell-token subset, rejects URI userinfo,
restricts revisions to a portable Git tag/branch/SHA subset, and rejects shell
expansion, double quote, backslash, and control text in messages. It supports no typed
`${...}` or `$[...]` placeholders, `localParams`, `varPool`, resources, or
runtime output. Opaque create/edit and export preserve inactive, inherited,
unsafe, and future native fields losslessly.

Eligible DVC workers must be POSIX hosts with `git` and `dvc` in `PATH`, a Git
identity, repository and DVC-remote permissions, network access, and
credentials configured through the worker's SSH agent, credential helper, or
provider environment. Do not put credentials in task URLs. The plugin owns
only a local shell process, has no resumable remote job or failover state, and
runs a script without `set -e`; some clone, directory, remote, commit, tag, and
push failures can be masked by later commands. The UI model's long-standing
`taskType: MLFLOW` default is an upstream UI defect, not the exact task identity;
the registered and authored wire type is `DVC`.

`MLFLOW/model_serve` is absent upstream through `3.0.6`, then has reviewed
typed membership on all nineteen exact profiles from `3.1.0` through `3.4.3`. This
is a model-serving tracer, not full MLFLOW support. It owns exactly the five
required native fields `mlflowTaskType: "MLflow Models"`,
`deployType: "MLFLOW"`, `mlflowTrackingUri`, `deployModelKey`, and
decimal-string `deployPort`. The URI must be absolute HTTP(S) with a host and
only a safe optional path; userinfo, query, fragment, whitespace, control text,
placeholders, and shell metacharacters are rejected. Model keys are limited to
conservative `models:/name/version-or-stage` or
`runs:/run-id/artifact[/subpath...]` forms with nonempty safe components and no
`.` or `..` component. Ports must be serialized as decimal strings from `1`
through `65535`.

Projects mode, Docker deployment, `localParams`, `varPool`, `resourceList`,
`registerModel`, runtime state, and future MLFLOW fields are rejected by typed
create/edit and remain lossless through opaque preservation. Eligible workers
must be POSIX hosts with the `mlflow` CLI in `PATH`, access to the tracking
server and artifact store, access to the selected model version or run artifact, an
available port, and credentials supplied through worker environment or
configuration rather than URI userinfo. Upstream runs a foreground local
service on `0.0.0.0`; cancellation controls only that process, with no remote
deployment ID, reconnect, or failover. A retry may collide with a surviving
process or occupied port. This review does not refresh live evidence, change
`tested`, or promote a profile.

`JUPYTER/preinstalled_notebook` is absent upstream through `3.0.6`, then has
reviewed typed membership on all nineteen profiles from `3.1.0` through `3.4.3`.
It owns a shell-safe preinstalled `condaEnvName`, distinct absolute safe
`.ipynb` `inputNotePath` and `outputNotePath` values, an optional literal
shell-safe string map, optional single-token `kernel` and `engine` selectors,
and optional strict positive integer `executionTimeout` and `startTimeout`
values. Projection omits an empty map, serializes a nonempty map as sorted
compact native JSON, and converts canonical timeout integers to the native
decimal-string wire.

Typed authoring does not own `.tar.gz` or `.txt` environment bootstrap,
`resourceList`, `others`, `localParams`, `varPool`, placeholders, runtime
state, or future native fields. An exact lowercase `.txt` or `.tar.gz`
`condaEnvName` is the only public raw selector and keeps the bootstrap payload
plus its companion native fields lossless through opaque create/edit. Other
excluded native forms remain lossless only during export and unchanged edits;
invalid preinstalled canonical input never downgrades to opaque. The raw
selector follows upstream's case-sensitive suffix check; typed validation
conservatively rejects suffix-like names case-insensitively, so uppercase
forms do not select raw mode. The literal map blocks URI userinfo and only a
bounded case-insensitive exact-name denylist (`password`, `passwd`, `secret`,
`token`, `credential`, `api_key`, `access_key`, and `private_key`). This is not
comprehensive secret detection or redaction. Upstream may log every authored
value and the assembled command, so callers must not use task parameters as
secret storage.

Eligible workers must be POSIX hosts with `conda.path` configured, the selected
conda environment plus Papermill, Jupyter, kernel, and engine already
installed, and readable input plus writable output paths. The output notebook
is a worker filesystem artifact rather than a DS task output. The plugin has
no remote application id or failover-resume protocol, and a retry executes the
notebook again. This review adds no live evidence or profile promotion.

`ZEPPELIN/paragraph` is absent upstream through `2.0.9`, then has reviewed
typed membership on all twenty-six exact profiles from `3.0.0` through `3.4.3`.
It owns one paragraph's safe `noteId` and `paragraphId`, an exact-version
`connectionMode`, and an optional literal string parameter map. The canonical
mode is projected away: `3.0.x` `WORKER_CONFIG` emits only the ids and needs
worker `zeppelin.rest.url`; `3.1.x` through `3.2.0` `REST_ENDPOINT` adds an
anonymous literal endpoint; `3.2.1` and newer `DATASOURCE` adds a positive
datasource id and native `type: ZEPPELIN`.

Canonical `parameters` are a string-to-string mapping that becomes the native
JSON string from `3.1.0`. On `3.0.x`, the same field must stay empty and is
omitted from the wire. Typed keys/values reject control text plus `${...}` and
`$[...]`. Where upstream owns them, whole-note execution, cloning,
authentication fields, `localParams`, `varPool`, `resourceList`, runtime
output, and future native fields remain available only through lossless opaque
authoring/preservation.

Datasource authentication is not inspected or certified by the CLI. On
`3.2.1` and newer, an anonymous/passwordless ZEPPELIN datasource is a user
runtime prerequisite for the typed subset; any credential-bearing mode is an
opaque, explicitly risk-managed choice. Upstream `3.2.0` logs inline
username/password with raw task parameters, `3.2.1` logs the resolved
datasource username after login, and `3.2.2` and newer log the resolved
parameter object including datasource credentials. The CLI does not detect or
redact arbitrary secret-like paragraph values, so do not use them as secret
storage. The synchronous executor exposes no dependable remote application
id or failover resume, so retries may repeat the paragraph. `3.4.1` constructs
result state without reliable downstream publication; `3.4.2`–`3.4.3` publish
runtime `taskName.result` through the var pool. This review adds no live evidence or profile promotion.

`HIVECLI` is absent upstream through `3.0.6`, then has reviewed typed inline
`SCRIPT` membership from `3.1.0` through `3.4.3`. The typed subset owns a
nonblank `hiveSqlScript`, optional literal `hiveCliOptions`, and unique
`IN`/`VARCHAR` `localParams`. Exact `3.1.x` workers substitute placeholders
across the assembled `hive -e` command; releases from `3.2.0` substitute SQL
only, write a temporary SQL file, and execute `hive -f`. To keep one portable
meaning, typed options reject `${...}` and `$[...]` on every reviewed release,
while SQL placeholders remain supported. Every eligible worker needs the Hive
CLI in `PATH`, Hive/HDFS client configuration, and access to HDFS and the Hive
Metastore; the CLI does not provision that runtime or a DS datasource.
`FILE`, `resourceList`, `varPool`, runtime output, and future native fields
remain opaque-preserve-only.

`EMR` is absent upstream on `1.3.9` and `2.0.0`–`2.0.9`, then has reviewed
typed membership from `3.0.0` through `3.4.3`. Exact `3.0.x` supports only
`RUN_JOB_FLOW` and implies that mode without a native `programType` field.
Releases from `3.1.0` support both `RUN_JOB_FLOW` and
`ADD_JOB_FLOW_STEPS`; DolphinScheduler substitutes `${...}` and `$[...]` in
the raw request JSON text only from `3.2.2`. EMR typed parameters are unique
`IN`/`VARCHAR` `localParams`; typed `varPool` is unavailable. A statically
parseable ADD request must contain exactly one `Steps` entry.

EMR execution also requires exact server-side AWS configuration. A `3.0.0`
worker reads `aws.access.key.id`, `aws.secret.access.key`, and `aws.region`.
Workers from `3.0.6` through `3.2.2` instead read
`resource.aws.access.key.id`, `resource.aws.secret.access.key`, and
`resource.aws.region`. Releases from `3.3.1` use AWS authentication and the
`aws.emr.*` entries in `aws.yaml`. These credentials are configured in
DolphinScheduler, not authored or stored by the CLI. Upstream EMR task failover
is not implemented; typed compatibility does not add or promise an EMR
high-availability layer.

`EMR_SERVERLESS/start_job_run` is typed on exact `3.4.2` and `3.4.3` and is absent
upstream before `3.4.2`. It owns literal `applicationId`,
`executionRoleArn`, and optional `jobName`, plus raw
`startJobRunRequestJson` and unique `IN`/`VARCHAR` `localParams`. DS
substitutes `${...}` and `$[...]` only inside the request JSON. Unresolved
runtime placeholders must stay in quoted JSON strings; exact local-parameter
substitution must still produce a JSON object. The top-level values stay
literal and override matching JSON members.

The worker needs `aws.emr.*` credentials and region or a usable AWS SDK default
credential chain, access to a pre-existing `STARTED` or `CREATED` application,
and the required service and job-data permissions. Custom endpoints use
`emr.serverless.endpoint` or `EMR_SERVERLESS_ENDPOINT`; the executor does not
read `aws.emr.endpoint`. Upstream persists `jobRunId` through task-instance
`appIds` and resumes polling after failover. Runtime and future native fields
remain opaque-preserve-only. This review does not refresh live evidence,
change `tested`, or promote `3.4.2`.

DolphinScheduler `3.4.2`–`3.4.3` resource-file SQL remains cataloged only for opaque
preservation. Other source-known, unreviewed task coordinates and additional
native fields likewise remain on their explicit opaque paths. No source
fingerprint authorizes a typed membership without a recorded semantic review,
and these memberships do not refresh live evidence, change `tested`, or
promote a profile.

### Workflow boundaries

- On exact `1.3.9`, instance detail reads dereference the current workflow
  definition. Reading an instance after deleting its definition can return
  generic upstream `10116`; that code alone does not prove the instance is
  absent. Inspect required runtime details before deleting the definition.

- On exact `3.3.1`, the upstream parent-online check reads the old child-code
  field and can miss offline child workflows. `3.3.2` fixes this mismatch.
  Publish children before running the parent; parent publication by itself
  does not establish child readiness. See the source explanation in
  [SUB_WORKFLOW boundaries](../development/task-authoring-boundaries.md#sub_workflow).

The normalized workflow surface also preserves exact upstream limits:

- `1.3.9` workflow create/edit/get/export/describe/digest and execution compile
  through its string-id/name-native whole-definition graph. Run, run-task, and
  backfill use the exact legacy execution recipes; run-task and task-scoped
  backfill do not hydrate nested-workflow names. Workflow-instance export and
  edit project the legacy `processInstanceJson` mutation without inventing
  code-native identities. Workflow lineage remains absent upstream.
- workflow lineage graph reads are available from `2.0.0`; dependent-task
  lineage is available from `3.2.2`.
- Exact `3.3.1` and `3.3.2` omit owned-lineage cleanup during whole-workflow
  deletion. The CLI checks lineage and blocks deletion when owned dependency
  records remain, including historical rows; failed reads also stop deletion.
  This does not repair existing orphan rows or exclude concurrent edits.
  See the [deletion contract](../reference/cli-contract.md#dsctl-workflow-delete-workflow---force)
  for guidance. DS `3.4.0` adds native whole-workflow lineage cleanup.
- `expectedParallelismNumber` is absent on `1.3.9`: omission sends no field and
  an explicit value fails before I/O. From `2.0.0`, omission normalizes to `2`.
- workflow-definition `tenantCode` exists from `2.0.0` through `3.1.9`, while
  workflow execution carries `tenantCode` from `3.2.0`. In `3.2.x`, definition
  mutations omit it but execution retains it.

### Alert compatibility boundaries

Alert groups and alert-plugin instances use exact generated wire contracts for
each reviewed profile. DS `1.3.9` alert-group create/update select the native
`EMAIL` or `SMS` group type, while later profiles associate alert-plugin
instance ids. Selected-version schema exposes the applicable input and hides
the inapplicable one; list/get/delete retain the same stable resource meaning.

DS `1.3.9` predates alert-plugin definitions and instances, so every
`alert-plugin` action is absent on that profile. Alert-plugin list, definition,
schema, get, create, update, and delete are representable from DS `2.0.0`.
Test-send is introduced in DS `3.2.1`; earlier exact profiles report
`upstream_capability_absent` without sending a request. Exact DS `2.0.0` has no
server-side instance-name search parameter, so `dsctl` applies the same bounded
case-insensitive substring filter locally. DS `3.2.1` and `3.2.2` also require
temporary delivery fields on mutation: create uses the official UI defaults
and update preserves the current warning type. Those wire-only details do not
change the stable CLI surface.

## Selection

```bash
export DS_VERSION="3.4.1"
dsctl version
```

When `--env-file` is used, that file is the isolated connection source.
Inherited process `DS_API_URL`, `DS_API_TOKEN`, and `DS_VERSION` values cannot
override it or fill omitted identity keys. Retry and timeout settings resolve
separately and may still come from the process environment; see
[Connection Settings](configuration.md#connection-settings).

The `version` command reports:

- selected server version
- generated contract version
- compatibility family
- support level
- selectable server versions

## Compatibility Policy

Services keep stable CLI terms such as `workflow` and `workflow-instance`.
Version-specific names such as `processDefinitionCode` belong in exact domain
bindings and generated wire contracts, not commands or service interfaces.

Static contract analysis is not enough for a stable support claim. A profile
may be selectable as `experimental` once all stable actions have terminal
decisions and supported actions have executable exact recipes. Promotion also
requires generated-artifact freshness, complete dependency and support gates,
service-level error translations, and release-specific live evidence. Registry
entries marked `full` or `legacy_core` must also be tested; untested selectable
versions remain `experimental`.

For developer workflow around contract diffs, see
[Codegen](../development/codegen.md).
