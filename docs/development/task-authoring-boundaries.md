# Reviewed task authoring boundaries

These exact-family rules bound task schemas, templates, validation, projection,
reference resolution, activation and opaque preservation. Source facts and review
decisions remain owned by the task profile compiler inputs; a model or shared
implementation does not grant a typed capability.

Templates expose one default fragment per type with short optional field hints.
Default retrieval needs no variant selector. Advertise a separate scene only when
it supplies complete coupled output code/declarations, resources/script invocation,
a different reference target, or a distinct business operation. Remove pure aliases
and field-only parameter/lifecycle selectors from both discovery and implementation;
short field examples consumed by default guidance are not complete alternate task
factories. Exact holes, typed facets and worker prerequisites still constrain every
fragment. See [complete task combinations](../user/task-examples.md) for scoped
parameter, branch, child-workflow and cross-workflow examples. This presentation
change does not expand typed memberships or opaque selectors.

## SWITCH parameter inputs

Exact `2.0.0` through `3.2.1` evaluate SWITCH conditions from workflow globals
and incoming task-instance varPool. Current-task localParams do not enter those
maps, including through the predecessor varPool preparation path. Their templates
therefore require `workflow.global_params.route` or a predecessor OUT value with
a real incoming edge. From `3.2.2`, the evaluator consumes the prepared input map,
which includes localParams. Templates and schema project this same boundary;
identical expressions do not prove identical input binding. `1.3.9` has no SWITCH.
Local compile checks do not execute the DS JavaScript evaluator or certify workers.

## Exact task coverage

Task source facts cover **37 exact releases**. The explicit reviewed set contains
**42 families and 902 exact memberships**, including 449 memberships admitted
from the 21 intermediate releases. The table records closed typed facets, not all
native plugin modes or worker readiness. An unavailable facet is not automatically
available for opaque create/edit; the family sections retain those separate rules.
Every exact member has its own source tree, semantic fingerprint and reviewed
source paths in [task facts](../../tools/ds_codegen/task_profile_facts.json) and
[review decisions](../../tools/ds_codegen/task_profile_reviews.json). No runtime
support is inferred from neighboring versions or shared Python models.

| Exact releases | Reviewed typed families per release |
| --- | ---: |
| `1.3.9` | 11 |
| `2.0.0` | 13 |
| `2.0.1`–`2.0.9` | 14 |
| `3.0.0`–`3.0.6` | 20 |
| `3.1.0`–`3.1.2` | 27 |
| `3.1.3` | 29 |
| `3.1.4` | 30 |
| `3.1.5`–`3.1.9` | 31 |
| `3.2.0` | 37 |
| `3.2.1` | 36 |
| `3.2.2` | 38 |
| `3.3.1`–`3.3.2`, `3.4.0`–`3.4.1` | 34 |
| `3.4.2`–`3.4.3` | 35 |

| Family | Exact typed membership | Count |
| --- | --- | ---: |
| `ALIYUN_SERVERLESS_SPARK` | `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 6 |
| `BLOCKING` | `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2` | 20 |
| `CHUNJUN` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 19 |
| `CONDITIONS` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `DATASYNC` | `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 9 |
| `DATAX` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.1`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 36 |
| `DATA_FACTORY` | `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 9 |
| `DATA_QUALITY` | `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2` | 20 |
| `DEPENDENT` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `DINKY` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 19 |
| `DMS` | `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 9 |
| `DVC` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 19 |
| `DYNAMIC` | `3.2.2` | 1 |
| `EMR` | `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 26 |
| `EMR_SERVERLESS` | `3.4.2`–`3.4.3` | 2 |
| `FLINK` | `3.0.0`–`3.0.6`, `3.1.2`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 24 |
| `FLINK_STREAM` | `3.1.5`–`3.1.9`, `3.2.0`–`3.2.2` | 8 |
| `GRPC` | `3.4.0`–`3.4.3` | 4 |
| `HIVECLI` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 19 |
| `HTTP` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `JAVA` | `3.2.0`, `3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 8 |
| `JUPYTER` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 19 |
| `K8S` | `3.1.4`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 15 |
| `KUBEFLOW` | `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 9 |
| `MLFLOW` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 19 |
| `MR` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `OPENMLDB` | `3.1.0`–`3.1.1`, `3.1.3`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 18 |
| `PIGEON` | `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2` | 30 |
| `PROCEDURE` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `PYTHON` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `PYTORCH` | `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2` | 15 |
| `REMOTESHELL` | `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 9 |
| `SAGEMAKER` | `3.1.0`, `3.1.3`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 17 |
| `SEATUNNEL` | `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 26 |
| `SHELL` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `SPARK` | `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 26 |
| `SQL` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `SQOOP` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `SUB_WORKFLOW` | `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 37 |
| `SWITCH` | `2.0.0`–`2.0.9`, `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 36 |
| `WATERDROP` | `2.0.1`–`2.0.9` | 9 |
| `ZEPPELIN` | `3.0.0`–`3.0.6`, `3.1.0`–`3.1.9`, `3.2.0`–`3.2.2`, `3.3.1`–`3.3.2`, `3.4.0`–`3.4.3` | 26 |

Typed SCRIPT resources and MR/JAVA JARs require a visible, non-directory FILE
resolved before mutation. Canonical resource names are relative to the FILE
root, such as `/jobs/app.jar`. Native wire through `3.1.9` uses the verified
positive resource id; from `3.2.0`, `ResourceInfo.resourceName` carries the
verified storage absolute `fullName`, such as `/tenant/resources/jobs/app.jar`.
A read without that verified binding remains opaque. This projection does not
repair the exact `3.2.1` JAVA double-prefix runtime hole or grant `1.3.9` PYTHON
resource authoring.

## Intermediate exact parameter and execution transitions

The following source transitions determine the admitted facets. Existing fields,
validation and worker prerequisites remain closed unless explicitly listed here.
These are static source-backed decisions; they do not claim a full behavioral or
live execution proof.

| Family / parameter path | Exact transition and boundary |
| --- | --- |
| WATERDROP | Worker `TaskPluginManager` first registers the SHELL alias in `2.0.1`; `2.0.0` remains unregistered. The compiler-owned local config wrapper is reviewed on `2.0.1`–`2.0.9`. |
| PROCEDURE | `2.0.1` retains positional call binding; `2.0.2` adds named binding and runtime `outProperty`. Runtime-owned output fields are not authorable. |
| CONDITIONS / SWITCH | Native branch reference lists contain strings on `2.0.0`–`2.0.7`, then longs on `2.0.8`–`2.0.9`. Canonical task-name references remain unchanged. |
| Workflow startup parameters | `2.0.0`–`2.0.7` accept overrides only for declared globals; `2.0.8`–`2.0.9` preserve arbitrary startup keys. |
| Task-definition deletion | `2.0.2` and `2.0.3` reject deletion whenever the task has `flag=YES`, including an orphan left after workflow deletion. An owned orphan must be released `OFFLINE` before deletion. `2.0.4` changes the rejection to require both `flag=YES` and a task belonging to an online workflow. This cleanup boundary does not change the authoring default. |
| SUB_WORKFLOW inputs | In `2.0.0`–`2.0.2`, child-defined globals win duplicate names. In `2.0.3`–`2.0.9`, parent globals win through `ProcessService.joinGlobalParams`. |
| SUB_WORKFLOW outputs | `2.0.0`–`2.0.6` have no child-output copy. `2.0.7`–`2.0.9` add a task-local OUT copy only through `SubTaskProcessor.killTask → dealFinish1`; normal successful completion does not call it. This corrects the prior unqualified `2.0.9` task-local-output description. |
| DEPENDENT | `thisMonthBegin` / `thisMonthEnd` join in `3.0.2` and `3.1.1` within their respective lines. |
| DATA_QUALITY | The exact master comparison makes canonical count equality compile to `NE` on `3.0.0`–`3.0.3`, `EQ` on `3.0.4`–`3.0.6`, then `NE` on `3.1.0`–`3.1.9`. Live rule and datasource attestation remain required. |
| FLINK | `3.1.0` resolves a missing SQL `mainJar`. `3.1.1` fixes that guard but reverses LOCAL/CLUSTER SQL targets; local-inline SQL remains excluded. `3.1.2` fixes target selection. |
| FLINK_STREAM | Its own missing-mainJar dereference remains through `3.1.4`; ordinary FLINK fixes do not repair it. `3.1.5` first admits this stream facet. |
| K8S | The watcher counts down on RUNNING with result `-1` in `3.1.0`–`3.1.3`. `3.1.4` first waits for terminal status; typed membership begins there. |
| OPENMLDB | Only `3.1.2` leaves the Python parent's parameters null, so inherited output handling fails after SQL execution. Typed and raw authoring are closed; existing native state can be preserved. |
| SAGEMAKER | `3.1.0` refreshes an instance status field correctly. `3.1.1`–`3.1.2` discard refreshed local status and can poll forever after initial EXECUTING; only those two are holes. `3.1.3` assigns the returned status. |
| DATAX | `3.1.1` repairs the null/empty map guard that excludes `3.1.0`; unsafe prepared-value shell forwarding persists, including after `3.1.4` quoting changes. Visible workflow globals are blocked on create, runtime/global edits and activation; startup/worker-prepared emptiness remains a separate prerequisite. |
| SEATUNNEL | `3.1.0`–`3.1.5` use `engine=SPARK`, `useCustom=true`, `deployMode=local` (enum renders client). `3.1.6` requires `deployMode=client`, `master=LOCAL` to retain client/local execution. `3.1.7` introduces `startupScript=seatunnel.sh`, custom/local native wire. Extra engines remain excluded. |
| CHUNJUN | `3.1.1` replaces nondurable post-exit app-id discovery with empty IDs. The UI custom-config default is false through `3.1.6`, true from `3.1.7`; typed strict integer custom mode is independent of that UI default. |
| MR | `2.0.2` adds configurable YARN kill on master failover. `3.1.1` transports nondurable application observations but retains soft-root cancellation; `3.1.2` changes process-tree cancellation. Neither grants durable callback identity or reattachment. |

The exact controller, parameter, worker/master and UI paths for each decision are
retained in its review's `evidence_paths`. The confirmed FLINK and OPENMLDB failure
chains are also documented in the
[intermediate source audit](intermediate-release-source-audit.md).

Exact `3.4.3` retains the 35 typed families of `3.4.2`, including all existing
facets and worker prerequisites. The review covers every changed source in those
families' evidence closures. DEPENDENT repairs the negative `ALL` key roundtrip;
the execution DTO still supplies the same instance id and start time, so date
windows and canonical targets stay unchanged. DataX adds an empty-object resource
fallback, requiring a nonempty inline object for its existing typed literal
facet. The shared task API maps SIGINT exit code 130 to killed without adding
durable identity or resume. LINKIS remains excluded and FLINK_STREAM retains its
exact runtime hole. Parameter-type qualification refreshes old fingerprints
against identical source trees without expanding their reviewed membership or
changing historical live evidence.

Exact legacy task-parameter
projection is version selected, DVC owns only its safe-shell three-mode
subset with no typed parameters or placeholders, MLFLOW owns only its safe
five-field `model_serve` subset, HIVECLI `FILE`, other MLFLOW modes and
fields, JUPYTER owns only its preinstalled-notebook safe-shell subset, and
EMR Serverless runtime/future fields remain
opaque-preserve-only, and `3.4.2`–`3.4.3` resource-file SQL remains
opaque-preserve-only; ZEPPELIN owns only one literal paragraph, exact
worker-config/REST/datasource projection, and literal string-map parameters,
while whole-note/clone/auth/output and future state remain opaque or
runtime-only, datasource anonymity is a user prerequisite rather than a
CLI-attested fact, and the executor has no failover resume; DINKY owns only
literal `address`, literal `taskId`, and strict-boolean `online` on the nineteen
exact profiles from `3.1.0`, exposes no raw opaque-authoring selector, and
keeps inherited/future state only for unchanged/export preservation; its
exact executor epochs keep the legacy/v0 request path free of variable
forwarding; from `3.2.1`, globals/local values and the later placeholder/full
`prepareParamsMap` behavior occur only when version negotiation selects the
Dinky 1.x `submitApplicationV1` branch, including conditional INFO map logging
on `3.4.2`–`3.4.3`, while remote requests have no authentication, explicit timeout,
durable job id, or failover resume and retries may submit again

## SEATUNNEL

`SEATUNNEL/literal_local_config_job` is typed in `DataIntegration` on all
twenty-six exact profiles from `3.0.0` through `3.4.3`. Canonical authoring owns
only one preserved literal ASCII `rawScript` SeaTunnel configuration with
LF/tab whitespace and no DS placeholders. Exact `3.0.x` projects a
compiler-owned REST-only POSIX wrapper that writes the config and launches
`start-seatunnel-spark.sh` in client/local mode, bypassing an upstream UI
defect that emits a Waterdrop command; exact `3.1.0`–`3.1.5` use native
SPARK/custom-config `deployMode=local`, whose enum renders client mode. Exact
`3.1.6` instead emits `deployMode=client` and `master=LOCAL`, retaining
`--deploy-mode client --master local`. Exact `3.1.7` onward uses the native
`seatunnel.sh` custom-config/local wire. Resource mode, arbitrary shell,
alternate engines or launchers, parameters, outputs, and extra state are
excluded, including the staged-resource path holes on `3.2.0` through
`3.2.2`. From `3.3.1`, typed compilation and online activation require a
parameter-free workflow because upstream forwards globals outside the
facet. Raw opaque create/edit is closed; richer native state is
unchanged/export preserve-only. Workers require Unix-like execution,
`SEATUNNEL_HOME`, Java, compatible launchers/connectors/catalogs, and data
access. Upstream INFO-logs task params, config, and commands, publishes no
output or durable application id, cannot reattach on failover, cancels
best-effort, and can replay the job on retry. This review refreshes no live
evidence and promotes no profile

## BLOCKING

`BLOCKING/same_workflow_state_gate` is typed in `Logic` on the twenty exact
profiles from `3.0.0` through `3.2.2` and is upstream-absent from `3.3.1`.
Canonical authoring owns `BlockingOnSuccess` or `BlockingOnFailed`, strict
`alertWhenBlocking`, and a nonempty two-level `AND`/`OR` tree of literal
same-workflow task-name `SUCCESS`/`FAILURE` predicates. The compiler resolves
every name to a positive `depTaskCode` and adds a real DAG predecessor edge.
The upstream tags expose no BLOCKING authoring form, so the facet is
REST-only. Typed create/edit is open and raw opaque create/edit is closed.
Richer state survives only from an existing-server baseline during unchanged
or metadata-only edits; standalone YAML carries no opaque provenance and
cannot independently reapply it. The task itself completes `SUCCESS`. A
match moves the workflow through drain-sensitive `READY_BLOCK` to terminal
`BLOCK`; a miss continues without ordinary task retry. Exact `3.0.0` through
`3.0.6` mark standby work `KILL`, while later reviewed versions use `PAUSE`.
`alertWhenBlocking` requests a record for the workflow `warningGroupId`, but
delivery requires a valid alert group and infrastructure that the facet does
not validate. Upstream INFO logs task codes, expected/actual states,
aggregates, and opportunity, so fields are not secret storage. The
master-local task has no worker, resource, datasource, structured output,
durable application id, or dedicated failover resume. Pause/kill changes
task-local state only through `3.1.9` and is warn/no-op on `3.2.x`; neither
epoch has a remote target. Infrastructure replay or workflow rerun may
reevaluate and repeat the alert. This review refreshes no live evidence and
promotes no profile

## SQOOP

`SQOOP/literal_command` is typed on all 37 exact profiles. Canonical
authoring owns only literal `subcommand: import|export` and a nonempty ordered
`args` list, and exact projection emits compiler-owned `jobType=CUSTOM`,
`localParams=[]`, and one POSIX-quoted `customShell`. Releases through
`3.1.0` write UTF-8; exact `3.1.1` and newer use a platform-default writer and
therefore restrict typed arguments to ASCII. CRLF becomes LF through
`3.1.9` and the worker OS separator from `3.2.0`. Controls, surrogates, edge
whitespace, placeholders, interactive `-P`, standalone `--password`, and
`--password=...` are rejected; worker-readable `--password-file` is allowed.
Native TEMPLATE jobs and arbitrary CUSTOM scripts use selector-restricted
opaque authoring, while richer state is unchanged/export preserve-only.
Command-bearing state is INFO-logged and not secret storage. SQOOP has no DS
output, durable application id, or failover reattachment; cancellation is
best-effort and retry/failover may duplicate the transfer

## CONDITIONS

exact `1.3.9` `CONDITIONS` owns only grouped same-workflow task-name predicates
with `SUCCESS`/`FAILURE`; both relation levels accept only `AND`/`OR`,
`dependTaskList` and every `dependItemList` are nonempty, and each success and
failure branch contains exactly one task name with different targets. Its
projector preserves the legacy split outer `TaskNode`
wire: `dependence` uses native `depTasks`, `conditionResult` uses branch names,
and both remain beside exact `params={}` rather than empty
`localParams`/`varPool` members.
Parameters/placeholders, resources, datasource state, richer native members,
and future fields are excluded from typed create/edit. Richer split outer
state survives only on an existing server baseline when the task payload,
task names, and topology are unchanged, as in a metadata-only edit;
standalone YAML export does not
carry it and is not lossless. No raw opaque create/edit selector exists, and
invalid typed input fails instead of downgrading to preservation. The task
runs master-local, logs task names and expected/actual states at INFO, and has
no remote cancel/resume identity; retry or master failover reevaluates
persisted state from the same process instance

## DEPENDENT

exact `1.3.9` `DEPENDENT` owns grouped cross-project literal-name targets,
with `DEPENDENT_ON_WORKFLOW` or `DEPENDENT_ON_TASK`, paired base
hour/day/week/month `cycle` and `dateValue` values, and no placeholders,
parameters, resources, datasource state, or raw opaque create/edit selector.
The workflow service resolves exact project/workflow names, including
numeric-looking names, to positive database IDs and verifies literal target
task names before the graph compiler emits `params={}` beside outer
`TaskNode.dependence`; whole-workflow targets use `depTasks=ALL`, while task
targets use the literal name. `DEPENDENT` creates no workflow DAG edge.
Safe workflow-definition reads reverse-bind names best-effort; unresolved or
richer split outer state remains opaque. Preservation requires a server
baseline and keeps that opaque task's name, type, task parameters, and command
unchanged, while description edits and unrelated topology additions remain
allowed; standalone YAML and workflow-instance export are not lossless
carriers. The task runs master-local, logs target identity/window/state at
INFO, exposes no output, durable remote app id, or reconnectable cancel/resume
target. Scheduled retry/failover retains the original `scheduleTime` while
reevaluating state; only manual/current-time reinitialization can shift the
relative window. Exact runtime date
projection is base-only on `1.3.9`, `2.0.0`–`2.0.9`, `3.0.0`–`3.0.1`, and `3.1.0`;
`thisMonthBegin` and `thisMonthEnd` join on `3.0.2`–`3.0.6`, `3.1.1`–`3.1.9`, and
`3.2.0` through `3.4.3`. `dateValue`, not `cycle`, selects the runtime
window. Target existence is checked while authoring, but upstream does not
attest target-project authorization; later target rename, deletion, or state
change remains a runtime prerequisite. This review refreshes no live evidence
and promotes no profile

## SUB_WORKFLOW

exact `1.3.9` canonical `SUB_WORKFLOW` owns only a literal same-project
`childWorkflowName`; the service resolves that name explicitly, including
numeric names, to a positive native `processDefinitionId`, audits the full
reachable native child closure for missing targets and cycles, and leaves the
legacy graph compiler solely responsible for emitting native `SUB_PROCESS`.
`workflowDefinitionCode`, parameters, resources, placeholders, and richer
native state are excluded from typed authoring. Safe reads reverse-resolve an
exact id-only package to the canonical name; unresolved or richer packages
remain opaque. The task is master-local, parent globals feed the child while
task `localParams` are ignored, durable parent/child linkage supports
recovery reuse, pause/stop propagates subject to child-task cancellation,
and child retry or REPEAT can repeat side effects. There is no structured
output or app id. Descendants can change after authoring, so current
existence, acyclicity, and release readiness remain operator prerequisites;
no live receipt or profile promotion is claimed

Exact `3.3.1` does not reliably reject an offline child when its parent is
published. `ProcessServiceImpl.findAllSubWorkflowDefinitionCode` checks
`CommandKeyConstants.CMD_PARAM_SUB_WORKFLOW_DEFINITION_CODE`, whose value is
still `processDefinitionCode`, while `SubWorkflowParameters` owns
`workflowDefinitionCode`. Exact `3.3.2` fixes that constant. Keep the native
task field and explicitly publish children before running their parent;
successful parent publication alone does not prove child readiness.

On `2.0.0`–`2.0.2`, child-defined globals win duplicate names; parent globals
win from `2.0.3`. Child output is absent through `2.0.6`. On `2.0.7`–`2.0.9`,
`dealFinish1` copies matching child globals into task-local OUT values only when
`killTask` calls it. Normal completion does not traverse this method; this is
`task-local-out-on-cancel`, not a general child output contract. These facts are
independent of startup-key acceptance, which expands in `2.0.8`.

## PYTHON

`PYTHON` is typed on all 37 exact profiles. Exact `1.3.9` owns `rawScript`,
unique `IN`-only nine-scalar `localParams`, and an empty `resourceList`, with
no `varPool`, structured output, `LIST`, or `FILE`. Process-definition
authorization checks only positive resource IDs, while the master's old
`id=0`/`res` full-name branch resolves a tenant without that user permission
context before the worker downloads the resource;
typed 1.3.9 therefore has no resource attachment or resource variant, and
nonempty native resource state remains unchanged/export opaque. Runtime
normalizes CRLF, substitutes
prepared values without Python escaping, writes UTF-8, and logs full params,
source, command, and output at INFO. These fields are not secret storage;
cancel is local best-effort, no process resumes after failover, and retry
reruns the whole script with possible duplicate side effects

## MR

`MR/literal_java_jar_job` is typed in `Universal` on all 37 exact profiles.
Its canonical payload owns one absolute shell-safe DS resource `fullName`
ending in `.jar`, one strict dotted ASCII Java `mainClass`, and ordered
shell-safe `mainArgs`. Through `3.1.9`, the service resolves `mainJar` to a
visible non-directory `FILE` with a positive id before emitting
`mainJar={id}`; from `3.2.0`, projection emits
`mainJar={resourceName}` and `yarnQueue=""`. Every epoch fixes
`programType=JAVA`, empty `appName`/`others`, and empty
`localParams`/`resourceList`; the JAR is not duplicated in `resourceList`
because the MR model appends it at runtime, and queue choice is
runtime-owned. Native `SCALA` has selector-restricted opaque create/edit,
while the UI-advertised `PYTHON` mode is excluded because the executor still
invokes `hadoop jar`; richer typed-adjacent state is unchanged/export
preserve-only. Workers need Hadoop/YARN, tenant execution permission,
readable DS resource storage, queue access, and data access. Upstream
INFO-logs complete params, command files, final commands, and child output;
fields are not secret storage. MR publishes no DS output or durable callback
identity, cannot reattach after failover, and has only best-effort
cancellation from an observed application id. Retry/failover can rerun the
whole JAR, while `3.3.1` onward also has a post-exit `appIds` context
transport hole. This review refreshes no live evidence and promotes no
profile

## SQL

Exact `1.3.9` `SQL` owns a closed inline subset: eight executable datasource
types excluding H2, positive datasource id, literal SQL, strict query/update
mode, nonnegative display/limit values, unique `IN`-only nine-scalar
parameters, nonblank pre/post statements, and a unique-key HIVE-only
`connParams` map. Projection fixes `sendEmail=false`, empty UDF/mail state,
and `showType=TABLE`; `groupId` and `varPool` are absent. Richer native state
selects explicit opaque authoring only as one complete native parameter
package; partial invalid typed input never downgrades, while unchanged/export
preservation remains lossless. The
workflow API does not attest datasource permission/type; upstream logs SQL,
connection params, bound values, and rows, has no transaction spanning all
phases, durable id, structured output, or reliable JDBC cancel, and retry or
failover replays the complete sequence

## SPARK

`SPARK/inline_local_sql` is typed from `3.0.0` through `3.4.3`; the earlier
profiles retain the SPARK plugin only through opaque paths because
their upstream model has no SQL program mode. The canonical payload owns only
literal `rawScript`; exact projection selects `SPARK2`, `SCRIPT`, or
`SCRIPT` plus `master=local` wire epochs. Typed SQL rejects DS placeholders
and all other task fields. Native `JAVA`, `SCALA`, `PYTHON`, and, from
`3.2.0`, SPARK SQL `FILE` use explicit opaque authoring. Workers use
`SPARK_HOME2` through `3.1.9` and `SPARK_HOME` from `3.2.0` and require Spark
SQL, Java, Hadoop, Hive catalog configuration, and data permissions. Upstream
logs SQL at INFO, exposes no durable local application id or failover resume,
and retries reexecute the whole SQL, so repeated side effects remain a caller
concern; this review refreshes no live evidence and promotes no profile

## FLINK

`FLINK/inline_local_sql` is typed on exact `3.0.0`–`3.0.6`, `3.1.2`–`3.1.9`,
and `3.2.0` through `3.4.3`; earlier profiles have no SQL program mode. Exact
`3.1.0` resolves `mainJar` before SQL initialization; `3.1.1` instead reverses
LOCAL/CLUSTER SQL execution targets. Both are typed holes. Exact `3.1.1`
permits only selector-restricted native opaque modes; canonical inline SQL
fails closed. The canonical payload owns only literal `rawScript` and projects
`programType=SQL`, `deployMode=local`, and `initScript=""`. Exact `3.0.x`
narrows to ASCII for its platform-default writer; `3.1.2` and newer use
UTF-8. Typed SQL rejects blanks, CR, DEL/C0/C1 controls, and placeholders but
preserves valid multi-statement SQL spelling. Recognized application and
exact non-local SQL modes use explicit opaque create/edit; local-inline extras
remain unchanged/export preserve-only. Workers use `PATH` through `3.2.2`
and `FLINK_HOME` from `3.3.1`; `3.4.2`–`3.4.3` replacement of prepared placeholders
stays behind the typed placeholder ban. Upstream logs parameters, SQL, paths,
and commands at INFO, requires a configured Flink SQL/Java/connector/catalog
runtime and data permissions, exposes no task output, durable id, or failover
resume, and reexecutes all SQL on retry; this review refreshes no live
evidence and promotes no profile

## FLINK_STREAM

`FLINK_STREAM/inline_local_sql` is typed only on exact `3.1.5`–`3.1.9` and
`3.2.0` through `3.2.2`; it is upstream-absent through `3.0.6`, exact
`3.1.0`–`3.1.4` retain the plugin-specific broken-`mainJar` hole, and `3.3.1` through `3.4.3` are runtime holes because
`ExecutorServiceImpl.execStreamTaskInstance` immediately throws
`Not supported`. Untyped registered coordinates keep their explicit opaque
authoring policies; `3.1.1`–`3.1.4` require a recognized native selector. Canonical `rawScript` projects to exactly four `taskParams`
fields—`programType=SQL`, `deployMode=local`, `initScript=""`, and unchanged
`rawScript`—plus compiler-owned top-level `taskExecuteType=STREAM`. All eight
typed exact profiles use UTF-8 and `PATH`, and reject blanks, CR, DEL/C0/C1 controls,
surrogates, placeholders, and extras without rewriting valid multi-statement
SQL. Recognized JAR and exact non-local SQL modes use explicit opaque
create/edit; local-inline extras and unknown state remain unchanged/export
preserve-only. Upstream logs parameters, SQL, paths, and commands at INFO and
requires Flink SQL/Java/connector/catalog/data setup. Local SQL produces no
expected application id, result output, durable id, or failover resume;
cancel/savepoint require an app id. Stop order is plugin cancel then PID kill
on `3.1.x`, PID kill then plugin cancel on `3.2.x`, and plugin cancel only
from `3.3.1`, with no reliable stop. An unbounded stream may continue after
the DS task stops; retry reexecutes all SQL on typed releases. This review
refreshes no live evidence and promotes no profile

## K8S

`K8S/literal_container_job` is typed in `Cloud` on the fifteen exact profiles
from `3.1.4` through `3.4.3`; earlier profiles through `3.0.6` are upstream-absent,
while exact `3.1.0`–`3.1.3` remain runtime exclusions because its watcher counts
down on `RUNNING` with exit status still `-1`. Its raw opaque create/edit is
selector-restricted to no canonical `connectionMode` or sibling `cluster`,
a nonblank image, and compact native namespace JSON with nonblank `name` and
`cluster`; canonical connection intent fails closed, and the default template
is an explicit risk scaffold. Canonical image tag/digest,
nonnegative CPU/MiB memory, and literal `IN` environment entries with unique
names are shared. Outer task names match
`^[A-Za-z0-9][A-Za-z0-9-]{0,51}$`; runtime lowercases with `Locale.ROOT`
and appends `-` plus the task-instance id. Exact connection projection uses
`NAMESPACE` plus compact
namespace/cluster JSON through `3.2.2`, then `DATASOURCE` with a positive id,
fixed `type=K8S`, and compiler-owned empty `namespace`/`kubeConfig` from
`3.3.1`. Exact `3.2.2` hides the namespace selector in its UI and validates
only image, but backend/master/runtime still require the legacy namespace
wire. Structured command/args, pull-secret object name, pull policy, labels,
and selectors join from `3.2.0`; selector keys may repeat as AND clauses,
`In`/`NotIn` require nonempty lists of unique nonempty Kubernetes label
values, and
`Gt`/`Lt` take one decimal-integer string in the list spanning zero through
the signed 64-bit maximum. Custom-label values may be empty; scope is
Job-only on `3.2.0` and Job plus Pod template from
`3.2.1`. Exact `3.2.0` requires a nonempty custom label. Typed outputs use
legacy `dsVal` only on `3.2.0`, `setValue` on
`3.2.1`, `3.2.2`, and `3.4.2`–`3.4.3`, and remain closed across the
`3.3.1`-through-`3.4.1` physical-executor transport hole. Published values
must be nonempty on `3.2.0` and `3.2.1`; `3.2.0` reserves `$VarPool$` and
keeps only the first `=` segment, `3.2.1` preserves later `=`, and `3.2.2`
plus `3.4.2`–`3.4.3` preserve `=` and accept empty values. Exact `3.4.2`–`3.4.3` also injects
compiler-owned OUT declarations into the Pod as empty-valued environment
entries. All prepared values are injected, so Kubernetes key
validity and `taskInstanceId` collision freedom are caller prerequisites.
Job watch omits namespace/current context, and `3.3.1` through `3.4.1` also
omit datasource namespace from Pod-log/output lookup. Values are not secret
storage; `3.3.1` onward INFO-logs resolved kubeconfig. Tags are mutable.
There is no durable app id or failover resume; cancel needs same-worker
in-memory state and retry can duplicate effects. Typed coordinates expose no
opaque create/edit; richer state is unchanged/export preserve-only. This
review refreshes no live evidence and promotes no profile

## JAVA

`JAVA/literal_fat_jar` is typed in `Universal` on exact `3.2.0`, `3.2.2`,
and `3.3.1` through `3.4.3`; earlier profiles are upstream-absent.
Exact `3.2.1` is an explicit runtime hole: resource staging supplies an
absolute local path, but `JavaTask` prefixes it again with `executePath` for
both `mainJar` and `resourceList`; exact `3.2.2` removes both duplicate
prefixes. The canonical payload owns one absolute shell-safe DS resource
`fullName` ending in `.jar` plus an ordered list of shell-safe `mainArgs`
tokens defaulting to empty. Projection duplicates the same `ResourceInfo`
into `mainJar` and `resourceList` because the worker downloads only the
latter, joins arguments with one space, and fixes `jvmArgs=""`,
`isModulePath=false`, and `localParams=[]`. Exact `3.2.0` and `3.2.2` use
`runType=JAR` plus `rawScript=""`; exact `3.3.1` onward uses
`runType=FAT_JAR` plus `mainClass=""`. Legacy JAVA source and modern
`NORMAL_JAR` remain explicit opaque authoring; JVM arguments, placeholders,
module path, parameters, output declarations, extra dependencies, and future
state remain outside typed authoring. Through `3.4.0`, native `jvmArgs`
follows the target and application arguments; `3.4.1` through `3.4.3` repair its
position and substitute the final command, but the shared typed subset keeps
it empty and rejects placeholders. Workers need `JAVA_HOME`, a compatible
JDK, tenant execution permission, and resource-storage access. Upstream
INFO-logs full params, the final unquoted shell command, and child output;
fields are not secret storage. Cancellation destroys only the direct process
through `3.2.2`, then kills the process tree and attempts generic application
cancellation from `3.3.1`. JAVA publishes no DS output, durable id, or
failover reattachment, and retry reruns the whole JAR with possible duplicate
side effects. Exact `3.2.1` permits selector-restricted opaque create/edit
only for a nonblank native `runType=JAVA` source payload; broken `JAR` state
is preserve-only, and canonical `mainJar`/`mainArgs` input fails closed
instead of downgrading. This review refreshes no live evidence and promotes
no profile

## DATA_FACTORY

`DATA_FACTORY/pipeline_trigger` is typed in `Cloud` from `3.2.0` through
`3.4.3` and upstream-absent earlier. Its identity wire owns only literal
`factoryName`, `resourceGroupName`, and `pipelineName`; Azure-derived `runId`,
`localParams`, `varPool`, resources, inherited state, and future fields stay
outside typed create/edit. It exposes no opaque create/edit selector and
retains excluded state only through unchanged/export preservation. Eligible
workers provide `resource.azure.client.id`, `resource.azure.client.secret`,
`resource.azure.subId`, and `resource.azure.tenant.id`; upstream logs task
parameters and identity at INFO without logging credential values. The plugin
accepts no DS placeholders, pipeline runtime parameters, resource files, or
DS output and polls using `resource.query.interval`, default `10000` ms. A
post-submit callback stores Azure `runId` in task-instance `appIds`; once
durable, failover resumes polling and cancellation of the same run. Failure
between `createRun` and callback persistence, or retry without `appIds`, can
submit a duplicate. This review
refreshes no live evidence and promotes no profile

## DATAX

`DATAX/literal_custom_json_job` is reviewed in `DataIntegration` on 36 exact
profiles: every target except `3.1.0`. Its canonical payload owns only one
preserved `json` string that must decode to a JSON object. Typed validation
rejects blank or invalid JSON, array/scalar roots, nonstandard
`NaN`/`Infinity` constants,
duplicate object keys at any depth, `${...}`/`$[...]` in source or decoded
keys/values, CR, decoded C0/C1/DEL controls, decoded unpaired Unicode
surrogates, and every additional task field without rewriting accepted
LF-formatted spelling.
On exact `3.4.3`, an empty object is also rejected. `DataxParameters` treats
blank or empty-object inline JSON as absent and requires exactly one
case-insensitive `.json` resource; `DataxTask` reads that file as UTF-8.
Typed literal authoring owns no resource-file fallback and retains nonempty
object spelling unchanged. Existing richer resource jobs remain opaque for
unchanged/export preservation.
Projection emits only `customConfig=1` plus `json` on `1.3.9`, and adds
compiler-owned `xms=1`/`xmx=1` from `2.0.0`; it never emits
`localParams` or `resourceList`. Decode may strip exact `localParams=[]` on
all profiles and UI-produced `resourceList=[]` only from `3.0.0`; earlier
empty resource state, every nonempty parameter/resource value,
datasource-generated mode, changed JVM memory, inherited state, and future
fields remain opaque. Positive typed coordinates close raw opaque
create/edit while retaining unchanged/export preservation. Exact `3.1.0`
has selector-restricted native opaque create/edit under reviewed exclusion
`null-empty-prepare-params-map-breaks-custom-command`: its custom-command
executor dereferences a null prepared map, while `3.1.1` adds the null/empty
guard. Only strict integer `customConfig=0/1` selects raw authoring;
canonical JSON-only input fails closed, and the default template is a
deliberately non-executable built-in-mode scaffold. Workers through `3.1.9` use
`DATAX_HOME/bin/datax.py`, replace CRLF with LF, and from `3.1.0` forward
prepared values as DataX `-p -D` options; workers from `3.2.0` use
`PYTHON_LAUNCHER` plus `DATAX_LAUNCHER` and the worker OS line separator.
Every epoch substitutes prepared values into JSON before writing UTF-8. On
positive typed profiles from `3.1.1`, every prepared workflow, startup,
local, or `varPool` value also enters a shell-built `-p -D` argument without
safe metacharacter quoting, so a parameter-free workflow is a prerequisite
for this facet's safe typed claim. Compilation rejects visible globals when
creating a task, changing its execution payload/settings, or changing workflow
globals; online activation rechecks existing globals. Metadata-only edits preserve
the original baseline. Empty local globals do not establish that startup or
worker-prepared parameters will be absent when the task runs.
Eligible workers still need compatible DataX plugins/drivers, connectivity,
and source/target permissions. Upstream logs complete task parameters, the
final command, and child output; job and command construction also appear at
DEBUG, so task fields are not secret storage and dsctl does not redact them.
The facet publishes no structured output or durable DataX id, cannot resume
after failover, and retry reexecutes the whole transfer. Cancellation is
worker-local: wrapper kill through `3.1.9`, direct-process destroy through
`3.2.2`, then process-tree plus generic application cancellation. This
review refreshes no live evidence and changes no profile promotion.

## CHUNJUN

`CHUNJUN/literal_local_json_job` is reviewed in `Other` on all nineteen exact
profiles from `3.1.0` through `3.4.3`. Canonical authoring owns only one
preserved literal JSON-object string; exact projection emits only strict
integer `customConfig=1`, unchanged `json`, and `deployMode=local`. It
rejects placeholders, CR, controls, surrogates, ambiguous JSON, parameters,
resources, `others`, dormant datasource-generation fields, and future state.
All nineteen upstream executors do accept `localParams` and prepared-map
substitution; the canonical facet deliberately excludes them and all DS
placeholders because replacement performs no JSON escaping, not because the
runtime lacks substitution.
Strict integer `customConfig=1` plus exact `standalone`, `yarn-session`, or
`yarn-per-job` selects restricted nonlocal opaque create/edit. Built-in
`customConfig=0`, the upstream UI typo `standlone`, richer local state, and
unrecognized state are unchanged/export preserve-only; built-in execution is
a runtime hole because `buildChunJunJsonFile` never constructs its JSON.
Exact `3.1.0`–`3.1.6` UI default `customConfig=false` is a UI defect: REST/compiler
strict integer `1` remains the runnable local wire and no cross-version wire
claim is inferred from UI defaults. The UI default becomes true in `3.1.7`.
Every epoch normalizes CRLF, substitutes prepared placeholders without JSON
escaping, and writes UTF-8. Exact `3.1.0` uses the legacy shell-file path and
nondurable post-exit log discovery; `3.1.1`–`3.1.9` keep that path but forces empty
`appIds`; `3.2.0` through `3.3.2` use the shell interceptor with empty
`appIds`; `3.4.0` through `3.4.3` use `taskRequest` with empty `appIds`.
Every exact upstream `chunjun.md` requires removing the trailing background
`&` from the `nohup` command in `${CHUNJUN_HOME}/bin/start-chunjun`; an
eligible worker must use that foreground launcher or DS status and cancel
semantics are untrustworthy.
Upstream INFO-logs complete parameters and DEBUG-logs expanded JSON, so task
fields are not secret storage. There is no structured output, durable id, or
failover resume. Cancellation is best-effort: `3.1.0` through `3.1.9` use
legacy wrapper soft/hard kill, `3.2.0` through `3.2.2` destroy/force the
direct process, and `3.3.1` through `3.4.3` kill the process tree plus attempt
generic application cancellation. Retry replays the whole job. This review
refreshes no live evidence and promotes no profile

## DATASYNC

`DATASYNC/create_and_execute` is reviewed in `Other` from exact `3.2.0`
through `3.4.3` and upstream-absent earlier. Normal typed create/edit owns
literal `name`, `sourceLocationArn`, `destinationLocationArn`, optional
`cloudWatchLogGroupArn`, and fixed `jsonFormat=false`. Discoverable
`raw-json` uses selector-restricted opaque create/edit with explicit
`jsonFormat=true` and one preserved valid JSON-object string; the worker maps
only UpperCamelCase known `DatasyncParameters` and ignores unknown fields.
Only unknown enum values in inherited `LocalParams`/`VarPool`
`Property.Direct/Type` become `null`; enum-like strings such as `FilterType`
are neither enum-validated nor null-converted and reach the SDK unchanged for
AWS validation. It is not arbitrary AWS passthrough. The
upstream UI couples outer and inner names on create/edit although REST can
separate them. `Options` are ineffective, `Includes` overwrites
`Excludes`, and `Schedule` leaves a recurring persistent Task. `localParams`
are runtime-dead. Workers use static `resource.aws.access.key.id`,
`resource.aws.secret.access.key`, and `resource.aws.region` through `3.2.2`,
then `aws.datasync.access.key.id`, `aws.datasync.access.key.secret`, and
`aws.datasync.region` from `3.3.1`, despite stale later docs.
Upstream INFO-logs original and converted parameters; fields are not secret
storage and dsctl does not redact. Every fresh attempt calls `CreateTask`
then `StartTaskExecution`, never deletes the Task or closes the client, and
callback-persists only `taskExecutionArn` in `appIds` for failover/cancel.
Pre-callback failure or retry can duplicate executions and leak Tasks;
polling has no deadline or output. This review refreshes no live evidence
and promotes no profile

## DMS

`DMS/resume_existing_full_load` is typed in `Cloud` from `3.2.0` through
`3.4.3` and upstream-absent earlier. Its identity wire keeps five required
fields explicit: `isRestartTask=true`, `isJsonFormat=false`,
`migrationType=full-load`, `startReplicationTaskType=resume-processing`, and
a literal `replicationTaskArn`. The caller must attest that the ARN is an
actual stopped, previously executed full-load task; DS checks only the ARN
and cannot verify remote type or state. Resume may reload partial or
not-yet-loaded tables, while a mismatched CDC task without a stop position
can report early DS success and continue remotely. Destructive
`reload-target`, other modes/fields, parameters, resources, and future state
are excluded from typed authoring and unchanged/export preserve-only; no raw
opaque create/edit selector exists. Workers use static `resource.aws.*`
configuration through `3.2.2` and `aws.dms.*` static or instance-profile
authentication from `3.3.1`. Upstream logs parameters/identifiers, polls
without an internal deadline, and returns no output. A callback stores the
ARN in `appIds` for failover tracking and cancel, but pre-callback failure or
retry without durable `appIds` can resubmit. This review refreshes no live
evidence and promotes no profile

## ALIYUN_SERVERLESS_SPARK

`ALIYUN_SERVERLESS_SPARK/literal_jar_submit` is typed in `Cloud` only on
exact `3.3.1` through `3.4.3`; earlier profiles are upstream-absent.
Its eight canonical fields are positive `datasource`, literal `workspaceId`,
`resourceQueueId`, `jobName`, absolute OSS `entryPoint`, nonempty literal
`entryPointArguments`, literal `sparkSubmitParameters`, and strict-boolean
`isProduction` defaulting to false. Projection joins arguments with `#` and
fixes native `type=ALIYUN_SERVERLESS_SPARK` and `codeType=JAR`; PYTHON/SQL
are explicit opaque while JAR extras stay unchanged/export preserve-only.
Every execution fetches the optional template and may prepend its Spark
configuration. Exact `3.3.1` retries status only, starts without a token,
maps `Failed` to `KILL`, and swallows logged cancel failures; `3.3.2` through
`3.4.1` retry all remote calls with one token per DS attempt and map `Failed`
to `FAILURE`; `3.4.2` also preserves start/cancel exception causes. Upstream
logs parameters/job/state at INFO. The datasource supplies static access
keys, region, and optional custom endpoint, but its connectivity check makes
no remote call. There is no durable callback or failover resume, retries may
duplicate, cancel needs the in-memory job id, and 10-second polling has no
deadline or output. This review refreshes no live evidence and promotes no
profile

## GRPC

`GRPC/literal_unary_string_record_call` is typed in `Universal` only on exact
`3.4.0`–`3.4.3`; earlier profiles are upstream-absent.
`GrpcLiteralUnaryStringRecordTaskParamsSpec` owns exactly literal `url`,
`channelCredentialType`, `serviceName`, `methodName`, flat string-record
`requestFields` and `responseFields`, exact-key string `message`, and positive
`grpcConnectTimeoutMs`. Projection generates matching package-free proto3 and
protobufjs definitions for one blocking unary call, writes the correct Java
`channelCredentialType`, and fixes default status handling. Packages,
streaming, complex fields, arbitrary proto/descriptor input, custom status,
placeholders, parameters, and resources are excluded. Typed create/edit is
enabled, raw opaque create/edit is closed, and complex native state remains
unchanged/export preserve-only. `TLS_DEFAULT` uses system trust and hostname
verification with no custom CA, mTLS, or token; the upstream UI's wrong
`grpcCredentialType` field can silently downgrade TLS. Upstream INFO-logs
complete params, publishes no output, implements no cancel, durable id, or
failover resume, can duplicate the RPC on retry/failover, and closes neither
channel nor event-loop group. This review refreshes no live evidence and
promotes no profile

## SAGEMAKER

`SAGEMAKER/start_pipeline_execution` is typed from `3.1.0` through `3.4.3`
except `3.1.1` and `3.1.2`. The literal public AWS request facet keeps its exact
credential source, request parsing, parameter forwarding, logging, callback and
retry semantics; model presence alone does not establish a working polling loop.
Exact `3.1.0` stores status in an instance field that each describe request
updates. Exact `3.1.1`–`3.1.2` instead retain a local initial status and discard
later returned values: when the first status is EXECUTING, ordinary pipeline
completion cannot end the loop. Exact `3.1.3` assigns the refreshed status.
The two holes close typed and raw create/edit and retain only unchanged/export
native preservation. Credentials, AWS permissions, SDK/network access and
runtime availability remain worker prerequisites. No local schema check proves
remote completion or prevents duplicate submissions after pre-callback failures.

## OPENMLDB

`OPENMLDB/literal_single_statement` is typed in `MachineLearning` from
`3.1.0` through `3.4.3` except the `3.1.2` runtime hole; earlier profiles
are upstream-absent, and
orphaned `3.0.x` UI files do not count as a registered plugin. Its four
required identity fields are `zk`, `zkPath`, lowercase `executeMode`, and one
literal Python-safe `sql` statement. Typed authoring rejects DS placeholders,
inherited parameters, resources, `rawScript`, runtime, and future state,
exposes no opaque create/edit selector, and preserves excluded native state
only through unchanged/export provenance. Workers select `PYTHON_HOME`
through `3.1.9` and `PYTHON_LAUNCHER` from `3.2.0`, require Python 3,
OpenMLDB, SQLAlchemy, ZooKeeper reachability, and data permissions, and use a
fixed `1800000` ms timeout for offline jobs. Upstream logs parameters, raw
SQL, rendered and final Python at INFO, discards results, has no durable id
or failover resume, and replays SQL on retry; no secrets, live evidence, or
profile promotion are claimed

Exact `3.1.2` leaves inherited `PythonTask.pythonParameters` null, so its
post-execution output handler fails even after SQL has run. Typed and raw opaque
create/edit are closed on that exact member; unchanged native state remains
opaque-preserve-only. Retrying may repeat SQL side effects. `3.1.1` does not
dereference that field after execution and `3.1.3` initializes it.

## PYTORCH

`PYTORCH/literal_resource_script` is typed in `MachineLearning` on the fifteen
exact profiles from `3.1.0` through `3.3.2`; it is upstream-absent on the
earlier profiles and all exact `3.4.x` profiles. Canonical authoring
owns only an absolute ASCII shell-safe `pythonExecutable`, one leading-slash
ASCII shell-safe `.py` `scriptResource` relative to the selected user's FILE
root, and ordered ASCII shell-safe `scriptArgs`. Exact `3.1.x` resolves a
positive resource id and emits `pythonCommand`; exact `3.2.0` through
`3.3.2` page-verify the FILE path, emit `pythonLauncher`, the verified
storage-absolute `resourceName`, and a base-relative `script`. Because exact
`3.2.x` admin resource-base queries ignore FILE versus UDF, the resolver
accepts only distinct strict sibling `resources` and `udfs` roots and
otherwise requires a non-admin tenant user. Modern reads become canonical
only after fresh exact identity revalidation proves that the observed
`resourceName` matches the selected FILE path; every unresolved or mismatched
package remains opaque. Parameters, placeholders, output, additional
resources, environment creation, inherited fields, and future state are
outside typed create/edit; raw opaque create/edit is closed, while richer
existing state is unchanged/export or metadata-only preserve-only. The
shared facet remains ASCII because `3.1.1` and later reviewed profiles use the worker platform
charset, requires an eligible Unix-like worker with Python and PyTorch
already present, publishes no output or durable app id, cannot reattach after
failover, and may repeat the whole script on retry. Exact `3.3.1` and `3.3.2`
require outer `timeout: 0`; cancellation is not reliable. This review
refreshes no live evidence and promotes no profile

## Evidence and promotion

Terminal compatibility, action support, verification evidence, and profile
promotion are separate facts. Old live receipts do not automatically attest
newly generated manifests. See
[support policy and verification](../user/version-compatibility.md#support-policy-and-verification)
for the policy labels and their relation to evidence.
