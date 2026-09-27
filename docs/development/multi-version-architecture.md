# Multi-Version Compatibility Architecture: DolphinScheduler 1.3.9-3.4.3

## Status and purpose

This document is the evidence record and target architecture for exact-version
support from Apache DolphinScheduler `1.3.9` through `3.4.3`. The architecture
and the implementation policy in this document are normative; migration state
is tracked in the [roadmap](roadmap.md#exact-version-compatibility-track-in-progress).
The [current action inventory](architecture.md#current-stable-surface) summarizes
generated profile counts; machine-readable profiles and the user-facing
compatibility reference remain the operational authorities. This design record
cannot itself promote a profile or turn static evidence into live verification.

Current runtime requirements apply to all 37 final releases in this range.
The original 15-release source corpus, controller/diff measurements, size
estimates, and recorded campaigns below remain a historical baseline; their
counts and receipts are not measurements of the expanded matrix. The
[final-release profile admission](stable-release-admission.md) records the 21
intermediate releases and the later `3.4.3` admission with their exact source
differences, without rewriting that baseline.

`3.4.1` remains the current stable runtime target until a newer exact profile
passes its release gates.

Implementation checkpoint: the compatibility compiler covers reviewed semantic
operations behind the stable CLI action catalog and materializes one terminal
action decision for each admitted exact release. Compiled exact artifacts
and profile-selected bound domains cover the matrix. They preserve the separate
`1.3.9` name/ID era, the later code-based
eras, and exact route, field,
result, and mutation rules rather than inheriting a neighboring profile.

Terminality is not a synonym for support, live verification, or release
promotion. A limited or absent coordinate is a deliberate zero-request product
decision, and a supported coordinate may still have only static or contract
evidence. `3.4.1` therefore remains the sole stable profile; the other exact
profiles remain experimental until their declared release gates pass.

The deep task-definition path uses `CompiledWireProgram`/`WireExecutor` programs
for every executable code-native profile from `2.0.0` through `3.4.3`, while
the all-version task domain also encodes the graph-backed `1.3.9` string-id/name
identity and later mutation epochs. Exact `3.4.1`/`3.4.2` remain the original tracer and
retain their nullable-versus-strict update-result distinction.
The committed `3.4.2` schema-v4 receipt remains useful evidence for the `0.4.0`
wheel and its earlier manifest, but it does not attest the newly materialized
all-version profile artifact. Preliminary behavior checks cannot substitute for
release preflight: source contents, Core Metadata, `RECORD` and generated claims
must agree with the tested wheel before it can supply promotion evidence.
Historical governed campaign
`promotion-167723bbe11d-r7-20260809` used an isolated clean-source build,
passed the public wheel-only exact-source, Core Metadata, `RECORD`, and manifest
preflight, and then validated all `180/180` invocations with one immutable
wheel. Its resulting receipts form the archived exact-read corpus. Current
profile fingerprints have changed, so those receipts remain historical.
Campaign `conformance-e8eacee57af9-r12-20260811` supplied then-current exact-read,
named-conformance, and `3.4.2` schema-6 evidence from one immutable wheel. Their
checkers passed for the recorded manifests; expanded fingerprints now make the
receipts historical. Neither result changes
support, tested status, or whole-profile promotion. `3.4.1` remains the sole
stable profile and `3.4.2` remains experimental with `tested=false`; see
[Live Testing](live-testing.md#generic-exact-profile-installed-wheel-read-gate).

The independent task-plugin extractor's original 15-release source run reported
`source_complete: true` with no diagnostics. Mechanical source facts now cover
all admitted target releases. This closes only the source-discovery prerequisite:
task semantics, CLI facets, runtime profiles, contract behavior, and live
support remain separate reviewed gates.

The runtime target is now all 37 final releases in the stated range. The corpus
inventory below retains the original 15-release research selection; the admission
record above covers the added releases. A release-line label or an anchor never
substitutes for an independently reviewed exact membership.

The questions investigated here are:

- which REST/controller eras actually exist across the target releases;
- which changes are source-type improvements, wire changes, semantic changes,
  or naming changes;
- what the official UI reveals about supported paths, defaults, and operation
  sequences;
- whether the transitional `/v2` APIs are a suitable common runtime base;
- how much cross-version reuse the evidence supports;
- where the current generated contract is authoritative and where it is only a
  candidate that requires stronger evidence;
- which compatibility surfaces, especially task-plugin authoring models, are
  outside the current REST snapshot.

The resulting decision is to keep the stable command and service vocabulary,
replace whole-version runtime duplication with deep caller-oriented domain
modules, and place a small typed generated wire seam beneath those modules.
Exact profiles select reviewed operation recipes; they do not inherit another
version's package. Generated fingerprints propose reuse, while a reviewed
compatibility decision and verification evidence authorize it.

In this document, adapting all target versions does not mean implementing 36
whole-version adapters in parallel. It means collecting mechanical evidence
and creating profile coordinates across all 36 exact versions, implementing
one cohesive domain across those profiles at a time, and promoting each exact
profile independently. This breadth/depth/promotion split is the execution
model used below.

## Evidence corpus and reproducibility

### Primary sources

The upstream-evolution portion used only:

1. the local read-only Apache DolphinScheduler checkout at
   `references/dolphinscheduler`, whose `origin` is
   `https://github.com/apache/dolphinscheduler` and which contains all selected
   release tags;
2. the exact generated snapshots under
   `build/ds_contract/snapshots-v2/`;
3. the exact source inventory
   `build/ds_contract/multi-version-inventory-v2.json` and its verified cached
   replay `build/ds_contract/multi-version-inventory-v2-cached.json`;
4. the reviewed semantic-impact artifact
   `build/ds_contract/read-compatibility-impact-v2.json`.

Every snapshot used here declares `exact: true`, a clean matching Git tag,
commit, tree, and contract digest. Both inventories report `complete: true`.
The cached replay changes only the immediate input kind/content digest to the
verified snapshot while retaining the originating Git evidence and contract
digest. The early reviewed semantic-impact artifact reports
`source_inventory_complete: true` but `complete: false`; it covers only
`project.page`, `project.get`, `workflow.page`, and `workflow.get`. It is kept as
historical research input, not the current compatibility ledger. That
distinction matters: complete source extraction is not complete product
adaptation.

The GitHub links in this document use exact release tags. The provenance table
also records the corresponding immutable commit prefix, so a tag-based source
link can be checked against the local snapshot provenance.

### Regenerating the exact corpus

Create one clean worktree for every release tag, then run the repository's
inventory entrypoint. [Codegen](codegen.md#exact-version-inventory) maintains
the canonical command and unsuffixed artifact convention. This research used a
`-v2` run namespace so it would not overwrite the earlier inventory; `v2` does
not denote a different schema. The complete research command is:

```bash
git -C references/dolphinscheduler worktree add \
  ../../build/upstream/ds-3.4.2 3.4.2

python tools/generate_ds_contract_inventory.py \
  --ds-source 1.3.9=build/upstream/ds-1.3.9 \
  --ds-source 2.0.0=build/upstream/ds-2.0.0 \
  --ds-source 2.0.9=build/upstream/ds-2.0.9 \
  --ds-source 3.0.0=build/upstream/ds-3.0.0 \
  --ds-source 3.0.6=build/upstream/ds-3.0.6 \
  --ds-source 3.1.0=build/upstream/ds-3.1.0 \
  --ds-source 3.1.9=build/upstream/ds-3.1.9 \
  --ds-source 3.2.0=build/upstream/ds-3.2.0 \
  --ds-source 3.2.1=build/upstream/ds-3.2.1 \
  --ds-source 3.2.2=build/upstream/ds-3.2.2 \
  --ds-source 3.3.1=build/upstream/ds-3.3.1 \
  --ds-source 3.3.2=build/upstream/ds-3.3.2 \
  --ds-source 3.4.0=build/upstream/ds-3.4.0 \
  --ds-source 3.4.1=build/upstream/ds-3.4.1 \
  --ds-source 3.4.2=build/upstream/ds-3.4.2 \
  --snapshot-dir build/ds_contract/snapshots-v2 \
  --output build/ds_contract/multi-version-inventory-v2.json
```

The cached replay invokes the same entrypoint with one
`--snapshot VERSION=build/ds_contract/snapshots-v2/ds-VERSION-contract.json`
argument per version and writes
`multi-version-inventory-v2-cached.json`. The reviewed impact artifact is then
reproducible without rescanning Java source:

```bash
python tools/analyze_ds_compatibility_impact.py \
  --inventory build/ds_contract/multi-version-inventory-v2-cached.json \
  --output build/ds_contract/read-compatibility-impact-v2.json
```

Useful source checks do not require switching the main checkout:

```bash
git -C references/dolphinscheduler show \
  3.4.2:dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/SchedulerController.java

git -C references/dolphinscheduler diff --name-status 3.4.1 3.4.2 -- \
  dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/v2

git -C references/dolphinscheduler grep -n 'v2/' 3.4.1 -- \
  dolphinscheduler-ui/src
```

The inventory and adjacency figures below can be reproduced from the cached
snapshots. The important comparison definitions are:

- **source operation identity**: generated `controller.method_name`;
- **route identity**: HTTP method plus resolved controller/method path;
- **extracted callable signature**: route plus the ordered parameters not
  marked `hidden` by the source extractor;
- **normalized logical candidate**: the snapshot's current
  `logical_return_type`, with Java primitive/boxed spellings normalized;
- **`/v2` operation**: the resolved path begins with `v2/`. This is more robust
  than the current `api_group` field because the 3.1 V2 controllers had not yet
  moved into the later `controller.v2` package.

The extracted callable signature is intentionally not called the effective
wire contract. Source annotation cleanup can change `hidden` or `required`
without changing an HTTP request. Query/form parameter order is also not a
semantic wire property. Those facts are preserved in the source contract but
must be normalized separately before compatibility grouping.

### Exact corpus inventory

`DTOs`, `models`, and `enums` below are generator categories, not counts of all
Java classes in the DolphinScheduler tree. In particular, the REST snapshot
does not extract task-plugin parameter classes such as `SqlParameters`.

| DS | exact commit | operations | DTOs | models | enums | `/v2` routes | raw/object `Result` | typed `Result<T>` | declared/logical conflicts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1.3.9` | [`174c78c4a90a`](https://github.com/apache/dolphinscheduler/commit/174c78c4a90a53fdfe7131e9b065edaa38b7936f) | 140 | 0 | 2 | 14 | 0 | 137 | 0 | 0 |
| `2.0.0` | [`9bd693b7efd8`](https://github.com/apache/dolphinscheduler/commit/9bd693b7efd8a20075685968ebe6a5941c22fd28) | 176 | 0 | 58 | 23 | 0 | 166 | 7 | 5 |
| `2.0.9` | [`233020545594`](https://github.com/apache/dolphinscheduler/commit/23302054559410093573fb169336bafeb02db97f) | 195 | 0 | 60 | 23 | 0 | 183 | 8 | 5 |
| `3.0.0` | [`9efd1ace7889`](https://github.com/apache/dolphinscheduler/commit/9efd1ace788998a44a4a7f25d7fd1d22a31d1e44) | 228 | 0 | 70 | 28 | 0 | 216 | 8 | 5 |
| `3.0.6` | [`109ac5486ece`](https://github.com/apache/dolphinscheduler/commit/109ac5486ece186c67725e9be1065e789f293871) | 229 | 0 | 70 | 28 | 0 | 217 | 8 | 5 |
| `3.1.0` | [`ae33ba594754`](https://github.com/apache/dolphinscheduler/commit/ae33ba594754425a2ac72100450adffd3b5e3971) | 256 | 5 | 81 | 30 | 12 | 231 | 9 | 5 |
| `3.1.9` | [`400ed9147a3f`](https://github.com/apache/dolphinscheduler/commit/400ed9147a3fd89c361e1c2bd53c94cb1e5c9b65) | 257 | 5 | 84 | 30 | 12 | 232 | 9 | 5 |
| `3.2.0` | [`f64b254809f2`](https://github.com/apache/dolphinscheduler/commit/f64b254809f21f811eaef780d489a43b490ca327) | 327 | 23 | 101 | 32 | 52 | 272 | 27 | 4 |
| `3.2.1` | [`8a4f111fd4e0`](https://github.com/apache/dolphinscheduler/commit/8a4f111fd4e0173e25d2fd56cbbfff893d4c690d) | 321 | 23 | 102 | 35 | 51 | 193 | 108 | 8 |
| `3.2.2` | [`212af27cb757`](https://github.com/apache/dolphinscheduler/commit/212af27cb757fccbe617c6ba4495ac96c757d0b1) | 324 | 23 | 107 | 34 | 51 | 189 | 114 | 8 |
| `3.3.1` | [`a05d4d474dcb`](https://github.com/apache/dolphinscheduler/commit/a05d4d474dcbacd44194f6cef7dc8405c0091b70) | 297 | 21 | 98 | 30 | 48 | 154 | 124 | 10 |
| `3.3.2` | [`59e26c883cb6`](https://github.com/apache/dolphinscheduler/commit/59e26c883cb6d0295757c6e84a168a924de3438c) | 297 | 21 | 98 | 30 | 48 | 154 | 124 | 10 |
| `3.4.0` | [`d5a666848c70`](https://github.com/apache/dolphinscheduler/commit/d5a666848c7056e77f64162a6024cfd6f72ab824) | 300 | 21 | 98 | 30 | 48 | 154 | 125 | 11 |
| `3.4.1` | [`f19eb8ce7dc4`](https://github.com/apache/dolphinscheduler/commit/f19eb8ce7dc4d7c0be9e213610d6812718294333) | 298 | 21 | 97 | 30 | 48 | 153 | 125 | 11 |
| `3.4.2` | [`71eb6412f940`](https://github.com/apache/dolphinscheduler/commit/71eb6412f940afa1f171f1097dc0e99ed61d16e2) | 243 | 0 | 85 | 30 | 0 | 61 | 175 | 16 |

`raw/object Result` counts declarations spelled `Result` or `Result<Object>`.
`typed Result<T>` excludes `Result<Object>`. `declared/logical conflicts`
counts operations where a non-`Object` declared payload disagrees with the
current inferred logical candidate after primitive/boxed normalization. A
conflict is a diagnostic to investigate, not evidence that either side is
automatically correct.

## Quantitative evolution

### Adjacent release comparison

The following table shows why one kind of fingerprint cannot answer all
compatibility questions. `Same request` is the extracted callable signature
defined above. `Same logical` is the current extractor's normalized logical
candidate and therefore inherits the inference limitations documented later.

| From | To | operation count | common source IDs | same route | same extracted request | same logical candidate | request and logical both |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `1.3.9` | `2.0.0` | 140 -> 176 | 126 | 39 | 30 | 9 | 4 |
| `2.0.0` | `2.0.9` | 176 -> 195 | 174 | 174 | 169 | 165 | 162 |
| `2.0.9` | `3.0.0` | 195 -> 228 | 195 | 195 | 187 | 188 | 180 |
| `3.0.0` | `3.0.6` | 228 -> 229 | 228 | 228 | 224 | 228 | 224 |
| `3.0.6` | `3.1.0` | 229 -> 256 | 223 | 223 | 208 | 215 | 201 |
| `3.1.0` | `3.1.9` | 256 -> 257 | 256 | 256 | 256 | 255 | 255 |
| `3.1.9` | `3.2.0` | 257 -> 327 | 253 | 244 | 6 | 242 | 6 |
| `3.2.0` | `3.2.1` | 327 -> 321 | 309 | 309 | 304 | 251 | 248 |
| `3.2.1` | `3.2.2` | 321 -> 324 | 315 | 315 | 314 | 308 | 307 |
| `3.2.2` | `3.3.1` | 324 -> 297 | 232 | 231 | 213 | 221 | 205 |
| `3.3.1` | `3.3.2` | 297 -> 297 | 297 | 297 | 297 | 297 | 297 |
| `3.3.2` | `3.4.0` | 297 -> 300 | 297 | 297 | 297 | 297 | 297 |
| `3.4.0` | `3.4.1` | 300 -> 298 | 298 | 298 | 298 | 297 | 297 |
| `3.4.1` | `3.4.2` | 298 -> 243 | 237 | 237 | 233 | 222 | 218 |

Three important cautions follow from the table:

1. The `3.1.9 -> 3.2.0` value of six identical extracted requests does not
   mean that 247 HTTP requests were redesigned. Of 253 common source IDs, 244
   retain the same method and path. Most apparent changes come from making the
   injected `loginUser` parameter hidden and making Spring parameter
   requiredness explicit. If request attributes are treated as server-injected,
   Java primitives are boxed for comparison, and Spring's implicit
   `required=true` is normalized, 221 common operations become candidate-equal
   requests. The remaining differences still require review.
2. `3.2.2 -> 3.3.1` has only 232 common source IDs but 248 common routes across
   the whole snapshots. Controller and method renaming can destroy source
   identity while preserving a route; route renaming can preserve product
   meaning while destroying both. Neither key is a semantic operation ID.
3. The current `logical_return_type` can be wrong when AST inference follows an
   implementation intermediate instead of the value serialized by the
   controller. It is useful as evidence, not yet a trustworthy effective-wire
   fingerprint.

### Reuse across longer ranges

For common source operation IDs over a whole contiguous range:

| Release range | common source IDs | identical extracted requests through whole range | identical request and current logical candidate |
| --- | ---: | ---: | ---: |
| `1.3.9` through `3.4.2` | 68 | 1 | 0 after exact spelling; 3 after limited normalization |
| `2.0.0` through `3.4.2` | 94 | 2 | 38 after limited normalization |
| `3.0.0` through `3.4.2` | 119 | 2 | 62 after limited normalization |
| `3.2.0` through `3.4.2` | 164 | 142 | 99 |
| `3.3.1` through `3.4.2` | 234 | 230 | 214 |

This is evidence for several high-cohesion wire-reuse candidates, not one
whole-server inheritance chain. It also shows that the largest reuse potential
is in the 3.3/3.4 main API, while `1.3.9` needs a genuine older identity and
request dialect.

## REST and controller eras

### 1.3.9: name/id and action-suffixed endpoints

The `1.3.9` API uses project names in nested paths, integer process IDs, and
many action-suffixed routes. Its snapshot contains 87 `GET` and 53 `POST`
operations, but no `PUT` or `DELETE` operation.

Representative source facts:

- projects use `GET projects/list-paging` and
  `GET projects/query-by-id`; see the
  [`1.3.9` ProjectController](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProjectController.java#L118-L159);
- workflow definitions use
  `projects/{projectName}/process`, with actions such as `/save`,
  `/select-by-id`, and `/list-paging`; see the
  [`1.3.9` ProcessDefinitionController](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProcessDefinitionController.java#L50-L84)
  and its
  [read operations](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProcessDefinitionController.java#L226-L296);
- workflow instances use `projects/{projectName}/instance/...`;
- schedules use `projects/{projectName}/schedule/...` and process-definition
  IDs rather than codes.

The official UI uses those same action paths. Its project store calls
`projects/list-paging` and `projects/query-by-id` in
[`actions.js`](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-ui/src/js/conf/home/store/projects/actions.js#L20-L44).

This is a real product identity boundary, not a cosmetic rename. A stable CLI
may preserve a project/workflow concept, but its older adapter cannot require
codes that the server does not expose.

### 2.0.0 through 3.1.9: code-based project-scoped main API

`2.0.0` introduces the long-lived code-based REST shape:

- `GET/POST projects`, `GET/PUT/DELETE projects/{code}` in the
  [`2.0.0` ProjectController](https://github.com/apache/dolphinscheduler/blob/2.0.0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProjectController.java#L63-L188);
- `projects/{projectCode}/process-definition/{code}` in the
  [`2.0.0` ProcessDefinitionController](https://github.com/apache/dolphinscheduler/blob/2.0.0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProcessDefinitionController.java#L87-L116);
- `projects/{projectCode}/process-instances/{id}`;
- `projects/{projectCode}/schedules`;
- a separate project-scoped `task-definition` controller.

The HTTP method distribution becomes 97 `GET`, 50 `POST`, 14 `PUT`, and 15
`DELETE` operations. The `2.0.0` UI changed its project store to the same
RESTful paths without inventing a client-only abstraction; see
[`actions.js`](https://github.com/apache/dolphinscheduler/blob/2.0.0/dolphinscheduler-ui/src/js/conf/home/store/projects/actions.js#L20-L80).

From `2.0.0` through `3.1.9`, the main project/process/task/schedule route
families are predominantly incremental. For example, `2.0.0 -> 2.0.9` keeps
all 174 common method/path pairs, `3.0.0 -> 3.0.6` keeps all 228, and
`3.1.0 -> 3.1.9` keeps all 256.

`3.1.0` adds 12 `/v2` routes, limited to access-token and project controllers.
This is the start of a parallel API experiment, not a replacement of the main
project-scoped routes.

### 3.2.x: V2 expansion and source-contract cleanup

`3.2.0` expands `/v2` to 52 operations across access tokens, projects, queues,
schedules, statistics, tasks, task instances, workflows, workflow instances,
and task relations. The V2 workflow controller is a global JSON-body API under
`/v2/workflows`; see
[`WorkflowV2Controller`](https://github.com/apache/dolphinscheduler/blob/3.2.0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/v2/WorkflowV2Controller.java#L62-L155).

The existing project-scoped process APIs remain present, and the UI continues
to call them. A literal `v2/` search in `dolphinscheduler-ui/src` returns zero
matches for `3.1.0`, `3.2.0`, `3.2.2`, `3.3.1`, `3.4.1`, and `3.4.2`.

`3.2.1` also begins a large controller-return typing cleanup: on routes shared
with `3.2.0`, 71 declarations move from raw/object `Result` to a concrete
`Result<T>`. No adjacent target pair in the corpus changes a concrete
`Result<T>` back to raw/object `Result` on the same route. That is strong
evidence of a monotonic source-quality direction, but not proof that every
inferred or declared `T` is the serialized wire payload.

The OpenAPI cleanup changes metadata as well. For example, injected
`loginUser` becomes explicitly hidden and many path/query parameters acquire an
explicit `required=true`. A source fingerprint must preserve those facts; an
effective request fingerprint must separately normalize framework behavior.

### 3.3.x: public process-to-workflow terminology transition

`3.3.1` changes the main public vocabulary and paths:

- `ProcessDefinitionController` becomes `WorkflowDefinitionController`;
- `ProcessInstanceController` becomes `WorkflowInstanceController`;
- `process-definition` becomes `workflow-definition`;
- `process-instances` becomes `workflow-instances`;
- relation and lineage controllers are renamed accordingly.

The new project-scoped route is visible in the
[`3.3.1` WorkflowDefinitionController](https://github.com/apache/dolphinscheduler/blob/3.3.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/WorkflowDefinitionController.java#L85-L114).
The official UI makes the same module and path transition from
[`process-definition` in 3.2.2](https://github.com/apache/dolphinscheduler/blob/3.2.2/dolphinscheduler-ui/src/service/modules/process-definition/index.ts#L29-L52)
to
[`workflow-definition` in 3.3.1](https://github.com/apache/dolphinscheduler/blob/3.3.1/dolphinscheduler-ui/src/service/modules/workflow-definition/index.ts#L29-L53).

This rename explains why source-operation identity is not a stable semantic
identity. The `3.2.2 -> 3.3.1` snapshots have 232 common source IDs but 248
common HTTP routes across the entire surfaces; at the same time, the principal
workflow routes intentionally change their path terminology while retaining
their product role.

After that transition, the main REST surface is exceptionally stable:

- `3.3.1` and `3.3.2` have 297 identical generated operation contracts;
- `3.3.2 -> 3.4.0` retains all 297 and adds three;
- `3.4.0 -> 3.4.1` retains 298 common routes, changes one current logical
  candidate, and removes two operations.

### 3.4.2: V2 removal, type hardening, and selective consolidation

The `3.4.1 -> 3.4.2` source-ID delta is six additions and 61 removals, but that
must not be read as six new and 61 lost product capabilities:

- all 48 `/v2` operations disappear;
- ten V2 controller source files are deleted;
- seven main `WorkflowTaskRelationController` routes disappear;
- two project-scoped logger aliases disappear;
- datasource and logger operations are renamed while retaining four existing
  routes;
- two genuinely new monitor routes are added for workflow and task executors.

At the route level, the snapshots share 241 routes; 57 routes disappear and
two are new. At the extracted request level, 233 of the 237 common source IDs
are unchanged. The complete V2 controller deletion is visible in the
[`3.4.1 -> 3.4.2` source diff](https://github.com/apache/dolphinscheduler/compare/3.4.1...3.4.2#files_bucket).

The strongest evidence that `/v2` is transitional rather than the official UI
base is that the six key UI service modules for projects, workflow definitions,
workflow instances, schedules, task definitions, and task instances have the
same Git blob in `3.4.1` and `3.4.2`. The schedule module, for example, keeps
using `/projects/{projectCode}/schedules` in
[`3.4.1`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-ui/src/service/modules/schedules/index.ts#L30-L111)
and
[`3.4.2`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/service/modules/schedules/index.ts#L30-L111).
Workflow-instance detail remains project-scoped in the identical
[`workflow-instances` module](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/service/modules/workflow-instances/index.ts#L30-L130).

This supports preferring retained project-scoped routes when they preserve the
CLI operation. It does not prove that every V2 route has a one-request main-API
replacement.

## V2 is not uniformly more correct: the schedule counterexample

The `3.4.1` V2 schedule API is attractive in isolation: it accepts JSON DTOs,
returns typed `Schedule` objects, and provides global `GET /v2/schedules/{id}`.
See
[`ScheduleV2Controller`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/v2/ScheduleV2Controller.java#L60-L154).

Its DTO semantics are not a lossless replacement for the main API:

- `ScheduleCreateRequest.environmentCode` is primitive `long`, whose omitted
  JSON value becomes zero; see
  [`ScheduleCreateRequest`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/dto/schedule/ScheduleCreateRequest.java#L36-L76);
- `ScheduleUpdateRequest.warningGroupId` and `environmentCode` are primitive
  values, and its merge code updates them only when nonzero; see
  [`ScheduleUpdateRequest`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/dto/schedule/ScheduleUpdateRequest.java#L49-L83)
  and the
  [zero guards](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/dto/schedule/ScheduleUpdateRequest.java#L98-L136);
- the main controller accepts nullable `Long environmentCode` with default
  `-1`, preserving DolphinScheduler's no-environment sentinel; see
  [`SchedulerController`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/SchedulerController.java#L94-L134).

Therefore, “V2 is newer” and “V2 is JSON” do not establish semantic dominance.
For schedules, route choice must account for omitted environment, explicit zero
values, lookup scope, and round-trip behavior. The V2 removal is a strong
direction signal; it is not sufficient evidence for a mechanical route rewrite
in every older profile.

## `Result<T>` evolution and evidence conflicts

### The envelope was generic from the beginning

`Result<T>` itself is not new in `3.4.2`. The
[`1.3.9` Result class](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/utils/Result.java#L28-L116)
already declares generic `T data` and `success(T data)`. What changes over the
release line is primarily controller use of raw `Result` versus concrete
`Result<T>` and, sometimes, what the controller actually puts in `data`.

The largest route-keyed raw-to-typed waves are:

| Adjacent release | same-route raw/object `Result` -> concrete `Result<T>` |
| --- | ---: |
| `3.2.0 -> 3.2.1` | 71 |
| `3.2.1 -> 3.2.2` | 2 |
| `3.2.2 -> 3.3.1` | 15 |
| `3.4.1 -> 3.4.2` | 78 |

No adjacent pair has a same-route concrete-to-raw regression. This makes later
typing valuable cross-version evidence.

### Confirmed refinements

Some refinements make an already observable wire payload explicit. For
example:

- `TaskDefinitionController.updateTaskWithUpstream` changes from raw `Result`
  in
  [`3.4.1`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java#L78-L99)
  to `Result<Long>` in
  [`3.4.2`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java#L76-L97);
- task detail changes from raw `Result` to `Result<TaskDefinitionVO>` on the
  same route; compare
  [`3.4.1`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java#L185-L203)
  with
  [`3.4.2`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java#L182-L200).

For the update case, the older implementation evidence refines rather than
erases the source difference. The normal `3.4.1` service path places
`taskCode` under `DATA_LIST`, but its successful no-op path can omit that value
and serialize `data: null`; see
[`TaskDefinitionServiceImpl`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java#L490-L559).

The implemented exact source correction therefore generates a strict
integer-or-null logical result for `3.4.1`, while `3.4.2` keeps its declared
strict integer. Task detail can share a reviewed response projection while
retaining exact generated models. Neither case is a reason for a `3.4.1`
profile to import a package branded `3.4.2`.

### Explicit declarations can be stale

An explicit generic is strong evidence, but not infallible. In `3.4.1`,
`queryWorkflowDefinitionByName` declares `Result<WorkflowDefinition>` and then
uses the legacy `returnDataList(Map)` path; see the
[`3.4.1` controller](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/WorkflowDefinitionController.java#L400-L419).
The service actually stores a `DagData` under `DATA_LIST`; see
[`WorkflowDefinitionServiceImpl`](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/WorkflowDefinitionServiceImpl.java#L724-L743).

In this case, implementation-flow evidence is more specific than the declared
generic.

### Deep implementation inference can also be wrong

The inverse failure exists in the current `3.4.2` snapshot:

- source declaration: `Result<List<String>>`;
- inferred return candidate: `Stream<ZonedDateTime>`;
- current snapshot `logical_return_type`: `Stream<ZonedDateTime>`.

The controller clearly assigns `schedulerService.previewSchedule(...)` to a
`List<String>` and returns `Result.success(previewDateList)`; see
[`SchedulerController.previewSchedule`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/SchedulerController.java#L288-L301).
The service interface also declares `List<String>`. The stream is an internal
implementation intermediate, not the serialized response.

The same class of current inference conflict affects `3.4.2`
`queryWorkflowDefinitionList` and
`getNodeListMapByDefinitionCodes`: their controllers explicitly return
`Result<List<DagData>>` and `Result<Map<Long, List<TaskDefinition>>>` from
service methods with the corresponding return types, while the snapshot's
current logical candidates are `Stream<WorkflowDefinition>` and `Void`. See the
[`3.4.2` controller list method](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/WorkflowDefinitionController.java#L414-L443)
and
[`batch task lookup`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/WorkflowDefinitionController.java#L535-L553).

Consequently, the 16 explicit-declaration/current-logical conflicts in the
`3.4.2` snapshot cannot be accepted as 16 upstream semantic changes. They are a
review queue containing both genuine stale declarations and extractor
overreach.

### Required semantic layers

The evidence requires at least these distinct concepts:

| Layer | What it records | Example |
| --- | --- | --- |
| Source declaration | Exact controller annotation, route, Java signature, DTO/model declaration, provenance | `3.4.1` task update declares raw `Result` |
| Inferred candidate | A typed hypothesis with its evidence path and confidence, never silently replacing source fact | service map stores a `DagData`; implementation contains a `Stream` intermediate |
| Effective wire contract | Reviewed request encoding and response value actually serialized for one source operation or shared wire program | task update returns an integer or `null` on `3.4.1`, and a strict integer on `3.4.2` |
| CLI-consumed projection | Known fields and semantics the stable CLI interprets or publishes | task code as integer; schedule environment's no-environment meaning |
| Preservation contract | Opaque native state, owned paths, unknown-field acceptance, and merge/encode policy required for safe writeback | preserve unowned task parameters while changing inline SQL |

An exact source fingerprint should change when upstream source changes. An
effective-wire fingerprint should normalize injected parameters, framework
defaults, irrelevant ordering, and primitive spelling, but retain method,
path, binding, body/form encoding, envelope, and validated response shape. A
CLI-consumed fingerprint should be narrower still. A separate preservation
fingerprint covers safe mutation of fields the CLI does not interpret.

Any conflict between strong evidence sources should block automatic wire-family
grouping and produce a review diagnostic. “Newest declaration wins” and
“deepest inferred type wins” are both contradicted by the upstream examples.

## UI evidence: valuable for recipes, not a wire oracle

The official UI is especially useful for identifying the product path users
actually exercise:

- the `1.3.9` UI uses the name/id action routes;
- the `2.0.0` UI follows the code-based REST transition;
- modern UI service modules keep project scope in workflow, instance, task,
  and schedule requests;
- the UI does not call `/v2` even while those controllers exist;
- the `3.4.1` and `3.4.2` key service modules are identical despite V2
  deletion;
- task forms expose conditional defaults and requiredness that are not obvious
  from a Controller alone.

The UI cannot prove that an unused REST endpoint is unsupported, and its
TypeScript service functions often return `any`. It is therefore behavioral
evidence for route preference, selector scope, defaults, lifecycle sequence,
and conditional fields. Controller mappings, response assembly, DTOs,
task-plugin validation, serialization, and live responses remain necessary for
wire claims.

## Task-plugin contracts evolve outside the REST snapshots

Controller compatibility is not complete workflow-authoring compatibility.
None of the 15 REST snapshots contains `SqlParameters`, even though SQL task
payloads are embedded inside workflow/task JSON accepted by the REST API.

The SQL task model demonstrates independent evolution:

- `1.3.9` keeps `SqlParameters` in `dolphinscheduler-common` and requires a
  datasource, type, and nonempty inline `sql`; see
  [`SqlParameters`](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/common/task/sql/SqlParameters.java#L32-L110)
  and
  [`checkParameters`](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/common/task/sql/SqlParameters.java#L232-L238);
- `2.0.0` moves the implementation into the SQL task plugin while retaining the
  essential inline-SQL contract;
- `3.x` moves the shared parameters into `dolphinscheduler-task-api`, while
  fields and resource behavior continue to evolve;
- `3.4.2` adds `SqlSourceType`, `sqlSource`, and `sqlResource`, and validation
  accepts either inline SQL or a resource file; see
  [`SqlSourceType`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-task-plugin/dolphinscheduler-task-api/src/main/java/org/apache/dolphinscheduler/plugin/task/api/enums/SqlSourceType.java#L18-L26)
  and
  [`SqlParameters`](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-task-plugin/dolphinscheduler-task-api/src/main/java/org/apache/dolphinscheduler/plugin/task/api/parameters/SqlParameters.java#L51-L116).

The `3.4.2` UI supplies the product semantics missing from the bare field list:

- `SCRIPT` is the default;
- absent `sqlSource` is treated as script mode for backward-compatible data;
- inline `sql` is required in script mode;
- `sqlResource` is required only in file mode;
- resource selection uses the DS resource `fullName` path.

See the
[`3.4.2` SQL field model](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/views/projects/task/components/node/fields/use-sql.ts#L25-L110)
and
[`SCRIPT` defaults](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/views/projects/task/components/node/tasks/use-sql.ts#L35-L55).

This is a new capability facet, not a generic cleanup that can be backported to
`3.4.1`. Conversely, ordinary inline SQL remains a shared semantic operation
and should not require duplicate product logic simply because `3.4.2` adds a
second input mode.

The same research method must eventually cover every task type promised by
workflow authoring: parameter classes, plugin validation, enums, serialized
field names, defaults, resource references, and UI conditional behavior.
Extracting all REST controllers cannot substitute for that second contract
surface.

## Architecture decision

### Baseline implementation pressure

At baseline commit `5a15c91aa958`, the repository contained several good seams
but the multi-version implementation was between two architectures. The
following historical counts explain the selected design; they are diagnostic,
not current-state measurements or targets:

| Area | Baseline size or shape | Architectural signal |
| --- | ---: | --- |
| `tools/ds_codegen` | 42 files, 16,498 lines | substantial compiler logic worth deepening rather than reimplementing manually |
| `src/dsctl/generated` | 320 files, 15,617 lines | one full `3.4.1` SDK plus two narrow exact-version slices |
| full `3.4.1` bundle | 229 files, 12,806 lines, 298 operations | the product adapter calls about 135 generated methods; a full controller surface is not the product closure |
| `src/dsctl/upstream` | 21 files, 9,738 lines | version adaptation is already a major subsystem |
| `DS341Adapter` module | 3,493 lines, 32 top-level classes | useful behavior with low locality because unrelated domains share one version file |
| upstream Protocols | 3,738 lines, 113 Protocol classes | many records mirror generated DS DTO fields instead of defining a narrow canonical boundary |
| tests | 161 files, 77,455 lines | test shape, more than generated bytes, is the dominant scaling warning |
| `tests/fakes.py` | 6,504 lines | fake records, pages, operation groups, and one broad fake session repeat the production wire shape |

At that baseline, the broad
[`UpstreamSession`](../../src/dsctl/upstream/protocols/session.py) exposed 27
operation-group properties. That shape was repeated in the `3.4.1`
session, the aggregate fake, the fake-runtime factory, registry accessors, and
several partial-session variants. A new horizontal slice can therefore require
changes through this chain:

```text
record Protocol -> operation Protocol -> version adapter -> broad session
  -> registry/runtime variant -> fake record/adapter/session -> service tests
```

Some baseline service code also knew native paths and form fields. Task update
constructed `taskDefinitionJsonObj` and `upstreamCodes` in
[`services/task.py`](../../src/dsctl/services/task.py); workflow and instance
editing had similar native request planning in their services. This made a
future route difference either a service branch or a duplicated service path,
even though the accepted boundary said native fragments belonged below the
version seam.

The compatibility analyzer has the inverse form of the same issue: its initial
semantic bindings manually enumerate transitive type closures. Doing that for
the complete stable surface would replace generated SDK duplication with a
large handwritten compatibility table. Semantic roots and reviewed decisions
should be handwritten; mechanical reachability should not.

The deletion test separates necessary depth from accidental structure:

- deleting the generator would redistribute exact routes, types, provenance,
  diffs, and corrections into handwritten code, so the generator stays;
- deleting typed generated request/response execution would redistribute wire
  validation into adapters, so the generated wire runtime stays;
- deleting schedule sentinels, pagination, selector resolution, lossless
  workflow editing, or error adaptation would push complexity into services,
  so those behaviors stay below a deeper interface;
- deleting the **whole-version organization** of those behaviors does not push
  complexity upward: cohesive domain modules can own it with better locality;
- deleting the broad session and DTO-mirroring Protocol tree after callers use
  narrow canonical modules removes most aggregate fakes and partial-session
  variants rather than recreating them elsewhere;
- deleting full per-version SDKs from the published runtime after compiler
  audit artifacts and reachable typed closures exist removes unused surface
  without sacrificing evidence.

The result is an evolution of the architecture's strongest parts, not a
rewrite. The compiler, transport, stable services, capability policy, and
generated response/error hooks survive; whole-version, whole-session, and
record-mirroring organization do not.

### Design goals and deliberate non-goals

The architecture must make a maintainer's normal change proportional to the
stable CLI behavior that changed, not to the total number of DS releases or
controllers. It must also make every support claim auditable. Specifically:

- one stable CLI action must have one product meaning across supported
  versions;
- exact upstream source facts must remain recoverable even when runtime code is
  shared;
- a version must fail closed when an action or task facet is not reviewed;
- mutations must preserve DS fields that the CLI does not interpret;
- adding a version should normally add profile mappings and evidence, not a
  cloned SDK, adapter, protocol tree, and fake hierarchy;
- route, DTO, UI, task-plugin, and live evidence must be distinguishable;
- generated code must continue to remove mechanical wire work without becoming
  the public or domain architecture.

The design intentionally does **not** try to expose every upstream endpoint,
make all versions look structurally identical, or create a generic DSL for
arbitrary DolphinScheduler calls. Full version support means that every stable
CLI action and relevant input facet is explicitly classified and verified; it
does not mean that all 140-327 upstream controller operations become public CLI
features.

The migration also does not justify changing already sound modules. The stable
commands, output contract, transport, error vocabulary, capability preflight,
and the request/error contract remain. The obsolete `GeneratedSessionAdapter`
bridge has been retired in favor of direct prepared-request execution. The
work is concentrated at the generated/upstream boundary and at service code
that still constructs version-native requests.

### Alternatives considered

#### Full generated SDK and adapter per version

This is precise and made the first release fast, but it scales by
`versions x endpoints`. The baseline full `3.4.1` runtime contained 298
generated operations while the product adapter consumed only about 135.
Repeating that full bundle for the original 15 target releases would mechanically
approach 192,000 generated lines before adapter and test duplication.

The larger cost is structural: every new version encourages another broad
session, whole-version adapter, record Protocol mirror, and fake tree. The
design remains useful as migration scaffolding and as an untracked audit
artifact, but is rejected as the long-term shipped runtime shape.

#### Newest version as a base with version patches

Using `3.4.2` models directly for `3.4.1` and older releases looks compact and
does correctly suggest some improvements, such as concrete `Result<Long>`
typing for an unchanged task-update payload. It is nevertheless unsound as a
general rule:

- `1.3.9` has a genuinely different identity model;
- `3.4.2` SQL file mode is a real new capability, not a type refinement;
- V2 disappears while main routes survive, so the newest controller tree is
  not a superset;
- newer explicit generics can be more accurate, but explicit declarations and
  deep inference each have confirmed counterexamples.

Later source is therefore cross-version evidence, not an inheritance parent.
Exact profiles may select the same reviewed recipe and wire programs but never
import another version package merely because that version is newer.

#### One generic declarative operation bus

A universal `execute("task.get", payload)` layer would reduce interface count,
but would move selector resolution, multi-request mutations, lossless editing,
and errors into either callers or an increasingly elaborate DSL. It would also
make tests stringly typed and expose wire-level vocabulary above the seam.

Declarative programs are valuable internally for one typed HTTP exchange. They
are rejected as the service-facing interface.

#### Fully handwritten REST integration

Removing generation would move exact routes, parameter bindings, envelopes,
DTOs, enums, provenance, source diffs, and stale corrections back into manual
code. The complexity would be redistributed rather than removed. This option
fails the deletion test and is rejected.

### Selected design

Use a hybrid of deep typed domain modules and generated typed wire programs.
The original design diagram below retains its historical `WireProgram` name;
the production implementation now uses `CompiledWireProgram`:

```text
command
  -> service use case
    -> caller-oriented domain module
       selector resolution / adaptation / mutation recipe / canonical result
      -> typed WireProgram selected by the exact VersionProfile
        -> WireExecutor -> shared HTTP transport -> DolphinScheduler REST

exact REST and task-plugin source
  -> compatibility compiler
    -> source evidence / wire candidates / typed programs / impact diagnostics

controller/service flow + official UI + live evidence
  -> reviewed decision ledger <- compiler candidates
    -> materialized exact VersionProfile
```

The diagram is both the selected steady-state architecture and the organizing
model for the current implementation. The compatibility compiler and reviewed
decision ledger materialize all 36 exact profiles and every terminal
action/version coordinate. Profile-selected bound domains now cover the stable
surface and keep selectors, adaptation, request budgets, result projection, and stable
errors below services. `DefinitionReads` remains the first useful historical
example of that boundary, including the distinct `1.3.9` identity recipe; it is
no longer the extent of the matrix implementation.

Executable ownership is now consolidated into 21 compiled plans. Every domain
selects generated request and response contracts through its bound-domain
recipes; the 36 exact packages retain metadata and enum catalogs, not native
operation wrappers. The legacy capture bridge and unused package reflection
loader are retired. Native authoring graph encoding and request projection now
live below the service seam as well; complete live evidence remains separate
from this runtime consolidation and from terminal compatibility decisions.

There are two intentional abstraction levels below services:

1. A **domain module** is deep and product-facing. It accepts canonical
   selectors and intents, may execute a multi-request recipe, and returns
   immutable canonical values or structured failure facts.
2. A **wire program** is narrow and internal. It represents one generated,
   typed HTTP exchange: argument validation, request encoding, envelope
   decoding, and native payload validation.

The first level keeps use cases readable. The second removes mechanical REST
duplication. Neither level exposes a versioned public command tree.

### Compatibility vocabulary

The design uses these terms at distinct levels:

| Term | Stable meaning |
| --- | --- |
| Source operation | One exact upstream controller method with provenance; useful evidence, not product identity |
| Semantic operation | One version-stable `dsctl` intent such as `task.get` or `schedule.update` |
| Recipe | Typed domain code that realizes one semantic operation for one or more exact profiles, possibly with several requests |
| Wire program | One typed HTTP exchange selected by a recipe |
| Wire family | An explicitly reviewed semantic/evidence identity whose exact members share one effective request/response contract |
| Wire kernel | One finite static implementation of a mechanical transport/projection shape; several families may select it |
| Version profile | The fully materialized mapping from one exact DS version to recipes, wire-family memberships, capabilities, and evidence |

There is deliberately no generic “operation family” or “recipe family” term.
Profiles may select the same recipe or reviewed wire family without inheritance.
Kernel sharing is compiler-derived and has no authority to merge families or
their evidence.

## Target runtime architecture

### Stable semantic operations are the product identity

The stable semantic key is a namespaced action or internal operation such as
`project.page`, `workflow.get`, `task.update`, or `schedule.online`. It is not a
Java method name, a URL, or a generated class. A public CLI action may depend on
several semantic operations; one semantic operation may serve several CLI
actions.

Every semantic operation declares:

- its canonical input selectors and intent;
- its canonical output projection;
- preservation requirements for data it reads and writes back;
- failure facts the service must translate;
- action and task-facet capability dependencies;
- one or more admissible implementation recipes;
- verification obligations according to risk.

This is the unit that should remain stable when DS renames process to workflow,
refines `Result<T>`, or moves an equivalent operation between controllers.

### Caller-oriented deep domain modules

Modules are organized by cohesive user intent, not by server version or
controller class. Expected modules include projects, workflow definitions and
tasks, schedules and execution, workflow/task instances and logs, resources,
governance, and plugin-backed authoring.

A representative task interface is:

```python
@dataclass(frozen=True)
class TaskSelector:
    project: ProjectRef
    workflow: WorkflowRef
    task: TaskRef

@dataclass(frozen=True)
class TaskUpdateIntent:
    selector: TaskSelector
    patch: TaskPatch

class TaskDefinitions(Protocol):
    def get(self, selector: TaskSelector) -> TaskView: ...
    def prepare_update(self, intent: TaskUpdateIntent) -> PreparedTaskUpdate: ...
    def apply(self, prepared: PreparedTaskUpdate) -> MutationOutcome[TaskView]: ...
```

The exact class and method names may change during implementation; the
interface invariants may not:

- project/workflow/task resolution remains below the interface;
- no URL, form field, `Result`, V2 label, generated entity, or DS JSON fragment
  crosses upward;
- `prepare_update` performs no mutation and produces the same sanitized request
  plan used for dry-run;
- a prepared mutation is bound to an exact profile and source/recipe
  fingerprint and cannot be applied under another profile;
- apply fresh-reads the native state and rejects the prepared plan before its
  mutation boundary when the task version, dependencies, or requested
  projection changed;
- `apply` performs the promised mutation cardinality and owns required
  readback;
- if mutation succeeds but readback fails, the result preserves the fact
  `mutation_applied=true` rather than misreporting the operation as untouched;
- if transport fails after dispatch, the result preserves the weaker fact
  `mutation_may_have_applied=true` and requires reconciliation without an
  automatic resend;
- service code owns final stable error wording and suggestions, while the
  module returns structured domain failure facts;
- unknown fields required for a safe round trip are preserved losslessly.

This boundary is deeper than the current DS-shaped operation Protocols. A
single `Page[T]` and small canonical values replace record-by-record mirrors of
the generated DTO tree. Tests fake or record the narrow domain module or wire
executor they need; they do not reconstruct a 27-property upstream session.

`DefinitionReads` is the first matrix-wide implementation of that rule. Its
public surface owns only project/workflow list and get, paging, selector
resolution, canonical identity, error translation, and attached-schedule
hydration. A minimal wire protocol hides exact compiled programs. The
code-based recipe uses direct numeric detail operations and paged name
resolution. The `1.3.9` recipe deliberately pays extra requests for safety:
its project detail endpoint lacks the modern permission guard, and its workflow
detail lookup is globally ID-addressed despite the project route. The module
therefore proves visibility and scope through authenticated pages and rejects
any detail identity mismatch before continuing. This is a product-semantic
adaptation, not pagination boilerplate.

Canonical `ProjectRef` and `WorkflowRef` values are tagged with their native
identity kind. Code-based profiles emit `code`/`projectCode`; `1.3.9` emits
`id`/`projectId`. Conversion never synthesizes a code from an older integer ID,
so stable output can remain truthful across both eras.

Alert groups and alert plugins apply the same boundary through separate bound
domains. Each domain selects its exact compiled programs, projects
version-specific entities and VOs into stable snapshots, owns bounded selector
resolution and mutation readback, and disables retries after mutation dispatch.
The split is intentional: DS `1.3.9` has legacy alert groups but no
plugin-instance or UI-plugin controller. Its group mutations select native
`EMAIL`/`SMS` group type, while later profiles select plugin-instance ids.
Keeping the domains independent allows those truthful group operations without
manufacturing an alert-plugin surface. DS `2.0.0` local search normalization
and DS `3.2.1`/`3.2.2` transient delivery-field defaults/preservation also stay
below the service interface.
Terminal absence or semantic limitation remains a capability decision and a
zero-request adapter fallback, never a guessed wire request.

Workflow create/edit and similar lifecycle actions follow the same rule. The
profile-selected workflow domain owns exact routes, code allocation, field
presence, execution vocabulary, and result projection. Authoring services own
stable YAML validation, typed/opaque decisions, prepared graph state and identity
allocation orchestration. The pure upstream graph compiler owns native JSON
encoding from the validated model and bound identity facts. Request projections
and preservation overlays stay beside that compiler; callers do not unpack
native graph fields. Task-scoped execution and instance-edit dry runs reuse
their exact prepared requests, including old route and tenant epochs. Version-
specific request coordination must not move back above the domain seam.

### Typed wire programs and executor

The original internal generic seam had the following conceptual form. This
historical sketch records ownership, not the current production classes or
executor signature:

```python
class WireProgram(Generic[ArgsT, PayloadT]):
    family: WireFamilyId
    wire_fingerprint: str
    response_fingerprint: str
    encode: Callable[[ArgsT], PreparedRequest]
    decode: Callable[[HttpResponse], PayloadT]

class WireExecutor(Protocol):
    def execute(
        self,
        program: WireProgram[ArgsT, PayloadT],
        args: ArgsT,
    ) -> PayloadT: ...
```

This sketch expresses ownership, not a requirement to store Python callables in
generated data. A generated program owns exactly one request binding and native
response validator. Foundation continues to own HTTP, authentication, retry,
and transport lifetime. The executor binds a typed program to that transport
and owns invocation, envelope/error hooks, trace collection, and shared
response safety. `GeneratedSessionAdapter` was the starting point for this
executor, not a requirement to retain generated invocation capture.

The initial implementation wrapped the generated `3.4.1` and `3.4.2`
project-scoped task detail/update operations. Preparing a call captured its
authentication-free request without I/O; executing it revalidated exact
profile, source digest, program fingerprint, and generated encoding before
sending exactly one request through the shared client. This narrow executable
seam established the legacy generated-invocation bridge.

All production wire programs now use `CompiledWireProgram` with direct request
encoding and response decoding. Preparing validates the generated request schema
once and stores a detached request. Execution checks its content digest, exact program binding,
and codec identity, sends that request through the shared transport, and decodes
only the received response. Public previews are copies. The legacy
capture/recapture implementation and its argument-bearing prepared calls have
been removed; fake-owned arguments live only in test `FakePreparedWireCall`
objects. Source reproduction and historical AST analysis remain tooling. The
executor retains once-only mutation dispatch and truthful completed-response
failure evidence without decoding synthetic response samples.

Domain modules receive already selected typed programs. Services cannot look up
programs by string, bypass the profile, or send an unreviewed controller
operation. Dry-run request plans are built at this level or below, so a preview
cannot drift from the real method, path, query, form, or body.

### Recipes handle semantic adaptation

A recipe implements one semantic operation for one or more exact profiles. It
may be:

- a single wire program plus a canonical projection;
- lookup followed by a project-scoped request;
- read/merge/update/readback with an explicit mutation boundary;
- an older name/id dialect that produces the same stable product meaning;
- a deliberately constrained implementation with a named unsupported facet.

Recipes are ordinary typed code, not a new workflow DSL. This keeps complex
branching, preservation, error handling, and request cardinality visible to
normal Python review and tests.

Compiled exact profiles explicitly materialize a finite `recipe_id` covered by
their profile digest. Reviewed codec combinations are guarded in the compiler;
the runtime selects the named Python recipe instead of reconstructing semantic
policy from mechanical codec names. Independent exact-version tests retain the
expected recipe decisions.

Reuse is authorized only when all consumed inputs and outputs, omission and
default rules, permission scope, failure semantics, idempotency/retry policy,
request cardinality, atomicity expectations, and preservation obligations
match. Equal routes or fingerprints are candidates, not sufficient proof.
Conversely, route or Java method renames do not require duplicate recipes when
the reviewed product semantics remain the same.

### Exact profiles are materialized compositions

An exact `VersionProfile` binds the server version to:

- source provenance and exact manifest;
- every stable semantic operation's recipe or explicit absence;
- every action and task facet's capability state;
- the runtime wire-program closure;
- verification receipts and their freshness;
- any exact-version constraints surfaced by schema or diagnostics.

Profiles must be fully materializable and diffable. Implementation helpers may
construct repeated mappings, but a released profile cannot rely on opaque
profile inheritance where inspecting `3.4.1` requires mentally applying a
`3.3` base plus several patches. The generated materialization is the review
and release artifact.

This prevents two opposite errors: cloning a whole adapter for an almost
identical patch release, and silently treating a newer version's new feature as
available in every older profile.

### Compatibility decision ledger

Automatic grouping stops at a candidate. A source-controlled decision ledger
records why an exact profile selects a recipe and why each source operation may
use a shared wire program. A decision contains:

- semantic operation and exact version members;
- selected source operation or operations;
- expected source, effective-wire, consumed-projection, and preservation
  fingerprints;
- controller/DTO/service evidence;
- UI evidence when it informs route preference, defaults, or sequence;
- task-plugin evidence when authoring payloads are involved;
- preservation, atomicity, error, and known limitation notes;
- required contract/live verification;
- source-shape guards for any correction or exception.

The ledger is handwritten because semantic equivalence is a product decision.
Its repetitive source facts and closures are compiler-generated. A changed
fingerprint makes the decision stale and blocks runtime regeneration until it
is reviewed; it never silently changes a recipe selection or wire-family
membership.

## Compatibility compiler and generated artifacts

### The generator's proper role

The generator becomes a **source-evidence compiler and typed-wire generator**,
not the runtime's domain model or the sole authority for effective behavior. It
owns:

- exact tag/commit/tree provenance and exact source manifests;
- controller routes, bindings, declarations, DTO/model/enum facts, and their
  source locations;
- separate declared and inferred response evidence with provenance and
  confidence;
- normalized effective-wire candidates without erasing exact source facts;
- automatic operation-to-request/response/nested-type closure;
- typed request encoders, native response validators, and wire programs;
- task-plugin contract inventory described below;
- wire-family candidate reports, conflict diagnostics, freshness, and compatibility
  impact.

It does not own stable CLI semantics, the consumed projection or preservation
contract, final recipe selection or wire-family membership, multi-request
recipes, support promotion, user-facing errors, or live compatibility claims.

The research-era return-type resolution established a constraint that remains
part of compiler review: direct concrete declarations and inferred
implementation candidates must remain separate. Raw or `Object` declarations
may use high-confidence inference as a candidate. A conflict between strong
sources becomes a blocking
diagnostic. The default is the concrete declaration, but a reviewed,
source-guarded correction may select a more specific implementation-flow result
when the upstream declaration is demonstrably stale. Neither “newest wins” nor
“deepest inference wins” is valid.

### Four fingerprints, four questions

The compiler maintains distinct fingerprints:

1. **Source fingerprint** changes when exact upstream declarations or relevant
   implementation evidence changes. It answers “did the source evidence move?”
2. **Effective-wire fingerprint** normalizes injected request attributes,
   framework defaults, irrelevant parameter order, and primitive spelling while
   retaining method, path, binding location, encoding, envelope, and validated
   response shape. It answers “can the same typed HTTP exchange be emitted?”
3. **Consumed-projection fingerprint** covers the fields and semantics the CLI
   interprets or publishes. The semantic operation and recipe decision define
   that contract; the compiler only computes its digest, source reachability,
   and change impact. It answers “can this recipe produce the same stable
   result?”
4. **Preservation fingerprint** covers owned paths, opaque-extension policy,
   validator handling of unknown fields, and lossless merge/encode strategy.
   It fingerprints a reviewed recipe-level contract, not an enumerable list of
   all future native fields. It answers “can this mutation write back without
   destroying state the CLI does not understand?”

The fingerprints deliberately do not collapse into one. `Result` becoming
`Result<Long>` should change source evidence but may preserve the effective
wire, consumed projection, and preservation policy. Adding SQL file mode
changes the source and capability facet even while inline SQL remains a shared
recipe.

### REST and task-plugin contracts are separate inventories

REST extraction alone cannot describe workflow authoring. The compiler must
inventory each promised task plugin independently:

- parameter classes and inheritance;
- serialized field names and types;
- enums and defaults;
- resource references;
- declared validation and source locations;
- UI defaults, conditional visibility, and conditional requiredness as
  reviewed behavioral evidence.

Arbitrary Java `checkParameters()` logic should not be translated into a
pretend-complete generated validator. The compiler can extract structural
facts and recognizable guards; nontrivial semantics belong in reviewed,
source-guarded rules with UI and live evidence. Task authoring schema is chosen
by the exact profile and task facet. It must not remain a single global
`3.4.1`-shaped model.

For SQL, inline script mode is a shared semantic facet across the release line.
The `3.4.2` resource-file mode is a separate capability facet with its own
schema and recipe. This distinction is the pattern for other independently
evolving task plugins.

The first compiler slice now implements this boundary in
`tools/ds_codegen/task_plugins.py`. It follows both historical binding shapes:
legacy `TaskParametersUtils` switch cases and plugin/SPI factory-to-channel
deserialization chains. This matters in `2.x`, where the worker plugin's
parameter class is authoritative even though a same-named common class remains
in the tree. The output is a separate immutable task-plugin snapshot with the
complete inherited and nested parameter-model closure, enum closure, binding
evidence, source paths, and AST fingerprints for validation and resource
methods. Discovery considers Maven production sources only and excludes
factories explicitly annotated `VisibleForTesting`; unresolved production
bindings and incomplete parameter-model or enum closures fail closed as source
diagnostics.

The generated diff has two independent gates. `source_complete` means every
selected server binding was resolved without a source diagnostic;
`review_complete` means every semantic change ID is covered by a reviewed
decision with explicit CLI impact, facets, status, and affected actions. That
explicit list was the first tracer format. Current profile compilation validates
reviewed decisions against the derived dependency closure rather than treating
the handwritten affected-action list as the sole impact source. Each review is
bound to both normalized snapshot fingerprints and both exact Git source-tree
identities, so changes to reviewed execution or UI evidence also invalidate it.
Every decision declares its required evidence planes, and source-backed
analysis verifies their repository-relative paths. The checked-in SQL
`3.4.1 -> 3.4.2` review records `SQL/inline_script` as
equivalent and `SQL/resource_file` as added. That review now feeds the first
materialized authoring catalog, but it remains evidence rather than a support
declaration by itself.

Every completeness marker records its extractor/schema revision, normalized
scan scope and denominator, discovered and resolved counts, and unresolved
diagnostics. `complete: true` without those facts is not release evidence.
Decision ledgers, compiler schemas, and release/profile manifests are tracked
and content-addressed; heavy exact source snapshots may remain reproducible
build artifacts.

This split also makes implementation noise visible without promoting it:
comments, getters, Lombok, and source moves remain exact source evidence, while
only normalized authoring-contract changes enter the semantic review queue.
The current compiler still does not extract conditional requiredness, UI
defaults, server execution precedence, or the declared action/facet dependency
graph. Semantic mappings remain source-guarded review decisions; once a change
is attached to a reviewed semantic root or facet, the target compiler derives
affected actions from that graph instead of asking a reviewer to enumerate them
twice. Generic extraction of all UI registries, defaults, and serializer paths
remains follow-up work.

### Stable authoring contract and profile projection

Workflow YAML is a `dsctl` product contract, not a transcription of one
DolphinScheduler release. Exact server source, task-plugin models, official UI
behavior, REST payloads, and live request/readback observations are evidence
for that contract; none of them alone is the YAML authority.

The accepted field names, omission rules, meanings, and round-trip guarantees
remain stable unless `dsctl` deliberately revises its authoring contract.
Template prose, comments, examples, key order, and formatting may improve
without constituting a new dialect. An internal authoring-contract revision
and a corpus from released tags or wheels make those guarantees testable. A
required file-level `apiVersion` is introduced only when a second incompatible
dialect actually exists, not in anticipation of one.

The current `3.4.1` implementation is the historical behavior oracle and
default parity target during migration, but it is not infallible. If its
behavior conflicts with the documented CLI contract, exact upstream evidence,
or live behavior, the difference is classified and corrected rather than
blindly projected. Such a correction is a reviewed CLI decision with regression
coverage, not an implicit adoption of the newest server's behavior.

One profile-selected task-authoring catalog drives all task-payload views of
the same contract:

- task templates and workflow/task authoring schema;
- lint and compile validation;
- export and unknown-field preservation;
- dry-run and applied mutation planning;
- action/facet capabilities and compatibility diagnostics.

Mechanical task-plugin source facts now cover all 36 exact releases. That broad
inventory is evidence input, not an automatic typed-authoring support claim.
The reviewed executable catalog remains intentionally narrower: `3.4.1` and
`3.4.2` select the same reviewed inline-SQL facet for typed create/edit and
opaque preservation. The `3.4.2` resource-file facet is known but permits only
opaque preservation; its public typed create/edit paths remain unavailable.
Stable `3.4.1` also enumerates its preexisting strict typed task models and
generic passthrough plugin types as a closed compatibility inventory. Those
memberships preserve an already-published CLI surface; they are not reviewed
facet evidence and cannot be inherited by another exact profile. Catalog
lookup fails closed for a missing task type, facet, compatibility membership,
or requested intent rather than borrowing behavior from `3.4.1`.

It supplies selected-version task restrictions and payload facts to the stable
`command_contract` interface. That existing interface remains authoritative
for CLI arguments, parser/default semantics, help, action routing, and the
command portion of `dsctl schema`; task authoring does not create a second
command grammar.

This makes the authoring module deep: callers see one coherent interface while
version-specific field sets, defaults, encoders, and preservation rules remain
below it. A task type can independently offer typed create, typed edit, and
opaque lossless preservation. A raw or unknown task type being preserved does
not imply that `dsctl` can safely author or edit it. Facets represent meaningful
user capabilities such as SQL inline script or SQL resource file, not one facet
for every Java field.

Profile projection happens at build time from reviewed evidence into a complete
exact profile. Reviewed facet membership authorizes reuse; content digests only
guard identity and freshness, while the compiler deduplicates mechanical
generated shapes. `3.4.1` never imports or inherits `3.4.2`. Inline SQL may be a
reviewed shared facet while resource-file SQL remains a separate, additive
facet. Once a projected domain becomes authoritative, its old schema, template,
lint, compiler, and
preservation path are removed in the same migration series so two authoring
truths cannot survive indefinitely.

### Build artifacts versus published runtime

Complete exact-version source manifests and full generated clients are useful
for audit, diffing, and compiler tests. They may remain reproducible `build/`
artifacts without all being committed or published.

Consolidation shares generated request and response closures by executable
content, while a separate data-only catalog retains explicitly reviewed semantic
family identities and exact memberships when present. The installed package
continues to carry the transitive wire-program closure needed by stable actions for
selectable profiles. Exact provenance remains attached to each profile, so
mechanical deduplication never pretends the source trees or evidence were
identical.

The current runtime retains 36 exact-version metadata and enum packages with
zero native operations; the former full stable `3.4.1` package is no longer
published. Executable request and response closures belong to the 21 compiled
plans, with exact source and profile identities retained independently of
physical sharing.

The source-controlled schema-2, renderer-ABI-3 ledger is the semantic-family
membership authority. One central catalog stores each family contract and exact
consumer digest; its active membership is now empty. In the earlier
wrapper/kernel implementation, exact controller modules statically imported
shared functions under private exchange aliases while retaining version-owned
classes and models. That historical representation is reproducible, but no longer
owns installed execution. Exact `3.1.0` and `3.1.9` task-type response differences
remain represented in their generated schemas.
Matching routes, hashes, fingerprints, generated bytes, or kernel shapes may
identify a candidate but are never sufficient to authorize membership.

Generated runtime size is not itself the primary quality metric. The goal is a
maintainable architecture with local change, explicit exact behavior, and typed
boundary validation; removing unused operations, duplicated code,
import/type-check surface, and whole-version navigation cost supports that goal.

## Cross-version semantic policy

### Project scope and selector resolution

Canonical selectors carry the project identity whenever the native operation
is project-scoped. The command layer may obtain it from an explicit option,
current selection, or a safe resolver, but the domain interface must not erase
it merely to preserve a global-ID convenience.

Retained project-scoped main routes are preferred when they preserve the same
intent. For a version such as `3.4.2`, where global V2 lookup is gone, missing
project scope should fail with an actionable stable error rather than scan
every visible project. Task-definition operations and project-scoped
workflow-instance and task-instance operations now require either explicit
`--project` or the current project context. `task-instance log` is the sole
projectless runtime-instance exception because the logger API is natively
ID-addressed. Schedule ID-first operations retain their separately reviewed,
bounded global recipe; instance scoping does not silently change that contract.

### V2 route policy

V2 is neither a product concept nor an automatic compatibility base. For each
existing V2 use, review:

1. whether the official UI and surviving main controller use a project-scoped
   route;
2. whether request fields, omitted values, and sentinels are equivalent;
3. whether one main request or a bounded recipe can preserve lookup cost,
   permission scope, atomicity, and output;
4. whether retaining V2 for an older exact profile has a material advantage.

Task compatibility is expressed as reviewed epochs rather than as a V2
inheritance rule. In `1.3.9`, task list/get/update preserve string-native ids
and exact-name selection through the containing process-definition graph; they
do not fabricate integer code/version identity. `2.0.0` can list and inspect
tasks, while task update replaces the containing workflow so relation-bound
versions advance coherently instead of using the unsafe standalone endpoint.
`2.0.9` can
update ordinary task fields, while explicit `depends_on` changes fail before
HTTP and direct the caller to workflow edit. `3.0.0`, `3.0.6`, and `3.1.0` can
update dependencies. `3.1.9` and `3.2.0` use ordinary-field-only standalone
updates guarded by a complete project workflow inventory at prepare and apply;
`3.2.1` and newer can update dependencies again.

Every code-native task profile builds typed detail and DAG programs, plus an
update program only where its exact route preserves the stable mutation. The
`3.4.1` and `3.4.2` programs use the long-lived project-scoped main routes, so
the stable profile no longer retains the V2 convenience that `3.4.2` deleted.
Where the selected task form cannot safely represent clearing the last upstream
dependency, the domain rejects that intent and directs the caller to workflow
edit. Schedule ID-first operations follow their own bounded global recipe;
schedule create/update still preserve exact nullable, sentinel, no-environment,
and cleared-zero semantics instead of treating V2 and main forms as
mechanically interchangeable.

### Mutation and unknown-field preservation

Read projection can be narrow; mutation state cannot be lossy. The target
generic recipe keeps the original native document or an explicit lossless
extension bag, applies a canonical patch only to declared owned paths,
validates the selected profile's rules, and writes back all unmodified fields.
Its complete plan will record exact profile/recipe identities, resolved native
identities, owned and preserved paths, a before digest, sanitized request
sequence, mutation boundary, and readback/cleanup behavior. Consumed projection
and preservation remain separate contracts.

Every executable task mutation recipe from `2.0.9` onward takes the conservative
subset its exact controller contract can preserve. It reads the workflow DAG
and project-scoped task detail, validates returned identity, reconstructs only
the reviewed top-level update shape, and preserves unknown members inside
nested `taskParams` opaquely. An unknown top-level task field fails closed
before mutation because the selected update form cannot prove that field will
survive. Dependency changes use the reviewed upstream-code form from `3.0.0`;
`2.0.9` rejects that intent, and affected main-route profiles also reject
clearing the final upstream relation. Both cases direct the caller to workflow
edit. No-op updates send no mutation.

`prepare_update` produces one profile- and recipe-bound generated request. The
dry-run and apply paths use the same selected `CompiledWireProgram` and
preparation logic; an applied invocation executes its detached encoded request
rather than rebuilding one from rendered preview JSON. Prepare first requires task detail,
the selected DAG task, and all relations that reference it to agree on workflow
identity and task version. Immediately before PUT, apply fresh-reads detail and
DAG, repeats that consistency check, and rejects the plan if task version,
normalized upstream codes, or any requested-field projection differs from the
prepared state.

`3.4.1` accepts its exact nullable integer result, while `3.4.2` requires a
strict integer. One immediate canonical readback verifies the requested field
projection, exact dependencies, task-version advance, and coherent
detail/DAG/relation versions. It does not retry readback for eventual
consistency. Response-decode or post-success verification failure records
`mutation_applied: true`. Transport failure after dispatch records the weaker
`mutation_may_have_applied: true`; the mutation wire disables transport retry,
so this path directs inspect/reconcile and never resends the PUT.

The tracer does not yet implement the target's general preservation
fingerprint, whole-native-state before digest, or universal concurrency
protocol. Its fresh-read check is optimistic only: DolphinScheduler exposes no
atomic version precondition on this PUT, so an unavoidable TOCTOU window remains
between the last read and the write. Those general mechanisms remain mutation
consolidation work rather than implied ledger features. Non-idempotent
mutations are never retried automatically without exact evidence for an
idempotency mechanism.
Every future multi-request recipe must declare a bounded request budget,
permission scope, atomicity, and partial-failure model; missing project scope
never authorizes scanning every visible project.

### Capability and support semantics

One materialized profile catalog drives preflight, selected-version schema,
`capabilities`, navigation, diagnostics, and release gates. Capabilities exist
at both action and independently evolving facet level. A task update action may
be supported for inline SQL while SQL file mode is unsupported.

The current catalog closes every stable action over all 36 exact profiles;
its counts are recorded in the [current inventory](architecture.md#current-stable-surface).
These terminal decisions answer whether and how an
intent can execute. Per-action evidence levels answer what has been verified;
the aggregate profile tier answers what has been promoted. None is inferred
from either of the others.

Every materialized profile cell is `supported`, `limited`, or `unsupported`;
there is no unknown cell. Unsupported means zero HTTP requests. `limited`
requires a named constraint that the service can preflight from the normalized
intent; it is not a documentation-only escape hatch.

Action/facet capability is checked before selector resolution, pagination, or
other convenience requests. When actual cluster availability cannot be known
statically, a bounded read-only probe is permitted only as an explicitly
modeled runtime-availability check; no mutation occurs until that check and the
intent guard pass. Such a probe does not change the static profile cell.

An aggregate version label is derived from the matrix and verification
evidence. A boolean `tested` flag cannot be the authority because one profile
can contain static-only helpers, live-smoked reads, fully exercised mutations,
and unavailable plugin facets at the same time.

The action/facet/profile matrix is materialized, not handwritten as a
Cartesian product. Each stable action declares the semantic operations and
facets it depends on. The compiler closes those dependencies through recipes,
wire programs, authoring schema, lint, compilation, preservation, and tests;
the exact profile then derives the action cell from the relevant coordinates.
An upstream change is attached to the changed semantic root or facet, and the
same dependency graph produces the review queue and invalidated gates.
Handwritten affected-action lists can explain a decision, but are never the
sole source of impact truth.

At invocation time, the normalized intent derives a capability requirement
expression from the base action and only the facets or modes actually requested.
The profile and runtime facts evaluate that expression before execution. An
aggregate action status is an index and navigation summary; it does not claim
that every possible task type, mode, or input combination is executable and it
does not materialize their Cartesian product.

Impact analysis distinguishes at least blocking wire changes, behavior-review
candidates, source-only noise, and deployment-sensitive unknowns. An empty
source or normalized-wire delta is useful evidence, but never transitive proof
that two exact releases are semantically equivalent. Direct exact membership
and its review evidence are retained for every shared recipe or wire family.

Static profile capability is also distinct from runtime availability. Cluster
plugin installation, deployment configuration, current-user permissions, and
server health can make a statically supported facet unavailable; `doctor` may
report those facts but must not rewrite the exact profile.

When the server exposes a reliable exact identity and it disagrees with the
selected profile, mutations are blocked. Where exact detection is unavailable,
explicit profile selection is an operator attestation; `doctor` and live
receipts mark the identity as unverifiable rather than implying a successful
match.

## Verification architecture

Static extraction, contract tests, and live evidence answer different
questions and are all retained:

1. **Compiler tests** cover all 36 exact snapshots, provenance, normalization,
   response-evidence conflicts, plugin contracts, automatic closure, stale
   decisions, and deterministic generation.
2. **Wire-family contract tests** are parameterized over every family member
   and assert exact method, path, query/form/body binding, envelope decoding,
   response projection, and sanitized trace output.
3. **Domain-module tests** use a recording or in-memory executor to prove
   selector resolution, multi-request sequencing, preservation, dry-run parity,
   errors, and mutation/readback outcomes without faking a whole DS session.
4. **Profile tests** prove every stable action/facet cell is explicit, maps only
   to its declared recipe closure, and performs zero requests when blocked.
5. **Shared domain scenarios** express one product behavior and run it over all
   applicable profiles instead of copying a whole test file per version.
6. **Live gates** run an exact-version read smoke for every released version, a
   named conformance-bundle scenario for every stable support tier, and exact
   mutation/negative cases wherever risk, plugin behavior, permissions, or
   server-side defaults are version-sensitive. Ordinary shared reads rely on
   wire/domain contracts plus each exact profile's smoke; they do not require a
   duplicate full live scenario per recipe.

Behavior checks alone do not validate an installation artifact. Promotion
requires a clean-source build, successful artifact preflight and evidence bound
to that same wheel's verification claims. Preliminary checks and artifacts
that fail preflight cannot enter the governed promotion corpus.

Historical governed campaign `promotion-167723bbe11d-r7-20260809` built one wheel
from an isolated clean source tree and first passed the public wheel-only
exact-source, Core Metadata, `RECORD`, and manifest preflight. It then repeated
12 sequential CLI invocations per profile (`180/180` total) against wheel
SHA256
`167723bbe11d919b9d30a91ade4b017530743ec75061fdcb78a93429fc0bde88`.
All schema-1 validators passed with zero remote or fixture mutation. The 15
receipts produced by the campaign are archived as
`docs/development/live-evidence/history/exact-read/<version>/2026-08-09-167723bbe11d.json`.
They validate bounded `project.list|get` and `workflow.list|get` behavior,
including exact `doctor` health handling, for that artifact. Current profile
fingerprints have changed, so those receipts remain historical. Campaign
`conformance-e8eacee57af9-r12-20260811` reran the exact-read gate on all 15
profiles with wheel SHA-256
`e8eacee57af9a9d2659194b05bccbbd7680eb10c310c4189d9cfa3ae8b5c9152`;
that same-wheel corpus passed its checker and supplied evidence only for those
four actions on the recorded manifest. Expanded fingerprints now make it
historical. Neither result promotes an entire profile.
The campaign boundary is recorded in
[Live Testing](live-testing.md#generic-exact-profile-installed-wheel-read-gate).

Completing a source inventory or filling every matrix cell does not promote a
version. Every exact profile offered under a stable support tier also passes a
package/profile-closure gate, an exact read smoke, and the bounded core scenario
declared by that tier. Full support includes the standard
project/workflow/task lifecycle; a future `legacy_core` tier defines a smaller
honest core instead of simulating absent capabilities. Experimental profiles
promote individual actions without implying a stable whole-profile tier. Every
mutation action or plugin facet actually promised by any tier carries its own
exact success, readback, applicable negative, and cleanup obligations.

Support tiers reference machine-readable named conformance bundles containing
required actions, facets, scenarios, and freshness rules. `full` and a future
`legacy_core` are derived from satisfying their bundles and evidence gates, not
from a free-text label or a complete-looking matrix. Experimental support
remains a set of independently promoted actions and facets.

Live receipts bind the immutable installed artifact or wheel digest, exact DS
image and source identity, materialized profile, recipe, wire, projection, and
preservation fingerprints, plugin set, relevant configuration, timezone,
principal, redacted operation trace, cleanup result, and evidence
timestamp/freshness policy. Shared contract scenarios reduce duplicate live
work; they never replace the exact-version smoke, named support bundle, or a
high-risk exact mutation gate.

The committed `3.4.2` schema-v4 receipt predates the all-version profile
materialization. It remains auditable evidence for the immutable `0.4.0` wheel,
its older manifest, and the 15 actions exercised by that gate. It must not be
reinterpreted as evidence for the current generated package or expanded
semantic manifest. The governed r7 generic-read corpus attests only its four
named stable read actions; a fresh current mutating installed-wheel run remains
required for the historical gate's mutation scope.

Tests migrate by replacement. Once a domain module and wire family cover an old
adapter/fake contract, implementation-shaped whole-version tests and fake
mirrors are deleted instead of retained as a second architecture; their stable
scenario, contract, corpus, and live assertions move to the new seam.

## Implementation path

The migration uses four coarse stages. It starts with vertical behavior and
extracts shared infrastructure only after a second domain tests the abstraction.
Each stage should land in coherent changes rather than dozens of permanent
intermediate layers.

### Delivery geometry

The work intentionally moves along three different axes:

| Work | Traversal rule | Why |
| --- | --- | --- |
| Source evidence and profile coordinates | Breadth-first across all 36 exact versions | Makes omissions and real release boundaries mechanically visible early |
| Steady-state runtime migration after seam validation | One cohesive domain across all applicable profiles before moving to the next domain | Keeps reasoning local and prevents 36 partial adapters |
| Verification and support promotion | One exact profile at a time | Prevents family reuse or matrix completeness from becoming a support claim |

Mechanical source and task-plugin extraction should cover the full release set
as early as possible. Manual semantic review remains incremental and is pulled
by the domain currently being implemented; otherwise the project would replace
adapter duplication with a long review waterfall. Stages 1 and 2 are deliberate
seam-validation exceptions: they prove two different domain shapes on a small
version set before Stage 3 adopts the domain-across-all-profiles discipline.

`unreviewed` exists only in compiler/build audit worklists. It never enters the
public `Availability` model or an installed runtime profile. Packaging may
include other complete profiles and reviewed experimental cells; every packaged
coordinate is materialized as `supported`, `limited`, or a zero-request
`unsupported` with a reason that distinguishes “not reviewed” from “server
lacks this capability”. An unreviewed coordinate blocks stable promotion only
when it is required by that tier's named conformance bundle.

Representative releases such as `3.4.1`/`3.4.2`, `3.2.2`, `2.0.9`, and `1.3.9`
are navigation and test-order anchors only. They are not inheritance roots,
and an adjacent release joins a shared recipe or wire family only through its
own exact, directly reviewed membership.

### 1. Correct evidence through the task-authoring tracer

The full-set mechanical extraction and build/audit coordinate inventory now
cover all 36 versions. The task-plugin source sweep is complete for the exact
source set, and the action ledger contains every terminal action/version
coordinate. The separate published-CLI compatibility corpus remains open: it
must index parsed YAML semantics, schema compatibility, template parse/compile
semantics, lint,
normalized dry-run semantics, stable errors, and lossless round-trip by CLI
artifact and exact DS profile. Migration-only byte snapshots may detect drift
but do not freeze template prose, formatting, key order, private preview layout,
or schema serialization bytes.

The first expanded baseline is
[`v0.3.0` / exact `3.4.1` SQL inline](../../tests/compatibility/corpus/v0.3.0-ds3.4.1-sql-inline.json).
Its source metadata binds the release tag to a commit, a locally rebuilt wheel,
and a digest of the package's source-file hashes. The rebuilt wheel and its
installed package were checked against every `dsctl` file in that commit; this
establishes a source-matched baseline, not possession of the original public
distribution binary. Synthetic inputs exercise the installed historical
package without a server. The fixture retains parsed template input, schema
constraints, lint and error semantics, normalized dry-run requests, and an
export/reparse/edit case. Tests select its exact DS profile explicitly.

Compatibility assertions allow equivalent enum ordering and additional accepted
inputs, such as datasource names alongside the previously accepted positive
IDs. Intentional behavior changes remain explicit: lint now accumulates
structured diagnostics for missing SQL, while workflow creation retains its
`user_input_error` boundary. Tests compare each error boundary with the same
historical action. They preserve historical expectations rather than rewriting
them from current output. Current edit compilation also preserves native SQL
parameters without inserting the four empty arrays added by the old parser.
Historical export retained unknown SQL parameter keys but did not expose
unmodeled native task fields; that recorded limit is not a promise of
whole-object losslessness. Additional artifacts, profiles and authoring facets
still need their own baselines.

Mechanical source completeness deliberately does not claim reviewed semantics
for every task type. The tracer's exact `3.4.1`/`3.4.2` SQL decision is complete;
other task-type semantic reviews proceed when a stable authoring facet requires
them. “Promised task types” means types already present in the stable
authoring/capability contract, not every plugin upstream happens to ship.

Historical tracer checkpoint: the first implementation corrected response-evidence
precedence without collapsing declared and inferred facts. It provided the minimum
`WireProgram`/`WireExecutor`, mechanically derived task closure, and deep
`TaskDefinitions` module needed for task list/get/update, inline SQL, dry-run,
and readback on `3.4.1` and `3.4.2`. `TaskDefinitions`, its DS-native compiler,
resolution, paging, and record projections live under `upstream/`; both
versions use project-scoped task detail/update, and `3.4.2` SQL file mode
remains a separate facet rather than being backported.

This tracer is intentionally difficult: it exercises source typing, selector
scope, a minimal SQL plugin inventory, mutation preservation, V2 migration, and
live verification. Its executable boundary is `task.get`, `task.update`, and
the SQL inline facet; task discovery/schema/template, lint, compile,
export/preservation, and dry-run must consume the same reviewed selection. SQL
resource-file mode is opaque-preserve-only and remains unavailable for public
typed create/edit. SQL completion does not promote whole `workflow.create`,
`workflow.edit`, or unrelated task-type lint.

This first tracer established blocked zero-request behavior, mutation
negative/readback behavior, authority selection, and deletion of the replaced
task V2 path. Its schema-v4 installed-wheel receipt proves the `3.4.2` mutation,
independent dry-run non-mutation, preservation, readback, and fixture restoration
for the `0.4.0` wheel and its older manifest. It is historical evidence, not a
current all-version-profile release receipt. The archived schema-6
installed-wheel receipt supplied bounded 15-action `live_smoke` evidence and
exact fixture restoration for its recorded wheel. The current schema-7
contract requires a new same-wheel receipt before those actions can count as
current evidence. A deterministic stale-plan negative remains required before
the current artifact's `task.update` can be promoted to `live_full`; the governed
exact-read corpus does not expand to task mutation, and unit/contract coverage
is not mislabeled as receipt evidence.

### 2. Validate the abstraction, then generalize the compiler and profiles

The task, schedule, and definition-read slices validated the common seam. The
compiler now owns the reviewed decision schema, automatic semantic/action
closure, exact profile projection, task-plugin source inventory, and the four
change questions represented by source, effective-wire, consumed-projection,
and preservation evidence. All 36 exact profiles use those mechanisms rather
than being backfilled from three inheritance anchors.

This implementation milestone does not by itself attest freshly built package
contents or any live cluster. The later r7 generic-read campaign supplied
bounded per-action behavior evidence for its historical artifact. The later r12
exact-read corpus supplied the same four-action evidence for its recorded wheel;
it too is now historical after manifest and fingerprint expansion. Neither
result is profile-wide evidence. Artifact publication and profile promotion
remain later release stages.

### 3. Migrate the stable surface and fill exact profiles

The stable surface now has a terminal decision for every action/profile
coordinate, and profile-selected cohesive domains realize supported recipes or
fail limited/absent intents before HTTP. The migration used `3.4.1`/`3.4.2`,
`3.2.2`, `2.0.9`, and `1.3.9` as navigation anchors only; every shared recipe
membership is an exact reviewed decision rather than transitive inheritance.
Project scope is explicit where required, and task-plugin facets remain
independent from aggregate action support.

The structural replacement in this stage is complete: all migrated stable
roots are independent of the broad session/adapter path, and the giant adapter,
broad runtime/protocol composition, whole-session fake, dead service helpers,
and implementation-shaped adapter tests have been deleted. Existing stable
output and error contracts remain the acceptance boundary. The complete
`3.4.1` source contract remains reproducible as a snapshot/build audit artifact,
while its published package now contains only the reviewed runtime slice.

The preferred domain order is: identity/doctor/project/workflow reads;
workflow and task authoring; schedules and execution; instances/watch/logs and
recovery; then resources, governance, administration, and lower-frequency
plugins. Evidence may justify reordering, but domains should not be split merely
to make version counts look complete.

This is delivery-tranche ordering, not a prescription for a few new god
modules. Actual module interfaces follow caller cohesion and are introduced
where a second adapter or recipe demonstrates independent variation.

### 4. Consolidate runtime artifacts and close release gates

All stable actions are represented, and the giant `DS341Adapter`, broad
`UpstreamSession`, and obsolete whole-session fake have been removed. Replacing
the full `3.4.1` published SDK with its reviewed runtime slice is complete while
full exact source manifests remain reproducible for audit. Reviewed wire-family
data lives in one central catalog, while the complete stable runtime slices
compile to 26 finite static kernels. Project lookup and paging, current-user
detail, task-type listing, the two task-log epochs, and an all-profile user-delete
mutation exercise the package-backed read/list/write seams. Access-token's
former paging family has been retired into its whole-domain compiled profiles.
New memberships still require explicit review; kernel equality cannot add one.

The 36-version terminal action matrix is complete. The recorded same-wheel
exact-read, named-conformance, and `3.4.2` schema-6 receipts are historical
after manifest and fingerprint expansion. Their current replacement requires
schema-2 exact-read, refreshed conformance, and schema-7 `3.4.2` receipts from
one canonical wheel. Remaining Stage 4 work is evidence-bearing: rerun the live
contract, mutation, and scenario campaigns against one canonical wheel, then
promote profiles independently. A profile is promoted only from explicit
evidence; publication remains a separate operation requiring release
authorization.

### Domain-slice definition of done

A domain slice replaces its previous implementation only when:

1. its stable selectors, intents, projections, preservation obligations,
   errors, and request budgets form one caller-oriented interface;
2. every evidence plane declared required by its decisions is present and
   reviewed, while inapplicable planes and unrelated extracted changes may
   remain absent or queued;
3. every target exact profile cell for the slice is explicit and selects only
   a derived recipe/wire closure, or fails before HTTP;
4. schema, template, lint, compilation, export, preservation, dry-run, and
   capabilities select the same task-authoring catalog where the slice authors
   data;
5. wire, domain, profile, historical-corpus, scenario, and required exact live
   gates pass, including negative and cleanup paths for mutations;
6. the composition root switches authority to the new deep module; an import
   and reference check proves the migrated roots no longer reach the old path,
   with no empty legacy wrapper or dual composition. Replaced service wire
   logic, adapter-shaped protocols, record mirrors, whole-session fakes, and
   implementation-coupled tests are deleted in the same series, while public
   contract, corpus, domain-scenario, and live assertions are retained. A broad
   adapter survives only while another domain consumes it;
7. the profile matrix, compatibility ledger, user-facing claims, and roadmap
   describe the resulting state without implying that one completed facet
   promotes an entire action or version.

### Future release adaptation

A future release starts with an exact source/task-plugin snapshot and a diff
against reviewed neighboring evidence. The dependency graph identifies stale
semantic roots, facets, recipes, projections, and tests. Review changes only
where behavior moved, materialize a new exact profile, run its closure and live
gates, then promote it independently. Unchanged coordinates join semantic
families only through direct review; their mechanical shapes may reuse existing
static kernels without importing another exact version.

New releases do not retroactively rewrite old profiles. A newer implementation
may reveal a CLI-general improvement that benefits several historical versions;
that change is applied only with evidence for each affected exact profile and a
rerun of its gates. This distinguishes a present-day cleanup informed by the
full historical view from an unsafe policy of continuously backporting the
latest server semantics.

## Architectural success criteria

The migration is complete when these invariants hold:

- no `DS_VERSION` branch exists above the internal composition root;
- no exact profile imports another version's generated package;
- services and dry-run code contain no DS paths, form field names, V2 concepts,
  generated entities, or result-envelope handling;
- every stable action and independently evolving task facet is explicit for all
  36 profiles, with unsupported actions causing zero requests;
- every runtime wire program is reachable from a stable semantic root and its
  closure is compiler-derived;
- every mechanical wire-kernel shape is emitted once, while reviewed family
  identity, exact membership, and provenance remain distinct and visible;
- no unresolved declaration/inference conflict or stale decision enters a
  runtime artifact;
- mutation recipes preserve unknown native fields and report partial outcomes
  truthfully;
- canonical results distinguish missing, explicit null, and unsupported values;
  an older profile never invents a newer identifier, field, or default;
- no profile is promoted before its authentication, result-envelope, stable
  error-classification, and server-identity/mismatch rules satisfy the declared
  support bundle;
- adding a release normally changes only affected source evidence, recipe,
  wire-family, projection and preservation decisions, profile mappings, and
  parameterized tests;
- new interface and wire-family tests replace, rather than permanently
  accompany, the old broad adapters and fake mirrors.

There is no arbitrary line-count target. Lower code volume is an expected
consequence, but locality, explicit support, generated reachability, duplicate
wire-family count, and exact evidence are the governing measures.

## Research limits and unresolved work

1. The original cross-version source research did not send live requests. The
   archived 15-version r7 corpus is separate bounded runtime evidence for four
   exact read actions on its historical artifact; the r12 exact-read corpus
   supplied those four per-action `live_smoke` values for its recorded wheel
   and is now historical. The schema-6 `3.4.2` receipt likewise covers only its
   recorded artifact and named mutating gate actions. New schema-2 exact-read
   and schema-7 `3.4.2` campaigns have not yet run, and both scopes remain
   distinct from whole-profile promotion.
   Serialization configuration, authentication filters,
   plugin packaging, database migration state, and runtime error behavior
   outside those gates can still differ from static declarations.
2. The REST snapshots omit task-plugin parameter contracts. The new independent
   inventory mechanically resolves legacy and SPI server bindings and can scan
   every task type, but only SQL `3.4.1 -> 3.4.2` currently has a checked-in
   source-guarded semantic review. Generic UI/default/serializer extraction and
   reviewed facets for the remaining promised task types are still required.
3. `DTO` and `model` counts are extractor categories and should not be used as
   a proxy for total upstream type complexity. The `3.4.2` DTO count of zero
   does not mean DolphinScheduler has no DTO classes.
4. The current inferred logical type has known false positives. Compatibility
   groups derived from it must be regenerated after the evidence model is
   corrected.
5. Route equality does not prove response equality, permission equality,
   atomicity, default equality, or error equality. Route inequality does not
   disprove semantic adaptation.
6. UI non-use of `/v2` strongly indicates the official interaction path but
   does not prove that every V2 endpoint is defective or unused by external
   clients.
7. The early reviewed semantic-impact artifact covers only four read operations
   and remains an intentionally narrow historical analysis. Its groups are
   candidate evidence, not the [current compatibility matrix](architecture.md#current-stable-surface).
8. Mutation compatibility needs unknown-field preservation and exact-version
   negative-path evidence. Read projection equality alone is insufficient.

## Primary-source index

The most important exact-tag sources cited above are collected here for audit:

- [`1.3.9` ProjectController](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProjectController.java)
- [`1.3.9` ProcessDefinitionController](https://github.com/apache/dolphinscheduler/blob/1.3.9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProcessDefinitionController.java)
- [`2.0.0` ProjectController](https://github.com/apache/dolphinscheduler/blob/2.0.0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProjectController.java)
- [`2.0.0` ProcessDefinitionController](https://github.com/apache/dolphinscheduler/blob/2.0.0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/ProcessDefinitionController.java)
- [`3.2.0` WorkflowV2Controller](https://github.com/apache/dolphinscheduler/blob/3.2.0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/v2/WorkflowV2Controller.java)
- [`3.3.1` WorkflowDefinitionController](https://github.com/apache/dolphinscheduler/blob/3.3.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/WorkflowDefinitionController.java)
- [`3.4.1` SchedulerController](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/SchedulerController.java)
- [`3.4.1` ScheduleV2Controller](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/v2/ScheduleV2Controller.java)
- [`3.4.1` TaskDefinitionController](https://github.com/apache/dolphinscheduler/blob/3.4.1/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java)
- [`3.4.2` SchedulerController](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/SchedulerController.java)
- [`3.4.2` TaskDefinitionController](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java)
- [`3.4.2` WorkflowDefinitionController](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/WorkflowDefinitionController.java)
- [`3.4.2` SQL task parameters](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-task-plugin/dolphinscheduler-task-api/src/main/java/org/apache/dolphinscheduler/plugin/task/api/parameters/SqlParameters.java)
- [`3.4.2` workflow UI service](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/service/modules/workflow-definition/index.ts)
- [`3.4.2` schedule UI service](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/service/modules/schedules/index.ts)
- [`3.4.2` SQL UI field behavior](https://github.com/apache/dolphinscheduler/blob/3.4.2/dolphinscheduler-ui/src/views/projects/task/components/node/fields/use-sql.ts)
