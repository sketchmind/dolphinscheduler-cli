# Live Testing

## Purpose

Live tests validate `dsctl` against a real DolphinScheduler cluster through the
same REST surface and CLI commands that users rely on.

They exist to catch issues that fixture-based tests cannot prove:

- auth and permission mismatches
- DS-native request/response contract drift
- async runtime state transitions
- schedule materialization and execution timing
- plugin and cluster-topology differences

Live tests are opt-in, slower, and potentially destructive. They complement
unit tests, command tests, adapter tests, and generator checks. They do not
replace them.

## Current Reality

The current offline suite is strong for local correctness, but it is still
offline:

- command tests use `CliRunner`
- service tests use fake adapters and mock runtimes
- adapter tests use mock transports

That is necessary, but it is not sufficient to prove that `dsctl` works
correctly against a real DolphinScheduler cluster.

Offline tests can prove:

- local validation logic
- output envelopes
- resolver and pagination logic
- type contracts and import boundaries
- handwritten request shaping against mocked transports

Offline tests cannot fully prove:

- the real DS request and response contract
- auth and permission behavior
- cluster-specific defaults and restrictions
- runtime state transitions and scheduling behavior
- plugin availability and capability differences

Therefore, no cluster-interacting surface is considered fully validated until
it has live coverage.

## Offline Gate Before Live Runs

Run `python tools/check_quality_gate.py --mode development` before spending time
on a real cluster. It owns lint, formatting, types, architectural boundaries,
generated freshness, static source checks and all offline test lanes.

After exporting the required live-test environment variables, append the live
suite with `--include-live`. The tool rejects missing enable flags or an absent
admin profile before executing checks. This source-tree campaign does not
replace current installed-wheel receipts: release readiness uses
`python tools/check_quality_gate.py --mode release` after those receipts exist.

When the change touches DS-facing failure handling, review the current error
model before or alongside the live run:

- `python tools/extract_ds_api_error_inventory.py --format summary`
- `python tools/audit_dsctl_error_translation.py --format summary`
- `python tools/extract_dsctl_error_translation_matrix.py --format summary`

Those tools answer different questions:

- inventory: what upstream DS exposes in source
- audit: where `dsctl` currently translates or leaves errors raw
- matrix: which DS codes currently map to which `dsctl` error types

## Core Principles

- Use `dsctl` itself as the black-box client. Do not bypass the CLI with
  handwritten HTTP calls unless the purpose is debugging a failing live test.
- Stay REST-only. Do not use Py4J, PyDolphinScheduler, or direct database
  mutation in the test path.
- Preserve DS-native semantics. When a live test exposes a mismatch, prefer
  fixing the generator, adapter, or service layer over inventing CLI-only
  behavior.
- Separate bootstrap authority from business-user execution.
- Make every live run traceable and cleanable with unique resource prefixes.
- Treat cluster-specific or plugin-specific capabilities as optional and
  explicitly labeled, not as silent assumptions.

## Coverage Policy

### Rule 1: Every Cluster-Interacting Command Needs Live Coverage

Every command that sends a request to a real DS cluster must have at least one
live test.

Grouped scenario coverage is allowed for efficiency, but grouping does not
remove the requirement. If a table row below lists multiple subcommands, each
listed subcommand still needs at least one live assertion somewhere in the
suite.

This rule is also enforced offline by a static governance test that compares
registered cluster-interacting command paths with the command paths referenced
from `tests/live/`.

### Rule 2: Local-Only Commands Do Not Need Live Coverage

Commands that do not talk to the cluster should stay covered by offline tests.

### Rule 3: Mutations Need More Than One Happy-Path Check

For cluster-interacting mutations, the minimum live obligation is:

- one successful mutation case
- one read-back assertion proving the mutation took effect
- one negative, permission, or lifecycle-precondition case where applicable
- cleanup or idempotent teardown

This is the `live_full` obligation. An experimental action with a deliberately
bounded gate may remain `live_smoke` after a successful round trip and cleanup,
but its missing negative must stay explicit and it is not definition-of-done
evidence.

### Rule 4: Runtime And Scheduling Need Scenario Coverage

For runtime and schedule commands, one shallow contract check is not enough.
These surfaces also need live scenario coverage because correctness depends on
time, state transitions, and DS scheduler behavior.

## Definition Of Done

### Local-Only Surface

Done when:

- offline tests exist
- command and service behavior is covered
- output contract is stable

### Cluster-Interacting Read Surface

Done when:

- offline tests exist
- at least one live contract test exists against a real cluster
- the live case proves the projected payload shape and selector semantics

### Cluster-Interacting Mutation Surface (`live_full`)

Done when:

- offline tests exist
- live round-trip coverage exists
- live negative or precondition coverage exists
- cleanup is proven

### Runtime Or Schedule Surface

Done when:

- offline tests exist
- live contract coverage exists
- live scenario coverage exists for state transitions

## Exact-Version Profile Gates

Every exact profile has a complete terminal catalog for all stable CLI
actions. The [current inventory](architecture.md#current-stable-surface) records
the profile and supported/upstream-absent coordinate counts. Catalog completion,
action availability, live verification and profile promotion are tracked
separately. The
[support policy](../user/version-compatibility.md#support-policy-and-verification)
is separate from these artifact-bound verification scopes.

A live gate exercises a named set of actions. Its receipt binds those actions
to an immutable artifact and manifest.
The retained `0.4.0` development candidate corpus binds all 37 profiles to the
same wheel: 37 four-action [exact-read receipts](live-evidence/exact-read/), 37
18-action [conformance receipts](live-evidence/conformance-bundles/) using
`full_core/v1`, and the separate 15-action `external-shell/v1` schema-7 receipt
on exact `3.4.2`. Each receipt records its immutable wheel digest and exact
contract identities. This wheel predates the later Typer and DataX runtime
fixes; a release candidate containing those fixes needs its own acceptance.

The generic read gate covers four remote stable actions: `project.list`,
`project.get`, `workflow.list`, and `workflow.get`. A promotion-grade generic
schema-2 corpus sets those four actions to `live_smoke` on every exact profile.
Conformance and mutation scopes have their own gates. The separate schema-7
`external-shell/v1` gate adds evidence for its fixed 15-action bundle on exact
`3.4.2`. Its scope is the named actions and artifact; profile support policy and
the `tested` flag are maintained separately.

All 37 profiles now mark these four reads as `live_smoke`. The additional 22
profiles use reviewed preliminary `full_core/v1` evidence from an installed
wheel, following the [release admission procedure](release.md#candidate-validation).
The [bounded review](live-evidence/history/read-admission/2026-09-26.json) binds
each original receipt, exact source identity, successful read trace and four
recipe fingerprints. The original receipt validator accepted all 22 receipts;
the four read decisions and source closures are unchanged. Whole-contract
changes in `3.1.0`–`3.1.7` concern only the alert-plugin paging response.
This changes 88 action verification cells and no profile support or `tested`
status. These preliminary results retain their original wheel identity and do
not replace the required final same-wheel 37-profile read and conformance gates.

An exact read gate must:

- start by running `version` with the gate's explicit `--env-file`, then assert
  the selected server, contract, family, and support level before any cluster
  request
- cover `project list|get` and `workflow list|get`, including name selection,
  returned native identities, pagination/search, and attached-schedule hydration
  in `workflow.get`
- retain a redacted CLI action trace for the behavior actually exercised
- bind that trace to the installed exact profile, generated manifest, selected
  domain recipes, and immutable wheel; source-bound contract tests separately
  prove the expected REST operations and path templates
- record the CLI revision or wheel digest, DS release/image identity, generated
  contract/profile identities, and recipe fingerprints with the result

The `3.4.2` extension additionally verifies `doctor` current-user
authentication and a project create → list/get → update → get → delete round
trip with cleanup. Duplicate-name conflict and post-delete not-found paths must
retain stable CLI error typing. A temporary externally provisioned scheduled
workflow drives the positive workflow reads because workflow mutation is
outside this gate's allowlist and evidence scope. The gate cross-checks full DAG
describe, compact digest topology/counts, raw YAML export, attached-schedule
hydration, and workflow-filtered schedule listing against that same fixture.

The expanded task-definition gate requires an exclusive, dedicated fixture. Its
workflow remains `OFFLINE`, contains one explicitly identified editable
`SHELL` task, and must not be changed by a user, scheduler, UI session, or
another gate while the run owns it. From the installed wheel, the gate verifies
`task list`, exact project-scoped `task get`, and the generated `task update`
request in dry-run mode before sending a real update. It independently reads
task detail and the DAG after dry-run to prove no state changed; the dry-run
envelope's `mutation_sent: false` assertion is necessary but not sufficient.

Within the applied command, the CLI fresh-reads task detail and the DAG before
PUT and rejects a prepared-state conflict in task version, upstream codes, or
the requested command projection. Every preflight and readback snapshot must
also keep the detail task version, DAG task version, and referencing relation
versions coherent. The gate then requires exact command and dependency
readback, a task-version advance, restores the original command byte-for-byte
in a `finally` path, and verifies the preserved non-owned task fields, upstream
relations, complete DAG topology, and `OFFLINE` workflow state after
restoration. A current schema-7 receipt produced by this procedure therefore
attests that the external fixture was mutated and fully reconciled by its named
wheel; it is not
described as read-only. It does not imply workflow mutation, schedule mutation
or other schedule actions, unrelated DS-native local authoring facets, or any
action outside the current 15-action allowlist. The earlier schema-v4 receipt
remains historical because it binds its older generated profile and semantic
manifest rather than the current materialized contract.

The installed-wheel scenario does not pause one in-process prepared plan,
perform an external mutation, and then resume that same plan. Consequently the
receipt does not attest the optimistic stale-plan negative even though the
applied command executes the guard. That negative is unit/contract-tested and
remains a separate `live_full` promotion obligation. Cleanup's refusal to
overwrite a changed command or version proves exclusive-fixture protection,
not the prepare-to-apply conflict path.

The `dsctl` phase of the `3.2.2` gate is read-only. Fixtures may be
pre-provisioned or created and cleaned by the external version-matrix harness
through the exact upstream REST contract; they must not be bootstrapped by
calling CLI actions outside this gate's allowlist. The internal schedule query
needed by `workflow.get` does not expand the gate's public evidence scope.

### Named Conformance Bundle Installed-Wheel Gate

The generated conformance catalog defines two inherited support-tier scenarios:

- `legacy_core/v1` contains 9 actions: `doctor`, project
  `create|delete|get|list|update`, `schedule.list`, and workflow `get|list`.
- `full_core/v1` inherits those 9 actions and adds workflow
  `create|delete|describe|digest|edit|export` plus task `get|list|update`, for
  18 actions total.

The static tracer marks both named bundles ready for every admitted exact
profile, with an executable static closure. This static expansion is not live
evidence.

For a campaign built from the current manifests, each exact version's highest
ready bundle is full core. The runner and both
black-box scenarios, strict schema-1 receipt validator, complete all-version
same-wheel corpus checker, default quality-gate integration, Python 3.11 CI
check, and final release-artifact enforcement are implemented. Full-core
workflow authoring uses an opaque source-known `SHELL` document and records no
typed-authoring facet claim.

Workflow revision checks follow exact upstream behavior. DS `1.3.9` creates
workflows at version `0` and does not increment that version when updating a
task or editing the workflow. The scenario and its failure cleanup accept the
non-negative legacy version, require it to remain unchanged across these edits,
and prove each change through its command or description readback. Modern
profiles retain positive workflow versions and their existing transition
checks. Ownership, task identity, topology, and zero-residue checks apply to
both paths.
The legacy export also retains `global_params: []` and the owned tasks'
`flag: YES` explicitly. The gate requires these canonical authoring fields even
when the legacy detail response has a null derived parameter map.
DS `1.3.9` also normalizes empty global parameters from null fields on creation
to `globalParams: "[]"` and `globalParamMap: {}` on update. Preservation checks
recognize that exact empty-value transition while still rejecting changes to
non-empty parameters and every other non-owned field.

The [generated schema-4 task-definition cleanup profile](../../src/dsctl/generated/task_definition_cleanup_profiles.py)
selects full-core reconciliation independently of the cleanup operation's source
presence. Its operation targets include `2.0.0` and `2.0.1`, but neither version
is selected for full-core private reconciliation or cross-process recovery.
The auxiliary `2.0.1` recipe includes its exact task `OFFLINE` release before
deletion: its [delete service](https://github.com/apache/dolphinscheduler/blob/bf2197995ac835c66b44874b8658579a3da916b0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java#L182)
rejects `flag=YES`, while its [release controller](https://github.com/apache/dolphinscheduler/blob/bf2197995ac835c66b44874b8658579a3da916b0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/TaskDefinitionController.java#L325)
and [service](https://github.com/apache/dolphinscheduler/blob/bf2197995ac835c66b44874b8658579a3da916b0/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java#L510)
set the task and current log `flag=NO`. This exact private source closure does
not admit `2.0.1` to the full-core gate or alter its receipt cardinality.
Workflow deletion leaves project-scoped task definitions on `2.0.2` through
`2.0.9`, `3.0.0` through `3.0.6`, and `3.1.0` through `3.1.2`. For those
18 full-core coordinates, the scenario uses the candidate wheel's private
Python with `-I -m` to prove the exact two owned task details before workflow
deletion, then delete each task definition once
and freshly reconcile zero residue before project deletion. Exact `2.0.2` and
`2.0.3` reject task deletion while `flag=YES`, even after workflow deletion.
Their cleanup recipe first proves that the owned tasks are no longer attached
to a workflow, releases each enabled task `OFFLINE`, and verifies `flag=NO`
before deletion. The scenario keeps its ordinary enabled task defaults; cleanup
does not avoid this upstream boundary by creating disabled tasks.
These two receipts account for eleven remote mutations, including two task
releases. The other direct-delete receipts claim nine; other full-core receipts
claim seven and legacy receipts claim three. Private cleanup reports count
successful releases and deletions separately, and their sum is the cleanup
mutation count. The private prove/cleanup trace entries are auxiliary
release-gate evidence and do not expand the named bundle's stable action set.
Cross-process recovery on `2.0.2` through `2.0.9` accepts freshly proven zero,
one, or two task residues and never publishes a receipt; pagination, ownership,
sibling state, or ambiguous release/delete drift fails closed before a subsequent
mutation. A source-proven precondition rejection is distinct from an uncertain
mutation result; diagnostics retain a bounded upstream code without exposing raw
response bodies. An unknown failure still requires fresh reconciliation.
The pre-delete release boundary follows the exact upstream
[2.0.2 service](https://github.com/apache/dolphinscheduler/blob/24ddd07493c6260a877db8a5c73e654263d356ad/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java#L182)
and [2.0.3 service](https://github.com/apache/dolphinscheduler/blob/f1a3c52a66eb8dec1f853b9c9a02b88cdebbd0dc/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java);
[2.0.4](https://github.com/apache/dolphinscheduler/blob/0ff01e3c68da6a174836216f87c05f707233f7f9/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java#L201)
adds the online-workflow condition. These facts belong to the auxiliary cleanup
contract and do not expand task authoring or public action support.
On `3.1.3` through `3.1.9`, workflow deletion itself cascades the two task
definitions. The private seam therefore proves the exact project-wide `BATCH`
and `STREAM` inventory, workflow binding, task details, and narrow version history before
workflow deletion, then requires a project-wide zero inventory afterward. It
never calls task delete, adds no auxiliary mutation, and the receipt remains at
seven remote mutations. Cross-process recovery on those seven versions uses
the same proof-only strategy and also never publishes a receipt.
Recovery is an explicit mode of the same installed-wheel runner. Its
`--recovery-run-id-file` is an owner-private strict JSON object with exact keys
`schema_version` (currently `1`) and the original 16–32-character lowercase
`run_id`. The requested coordinate must be one of the generated cross-process
recovery versions (`2.0.2` through `2.0.9` or `3.1.3` through `3.1.9`) with
`full_core/v1`; the ordinary `--evidence` destination must not exist and remains
absent on success. The runner snapshots the identity, re-attests the candidate
wheel, dispatches the
fixed live entry, and returns without invoking the evidence validator or
publisher. Other coordinates and malformed, duplicated-key, symlinked, or
non-private identity files fail before installation or cluster I/O.

The named conformance runner consumes a schema-2 cluster manifest and a
schema-1 fixture manifest. These are conformance-specific inputs: the generic
exact-read cluster and fixture manifests described below remain schema 1. An
external version-matrix orchestrator first prepares and REST-verifies the
generic exact-read fixture, its private ready state, and a fresh node-local
image inspection. The tracked projector then validates and converts those
inputs without changing the matrix topology:

```bash
python tools/project_conformance_matrix_fixture.py \
  --ds-version VERSION \
  --cluster-manifest /secure/matrix/VERSION-cluster-schema1.json \
  --fixture-manifest /secure/matrix/VERSION-fixture-schema1.json \
  --state-file /secure/matrix/VERSION-state.json \
  --image-inspection /secure/matrix/VERSION-image-inspection-schema2.json \
  --cluster-output /secure/campaign/VERSION-cluster-schema2.json \
  --fixture-output /secure/campaign/VERSION-fixture-schema1.json
```

The image-inspection input is schema 2 with one of two discriminated shapes.
A registry-backed API image uses this exact shape:

```json
{
  "schema_version": 2,
  "kind": "registry-digest/v1",
  "image_ref": "apache/dolphinscheduler-api:3.4.2",
  "image_id": "sha256:<64-hex-local-image-id>",
  "repo_digests": [
    "apache/dolphinscheduler-api@sha256:<64-hex-repository-digest>"
  ],
  "selected_repo_digest": "apache/dolphinscheduler-api@sha256:<64-hex-repository-digest>"
}
```

Every `RepoDigest` must be unique and the selected value must occur exactly
once. A tag-only `image_ref` requires exactly one digest for the API-image
repository, which must be the selected value. An `image_ref` with an explicit
published digest may retain other unique digests for that repository only when
the pin equals the selected `RepoDigest`. The only accepted API repositories are
`apache/dolphinscheduler` for `1.3.9`, `2.0.1` through `2.0.3`, `2.0.5`, and `2.0.6`,
`dsmatrix-local/dolphinscheduler` for `2.0.0`, `2.0.4`, `2.0.7`, `2.0.8`, and `2.0.9`,
`dsmatrix-local/dolphinscheduler-api` for `3.0.2` and `3.1.2`, and
`apache/dolphinscheduler-api` for every other target from `3.0.0` through `3.4.3`.

Public repository declarations follow the exact upstream deployment sources.
The reviewed managed `2.0.7` and `2.0.8` admissions retain that native unified
packaging with exact official source and binary release inputs: the public
`2.0.7` tag was observed with only Linux arm64 on 2026-09-16, and the exact
`2.0.8` tag returned 404. Their managed admission requires the same source,
binary, base and archive provenance as `2.0.9`; it does not prove that these
managed images have been built or exercised. Registry availability also does
not prove source identity or node-local runtime identity. The exact release tag
and inspected digest are still mandatory. In particular, the stale tags in
upstream `2.0.5` and `2.0.8` deployment examples are not acceptable evidence for
those releases.

DS `2.0.4` also requires an exact official source/binary build. On 2026-09-19,
the published `apache/dolphinscheduler:2.0.4` image at digest
`sha256:6191c39acc35dc0afb8cfffdbea375e6ab2f4ee94244e40c3bef39973a1a51eb`
contained API and DAO JARs whose Maven metadata identified `2.0.5`.
It cannot establish exact `2.0.4` behavior. Its managed admission requires the
same source, binary, base and archive provenance as `2.0.9`; admission alone
does not claim a successful build or live verification.

Before collecting exact-version receipts, bind the running API container and
image ID to the API and DAO JARs' Maven group, artifact and release version.
Check both artifacts against the requested exact release. Keep that observation
with the campaign's image evidence. A repository tag, a configured `DS_VERSION`,
or a successful read alone cannot establish the version of the running code.
Database and `sql/soft_version` markers are only supporting observations:
the exact source retains stale markers in `2.0.8`, `3.0.3`, and `3.2.2`.

Before resource-mutation acceptance on DS `2.0.0`–`2.0.2` with PostgreSQL, check
the [upstream resource-column defect](../user/version-compatibility.md#postgresql-resource-prerequisite).
For an affected empty test table, the bounded correction is the two upstream
`2.0.3` ALTER statements: convert `is_directory` to boolean and set its default
to false. Verify the original schema and empty-table condition under a table
lock, then independently read back both postconditions. A nonempty table needs
separate data reconciliation. Preserve the original failed receipt and record
the schema correction separately from the unchanged CLI wheel's resource
lifecycle rerun and fixture-cleanup evidence. Do not run the entire upstream
upgrade script merely to correct this column.

A matrix-managed unified API image uses this exact outer shape:

```json
{
  "schema_version": 2,
  "kind": "managed-image-lock/v1",
  "image_ref": "dsmatrix-local/dolphinscheduler:2.0.9",
  "image_id": "sha256:<64-hex-local-image-id>",
  "management": {
    "commit": "<40-lowercase-hex>",
    "lock_file_sha256": "sha256:<64-lowercase-hex>",
    "worktree_clean": true
  },
  "lock": {
    "archive_basename": "unified-2.0.9.tar",
    "archive_sha256": "sha256:<64-lowercase-hex>",
    "base_manifest": "REPOSITORY@sha256:<64-lowercase-hex>",
    "binary_sha512": "sha512:<128-lowercase-hex>",
    "component": "unified",
    "image_id": "sha256:<64-hex-local-image-id>",
    "image_ref": "dsmatrix-local/dolphinscheduler:2.0.9",
    "published_manifest": null,
    "schema_sha256": null,
    "source_commit": "<40-lowercase-hex>",
    "source_sha512": "sha512:<128-lowercase-hex>",
    "version": "2.0.9"
  },
  "actual": {
    "archive_sha256": "sha256:<same-digest-as-lock>",
    "archive_verified": true,
    "labels": {
      "org.apache.dolphinscheduler.matrix.base-digest": "REPOSITORY@sha256:<64-lowercase-hex>",
      "org.apache.dolphinscheduler.matrix.binary-sha512": "<128-lowercase-hex>",
      "org.apache.dolphinscheduler.matrix.source-commit": "<40-lowercase-hex>",
      "org.apache.dolphinscheduler.matrix.source-sha512": "<128-lowercase-hex>",
      "org.apache.dolphinscheduler.matrix.source-tag": "2.0.9"
    },
    "repo_digests": []
  }
}
```

Managed-lock provenance is accepted only for `1.3.9`, `2.0.0`, `2.0.4`, `2.0.7`, `2.0.8`,
`2.0.9`, `3.0.2`, and `3.1.2`.
It requires a clean management worktree, the exact management commit and lock-
file digest, and one exact `component=unified` lock row whose version, image
reference, and image ID match the observation. The lock row always contains a
safe archive basename and archive SHA-256; its nullable provenance fields obey
this exact presence policy:

For `3.0.2` and `3.1.2`, `component=unified` means that each role image contains
the same complete, verified official binary distribution. Role-specific image
metadata selects the API, master, worker, or alert process; it does not narrow
the distribution payload covered by the lock.

| DS version | Required non-null lock provenance | Required null lock provenance | Exact inspected labels |
| --- | --- | --- | --- |
| `1.3.9` | `source_commit`, `published_manifest` | `base_manifest`, `binary_sha512`, `schema_sha256`, `source_sha512` | none |
| `2.0.0` | `base_manifest`, `schema_sha256`, `source_commit`, `source_sha512` | `binary_sha512`, `published_manifest` | base digest, source commit, source SHA-512, source tag |
| `2.0.4` | `base_manifest`, `binary_sha512`, `source_commit`, `source_sha512` | `published_manifest`, `schema_sha256` | base digest, binary SHA-512, source commit, source SHA-512, source tag |
| `2.0.7` | `base_manifest`, `binary_sha512`, `source_commit`, `source_sha512` | `published_manifest`, `schema_sha256` | base digest, binary SHA-512, source commit, source SHA-512, source tag |
| `2.0.8` | `base_manifest`, `binary_sha512`, `source_commit`, `source_sha512` | `published_manifest`, `schema_sha256` | base digest, binary SHA-512, source commit, source SHA-512, source tag |
| `2.0.9` | `base_manifest`, `binary_sha512`, `source_commit`, `source_sha512` | `published_manifest`, `schema_sha256` | base digest, binary SHA-512, source commit, source SHA-512, source tag |
| `3.0.2` | `base_manifest`, `binary_sha512`, `source_commit`, `source_sha512` | `published_manifest`, `schema_sha256` | base digest, binary SHA-512, source commit, source SHA-512, source tag |
| `3.1.2` | `base_manifest`, `binary_sha512`, `source_commit`, `source_sha512` | `published_manifest`, `schema_sha256` | base digest, binary SHA-512, source commit, source SHA-512, source tag |

The actual observation's labels must match the lock row exactly. Its
`archive_verified` boolean must agree with whether `actual.archive_sha256` is
present; a present digest must equal the lock's archive digest. When the
managed image has no `RepoDigests`, that verified actual archive SHA-256 is
mandatory. If actual `RepoDigests` exist, they must be unique and contain the
locked published manifest. A managed-lock proof for any other API version is
rejected; those versions require the registry branch and fail closed when the
unique API `RepoDigest` is unavailable.

The projector emits cluster schema 2 with
`image_source="node-local-inspection"` and preserves the validated registry or
managed proof under `image_provenance`. Its fixture output deliberately remains
schema 1 and uses `provisioner="dsmatrix-conformance-fixture/v1"`. The runner
and independent receipt validator revalidate the same image-reference and
provenance contract; projection alone is not candidate wire evidence.

All four projection inputs must be owner-private regular files, not symbolic
links or group/other-readable files. The two outputs are published as a new
owner-only pair; an existing destination fails closed and is never
overwritten. Collecting the schema-2 inspection and any management lock facts
belongs to the external matrix orchestrator. A temporary campaign collector
may supply them, but this repository does not claim that collector as a
tracked, long-term command.

The governed corpus lives under
`docs/development/live-evidence/conformance-bundles/`. Each receipt records its
exact version, named bundle, assessment and immutable wheel identity. Directory
placement alone does not establish freshness: the corpus checker must match
those bindings to the current source and candidate artifact. Named-bundle
evidence does not change per-action verification, support level, `tested`,
whole-profile promotion, or authoring facets. See
[Release Process](release.md#local-gate)
for the build-once ordering and [Tooling](tooling.md) for the checker boundary.

### Generic Exact-Profile Installed-Wheel Read Gate

The public single-version runner is:

```bash
python tools/run_exact_profile_read_gate.py \
  --ds-version 3.2.2 \
  --wheel /secure/artifacts/dolphinscheduler_cli-X.Y.Z-py3-none-any.whl \
  --env-file /secure/profiles/ds-3.2.2-etl.env \
  --attestation-key-file /secure/campaign/attestation.key \
  --cluster-manifest /secure/campaign/3.2.2-cluster.json \
  --fixture-manifest /secure/campaign/3.2.2-fixture.json \
  --evidence /secure/campaign/receipts/3.2.2.json
```

The runner handles exactly one version. Cluster activation, persona and fixture
provisioning, cleanup, rotation, and campaign resume belong to the external
version-matrix orchestrator. A matrix campaign builds one wheel, verifies its
SHA-256 after transfer, and passes the same bytes to every invocation; it must
not rebuild between versions.

The runner requires owner-only access to the profile and attestation key,
snapshots every mutable input, creates a clean virtual environment without
system site-packages, installs the snapshotted wheel, removes ambient
`DS_API_*`, `DS_VERSION`, `DS_LIVE_*`, and `PYTHONPATH` values, and invokes only
the installed console script. The explicit profile is therefore the sole
configuration authority for the live commands. The profile API host is added
to `NO_PROXY` and `no_proxy` automatically.

The generic schema-1 cluster manifest is:

```json
{
  "schema_version": 1,
  "ds_version": "3.2.2",
  "image_ref": "apache/dolphinscheduler-api:3.2.2@sha256:<published-digest>",
  "image_id": "sha256:<64-hex-local-image-id>",
  "image_source": "swarm task plus node-local Docker inspection",
  "image_observed_at": "YYYY-MM-DDTHH:MM:SSZ",
  "api_target_hmac_sha256": "hmac-sha256:<64-hex-hmac>",
  "principal_hmac_sha256": "hmac-sha256:<64-hex-hmac>",
  "persona": "etl-developer"
}
```

The observation must be no more than 15 minutes older than gate startup.
`image_ref` must contain the exact version tag; its published digest suffix is
accepted when Swarm retained one, but is not required. The immutable node-local
`image_id` is always required. This also covers matrix-managed `2.0.0`, `2.0.4`,
`2.0.7`, `2.0.8`, `2.0.9`, `3.0.2`, and `3.1.2` images whose offline archive trust
chain has no `RepoDigests`. The
keyed API-target and principal identities bind the explicit profile and
authenticated `GENERAL_USER` without disclosing either value.

The generic schema-1 fixture manifest is:

```json
{
  "schema_version": 1,
  "ds_version": "3.2.2",
  "image_ref": "apache/dolphinscheduler-api:3.2.2@sha256:<published-digest>",
  "image_id": "sha256:<64-hex-local-image-id>",
  "provisioner": "external-version-matrix",
  "project": {
    "name": "campaign-owned-project",
    "identity": {"kind": "code", "value": 123}
  },
  "workflow": {
    "name": "campaign-owned-workflow",
    "identity": {"kind": "code", "value": 456},
    "scheduled": true,
    "schedule_id": 789,
    "release_state": "OFFLINE"
  }
}
```

`1.3.9` uses `id` for both native identities; every later target uses `code`.
The gate rejects a manifest from the wrong identity epoch or image. The fixture
may be `ONLINE` or `OFFLINE`, but it must have an attached schedule and must not
be mutated during the CLI phase.

The installed-wheel probe cross-checks the archive manifest, imported exact
manifest, generated semantic version profile, source identity, four profile
fingerprints, the accepted `project.page`, `project.get`, `workflow.page`, and
`workflow.get` decision fingerprints, and the four actions' verification levels
from that generated profile. Every action in this gate must be `live_smoke`;
the black-box `capabilities --action` results independently assert the same
values. The receipt no longer exposes adapter class names or full-adapter
presence as evidence. Exact package selection may be `full`, with an empty
semantic-operation root set, or `runtime-slice`, with the four required read
operations; the current corpus checker binds the receipt to each tracked
manifest rather than assigning a selection from the DS version. Package
selection is not a support or promotion claim. A runtime-slice manifest's
semantic-operation roots and rendered closure operation count are distinct facts
and are not required to have the same cardinality.

The black-box scenario runs `version`, the four capability checks, `doctor`,
and project/workflow list/get by name and native identity. It requires the
authenticated current user to be a `GENERAL_USER`. Exact capability selection
is authoritative for `doctor`: `1.3.9`, `2.0.0`, and `2.0.9` mark
`monitor.health` upstream-absent, so `doctor` sends no actuator request, records
a warning with reason `upstream_endpoint_absent`, and requires the current-user
fallback to pass. The scenario requires health to succeed for every profile
from `3.0.0` through `3.4.3`. The resulting receipt declares
`effects.remote_mutations: 0` and `effects.fixture_mutated: false`; it is
rejected if it contains URLs, credentials, fixture names/identities, raw
output, or raw argv.

Generic receipts use their own `exact-profile-read` schema and should be stored
under `docs/development/live-evidence/exact-read/<version>/` only after they are
accepted as current evidence. Current promotion receipts use semantic schema 2,
require `read_bundle.action_verifications`, and include that mapping in the
canonical read-bundle digest. Schema 1 remains accepted only to audit historical
generic receipts, including those that predate per-action verification binding;
it cannot enter a current promotion corpus. This contract is separate from the
exact `3.4.2` mutating schemas: schema 7 is current there, while schema 3 through
schema 6 are historical-audit-only. The release quality gate runs
`tools/check_exact_profile_read_evidence.py`, which requires one current
schema-2 receipt per exact version, one full wheel digest across the corpus, and
current contract, semantic profile, recipe, and `live_smoke` bindings.

Successful behavior checks are insufficient for promotion when the tested
wheel lacks the required action-verification claims or fails artifact
preflight. Build candidates from an isolated clean source tree and verify
source contents, Core Metadata, `RECORD` and generated manifests before live
testing. Preliminary results retain their original artifact identity and
cannot substitute for a governed same-wheel corpus.

Archived schema-1 read receipts remain at
`docs/development/live-evidence/history/exact-read/<version>/2026-08-09-167723bbe11d.json`.
They preserve the original four-action, zero-mutation evidence for their named
wheel and profiles. Current-schema receipts under
`live-evidence/exact-read/<version>/` must independently satisfy the current
same-wheel checker. Neither an earlier successful campaign nor a receipt's
location refreshes its fingerprints or extends its scope to new profiles.

### Exact `3.4.2` Installed-Wheel Gate

This gate verifies the `external-shell/v1` fixture-preservation and restoration
scenario on its reviewed exact `3.4.2` target. The current writer and promotion
checker use semantic schema 7 and require a
receipt bound to the canonical wheel and current generated fingerprints.
Fixture ownership, identity, mutation, restoration,
redaction, semantic profile, manifest, and domain-recipe bindings remain part of
the current safety contract. The previous schema-6 results are archived with the
earlier schema-v3/v4/v5 receipts; each remains auditable for the historical
artifact and action scope it names, but none can be copied or reclassified as
schema-7 current evidence.

The version-selected mutating gate has one supported entry point. Its explicit
`external-shell/v1` policy is currently reviewed for `3.4.2`; selecting another
version does not infer approval from shared wire contracts:

```bash
python tools/run_exact_profile_live_gate.py \
  --version 3.4.2 \
  --wheel dist/dolphinscheduler_cli-X.Y.Z-py3-none-any.whl \
  --env-file /secure/ds-3.4.2.env \
  --attestation-key-file /secure/ds-3.4.2-attestation.key \
  --cluster-manifest /secure/ds-3.4.2-cluster.json \
  --fixture-manifest /secure/ds-3.4.2-fixture.json \
  --evidence docs/development/live-evidence/external-shell/3.4.2/DATE-WHEEL_SHA.json
```

The profile and attestation-key file must be readable only by their owner, for
example mode `0600`; the key contains at least 32 random bytes and is never
committed. The runner first makes stable private snapshots of the wheel,
profile, key, and manifests. It then creates a temporary virtual environment,
installs only the snapshotted wheel and its declared dependencies without
inheriting system site-packages, removes source-checkout import paths, and
executes the installed `dsctl` console script. It refuses to overwrite evidence
and publishes a passing receipt atomically only after the complete pytest
process, all assertions, and project cleanup succeed.

If the API target is on a private network and the execution environment defines
an HTTP proxy, set `NO_PROXY`/`no_proxy` for that host before starting the gate.
The runner preserves those standard proxy-bypass variables; a proxy-generated
HTTP error is not valid cluster evidence.

The cluster manifest is non-secret and identifies the real deployment by an
immutable image digest:

```json
{
  "schema_version": 2,
  "ds_version": "3.4.2",
  "image_tag": "apache/dolphinscheduler-api:3.4.2",
  "image_digest": "apache/dolphinscheduler-api@sha256:<64-hex-digest>",
  "image_source": "swarm service inspect plus image RepoDigests",
  "image_observed_at": "YYYY-MM-DDTHH:MM:SSZ",
  "api_target_hmac_sha256": "hmac-sha256:<64-hex-hmac>",
  "principal_hmac_sha256": "hmac-sha256:<64-hex-hmac>",
  "persona": "etl-developer"
}
```

The workflow fixture is provisioned outside the exact CLI phase because
workflow mutation is outside this gate's allowlist and evidence scope. Its
temporary manifest is also non-secret:

```json
{
  "schema_version": 2,
  "ds_version": "3.4.2",
  "image_tag": "apache/dolphinscheduler-api:3.4.2",
  "image_digest": "apache/dolphinscheduler-api@sha256:<64-hex-digest>",
  "exclusive": true,
  "provisioner": "dsmatrix-exact-read-state-projection/v2",
  "project": {"name": "temporary-project", "code": 123},
  "workflow": {
    "name": "temporary-workflow",
    "code": 456,
    "scheduled": true,
    "schedule_id": 789,
    "release_state": "OFFLINE",
    "editable_task": {
      "name": "temporary-shell-task",
      "code": 987,
      "type": "SHELL"
    }
  }
}
```

All fields shown in both schema-2 manifests are required gate inputs.
`exclusive` and `scheduled` must be the JSON boolean `true`, `release_state`
must be `OFFLINE`, `editable_task.type` must be `SHELL`, and the fixture image
tag/digest must exactly match the cluster manifest. HMAC values use the literal
`hmac-sha256:` prefix followed by 64 lowercase hexadecimal characters.

The external provisioner must use the same clean-environment rule as the gate.
Before mutation it verifies the selected version, keyed target identity, and
keyed current-user identity. After provisioning it leaves the workflow
`OFFLINE`, then uses the exact `3.4.2` profile to read back the project,
workflow, attached schedule, and editable task; only then may it write the
fixture manifest. A compatibility authoring profile may be used as a temporary
bootstrap implementation, but it is not exact-contract evidence and must be
named honestly in `provisioner`.

When the fixture already exists in the resident version matrix,
`tools/project_exact_profile_matrix_fixture.py` may convert the matrix's
REST-verified schema-1 cluster/fixture manifests and private ready-state file
into the strict schema-2 manifests above. This helper exists so an active warm
lease can be reused without changing the matrix repository or its resident
topology. It also requires an owner-private image-inspection file with this
exact historical schema-1 shape:

```json
{
  "schema_version": 1,
  "image_ref": "apache/dolphinscheduler-api:3.4.2",
  "image_id": "sha256:<64-hex-image-id>",
  "repo_digests": [
    "apache/dolphinscheduler-api@sha256:<64-hex-repository-digest>"
  ],
  "selected_repo_digest": "apache/dolphinscheduler-api@sha256:<64-hex-repository-digest>"
}
```

The current collector may instead supply schema 2 with the same four image
identity fields plus the exact field `"kind": "registry-digest/v1"`. The
projector accepts only these two closed envelopes, then applies the same image
provenance validation to both; unknown fields, kinds, or schema versions fail
closed.

The projector rejects symbolic links, group/other-readable inputs, mismatched
image IDs or tags, a selected digest absent from `repo_digests`, duplicate
entries, and multiple matching digests for the service repository. It has no
bare digest option. It publishes both output manifests as owner-only files and
labels the fixture `dsmatrix-exact-read-state-projection/v2`. The current gate
and schema-7 evidence validator require that exact provisioner, so an older
projection/v1 receipt is historical rather than current promotion evidence.
The first local campaign attempt used projection/v1 and was withdrawn before
commit after review found that its operator-supplied RepoDigest was not bound
to the inspected image ID. Its receipt remains private audit material and is
not tracked. The r2 campaign below used projection/v2 and the strict inspection
record described above.
The projector neither calls the candidate wheel nor observes its HTTP
requests. The projection is therefore fixture-input preparation, not candidate
wire evidence; the installed-wheel gate must still perform every exact read,
mutation, readback, and restoration assertion itself.

The provisioner must allocate this fixture exclusively to one gate run and
remove or release it only after the gate has reconciled the recorded baseline.
Provisioning the object is not enough: the gate's preflight re-reads task
detail, workflow DAG, schedule, and workflow release state and snapshots the
values needed to detect interference and prove restoration. Shared demo
workflows, workflows left `ONLINE`, and fixtures open for manual UI editing
are invalid inputs.

Neither manifest nor the committed receipt contains credentials. The receipt
also omits fixture names/codes, raw arguments, stdout/stderr, API URLs, and
tokens. Keyed HMAC identities bind the manifest to the profile and authenticated
user without publishing dictionary-reversible hashes; the HMAC key remains a
private gate input. The image observation must come from the deployment control
plane used for the run and be no more than 15 minutes old. It is a fresh
point-in-time preflight observation. The `version` command confirms the selected
client contract/profile; the control-plane observation remains the server release
evidence. The persona must be `etl-developer`, and
`doctor` must report DS-native `userType=GENERAL_USER`; an admin token cannot
satisfy this gate. The receipt records only allowlisted identities and redacted
action shapes. The current promotion checker builds its expected contract from
the current generated manifest and requires a matching current receipt. It
therefore does not accept the old schema-v4 `0.4.0` receipt as current gate
evidence. A current schema-7 receipt must also bind the exact semantic profile,
manifest, domain recipes, and claimed evidence levels so a new `live_smoke`
claim cannot silently outrun the evidence.

The historical evidence writer emitted schema v4 for the expanded
task-definition gate, and schema v5 changed only its installed adapter roles.
Schema v6 added the four profile fingerprints, complete generated-manifest
identity, and canonical `gate_bundle` while retaining implementation-shaped
adapter fields. The current writer emits schema v7: it keeps the semantic
profile fingerprints, manifest, and gate bundle but removes adapter class names
and full-adapter presence from the receipt contract. The bundle fixes the
ordered 15 actions, requires every installed profile verification to be
`live_smoke`, and binds each action to its semantic operation, accepted build
status, and four decision fingerprints. Its SHA-256 digest covers actions,
verification values, and recipes but not the digest field itself. Its cleanup
attestation records the task mutation, successful reconciliation, exact command
restoration, post-restore DAG/detail version consistency, preserved non-owned
task fields and topology, and preserved `OFFLINE` workflow state. The validator
continues to accept schema 3/4/5/6 only against their historical contracts so
old evidence remains auditable. The independent promotion checker, default
quality gate, and Python 3.11 CI lane require schema 7 once the complete
15-action metadata claim exists. Schema version alone is not freshness: the
receipt must also match the current profile, recipes, manifest, package version,
wheel filename, and full wheel SHA-256.

The retained [August 10 schema-6 receipt](live-evidence/history/external-shell/3.4.2/2026-08-10-f328d3e2d6ba.json)
and [August 12 schema-6 receipt](live-evidence/external-shell/3.4.2/2026-08-12-e8eacee57af9.json)
record their exact wheel identities, fixed 15-action `live_smoke` bundles,
operation traces and fixture-restoration attestations. They are historical
evidence and cannot satisfy the current schema-7 checker. Neither receipt
attests the deterministic prepare/interfere/apply stale-plan negative required
for `task.update=live_full`, sets `profile.tested`, or promotes a support level.

Task update is non-idempotent from the gate operator's perspective. If the gate
fails after the mutation boundary, first inspect task detail, the workflow DAG,
and the fixture's release state and compare them with the provisioned baseline.
Do not blindly rerun the gate: a response or readback failure can mean that the
mutation succeeded, and an immediate retry can overwrite a concurrent change.
The task mutation wire disables transport retry, and its immediate readback has
no eventual-consistency retry loop; ambiguous transport state is reconciled,
never resolved by resending the PUT. Reconcile or restore the exclusive
fixture, then start a fresh run. The fresh-read guard narrows but cannot remove
the server API's TOCTOU window between its final read and the PUT, which is why
exclusive fixture ownership remains mandatory.

## Test Personas

Live testing should use at least two personas.

### `admin-bootstrap`

Use the cluster-provided admin token only for:

- initial connectivity verification
- creating or validating the tenant, user, and access token used by the test
  suite
- explicitly admin-only governance tests such as user, tenant, token, and
  permission administration

Do not use the admin token for the main workflow, schedule, and runtime test
paths. That would hide real permission problems and produce a false sense of
coverage.

### `etl-developer`

Use a normal non-admin user token for the main live suite:

- project lifecycle
- workflow authoring and reads
- workflow run and instance inspection
- task-instance inspection and control
- schedule lifecycle and schedule-triggered runtime behavior
- resource, datasource, namespace, and other day-to-day ETL operations as
  allowed by cluster policy

This persona should reflect the real operator or ETL developer experience.

## Bootstrap Flow

Keep separate env files for the two personas.

Example `live-admin.env`:

```dotenv
DS_VERSION=3.4.1
DS_API_URL=http://cluster.example/dolphinscheduler
DS_API_TOKEN=admin-token
```

Example `live-etl.env`:

```dotenv
DS_VERSION=3.4.1
DS_API_URL=http://cluster.example/dolphinscheduler
DS_API_TOKEN=etl-user-token
```

The env files passed to `dsctl --env-file` should contain only runtime profile
settings. Live-harness metadata belongs in the process environment or in the
source file consumed by the harness before it materializes a clean temporary
profile.

The CLI treats an explicit env file as an isolated profile and ignores inherited
CLI profile keys (`DS_VERSION`, `DS_API_URL`, `DS_API_TOKEN`,
`DS_API_RETRY_ATTEMPTS`, and `DS_API_RETRY_BACKOFF_MS`). The live harness must
pass a complete materialized profile containing the intended exact
`DS_VERSION`. Keep harness-only `DS_LIVE_*` and `DSCTL_*` metadata separate so
their lifecycle remains independent from the CLI profile contract.

Example harness settings:

```bash
export DSCTL_RUN_LIVE_TESTS=1
export DSCTL_RUN_LIVE_ADMIN_TESTS=1
export DS_LIVE_ADMIN_ENV_FILE=/path/to/local/live-admin.env
export DS_LIVE_TENANT_CODE=dsctl-live
export DS_LIVE_MONITOR_DATABASE_TYPE=POSTGRE_SQL
```

`DS_LIVE_TENANT_CODE` is live-harness metadata only. The CLI runtime itself does
not read tenant defaults from profile config.

`DS_LIVE_MONITOR_DATABASE_TYPE` supplies the independently known metadata
database type for the monitor scenario; use the native value for that cluster.
For PostgreSQL, DS through 3.2.0 returns `POSTGRESQL`; DS 3.2.1 and later returns
`POSTGRE_SQL`. Missing audit history is valid on a fresh
fixture: an empty page must still satisfy the pagination contract.

To exercise an installed candidate with the existing surface tests, set
`DSCTL_LIVE_EXECUTABLE=/path/to/candidate/bin/dsctl`. An explicit executable
argument takes precedence; a configured missing executable fails without
falling back to source. Without either selection, the harness retains its
source-checkout mode. The campaign must independently verify the installed
package against its fixed wheel and bind the test sources to the result.

An existing `DS_LIVE_ETL_ENV_FILE` is sufficient for ETL-only scenarios.
Administrator fixtures are requested only by tests that need them or when the
harness must create an ETL identity. Supplying an ETL profile does not authorize
administrator tests or provide a managed user for grant/revoke scenarios.

Optional datasource lifecycle coverage requires a disposable external
datasource target. Configure it through process environment variables, not
checked-in env files:

```bash
export DS_LIVE_DATASOURCE_HOST=database.example
export DS_LIVE_DATASOURCE_PORT=3306
export DS_LIVE_DATASOURCE_DATABASE=dolphinscheduler
export DS_LIVE_DATASOURCE_USER=dsctl_live
export DS_LIVE_DATASOURCE_PASSWORD='...'
```

`DS_LIVE_DATASOURCE_TYPE` defaults to `MYSQL`. If any required datasource
setting is absent, the optional datasource lifecycle test is skipped.
Readback uses the selected exact profile's native type for the configured
backend. The scenario checks its unique name is absent before creation,
attempts one revoke even when a grant response is uncertain, and verifies
resource absence after deletion. A failed revoke remains a cleanup failure;
successful datasource deletion does not turn it into a successful revoke.

Recommended bootstrap flow:

1. Validate connectivity with the admin token.
2. Create or verify a dedicated live-test tenant.
3. Create or verify a dedicated non-admin live-test user.
4. Create an access token for that user through `dsctl`.
5. Persist the user token into a separate env file.
6. Run the main live suite with the non-admin env file.

Minimal bootstrap commands:

```bash
dsctl_clean() {
  env -u DS_VERSION -u DS_API_URL -u DS_API_TOKEN \
    -u DS_API_RETRY_ATTEMPTS -u DS_API_RETRY_BACKOFF_MS \
    dsctl "$@"
}

dsctl_clean --env-file live-admin.env doctor
dsctl_clean --env-file live-admin.env tenant create \
  --tenant-code dsctl-live \
  --queue default
dsctl_clean --env-file live-admin.env user create \
  --user-name dsctl_live_etl \
  --password 'change-me-now' \
  --email dsctl-live@example.com \
  --tenant dsctl-live \
  --state 1
dsctl_clean --env-file live-admin.env access-token create \
  --user dsctl_live_etl \
  --expire-time '2027-12-31 00:00:00'
```

The created token should then be written into `live-etl.env`. The main suite
should not fall back to the admin env file.

### Installed-Wheel Runtime And Schedule Journeys

The tracked runtime journey entry runs only through an explicitly installed
`dsctl` executable. Its env file must select the same exact DolphinScheduler
version named by the harness:

```bash
export DSCTL_RUN_LIVE_TESTS=1
export DSCTL_JOURNEY_EXECUTABLE=/path/to/candidate-venv/bin/dsctl
export DSCTL_JOURNEY_ENV_FILE=/path/to/live-etl.env
export DSCTL_JOURNEY_VERSION=3.4.1

python -m pytest -m live_runtime_journey \
  tests/live/test_runtime_journeys.py
```

The executable must share its `bin` directory with the candidate wheel's
Python interpreter. If all three `DSCTL_JOURNEY_*` settings are absent, these
tests skip so an ordinary live run does not opt into the long scenarios. A
partial configuration fails during setup rather than falling back to source.

Use a dedicated `etl-developer` token with a usable tenant and queue, and a
cluster with an active worker that can execute `SHELL` tasks. The tenant must
have a corresponding worker OS user or working native account creation. On
`2.0.0`–`2.0.9`, native creation requires both tenant auto-creation and sudo to
be enabled; a REST tenant alone is insufficient. The runtime case
proves an initial success, an intentional task failure, the failed-watch error,
then instance repair and `recover-failed` or rerun back to success, including
task-log markers. The schedule case proves preview and confirmation behavior,
create/update/online/offline/delete, and a real cron-originated workflow
instance that reaches `SUCCESS`. Its start window is in the future so it also
respects releases that reject a past schedule start.

Both cases create uniquely owned projects and enter ownership-checked cleanup
on success or failure. Cleanup first disables schedules and quiesces active
instances before removing definitions and the project; a passing test requires
zero owned project residue. A cleanup failure fails the test and leaves its
local evidence for reconciliation, so do not blindly rerun an ambiguous
mutation. These zero-residue assertions cover DS resources. Deleting a DS
tenant does not remove worker OS accounts; the environment owner must
separately track and clean accounts created for the test, using their recorded
ownership and after their processes have finished.

These journeys are supplemental scenario evidence. They are independent of the
named `full_core/v1` conformance bundle and do not satisfy a release gate,
change per-action verification, or promote an exact profile or support tier.

## Current Stable Surface Matrix

The tables below define the shared scenario obligations, with `3.4.1` as the
historical baseline. Every exact profile uses the same standard for its
supported actions. Select scenarios from the exact contracts; neither a nearby
version nor the stable label supplies evidence for another coordinate.

The common user/project lifecycle updates `phone` and verifies project access
before grant, after grant and after revoke. It does not imply coverage of every
optional user field: time-zone updates require DS `3.0.0` or newer and need
separate evidence. DS `2.0.0` and `2.0.1` cannot run this complete lifecycle
because their exact profiles lack a safe single-project revoke operation.
The common worker-group lifecycle creates and renames the group. Description
fields exist from DS `3.1.0`, but exact `3.1.0` and `3.1.1` incorrectly reject
same-name updates; the independent positive description-update scenario applies
from `3.1.2` onward.
The independent scenarios in `tests/live/test_optional_governance_fields.py`
exercise those two fields through update and separate native readback. They
verify unique names are absent before creation, bind returned native IDs, and
check ownership again before cleanup. Their presence in the harness is not
live evidence. Similarly, `test_admin_audit_list_without_metadata_endpoints`
in `tests/live/test_admin_surfaces.py` covers the 19 profiles from `3.0.0`
through `3.2.1` whose audit-list action exists without filter-metadata endpoints;
it issues only the list query and checks the native page and returned rows.

Governance names derive from the complete unique run name. Resource and
user/project cleanup register successful creations before checking response
fields, attempt all registered removals and report cleanup failures. Resource
checks exhaust the directory pages and verify that owned paths have disappeared.
The project lifecycle first uses the administrator profile to prove both unique
project names absent. Its ordinary create, reads, update, readback and delete
still run as the ETL user. Failure cleanup uses a fresh administrator read,
accepts only the exact `resolved.project.id` or `resolved.project.code` native
identity with this run's name and description, and finishes with fresh gets plus
each unique name's native first-page search proving `total=0` and
`totalList=[]`. The unique names make that empty first page a complete absence
proof without relying on synthesized pagination coverage.
Permission or ownership failures are cleanup failures, not absence evidence.
An explicitly attempted deletion is not repeated in `finally` after an uncertain
response; reconcile it through a fresh read and retain any unresolved residue.
Project worker-group cleanup is a prerequisite to project deletion because
upstream project deletion does not remove those relation rows. The write
lifecycle uses the administrator identity for assignment and cleanup, requires
an exact empty list before assignment and after a denied ETL assignment, and
accepts only the exact intended single-group readback whose `projectCode`
matches the freshly owned project. Before deleting the
freshly located owned project, cleanup performs a fresh administrator relation
list. It clears a verified binding at most once and requires a separate fresh
empty list; a failed or uncertain clear with remaining rows retains the project.
An uncertain clear whose effect is freshly proven empty is not retried.

This full write lifecycle is limited to `3.3.1` through `3.4.3`. The `3.2.2`
controller exposes the same project worker-group endpoint, and non-empty
assignment plus reads exist, but its service rejects an empty `workerGroups`
list with `WORKER_GROUP_TO_PROJECT_IS_EMPTY`. It has no REST path to remove the
last relation, and project deletion does not cascade it. Tests must therefore
reject the `3.2.2` full write lifecycle before creating a binding; this is a
mutation-cleanup hole, not absence of the whole project worker-group surface.
An installed-wheel campaign must also verify bootstrap identities and cleanup;
pytest passing alone does not prove that an externally prepared fixture was
removed. Preserve access to the owned fixture when cleanup cannot be proven,
so its remaining resources can be reconciled before deleting its user or tenant.

Track action availability, scenario completion and environment prerequisites
separately. A command recorded during setup, polling or cleanup does not prove
its full lifecycle. Upstream absence is an explicit contract result; a missing
backend is unverified coverage, never a passing test. A composite scenario with
mixed availability needs to be split or adapted so that its supported actions
are not silently omitted. For example, project parameters have a common
`VARCHAR` lifecycle and a separate native data-type transition scenario.

Use boundary profiles to validate a reusable scenario before applying it to
all eligible exact versions. Keep independent per-version results and cleanup
proofs. A completed core campaign need not be repeated while filling other
scenario gaps against the same wheel. Product changes and the final release
candidate remain subject to the artifact-bound release requirements.

Task-group mutation scenarios have an additional cleanup gate. Although the
task-group surface exists on all 26 exact profiles from `3.0.0` onward, only
`3.2.0` through `3.4.3` delete a project's task groups and queue rows when the
project is deleted. There is no direct task-group delete REST operation. The
shared live lifecycle must therefore stop before its first mutation on
`3.0.0` through `3.1.9`. Coverage for those 17 profiles requires a disposable
one-run environment or an explicit, owned state-restoration plan; deleting the
project or bootstrap user and hiding the remaining row is not cleanup. On the
nine cascade-capable profiles, cleanup uses the creator ETL identity to require
both a global `task-group get` not-found result and a complete global list with
no matching name after project deletion. Project-scoped reads cannot prove this
absence once the project itself is gone.

### Local-Only Or Local-First Surfaces

These commands do not require live coverage unless they grow cluster I/O in the
future.

| Surface | Commands | Live required | Notes |
| --- | --- | --- | --- |
| meta | `version`, `context` | No | local metadata and effective target inspection |
| schema | `schema`, `capabilities` | No | static CLI self-description |
| enum | `enum names`, `enum list` | No | generated enum metadata |
| context | `context list|get|create|update|delete` | No | named connection references and project defaults |
| config | `config get|set|unset` | No | saved user default and effective-selection readback |
| lint | `lint workflow FILE` | No | local validation only |
| template | `template workflow|workflow-patch|workflow-instance-patch|params|environment|cluster|datasource|task` | No | local template rendering |

### Meta And Diagnostics

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| doctor | `doctor` | `admin-bootstrap` and `etl-developer` | Yes | healthy preflight and broken-auth or broken-config variant |
| task-type | `task-type list` | `etl-developer` | Yes | real task-type discovery and category projection |
| task-type | `task-type get|schema` | No profile needed | No | local authoring summary and schema contracts |
| monitor | `monitor health`, `monitor server`, `monitor database` | `admin-bootstrap` | Yes | remote health, server, and database payloads reachable and shaped |
| audit | `audit list`, `audit model-types`, `audit operation-types` | `admin-bootstrap` or delegated governance user | Yes | remote filter metadata plus list query with real payload shape |

### Governance Surfaces

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| environment | `environment list|get|create|update|delete` | delegated governance user | Yes | CRUD round-trip plus not-found or validation case |
| cluster | `cluster list|get|create|update|delete` | delegated governance user | Yes | CRUD round-trip plus delete cleanup |
| datasource | `datasource list|get|create|update|delete|test` | delegated governance user | Yes | CRUD round-trip and real connectivity test where the capability exists |
| namespace | `namespace list|get|available|create|delete` | delegated governance user | Yes | list and availability plus create/delete round-trip |
| resource | `resource list|view|upload|create|mkdir|download|delete` | `etl-developer` | Yes | file and directory lifecycle with content round-trip |
| queue | `queue list|get|create|update|delete` | `admin-bootstrap` or delegated governance user | Yes | CRUD round-trip plus permission boundary |
| worker-group | `worker-group list|get|create|update|delete` | `admin-bootstrap` or delegated governance user | Yes | CRUD round-trip plus selector correctness |
| alert-plugin | `alert-plugin list|get|definition list|schema|create|update|delete|test` | `admin-bootstrap` or delegated governance user | Yes | definition/schema/read paths plus create/update/delete and plugin test where installed |
| alert-group | `alert-group list|get|create|update|delete` | delegated governance user | Yes | CRUD round-trip and referenceable group payload shape |
| tenant | `tenant list|get|create|update|delete` | `admin-bootstrap` | Yes | CRUD round-trip plus non-admin denial case |
| user | `user list|get|create|update|delete|grant project|datasource|namespace|revoke project|datasource|namespace` | `admin-bootstrap` | Yes | CRUD plus grant/revoke effect and non-admin denial case |
| access-token | `access-token list|get|create|update|delete|generate` | `admin-bootstrap` for bootstrap, `etl-developer` for self-use verification | Yes | token lifecycle plus created token actually authenticates against the cluster |

### Project Surfaces

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| project | `project list|get|create|update|delete` | `etl-developer` | Yes | CRUD round-trip and context-independent selection |
| project-parameter | `project-parameter list|get|create|update|delete` | `etl-developer` | Yes | CRUD round-trip inside one live project |
| project-preference | `project-preference get|update|enable|disable` | `etl-developer` | Yes | get/update plus enable/disable state reflection |
| project-worker-group | `project-worker-group list|set|clear` | project owner or delegated governance user | Yes | assignment effect visible on read-back |

### Design And Runtime Surfaces

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| workflow | `workflow list|get|describe|digest|create|edit|online|offline|run|run-task|backfill` | `etl-developer` | Yes | authoring, read-back, release-state transition, run, task-scoped run, backfill dry-run, and dry-run consistency |
| task | `task list|get|update` | `etl-developer` | Yes | live task projection and safe update reflected by later reads |
| schedule | `schedule list|get|preview|explain|create|update|delete|online|offline` | `etl-developer` | Yes | preview and explain aligned with create/update, both no-environment and explicit-environment adapter paths, and online schedules producing real workflow instances |
| workflow-instance | `workflow-instance list|get|parent|digest|edit|watch|stop|rerun|recover-failed|execute-task` | `etl-developer` | Yes | explicit/current project scope, instance lifecycle transitions, finished-instance DAG edit semantics, runtime control, and parent/sub-workflow relation reads under real runtime conditions |
| task-instance | `task-instance list|get|watch|sub-workflow|log|force-success|savepoint|stop` | `etl-developer` | Yes | explicit/current project scope for every operation except the native ID-addressed `log`, plus child relation reads and runtime controls |

### Notes On Capability-Gated Resources

Some resources still require live coverage, but only in a compatible cluster:

- datasource connection tests need a reachable backend
- alert-plugin tests need installed plugin instances or plugin backends
- namespace, resource, environment, and cluster operations may depend on deployment
  topology and storage configuration

Alert configuration CRUD and test-send use separate test nodes. The CRUD node
does not send alerts. To exercise the test-send endpoint, explicitly set
`DSCTL_RUN_LIVE_ALERT_TESTS=1` and `DS_LIVE_ALERT_TEST_INSTANCE` to a dedicated,
fully configured instance on a version supporting `alert-plugin.test`, with an
approved test destination. That node requires DS to report `tested: true`;
end-to-end delivery additionally needs evidence from the receiving backend.
Without this opt-in, test-send remains unexecuted and is not covered by a CRUD
receipt. The test does not modify or delete the configured instance.

The rule is not “skip forever”. The rule is:

- every cluster-interacting command needs live coverage
- capability-gated commands may run in a dedicated compatible environment
- any skip must be explicit and explained by missing cluster capability

## Current Coverage Snapshot

The retained development candidate corpus records all 37 profiles with
four-action exact-read schema-2 receipts and 18-action `full_core/v1` receipts,
plus the separate 15-action `external-shell/v1` schema-7 gate on exact `3.4.2`.
These receipts bind the same immutable candidate wheel.
They predate the subsequent Typer and DataX runtime fixes and do not attest
current source. A new release candidate needs its own artifact-bound acceptance.
Historical schema-1 and schema-6 evidence also remains valid only for its
recorded artifacts; passing a bounded gate does not promote an entire profile.

Broader development scenarios were verified on earlier installed candidates.
They are separate from that release corpus and must retain their
original artifact, scenario, revision and cleanup bindings:

| Scenario | Verified development scope | Boundary |
| --- | --- | --- |
| Runtime controls | 37 versions, 134 applicable scenarios | 131 selected original pytest passes plus three independent proof supplements; original failed assertions remain preserved |
| Workflow runtime, ordinary scheduling and core lifecycles | All 37 versions | Common lifecycle coverage does not attest every optional field or permission path |
| Schedule environments | 36 applicable versions | Seven prove schedule inheritance; 29 prove positive schedule-environment rejection and explicit task environments; `1.3.9` retains ordinary scheduling evidence |
| Lineage reads | 36 applicable versions; reverse dependencies on seven | No claim of generic orphan-repair capability |
| Audit list without filter metadata | 19 profiles from `3.0.0` through `3.2.1` | Read-only list coverage, separate from optional governance fields |
| Optional governance fields | 43 positive cases on 26 profiles | Two exact `3.1.0`/`3.1.1` worker-group same-name update limitations remain; not 45 passing cases |
| Project-parameter type update | Six applicable profiles | VARCHAR-to-INT update preserves native identity and is read back before cleanup |
| Stale prepared task update | Exact `3.4.2` | Same-object conflict after interference, zero stale PUT calls and fixture restoration; not generic atomic CAS |
| Task-group queue controls | Nine cleanup-safe profiles | Waiting identity, priority readback, full capacity and real execution overlap after force-start |
| PostgreSQL datasource lifecycle | All 37 profiles | Real connections, grants/revocation and cleanup; no claim for other backends or task-worker SQL execution |
| Resource file lifecycle | All 37 profiles | Byte-preserving upload/download and cleanup; mixed-candidate evidence |
| Alert configuration and namespace reads | 36 and 26 profiles respectively | Configuration and reads do not establish sending or namespace mutations |
| Actual alert delivery | Eight applicable profiles | Six native CLI successes and two delivered/native-result-limited cases |

Exact `3.2.1` alert delivery returns native `110014` because of the upstream
success-check inversion despite receiver confirmation. Exact `3.3.1` also
returned `110014` after actual delivery; that historical test supplied 2 ms
rather than the intended 2 s. This does not establish an intrinsic version
defect or prove timeout causality. Delivery to the owned receiver on exact
`3.4.1` is verified. Alert objects, receivers and temporary network settings
were cleaned up or restored.

Independent cleanup evidence accompanies these scenarios, including owned DS
objects, external fixtures, temporary credentials and applicable test OS
accounts. Corrected checker proofs and cleanup supplements do not rewrite
original failed receipts or transfer evidence to a newer wheel. Detailed local
run logs, candidate nicknames, timing and recovery transcripts are not part of
the public coverage contract.

Unclosed supported scope includes legacy task-group and queue mutations where
REST deletion or cascade cleanup is absent, and exact `3.2.2` project-worker-group
assignment cleanup. Positive namespace mutation requires a working K8S backend
and remains outside the current acceptance scope; a missing-backend probe does
not prove CRUD. Keep these limits explicit. The
[release closeout criteria](release.md#closing-development-acceptance) define
which missing evidence blocks each claim; the shared scenario obligations above
remain in force.

The current live suite covers these black-box CLI files:

- `tests/live/test_preflight.py`
- `tests/live/test_admin_surfaces.py`
- `tests/live/test_governance_surfaces.py`
- `tests/live/test_governance_optional_surfaces.py`
- `tests/live/test_project_surfaces.py`
- `tests/live/test_runtime_surfaces.py`
- `tests/live/test_runtime_control_surfaces.py`
- `tests/live/test_schedule_surfaces.py`
- `tests/live/test_runtime_journeys.py`
- `tests/live/test_workflow_lineage_surfaces.py`
- `tests/live/test_workflow_runtime_surfaces.py`

The tenant ETL denial case first reads the complete queue inventory with the
ETL identity and uses an administrator-resolved numeric queue ID. A denied
queue read or an invisible queue proves CLI preflight access blocking; it does
not prove that the tenant write endpoint was reached. A visible queue requires
`permission_denied` from the attempted tenant create. Worker-group CRUD uses a
live address discovered through `monitor server worker`; description remains
a separate version-specific facet. User association checks use `tenantId`,
with `tenantCode` checked only when the native response supplies it.

The user-create denial probe uses the ETL account's own tenant and first checks
its visibility through a complete tenant listing. If that tenant is hidden,
the expected `not_found` must identify that exact tenant; this verifies the
CLI prerequisite boundary, not the upstream user-create permission check.
When the tenant is visible, the attempted creation must return
`permission_denied`. These outcomes are predicted from fresh visibility
evidence, not accepted interchangeably.

The historical `3.4.1` coverage inventory includes:

- preflight: `doctor`, `monitor health`, ETL token bootstrap, and non-admin
  denial
- admin read surfaces: `monitor server`, `monitor database`, `audit list`,
  `audit model-types`, `audit operation-types`
- admin governance: `access-token`, `queue`, `worker-group`, `tenant`, `user`
  plus project grant effects
- optional governance: `datasource`, `alert-plugin`, `alert-group`, and
  namespace capability/error paths
- project surfaces: `task-type`, `project`, `project-parameter`,
  `project-preference`, `project-worker-group`
- runtime-adjacent governance: `cluster`, `environment`, `resource`
- workflow runtime surfaces: `workflow`, `task`, `workflow-instance`,
  `task-instance`, parent/sub-workflow relation reads, finished-instance DAG
  update with and without definition sync, schedule-triggered runtime,
  workflow lineage, and task-group queue controls

This inventory does not establish that the current wheel or edited test files
have passed those scenarios. Acceptance receipts bind the exact version,
installed wheel, test source and assertions. Reuse completed scenarios while
filling gaps, preserving their original bindings; final candidate acceptance
must still meet its own release obligations. Test counts alone are not the
coverage contract.

## Exact behavior and environment prerequisites

These source-reviewed behaviors and observed environment failure modes guide
scenario design. Check prerequisites on the selected deployment; a past
cluster observation does not describe every installation of that release.

- `resource list` must send `searchVal=""` when no search term is provided.
  Treat this as an upstream 3.4.1 contract quirk, not a CLI feature choice.
- `resource view` is not reliable through the DS view endpoint because the
  upstream controller misuses the `limit` parameter. The adapter now reads the
  download endpoint and applies the line window client-side.
- `task-instance list` in DS 3.4.1 is backed by the project-scoped
  `GET /projects/{projectCode}/task-instances` paging query. The CLI narrows
  the common per-run inspection path by sending `workflowInstanceId`, and it
  uses the same path for broader project-scoped runtime triage filters.
  Workflow-definition filtering is intentionally not exposed here because the
  upstream BATCH query does not reliably apply `workflowDefinitionName`.
- `workflow describe` returns one root sentinel relation with
  `preTaskCode=0`. That row is part of the DS DAG encoding and should not be
  confused with a user-authored dependency edge.
- `project delete` can briefly return DS result code `10137` immediately after
  `workflow delete`. Treat this as a short eventual-consistency window and
  retry cleanup before declaring failure.
- If a newly created workflow instance reports `SUCCESS` but produces zero
  `task-instance` rows, inspect the compiled `taskDefinitionJson` before
  blaming the runtime read paths. In DS 3.4.1, `taskDefinitionJson.flag` must
  be emitted as the DS-native enabled state (`YES`). Emitting the disabled
  state (`NO`) yields a valid-looking workflow definition whose DAG nodes are
  marked forbidden, so the workflow completes without creating task instances.
- generated boolean fields such as Java `isDirectory` map to DS wire names like
  `directory`. The generator must preserve the Python attribute while emitting
  the correct alias.
- `PageInfo.currentPage` is nullable in real DS responses and must stay
  nullable in generated contracts.
- `cluster update` can initialize a Kubernetes client even when only cluster
  metadata changes. Its positive lifecycle test requires a successful update
  and readback; native `120024` is a failed update, not an alternative passing
  outcome. Diagnose the server exception and configuration when it occurs.
  Negative error-contract tests remain separate from CRUD acceptance.
- schedule create/preview/explain must use Quartz-style cron expressions in the
  current 3.4.1 cluster. The live suite uses forms such as `0 * * * * ?`
  rather than five-field cron strings.
- schedule create needs the workflow definition to be `ONLINE`. The DS 3.4.1
  v2 create/update contracts also require a valid nonzero `environmentCode`,
  but the project-scoped legacy contracts support DS-native schedules without
  an environment. Live coverage therefore keeps both adapter paths: omitted
  environment and an explicit positive environment code.
  An explicit code `0` is a separate selection from omission: it bypasses
  project environment preferences. Check that unrelated updates preserve the
  selected environment, and that an explicit `0` clears it. A positive
  environment fixture should export only a unique shell marker, without
  replacing host-specific settings such as `JAVA_HOME`. Its runtime proof
  requires a scheduler-originated instance, nonempty `scheduleTime`, terminal
  success and the marker in its task log. Prove schedules absent while their
  owned project is still visible, then require a complete scoped instance list
  with no active executions before deleting definitions and the project.
  Delete the environment after project cleanup has removed residual task
  definitions that may still reference it on older releases. Where the profile
  exposes tenant selection, use the same `default` runtime tenant as the
  schedule journey; the bootstrap tenant owns API fixtures and need not have
  a worker OS account. The selected runtime tenant must be runnable on the worker.
  Apply positive schedule inheritance assertions only where the exact runtime
  profile supports them (`3.2.2` onward). Earlier profiles must reject positive
  selections before mutation; verify the alternative by setting an explicit
  task environment and observing its marker in a real scheduled run. Keep
  those assertions distinct from schedule inheritance and retain original
  failed receipts when upstream source explains a capability boundary.
- `alert-plugin definition list` discovers supported plugin definitions, while
  `alert-plugin schema PLUGIN` fetches the full DS UI parameter form for one
  definition when the upstream detail endpoint exposes it.
- `alert-plugin test` against the current Script plugin returns DS result code
  `110014` when no executable script backend is configured. This is a valid
  cluster capability failure, not a transport error.
- positive `namespace create/delete` coverage still depends on compatible
  Kubernetes integration. The current shared cluster returns DS result code
  `1300006` for namespace creation because no usable K8s namespace backend is
  configured. The isolated missing-backend probe registers its cluster cleanup
  before inspecting the creation response and checks namespace and cluster
  absence afterward. Missing-resource checks run separately so releases with
  namespace endpoints but no cluster CRUD can still exercise them. Neither
  negative scenario establishes positive Kubernetes CRUD coverage.
- workflow-instance stop and task-instance stop/savepoint are request surfaces,
  not guaranteed immediate state transitions. Check the returned target
  identity, then observe the actual task and workflow. For the single running
  SHELL task scenario, require task `KILL`, or accept task `FAILURE` only when
  the same task was first observed running, its stop request was accepted, and
  its fully scanned CLI task-log tail has one native executor line with both
  `exitStatusCode` and `processExitValue` equal to `143` on `3.1.0`–`3.2.2` or `130`
  on `3.3.1`–`3.4.2`. For task `KILL`, the reviewed workflow terminal state is
  `FAILURE` on `3.1.0`–`3.2.2` and `STOP` on `3.3.1`–`3.4.3`; for a proven
  signal-exit task `FAILURE`, require workflow `FAILURE`. An accepted savepoint
  request on SHELL proves the request surface, not creation of a checkpoint
  artifact. A timeout or an unproven terminal state fails the scenario.
  The independent `workflow-instance stop` scenario uses a distinct
  `workflow-stop-task`, requires that same task to be running before the
  accepted workflow stop, then proves its final task state. A task `KILL`
  requires workflow `STOP`. A signal-proven task `FAILURE` still leads to
  workflow `STOP` on the `3.1.x`/`3.2.x` native `READY_STOP` path, but to
  workflow `FAILURE` on `3.3.1`–`3.4.2`. Apply the same exact-version signal
  log proof before accepting `FAILURE`; do not accept an unrelated failure.
  Check native worker process-control dependencies before creating fixtures.
  In particular, the DS `ProcessUtils.getPidsStr` kill path requires `pstree`
  (provided by `psmisc` on Debian-based images). A worker can accept the kill
  RPC yet fail to enumerate child processes when this utility is missing.
  Record and correct that environment prerequisite separately from CLI
  behavior; a successful REST response does not prove the task stopped.
- recovery and rerun are separate scenarios on all 37 profiles; task
  force-success applies to 36. Execute-task has a REST operation on nine
  (`3.2.0` onward), but `3.3.1`, `3.3.2`, `3.4.0` and `3.4.1` have no master
  `EXECUTE_TASK` handler. Keep that runtime gap separate from endpoint presence;
  do not record the four skipped positive scenarios as passing or upstream-absent.
  Positive execution uses an independent SHELL task with `scope=self` on the
  other five profiles. DS `3.2.0`/`3.2.1` can reuse the original task ID, so
  require the `EXECUTE_TASK` workflow's increased `runTimes`, the same task's
  strictly later start and end times, and both task and workflow success.
  `3.2.2` and `3.4.2`/`3.4.3` require a new task ID and both successes.
  Only `3.2.0`–`3.2.2` increments `runTimes`;
  the `3.4.2`/`3.4.3` handler reuses the counter. Dependency-subgraph execution
  is outside this bounded scenario.
  Do not hide an available action behind an unavailable composite scenario.
  Observe recovery/rerun using the prior `runTimes` baseline and require a newer round;
  an earlier terminal result cannot prove replay completion. After an uncertain
  dispatch, cleanup must establish quiescence or retain the owned fixture for
  reconciliation. Reuse the fresh-workflow instance resolver on older executors
  that accept a run without returning instance IDs.
- `workflow-instance edit` in DS 3.4.1 is a finished-instance save path, not
  a live-definition edit path. The server requires final-state instances with
  usable `dagData`; `syncDefine=false` keeps the current workflow definition
  unchanged, while `syncDefine=true` also persists the saved DAG back onto the
  definition and bumps its version.
- positive `task-group queue force-start/set-priority` coverage requires real
  tasks bound with `taskGroupId/taskGroupPriority`. The CLI workflow YAML now
  supports that path directly, and the live suite uses it instead of any
  database shortcut. Resolve the fresh workflow execution before selecting a
  waiting row owned by the exact project, workflow and task group. Re-list to
  prove the priority was stored; the mutation echo is insufficient. Prove
  force-start by overlapping task execution while a one-slot group was full,
  rather than by request acceptance or eventual workflow success. Native queue
  rows may be retained or deleted after force-start, depending on the release.
  The scenario allows cleanup time for both tasks to finish serially if an
  earlier assertion fails; unknown execution state or group ownership retains
  the fixture for reconciliation. Its portable checks do not replace live
  acceptance on the exact versions with safe REST project cleanup.

## What A Professional Live Suite Should Cover

The upstream DS tests are a strong starting point, but a professional CLI live
suite should cover more than basic create/list/delete flows.

### 1. Connectivity and Authentication

- `doctor` and `monitor health`
- invalid token behavior
- expired token behavior
- permission denied behavior for non-admin users

### 2. Governance Bootstrap and Permission Boundaries

- tenant, user, and access-token lifecycle under the admin persona
- grant and revoke flows where the cluster policy allows them
- negative tests proving the ETL persona cannot perform admin-only mutations

### 3. Definition Lifecycle

- project create/get/list/update/delete
- workflow create, get, describe, digest, online, offline
- task read and safe task update paths
- resource upload/view/download/delete

### 4. Runtime Lifecycle

- workflow run and backfill dry-run
- workflow-instance list/get/digest/edit/watch
- task-instance list/get/log
- stop, rerun, recover-failed, execute-task

### 5. Schedule Semantics

- create, preview, explain, online, offline, update, delete
- schedule-generated workflow instances actually appear
- next-fire-time or preview output matches server behavior closely enough for
  safe operator use
- high-frequency schedule warning and confirmation paths

### 6. Negative and Error Paths

- not-found selectors
- offline-only and online-only lifecycle violations
- delete while still referenced or still online
- malformed workflow spec rejected by lint or create
- empty result pages and out-of-range pagination

### 7. Recovery and State Transitions

- paused, stopped, rerun, and recover-failed runtime transitions
- sub-workflow behavior
- schedule on/off toggling under existing runtime load
- behavior after partially failed mutations

### 8. Real Data and Encoding Edges

- Unicode names and descriptions
- time zone handling
- date/time input round-tripping
- large payloads and large page sizes

### 9. Observability and Diagnostics

- command warnings are useful and specific
- `resolved` metadata is sufficient to explain what the CLI actually selected
- error `suggestion` values point to concrete next actions

### 10. Cleanup Robustness

- resources created during a failing run are still discoverable and removable
- cleanup remains idempotent
- orphaned resources are reported, not silently ignored

## Recommended Suite Tiers

A practical live program should be tiered.

### Tier 0: Preflight

Run on every live invocation.

- `doctor`
- `monitor health`
- current user identity and permission sanity checks
- unique run prefix generation

### Tier 1: Fast Smoke

Run in manual verification and CI against a stable shared cluster.

- project lifecycle
- workflow create and get
- workflow run and successful completion
- workflow backfill dry-run
- workflow-instance and task-instance read paths

### Tier 2: Developer Lifecycle

Run manually and before releases.

- context-aware selection
- workflow edit
- schedule preview, explain, create, online, offline
- resource and datasource basics when available

### Tier 3: Runtime Semantics

Run nightly or before significant runtime changes.

- rerun
- recover-failed
- execute-task
- stop and watch semantics
- sub-workflow and dependency-sensitive flows

### Tier 4: Governance and Admin

Run separately because it requires elevated credentials.

- user, tenant, token administration
- grants and revokes
- admin-only negative tests

### Tier 5: Optional Capability Suites

Run only when the cluster provides the dependency.

- datasource-specific suites
- alert plugin suites
- namespace and resource integrations
- task-plugin-specific workflow suites

## Execution Model

Recommended markers:

- `live`
- `live_admin`
- `live_developer`
- `live_exact_read`
- `live_exact_profile`
- `destructive`
- `slow`
- `optional_capability`

Recommended run naming:

- one unique prefix per run, such as `dsctl-live-20260412-153000-ab12`
- use that prefix in tenant codes, project names, workflow names, and resource
  paths

Recommended cleanup policy:

- register cleanup as soon as a resource is created
- cleanup should run in reverse dependency order
- cleanup failures should fail the suite and print exact leftover identifiers

## Execution Policy

Define the target, personas, permitted mutations, owned fixtures and cleanup
requirements before a campaign. Keep credentials and raw traces private;
record reusable results through the artifact-bound receipt contracts above.

Stop dispatching affected scenarios if fixture ownership or cleanup is uncertain,
required credentials or backends are unavailable, or the failure exposes an
unresolved product or upstream-semantics decision. Diagnose the existing state
before proceeding. Resume only applicable unfinished scenarios once their
prerequisites and ownership are established, preserving failed attempts and
previously verified evidence under their original artifact identities.

## Failure Analysis And Repair Principles

When a live test fails, do not jump straight to patching the command that
surfaced the failure. Classify the failure first.

### Step 1: Reproduce And Narrow

- inspect the original command result and trace; reproduce read-only failures
  with the same selected target
- for uncertain writes, establish the actual outcome and fixture state before
  deciding whether another mutation is appropriate
- inspect `doctor` and health output
- confirm whether the failure is deterministic
- confirm whether the failure happens only for one persona or both

### Step 2: Classify The Failure

Typical buckets:

- cluster health or environment issue
- permission model mismatch
- generator contract mismatch
- adapter request mapping bug
- service-layer selection, validation, pagination, or shaping bug
- runtime polling or state-mapping bug
- output contract or warning-quality issue
- bad test assumption
- real DS upstream bug or cluster-specific constraint

### Step 3: Fix At The Lowest Correct Layer

- wrong DS request or response shape: fix the generator first
- version-specific contract mismatch: fix the adapter
- selection or lifecycle logic error: fix the service layer
- bad suggestion, warning, or envelope detail: fix output shaping
- bootstrap or persona misuse: fix the live harness or docs

Do not paper over a lower-layer bug with a higher-layer workaround unless that
workaround is the explicit product decision.

### Step 4: Preserve DS Semantics

Do not “fix” a failing live test by inventing a CLI translation that hides the
actual DS behavior. If the upstream object is `OFFLINE`, `SERIAL_WAIT`, or a
DS-native enum or field shape, the CLI should stay honest about that.

If the failure is an unhelpful raw DS result error, classify it before adding a
new translation:

- use `extract_ds_api_error_inventory.py` to confirm whether the code is a
  stable upstream status or only a generic fallback
- use `extract_dsctl_error_translation_matrix.py` to see whether the same code
  is already translated elsewhere
- use `audit_dsctl_error_translation.py` to find the exact service boundary
  where translation is missing
- update `check_error_translation_governance.py` allowlist only for reviewed
  raw cases that should intentionally stay raw

Do not add allowlist entries just to silence a live failure. The allowlist is
for reviewed exceptions, not for unknown behavior.

### Step 5: Add Regression Coverage

After fixing a live failure:

- add or tighten a non-live regression test whenever the behavior can be
  modeled offline
- keep the live case if the bug depended on real permissions, timing,
  scheduling, plugin availability, or cluster topology

### Step 6: Update The Docs Or Capability Surface

If the failure reveals a real environment constraint:

- document it in the live-testing notes
- expose it in `doctor`, `capabilities`, or command warnings when useful
- mark the relevant live suite as optional if it depends on cluster-specific
  components

## What Live Findings Should Improve In The CLI

Live testing is not just a pass/fail gate. It should feed product improvements.

### `doctor` And `monitor`

Improve:

- auth diagnostics
- permission diagnostics
- dependency or optional capability detection
- clearer cluster-health failure messages

### Selection And Resolution

Improve:

- `resolved` metadata
- name-vs-id ambiguity handling
- context fallback visibility
- missing-project or missing-workflow guidance

### Explain, Preview, And Dry Run

Improve:

- effective defaults shown to the user
- request-shape previews
- schedule risk warnings
- offline or online precondition visibility

### Runtime Commands

Improve:

- watch polling behavior
- terminal-state detection
- stalled or long-running instance diagnosis
- task-instance log and control ergonomics

### Error Quality

Improve:

- structured details
- precise `suggestion` values
- distinction between auth, permission, not-found, conflict, and unsupported
  capability failures

### Templates And Validation

Improve:

- workflow and task templates when real clusters repeatedly reject common
  authoring patterns
- local lint coverage for issues that currently surface only in live runs

## What Not To Do

- do not run the whole suite as admin
- do not reuse shared long-lived project names
- do not rely on direct DB writes to prepare state
- do not silently skip cleanup
- do not encode plugin-specific assumptions into the generic live suite
- do not treat a passing live run as a substitute for offline regression tests

## Relationship To Upstream DS Tests

Local upstream source checkouts are useful in three different ways. During
development they are usually mounted under `references/`, but that directory is
ignored, not packaged, and not required for installed CLI usage.

- `dolphinscheduler-api-test`: best reference for our REST black-box flows
- `dolphinscheduler-master` integration cases: best reference for runtime and
  scheduling semantics
- `dolphinscheduler-e2e`: useful for scenario discovery, but not the right
  harness model for our CLI

Use the upstream cases to discover missing scenarios. Do not copy their
assertion style blindly.
