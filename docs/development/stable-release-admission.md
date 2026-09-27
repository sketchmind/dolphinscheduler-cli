# Final-Release Profile Admission

This record documents the admission of independent exact profiles for all final
Apache DolphinScheduler releases from `1.3.9` through `3.4.3`, including
`2.0.1`–`2.0.8`, `3.0.1`–`3.0.5`, and `3.1.1`–`3.1.8` after the
[intermediate source audit](intermediate-release-source-audit.md).
`3.3.0-alpha` is a prerelease and is outside this inventory.

The added profiles were admitted as experimental with `tested=false`; admission
did not promote them. The [current architecture](architecture.md#current-stable-surface)
owns the installed action inventory and runtime state, and
[version compatibility](../user/version-compatibility.md) owns published support
status. Exact source and contract verification do not supply installed-server
receipts. Historical receipts remain bound to their recorded artifacts.

## Architecture and Evidence

The existing 21 compiled domain plans handle all admitted releases. Each exact
profile selects reviewed operation, DTO, enum and task semantics; there is no
nearest-version fallback or complete-release adapter alias. Equal wire
implementations can share generated records while exact source ownership and
membership stay independent.

The three inventories remain separate:

- `tools/ds_codegen/exact_sources.json` identifies the mechanical source inputs.
- `tools/ds_codegen/version_profile_decisions.json` records each exact tag,
  commit, tree, action decision and support level.
- `tools/ds_codegen/runtime_bundles.json` selects the installed packages.

Admission closed the selected action/version matrix with explicit supported,
limited or upstream-absent decisions. The current counts are maintained in
[architecture](architecture.md#current-stable-surface), rather than frozen in
this admission record. Action availability does not imply that every task family
or authoring facet is available on that release.

## Differences Retained

| Surface | Exact source behavior retained |
| --- | --- |
| `2.0.1` users, tokens and projects | Legacy mutation result handling; project grants use IDs and no safe single-project revoke route exists. Empty grant input deletes all project grants. |
| `2.0.1` alerts and database monitor | Alert pages already use entity/search contracts; the database-type enum has moved into SPI. These do not inherit the complete `2.0.0` contract. |
| Early `2.0.x` resources and tasks | Resource view-map and response-type epochs, optional update flags, and void versus nullable task-delete results stay exact. |
| `2.0.6`–`2.0.7` switch parameters | Native `nextNodeList` element types remain distinct from later releases. |
| `3.0.1` schedules | The upstream priority default remains explicit. |
| Schedule errors | Upstream rejection of a past start time becomes `user_input_error` with a future-start suggestion for create, update and activation. |
| `3.1.0`–`3.1.6` task states | Their execution-status enum has no `STOP`; it is not borrowed from `3.1.7`. |
| Datasources | Each new release has its own source/DTO closure evidence. `2.0.1`–`2.0.8` and `3.0.1`–`3.0.5` reuse the reviewed HDFS/Oracle implementation; `3.1.1`–`3.1.8` reuse the Athena implementation. |

Task mutation review also corrects earlier representative-version assumptions:
`2.0.0`–`2.0.2` use the complete workflow update path; `2.0.3` keeps that route
for ordinary fields but rejects dependency edits. `2.0.4`–`3.2.0` use standalone
ordinary-field updates with a unique-workflow-binding guard and reject explicit
`depends_on`. Some earlier `3.0.x` and `3.1.x` routes accept dependency parameters
but discard the modified relation list or make the relation update unreachable.
The presence of those parameters therefore does not prove dependency support.

The separate `2.0.3` workflow-definition guard conservatively rejects relation
changes because `updateDagDefine` can compare the input graph to itself, silently
skip an equal-size rewire, or fail on duplicate successor keys. Identity-preserving
renames and ordinary field updates still work. `2.0.4` compares incoming and
stored relation sets and does not receive this restriction; create and instance
edit do not call the defective definition-update method.

At admission, the task facts and reviews retained 42 families and 867 positive
exact memberships. All original 418 memberships remained; the added releases
contributed 449:

| Added profiles | Typed families per profile |
| --- | ---: |
| `2.0.1`–`2.0.8` | 14 |
| `3.0.1`–`3.0.5` | 20 |
| `3.1.1`, `3.1.2` | 27 |
| `3.1.3` | 29 |
| `3.1.4` | 30 |
| `3.1.5`–`3.1.8` | 31 |

The task review preserves worker prerequisites, invalid typed-input rejection,
opaque authoring limits and baseline preservation separately. In particular,
SageMaker's stale polling state affects `3.1.1`–`3.1.2`, not `3.1.0`: the latter
updates its instance field inside the polling method. The
[reviewed task boundaries](task-authoring-boundaries.md) own the complete
per-family defect windows and parameter propagation rules.

## DolphinScheduler 3.4.3

The later `3.4.3` admission uses its own tag, commit, source tree, contract and
task reviews in the same three inventories. It adds 35 typed task memberships;
the current totals belong to [task boundaries](task-authoring-boundaries.md).
The profile remains experimental with `tested=false` and has no new live receipt.

| Surface | Reviewed change |
| --- | --- |
| Schedule | Nested `missedFirePolicy` supports three native policies; omitted create input uses `FIRE_ALL_MISSED`, and omitted update input preserves the stored policy. Older profiles do not accept the field. |
| Task update | Removed standalone routes are replaced by the native whole-workflow transaction, preserving unrelated tasks, relations and resources with stale-plan and readback checks. |
| Workflow instances | Paging and trigger responses use summary VOs; detail remains the entity contract. Lightweight rows do not fabricate detail fields or trigger per-row requests. |
| Resource preview | The repaired native view endpoint supplies the requested line window; earlier broken epochs retain their reviewed download-based path. |
| Governance | Current-user lookup, enabled-user and datasource-permission projections, cluster pagination and audit scope retain the exact controller/VO behavior. |
| DATAX | A literal empty JSON object no longer satisfies the native custom-config requirement. The typed inline facet rejects it; source presence does not admit a file fallback. |

These changes reuse domain and task-family owners. They do not add a release-wide
fallback adapter or inherit `3.4.2` support from matching generated shapes.
Development checks establish local correctness; the new candidate still needs
its own installed-wheel and server acceptance before publication.

## Version Selection

All 21 added releases require explicit `DS_VERSION`. Their official API trees
provide neither trusted product-information nor OpenAPI version metadata.
Database schema versions are retained as evidence, not used as aliases:
`2.0.8` reports `2.0.7`, `3.0.3` reports `3.0.2`, and the existing `3.2.2`
profile reports `3.3.0`. Failed or ambiguous discovery never selects a neighbor.

## Validation Method and Historical Scope

The admission decision was checked through the full development gate, including
portable behavior, exact source-contract verification and byte-identical source
rebuilds. Local package integration was checked using an installed wheel with
the source checkout absent from the import path. These historical observations
established source admission and local integration; they did not contact a DS
server, refresh live receipts or prove that a later artifact is releasable.

Reproduce the source and packaging checks for the tree being reviewed:

1. Prepare the exact source inputs and current-fingerprint full snapshots using
   [codegen](codegen.md#exact-target-source-matrix). Check the tag, commit, tree,
   extractor fingerprint and snapshot digests before reuse. Source checkouts
   remain optional, ignored and read-only from this project's perspective.
2. Run `python tools/check_quality_gate.py --mode development`. Its portable,
   source-contract and source-rebuild lanes verify exact compilation, task and
   datasource decisions, generated freshness, types and structural boundaries.
3. Build from a clean source snapshot and perform the
   [wheel-only preflight](release.md#candidate-validation), comparing the entire
   packaged runtime and metadata with source bytes. Install the wheel into an
   isolated environment without editable/source imports.
4. Exercise all selected profiles, domain bindings, command help, task templates,
   capabilities, schemas, exact version selection and env-file precedence.
   Treat warnings as errors and validate template-to-lint/compile journeys.
5. Obtain the separate [installed-server receipts](live-testing.md#exact-version-profile-gates)
   required for the same candidate wheel before release or profile promotion.
   Keep historical admission results and archived receipts bound to their
   original scope; never relabel them as current evidence.
