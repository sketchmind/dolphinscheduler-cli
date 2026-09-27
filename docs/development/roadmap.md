# Roadmap

This document tracks current product scope and unfinished work. Architecture,
command contracts and release receipts have their own owners; completed
implementation logs and local candidate reports do not establish readiness.

## Current State

The [current architecture](architecture.md#current-stable-surface) owns the
installed action inventory and exact action/version decision counts. The
compiler covers 153 semantic operations through 21 domain plans. All 37 final DS
releases from `1.3.9` through `3.4.3` have independent exact profiles. DS `3.4.1`
remains the stable runtime target; terminal decisions, source admission and local tests do not promote an
experimental profile.

The [final-release admission record](stable-release-admission.md) owns the added
release decisions and exact differences. The
[reviewed task authoring boundaries](task-authoring-boundaries.md) own the 42
families, 902 exact memberships, runtime holes and worker prerequisites. Typed
validation, explicit opaque authoring and existing-baseline preservation remain
separate claims. Do not infer task support from a native model's presence.

Implemented behavior is maintained in these documents rather than repeated here:

| Area | Authoritative document |
| --- | --- |
| Layer ownership, command catalog and compiled runtime | [Architecture](architecture.md), [compatibility ADR](decisions/0001-multi-version-compatibility.md) |
| Exact source decisions, regeneration and runtime packaging | [Codegen](codegen.md), [compatibility architecture](multi-version-architecture.md) |
| Invocations, schema, errors, previews and output | [CLI contract](../reference/cli-contract.md), [CLI UX decisions](cli-ux-convergence.md#product-decisions) |
| Connection selection and persistence | [Configuration](../user/configuration.md), [named-context design](persistent-connection-context-design.md) |
| Workflow and task create/edit/preservation | [Workflow authoring](../user/workflow-authoring.md), [task boundaries](task-authoring-boundaries.md), [task examples](../user/task-examples.md) |
| Schedule, execution, recovery and mutation uncertainty | [CLI contract](../reference/cli-contract.md), [execution outcomes](execution-outcomes.md) |
| Relations and compact business lists | [Frontend operation relations](frontend-operation-relations.md), [compact JSON boundaries](compact-json-study.md) |
| Contributor checks and live scenario obligations | [Tooling](tooling.md#test-and-quality-gate-lanes), [live testing](live-testing.md#coverage-policy) |
| Release eligibility and publication | [Release process](release.md) |

## Correctness and Discovery Follow-up

Confirmed correctness defects and misleading contracts require correction before
a release. Broader audits below establish whether further changes are needed;
they do not authorize adding commands merely to complete a checklist. Optional
ergonomic extensions are listed separately and need a demonstrated user benefit.

- [x] Render effective project schedule preferences in `workflow.create
  --dry-run` and bind confirmation to those values. Recheck before schedule
  creation; changed effective values stop that stage and report any already
  completed workflow mutations.

- [x] Translate explicit native `60002` (`STORAGE_NOT_STARTUP`) to `invalid_state`
  with server-storage guidance, retaining its upstream source. Source review
  across all 37 releases found this status defined consistently in the 26
  releases from `3.0.0`; native resource guards in `3.0.x` and `3.1.x` return it
  when resource storage is disabled. The shared service rule covers resource
  commands, default-directory discovery and typed task-resource resolution. Generic
  `10057`/`10061` errors remain distinct. This fixes error guidance, not the
  server's storage configuration or the pending resource live coverage.
- [x] Prevent whole-workflow deletion from orphaning observed owner lineage on
  exact `3.3.1` and `3.3.2`: a source-backed recipe selects a read-only preflight,
  with service-owned `conflict` guidance and no automatic version-history repair.
  The [deletion contract](../reference/cli-contract.md#dsctl-workflow-delete-workflow---force)
  preserves the REST read/delete concurrency boundary.
- [ ] Improve diagnosis of lineage already orphaned by earlier whole-workflow
  deletions on `3.3.1` and `3.3.2`. Generic native `50022` alone does not prove
  orphan lineage, and the CLI has no REST repair for a deleted owner.
- [x] Reconcile `workflow-instance.execute-task` runtime support with its native
  executor: `3.3.1`, `3.3.2`, `3.4.0` and `3.4.1` expose the API but have no
  master `EXECUTE_TASK` handler. API acceptance is not execution evidence.
  Capabilities and schema report `limited`, and CLI preflight blocks dispatch
  while retaining the native wire evidence. Positive self-execution scenarios target
  `3.2.0`–`3.2.2` and `3.4.2`–`3.4.3`; only the former increments `runTimes`.
  Dependency-subgraph execution remains a separate unverified facet.
- [x] Verify native status provenance through alert-plugin and alert-group
  service error payloads, including UI plugin schema discovery. The shared
  exception-chain renderer already retains upstream facts; preserve that
  mechanism. Reference conflicts guide users through group list and detail
  inspection because older list projections can omit instance associations.
- [ ] Prepare reversible handling of dangling references in upstream seed
  groups for live acceptance, without treating a missing list field as proof
  that no reference exists.
- [x] Review workflow create/edit final readbacks and online/offline pre-mutation
  detail and post-mutation readbacks. Pre-edit detail failures use the shared
  workflow read translation; release readback failures retain already translated
  domain error types and the applied write. Production exact `3.4.3` adapter
  regressions cover both release actions, both edit input forms and successive
  pre-edit detail reads in
  [workflow error boundaries](../../tests/services/workflow/test_error_boundaries.py).
  [Mutation progress regressions](../../tests/services/workflow/test_mutation_progress.py)
  retain create/edit identities, completed stages and uncertain outcomes. This
  closes the reviewed readback/detail scope, not every workflow mutation error
  path; [execution outcomes](execution-outcomes.md#partial-mutations) remains the
  behavior owner. A readback failure must not imply an unapplied preceding write.
- [x] Verify that parent/child relations do not generate navigation scoped to
  the source project. Current relation replies retain native IDs without
  inferred target-project commands; regression tests cover both directions.
  Relation IDs alone do not establish shared project scope. Keep the
  [relation ownership rules](frontend-operation-relations.md) and
  [operational investigation](../user/operational-investigation.md) intact.
- [x] Let task-instance get/watch/savepoint/stop resolve directly from `--project`
  as well as `--workflow-instance`, so standalone STREAM rows can expose
  savepoint and stop without inventing a workflow-instance ID. Project-only
  selection uses bounded BATCH/STREAM paging where supported; force-success
  and sub-workflow retain their required workflow selector. Preserve native
  ID-only log access and the selected command's exact availability in the
  [CLI contract](../reference/cli-contract.md).
- [x] Review schema parity for deterministic cross-field command validators.
  Mode, source, update, selector, force and log-window guards are represented in
  `command.constraints`; alert-group updates retain their exact-version field
  sets. Constraint references are checked against projected options on all 37
  profiles. Future validators must update the same
  [schema contract](../reference/cli-contract.md#dsctl-schema).
- [x] Review actionable guidance for common configuration, selection, authoring,
  permission and execution failures. Non-mapping workflow YAML now returns
  `user_input_error`; create conflicts/project/permission errors and missing
  instance tasks supply scoped next steps. RPC failures retain dispatch
  uncertainty; nested recovery hints are emitted once. Future translations
  follow the [stable error envelope](../reference/cli-contract.md#standard-envelope).
- [x] Review mutation previews and confirmations against all 181 catalog actions.
  Compiled edits/execution plans retain their seven dry-run surfaces; unchanged
  workflow, task and workflow-instance edits now expose empty mutation plans.
  Direct operations, destructive confirmation, schedule risk review and atomic
  local writes follow the
  [mutation review contract](../reference/cli-contract.md#mutation-review-and-confirmation).
  Nested workflow failures retain their precise stage and one recovery hint.
- [x] Verify that `context` and `doctor` separate saved selection, effective
  settings and remote validity. Context inspection stays local; configuration
  writes report saved and effective selection separately. Doctor describes
  connection/version/API checks and makes no project-permission or task-execution
  claim. See the [context contract](../reference/cli-contract.md#dsctl-context).

## Exact-Version Compatibility Track (In Progress)

The exact wire matrix and compiled-domain ownership are implemented. Remaining
work concerns behavior evidence and specific semantic decisions, rather than
another adapter/family migration. The
[compatibility architecture](multi-version-architecture.md) and
[exact profile gates](live-testing.md#exact-version-profile-gates) define the
review and promotion boundaries.

- [x] Correct the `3.0.0`–`3.0.6` namespace-permission projection when native
  responses have no `clusterCode`; retain absence without inventing an identity.
- [x] Review legacy `1.3.9` project and datasource GET deletions independently.
  Both now execute once and preserve an uncertain mutation outcome instead of
  retrying because the upstream route uses GET. Independent transport tests
  retain the native encodings and failure behavior.
- [x] Aligned cross-version project-selector help and schema with the native
  identity contract: DS `1.3.9` uses `id`, while newer releases use `code`.
  Project-only `name_or_native_identity` metadata makes this distinction
  explicit; code-only domain selectors retain their existing meaning.
- [x] Preserved pagination evidence in project/workflow `DefinitionPage`
  output by reusing the shared pagination implementation. Both single-page
  and `--all` output retain original totals and observed scope; materialized
  counts describe returned rows. Regression coverage includes a non-first
  start page, where returned count differs from the original total. Existing
  project-lifecycle receipts still rely on native-identity GETs and explicit
  first-page absence searches, independently of this next-candidate fix.
- [x] Corrected exact `3.2.2` project-worker-group clear capability: the
  action-specific source decision is `limited` / `not_executable`, while list
  and nonempty set remain supported. Capabilities, schema and CLI preflight
  expose the same constraint; remote `1402003` preserves its source and
  explains the nonempty assignment requirement. All 37 profiles were
  regenerated, with focused generator and CLI regressions. Receipts collected
  before this correction retain their original artifact identity. Persistent
  `3.2.2` assignment tests still need a
  safe cleanup plan because project deletion does not cascade assignment rows.
- [ ] Capture released parsed-YAML semantics, schema compatibility, template
  parse/compile semantics, lint, dry-run, errors and round-trip behavior by CLI
  artifact and exact DS profile. Do not freeze presentation bytes as the public
  compatibility contract. See the
  [published-CLI compatibility corpus requirement](multi-version-architecture.md#1-correct-evidence-through-the-task-authoring-tracer).
- [x] Expanded the `v0.3.0` / exact `3.4.1` SQL inline baseline with
  source-matched historical package captures for those semantic boundaries.
  Comparisons explicitly select the profile, preserve native update identities
  and unknown SQL parameters, and record intentional lint/schema migrations.
  This is a local rebuild of the release tag, not verification of the original
  distribution binary; other artifacts, profiles and authoring facets remain
  covered by the open corpus requirement above.
- [x] Added the same bounded `v0.3.0` / exact `3.4.1` baseline for SHELL
  command shorthand: parsed input, command schema, template compilation, lint
  and create errors, create dry-run, and export/reparse/edit preservation.
  Resource-list schema changes remain outside this command-only slice.
- [x] Add deterministic installed-wheel evidence for the optimistic stale-plan
  negative before promoting `task.update` to `live_full`. Unit/contract tests
  cover the guard; live cleanup proves only that an observed concurrent fixture
  change is not overwritten. Preserve the PUT concurrency/readback limits in
  the [task mutation policy](multi-version-architecture.md#mutation-and-unknown-field-preservation).
  `tests/live/test_task_stale_plan.py` now supplies the exact `3.4.2` scenario:
  retain one prepared object, apply a separate legal update, then require a
  conflict with no stale write and safely restore the exclusive fixture.
  The exact `3.4.2` installed-wheel scenario is independently verified: the same
  prepared object conflicts after interference, makes zero stale PUT calls,
  preserves the intervening state and restores the owned fixture. This does not
  establish generic atomic CAS, coverage on other versions or current-wheel
  release evidence.
- [ ] Close the remaining supported scenarios under the
  [shared live obligations](live-testing.md#current-stable-surface-matrix).
  Completed development coverage is summarized in the
  [coverage snapshot](live-testing.md#current-coverage-snapshot); do not repeat
  completed scenarios because an earlier checkpoint lists them as pending.
  Remaining cleanup constraints concern legacy task-group mutations, queues
  without DELETE and exact `3.2.2` project-worker-group assignments. Positive
  namespace CRUD also requires a working K8S backend and is outside the current
  acceptance scope. Existing missing-backend probes do not establish positive
  mutations. Other optional fields and permission paths remain separate from
  common lifecycle coverage. Apply the
  [acceptance closeout criteria](release.md#closing-development-acceptance)
  to each promised scope.
- [x] Verify the audit-list subset on all 19 profiles without filter-metadata
  endpoints. The installed-wheel evidence covers read-only list behavior and
  temporary credential cleanup; it does not close unrelated governance cases.
- [x] Verify task-group queue controls on all nine cleanup-safe profiles:
  owned waiting-queue identity, priority readback, full capacity and actual
  execution overlap after force-start. Preserve independent proofs and cleanup
  records, including corrected interpretations of transient queue snapshots.
- [x] Adapt datasource and namespace failure cleanup: use the configured
  backend's exact native type, verify grant removal and object absence, and
  register namespace-probe cluster cleanup before response assertions. Separate
  namespace missing-resource assertions from the cluster-dependent probe.
  Portable harness checks do not establish live acceptance.
- [x] Adapt runtime-control scenarios before batch execution: use
  exact stop states and independent recovery/rerun, force-success and execute-task
  scenarios. Require evidence of the new execution before cleanup; an old
  terminal result cannot settle an uncertain start or replay. Harness regression
  checks remain separate from installed-wheel live evidence.
- [x] Adapt the task-group queue scenario before batch execution: resolve the
  fresh run, verify the owned waiting row and persisted priority, and prove
  force-start through actual overlapping task execution at full group capacity.
  Cleanup checks quiescence and group/project ownership before deletion.
  Portable checks do not establish live acceptance; fixture prerequisites and
  exact cleanup restrictions still apply.
- [x] Complete the `0.4.0` same-wheel release corpus: 37 conformance receipts,
  37 exact-read receipts and the separate exact `3.4.2` schema-7 gate, with
  independently verified cleanup. The corrected full release gate and package
  validation passed; subsequent documentation changes have separate review and
  validation. This does not publish the release or promote entire profiles.
  Every future candidate must satisfy the same
  [release process](release.md#candidate-validation). Historical receipts keep
  their original artifacts and scopes; do not relabel them as new evidence.

## Measured Ergonomics and Agent Acceptance

- [ ] Add resource-specific `digest` views only where they materially reduce
  context while preserving task correctness. Workflow and workflow-instance
  digests are delivered; additional candidates belong to the
  [future capability inventory](../reference/future-capabilities.md).
- [ ] Assess broader `explain` views for execution-context and parameter
  reasoning. Preserve the delivered schedule explain contract and require a
  concrete user journey before adding another surface.
- [ ] Compare dynamic action-index designs on equivalent black-box tasks before
  changing existing discovery groups. Preserve identity, coverage and
  uncertainty; new UI-derived actions need separate exact-release review. Use
  the [comparison requirements](compact-json-study.md#验收要求) and
  [relation rules](frontend-operation-relations.md).
- [ ] Validate the repository-distributed `dsctl` skill end-to-end: create a
  multi-task workflow from an instruction, trigger and observe the new execution,
  retrieve logs on failure, report the result, then edit and retry where needed.
  The skill is already distributed with progressive references; distribution
  does not prove task completion or a general benefit. Compare agent outcomes
  under the [same correctness-first acceptance](compact-json-study.md#验收要求).

## Continuing Quality Obligations

These obligations apply to future changes as well as the current release.
Completed local checks are observations of their tree and artifact, rather than
permanent checkmarks.

- [ ] Prioritize shared rule ownership and mechanical replacement by remaining
  maintenance effort, rather than domain counts or line targets. Remove
  superseded implementations in the same change, retain independent expected
  behavior/evidence, and follow the
  [one-edit-path maintenance goals](refactoring-plan.md#next-maintenance-goals).
- [ ] Establish and maintain at least 80% test coverage for `services/` and
  `models/`, with meaningful behavior tests. Passing test counts alone do not
  establish this coverage target.
- [ ] Keep machine-specific private hosts, literal credentials and local
  development reports out of submitted source and release payloads. Retain
  legitimate placeholders, upstream source provenance and secret-free receipt
  evidence. Handwritten literal scanning is delivered; package and documentation
  boundaries remain subject to the [release review](release.md#product-review-before-the-candidate-build).
- [ ] Keep [architecture](architecture.md), this roadmap and the
  [domain model](../reference/domain-model.md) aligned with current code, exact
  DS semantics and unresolved decisions. Run documentation link checks against
  clean-checkout files or immutable upstream sources when updating them.

Substantial implementation changes must pass
`python tools/check_quality_gate.py --mode development` with the documented
lanes. Review changed user journeys before freezing a candidate. Release
readiness requires the complete release gate and fresh receipts for the same
wheel; a development gate or historical campaign cannot substitute for it.

The stable live suite, personas, optional capability blocks, cleanup policy and
runtime/recovery scenarios are maintained in
[live testing](live-testing.md#current-coverage-snapshot). New or changed
cluster-interacting behavior needs appropriate scenario coverage. Credentials,
optional backends and unsafe shared-cluster cleanup remain real execution
prerequisites, rather than reasons to mark an unrun gate complete.
