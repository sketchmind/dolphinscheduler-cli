# Roadmap

This page tracks open product work and continuing quality requirements.
Implemented behavior belongs in its owning guide, and release changes belong
in the [changelog](../../CHANGELOG.md). Local plans and campaign progress are
not a second public roadmap. The items below describe scope, not a release
schedule or authorization to mutate a deployment.

## Current State

The [architecture](architecture.md#current-stable-surface) owns the current
command inventory and compiled runtime. All 37 final DS releases from `1.3.9`
through `3.4.3` have exact profiles. Profile coverage, live verification and
release promotion are separate claims; see
[support policy and verification](../user/version-compatibility.md#support-policy-and-verification).

Use these documents for implemented behavior and evidence:

| Topic | Owner |
| --- | --- |
| Exact source and compatibility decisions | [Compatibility architecture](multi-version-architecture.md), [profile admission](stable-release-admission.md) |
| Public commands, output, errors and previews | [CLI contract](../reference/cli-contract.md) |
| Task support, preservation and prerequisites | [Task authoring boundaries](task-authoring-boundaries.md) |
| Live obligations and recorded coverage | [Live testing](live-testing.md#current-coverage-snapshot) |
| Candidate acceptance and publication | [Release process](release.md) |

## Exact-Version Compatibility Track (In Progress)

The compiler and exact profile matrix are implemented. Remaining work concerns
semantic evidence, cleanup constraints and specific user journeys:

- [ ] Diagnose lineage already orphaned by earlier whole-workflow deletions on
  `3.3.1` and `3.3.2`. The implemented preflight prevents new observed cases;
  generic native `50022` alone does not identify an orphan, and there is no
  CLI REST repair for a deleted owner.
- [ ] Expand the published-CLI compatibility corpus by artifact and exact DS
  profile. Capture parsed-YAML semantics, schema compatibility, template
  compilation, lint, dry-run, errors and round trips rather than freezing
  presentation bytes. Existing `v0.3.0` / exact `3.4.1` SQL-inline and SHELL
  command-only slices are bounded rebuilds of the release tag, not proof of
  the original distribution binary or other authoring facets. See the
  [corpus requirement](multi-version-architecture.md#1-correct-evidence-through-the-task-authoring-tracer).
- [ ] Close applicable supported scenarios under the
  [shared live obligations](live-testing.md#current-stable-surface-matrix).
  Legacy task-group and queue mutations lack safe REST deletion or cascade
  cleanup; exact `3.2.2` project-worker-group assignments also need a safe
  cleanup plan. Project deletion does not remove those assignments, and empty
  assignment is natively rejected. These constraints must be resolved before
  claiming lifecycle coverage.
- [ ] Validate positive namespace mutations with a working K8S backend when
  that scope is taken up. It is currently deferred; a missing-backend probe
  does not establish positive CRUD. Optional fields and permission paths
  remain distinct from common lifecycle coverage.

Previously completed runtime, schedule, datasource, resource, alert and
administrative scenarios are recorded in the
[coverage snapshot](live-testing.md#current-coverage-snapshot). Preserve their
artifact and scenario bindings when addressing gaps; a historical pending list
is not a reason to repeat completed work.

## Release Acceptance and Support Policy

The `0.4.1` release candidate includes the legacy workflow layout and
automatic API discovery fixes. Its
[artifact-bound corpus](live-testing.md#exact-version-profile-gates) contains
37 four-action exact-read receipts, 37 18-action `full_core/v1` receipts and
one 15-action `external-shell/v1` schema-7 receipt on exact `3.4.2`, with
independent cleanup verification. Earlier receipts retain their original
artifact bindings and cleanup limits in [history](live-evidence/history/).

Each publication requires the strict release gate, canonical artifact checks
and protected source integration described in
[candidate validation](release.md#candidate-validation).

- [ ] Review support-policy labels against the recorded verification scopes.
  Keep the offline default and historical `full` / `tested=true` designation
  distinct from artifact-bound coverage. Record the scope and rationale of any
  policy change in an explicit reviewed decision.

## Ergonomics to Evaluate

These are questions to investigate before adding commands. Establish a user
need and exact DS scope, then update the owning contract with the implementation.

- [ ] Additional `digest` views: which resources need summaries beyond the
  existing workflow and workflow-instance digests, and which identities, state
  and coverage facts must they retain?
- [ ] Broader `explain` views: can execution-context or parameter reasoning
  answer a concrete question that current schema, previews and schedule explain
  do not answer?
- [ ] Runtime observation: would event streaming or richer progress views
  improve an observed journey while retaining terminal outcomes, polling limits
  and query coverage?
- [ ] Action-index design: compare alternatives on equivalent black-box tasks
  before changing existing discovery groups. New UI-derived actions need
  separate exact-release review under the
  [relation rules](frontend-operation-relations.md).
- [ ] Agent skill acceptance: exercise workflow creation, execution, failure
  logs, result reporting and an edit/retry journey through the public CLI.
  Distributing the skill does not prove task completion or a general benefit.

Use the [usability evaluation protocol](cli-ux-convergence.md#usability-measurement)
for equivalent inputs, correctness criteria and actual usage measurements.
The [output encoding checks](cli-ux-convergence.md#output-encoding-acceptance)
protect identities, metadata and query coverage while changing representation.

## Continuing Quality Obligations

- Keep one owner for a rule and preserve independent behavioral expectations;
  prioritize reduced maintenance effort over domain or line-count targets.
  Follow the [maintenance principles](architecture.md#maintenance-principles).
- Establish and maintain at least 80% test coverage for `services/` and
  `models/` with meaningful behavior tests. Passing test counts do not establish
  this coverage target.
- Keep private hosts, credentials and local development reports out of source
  and release payloads. Preserve legitimate placeholders, upstream provenance
  and governed secret-free receipts under the
  [release review](release.md#product-review-before-the-candidate-build).
- Keep user guidance, architecture and source decisions aligned with code.
  Verify links against clean-checkout files or immutable upstream sources.

Substantial implementation changes require the full development gate; formal
release readiness requires the strict release gate and receipts for the same
wheel. Neither local checks nor historical campaigns substitute for that
candidate's obligations. See [Tooling](tooling.md#test-and-quality-gate-lanes)
for the available lanes and their prerequisites.
