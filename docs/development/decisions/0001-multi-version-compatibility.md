# ADR 0001: Multi-Version Compatibility Runtime

- Status: Accepted
- Date: 2026-08-03
- Amended: 2026-09-09
- Scope: Apache DolphinScheduler `1.3.9` through `3.4.2`

## Context

At adoption, `dsctl` shipped one full exact `3.4.1` generated package plus
narrow exact runtime slices for `3.2.2` and `3.4.2`. The complete stable surface
still used one broad handwritten `3.4.1` adapter; `3.3.2` and `3.4.0` reused it,
while the exact slices bound narrower read, inspection, identity, and project
ports. The registry had per-profile action catalogs, but runtime composition
and test doubles remained substantially whole-version and whole-session shaped.

That model does not extend safely across the target release set:

- `1.3.9` addresses projects by name and process definitions by numeric id;
- `2.x` introduces project and process codes and a separate task-definition
  model;
- `3.1` and `3.2` add transitional v2 controllers alongside legacy APIs;
- `3.3` changes public process terminology to workflow terminology;
- `3.4.2` removes the transitional v2 controllers while the official UI and
  most retained product paths remain on the main project-scoped controllers;
  some former V2 conveniences require explicit scope or a different recipe.

Task-plugin payloads, DAG serialization, defaults, enums, and error behavior
also evolve independently from controller paths. Static controller extraction
is therefore necessary evidence, but not a sufficient compatibility claim.
The complete evidence and quantitative comparison are maintained in
[Multi-Version Compatibility Architecture](../multi-version-architecture.md).

## Decision

Keep one stable CLI and service vocabulary. Hide exact server differences
behind a deep compatibility runtime in `dsctl.upstream`.

The runtime keeps caller-oriented domain ports such as projects, workflows,
schedules, and workflow instances. It does not expose a stringly typed command
bus. Ports accept and return narrow canonical value objects instead of
generated entities or DS-version-specific request fragments.

```text
commands -> services -> upstream deep domain modules and recipes
                                      -> generated typed wire programs -> REST
```

The public command tree does not gain versioned aliases such as `process-v1`
or `workflow-v2`. An exact `DS_VERSION` selects a private `VersionProfile`.

The 2026-09-09 amendment separates server identification from reviewed read
execution. Exact metadata or explicit configuration still owns exact selection.
Source-generated Swagger/OpenAPI route candidates never select a server release,
even for a singleton. Verifiable operation contracts can admit an explicitly
reviewed read closure only when its complete wire and semantic contracts agree
across every candidate. The chosen compiler profile remains an internal
execution detail; public `ds_version` stays null. Mutations and authoring retain
the exact-version requirement. This exception does not infer support from GET,
codec equality alone or version proximity. See
[Contract Read Discovery](../contract-read-discovery.md) for scope and bounds.

The implemented outcome now has reviewed deep domains for every stable remote
action, no broad session/adapter or whole-session fake, and 15 exact generated
runtime slices. Complete exact source contracts remain reproducible audit
artifacts. Mechanical transport/projection shapes are deduplicated as finite
static kernels, while semantic wire-family membership remains explicit review
data. This consolidation does not change profile support or evidence.

## Canonical Boundary

Services may depend on stable concepts such as:

- `ProjectRef`, preserving the native name, id, and code only when each exists;
- `WorkflowRef`;
- `Page[T]`;
- workflow, schedule, and runtime summaries used by stable output contracts;
- a version-neutral workflow plan containing tasks, relations, parameters, and
  layout.

Services must not require every server to have a project or workflow code, and
must not invent one for older versions. Version-native artifacts such as
`processDefinitionJson`, `taskDefinitionJson`, `taskRelationJson`, `connects`,
and controller-specific form parameters belong below this boundary.

The CLI may normalize an irregular older API when it can preserve the stable
product meaning. It must reject an action when preserving that meaning would
require guessing, silently dropping data, or emulating a non-atomic mutation
without an explicit product contract.

## Version Profiles and Domain Recipes

An exact server version is described by a composition rather than one giant
adapter:

```text
VersionProfile
  exact server version
  generated typed wire-program closure
  domain recipe bindings
  capability catalog
  verification evidence
```

Domain-module seams are introduced only where upstream behavior evolves
independently. Expected high-cohesion areas include:

- identity, authentication, and projects;
- workflow and task authoring;
- schedules and execution;
- workflow instances, task instances, and logs;
- governance, resources, and plugins.

Semantic reuse is based on the consumed-projection and preservation contract
closures plus live evidence, not semantic-version proximity. Every selectable
version still has an exact source manifest and at least an exact-version read
smoke before it can be a stable support claim.

Profiles are fully materialized and diffable. They may select the same recipe
or wire family, but do not inherit another exact version's generated package.
Source, effective-wire, CLI-consumed-projection, and preservation fingerprints
are kept separate. A guarded handwritten decision ledger authorizes recipe
selection and wire-family membership and defines the consumed projection and
opaque-state preservation obligations; the compiler only calculates their
digests and impact. Matching generated fingerprints only propose candidates.

## Capability Model

Capabilities are evaluated for stable actions. They separate:

- availability: `supported`, `limited`, or `unsupported`;
- fidelity: equivalent or explicitly constrained;
- verification: static, contract-tested, live-smoke, or live-full evidence;
- promise scope: core or extended coverage for the version.

`unreviewed` may exist only in compiler/build audit worklists. It never appears
in public runtime `Availability`; every packaged coordinate is `supported`,
`limited`, or zero-request `unsupported`. It blocks a stable tier when its
named bundle requires that coordinate, not an unrelated complete profile or
reviewed experimental cell in the same wheel. Version capability, cluster
configuration, and current-user authorization are separate facts.

Stable support levels are derived from machine-readable named conformance
bundles and fresh evidence. Experimental profiles may promote reviewed actions
and facets independently without implying a stable whole-profile tier.

One catalog drives runtime preflight, action-local schema availability,
capability discovery, doctor diagnostics, navigation suggestions, and release
evidence. Unsupported actions fail before an HTTP request.

`limited` is reserved for a known constrained action, but remains fail-closed
and absent from executable navigation until a normalized service intent can
name and gate every relevant input facet. The first limited profile must add
that intent-level integration and its production-path tests; the catalog does
not publish a dormant facet API in advance.

Known commands remain directly executable. `capabilities` and `doctor` are not
mandatory preflight steps. Action-local help exposes stable parser facts;
action-local schema and capabilities expose bounded selected-version
restrictions. The full cross-version matrix remains an explicit audit view.

## Generator Role

The generated-first rule applies to DS-native wire facts. The generator is a
development-time contract compiler and impact analyzer, not the authority for
product semantics.

It owns:

- exact-tag source extraction with provenance;
- exact declarations plus separately preserved inferred response evidence;
- normalized effective-wire candidates and mechanical fingerprints for
  reviewed consumed-projection and preservation contracts;
- typed wire programs, request encoding, and response validation;
- generated runtime closures for operations used by stable domain ports;
- automatic transitive request/response/type closure;
- a separate task-plugin contract inventory for authoring payloads that REST
  controller extraction cannot see;
- contract diffs and their mapping to affected domain operations and actions;
- generated freshness checks.

It does not own:

- stable commands or service interfaces;
- consumed projection or unknown-field preservation semantics;
- semantic compatibility decisions;
- domain-level multi-request adaptation;
- support levels, user-facing errors, or suggestions.

Source corrections are scoped by exact source version or source shape. Each
correction asserts the shape it replaces and fails when that shape goes stale.
A correction for one release must not block extraction of another release.
Conflicts between strong declaration and inference evidence block automatic
wire-family grouping. Neither the newest declaration nor the deepest implementation
intermediate wins automatically.

## Upstream UI Evidence

The DolphinScheduler web UI is a first-class behavioral reference for:

- which user actions are exposed together;
- field defaults and conditional visibility;
- request ordering and lifecycle preconditions;
- how names, ids, codes, task plugins, and schedules are selected.

Controller annotations, DTOs, services, task-plugin models, and live responses
remain the protocol sources of truth. UI behavior may guide a clearer CLI
default or workflow, but it cannot justify an undocumented wire assumption.

## All-Version Execution and Authoring Policy

All-version work uses three different units: exact source and profile
coordinates are established across the full 15-version set; runtime behavior is
implemented one cohesive domain across applicable profiles at a time; and
support is promoted one exact profile at a time. Representative releases are
navigation and test-order anchors, never inheritance roots or transitive
compatibility evidence. Complete extraction or a filled support matrix is not
a support claim.

Workflow YAML is a stable `dsctl` semantic contract. Templates are projections
of it and may change prose or formatting. One exact-profile task-authoring
catalog drives task schema, template, lint, compile, export, preservation,
dry-run, and task-facet results, while the existing `command_contract` remains
the authority for CLI arguments, help, and command schema. The existing `3.4.1`
behavior is the migration parity default, but conflicts with the public
contract, exact upstream evidence, or live behavior are classified and
corrected rather than projected as bugs.

The detailed delivery geometry, replacement rule, domain definition of done,
and future-release flow are maintained in the
[implementation path](../multi-version-architecture.md#implementation-path).

## Verification

The compatibility gate has four layers:

1. Every target tag produces a deterministic source manifest; scoped
   corrections validate their source shapes.
2. Every stable action in every released profile resolves to supported,
   limited, or unsupported, with no unknown state.
3. Wire-family and domain-module contract tests prove request binding,
   canonical results, error translation, preservation, and zero-request
   unsupported behavior.
4. The live matrix runs an exact-version read smoke for every released profile,
   the named conformance-bundle scenario for each stable support tier, and
   exact cases for every promised version-sensitive or high-risk mutation and
   facet. Other recipes are covered by wire/domain scenarios rather than a
   recipe-by-version live product.

Live evidence records the immutable CLI artifact or wheel digest, DS image and
source identity, profile/recipe/wire/projection/preservation fingerprints,
plugin and relevant cluster configuration, timezone, principal, redacted
operation trace, cleanup result, and freshness.

## Migration

Migration is vertical and uses stable `3.4.1` product behavior as its default
parity target, subject to the reviewed-correction policy above, in four coarse
stages:

1. correct response-evidence precedence and prove the minimum typed wire seam,
   guarded decision, derived closure, SQL facet, and deep task-authoring module
   through one `3.4.1`/`3.4.2` vertical tracer;
2. migrate schedules and existing project/workflow reads as an independent
   second slice, then generalize the four fingerprints, decision-ledger schema,
   automatic closures, task-plugin inventory, and materialized profiles from
   the two proven shapes;
3. migrate the stable surface one cohesive domain across all exact profiles,
   using `3.2.2`, `2.0.9`, and `1.3.9` only as navigation anchors before
   reviewing adjacent exact deltas, while deleting replaced broad protocols,
   fakes, and tests;
4. ship only deduplicated reviewed runtime closures, remove whole-version
   scaffolding after replacement, and close the complete 15-version action,
   task-facet, contract, live, and release gates.

The detailed stage exits and structural success criteria are maintained in
[Multi-Version Compatibility Architecture](../multi-version-architecture.md#implementation-path).

## Consequences

The design adds an explicit compatibility catalog and canonical domain models,
but removes the need to clone a monolithic adapter and DS-shaped Protocol layer
for every release. A version change should have high locality: generated wire
facts, the affected recipe, one profile, and its tests.

Generated manifests or matching fingerprints identify compatibility
candidates; they never replace semantic review or live evidence.
Complete generated SDKs remain useful audit and migration artifacts, but are
not the target published runtime shape. The installed package ultimately keeps
only stable-action closures, with mechanical transport/projection shapes
deduplicated as finite static kernels. Reviewed wire-family identity and exact
profile provenance remain separate and intact.
