# Codegen

`src/dsctl/generated/` contains tracked runtime code produced from upstream
Apache DolphinScheduler source.

The generator is a development-time wire-contract compiler. It extracts exact
source facts, emits typed low-level clients, and reports compatibility impact.
Semantic compatibility, stable CLI behavior, and support promotion remain
handwritten product decisions; see
[ADR 0001](decisions/0001-multi-version-compatibility.md) and the evidence-backed
[Multi-Version Compatibility Architecture](multi-version-architecture.md).

## Rules

- Do not hand-edit generated packages.
- Treat `references/` as an optional, ignored local workspace for upstream
  source checkouts.
- Treat upstream source mounted under `references/` as read-only from this
  project's perspective.
- Prefer generator fixes over handwritten DS-facing shapes.
- Keep generated imports inside `dsctl.upstream`.
- Keep `JsonValue` and `JsonObject` at transport and boundary layers.
- Scope manual source corrections by exact source version or asserted source
  shape; a correction for one release must not block another release.
- Preserve declared and inferred response evidence separately, including its
  provenance; never silently replace one with the other.
- Normalize injected parameters and framework defaults only in a derived wire
  view, not by erasing exact source facts.
- Block wire-family catalog generation on unresolved strong evidence conflicts
  or a stale reviewed compatibility decision.
- Generate runtime closures from the stable operations the CLI consumes when
  doing so materially reduces duplicated output; complete source manifests may
  remain development artifacts.
- Treat `tools/ds_codegen/runtime_bundles.json` as the single source of truth
  for runtime bundle inventory and source selection. It does not own action
  support decisions.
- Treat `tools/ds_codegen/exact_sources.json` as a mechanical source inventory.
  It can include releases with no reviewed profile and does not define the
  generator's reviewed or packaged version set.
- Treat `tools/ds_codegen/version_profile_decisions.json` as the reviewed source
  of semantic-operation and stable-action decisions. It does not choose which
  source bundles are packaged.
- Treat `tools/ds_codegen/datasource_profile_reviews.json` as the reviewed
  plugin-DTO facet input. Exact `DbType` values and base payload fields still
  come from each generated contract snapshot; the unified generator validates
  the reviewed plugin fields against that exact enum before materializing the
  runtime datasource profile.

Source extraction shares read-only Java syntax trees and filename/package
indexes within one call, then releases them and the inference caches, including
when extraction fails. Complete source contracts remain independently rebuilt
for every exact release. Domain compilation similarly reuses a snapshot's
rendering context within one invocation; exact membership, schema collisions,
type closures and executable output checks still run for each selected binding.
Neither optimization reuses a result across invocations by version label.

## Version-Profile Compiler Contract

`profile_ledger.py` derives reviewed membership from the decision ledger's
`sources` entries. There is no second handwritten version list in
`source_matrix.py`. The runtime generator validates its selected versions
against the ledger before reading upstream source, then passes that selection
through domain plans and every generated profile. Task facts may cover more
versions than task reviews; each selected version still requires both.

An explicit subset manifest can generate a complete selected artifact tree for
development under `build/`. It cannot admit an unreviewed version or replace a
full release check. Default regeneration and freshness use the complete tracked
runtime manifest; release mode continues to reject partial or skipped lanes.

The compiler consumes reviewed semantic operations and the stable CLI action
catalog, then materializes one exact profile for each reviewed target version.
The [current inventory](architecture.md#current-stable-surface) records the
action and coordinate counts. Every action/version coordinate receives a
terminal decision, and every upstream-present stable action has an executable
wire recipe or an explicit reviewed runtime limitation.

An existing wire binding does not establish runtime support. The ledger reason
`upstream_runtime_limited` requires `limited`, `not_executable`, nonempty source
evidence and an existing binding. Its build decision is blocked while keeping
the binding's source operations, type closure and four fingerprints. The shared
action preflight blocks dispatch. This models the missing master `EXECUTE_TASK`
handler on `3.3.1`–`3.4.1`; endpoint-absence decisions still require no binding.

Terminal completeness means only that every coordinate has an explicit
decision. It does not collapse the other gates: terminal is not synonymous
with supported, supported is not synonymous with live-verified, and
live-verified does not automatically promote a version. `3.4.1` remains the
stable full/tested runtime target; the other exact profiles remain
experimental.

For every accepted or terminally blocked semantic operation, the compiler
produces four independent fingerprints: exact source evidence, normalized
effective wire, the reviewed CLI-consumed projection, and the reviewed
preservation contract. Preservation covers owned paths, unknown-field and
opaque-extension policy, and merge/encode behavior; it cannot be inferred from
the known fields that the CLI reads. The semantic operation or recipe defines
the consumed and preservation contracts, while the compiler computes source
reachability, transitive request/response/type closure, and action impact.

It also inventories task-plugin authoring contracts separately from REST
controllers. Parameter fields, enums, defaults, and resource references are
extractable facts; UI conditional behavior and nontrivial plugin validation are
reviewed evidence, not automatically promoted semantics.

Task definitions are now one exact domain within this general compiler rather
than the pilot for a future ledger. Their source facts and version contracts
cover all reviewed releases. Every executable code-native task profile from `2.0.0`
through `3.4.3` binds exact `CompiledWireProgram`/`WireExecutor` detail and DAG
reads; safe updates select either a standalone operation or a whole-workflow
strategy.
`1.3.9` uses a graph-backed string-id/name recipe rather than inventing a
code/version identity. The original exact
`3.4.1`/`3.4.2` tracer and its special update-result contracts no longer define
the boundary of typed task execution or the product's profile architecture.

Full exact source manifests and clients remain reproducible audit artifacts.
Published runtime slices now retain enum catalogs and provenance with zero native
operations; generated executable requests and complete response closures live in
the 21 compiled plans. One version never imports another version's package as its
source truth. Production programs validate and encode detached requests directly,
without the retired generated-invocation capture bridge or exact-package loader.

The former family ledger, data-only catalog, wrapper-kernel renderer and their
exclusive analyzer paths have been retired. Exact audit-package rendering
remains reproducible. Mechanical reuse never creates semantic membership;
response differences stay bound to each exact schema and profile.

Whole-domain compiled artifacts keep a second identity boundary explicit. Their
program digest (schema 4) covers only the operation coordinate, codec, request
and response schemas, and envelope. Their exact profile digest separately
covers full source provenance, program decisions, and the selected recipe.
The installation manifest (schema 5, renderer ABI 15) binds the emitted bytes and
shared runtime. Thus changing an unconsumed upstream operation does not churn
unchanged executable digests, but still changes the exact source/profile
evidence. Runtime source checks and wheel-bound live receipts are not relaxed;
digest equality does not authorize semantic membership or profile promotion.

Compiled request and response support modules live under
`wire_programs/_schemas/<domain>/`. Physical names retain the schema role and a
12-character digest prefix; full executable digests remain authoritative, and
the compiler rejects prefix collisions. Package initializers belong to the
recursive, content-bound support inventory. Moving the relative imports changes
executable source digests and their dependent program/profile identities even
when field behavior is unchanged.

Each domain stores complete identical codec values once in `CODEC_RECORDS` and
retains every exact codec key in `CODEC_BINDINGS`. The fixed `CODECS` expansion
deep-copies each binding, keeping nested records independent at runtime. The
shared static reader in `ds_codegen/compiled_literals.py` recognizes only that
closed expansion or a literal codec dictionary; dependency and evidence checks
never execute candidate modules to recover the records.

The closed compiled transport vocabulary includes explicitly empty GET queries
and GET with one integer path segment. Request-only schema extraction removes
response roots rather than importing their models. Structured response modules
export `RESPONSE_TYPE` for both class and collection roots; its source-rendered
annotation and complete type closure retain the same executable digest contract.
Alert-plugin uses these forms for its reviewed instance-list, definition, and
transient update-baseline recipes. Unreviewed field, path, or response changes
still fail domain compilation.

Access-token's fifteen exact profiles use the same compiled-domain engine.
An optional, explicit required-field set distinguishes request epochs with
identical routes and field names but different requiredness. Selection reuses
the request renderer's source rule, while the rendered models still own types,
defaults, and validation. User lookup is an explicit delegated dependency, not
token-owned source. The superseded `access-token-list-v1` wrapper family and its
now-empty ledger have been retired; version history and complete exact
source/consumer evidence retain the audit trail.

Namespace reuses the exact governance recipe rather than duplicating its version
table. In particular, an identical delete wire does not establish identical
Kubernetes side effects: reviewed destructive and registration-only decisions
select distinct compiled recipe coordinates. Its page, empty-query available
list, and form mutations require no new transport shape. Removing those owned
operations leaves user authorization queries and their referenced namespace
models in the exact runtime slice.

Task-group compiles its management and queue operations together from the existing
reviewed contract. Its nine primitives use the current GET-query and POST-form
vocabulary; response closures retain exact numeric/enum status, nullable ids,
paging strictness, and process/workflow identity epochs. Four runtime recipes
preserve mutation verification and queue behavior while the compiler separately
checks `3.4.2`'s native typed-result declarations. The controller's nine owned
operations and two unreferenced entities retire together; project discovery
continues through the existing definition-read interface.

The counts above describe the reviewed compiler result. A release may cite the
materialized artifacts only after the unified regeneration and generated-
freshness checks reproduce them; this document does not assert that the current
worktree has passed those gates.

## Prepare Upstream Source

The installed CLI does not need `references/`. The directory is only needed
when developing the generator, auditing DS-facing behavior, or comparing
upstream DolphinScheduler versions.

Use a local checkout for the default DS source:

```bash
mkdir -p references
git clone https://github.com/apache/dolphinscheduler.git references/dolphinscheduler
git -C references/dolphinscheduler checkout 3.4.1
```

`references/dolphinscheduler` is ignored by git, excluded from distributions,
and checked by package-content gates so it cannot be published accidentally.

For cross-version analysis and runtime generation, keep temporary worktrees
under `build/`:

```bash
mkdir -p build/upstream build/runtime-sources
git clone --bare https://github.com/apache/dolphinscheduler.git build/upstream-source.git
git --git-dir=build/upstream-source.git worktree add build/upstream/ds-3.2.2 3.2.2
git --git-dir=build/upstream-source.git worktree add build/runtime-sources/ds-3.4.2 3.4.2
```

Use an independent Git store for preparation; mounted reference sources remain
read-only.

## Analyze a Candidate Release

The existing diff tool has two scopes. `--scope full` compares complete extracted
contracts. `--scope cli` selects the reviewed base version's consumed operation
and type roots, compares their strict closure and normalized request/response
contracts, and preserves the candidate's actual version and provenance.

For example, compare a cached reviewed base with a candidate source:

```bash
python tools/analyze_ds_version_diff.py \
  --scope cli \
  --snapshot 3.0.6=build/ds_contract/snapshots-v2/ds-3.0.6-contract.json \
  --ds-source 3.0.5=build/upstream/ds-3.0.5 \
  --base 3.0.6 --target 3.0.5 \
  --format json --output build/ds_contract/3.0.5-cli-diff.json
```

`--source-manifest` accepts an inventory with the same format as
`exact_sources.json`; relative source paths resolve against `--repo-root`.
Only the selected base and targets are loaded. Use `--snapshot` for an already
extracted candidate to avoid repeating extraction, and declare each label only
once. CLI scope requires exact provenance, a valid contract digest, and the
current extractor fingerprint. A stale snapshot must be regenerated; it cannot
silently enter a comparison with current extraction rules.

The result distinguishes `equal`, `different`, and `incomplete_evidence`.
Missing operations/types and unresolved closure evidence are reported explicitly.
`equal` describes only the selected static contract. Task execution behavior,
worker prerequisites, preservation decisions, live acceptance, and semantic
support remain separately reviewed; reports always mark semantic support as
`not_assessed`. Neither analysis scope modifies runtime membership.

`extract/return_type_rules.py` contains named response correction rules and
their source guards. `extract/return_type_reviews.py` binds exact reviewed
coordinates to those rules. `return_type_resolution.py` applies and checks them.
Only rules explicitly enabled for candidates can apply outside reviewed
membership, and their source guard must pass there too. Exact reviews retain
strict declared/inferred type and import checks; candidate rules never fill
absent correction coordinates in an already reviewed release. A failed proof is an error,
not permission to guess from a nearby version; rules without a reusable proof
remain exact-only.

Operation-scoped corrections may also derive one generated response view
without changing a shared source model. Alert-plugin paging in exact DS
`3.1.0` through `3.1.7` is one such case: `PageInfo.totalList` has a non-null
initializer, but `AlertPluginInstanceServiceImpl.listPaging` overwrites it with
a helper that returns null for empty instances or missing plugin definitions.
The reviewed rule proves those setter and helper branches, and only that
operation's generated response admits a present null list. Missing fields and
non-list values remain contract errors; other `PageInfo` consumers retain the
source model's non-null contract. A null list is projected as empty only when
the requested page is consistent with the reported total; a null list on a
page whose offset is below that total is an upstream inconsistency and remains
an `api_transport_error`.

## Generate

```bash
python tools/generate_ds_runtime_bundles.py
```

The runtime entrypoint reads `tools/ds_codegen/runtime_bundles.json`, verifies
the exact provenance of every declared source, compiles every bundle into a
staging directory, verifies that the source trees did not change during the
run, and then replaces the complete `src/dsctl/generated/` namespace. All
declared source roots must therefore be present before regeneration starts.
The same atomic pass writes the materialized version, task-definition,
task-definition-cleanup, workflow, runtime-instance, and datasource profiles
alongside every exact package. The cleanup profile is an installed-wheel
release-gate input for the exact `2.0.0`, `2.0.9`, `3.0.0`, `3.0.6`, and
`3.1.0` slices with project-wide task deletion, plus the `3.1.9` slice with
workflow-binding and version-history proof only. The `3.1.9` recipe has no task
delete operation. This private semantic root does not enter the stable action
ledger. The same atomic pass writes the task-authoring profile described below.

By default, the generator prefers the local exact-contract cache under
`build/ds_contract/snapshots-v2`. This does not bypass source verification: it
first requires every configured worktree to be a clean matching release tag,
then requires the snapshot's embedded tag, commit, and tree to match that
current worktree, verifies the snapshot contract digest, and requires the
fingerprint of the current extractor and Java parser. A missing, stale, or
incomplete preferred snapshot falls back explicitly to exact-source extraction
and is refreshed atomically only after the complete bundle set compiles.

Use `--snapshot-mode require` for a fast fail-closed cache-only run. Use
`--snapshot-mode source` after changing extraction rules, or whenever a clean
exact-source proof is required; this mode ignores all snapshots and does not
rewrite the cache. Follow it with the default `prefer` mode to replace stale
snapshots atomically after the complete bundle set compiles. The default
`prefer` mode reports every fallback and the final snapshot/source counts.

Current tracked bundles:

| Version | Selection | Generated scope |
| --- | --- | --- |
| `1.3.9` | `runtime-slice` | exact reviewed semantic closure for the legacy name/ID dialect |
| 36 declared releases from `2.0.0` through `3.4.3` | `runtime-slice` | exact reviewed semantic closure for each listed code-based release |

The runtime inventory now contains all 37 exact releases. Runtime-slice roots
are compiler inputs, not support claims: a generated operation remains
unreachable when the exact version profile marks its action unsupported or no
reviewed domain adapter binds it. The public `list` actions map to internal
`page` semantics. Rendering includes the source operations and types in each
reviewed transitive closure, including lightweight reference and attached-
schedule reads required by `workflow.get`; those dependencies do not imply a
public schedule action.

The `1.3.9` definition-read slice is intentionally distinct. It selects the
name/ID-era project, process-definition, and schedule operations, while later
profiles select their exact code-based package. The extractor projects the
legacy internal `PageInfo<T>` to the four fields that
`BaseController.returnDataListPaging` actually serializes:
`totalList`, `total`, `totalPage`, and `currentPage`. This is a guarded source
projection, not a permissive alias for the internal Java fields.

Earlier checkpoints compiled `3.2.2` from four project/workflow read semantics
and `3.4.2` from a narrow identity/project/workflow/schedule/task root set.
Those descriptions remain useful when interpreting their historical live
receipts, but they are not the current capability boundary. The current slice
roots come from accepted profile decisions across the full stable action
surface. Generated semantic operations remain wire-closure inputs rather than
one-to-one public CLI actions: for example, `page` commonly backs public
`list`, and one action may require several selector and readback operations.

Task-definition contracts are selected from exact controller and service
behavior. `1.3.9` cannot map its string-native task identity to the stable
integer code/version contract. `2.0.0`–`2.0.3` use complete workflow updates
because their standalone routes cannot preserve relation-bound versions;
`2.0.3` additionally rejects dependency changes. `2.0.4`–`3.2.0` support
standalone ordinary-field updates only after an exhaustive single-workflow
binding proof at prepare and apply, and reject explicit dependency edits.
`3.2.1` and newer support dependency updates subject to their exact guards.
The [admission record](stable-release-admission.md#differences-retained)
explains the service defects that invalidate earlier representative-version
assumptions. Endpoint parameter presence alone does not establish support.

The task update source contract is intentionally exact rather than normalized
to the newest declaration. In `3.4.1`, `updateTaskWithUpstream` declares raw
`Result`, and the successful no-op branch can serialize `data: null`; a
source-scoped generator correction therefore emits a strict integer-or-null
logical result. In `3.4.2`, the declared `Result<Long>` emits a strict integer.
Both versions use the project-scoped main controller operation; the tracer does
not retain the removed V2 route. Here “tracer” names the historical path that
first proved this contract; the path is now part of the all-version task domain.

Controller response assembly is part of the extracted wire contract.
`returnDataList(serviceResult)` serializes the service map's `DATA_LIST` value
directly as outer `Result.data`, so the transport unwrap is sufficient and the
generated operation uses a direct payload. `Result.success(resultMap)`, on the
other hand, serializes the map itself and may legitimately generate a second
field projection. Regression tests use the exact `3.4.1` workflow create/update
flow to prevent these cases from collapsing into one heuristic.

Runtime-only bindings pass the same fail-closed semantic validator as read
compatibility analysis. Selector-free identity is declared explicitly;
project update/delete each include both name-resolution paging and direct-code
get operations before their mutation, so a future per-action slice cannot
silently omit one selector path.

Every generated package manifest records its selection mode, semantic
operations, exact source tag/commit/tree, source and rendered contract digests,
and generated surface counts. Do not edit these manifests or refresh only one
tracked package by hand.

Generated artifact ownership is deliberately split:

- `runtime_bundles.json` owns the 36-package inventory and exact source
  selection;
- `version_profile_decisions.json` owns reviewed semantic-operation and action
  decisions;
- `conformance_bundles.json` owns named support-tier action closures and
  inheritance, while intentionally owning no promotion or live-evidence claim;
- the unified runtime generator writes every admitted exact-version package plus
  `generated/version_profiles.py`, `generated/conformance_bundles.py`,
  `generated/datasource_profiles.py`, `generated/task_definition_profiles.py`,
  `generated/task_definition_cleanup_profiles.py`,
  `generated/workflow_profiles.py`, `generated/runtime_instance_profiles.py`,
  and `generated/version_discovery.py`;
- `version_discovery.py` derives bootstrap paths and response fields from full
  controller snapshots, and validates OpenAPI configuration plus database version
  initialization against exact source. It preserves the legacy plugin route
  collision and the stock `3.2.2` → `3.3.0` metadata mismatch; bootstrap probes
  add no public actions or domain operations;
- `task_definition_cleanup_contract.py` owns the schema-4 private cleanup
  profile: its auxiliary semantic root, exact source-operation membership,
  full-core reconciliation and recovery boundaries, and each
  inventory/detail/delete-or-history recipe, without adding a stable action.
  A separate pre-delete release capability records the `2.0.2` and `2.0.3`
  requirement to take an enabled task offline before deleting it. Other direct
  deletion and workflow-cascade proof-only recipes retain their own boundaries;
  it also marks cleanup-owned page and task identity integers for generated
  `StrictInt` rendering so boolean or floating-point wire values cannot be
  coerced before ownership checks;
- `datasource_profile_reviews.json` records source-reviewed plugin DTO fields;
  exact enum aliases, base fields, and the final sensitive-field set are
  compiled into the generated datasource profile;
- `task_profile_facts.json` records mechanical all-version task-plugin facts,
  while `task_profile_reviews.json` records typed-authoring decisions. Each
  positive decision is guarded by the exact Git tree recorded in the facts and
  names structured repository-relative evidence paths. The task-profile
  compiler validates those paths when exact source roots are supplied. A
  profile-local `typed_authoring_exclusions` entry may bind one deliberate
  runtime exclusion to the same exact task fingerprint and structured source
  evidence without creating a positive typed membership. The tree guard,
  evidence paths, and exclusion decision remain generator-only review metadata;
- `generate_ds_task_profiles.py` writes
  `src/dsctl/generated/task_profiles.py`; the canonical atomic runtime
  generator emits the same artifact. `_TASK_PROFILE_JSON` uses indented,
  named records and searchable hexadecimal fingerprints. Review narrative and
  evidence paths remain in the compiler ledgers. Public `TASK_PROFILES` retains
  the selected models, semantic fingerprints, review/reason identities, and
  exact membership decisions. Freshness tests independently rebuild and compare
  this complete runtime projection, and compiler tests still require valid
  evidence even though narrative-only edits no longer change runtime bytes.

`generated/version_profiles.py` also uses indented, named records. It shares
complete repeated decisions and field collections through readable keys,
retaining separate materialized exact profiles. A shared record's first
operation/version label is a storage key, not an inherited compatibility
baseline. The renderer has no positional tuple or Base64 fingerprint decoder.
Repeated generated programs and exact enum identities remain independent where
their current structure already makes ownership clear; fewer lines alone do
not justify additional indirection.

No artifact substitutes for another: package inventory does not declare
support, terminal action decisions do not prove source freshness, and complete
task-plugin extraction does not expose a typed authoring facet.

For one-source extraction, analysis, or an untracked sample package, use:

```bash
python tools/generate_ds_contract.py \
  --package-output build/ds_contract/package_sample
```

That command reads the version from the DolphinScheduler Maven POM and writes
development artifacts under `build/`; it is not the committed multi-bundle
regeneration path.

## Freshness

Run the freshness check after changing generator logic, the runtime manifest,
or tracked generated code:

```bash
python tools/check_generated_freshness.py
```

Freshness validates the configured bundle inventory and every generated
package as one unit, so a missing, stale, or unexpected exact-version bundle
fails the check. Its cache key includes the runtime manifest, generator inputs,
snapshot mode and contents, plus each declared source's clean exact Git tag,
commit, tree, version, and selection. A hit skips both contract extraction and
rendering. On a miss, the default mode uses the same verified snapshot path as
the generator and rechecks all inputs before replacing the freshness cache.

For the independent clean exact-source gate, run:

```bash
python tools/check_generated_freshness.py --snapshot-mode source
```

`source` mode deliberately bypasses both contract snapshots and the freshness
output cache, so every invocation recompiles all configured source trees.

The umbrella freshness check also compares the independently generated task
profiles. Use the focused check while editing only task-profile facts or
reviews:

```bash
python tools/generate_ds_task_profiles.py --check
```

Terminal action coverage is a separate materialized-profile gate:

```bash
python tools/analyze_ds_support_coverage.py --require-complete
```

It reads `src/dsctl/generated/version_profiles.py` and fails while any reviewed
action/version coordinate is pending. It does not prove that the generated
module is fresh, that a supported action reaches its expected runtime seam, or
that an experimental profile has live evidence.

The named static conformance projection is checked independently:

```bash
python tools/analyze_ds_conformance_bundles.py \
  --check-generated \
  --require-assessed \
  --output build/ds_contract/conformance-assessment.json
```

The generated module records a normalized catalog digest, per-bundle action
digests, and a canonical digest over the complete assessment. The current
`static-action-closure-only` result is not a facet, scenario, freshness, live,
support-level, `tested`, or promotion claim.

Audit runtime reachability independently:

```bash
python tools/analyze_ds_stable_action_dependencies.py
python tools/analyze_ds_stable_action_version_matrix.py
```

The first command must resolve every stable action in the command catalog to
its local, diagnostic, legacy, generated-domain, or typed-wire dependency path.
The second projects the resulting `3.4.1` source-operation closure across all reviewed exact
contracts. Its output contains mechanical review candidates only; it neither
overrides the reviewed decision ledger nor declares support. Do not describe
either gate as passed merely because the profile compiler has no pending
coordinate.

## Version Diff

Use the version diff analyzer before adding support for another
DolphinScheduler release:

```bash
git -C references/dolphinscheduler worktree add ../../build/upstream/ds-3.3.2 3.3.2
python tools/analyze_ds_version_diff.py \
  --ds-source 3.4.1=references/dolphinscheduler \
  --ds-source 3.3.2=build/upstream/ds-3.3.2 \
  --base 3.4.1 \
  --target 3.3.2 \
  --format markdown \
  --output build/ds_contract/diff-3.4.1-to-3.3.2.md
```

Generated reports and snapshots belong under `build/`, not `docs/`, unless a
specific reference document is intentionally promoted and reviewed.

## Exact-Version Inventory

Generate one deterministic inventory while retaining every successful raw
snapshot for cached diff and impact runs:

```bash
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
  --ds-source 3.4.1=references/dolphinscheduler \
  --ds-source 3.4.2=build/upstream/ds-3.4.2 \
  --snapshot-dir build/ds_contract/snapshots \
  --output build/ds_contract/multi-version-inventory.json
```

Each input is compiled independently. Exit `0` means the inventory is
complete, exit `1` means the JSON was written with one or more per-version
diagnostics, and exit `2` means the invocation or output itself was invalid.
An input label must be the exact version found in that snapshot or source tree.
An exact target is accepted only from a clean Git worktree whose checked-out
commit is pointed to by the matching release tag, or from a cached snapshot
that carries the same verified origin provenance. A dirty worktree, a tag/label
mismatch, a non-Git source directory, or a legacy snapshot without provenance
is retained as a per-input diagnostic instead of being silently presented as
an exact target.

Every successful inventory target records the current input kind and content
digest, the extracted contract digest, and its originating Git commit, tree,
tag, and ref. `--snapshot-dir` embeds that provenance in each cached snapshot;
loading the cache verifies that its contract digest still matches, then records
`input_kind: snapshot` while preserving the original Git evidence. This makes
cached inventory and diff runs auditable without treating a filename or Maven
version string as proof of exact source identity.

Once the inventory exists, map reviewed semantic read bindings onto it without
rescanning upstream source:

```bash
python tools/analyze_ds_compatibility_impact.py \
  --inventory build/ds_contract/multi-version-inventory.json \
  --output build/ds_contract/read-compatibility-impact.json
```

This compatibility-impact command is the original, intentionally scoped read
analyzer. Its reviewed dataset is `project.page`, `project.get`,
`workflow.page`, and `workflow.get`; it is historical architecture evidence,
not the current product support ledger. Each version binding records the
complete operation chain used by name/native-identity selection, including the
lightweight workflow-reference and attached-schedule reads used by
`workflow.get`. Selector metadata distinguishes inputs consumed by an action
from identities exposed for later discovery. Bindings also retain controller
and UI evidence plus an explicit transitive closure of request and response
models, DTOs, and enums. The analyzer rejects incomplete reviewed bindings,
fails closed when any closure member is absent, and groups only equal
operation-and-type closure fingerprints. Those groups are wire-compatibility
candidates only; canonical-model adaptation, contract tests, and live evidence
still decide semantic reuse and support.

Raw source diffs are only the first compatibility stage. Before sharing a wire
bundle or semantic recipe, classify whether each change is reachable from a
stable domain operation, map reachable changes to CLI actions, and require the
live evidence defined by the selected version profile. The product compiler now
derives operation/type closures for the reviewed 153-operation decision ledger;
the four-read analyzer above keeps its own narrower manual input because it is a
standalone historical analysis tool. Matching complete snapshots are
sufficient neither to prove semantic compatibility nor to skip an exact-version
read smoke.

## Task-Plugin Inventory and Impact Review

Workflow task parameters are a separate authoring contract embedded inside
REST workflow payloads. They are intentionally not added to the
controller-oriented `ContractSnapshot`. Prepare the tracked 36-version source
matrix, then generate their own exact inventory:

```bash
python tools/prepare_ds_source_matrix.py

python tools/generate_ds_task_plugin_inventory.py \
  --source-manifest tools/ds_codegen/exact_sources.json \
  --snapshot-dir build/ds_task_plugins/all-snapshots \
  --output build/ds_task_plugins/all-version-inventory.json
```

The source-contract lane optionally audits this historical inventory against the
tracked task-profile facts, including its exact provenance and extractor
fingerprint. It skips that audit when the inventory is absent; runtime source
preparation does not create this separate artifact. The portable lane excludes
this audit. Tracked fact/review attestations and generated task-profile freshness
remain checked without the optional inventory.

`tools/ds_codegen/exact_sources.json` is the mechanical analysis corpus, not
the runtime bundle plan. Its loader rejects a missing, duplicate, unexpected,
or reordered target, while the prepare step rejects any source root that is not
a clean Git checkout at the matching release tag. This keeps source coverage
at exactly `1.3.9` through `3.4.3` without changing runtime bundle selection or
making a support claim. Pass a local checkout URL with `--remote file:///...`
to prepare from an existing upstream object store without modifying that
checkout.

Omit `--task-type`, as above, to inventory every mechanically resolved server
binding. Add `--task-type SQL` for a focused SQL slice, or repeat the option to
select several task types. The extractor follows the source's actual binding
chain instead of guessing from filenames:

- legacy `TaskParametersUtils.getParameters()` switch cases cover `1.3.x` and
  legacy-only `2.x` task types;
- `TaskChannelFactory.getName()` plus the channel/task deserialization chain
  covers plugin-backed `2.x` and SPI-based `3.x` task types;
- when both paths exist, the worker plugin binding is primary and a type found
  only in the legacy registry is retained.

Only Maven production sources under `src/main/java` participate. Factories
marked `VisibleForTesting` are excluded even when an older upstream release
placed them in a production source set. A production factory, channel, or
legacy case that cannot be resolved emits a diagnostic and makes
`source_complete` false; it is never silently treated as absent. The same gate
applies when any discovered model or enum closure member cannot be extracted.
`source_complete: true` means only that these mechanical source facts were
resolved for every declared target. Semantic review, runtime profile support,
and live verification remain independent gates.

For each binding, the snapshot records the full parameter-model closure,
including inherited and nested models, serialized names and Java types,
referenced enums, exact source
paths, and structural fingerprints for `checkParameters()` and resource
methods. Getter, comment, formatting, and Lombok churn changes source evidence
without manufacturing a semantic field diff. The extractor does not convert
arbitrary Java validation into `required`, UI defaults, compatibility claims,
or CLI actions.

Serialized field names follow Jackson's wire contract. For a Java field,
`@JsonProperty("name")` and `@JsonProperty(value = "name")` take precedence
over `@Schema(name = "...")`, which in turn takes precedence over the Java
bean name. This ordering is covered by extraction tests because using a schema
display name or bean name in place of `JsonProperty` changes task fingerprints
and can make a reviewed authoring membership stale. Refreshing those mechanical
facts does not itself add a typed facet, update live evidence, or promote a
profile.

Compare source trees or cached task-plugin snapshots independently from the
REST diff:

```bash
python tools/analyze_ds_task_plugin_diff.py \
  --ds-source 3.4.1=references/dolphinscheduler \
  --ds-source 3.4.2=build/upstream/ds-3.4.2 \
  --base 3.4.1 \
  --target 3.4.2 \
  --task-type SQL \
  --format markdown \
  --output build/ds_task_plugins/sql-3.4.1-to-3.4.2.md
```

The raw report distinguishes `source_complete` from `review_complete`. Every
semantic change receives a stable review ID, while newly observed facts remain
unmapped and fail the review gate. A reviewed impact file then declares each
decision's CLI impact, user-facing facets, implementation status, and affected
actions. The review is guarded by both normalized contract fingerprints and
the exact Git source-tree identities for its base and target:

```bash
python tools/analyze_ds_task_plugin_diff.py \
  --ds-source 3.4.1=references/dolphinscheduler \
  --ds-source 3.4.2=build/upstream/ds-3.4.2 \
  --base 3.4.1 \
  --target 3.4.2 \
  --task-type SQL \
  --review tools/ds_codegen/task_plugin_reviews/sql-3.4.1-to-3.4.2.yaml \
  --format json \
  --output build/ds_task_plugins/sql-3.4.1-to-3.4.2-reviewed.json
```

The exact source-tree guard covers evidence outside the parameter model, such
as execution code and UI authoring/serialization paths. It therefore becomes
stale when those reviewed files change even if the normalized task parameter
shape does not. Reviews without both exact endpoint identities cannot make
such external evidence claims. Each decision names the evidence planes it
depends on. When both source trees are supplied, the analyzer also verifies
that every declared repository-relative evidence path exists in at least one
of those trees; exact cached snapshots retain the tree-identity guard.
With `--review`, an incomplete review returns exit status `1`; malformed,
stale, or non-exact review evidence returns `2`.

The focused `3.4.1 -> 3.4.2` review classifies inline SQL as the shared facet
and resource-file SQL as a new `3.4.2` facet. Across the current matrix,
separate exact reviews enable forty-two distinct typed families and 902
exact memberships, including eleven on `1.3.9`. The older source key `SUB_PROCESS`
explicitly maps to the canonical CLI key `SUB_WORKFLOW`. Coordinates without a
positive review remain on their opaque authoring/preservation paths. The
`3.4.2` and `3.4.3` resource-file facet is narrower and opaque-preserve-only: public typed
create/edit remains unavailable. The focused SQL review records the official
UI's defaults and serializer mismatch as reviewed evidence. A changed
fingerprint, missing change ID, duplicate mapping, or stale mapping fails
closed. Completing this review does not itself promote the facet or version;
profile materialization, contract tests, preservation tests, and live evidence
remain separate gates.
The matrix governance test also derives the fully unreviewed remainder from
source task types rather than canonical aliases and now locks it at zero.

Per-family support ranges, native wire epochs, worker prerequisites and
preservation restrictions are maintained in
[Reviewed task authoring boundaries](task-authoring-boundaries.md), alongside
links to the exact facts and review decisions. Keep these rules there rather
than copying a second family matrix into the code-generation guide. When a
release changes, update its own reviewed evidence, then regenerate every
admitted profile atomically; shared models do not widen exact membership.
The [final-release admission record](stable-release-admission.md) documents the
21 added profiles and the source differences found during their review.

## Exact Target Source Matrix

The compatibility source matrix enumerates deterministic manifests for these
37 exact tags:

```text
1.3.9
2.0.0  2.0.1  2.0.2  2.0.3  2.0.4  2.0.5  2.0.6  2.0.7  2.0.8  2.0.9
3.0.0  3.0.1  3.0.2  3.0.3  3.0.4  3.0.5  3.0.6
3.1.0  3.1.1  3.1.2  3.1.3  3.1.4  3.1.5  3.1.6  3.1.7  3.1.8  3.1.9
3.2.0  3.2.1  3.2.2
3.3.1  3.3.2
3.4.0  3.4.1  3.4.2  3.4.3
```

Extraction success is not a support claim. It is the static prerequisite for
building and reviewing one.

See [Version Compatibility](../user/version-compatibility.md) for the support
policy.
