# Tooling

Repository tools live under `tools/`. They are development and release helpers,
not runtime modules shipped as part of the `dsctl` import package.

`tools/run_exact_profile_read_gate.py` is the public single-version,
installed-wheel read gate for all admitted exact profiles. It takes only explicit
wheel/profile/attestation/cluster/fixture/evidence inputs and deliberately does
not own SSH, cluster activation, persona provisioning, fixture mutation, or
matrix rotation. `tools/live_gate/exact_profile_read_evidence.py` independently
validates the current semantic schema-2, secret-free receipt and retains exact
schema-1 validation only for historical audit. Shared input snapshotting,
isolated wheel installation, environment scrubbing, direct-target binding, and
no-overwrite publication also live under `tools/live_gate/`; the two public
runners remain thin scenario-specific wrappers.
`tools/run_exact_profile_live_gate.py` and `tools/exact_342_evidence.py` own the
separate current mutating `3.4.2` schema-7 release contract while retaining
schema-v3/v4/v5/v6 validation for historical receipts.

The mutating runner, fixture projector and promotion checker require
`--version`. `tools/live_gate/exact_profile_policy.py` selects an explicitly
reviewed scenario; currently `external-shell/v1` is reviewed only for `3.4.2`.
Unreviewed versions are rejected before fixture access. Common execution lives
in `exact_profile_runner.py`, `exact_profile_fixture.py` and
`exact_profile_promotion_evidence.py`; the former version-named entry points
are retired. The historical `exact_342_evidence.py` schema validator remains
because its receipt lineage and digest obligations are real compatibility
contracts. The broader `full_core/v1` action set does not establish the external
SHELL fixture's restoration obligations.

`tools/live_gate/conformance_bundle_evidence.py` exposes the receipt-validation
interface. Its `conformance_evidence/` package separates current-source reading,
coverage assessment, binding and provenance checks, receipt checks and ordered
trace validation. Candidate source is inspected statically. The existing live
scenario runner still owns execution and failure cleanup as one operation.

`tools/project_exact_profile_matrix_fixture.py` projects an existing matrix-owned,
REST-verified schema-1 `3.4.2` fixture plus its private ready-state and strict
image-inspection files into the schema-2 manifests consumed by the mutating
gate. The inspection record binds the service tag, local image ID, inspected
`RepoDigests`, and the uniquely selected digest; the CLI intentionally has no
bare `--image-digest` input. It is intended for an active warm lease where
changing the matrix repository or resident topology would create management
drift. The helper cross-validates and sanitizes fixture inputs; it does not
execute the candidate wheel or observe candidate requests, so its output is
fixture preparation rather than candidate wire evidence. Current schema-7
evidence must record `dsmatrix-exact-read-state-projection/v2`; projection/v1
receipts remain historical and cannot satisfy the current promotion checker.
The untracked first campaign attempt used projection/v1 and was withdrawn when
review showed that its bare RepoDigest was not bound to the inspected image ID;
the projection/v2 r2 receipt below is archived historical evidence and cannot
satisfy the current checker after later profile changes.

`tools/project_conformance_matrix_fixture.py` is the separate all-version
projector for named conformance bundles. It accepts the matrix's unchanged
generic schema-1 cluster and fixture manifests, private ready state, and a
schema-2 image-inspection discriminated union. `registry-digest/v1` requires a
selected API-repository `RepoDigest` that occurs exactly once. A tag-only
reference requires one matching repository digest; an explicit digest pin may
disambiguate multiple unique matching digests.
`managed-image-lock/v1` is restricted to `1.3.9`, `2.0.0`, `2.0.4`, `2.0.7`,
`2.0.8`, `2.0.9`, `3.0.2`, and `3.1.2` and
binds a clean matrix-management revision, exact lock-file digest and unified
lock row, and the inspected archive/digest and label facts. Other versions
have no managed-image fallback and fail closed without registry-digest proof.
The output cluster manifest is schema 2 and retains the normalized proof under
`image_provenance`; the conformance fixture manifest remains schema 1.

`tools/live_gate/conformance_image_contract.py` is the declarative repository,
managed-version, and lock-presence contract shared with independent evidence
validation, while `conformance_image_provenance.py` owns strict projection and
runner validation. The projector requires owner-private, non-symlink inputs
and atomically publishes a new 0600 output pair without overwriting either
destination. It validates prepared fixture input but neither collects control-
plane facts nor executes the candidate wheel. Image inspection collection is
an external version-matrix orchestration responsibility; a temporary campaign
collector is not a tracked long-term CLI contract. The exact command and both
inspection shapes are documented in [Live Testing](live-testing.md#named-conformance-bundle-installed-wheel-gate).

`tools/check_exact_profile_read_evidence.py` is the thin entry point for the
tracked promotion corpus under
`docs/development/live-evidence/exact-read/<version>/`. Its reusable checker
lives in `tools/live_gate/exact_profile_read_corpus.py` and is part of the
release quality gate. It requires exactly one current receipt for every exact
version, schema 2 for every receipt, one shared wheel filename and full SHA-256,
current generated contract and semantic profile bindings, the four current
read-recipe fingerprints, and explicit installed-wheel `live_smoke`
verification for `project.list|get` and `workflow.list|get`. Missing, extra,
schema-1 historical-only, stale, mixed-wheel, mutating, or secret-bearing
evidence fails closed. Candidate source roots are inspected through AST literal
and JSON parsing; the checker never executes generated Python while validating
evidence.

`tools/check_conformance_bundle_evidence.py` is the public fail-closed checker
for `docs/development/live-evidence/conformance-bundles/<version>/`. A current
campaign requires exactly one highest-ready named-bundle receipt for each of
the admitted exact versions; with the current static assessment that means 37
`full_core/v1` receipts. Every receipt must bind the same wheel
basename and full SHA-256 plus current source assessment, profile, manifest,
action-recipe, scenario, cleanup, and sanitization facts. Missing, extra,
lower-bundle, legacy-only, stale, mixed-wheel, wrong-wheel, or secret-bearing
evidence fails closed. The release quality gate and artifact publication checks
run this checker without an allowance.

`tools/check_exact_profile_promotion_evidence.py` governs the separate mutating
`3.4.2` claim. Its checker requires the fixed 15-action gate to be either
entirely unclaimed or entirely `live_smoke`; a partial claim always fails. Once
all 15 actions are promoted, the default checker requires exactly one current
schema-7 receipt bound to the current semantic profile fingerprints, action
verifications, decision recipes, generated manifest, package version, wheel
filename, and full wheel SHA-256. The explicit
`--allow-missing-current-receipt` mode is pre-campaign only: it accepts only
the one state where final metadata is complete and no schema-7 receipt exists,
and it becomes an error as soon as that receipt is present. Historical schema
3/4/5/6 receipts remain auditable but cannot satisfy current promotion.

Historical campaign `promotion-f328d3e2-schema6-r2-20260810` produced the
archived receipt at
`docs/development/live-evidence/history/3.4.2/2026-08-10-f328d3e2d6ba.json`
from wheel
SHA-256
`f328d3e2d6ba261c26f9f74decb12a9229b5d471b6f2efdd2a07d55b116af06f`.
It records 48 operations (45 successes and three expected negative paths), all
15 action verifications as `live_smoke`, and exact external `SHELL` task
restoration. Task-update recipe/profile changes made it stale for the current
source. Campaign `conformance-e8eacee57af9-r12-20260811` produced a later
schema-6 receipt with wheel SHA-256
`e8eacee57af9a9d2659194b05bccbbd7680eb10c310c4189d9cfa3ae8b5c9152`;
the checker passed its 15-action bundle and restoration contract for that
recorded manifest. The expanded manifests make it historical, so a future
campaign must use a new canonical wheel and matching receipt. This checker
promotes per-action `live_smoke` evidence only:
`3.4.2` remains experimental with `tested=false`, and `3.4.1` remains the sole
stable profile.

`tools/check_release_artifacts.py --wheel-only <wheel>` is the public artifact
preflight required before an installed-wheel live campaign. It compares the
complete wheel runtime with current `src/dsctl`, then verifies Core Metadata,
entry points, the exact semantic manifest, and `RECORD`. Campaign candidate
`promotion-e1f1832fc79d-r6-20260809` demonstrated why this check precedes live
testing: although its `180/180` behavior sweep passed, the preflight found eight
deleted service modules copied from a stale `build/lib` tree and README Core
Metadata drift, so its promotion status was revoked. Historical governed campaign
`promotion-167723bbe11d-r7-20260809` used an isolated clean-source build, passed
the wheel-only preflight first, and then produced the archived 15-version
exact-read corpus from its `180/180` sweep. Current profile fingerprints have
changed, so those receipts remain historical. Campaign
`conformance-e8eacee57af9-r12-20260811` produced a replacement 15-version
exact-read corpus and the separate 15-version highest-ready conformance corpus
with the same immutable wheel; both checkers passed for their recorded
manifests. The later all-version compatibility expansion changes those
manifests, so neither corpus is evidence for the expanded coordinates.

The full `tools/check_release_artifacts.py --tag ... <wheel> <sdist>` path first
validates both artifacts, then calls the public conformance-corpus checker with
the candidate wheel basename, full `sha256:` identity, and candidate source
root. It separately validates the exact-`3.4.2` promotion receipt. Wheel-only
preflight intentionally consults neither live-evidence source. This ordering
lets one canonical wheel be built and preflighted before its live receipts
exist, while making the final release pair fail closed on absent, stale, or
wrong-wheel evidence.

## Script Naming

Use these prefixes consistently:

- `check_*.py`: quality gates that exit non-zero on failure
- `generate_*.py`: generators that write code or generated artifacts
- `analyze_*.py`: read-only analysis that writes reports or stdout
- `extract_*.py`: inventory or matrix extraction from upstream or local source
- `audit_*.py`: review reports for governance and policy checks

Keep reusable implementation code inside a tool package such as
`tools/ds_codegen/`; keep thin command-line wrappers at `tools/*.py`.

## Test and Quality-Gate Lanes

Run tooling from the project virtual environment, for example
`.venv/bin/python tools/check_quality_gate.py --mode development`. Its isolated
Python subprocesses must resolve the same supported runtime dependencies.
Portable process tests start temporary HTTP servers on `127.0.0.1`; the execution
environment must permit local loopback listeners.

The repository has three mutually exclusive non-live test lanes:

- `python -m pytest -m "not live and not source_contract and not source_rebuild" -q`
  is the portable
  lane. It must run from a clean checkout and must not read ignored exact-source
  worktrees or generated contract snapshots.
- `python -m pytest -m "source_contract and not live" -q` is the explicit
  source-contract lane. It tests compatibility against the validated exact
  source/snapshot corpus prepared under `build/`, without the full source
  re-extraction and package-reproduction campaigns.
- `python -m pytest -m "source_rebuild and not live" -q` is the source-rebuild
  lane. It re-extracts complete Java contracts and reproduces full or sliced
  packages, including the 36-release source-versus-snapshot comparison.

Both source lanes require explicitly prepared inputs. Missing inputs are a
failed prerequisite, not a skipped test or a fallback to `references/`.
Collection rejects overlapping markers and unmarked consumers of the corpus.

Positive whole-domain composition tests share one session compilation of the
complete current registry through `compiled_all_domains`, with a deep-isolated
copy per test module. This shares preparation only: each test retains its own
expected plans, ownership assertions, and separately compiled subset baseline.
Negative tests compile their modified inputs directly; the production compiler
does not cache test results. A negative case confined to one exact release passes
only that modified bundle. Whole-matrix positive checks, cross-version schema
collisions and domain ownership checks retain their complete input sets.

Each quality-gate step reports elapsed time, including failures, and each pytest
lane reports its 20 slowest tests. Use these measurements to select focused
feedback during implementation, then run the complete gate on the settled
candidate. Sharing preparation must not replace independent expected results or
the all-version cold source rebuild.

Source rebuilds compare and render one release at a time. Parsed Java syntax
trees and source indexes live within one extraction and are released on success
or failure; the prepared session corpus remains a separate baseline cost.
Compiler graph preparation is shared only for the same snapshot object during
one compilation. Neither cache carries results into a later changed input.
Receipt-only release tests share their source/archive baseline and isolate their
receipt files; tests that mutate source or archive bytes retain separate inputs.

`python tools/check_quality_gate.py` defaults to `--mode development`: static
checks, generated freshness and type checks, static conformance assessment, and
all three test lanes. `--mode release` adds the three current-wheel evidence
checks and rejects every skip or partial-lane option. Development success makes
no live-evidence or promotion claim. See [Release](release.md#local-gate) for the
build-once sequence.

`--portable` uses only inputs present in a clean tracked checkout. After the
complete development gate runs on the primary Python version, additional CI
interpreters use `--portable --tests-only`. CI delegates to this tool so its YAML
contains scheduling policy rather than another list of quality commands.
A separate Python 3.11 dependency matrix installs minimum and known-current
runtime versions and checks command parsing/discovery, process journeys,
generated defaults and every exact task authoring catalog. Both rows include
external Click 8.5 to verify Typer's vendored boundary. See
[Dependency compatibility](dependency-compatibility.md) for the supported floor
and the reproduced failure that set it.
Diagnostic skip options print a partial result; they cannot pass the release
gate. A source-contract-only run shortens feedback but does not replace source
rebuilds in development CI or release validation.

### Migration Batch Workflow

Separate shared transport/compiler extensions from batches that reuse an existing
request shape. Group compatible small domains into one batch instead of repeating
the complete integration cycle for each domain. This changes work scheduling, not
exact-version review, support claims, or release obligations.

Each batch has one owner for the frozen baseline and final acceptance. Record the
base commit and freeze independently authored behavior tests before switching the
runtime implementation. Run those same test files and collection identities against
the fixed old checkout and the candidate in the required dependency environments.
Other contributors use targeted feedback; they do not recreate the baseline.
Candidate extraction may suggest declarations, but never supplies its own expected
behavior or decides compatibility.

Before freezing ownership, inventory incoming consumers as well as the selected
domain's local sources. A controller read may be used by a package-backed domain
whose filename does not resemble the provider. Check each consumer's complete
source closure and actual runtime port; introduce a narrow reviewed auxiliary
root when it consumes only part of the provider's public resolver recipe. Make
version-limited dependencies explicit rather than inferring them from missing
sources, and preserve each caller's existing projection policy.

Use relevant tests and static checks during implementation. After changes and
independent review have settled, run the required final gates against the frozen
candidate. Changes after that point invalidate the affected checks; rerun those
checks explicitly rather than treating earlier results as current. Test-only
preparation changes do not require rebuilding an unchanged runtime wheel, and a
targeted local check is not a claim that the full CI/release gate passed.

Keep disposable migration drivers, baseline checkouts, logs, and cost reports
outside the repository and installation artifact. Record batch scope, elapsed
time, aggregate main/sub-agent token usage when available, validation performed,
and reasons for rework. Distinguish cached input from uncached input and do not
equate raw token counts with billed cost. Compare equivalent work before claiming
an efficiency improvement.

## Compatibility Inputs and Generated Artifacts

Generated reports, snapshots, package samples, and temporary upstream worktrees
belong under `build/`.

The tracked `tools/ds_codegen/exact_sources.json` file currently declares a
36-version source corpus used by mechanical compatibility analyzers. An
alternative source manifest may include unreviewed candidate releases.
`tools/prepare_ds_source_matrix.py` materializes its exact-tag worktrees under
`build/upstream/`; it is independent from runtime bundle selection and does not
assert semantic or live support.

Tracked inputs and generated outputs have non-overlapping ownership:

| Path | Owns | Does not own |
| --- | --- | --- |
| `tools/ds_codegen/runtime_bundles.json` | the 36-package runtime inventory and exact source selection | action support or task-authoring facets |
| `tools/ds_codegen/version_profile_decisions.json` | reviewed semantic-operation decisions and terminal stable-action states | bundle packaging or live promotion |
| `tools/ds_codegen/conformance_bundles.json` | named support-tier action closures and inheritance | facet, scenario, freshness, live-evidence, support-level, or tested claims |
| `tools/ds_codegen/exact_sources.json` | mechanical source inventory, independently extensible | reviewed membership, runtime selection or support |
| `tools/ds_codegen/task_profile_facts.json` | exact mechanically extracted task-plugin facts | typed-authoring support |
| `tools/ds_codegen/task_profile_reviews.json` | reviewed task-authoring facet decisions | REST operation support |

`source_matrix.py` loads the source inventory; `profile_ledger.py` derives the
reviewed set from the version decision ledger; `runtime_bundle_manifest.py`
loads the packaging selection. Consumers use the set owned by their boundary.
The runtime generator rejects unreviewed selected versions before source I/O,
then requires complete selected domain, task, and datasource profiles.

`tools/analyze_ds_version_diff.py --scope cli` compares a candidate against a
reviewed base's consumed static closure without admitting the candidate. It
accepts a source manifest or exact source/snapshot inputs, loads only selected
versions, and rejects stale cached extraction evidence. See
[candidate analysis](codegen.md#analyze-a-candidate-release) for commands and
the distinction between equality, contract differences, and incomplete evidence.

`tools/generate_ds_runtime_bundles.py` is the unified REST runtime generator. It
compiles 21 domain plans from the original exact bundles, strips their local
operation ownership union, and atomically replaces the exact enum/provenance
slices, compiled programs and shared runtime support. Closed identical enum
implementations are content-addressed under `wire_programs/_schemas/_enums/`;
exact root schemas and complete source closures retain independent identities.
The retired family catalog and wrapper-kernel compiler are removed. Historical
schema-4 artifact sidecars retain empty family-binding digest fields.
The same pass writes these generated profile modules:

- `src/dsctl/generated/version_profiles.py`
- `src/dsctl/generated/conformance_bundles.py`
- `src/dsctl/generated/task_definition_profiles.py`
- `src/dsctl/generated/workflow_profiles.py`
- `src/dsctl/generated/runtime_instance_profiles.py`

After freshness succeeds, the generated-package type gate reuses the complete
tree materialized by freshness under
`build/ds_contract/.freshness_cache/fresh/generated/`. It passes all three
generated namespaces to Mypy explicitly: `generated/versions`,
`generated/wire_programs`, and `generated/wire_runtime`. It does not run the
bundle generator a second time. The project-level generated-module override is
not a replacement for that dedicated check.

`tools/generate_ds_task_profiles.py` compiles the task facts and review ledger
into `src/dsctl/generated/task_profiles.py`; use `--check` to verify that output
without writing it. The canonical atomic runtime generator includes the same
artifact, and `tools/check_generated_freshness.py` verifies the entire namespace
plus the focused task compiler projection. Positive typed-authoring reviews
name their exact source-tree guard
and structured evidence paths. Those fields fail closed during compilation
and may be checked against locally available exact source roots; they are not
projected into the runtime task-profile module.

The version-profile compiler resolves reviewed semantic operations and stable
actions across the admitted exact versions. The
[current inventory](architecture.md#current-stable-surface) records its terminal
action coordinates and availability counts. These dimensions must not be
collapsed: terminal does not mean supported, supported does not mean
live-verified, and live evidence does not
automatically promote a profile. `3.4.1` remains stable/full; the other exact
profiles remain experimental.

Check that the materialized profile contains no pending coordinate with:

```bash
python tools/analyze_ds_support_coverage.py --require-complete
```

This check reads generated output. It is not a substitute for unified
regeneration or freshness, and a successful in-memory ledger compilation does
not prove that the tracked module is current.

Assess the two named conformance action closures with:

```bash
python tools/analyze_ds_conformance_bundles.py
```

The default stdout is deterministic machine JSON. The current static tracer
marks both `legacy_core/v1` and `full_core/v1` ready for every admitted exact
profile; its summary records the bundle/version coordinate counts.
This is `static-action-closure-only`: it sets `promotion_claimed`,
`support_level_changes`, and `tested_changes` to false and leaves facets,
scenarios, freshness, and live evidence unassessed. `catalog_digest` binds the
normalized resolved catalog, each `bundle_digest` binds its name and resolved
actions, and `assessment_digest` binds the complete static result.

The quality gate runs the quiet, fail-closed form:

```bash
python tools/analyze_ds_conformance_bundles.py \
  --check-generated \
  --require-assessed \
  --output build/ds_contract/conformance-assessment.json
```

`blocked` is a terminal assessment, not a gate failure. Missing coordinates,
nonterminal states, stale digests, generated drift, or inconsistent counts fail.
The release quality gate adds the independent live-evidence check:

```bash
python tools/check_conformance_bundle_evidence.py
```

Campaign `conformance-e8eacee57af9-r12-20260811` produced and tracked a
complete corpus for its then-current assessment. That corpus is artifact-bound
and does not attest the expanded manifests or newly ready full-core
coordinates. For a future build-once campaign, the development gate validates code before
building; the release gate rejects missing, stale or invalid receipts until the
canonical wheel completes its campaigns.

`tools/analyze_ds_stable_action_dependencies.py` joins stable commands to the
explicit `--baseline-version` wire baseline, defaulting to `3.4.1`.
Id-native and code-native adapter paths are followed statically.
Migrated deep modules use a narrower rule: the analyzer first proves that the
service reaches the expected `DefinitionReads` or `BoundDomain` seam, then
joins `ledger.operation_contracts` to the reviewed runtime binding for that
semantic operation. It does not attempt to infer generic domain internals from
AST shapes, and a missing or changed seam fails closed instead of allowing the
ledger to mask disconnected code. The resulting dependency report is the
input to `tools/analyze_ds_stable_action_version_matrix.py`, which projects
those exact source-operation closures across the 36-version corpus as
mechanical review candidates only.

Run the dependency analyzer as a fail-closed gate:

```bash
python tools/analyze_ds_stable_action_dependencies.py --baseline-version 3.4.2
```

Its report is complete only when every action in the command catalog resolves
through its expected local, diagnostic, legacy, `DefinitionReads`, `BoundDomain`,
or typed-wire seam.
Profile terminality cannot mask a disconnected implementation. An action absent
in the selected baseline requires an explicit reviewed absence decision and
still needs a proved command/service seam; a missing binding is not absence.

Then build the independent exact-source candidate matrix with:

```bash
python tools/analyze_ds_stable_action_version_matrix.py --baseline-version 3.4.2
```

The matrix reads the dependency report's exact baseline and rejects a conflicting
explicit selection. It reports whether those source dependencies can be found
in each exact upstream contract. Absent-baseline dependencies stay
non-projectable and require manual review; they cannot pass the mechanical
gate as empty wire closures. It is a mechanical review input, not
the product support catalog; only reviewed profile decisions may classify an
action as supported, upstream-limited, or upstream-absent.

Local upstream checkouts may live under `references/`, but that directory is an
ignored development workspace and not a generated artifact. See
[Codegen](codegen.md) for setup.

Stable reviewed documentation belongs under `docs/`:

- `docs/user/` for user-facing usage
- `docs/development/` for contributor workflows
- `docs/reference/` for stable contracts and domain references

Do not write generated reports directly into `docs/` unless they are promoted
to reviewed reference material.

## Runtime Boundary

The published wheel should contain only the `dsctl` runtime package and package
metadata. Development-only tools, tests, upstream references, local env files,
and caches must not be included in the wheel.

The source distribution may contain tests and tools for downstream auditing,
but must not contain the separately distributed `skills/` tree, local env
files, `references/`, build outputs, caches, or machine-local context files.
Package-content checks require the generated conformance module in wheels and
the catalog, compiler, and analyzer in source distributions.

Run the package content check after building distributions:

```bash
python tools/check_package_contents.py dist/*
```
