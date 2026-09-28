# Release Process

This checklist keeps GitHub and PyPI publication explicit and reproducible.

## Branch Flow

`main` is the only long-lived development branch. Keep it green and releasable.
Feature, fix, documentation, and release-preparation work is done on short-lived
branches and reviewed pull requests targeting `main`.

Prepare a normal release with this flow:

1. Create `release-prep/<version>` from an up-to-date `main`.
2. Update the version, changelog, release notes, and final documentation.
3. Build the wheel once in a clean source tree, preflight its exact payload,
   and run the local and installed-wheel live gates.
4. Commit the required secret-free receipt corpora; do not rebuild the
   validated wheel. Rerun the default evidence gates without allowances.
5. Build the sdist once, then validate the canonical wheel/sdist pair.
6. Merge the reviewed release-preparation pull request into `main`.
7. Wait for the resulting `main` commit to pass CI and record its full SHA.
8. Tag that SHA and attach the canonical pair to a draft GitHub Release.
9. From protected `main`, explicitly dispatch `target=testpypi` to publish and
   verify those bytes on TestPyPI.
10. After a separate explicit authorization and environment approval, dispatch
    `target=pypi` from protected `main` to promote and verify the same bytes.
11. Publish the draft GitHub Release only after the PyPI workflow succeeds.

The version section in `CHANGELOG.md` is the source for the GitHub Release
body. Keep an empty `Unreleased` section above it so post-release work has an
explicit landing place.

### Maintenance Releases

Do not create a persistent release branch for every normal release. Create
`release/<major>.<minor>` only when an older line needs continued patches or a
stabilization window must remain isolated while `main` advances.

Create the maintenance branch from the published tag that starts the supported
line, not from the current `main`. For example, to maintain the `0.3` line from
its first release:

```bash
minor=0.3
base_tag=v0.3.0
git switch --create "release/$minor" "$base_tag"
git push --set-upstream origin "release/$minor"
```

Fixes should normally land on `main` first, then be backported with
`git cherry-pick -x` through a reviewed pull request targeting the maintenance
branch. For an urgent production-first fix, forward-port the same change to
`main` immediately so the next release cannot regress. Keep release-specific
version changes on the maintenance branch instead of merging the branch
wholesale into `main`. Prepare the patch version and changelog on a short-lived
branch targeting `release/<major>.<minor>`, then run the same TestPyPI and tag
checks against that maintenance branch.

### Historical Note: `0.3.0`

The [`v0.3.0` release pull request](https://github.com/sketchmind/dolphinscheduler-cli/pull/7)
reconciled parallel `main` and `dev` histories before the project moved to a
single `main` trunk. Its replay and `ours` merge were a verified one-time
repair, not a reusable release procedure.

## Pre-Release Decisions

- Confirm the public package name.
- Confirm the CLI command name remains `dsctl`.
- Confirm `project.urls` in `pyproject.toml` points at the public GitHub
  repository.
- Confirm the Python support range matches CI.
- Confirm the DolphinScheduler support matrix in
  [Version Compatibility](../user/version-compatibility.md).
- Confirm the README does not imply official Apache project status.

## Local Gate

### Closing development acceptance

Close the development campaign before freezing the release candidate. Reconcile
completed scenario receipts into one coverage inventory, retaining each exact
version, wheel, test revision and cleanup proof. Keep local campaign inventories
and progress reports in ignored build output; commit only the governed,
secret-free receipt corpora and reusable guidance.

Use these separate completion criteria:

| Claim | Required evidence | What does not establish it |
| --- | --- | --- |
| A development scenario is verified | Its applicable version's completed assertions and independently checked cleanup, bound to its original wheel | A command observed during setup, a passing neighbouring version, or a static capability |
| A package is eligible for publication | Resolved release-blocking defects, reviewed changed journeys, complete development/release checks, and the final artifact check below | Combining passes from different candidate wheels |
| Profiles have the same whole-profile verification level | The same applicable scenario and facet obligations on each exact profile, followed by an evidence-backed support-policy change | The core/read bundles, the stable label, or a successful package release alone |
| An action is `live_full` | Its positive, negative/precondition, round-trip and cleanup obligations; runtime actions also need state-transition evidence | A `live_smoke` claim or incidental execution in another scenario |

Classify remaining entries as an unverified supported scenario, an unmet
environment/cleanup prerequisite, an exact upstream absence, or a product
defect. A composite test's unsupported step does not make all of its actions
absent: recovery/rerun, for example, have wider availability than execute-task.
Split the supported portion before declaring its coverage complete. Likewise,
a missing-Kubernetes error probe is not namespace CRUD acceptance, and alert
configuration is not test-send or receiver-side delivery evidence.

An environment limitation may remain explicitly unverified only when the release
does not promise that verified scope. It still blocks the corresponding
whole-profile parity or `live_full` claim. This does not waive the mandatory
current-wheel receipt corpora or final artifact check below. Do not silently
drop an existing
obligation or treat a skipped test as a pass. Repair a scenario's native-state
assertions and cleanup before dispatch; configuration changes or new backends
are not justified merely to increase a pass count.

Finalize the supported scope, verification metadata, documentation and required
test adaptations before building the canonical wheel. During gap filling, reuse
accepted evidence with its original binding. After the final freeze, collect
the required current-wheel evidence once; do not rebuild between campaigns.

### Product review before the candidate build

Review the changed user journeys before freezing a wheel. Help, capabilities,
schemas, templates, returned commands and the agent skill must agree with actual
execution under the same selected target. Check exact-version boundaries,
compositions of different task families, deferred remote references, failure
recovery and explicit output projections. A passing test suite is evidence for
those behaviors, not a substitute for reviewing their usefulness and clarity.

Resolve actionable correctness and discovery defects before entering the live
campaign. Record intentional scope limits in the public contract or roadmap.
Keep reusable architecture, contributor instructions and secret-free release
receipts in the repository; keep machine-specific paths, session narratives,
temporary experiments and candidate progress reports in ignored build output.
Validate documentation links against files shipped in a clean checkout or
immutable upstream references.

### Candidate validation

Before freezing the final release wheel, inspect the installed verification
levels for `project.list`, `project.get`, `workflow.list`, and `workflow.get` on
every admitted exact profile. The generic read gate and its independent corpus
checker require all four to be `live_smoke`; a newly admitted `contract_tested`
profile cannot enter that promotion gate yet.

For new profiles, first use a preflighted preliminary wheel to collect actual
read behavior against the exact source-bound cluster and controlled fixture.
Retain the commands, results, target identity, wheel identity and fixture
readback as preliminary evidence. This is not a promotion-grade read receipt.
The existing named conformance runner with `--bundle full_core/v1` can supply
that evidence: it includes all four project/workflow reads and accepts their
installed `contract_tested` level. It also performs mutations, so use its
dedicated fixtures and cleanup protocol. Its receipt makes no promotion claim;
the generic read runner cannot serve this preliminary role unchanged.
Review that evidence before changing the exact per-action verification ledger;
do not infer live success from contract tests, set verification levels in advance,
or bypass the gate's installed-profile checks. A read-action promotion leaves
the whole profile's support level and `tested` status unchanged.

After an evidence-backed ledger change, atomically regenerate, run the applicable
development checks, and build a new final wheel from a clean source snapshot.
Preflight and locally install that wheel before collecting the complete final
same-wheel read, conformance and `external-shell/v1` receipts on the reviewed
exact `3.4.2` target. Never relabel preliminary
results with the new wheel hash or rebuild the final wheel during that campaign.

Build from a dedicated clean checkout or isolated source snapshot. Before the
build, that tree must not contain an earlier `build/`, `dist/`,
`src/*.egg-info/`, ignored package directories, or empty namespace-package
directories under `src/`. Start with an empty artifact directory and build the
wheel only:

```bash
python tools/check_quality_gate.py --mode development
python tools/check_release_version.py --tag vX.Y.Z
python -m build --wheel
python tools/check_release_artifacts.py --wheel-only \
  dist/dolphinscheduler_cli-X.Y.Z-py3-none-any.whl
```

The development gate checks code, generated freshness and types, static source
closure, and all three test lanes. It needs no current-wheel live receipts and
makes no release or promotion claim. After the canonical wheel passes its live
campaigns and the receipts are tracked, run:

```bash
python tools/check_quality_gate.py --mode release
```

The release gate runs the complete development gate plus conformance-bundle,
exact-profile read, and `external-shell/v1` evidence checks on exact `3.4.2`.
It rejects portable lanes and every skip option. The old evidence skip/allowance options have been
removed from the quality gate; pre-campaign work uses development mode.
Individual evidence checkers remain available for campaign diagnosis, including
`check_exact_profile_promotion_evidence.py --version 3.4.2 --allow-missing-current-receipt`, whose
narrow missing-receipt semantics do not establish release readiness.

`--mode release` is necessary but is not the final artifact-to-evidence check.
Its independent corpus checkers validate their own contracts and wheel
consistency without receiving the canonical wheel file. The final
`check_release_artifacts.py` invocation with the wheel/sdist pair binds all
three evidence families to that wheel's filename, full SHA-256 and source
snapshot. It also requires an actual current `external-shell/v1` receipt on
exact `3.4.2`; the standalone promotion checker can return `unclaimed` when the
expanded mutation claim is absent. Publication requires both checks.

The `external-shell/v1` gate is a reviewed scenario obligation, separate from
[profile support policy](../user/version-compatibility.md#support-policy-and-verification).
It verifies preservation and restoration of a pre-existing SHELL task,
complementing the core lifecycle scenario. A replacement gate must cover the
same restoration obligations and pass independent review.

If profile fingerprints invalidate existing receipts, retain them as historical
evidence and rerun all required exact coordinates with the same canonical
wheel.

The wheel-only preflight is required before any live gate. It compares the
complete packaged runtime with `src/dsctl` byte for byte and validates the
wheel member set, metadata, entry points, exact manifest, and `RECORD`. This
catches a stale `build/lib` payload or README metadata drift before expensive
cluster testing. `check_package_contents.py` remains a complementary structural
check; it is not a substitute for this exact source comparison.

The development gate also regenerates and checks the named conformance
assessment, requiring both named bundles for every admitted exact profile to
terminate as `ready` or `blocked`. Its quiet report is written under
`build/ds_contract/conformance-assessment.json`. Confirm that the generated
artifact's canonical `catalog_digest`, per-bundle digests, and full
`assessment_digest` are current. This gate proves only static action closure:
it does not assess support-tier scenarios, facets, evidence freshness, or live
execution, and it never changes `support_level` or `tested`.

The release gate includes the separate live-corpus checker:

```bash
python tools/check_conformance_bundle_evidence.py
```

That checker fails while the current corpus is absent. Build and preflight the
canonical wheel, run every required exact-version gate with those same bytes,
track the receipts, and then run the release gate. Do not bump the version or
rebuild the wheel between those stages.

The canonical build-once order is:

1. finalize runtime code, package metadata, README content, conformance
   contracts, and these runbook instructions in a clean source snapshot;
2. pass the development gate, build the wheel
   once, pass `check_release_artifacts.py --wheel-only`, and freeze its basename
   and full SHA-256;
3. prepare and project each version's fresh matrix fixture and image proof,
   then run all admitted highest-ready coordinates with those identical wheel bytes;
4. track the complete receipt corpus and pass the conformance checker and
   release gate; and
5. build only the sdist and run the full two-artifact release check shown
   below. Never use `python -m build` without `--sdist` at this stage because it
   would replace the attested wheel.

If a campaign exposes a runtime or cleanup correctness defect, preserve its
failed attempt and original receipts, complete owned cleanup, and fix the source
before freezing a new wheel. Repeat the complete same-wheel acceptance corpus
for that new candidate; neither earlier passing rows nor later residue cleanup
can replace its receipts. On `2.0.0` and `2.0.1`, cleanup must prove the complete
native task inventory empty before deleting its owned project, as detailed in
[Live Testing](live-testing.md#named-conformance-bundle-installed-wheel-gate).

Inspect the wheel and install it in a clean virtual environment:

```bash
python -m zipfile --list dist/*.whl
python3 -m venv /tmp/dsctl-release-check
/tmp/dsctl-release-check/bin/python -m pip install dist/dolphinscheduler_cli-*.whl
/tmp/dsctl-release-check/bin/dsctl version
/tmp/dsctl-release-check/bin/dsctl schema
/tmp/dsctl-release-check/bin/dsctl capabilities
DS_VERSION=3.4.3 /tmp/dsctl-release-check/bin/dsctl version
DS_VERSION=3.4.3 /tmp/dsctl-release-check/bin/dsctl capabilities --action project.create
```

The exact-profile commands force the clean installation to import the `3.4.3`
generated client, adapter, and registry path rather than proving only the
default `3.4.1` package.

Run the highest ready named conformance bundle for each exact version with the
same canonical wheel. The generated assessment is authoritative: all admitted exact
versions currently require `full_core/v1`. The historical r12 campaign used
`legacy_core/v1` for `1.3.9` and `2.0.0`; those receipts do not cover their
current highest-ready bundles.

For each version, the external matrix orchestrator first prepares the unchanged
generic exact-read schema-1 manifests, private ready state, and a fresh schema-2
image inspection. Project them into the conformance-specific schema-2 cluster
manifest and schema-1 fixture manifest before invoking the wheel:

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

The schema-2 inspection is the strict `registry-digest/v1` or
`managed-image-lock/v1` union documented in
[Live Testing](live-testing.md#named-conformance-bundle-installed-wheel-gate).
Registry proof requires a selected API `RepoDigest` that occurs exactly once.
A tag-only image reference requires one matching repository digest; an explicit
published-digest pin may disambiguate a larger unique inventory. Managed proof
is accepted only for `1.3.9`, `2.0.0`, `2.0.4`, `2.0.7`, `2.0.8`, `2.0.9`,
`3.0.2`, and `3.1.2`, including
the clean management revision, exact lock row and field-presence policy,
source commit bound to the exact profile, matching actual archive/digest
observations, and exact labels. Other versions fail closed without registry
proof. Keep all inputs owner-private. Projection publishes a
new 0600 output pair and refuses either existing destination; it does not run
the candidate wheel or create wire evidence. The matrix orchestrator may use a
temporary collector to assemble inspection facts, but this repository does not
define that collector as a tracked long-term command. Before dispatch, verify
the exact API and DAO Maven versions in the running container against its
observed image ID, as described in Live Testing. Published tags can contain
the wrong runtime version; the observed `2.0.4` public image contains `2.0.5`
artifacts and is excluded from the exact `2.0.4` campaign.

```bash
python tools/run_exact_conformance_bundle_gate.py \
  --ds-version VERSION \
  --bundle BUNDLE \
  --wheel dist/dolphinscheduler_cli-X.Y.Z-py3-none-any.whl \
  --env-file /secure/ds-VERSION-etl.env \
  --attestation-key-file /secure/campaign/attestation.key \
  --cluster-manifest /secure/campaign/VERSION-cluster-schema2.json \
  --fixture-manifest /secure/campaign/VERSION-fixture-schema1.json \
  --run-id-output-file /secure/campaign/run-ids/VERSION-ATTEMPT.json \
  --evidence /secure/campaign/receipts/VERSION.json
```

For `full_core/v1`, the optional `--run-id-output-file` shown above requires a parent
directory owned by the current user with mode `0700` and creates a new `0600`
identity file before live execution. The parent directory must already exist.
Choose a unique path for each attempt, including concurrent runs; existing
destinations are never overwritten. Retain the file after failure or
interruption for explicit `--recovery-run-id-file` use on a generated recovery
coordinate, with the original candidate and fresh target/fixture proofs.
Recovery never produces a passing receipt. See
[Live Testing](live-testing.md#named-conformance-bundle-installed-wheel-gate)
for the exact versions and ownership checks.

Track exactly one accepted receipt per version under
`docs/development/live-evidence/conformance-bundles/<version>/`, then run
`python tools/check_conformance_bundle_evidence.py`. The corpus must contain
all admitted highest-ready coordinates and bind one wheel basename and full SHA-256.
Missing, legacy-only, stale, mixed-wheel, or wrong-wheel evidence fails closed.
This named-bundle evidence remains separate from support level, `tested`, and
profile promotion.

For each exact profile whose compatibility claim is being promoted, run the
generic installed-wheel read gate with the same canonical wheel bytes:

```bash
python tools/run_exact_profile_read_gate.py \
  --ds-version VERSION \
  --wheel dist/dolphinscheduler_cli-X.Y.Z-py3-none-any.whl \
  --env-file /secure/ds-VERSION-etl.env \
  --attestation-key-file /secure/campaign/attestation.key \
  --cluster-manifest /secure/campaign/VERSION-cluster.json \
  --fixture-manifest /secure/campaign/VERSION-fixture.json \
  --evidence /secure/campaign/receipts/VERSION.json
```

Matrix activation and external fixture lifecycle are separate from this
read-only command. Do not rebuild the wheel between profile runs. A generic
read receipt proves only its fixed project/workflow read scenario and does not
replace a support-tier core scenario or a mutation/facet-specific gate.

Per-action promotion requires a schema-2 receipt from the installed wheel that
uses the semantic profile shape and includes the four
`read_bundle.action_verifications` values in its canonical digest. Schema-1
generic receipts remain auditable only as historical evidence.
After the same-wheel sweep passes every exact version, store exactly one
secret-free receipt per version at
`docs/development/live-evidence/exact-read/<version>/DATE-WHEEL_SHA.json` and
run `python tools/check_exact_profile_read_evidence.py`. The checker rejects a
schema-1 historical-only receipt, an incomplete or mixed-wheel corpus, stale
generated contracts/profiles/recipes, or any verification value that is not the
current installed `live_smoke` claim. This checker is also part of the release
gate; do not commit promoted metadata without the complete governed
corpus.

A current `external-shell/v1` claim on exact `3.4.2` requires its schema-7
receipt before the release gate can pass. If a live DolphinScheduler cluster is available, the development
gate can append the destructive source-tree suite:

```bash
export DSCTL_RUN_LIVE_TESTS=1
export DSCTL_RUN_LIVE_ADMIN_TESTS=1
export DS_LIVE_ADMIN_ENV_FILE=$PWD/.env
export DS_LIVE_QUEUE=default
python tools/check_quality_gate.py --mode development --include-live
```

Use `.env.example` as the local profile template. The real `.env` file is
ignored by git and must not be committed. This source-tree suite is a campaign
check; it does not replace installed-wheel evidence or the release gate.

Every release must run the installed-wheel `external-shell/v1` scenario on its
reviewed exact `3.4.2` target against the canonical wheel. The source-tree live
suite cannot replace this gate. It binds live evidence to the exact wheel
SHA-256, so it is required even when the release does not change that profile:

When an active resident-matrix warm lease already owns a REST-verified `3.4.2`
fixture, project its schema-1 manifests and private ready state into the strict
schema-2 gate inputs without changing the matrix repository:

```bash
python tools/project_exact_profile_matrix_fixture.py \
  --version 3.4.2 \
  --cluster-manifest /secure/matrix/cluster-schema1.json \
  --fixture-manifest /secure/matrix/fixture-schema1.json \
  --state-file /secure/matrix/state.json \
  --image-inspection /secure/matrix/image-inspection.json \
  --cluster-output /secure/campaign/ds-3.4.2-cluster.json \
  --fixture-output /secure/campaign/ds-3.4.2-fixture.json
```

The helper validates and projects only state that the matrix has already read
back through REST. The private image-inspection input must use the exact schema
documented in `live-testing.md`; the selected RepoDigest must occur exactly
once in the inspected `RepoDigests`, and its tag, image ID, and repository must
match the schema-1 cluster manifest. Multiple digests for that repository
require an explicit image-reference pin equal to the selection. A bare
operator-supplied digest is not an accepted input. The helper does not run the
candidate wheel, inspect candidate
HTTP traffic, or create wire evidence; the exact installed-wheel gate below
remains the evidence producer. Keep the ready-state, image-inspection, and
generated-manifest files owner-private and untracked. This projection path is
for reusing the active warm lease, not for mutating matrix topology or its
repository.

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

The requirements and manifest schemas are documented in
[Live Testing](live-testing.md#exact-342-installed-wheel-gate). Commit the
secret-free schema-7 receipt, then rerun
`python tools/check_exact_profile_promotion_evidence.py --version 3.4.2` and
`python tools/check_quality_gate.py --mode release`. The checker binds all 15
`live_smoke` claims to the receipt's profile fingerprints, generated decisions,
manifest, package version, and exact wheel bytes. Never commit the profile,
attestation key, cluster manifest, or fixture manifest.

Historical schema-3/4/5/6 receipts remain auditable for their recorded artifacts;
they cannot satisfy the current schema-7 release gate. A passing gate supplies
only its named per-action evidence, not whole-profile promotion.
Image provenance must satisfy the current projection contract: a bare
RepoDigest without its node-local image-ID binding is insufficient.

The expanded task-definition gate is mutating. It must own a dedicated
`OFFLINE` workflow with one editable `SHELL` task exclusively for the complete
run. Exclusive ownership protects the interval between the separate dry-run
and apply invocations; within apply, the CLI fresh-reads and guards its own
prepared state before PUT. The gate independently proves that dry-run made no
change, performs a real task update, verifies the requested
fields/dependencies and task-version advance against a coherent
detail/DAG/relation readback, and restores command, non-owned task fields,
relations/topology, task/DAG version consistency, and workflow release state.
Historical schema-v3/v4/v5/v6 receipts remain auditable for their old action,
adapter, semantic-profile, and bundle contracts but cannot satisfy the current
expanded release gate. Schema 6 was the first to carry the digest-bound
15-action verification and recipe bundle; only semantic schema 7 satisfies the
current promotion contract.

This `live_smoke` receipt does not claim a controlled prepare/interfere/apply
negative. The stale-plan guard is unit/contract-tested; deterministic
installed-wheel conflict evidence is required only before promoting the action
to `live_full`. Cleanup refusal after an observed fixture change is separate
evidence and must not be described as guard coverage.

If this gate fails at or after the mutation boundary, inspect task detail, the
workflow DAG, and release state before retrying. A response-decode or readback
failure can occur after DolphinScheduler accepted the write, so blindly
rerunning the non-idempotent mutation can overwrite the restoration or a
concurrent change. Reconcile the exclusive fixture to its recorded baseline,
then begin a fresh gate run. The mutation wire never automatically retries the
PUT, and its readback currently has no eventual-consistency retry loop. The
fresh-read conflict guard also cannot eliminate the server PUT's final TOCTOU
window, so exclusive fixture ownership is still required.

After committing every required receipt corpus and passing the default quality
gate without allowances, build only the sdist. Do not invoke a command that
rebuilds or replaces the wheel:

```bash
python -m build --sdist
python tools/check_release_artifacts.py --tag vX.Y.Z dist/*
python -m twine check dist/*
python tools/check_package_contents.py dist/*
tar -tf dist/*.tar.gz | sort
```

The artifact checker requires exactly one canonical wheel and one sdist. It
first compares the wheel's runtime payload byte-for-byte with `src/dsctl` and
validates the sdist. It then validates the complete conformance corpus,
the complete exact-read corpus and the separate `external-shell/v1` receipt on
exact `3.4.2` against the candidate wheel
basename, full SHA-256, and candidate source root. An old, missing, mixed, or
wrong-wheel corpus cannot satisfy the full release gate. `--wheel-only` remains
the pre-campaign payload check and deliberately requires no live evidence. The
publish workflow never builds distributions; it only validates and promotes
these Release assets. If packaged runtime code or metadata changes after the
live gate, discard the pair, build a new wheel, and rerun the gates.

## Repository and Environment Protection

Configure the release controls before the first publication:

- Protect `main` with a ruleset that requires reviewed pull requests and the
  release quality checks, blocks force-pushes and deletion, and tightly limits
  bypass access.
- Protect `v*` tags with a tag ruleset that restricts creation and blocks tag
  update or deletion. A release tag identifies immutable source; moving it must
  not be a recovery mechanism.
- Configure both the `testpypi` and `pypi` GitHub environments with required
  reviewers, enable prevent self-review, and limit deployments to protected
  `main`. The person who requests a publication cannot approve their own
  environment deployment.

Both package-index publications are manual workflow dispatches from protected
`main`. The `release_tag` input is data that identifies the source and draft
Release assets; it does not select the workflow definition. For a maintenance
release, the workflow still runs from `main` and validates that the supplied tag
belongs to an allowed `release/<major>.<minor>` ancestry. Environment approval
is an independent control in addition to the explicit authorization required
before each dispatch.

## TestPyPI

Publish to TestPyPI first, then install from TestPyPI in a clean environment.
Do not promote to PyPI until the installed command works outside the source
checkout.

This repository publishes through GitHub Actions Trusted Publishing. Configure
a pending publisher in TestPyPI with these values:

- PyPI project name: `dolphinscheduler-cli`
- Owner: `sketchmind`
- Repository name: `dolphinscheduler-cli`
- Workflow name: `publish.yml`
- Environment name: `testpypi`

Set the candidate version and release ref explicitly. Use `main` for a normal
release or the maintained `release/<major>.<minor>` branch for a patch on an
older line. Fetch that ref, record its exact remote commit, and create the tag:

```bash
version="X.Y.Z"  # replace with the candidate version
release_ref=main  # or release/X.Y for a maintained line
git fetch origin "$release_ref"
release_sha=$(git rev-parse "origin/$release_ref^{commit}")
tag="v$version"
git tag -a "$tag" "$release_sha" -m "Release $version"
test "$(git rev-parse "${tag}^{}")" = "$release_sha"
git push origin "$tag"
```

Create a draft GitHub Release for that existing tag and upload the canonical
pair. `RELEASE_NOTES.md` should contain the matching changelog section. Never
use `--clobber`; changing either asset requires a new wheel, live receipt, and
release candidate:

```bash
gh release create "$tag" \
  --verify-tag \
  --draft \
  --title "$tag" \
  --notes-file RELEASE_NOTES.md \
  dist/dolphinscheduler_cli-"$version"-py3-none-any.whl \
  dist/dolphinscheduler_cli-"$version".tar.gz
```

These tag, push, Release, and upload operations require explicit release
authorization. Once the draft and its two assets have been reviewed, obtain
explicit authorization for the TestPyPI publication and dispatch the workflow
from protected `main`. The environment's required reviewer must then approve
that deployment:

```bash
gh workflow run publish.yml --ref main -f release_tag="$tag" -f target=testpypi
```

Find and watch the dispatched run. Its workflow ref is `main`; the release tag
is validated as data inside the workflow, so the run's `headSha` is not expected
to equal the tagged release commit:

```bash
gh run list --workflow publish.yml --branch main \
  --event workflow_dispatch --limit 5
run_id="RUN_ID"  # copy the matching run ID from the list
gh run watch "$run_id" --exit-status
test "$(gh run view "$run_id" --json event --jq .event)" = workflow_dispatch
test "$(gh run view "$run_id" --json headBranch --jq .headBranch)" = main
```

The workflow requires a draft Release on the supplied tag, verifies the wheel
against its committed live receipt and tagged `src/dsctl`, then uploads the two
Release assets without rebuilding them. It polls TestPyPI metadata, compares
both SHA-256 digests with the local assets, downloads the wheel cleanly, and
runs smoke commands. TestPyPI authorization covers only this TestPyPI dispatch;
it does not authorize PyPI or publication of the draft GitHub Release.
TestPyPI and PyPI are separate indexes, but neither permits replacing an
already uploaded distribution filename; never overwrite or silently reuse a
candidate version.

Install from TestPyPI in a clean environment:

```bash
check_dir="/tmp/dsctl-testpypi-$version"
python3 -m venv "$check_dir"
"$check_dir/bin/python" -m pip install --upgrade pip
"$check_dir/bin/python" -m pip download --no-cache-dir --no-deps \
  --only-binary=:all: --index-url https://test.pypi.org/simple/ \
  --dest "$check_dir" "dolphinscheduler-cli==$version"
"$check_dir/bin/python" -m pip install \
  "$check_dir"/dolphinscheduler_cli-"$version"-*.whl
"$check_dir/bin/dsctl" version
"$check_dir/bin/dsctl" schema --list-groups
"$check_dir/bin/dsctl" capabilities
```

## PyPI

Use PyPI Trusted Publishing from GitHub Actions instead of storing a long-lived
PyPI API token in repository secrets.

Configure a pending publisher in PyPI with these values:

- PyPI project name: `dolphinscheduler-cli`
- Owner: `sketchmind`
- Repository name: `dolphinscheduler-cli`
- Workflow name: `publish.yml`
- Environment name: `pypi`

Promote the validated candidate with a second, separately authorized dispatch:

1. Confirm the recorded release commit passed the applicable branch CI and the
   TestPyPI workflow, including its clean install and exact-byte checks.
2. Verify that the GitHub Release is still a draft on the recorded tag and has
   exactly the canonical wheel and sdist assets.
3. Obtain explicit authorization for PyPI, then dispatch from protected `main`
   with the same tag. The `pypi` environment's required reviewer must approve
   this deployment independently:

   ```bash
   gh workflow run publish.yml --ref main -f release_tag="$tag" -f target=pypi
   ```

4. Find and watch the matching `workflow_dispatch` run. Before PyPI upload, it
   downloads the draft Release assets again, verifies the live-tested wheel,
   and requires both assets to match TestPyPI SHA-256 metadata exactly. The run
   then verifies the PyPI metadata after upload.
5. Only after that PyPI run succeeds, obtain explicit authorization to expose
   the GitHub Release. Immediately before publishing the draft, require its
   asset list and bytes to still equal the local canonical pair:

   ```bash
   expected_assets=$(printf '%s\n' \
     "dolphinscheduler_cli-$version-py3-none-any.whl" \
     "dolphinscheduler_cli-$version.tar.gz" | sort)
   actual_assets=$(gh release view "$tag" --json assets \
     --jq '.assets[].name' | sort)
   test "$actual_assets" = "$expected_assets"
   release_verify_dir=$(mktemp -d)
   gh release download "$tag" --dir "$release_verify_dir" \
     --pattern "dolphinscheduler_cli-$version-py3-none-any.whl" \
     --pattern "dolphinscheduler_cli-$version.tar.gz"
   cmp dist/dolphinscheduler_cli-"$version"-py3-none-any.whl \
     "$release_verify_dir"/dolphinscheduler_cli-"$version"-py3-none-any.whl
   cmp dist/dolphinscheduler_cli-"$version".tar.gz \
     "$release_verify_dir"/dolphinscheduler_cli-"$version".tar.gz
   gh release edit "$tag" --draft=false
   ```

6. Verify an exact-version clean install from PyPI and confirm `dsctl version`,
   `dsctl schema --list-groups`, and `dsctl capabilities`.

Sign the tag when a verified signing key is configured; do not claim an
unsigned tag is signed. Enable immutable GitHub Releases when the repository
setting is available. TestPyPI and PyPI publishing are separate manual
dispatches from protected `main`; publishing the draft GitHub Release does not
trigger a package upload. The tag input selects only the already reviewed source
and canonical assets, so an unprotected branch head cannot replace the trusted
workflow definition or bypass the release chain.
