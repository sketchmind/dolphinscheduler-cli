# Contributing

Start with [README](README.md), [Architecture](docs/development/architecture.md)
and the [CLI overview](docs/reference/cli-overview.md). Before changing a command,
read its section and shared rules in the
[complete CLI contract](docs/reference/cli-contract.md). Before changing task
authoring or preservation, also read the relevant
[reviewed family boundary](docs/development/task-authoring-boundaries.md).

This project is a generated-first, REST-only CLI for Apache DolphinScheduler.
Contributions should preserve DolphinScheduler-native behavior while keeping
the public `dsctl` surface stable and understandable.

## Development Setup

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -c tools/lint-constraints.txt -e '.[dev]'
pre-commit install
```

Use Python 3.11 for the baseline development environment. Keep the supported
Python range in `pyproject.toml` and the CI matrix aligned when changing it.

Local development and CI use the Ruff version in `tools/lint-constraints.txt`.
Update it together with the Ruff revision in `.pre-commit-config.yaml`, and
review new lint rules and formatting changes before adopting a new version.

## Branching Model

`main` is the only long-lived development branch. Keep it green and releasable;
normal pull requests target `main` and must pass the required checks before
merge.

Create short-lived feature, fix, documentation, and `release-prep/<version>`
branches from an up-to-date `main`. Squash-merge those pull requests and delete
the source branch after merge. Release-preparation branches contain only the
version, changelog, final documentation, and small blocking fixes needed for a
single release.

Create `release/<major>.<minor>` only when an older release line needs ongoing
maintenance or a real stabilization window, and base it on the corresponding
release tag rather than the current `main`. Product fixes should normally land
on `main` first and then be backported with `git cherry-pick -x` through a
reviewed pull request. Do not use a maintenance branch as a second development
trunk or merge its version-specific state wholesale back into `main`.

Normal release tags point at the exact reviewed `main` commit. A maintained
older line may be tagged from its `release/*` branch. Attach the canonical
wheel and sdist to a draft GitHub Release, then use separately authorized manual
dispatches from protected `main` for TestPyPI and PyPI. Publish the draft
GitHub Release only after PyPI verification; publishing it does not trigger a
package-index upload.

## Documentation

Use the [documentation map](docs/README.md) to choose a guide by task.
`docs/user/` owns usage, `docs/reference/` owns public contracts, and
`docs/development/` owns architecture, source-review decisions and contributor
workflows. Start with the relevant guide rather than reading every design note.

Write shared documentation in English. Keep examples runnable from a clean
checkout with explicit placeholders and prerequisites. Update the existing
owner of a rule and link to it instead of copying the rule into stage reports.
Preserve exact-version exceptions and distinguish implemented behavior,
historical observations and open work.

A shared document should help someone use the CLI, change it correctly,
reproduce a check, or understand a durable design decision. Put setup and
usage in user guides, rules in their owning reference, and open work in the
roadmap. Keep a separate design note only when it explains rationale, exact
source differences or regression requirements that those guides do not cover.
Merge completed proposals into their owner; do not keep parallel current-state
descriptions or copies of campaign results across the README and guides.

Commit reusable guidance and governed, secret-free receipts. Keep local plans,
session transcripts, timings, candidate progress, raw logs and private fixture
material in ignored `build/` or `localdevdocs/`. The governed JSON files under
`docs/development/live-evidence/` are checker inputs, not disposable logs;
retain their original artifact identities. Most documentation is also shipped
in the sdist, so the same boundary applies to package contents.

For documentation-only changes, check spelling, relative links and changed
section anchors, then run applicable command-reference checks. If a change also
alters product behavior or tooling, apply the corresponding quality gates below.

## Quality Gates

Before opening a substantial change, run:

```bash
python tools/check_quality_gate.py --mode development
```

The development gate owns lint, formatting, types, architecture boundaries,
generated freshness, static conformance and all three offline test lanes.
Run focused tests while iterating; use `--portable` for a clean checkout without
prepared exact sources. Diagnostic skip options report partial results.
CI also checks command, process and authoring contracts with minimum and
known-current runtime dependency versions; do not assume the latest Pydantic
schema format. See [Dependency compatibility](docs/development/dependency-compatibility.md).

After the canonical wheel completes its required live campaigns, run
`python tools/check_quality_gate.py --mode release`. It includes every
development check and the three current-wheel receipt checks, and rejects skip
options or partial lanes. Passing development checks does not establish release
readiness or promote a DS profile.

For non-release packaging development, also run:

```bash
python -m build
python -m twine check dist/*
```

Do not use that combined build command for a release candidate after its wheel
has passed the installed-wheel live gate. Formal releases must follow the
wheel → evidence → sdist build-once sequence in the release checklist.

For destructive real-cluster coverage, export the live-test environment
variables and run:

```bash
python tools/check_quality_gate.py --mode development --include-live
```

## Development Rules

- Use REST only. Do not use Py4J, PyDolphinScheduler, or the Python gateway.
- Treat `references/` as an optional, ignored local workspace for upstream
  source checkouts. Any source mounted there is read-only from this project's
  perspective.
- Before changing DS-facing behavior, identify the relevant upstream
  controller, enum, DTO, request shape, or task-plugin model.
- Prefer improving the generator before adding handwritten DS-native shapes.
- Keep handwritten code above the generated layer focused on version
  adaptation, transport/runtime integration, CLI ergonomics, and output
  shaping.
- Keep generated imports inside `dsctl.upstream`.
- Do not let raw upstream `ApiResultError` leak when a service-level domain
  error is more actionable.
- Update tracked docs when stable architecture, behavior, or release process
  changes.

## Pull Requests

- Keep commits logically grouped.
- Update tests for behavior changes.
- Update docs for command, output, error, compatibility, or release changes.
- Prefer cluster-backed smoke coverage when a change touches real
  DolphinScheduler compatibility behavior.

## Compatibility Notes

The runtime registry selects exact generated profiles for all 37 releases from
`1.3.9` through `3.4.3`. DolphinScheduler `3.4.1` is the offline default and
retains the historical `full` / `tested=true` release policy. Artifact-bound
verification coverage is reported separately. Read
[support policy and verification](docs/user/version-compatibility.md#support-policy-and-verification)
before changing a profile's coverage or promotion state.
