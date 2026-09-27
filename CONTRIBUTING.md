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

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pre-commit install
```

Use Python 3.11 unless the CI matrix and `pyproject.toml` are widened together.

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

## Documentation Map

- User docs live under `docs/user/`.
- Developer docs live under `docs/development/`.
- Stable contract and reference docs live under `docs/reference/`.
- The code generation workflow is documented in `docs/development/codegen.md`.
- Tool naming and generated artifact paths are documented in
  `docs/development/tooling.md`.
- The release checklist is documented in `docs/development/release.md`.

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

The default and only stable runtime target is DolphinScheduler `3.4.1`. The
runtime registry selects exact generated profiles for all 37 releases from
`1.3.9` through `3.4.3`; the other 36 profiles retain their recorded experimental
support level until an evidence-backed release-policy decision promotes them.
See `docs/user/version-compatibility.md` before changing a profile's coverage or
promotion state.
