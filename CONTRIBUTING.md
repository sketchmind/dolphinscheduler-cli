# Contributing

This project is a generated-first, REST-only CLI for Apache DolphinScheduler.
Contributions should preserve DolphinScheduler-native behavior while keeping
the public `dsctl` surface stable and understandable.

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .[dev]
pre-commit install
```

Use Python 3.11 unless the CI matrix and `pyproject.toml` are widened together.

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
python tools/check_quality_gate.py
```

For targeted work, use the same underlying checks as CI:

```bash
python -m ruff check src tests tools
python -m ruff format --check src tests tools
python tools/check_project_layout.py
python tools/check_explicit_object.py
lint-imports
python tools/check_generated_freshness.py
python tools/check_error_translation_governance.py
python -m mypy src tests tools
codespell --toml pyproject.toml
python -m pytest -q
```

For packaging changes, also run:

```bash
python -m build
python -m twine check dist/*
```

For destructive real-cluster coverage, export the live-test environment
variables and run:

```bash
python tools/check_quality_gate.py --include-live
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

The default target is DolphinScheduler `3.4.1`. The runtime registry also
selects `3.4.0` and `3.3.2` through the current compatibility family. See
`docs/user/version-compatibility.md` before extending support to another
DolphinScheduler version.
