# Tooling

Repository tools live under `tools/`. They are development and release helpers,
not runtime modules shipped as part of the `dsctl` import package.

## Script Naming

Use these prefixes consistently:

- `check_*.py`: quality gates that exit non-zero on failure
- `generate_*.py`: generators that write code or generated artifacts
- `analyze_*.py`: read-only analysis that writes reports or stdout
- `extract_*.py`: inventory or matrix extraction from upstream or local source
- `audit_*.py`: review reports for governance and policy checks

Keep reusable implementation code inside a tool package such as
`tools/ds_codegen/`; keep thin command-line wrappers at `tools/*.py`.

## Generated Artifacts

Generated reports, snapshots, package samples, and temporary upstream worktrees
belong under `build/`.

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
but must not contain local env files, `references/`, build outputs, caches, or
machine-local context files.

Run the package content check after building distributions:

```bash
python tools/check_package_contents.py dist/*
```
