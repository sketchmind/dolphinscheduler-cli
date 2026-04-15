# Release Process

This checklist keeps GitHub and PyPI publication explicit and reproducible.

## Pre-Release Decisions

- Confirm the public package name.
- Confirm the CLI command name remains `dsctl`.
- Confirm `project.urls` in `pyproject.toml` points at the public GitHub
  repository.
- Confirm the Python support range matches CI.
- Confirm the DolphinScheduler support matrix in
  [Version Compatibility](../user/version-compatibility.md).
- Confirm the README does not imply official Apache project status.
- Decide whether the public GitHub repository preserves full history or starts
  from a clean initial commit.

## Local Gate

```bash
python tools/check_quality_gate.py
python -m build
python -m twine check dist/*
python tools/check_package_contents.py dist/*
```

Inspect the distributions:

```bash
tar -tf dist/*.tar.gz | sort
python -m zipfile --list dist/*.whl
```

Install the wheel in a clean virtual environment and run smoke commands:

```bash
python3 -m venv /tmp/dsctl-release-check
/tmp/dsctl-release-check/bin/python -m pip install dist/dolphinscheduler_cli-*.whl
/tmp/dsctl-release-check/bin/dsctl version
/tmp/dsctl-release-check/bin/dsctl schema
/tmp/dsctl-release-check/bin/dsctl capabilities
```

## TestPyPI

Publish to TestPyPI first, then install from TestPyPI in a clean environment.
Do not promote to PyPI until the installed command works outside the source
checkout.

## PyPI

Use PyPI Trusted Publishing from GitHub Actions instead of storing a long-lived
PyPI API token in repository secrets.

Recommended release trigger:

1. Update `CHANGELOG.md`.
2. Update `pyproject.toml` and `src/dsctl/__init__.py` to the release version.
3. Push a signed tag such as `v0.1.0`.
4. Let the publish workflow build and upload the distributions.
5. Verify `pipx install dolphinscheduler-cli` exposes `dsctl`.
