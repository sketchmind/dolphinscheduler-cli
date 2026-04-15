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

If a live DolphinScheduler cluster is available, run the destructive live gate
before publishing:

```bash
export DSCTL_RUN_LIVE_TESTS=1
export DSCTL_RUN_LIVE_ADMIN_TESTS=1
export DS_LIVE_ADMIN_ENV_FILE=$PWD/.env
export DS_LIVE_QUEUE=default
python tools/check_quality_gate.py --include-live
```

Use `.env.example` as the local profile template. The real `.env` file is
ignored by git and must not be committed.

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

Then run the `Publish` workflow manually and choose `testpypi`.

Install from TestPyPI in a clean environment:

```bash
python3 -m venv /tmp/dsctl-testpypi-check
/tmp/dsctl-testpypi-check/bin/python -m pip install --upgrade pip
/tmp/dsctl-testpypi-check/bin/python -m pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  dolphinscheduler-cli
/tmp/dsctl-testpypi-check/bin/dsctl version
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

Recommended release trigger:

1. Update `CHANGELOG.md`.
2. Update `pyproject.toml` and `src/dsctl/__init__.py` to the release version.
3. Push a signed tag such as `v0.1.0`.
4. Create a GitHub Release from the tag.
5. Let the `Publish` workflow build and upload the distributions.
6. Verify `pipx install dolphinscheduler-cli` exposes `dsctl`.

The `Publish` workflow can also be started manually with `repository=pypi`.
Prefer the GitHub Release path for normal public releases so the published PyPI
version has a matching source tag and release page.
