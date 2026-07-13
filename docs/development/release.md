# Release Process

This checklist keeps GitHub and PyPI publication explicit and reproducible.

## Branch Flow

`main` is the only long-lived development branch. Keep it green and releasable.
Feature, fix, documentation, and release-preparation work is done on short-lived
branches and reviewed pull requests targeting `main`.

Prepare a normal release with this flow:

1. Create `release-prep/<version>` from an up-to-date `main`.
2. Update the version, changelog, release notes, and final documentation.
3. Run the local gate, package checks, and live gate when a cluster is available.
4. Merge the reviewed release-preparation pull request into `main`.
5. Wait for the resulting `main` commit to pass CI and record its full SHA.
6. Publish that commit to TestPyPI and verify a clean installation.
7. Create the version tag at that exact SHA.
8. Create the GitHub Release to publish to PyPI, then verify the public install.

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

```bash
python tools/check_quality_gate.py
python tools/check_release_version.py --tag vX.Y.Z
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

Set the candidate version and release ref explicitly. Use `main` for a normal
release or the maintained `release/<major>.<minor>` branch for a patch on an
older line. Fetch that ref and record the exact remote commit before dispatch:

```bash
version="X.Y.Z"  # replace with the candidate version
release_ref=main  # or release/X.Y for a maintained line
git fetch origin "$release_ref"
release_sha=$(git rev-parse "origin/$release_ref^{commit}")
gh workflow run publish.yml --ref "$release_ref"
```

Find and watch the dispatched run, then require its `headSha` to equal the
recorded release commit:

```bash
gh run list --workflow publish.yml --branch "$release_ref" \
  --event workflow_dispatch --limit 5
run_id="RUN_ID"  # copy the matching run ID from the list
gh run watch "$run_id" --exit-status
run_sha=$(gh run view "$run_id" --json headSha --jq .headSha)
test "$run_sha" = "$release_sha"
```

The final version tag must point at that same commit. TestPyPI and PyPI are
separate indexes, but neither permits replacing an already uploaded
distribution filename; never overwrite or silently reuse a candidate version.

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

Recommended release trigger:

1. Confirm the recorded release commit passed `main` CI and the TestPyPI clean
   install. For a maintenance release, require the corresponding maintenance
   branch CI instead.
2. Verify `python tools/check_release_version.py --tag "v$version"` against that
   commit.
3. Create an annotated tag at the recorded SHA, verify its peeled commit, and
   push it:

   ```bash
   tag="v$version"
   git tag -a "$tag" "$release_sha" -m "Release $version"
   test "$(git rev-parse "${tag}^{}")" = "$release_sha"
   git push origin "$tag"
   ```

   Sign the tag when a verified signing key is configured; do not claim an
   unsigned tag is signed.
4. Create a GitHub Release from the tag using the matching changelog section.
5. Let the `Publish` workflow build and upload the distributions.
6. Verify an exact-version clean install from PyPI and confirm `dsctl version`,
   `dsctl schema --list-groups`, and `dsctl capabilities`.

Manual workflow dispatch publishes only to TestPyPI and is restricted to
`main` or `release/*`. Formal PyPI publishing is triggered only by a published
GitHub Release, so an untagged branch head cannot bypass the release chain.
