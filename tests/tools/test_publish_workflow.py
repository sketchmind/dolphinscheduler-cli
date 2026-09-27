from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def _workflow_text() -> str:
    return (ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")


def test_publish_workflow_is_valid_yaml() -> None:
    workflow = yaml.safe_load(_workflow_text())

    assert isinstance(workflow, dict)
    assert "jobs" in workflow


def _job_block(workflow: str, name: str) -> str:
    lines = workflow.splitlines()
    marker = f"  {name}:"
    start = lines.index(marker)
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.startswith("  ") and not line.startswith("    ") and line.endswith(":"):
            end = index
            break
    return "\n".join(lines[start:end])


def test_publish_workflow_is_manual_and_uses_the_tag_as_data() -> None:
    workflow = _workflow_text()
    fetch = _job_block(workflow, "fetch-draft-assets")
    validate = _job_block(workflow, "validate-artifacts")

    assert "\n  workflow_dispatch:\n" in workflow
    assert "\n  release:\n" not in workflow
    assert "release_tag:" in workflow
    assert "target:" in workflow
    assert 'test "$GITHUB_REF" = refs/heads/main' in fetch
    assert "GH_REPO: ${{ github.repository }}" in fetch
    assert '[[ "$RELEASE_TAG" =~ ^v[0-9]+' in fetch
    assert 'gh release view "$RELEASE_TAG"' in fetch
    assert "--json isDraft --jq .isDraft" in fetch
    assert "--json isPrerelease --jq .isPrerelease" in fetch
    assert "--json assets --jq '.assets[].name'" in fetch
    assert 'test "$actual_assets" = "$expected_assets"' in fetch
    assert "ref: ${{ env.RELEASE_TAG }}" in validate
    assert "refs/tags/$RELEASE_TAG" in validate


def test_publish_workflow_promotes_one_live_tested_artifact_set() -> None:
    workflow = _workflow_text()
    validate = _job_block(workflow, "validate-artifacts")

    assert "python -m build" not in workflow
    assert "gh release download" in workflow
    assert "actions/upload-artifact@" in workflow
    assert "actions/download-artifact@" in workflow
    assert "tools/check_release_artifacts.py" in validate
    assert "python tools/check_exact_profile_read_evidence.py" in validate
    assert "--index-json testpypi.json" in validate
    assert "cmp dist/*.whl /tmp/testpypi-download/*.whl" in workflow
    assert "--clobber" not in workflow
    for name in ("validate-artifacts", "verify-testpypi", "verify-pypi"):
        job = _job_block(workflow, name)
        assert "pip install --upgrade packaging" in job


def test_publish_verification_smokes_installed_generated_artifacts() -> None:
    workflow = _workflow_text()
    for statement in (
        "validate_compiled_wire_installation()",
        "for ds_version in TARGET_DS_VERSIONS",
        'WORKFLOW_PROGRAMS.profile(ds_version).program("task_log")',
        "RESOURCE_PROGRAMS.profile(ds_version)",
        'resource.program("upload").codec.file_fields == ("file",)',
        'resource.program("download").codec.response_transport == "binary"',
        'TaskTypeAdapter.for_version("3.4.2")',
    ):
        assert workflow.count(statement) == 2
    assert "ExactGeneratedPackage" not in workflow


@pytest.mark.parametrize(
    "name", ["validate-artifacts", "verify-testpypi", "verify-pypi"]
)
def test_source_verifiers_install_canonical_runtime_and_codegen_dependencies(
    name: str,
) -> None:
    workflow = yaml.safe_load(_workflow_text())
    job = workflow["jobs"][name]
    steps = job["steps"]
    install_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Install verification tooling"
    )
    command = shlex.split(steps[install_index]["run"])
    assert command[:4] == ["python", "-m", "pip", "install"]
    assert {"packaging", "javalang>=0.13,<1", "dist/*.whl"} <= set(command)
    if name == "validate-artifacts":
        assert "twine" in command
    assert any(
        "actions/download-artifact@" in step.get("uses", "")
        for step in steps[:install_index]
    )
    verifier_indices = [
        index
        for index, step in enumerate(steps)
        if "tools/check_release_artifacts.py" in step.get("run", "")
        or "tools/check_exact_profile_read_evidence.py" in step.get("run", "")
    ]
    assert verifier_indices
    assert all(index > install_index for index in verifier_indices)
    assert "id-token" not in job.get("permissions", {})


def test_each_index_is_a_separate_explicit_environment_target() -> None:
    workflow = _workflow_text()
    testpypi = _job_block(workflow, "publish-testpypi")
    pypi = _job_block(workflow, "publish-pypi")

    assert "type: choice" in workflow
    assert "- testpypi" in workflow
    assert "- pypi" in workflow
    assert "if: inputs.target == 'testpypi'" in testpypi
    assert "environment:\n      name: testpypi" in testpypi
    assert "if: inputs.target == 'pypi'" in pypi
    assert "environment:\n      name: pypi" in pypi


def test_oidc_is_confined_to_minimal_publish_jobs() -> None:
    workflow = _workflow_text()
    fetch = _job_block(workflow, "fetch-draft-assets")
    validate = _job_block(workflow, "validate-artifacts")

    assert workflow.count("id-token: write") == 2
    assert "contents: write" in fetch
    assert "actions/checkout@" not in fetch
    assert "id-token: write" not in validate
    assert "persist-credentials: false" in validate

    for name in ("publish-testpypi", "publish-pypi"):
        publish = _job_block(workflow, name)
        assert "id-token: write" in publish
        assert "actions/download-artifact@" in publish
        assert "pypa/gh-action-pypi-publish@" in publish
        assert "actions/checkout@" not in publish
        assert "run:" not in publish


def test_publish_workflow_pins_every_action_to_an_immutable_sha() -> None:
    workflow = _workflow_text()
    uses_lines = [
        line.strip()
        for line in workflow.splitlines()
        if line.strip().startswith("uses:")
    ]

    assert uses_lines
    assert all(
        re.fullmatch(r"uses: [^@\s]+@[0-9a-f]{40}(?: # .+)?", line)
        for line in uses_lines
    )


def test_release_docs_require_two_approved_dispatches_before_publication() -> None:
    release = (ROOT / "docs" / "development" / "release.md").read_text(encoding="utf-8")
    normalized = " ".join(release.split())

    assert "Every release must run" in normalized
    assert "Protect `main` with a ruleset" in normalized
    assert "Protect `v*` tags with a tag ruleset" in normalized
    assert "required reviewers" in normalized
    assert "prevent self-review" in normalized
    assert "explicit authorization for the TestPyPI publication" in normalized
    assert "explicit authorization for PyPI" in normalized
    assert '--ref main -f release_tag="$tag" -f target=testpypi' in normalized
    assert '--ref main -f release_tag="$tag" -f target=pypi' in normalized
    assert '--ref "$tag"' not in release
    assert 'test "$actual_assets" = "$expected_assets"' in normalized
    assert "cmp dist/dolphinscheduler_cli-" in normalized
    pypi_dispatch = release.index("target=pypi")
    github_release = release.index('gh release edit "$tag" --draft=false')
    assert pypi_dispatch < github_release
