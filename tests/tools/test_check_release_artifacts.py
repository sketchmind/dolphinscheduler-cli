from __future__ import annotations

import base64
import csv
import hashlib
import importlib
import io
import json
import re
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_RECEIPT = (
    REPO_ROOT
    / "docs"
    / "development"
    / "live-evidence"
    / "3.4.2"
    / "2026-08-04-13b81eedd389.json"
)
METADATA = b"""Metadata-Version: 2.4
Name: dolphinscheduler-cli
Version: 0.4.0
Summary: Fixture package summary
Author: Fixture Contributors
Author-email: Fixture Release <release@example.test>
Maintainer-email: maintainer@example.test
License-Expression: Apache-2.0
Project-URL: Homepage, https://example.test/dsctl
Project-URL: Issues, https://example.test/dsctl/issues
Keywords: fixture,release
Classifier: Development Status :: 3 - Alpha
Classifier: Environment :: Console
Requires-Python: >=3.11
Description-Content-Type: text/markdown
License-File: LICENSE
Requires-Dist: httpx<1,>=0.27
Provides-Extra: dev
Requires-Dist: pytest<9,>=8; extra == "dev"
Dynamic: license-file

fixture package
"""
ENTRY_POINTS = b"[console_scripts]\ndsctl = dsctl.app:main\n"
TOP_LEVEL = b"dsctl\n"
WHEEL = (
    b"Wheel-Version: 1.0\n"
    b"Generator: release-fixture\n"
    b"Root-Is-Purelib: true\n"
    b"Tag: py3-none-any\n\n"
)
SETUP_CFG = b"[egg_info]\ntag_build = \ntag_date = 0\n\n"
PROMOTION_RECIPES = (
    ("doctor", "identity.current"),
    ("project.create", "project.create"),
    ("project.delete", "project.delete"),
    ("project.get", "project.get"),
    ("project.list", "project.page"),
    ("project.update", "project.update"),
    ("schedule.list", "schedule.page"),
    ("workflow.describe", "workflow.describe"),
    ("workflow.digest", "workflow.digest"),
    ("workflow.export", "workflow.export"),
    ("workflow.get", "workflow.get"),
    ("workflow.list", "workflow.page"),
    ("task.list", "task.list"),
    ("task.get", "task.get"),
    ("task.update", "task.update"),
)


def _load_module() -> ModuleType:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("check_release_artifacts")


@dataclass(frozen=True)
class _ReleaseBaseline:
    root: Path
    artifacts: tuple[Path, ...]
    receipt_name: str
    receipt_bytes: bytes


@pytest.fixture(scope="module")
def release_baseline(tmp_path_factory: pytest.TempPathFactory) -> _ReleaseBaseline:
    """Share read-only source and archives only with receipt/index mutation tests."""
    _load_module()
    root, artifacts, evidence_dir = _release_fixture(
        tmp_path_factory.mktemp("release-baseline")
    )
    receipt = next(evidence_dir.glob("*.json"))
    return _ReleaseBaseline(root, tuple(artifacts), receipt.name, receipt.read_bytes())


@pytest.fixture(autouse=True)
def accepted_exact_read_corpus(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[Path, dict[str, object]]]:
    calls: list[tuple[Path, dict[str, object]]] = []

    def check(evidence_root: Path, **expected: object) -> object:
        calls.append((evidence_root, expected))
        return SimpleNamespace(receipts=tuple(range(36)))

    monkeypatch.setattr(
        _load_module(), "check_exact_profile_read_evidence_corpus", check
    )
    return calls


@pytest.fixture(autouse=True)
def accepted_conformance_corpus(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[tuple[Path, dict[str, object]]], object]:
    checker = _load_module()
    calls: list[tuple[Path, dict[str, object]]] = []
    summary = SimpleNamespace(
        wheel_filename="synthetic-wheel.whl",
        wheel_sha256="sha256:" + "0" * 64,
        receipts=tuple(range(15)),
    )

    def check(evidence_root: Path, **expected: object) -> object:
        calls.append((evidence_root, expected))
        return summary

    monkeypatch.setattr(checker, "check_conformance_bundle_evidence_corpus", check)
    return calls, summary


def test_release_artifacts_bind_source_live_wheel_sdist_and_index(
    tmp_path: Path,
    accepted_conformance_corpus: tuple[list[tuple[Path, dict[str, object]]], object],
    accepted_exact_read_corpus: list[tuple[Path, dict[str, object]]],
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    conformance_root = tmp_path / "conformance-bundles"

    checked = checker.check_release_artifacts(
        root,
        artifacts,
        tag="v0.4.0",
        evidence_dir=evidence_dir,
        conformance_evidence_root=conformance_root,
        index_payload=_index_payload(artifacts, version="0.4.0"),
    )

    assert checked.version == "0.4.0"
    assert checked.wheel.name == "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    assert checked.sdist.name == "dolphinscheduler_cli-0.4.0.tar.gz"
    receipt = json.loads(checked.receipt.read_text(encoding="utf-8"))
    assert receipt["schema_version"] == 7
    assert set(receipt["profile"]["fingerprints"]) == {
        "consumed_projection",
        "effective_wire",
        "preservation",
        "source",
    }
    assert receipt["gate_bundle"]["actions"] == [
        action for action, _operation in PROMOTION_RECIPES
    ]
    assert len(receipt["gate_bundle"]["action_verifications"]) == 15
    assert len(receipt["gate_bundle"]["recipes"]) == 15
    calls, summary = accepted_conformance_corpus
    assert calls == [
        (
            conformance_root,
            {
                "expected_wheel_filename": checked.wheel.name,
                "expected_wheel_sha256": (
                    "sha256:" + checked.sha256_by_filename[checked.wheel.name]
                ),
                "source_root": root,
            },
        )
    ]
    assert checked.conformance_corpus is summary
    assert accepted_exact_read_corpus == [
        (
            root / "docs/development/live-evidence/exact-read",
            {
                "expected_wheel_filename": checked.wheel.name,
                "expected_wheel_sha256": (
                    "sha256:" + checked.sha256_by_filename[checked.wheel.name]
                ),
                "source_root": root,
            },
        )
    ]


def test_release_artifacts_preserve_utf8_wheel_and_sdist_description(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    readme = "fixture package \N{EN DASH} 多版本发布\n".encode()
    root, artifacts, evidence_dir = _release_fixture(tmp_path, readme=readme)

    checked = checker.check_release_artifacts(
        root,
        artifacts,
        tag="v0.4.0",
        evidence_dir=evidence_dir,
        conformance_evidence_root=tmp_path / "conformance-bundles",
        index_payload=_index_payload(artifacts, version="0.4.0"),
    )
    assert checked.wheel == artifacts[0]
    assert checked.sdist == artifacts[1]

    (root / "README.md").write_bytes("fixture package - 多版本发布\n".encode())
    with pytest.raises(
        ValueError, match="description metadata differs from project readme"
    ):
        checker.check_wheel_artifact(root, artifacts[0])


def test_release_artifacts_reject_missing_exact_read_corpus(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checker = _load_module()
    reader = importlib.import_module("live_gate.exact_profile_read_corpus")
    monkeypatch.setattr(
        checker,
        "check_exact_profile_read_evidence_corpus",
        reader.check_exact_profile_read_evidence_corpus,
    )
    root, artifacts, evidence_dir = _release_fixture(tmp_path)

    with pytest.raises(
        FileNotFoundError, match="exact-profile read evidence root does not exist"
    ):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_validate_sdist_before_conformance_corpus(
    tmp_path: Path,
    accepted_conformance_corpus: tuple[list[tuple[Path, dict[str, object]]], object],
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    _write_sdist(root, artifacts[1], metadata=b"Metadata-Version: 2.4\n")

    with pytest.raises(ValueError, match="sdist package metadata"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )

    calls, _summary = accepted_conformance_corpus
    assert calls == []


@pytest.mark.parametrize(
    "message",
    [
        "conformance-bundle evidence root does not exist",
        "legacy conformance receipt cannot satisfy current corpus",
        "wheel SHA-256 differs from expected value",
    ],
)
def test_release_artifacts_fail_closed_when_conformance_corpus_is_unsatisfied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    message: str,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)

    def reject(_evidence_root: Path, **_expected: object) -> object:
        raise ValueError(message)

    monkeypatch.setattr(
        checker,
        "check_conformance_bundle_evidence_corpus",
        reject,
    )

    with pytest.raises(ValueError, match=message):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_wheel_only_preflight_never_requires_conformance_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)

    def unexpected(*_args: object, **_kwargs: object) -> object:
        message = "wheel-only preflight consulted live evidence"
        raise AssertionError(message)

    monkeypatch.setattr(
        checker,
        "check_conformance_bundle_evidence_corpus",
        unexpected,
    )
    monkeypatch.setattr(checker, "check_exact_profile_read_evidence_corpus", unexpected)

    checked = checker.check_wheel_artifact(root, wheel)

    assert checked.wheel == wheel


def test_wheel_preflight_rejects_stale_build_lib_runtime_member(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    stale_module = root / "src" / "dsctl" / "services" / "_stale.py"
    stale_module.parent.mkdir(parents=True)
    stale_module.write_text("STALE = True\n", encoding="utf-8")
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)
    stale_module.unlink()

    with pytest.raises(ValueError, match="fail-closed package"):
        checker.check_wheel_artifact(root, wheel)


def test_wheel_preflight_returns_digest_and_exact_contract(tmp_path: Path) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)

    checked = checker.check_wheel_artifact(root, wheel)

    assert checked.version == "0.4.0"
    assert checked.wheel == wheel
    assert checked.sha256 == _sha256(wheel)
    assert checked.contract["ds_version"] == "3.4.2"


def test_wheel_preflight_accepts_core_metadata_header_reordering(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    headers, description = METADATA.split(b"\n\n", maxsplit=1)
    reordered = b"\n".join(reversed(headers.splitlines())) + b"\n\n" + description
    _write_wheel(root, wheel, metadata=reordered)

    checked = checker.check_wheel_artifact(root, wheel)

    assert checked.wheel == wheel


@pytest.mark.parametrize(
    ("header", "field", "metadata_version"),
    [
        (b"Platform: any", "Platform", b"2.4"),
        (b"Supported-Platform: any", "Supported-Platform", b"2.4"),
        (b"Home-page: https://example.test", "Home-page", b"2.4"),
        (
            b"Download-URL: https://example.test/download",
            "Download-URL",
            b"2.4",
        ),
        (b"Requires-External: make", "Requires-External", b"2.4"),
        (b"Provides-Dist: other (1)", "Provides-Dist", b"2.4"),
        (b"Obsoletes-Dist: old", "Obsoletes-Dist", b"2.4"),
        (b"Requires: olddep", "Requires", b"2.4"),
        (b"Provides: other", "Provides", b"2.4"),
        (b"Obsoletes: old", "Obsoletes", b"2.4"),
        (b"Import-Name: dsctl", "Import-Name", b"2.5"),
        (b"Import-Namespace: dsctl.extra", "Import-Namespace", b"2.5"),
    ],
)
def test_wheel_preflight_rejects_undeclared_core_metadata(
    tmp_path: Path,
    header: bytes,
    field: str,
    metadata_version: bytes,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA.replace(
        b"Metadata-Version: 2.4",
        b"Metadata-Version: " + metadata_version,
    )
    metadata = metadata.replace(
        b"\n\nfixture package",
        b"\n" + header + b"\n\nfixture package",
    )
    _write_wheel(root, wheel, metadata=metadata)

    with pytest.raises(ValueError, match=field):
        checker.check_wheel_artifact(root, wheel)


def test_wheel_preflight_requires_current_setuptools_dynamic_metadata(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA.replace(b"Dynamic: license-file\n", b"")
    _write_wheel(root, wheel, metadata=metadata)

    with pytest.raises(ValueError, match="Dynamic"):
        checker.check_wheel_artifact(root, wheel)


def test_wheel_preflight_requires_current_setuptools_metadata_version(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA.replace(b"Metadata-Version: 2.4", b"Metadata-Version: 2.5")
    _write_wheel(root, wheel, metadata=metadata)

    with pytest.raises(ValueError, match="Metadata-Version"):
        checker.check_wheel_artifact(root, wheel)


@pytest.mark.parametrize(
    ("source_expression", "wheel_expression"),
    [
        ("apache-2.0", "Apache-2.0"),
        (
            "mit and ( apache-2.0 or gpl-2.0-only with classpath-exception-2.0 )",
            "MIT AND(Apache-2.0 OR GPL-2.0-only WITH Classpath-exception-2.0)",
        ),
    ],
)
def test_wheel_preflight_normalizes_spdx_token_case_and_whitespace(
    tmp_path: Path,
    source_expression: str,
    wheel_expression: str,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    _replace_project_license(root, source_expression)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA.replace(
        b"License-Expression: Apache-2.0",
        f"License-Expression: {wheel_expression}".encode(),
    )
    _write_wheel(root, wheel, metadata=metadata)

    checked = checker.check_wheel_artifact(root, wheel)

    assert checked.wheel == wheel


def test_wheel_preflight_rejects_different_spdx_token_sequence(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    _replace_project_license(root, "MIT OR Apache-2.0")
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA.replace(
        b"License-Expression: Apache-2.0",
        b"License-Expression: MIT AND Apache-2.0",
    )
    _write_wheel(root, wheel, metadata=metadata)

    with pytest.raises(ValueError, match="License-Expression"):
        checker.check_wheel_artifact(root, wheel)


@pytest.mark.parametrize("invalid_source", [False, True])
def test_wheel_preflight_rejects_unparseable_spdx_expression(
    tmp_path: Path,
    *,
    invalid_source: bool,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA
    if invalid_source:
        _replace_project_license(root, "Apache-2.0 @ MIT")
    else:
        metadata = METADATA.replace(
            b"License-Expression: Apache-2.0",
            b"License-Expression: Apache-2.0 @ MIT",
        )
    _write_wheel(root, wheel, metadata=metadata)

    with pytest.raises(ValueError, match="invalid SPDX license expression"):
        checker.check_wheel_artifact(root, wheel)


@pytest.mark.parametrize(
    "expression",
    ["(" * 32 + "MIT" + ")" * 32, " OR ".join(["MIT"] * 128)],
    ids=["maximum-nesting", "maximum-valid-token-count"],
)
def test_wheel_preflight_accepts_spdx_budget_boundary(
    tmp_path: Path,
    expression: str,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    _replace_project_license(root, expression)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    metadata = METADATA.replace(
        b"License-Expression: Apache-2.0",
        f"License-Expression: {expression}".encode(),
    )
    _write_wheel(root, wheel, metadata=metadata)

    checked = checker.check_wheel_artifact(root, wheel)

    assert checked.wheel == wheel


@pytest.mark.parametrize(
    ("expression", "match"),
    [
        ("(" * 33 + "MIT" + ")" * 33, "nesting"),
        (" OR ".join(["MIT"] * 129), "token count"),
    ],
    ids=["nesting-over-limit", "tokens-over-limit"],
)
def test_wheel_preflight_rejects_spdx_over_budget(
    tmp_path: Path,
    expression: str,
    match: str,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    _replace_project_license(root, expression)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)

    with pytest.raises(ValueError, match=match):
        checker.check_wheel_artifact(root, wheel)


def test_wheel_preflight_cli_rejects_deep_spdx_without_recursion_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    expression = "(" * 600 + "MIT" + ")" * 600
    metadata = METADATA.replace(
        b"License-Expression: Apache-2.0",
        f"License-Expression: {expression}".encode(),
    )
    _write_wheel(root, wheel, metadata=metadata)

    exit_code = checker.main(["--wheel-only", "--root", str(root), str(wheel)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.splitlines() == [
        "release artifact check failed: invalid SPDX license expression: "
        "nesting exceeds limit 32"
    ]


def test_wheel_preflight_cli_reports_canonical_digest(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)

    exit_code = checker.main(["--wheel-only", "--root", str(root), str(wheel)])

    captured = capsys.readouterr()
    assert (exit_code, captured.out, captured.err) == (
        0,
        (
            "wheel artifact preflight passed: 0.4.0; "
            f"{wheel.name}=sha256:{_sha256(wheel)}\n"
        ),
        "",
    )


def test_wheel_preflight_cli_rejects_readme_drift(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)
    (root / "README.md").write_text("changed after build\n", encoding="utf-8")

    exit_code = checker.main(["--wheel-only", "--root", str(root), str(wheel)])

    assert exit_code == 1
    assert "description metadata differs from project readme" in capsys.readouterr().err


def test_wheel_preflight_cli_rejects_bad_zip_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    wheel.write_bytes(b"not a zip archive")

    exit_code = checker.main(["--wheel-only", "--root", str(root), str(wheel)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.splitlines() == [
        "release artifact check failed: File is not a zip file"
    ]


@pytest.mark.parametrize("corruption", ["unsupported-compression", "encrypted"])
def test_wheel_preflight_cli_normalizes_zip_member_read_failures(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    corruption: str,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)
    payload = bytearray(wheel.read_bytes())
    local_header = payload.index(b"PK\x03\x04")
    central_header = payload.index(b"PK\x01\x02")
    if corruption == "unsupported-compression":
        payload[local_header + 8 : local_header + 10] = (99).to_bytes(2, "little")
        payload[central_header + 10 : central_header + 12] = (99).to_bytes(
            2,
            "little",
        )
    else:
        local_flags = int.from_bytes(
            payload[local_header + 6 : local_header + 8],
            "little",
        )
        central_flags = int.from_bytes(
            payload[central_header + 8 : central_header + 10],
            "little",
        )
        payload[local_header + 6 : local_header + 8] = (local_flags | 1).to_bytes(
            2,
            "little",
        )
        payload[central_header + 8 : central_header + 10] = (
            central_flags | 1
        ).to_bytes(2, "little")
    wheel.write_bytes(payload)

    exit_code = checker.main(["--wheel-only", "--root", str(root), str(wheel)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert len(captured.err.splitlines()) == 1
    assert captured.err.startswith("release artifact check failed: ")


@pytest.mark.parametrize("explicit_default", [False, True])
def test_wheel_preflight_cli_rejects_explicit_evidence_dir(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    *,
    explicit_default: bool,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)
    evidence_dir = (
        checker.DEFAULT_EVIDENCE_DIR if explicit_default else tmp_path / "evidence"
    )

    exit_code = checker.main(
        [
            "--wheel-only",
            "--root",
            str(root),
            "--evidence-dir",
            str(evidence_dir),
            str(wheel),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "wheel-only preflight does not accept --evidence-dir" in captured.err


def test_wheel_preflight_cli_rejects_explicit_conformance_evidence_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)

    exit_code = checker.main(
        [
            "--wheel-only",
            "--root",
            str(root),
            "--conformance-evidence-root",
            str(tmp_path / "conformance-bundles"),
            str(wheel),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert (
        "wheel-only preflight does not accept --conformance-evidence-root"
        in captured.err
    )


@pytest.mark.parametrize(
    ("source", "replacement", "match"),
    [
        (
            'description = "Fixture package summary"',
            'description = "Changed package summary"',
            "Summary",
        ),
        (
            '{name = "Fixture Contributors"}',
            '{name = "Changed Contributors"}',
            "Author",
        ),
        (
            'email = "release@example.test"',
            'email = "changed-release@example.test"',
            "Author-email",
        ),
        (
            'email = "maintainer@example.test"',
            'email = "changed-maintainer@example.test"',
            "Maintainer-email",
        ),
        (
            'license = "Apache-2.0"',
            'license = "MIT"',
            "License-Expression",
        ),
        (
            'keywords = ["fixture", "release"]',
            'keywords = ["fixture", "changed"]',
            "Keywords",
        ),
        (
            'classifiers = ["Development Status :: 3 - Alpha", '
            '"Environment :: Console"]',
            'classifiers = ["Development Status :: 4 - Beta", '
            '"Environment :: Console"]',
            "Classifier",
        ),
        (
            'Homepage = "https://example.test/dsctl"',
            'Homepage = "https://changed.example.test/dsctl"',
            "Project-URL",
        ),
        (
            'license-files = ["LICENSE"]',
            'license-files = ["NOTICE"]',
            "License-File",
        ),
    ],
)
def test_wheel_preflight_rejects_project_core_metadata_drift(
    tmp_path: Path,
    source: str,
    replacement: str,
    match: str,
) -> None:
    checker = _load_module()
    root = tmp_path / "repo"
    _write_source_tree(root)
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_wheel(root, wheel)
    pyproject = root / "pyproject.toml"
    content = pyproject.read_text(encoding="utf-8")
    assert source in content
    pyproject.write_text(content.replace(source, replacement), encoding="utf-8")

    with pytest.raises(ValueError, match=match):
        checker.check_wheel_artifact(root, wheel)


def test_full_release_cli_mode_remains_available(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    accepted_conformance_corpus: tuple[list[tuple[Path, dict[str, object]]], object],
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)

    exit_code = checker.main(
        [
            "--tag",
            "v0.4.0",
            "--root",
            str(root),
            "--evidence-dir",
            str(evidence_dir),
            *(str(path) for path in artifacts),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "release artifact check passed: 0.4.0" in captured.out
    assert captured.err == ""
    calls, _summary = accepted_conformance_corpus
    assert calls[0][0] == (
        root / "docs" / "development" / "live-evidence" / "conformance-bundles"
    )


def test_release_artifacts_reject_wheel_not_bound_by_receipt(tmp_path: Path) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    with zipfile.ZipFile(artifacts[0], "a") as archive:
        archive.comment = b"same payload, different wheel bytes"

    with pytest.raises(ValueError, match="wheel SHA-256 differs"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_wheel_payload_that_differs_from_source(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    (root / "src" / "dsctl" / "__init__.py").write_text(
        '__version__ = "0.4.0"\nCHANGED = True\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="runtime content differs"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_extra_wheel_install_member(tmp_path: Path) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    with zipfile.ZipFile(artifacts[0], "a") as archive:
        archive.writestr("review_probe.pth", "import review_probe\n")

    with pytest.raises(ValueError, match="fail-closed package"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_wheel_metadata_that_differs_from_pyproject(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    pyproject = root / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace(
            '"httpx>=0.27,<1"',
            '"httpx>=0.28,<1"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Requires-Dist"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_wheel_script_that_differs_from_pyproject(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    pyproject = root / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace(
            'dsctl = "dsctl.app:main"',
            'dsctl = "dsctl.app:other"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="console scripts"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_sdist_source_that_differs_from_tag(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    (root / "docs" / "guide.md").write_text("changed after build\n", encoding="utf-8")

    with pytest.raises(ValueError, match="sdist controlled content differs"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


@pytest.mark.parametrize(
    "fixture_path",
    [
        "tests/fixtures/task_authoring/parameter_examples.json",
        "tests/fixtures/version_discovery/README.md",
        "tests/compatibility/corpus/v0.3.0-ds3.4.1-sql-inline.json",
    ],
)
@pytest.mark.parametrize("drift", ["missing", "tampered"])
def test_release_artifacts_reject_missing_or_tampered_test_input(
    tmp_path: Path, fixture_path: str, drift: str
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    _write_sdist(
        root,
        artifacts[1],
        omitted=fixture_path if drift == "missing" else None,
        replacement=(fixture_path, b"tampered fixture\n")
        if drift == "tampered"
        else None,
    )

    expected_error = (
        "sdist file set differs"
        if drift == "missing"
        else "sdist controlled content differs"
    )
    with pytest.raises(ValueError, match=expected_error) as exc_info:
        checker.check_release_artifacts(
            root, artifacts, tag="v0.4.0", evidence_dir=evidence_dir
        )
    assert fixture_path in str(exc_info.value)


@pytest.mark.parametrize(
    "private_path",
    [
        "tests/fixtures/.codex/config.json",
        "tests/fixtures/.CLAUDE/settings.json",
        "tests/fixtures/__pycache__/cached.json",
        "tests/fixtures/.pytest_cache/cached.json",
        "tests/fixtures/.env.production.json",
        "tests/fixtures/notes.yaml",
    ],
)
def test_release_artifacts_reject_uncontrolled_test_fixture_even_in_source(
    tmp_path: Path, private_path: str
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    private_source = root / private_path
    private_source.parent.mkdir(parents=True, exist_ok=True)
    private_source.write_bytes(b"private input\n")
    assert private_path not in checker._controlled_source_payload(root)
    _write_sdist(root, artifacts[1], replacement=(private_path, b"private input\n"))

    with pytest.raises(ValueError, match="sdist file set differs") as exc_info:
        checker.check_release_artifacts(
            root, artifacts, tag="v0.4.0", evidence_dir=evidence_dir
        )
    assert private_path in str(exc_info.value)


def test_release_artifacts_reject_stale_sdist_metadata(tmp_path: Path) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    _write_sdist(root, artifacts[1], metadata=b"Metadata-Version: 2.4\n")

    with pytest.raises(ValueError, match="sdist package metadata"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_stale_sdist_requirements(tmp_path: Path) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    _write_sdist(
        root,
        artifacts[1],
        requires=b"evil-startup-package==9.9.9\n",
    )

    with pytest.raises(ValueError, match="dependency metadata"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_require_complete_receipt(
    tmp_path: Path, release_baseline: _ReleaseBaseline
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["operation_trace"] = []
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="missing successful actions"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_require_receipt_contract_from_wheel(
    tmp_path: Path, release_baseline: _ReleaseBaseline
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["contract"]["source_contract_digest"] = f"sha256:{'0' * 64}"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="contract differs"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


@pytest.mark.parametrize(
    "fingerprint",
    ["consumed_projection", "effective_wire", "preservation", "source"],
)
def test_release_artifacts_require_current_profile_fingerprints(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
    fingerprint: str,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["profile"]["fingerprints"][fingerprint] = "sha256:" + "b" * 64
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=r"profile differs.+fingerprints"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


@pytest.mark.parametrize("schema_version", [3, 4, 5, 6])
def test_release_artifacts_keep_legacy_receipts_audit_only(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
    schema_version: int,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["schema_version"] = schema_version
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=r"requires one current schema-7 receipt"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


@pytest.mark.parametrize("action", [action for action, _ in PROMOTION_RECIPES])
def test_release_artifacts_require_all_current_action_verifications(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
    action: str,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["gate_bundle"]["action_verifications"][action] = "live_full"
    _refresh_gate_bundle_digest(payload)
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=rf"gate action verification {re.escape(action)} must equal 'live_smoke'",
    ):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


@pytest.mark.parametrize(
    "fingerprint",
    ["consumed_projection", "effective_wire", "preservation", "source"],
)
def test_release_artifacts_require_current_recipe_fingerprints(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
    fingerprint: str,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["gate_bundle"]["recipes"][14]["fingerprints"][fingerprint] = (
        "sha256:" + "b" * 64
    )
    _refresh_gate_bundle_digest(payload)
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=r"gate_bundle differs.+recipes"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_require_digest_bound_receipt_filename(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    receipt = next(evidence_dir.glob("*.json"))
    receipt.rename(evidence_dir / "receipt.json")

    with pytest.raises(ValueError, match="receipt filename must match"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


def test_release_artifacts_reject_test_index_byte_drift(
    tmp_path: Path, release_baseline: _ReleaseBaseline
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    index_payload = _index_payload(artifacts, version="0.4.0")
    urls = index_payload["urls"]
    assert isinstance(urls, list)
    file = urls[1]
    assert isinstance(file, dict)
    digests = file["digests"]
    assert isinstance(digests, dict)
    digests["sha256"] = "0" * 64

    with pytest.raises(ValueError, match="canonical release bytes"):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
            index_payload=index_payload,
        )


def test_release_artifacts_require_exact_canonical_pair(tmp_path: Path) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _release_fixture(tmp_path)
    extra = tmp_path / "dist" / "checksums.txt"
    extra.write_text("not a distribution", encoding="utf-8")

    with pytest.raises(ValueError, match="must be exactly"):
        checker.check_release_artifacts(
            root,
            [*artifacts, extra],
            tag="v0.4.0",
            evidence_dir=evidence_dir,
        )


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("packagetype", "sdist", "unexpected package type"),
        ("yanked", True, "yanked"),
    ],
)
def test_release_artifacts_reject_invalid_index_file_metadata(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
    field: str,
    value: object,
    match: str,
) -> None:
    checker = _load_module()
    root, artifacts, evidence_dir = _receipt_mutation_fixture(
        tmp_path, release_baseline
    )
    index_payload = _index_payload(artifacts, version="0.4.0")
    urls = index_payload["urls"]
    assert isinstance(urls, list)
    wheel = urls[0]
    assert isinstance(wheel, dict)
    wheel[field] = value

    with pytest.raises(ValueError, match=match):
        checker.check_release_artifacts(
            root,
            artifacts,
            tag="v0.4.0",
            evidence_dir=evidence_dir,
            index_payload=index_payload,
        )


def test_shared_release_inputs_keep_receipt_mutations_isolated(
    tmp_path: Path,
    release_baseline: _ReleaseBaseline,
) -> None:
    root, artifacts, first = _receipt_mutation_fixture(
        tmp_path / "first", release_baseline
    )
    _, other_artifacts, second = _receipt_mutation_fixture(
        tmp_path / "second", release_baseline
    )
    first_receipt = first / release_baseline.receipt_name
    first_receipt.write_bytes(b"changed receipt")
    artifacts.clear()

    assert root == release_baseline.root
    assert tuple(other_artifacts) == release_baseline.artifacts
    assert (
        second / release_baseline.receipt_name
    ).read_bytes() == release_baseline.receipt_bytes


def _receipt_mutation_fixture(
    tmp_path: Path,
    baseline: _ReleaseBaseline,
) -> tuple[Path, list[Path], Path]:
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / baseline.receipt_name).write_bytes(baseline.receipt_bytes)
    return baseline.root, list(baseline.artifacts), evidence_dir


def _release_fixture(
    tmp_path: Path, *, readme: bytes = b"fixture package\n"
) -> tuple[Path, list[Path], Path]:
    root = tmp_path / "repo"
    _write_source_tree(root)
    (root / "README.md").write_bytes(readme)
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    sdist = dist / "dolphinscheduler_cli-0.4.0.tar.gz"
    metadata = METADATA.replace(b"fixture package\n", readme)
    _write_wheel(root, wheel, metadata=metadata)
    _write_sdist(root, sdist, metadata=metadata)

    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    receipt = _schema_seven_receipt(root, wheel=wheel)
    receipt_name = f"2026-08-03-{_sha256(wheel)[:12]}.json"
    (evidence_dir / receipt_name).write_text(
        json.dumps(receipt),
        encoding="utf-8",
    )
    return root, [wheel, sdist], evidence_dir


def _schema_seven_receipt(root: Path, *, wheel: Path) -> dict[str, object]:
    corpus = importlib.import_module("live_gate.exact_profile_read_corpus")
    artifacts = corpus.load_tracked_artifacts(root)
    profile = artifacts.profiles["3.4.2"]
    assert isinstance(profile, dict)
    actions = profile["actions"]
    decisions = profile["build_decisions"]
    assert isinstance(actions, dict)
    assert isinstance(decisions, dict)
    recipes = []
    action_verifications = {}
    for action, operation in PROMOTION_RECIPES:
        capability = actions[action]
        decision = decisions[operation]
        assert isinstance(capability, dict)
        assert isinstance(decision, dict)
        action_verifications[action] = capability["verification"]
        recipes.append(
            {
                "action": action,
                "semantic_operation": decision["semantic_operation"],
                "build_status": decision["build_status"],
                "fingerprints": decision["fingerprints"],
            }
        )
    gate_bundle = {
        "actions": [action for action, _operation in PROMOTION_RECIPES],
        "action_verifications": action_verifications,
        "recipes": recipes,
    }
    receipt: dict[str, object] = json.loads(SOURCE_RECEIPT.read_text(encoding="utf-8"))
    receipt["schema_version"] = 7
    fixture = receipt["fixture"]
    assert isinstance(fixture, dict)
    fixture["provisioner"] = "dsmatrix-exact-read-state-projection/v2"
    receipt["runner"] = {
        "artifact": "installed-wheel-console-script",
        "cli_version": artifacts.cli_version,
        "wheel_filename": wheel.name,
        "wheel_sha256": f"sha256:{_sha256(wheel)}",
    }
    receipt["profile"] = {
        "ds": profile["server_version"],
        "selected_ds_version": profile["server_version"],
        "contract_version": profile["contract_version"],
        "family": profile["family"],
        "support_level": profile["support_level"],
        "tested": profile["tested"],
        "fingerprints": profile["fingerprints"],
    }
    receipt["contract"] = artifacts.contracts["3.4.2"]
    receipt["gate_bundle"] = {
        **gate_bundle,
        "digest": _canonical_digest(gate_bundle),
    }
    return receipt


def _canonical_digest(value: object) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _refresh_gate_bundle_digest(payload: dict[str, object]) -> None:
    gate_bundle = payload["gate_bundle"]
    assert isinstance(gate_bundle, dict)
    digest_payload = {
        key: gate_bundle[key] for key in ("actions", "action_verifications", "recipes")
    }
    gate_bundle["digest"] = _canonical_digest(digest_payload)


def _write_source_tree(root: Path) -> None:
    files = {
        "pyproject.toml": """[build-system]
requires = ["setuptools>=77", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "dolphinscheduler-cli"
version = "0.4.0"
description = "Fixture package summary"
readme = "README.md"
requires-python = ">=3.11"
authors = [
  {name = "Fixture Contributors"},
  {name = "Fixture Release", email = "release@example.test"},
]
maintainers = [
  {email = "maintainer@example.test"},
]
license = "Apache-2.0"
license-files = ["LICENSE"]
keywords = ["fixture", "release"]
classifiers = ["Development Status :: 3 - Alpha", "Environment :: Console"]
dependencies = ["httpx>=0.27,<1"]

[project.urls]
Homepage = "https://example.test/dsctl"
Issues = "https://example.test/dsctl/issues"

[project.scripts]
dsctl = "dsctl.app:main"

[project.optional-dependencies]
dev = ["pytest>=8,<9"]
""",
        "MANIFEST.in": "include README.md\nrecursive-include docs *.md\n",
        "README.md": "fixture package\n",
        "LICENSE": "license\n",
        "CHANGELOG.md": "changes\n",
        "SECURITY.md": "security\n",
        "CONTRIBUTING.md": "contributing\n",
        "src/dsctl/__init__.py": '__version__ = "0.4.0"\n',
        "docs/guide.md": "guide\n",
        "docs/development/live-evidence/3.4.2/fixture.json": "{}\n",
        "tests/test_fixture.py": "def test_fixture():\n    assert True\n",
        "tests/fixtures/task_authoring/parameter_examples.json": '{"tasks": []}\n',
        "tests/fixtures/version_discovery/README.md": "Reviewed Swagger fixture.\n",
        "tests/compatibility/corpus/v0.3.0-ds3.4.1-sql-inline.json": (
            '{"workflow": {}}\n'
        ),
        "tools/helper.py": "VALUE = 1\n",
        "tools/data.json": "{}\n",
        "tools/allowlist.txt": "entry\n",
    }
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    source_generated = REPO_ROOT / "src" / "dsctl" / "generated"
    fixture_generated = root / "src" / "dsctl" / "generated"
    fixture_generated.mkdir(parents=True, exist_ok=True)
    (fixture_generated / "version_profiles.py").write_bytes(
        (source_generated / "version_profiles.py").read_bytes()
    )
    for manifest in (
        *(source_generated / "versions").glob("ds_*/_manifest.py"),
        *(source_generated / "wire_programs").rglob("*.py"),
        *(source_generated / "wire_runtime").rglob("*.py"),
    ):
        fixture_manifest = fixture_generated / manifest.relative_to(source_generated)
        fixture_manifest.parent.mkdir(parents=True, exist_ok=True)
        fixture_manifest.write_bytes(manifest.read_bytes())


def _replace_project_license(root: Path, expression: str) -> None:
    pyproject = root / "pyproject.toml"
    content = pyproject.read_text(encoding="utf-8")
    source = 'license = "Apache-2.0"'
    assert source in content
    pyproject.write_text(
        content.replace(source, f'license = "{expression}"'),
        encoding="utf-8",
    )


def _write_wheel(
    root: Path,
    wheel: Path,
    *,
    metadata: bytes = METADATA,
) -> None:
    files = {
        source.relative_to(root / "src").as_posix(): source.read_bytes()
        for source in (root / "src" / "dsctl").rglob("*")
        if source.is_file()
    }
    dist_info = "dolphinscheduler_cli-0.4.0.dist-info"
    files.update(
        {
            f"{dist_info}/METADATA": metadata,
            f"{dist_info}/WHEEL": WHEEL,
            f"{dist_info}/entry_points.txt": ENTRY_POINTS,
            f"{dist_info}/top_level.txt": TOP_LEVEL,
            f"{dist_info}/licenses/LICENSE": (root / "LICENSE").read_bytes(),
        }
    )
    record_name = f"{dist_info}/RECORD"
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, content in sorted(files.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest())
        writer.writerow([name, f"sha256={digest.rstrip(b'=').decode()}", len(content)])
    writer.writerow([record_name, "", ""])
    files[record_name] = record.getvalue().encode()
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, content in sorted(files.items()):
            archive.writestr(name, content)


def _write_sdist(
    root: Path,
    sdist: Path,
    *,
    metadata: bytes = METADATA,
    requires: bytes = b"httpx<1,>=0.27\n\n[dev]\npytest<9,>=8\n",
    omitted: str | None = None,
    replacement: tuple[str, bytes] | None = None,
) -> None:
    controlled = _controlled_source_payload(root)
    if omitted is not None:
        del controlled[omitted]
    if replacement is not None:
        controlled[replacement[0]] = replacement[1]
    egg_info = "src/dolphinscheduler_cli.egg-info"
    generated = {
        "PKG-INFO": metadata,
        "setup.cfg": SETUP_CFG,
        f"{egg_info}/PKG-INFO": metadata,
        f"{egg_info}/dependency_links.txt": b"\n",
        f"{egg_info}/entry_points.txt": ENTRY_POINTS,
        f"{egg_info}/requires.txt": requires,
        f"{egg_info}/top_level.txt": TOP_LEVEL,
    }
    sources = {
        *controlled,
        f"{egg_info}/PKG-INFO",
        f"{egg_info}/SOURCES.txt",
        f"{egg_info}/dependency_links.txt",
        f"{egg_info}/entry_points.txt",
        f"{egg_info}/requires.txt",
        f"{egg_info}/top_level.txt",
    }
    generated[f"{egg_info}/SOURCES.txt"] = ("\n".join(sorted(sources)) + "\n").encode()
    prefix = "dolphinscheduler_cli-0.4.0"
    with tarfile.open(sdist, "w:gz") as archive:
        for relative, content in sorted({**controlled, **generated}.items()):
            info = tarfile.TarInfo(f"{prefix}/{relative}")
            info.size = len(content)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(content))


def _controlled_source_payload(root: Path) -> dict[str, bytes]:
    root_files = {
        name: (root / name).read_bytes()
        for name in (
            "pyproject.toml",
            "MANIFEST.in",
            "README.md",
            "LICENSE",
            "CHANGELOG.md",
            "SECURITY.md",
            "CONTRIBUTING.md",
        )
    }
    patterns = (
        ("src/dsctl", {"*"}),
        ("docs", {".md"}),
        ("docs/development/live-evidence", {".json"}),
        ("tests", {".py"}),
        ("tests/fixtures", {".json"}),
        ("tests/compatibility/corpus", {".json"}),
        ("tools", {".py", ".json", ".txt"}),
    )
    payload = dict(root_files)
    readme = "tests/fixtures/version_discovery/README.md"
    payload[readme] = (root / readme).read_bytes()
    for base, suffixes in patterns:
        for path in (root / base).rglob("*"):
            if not path.is_file():
                continue
            if "*" not in suffixes and path.suffix not in suffixes:
                continue
            payload[path.relative_to(root).as_posix()] = path.read_bytes()
    return payload


def _index_payload(artifacts: list[Path], *, version: str) -> dict[str, object]:
    return {
        "info": {"version": version},
        "urls": [
            {
                "filename": path.name,
                "digests": {"sha256": _sha256(path)},
                "packagetype": "bdist_wheel" if path.suffix == ".whl" else "sdist",
                "yanked": False,
            }
            for path in artifacts
        ],
    }


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
