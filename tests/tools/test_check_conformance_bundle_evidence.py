from __future__ import annotations

import importlib
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from dsctl import __version__
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType


_LEGACY_BUNDLE = "legacy_core/v1"
_FULL_BUNDLE = "full_core/v1"
_WHEEL_FILENAME = f"dolphinscheduler_cli-{__version__}-py3-none-any.whl"
_WHEEL_SHA256 = "sha256:" + "a" * 64
_RECEIPT_DATE = "2026-08-10"


def test_checker_accepts_exactly_one_highest_bundle_receipt_per_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)
    calls: list[tuple[str, str]] = []

    def validate(receipt: object, **expected: str | None) -> object:
        assert isinstance(receipt, dict)
        version = receipt["version"]
        assert isinstance(version, str)
        bundle = _expected_bundle(version)
        assert expected == {
            "expected_ds_version": version,
            "expected_bundle": bundle,
            "expected_wheel_filename": None if not calls else _WHEEL_FILENAME,
            "expected_wheel_sha256": None if not calls else _WHEEL_SHA256,
        }
        calls.append((version, bundle))
        return SimpleNamespace(
            schema_version=1,
            ds_version=version,
            bundle=bundle,
            wheel_filename=_WHEEL_FILENAME,
            wheel_sha256=_WHEEL_SHA256,
            receipt_digest="sha256:" + version.replace(".", "0").ljust(64, "0"),
            required_actions=("doctor",),
            authoring_mode="opaque",
        )

    validator_loads = _install_prepared_validator(
        corpus,
        monkeypatch,
        source_root=corpus.ROOT,
        validate=validate,
    )

    summary = corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert summary.versions == TARGET_DS_VERSIONS
    assert summary.wheel_filename == _WHEEL_FILENAME
    assert summary.wheel_sha256 == _WHEEL_SHA256
    assert summary.bundle_counts == _expected_bundle_counts()
    assert len(summary.receipts) == len(TARGET_DS_VERSIONS)
    assert calls == [
        (version, _expected_bundle(version)) for version in TARGET_DS_VERSIONS
    ]
    assert validator_loads == [corpus.ROOT]


def test_cli_emits_a_machine_auditable_same_wheel_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_cli_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)

    returncode = checker.main(
        [
            "--evidence-root",
            str(evidence_root),
            "--expected-wheel-filename",
            _WHEEL_FILENAME,
            "--expected-wheel-sha256",
            _WHEEL_SHA256,
        ]
    )

    assert returncode == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "schema_version": 1,
        "kind": "dsctl-conformance-bundle-corpus-summary",
        "status": "passed",
        "evidence_root": str(evidence_root.absolute()),
        "receipt_count": len(TARGET_DS_VERSIONS),
        "versions": list(TARGET_DS_VERSIONS),
        "bundles": _expected_bundle_counts(),
        "wheel": {
            "filename": _WHEEL_FILENAME,
            "sha256": _WHEEL_SHA256,
        },
        "receipts": [
            {
                "ds_version": version,
                "bundle": _expected_bundle(version),
                "path": (f"{version}/{_RECEIPT_DATE}-{_WHEEL_SHA256[7:19]}.json"),
                "receipt_digest": _receipt_digest(evidence_root, version),
                "authoring_mode": (
                    "typed" if _expected_bundle(version) == _LEGACY_BUNDLE else "opaque"
                ),
            }
            for version in TARGET_DS_VERSIONS
        ],
    }


@pytest.mark.parametrize("missing_version", ["2.0.9", "3.4.3"])
def test_checker_reports_missing_and_unexpected_version_directories(
    tmp_path: Path,
    missing_version: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    shutil.rmtree(evidence_root / missing_version)
    (evidence_root / "9.9.9").mkdir()
    escaped_version = missing_version.replace(".", r"\.")

    with pytest.raises(
        ValueError,
        match=(
            rf"missing version directories: {escaped_version}; "
            r"unexpected root entries: 9\.9\.9"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_any_non_version_entry_at_the_corpus_root(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    (evidence_root / "README.md").write_text("not a receipt\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"unexpected root entries: README\.md"):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_requires_exactly_one_receipt_per_version_directory(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    duplicate = evidence_root / "3.2.2" / "2026-08-11-aaaaaaaaaaaa.json"
    duplicate.write_text("{}\n", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 requires exactly one highest-bundle receipt; found 2",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


@pytest.mark.parametrize(
    ("filename", "message"),
    [
        ("receipt.json", "receipt filename must match"),
        ("2026-02-30-aaaaaaaaaaaa.json", "date is not a calendar date"),
        ("2026-08-10-bbbbbbbbbbbb.json", "filename digest suffix must be"),
    ],
)
def test_checker_requires_the_exact_governed_receipt_filename(
    tmp_path: Path,
    filename: str,
    message: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    receipt.rename(receipt.with_name(filename))

    with pytest.raises(ValueError, match=message):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_binds_receipt_filename_date_to_recorded_at_utc_date(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    receipt["recorded_at"] = "2026-08-11T00:00:00+00:00"
    cluster = receipt["dolphinscheduler"]
    assert isinstance(cluster, dict)
    cluster["image_observed_at"] = "2026-08-10T23:59:00+00:00"
    _refresh_receipt_digest(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=(
            r"filename date '2026-08-10' must equal receipt recorded_at "
            r"UTC date '2026-08-11'"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_receipt_without_public_image_provenance(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "2.0.9")
    receipt = _read_receipt(receipt_path)
    cluster = receipt["dolphinscheduler"]
    assert isinstance(cluster, dict)
    del cluster["image_provenance"]
    _refresh_receipt_digest(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(ValueError, match="dolphinscheduler keys differ"):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_uses_utc_when_binding_filename_date_to_recorded_at(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    receipt["recorded_at"] = "2026-08-11T01:00:00+08:00"
    cluster = receipt["dolphinscheduler"]
    assert isinstance(cluster, dict)
    cluster["image_observed_at"] = "2026-08-10T16:59:00+00:00"
    _refresh_receipt_digest(receipt)
    _write_receipt(receipt_path, receipt)

    summary = corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert len(summary.receipts) == len(TARGET_DS_VERSIONS)


def test_checker_rejects_a_lower_inherited_bundle_when_full_is_ready(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _new_receipt(bundle_name=_LEGACY_BUNDLE, ds_version="2.0.9")
    _write_receipt(_receipt_path(evidence_root, "2.0.9"), receipt)

    with pytest.raises(
        ValueError,
        match=r"bundle must equal expected value 'full_core/v1'",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


@pytest.mark.parametrize("runner_field", ["wheel_filename", "wheel_sha256"])
def test_checker_requires_one_exact_wheel_identity_across_all_receipts(
    tmp_path: Path,
    runner_field: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.4.2")
    receipt = _read_receipt(receipt_path)
    runner = receipt["runner"]
    assert isinstance(runner, dict)
    runner[runner_field] = (
        "different.whl" if runner_field == "wheel_filename" else "sha256:" + "b" * 64
    )
    _refresh_receipt_digest(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=rf"{runner_field.replace('_', ' ')} must equal expected value",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_binds_the_corpus_to_explicit_expected_wheel_identity(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)

    with pytest.raises(ValueError, match="wheel filename must equal expected value"):
        corpus.check_conformance_bundle_evidence_corpus(
            evidence_root,
            expected_wheel_filename="another.whl",
            expected_wheel_sha256="sha256:" + "b" * 64,
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("schema", r"schema_version must equal 1"),
        ("digest", r"receipt_digest must equal"),
        ("assessment", r"bundle assessment_digest must equal"),
        ("inheritance", r"bundle inheritance count differs from extends"),
    ],
)
def test_checker_delegates_schema_digest_and_current_assessment_validation(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    if case == "schema":
        receipt["schema_version"] = 2
        _refresh_receipt_digest(receipt)
    elif case == "digest":
        receipt["receipt_digest"] = "sha256:" + "0" * 64
    else:
        bundle = receipt["conformance_bundle"]
        assert isinstance(bundle, dict)
        if case == "assessment":
            bundle["assessment_digest"] = "sha256:" + "0" * 64
        else:
            bundle["inheritance"] = []
        _refresh_receipt_digest(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(ValueError, match=message):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_cli_default_tracked_root_is_fail_closed_until_corpus_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_cli_module()
    missing = tmp_path / "not-yet-tracked-conformance"
    monkeypatch.setattr(checker, "DEFAULT_EVIDENCE_ROOT", missing)

    returncode = checker.main([])

    assert returncode == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert f"conformance-bundle evidence root does not exist: {missing}" in captured.err
    assert "track one current highest-bundle receipt" in captured.err


def test_checker_safely_loads_versions_from_the_selected_source_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    support = importlib.import_module("tests.tools.test_conformance_bundle_evidence")
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)
    source_root = support._copy_current_source_truth(tmp_path)
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    marker = tmp_path / "generated-profile-executed"
    profile_path.write_text(
        profile_path.read_text(encoding="utf-8")
        + f"\n__import__('pathlib').Path({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )
    calls: list[str] = []

    def validate(receipt: object, **expected: object) -> object:
        assert isinstance(receipt, dict)
        version = receipt["version"]
        assert isinstance(version, str)
        calls.append(version)
        return SimpleNamespace(
            schema_version=1,
            ds_version=version,
            bundle=_expected_bundle(version),
            wheel_filename=_WHEEL_FILENAME,
            wheel_sha256=_WHEEL_SHA256,
            receipt_digest="sha256:" + version.replace(".", "0").ljust(64, "0"),
            required_actions=("doctor",),
            authoring_mode="opaque",
        )

    validator_loads = _install_prepared_validator(
        corpus,
        monkeypatch,
        source_root=source_root,
        validate=validate,
    )

    summary = corpus.check_conformance_bundle_evidence_corpus(
        evidence_root,
        source_root=source_root,
    )

    assert summary.versions == TARGET_DS_VERSIONS
    assert calls == list(TARGET_DS_VERSIONS)
    assert validator_loads == [source_root]
    assert not marker.exists()


def test_checker_follows_a_new_highest_ready_bundle_from_source_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    source_root, assessment, support = _candidate_assessment(tmp_path)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    full = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == _FULL_BUNDLE
    )
    extended = deepcopy(full)
    extended_name = "extended_core/v1"
    extended["name"] = extended_name
    extended["extends"] = [_FULL_BUNDLE]
    extended["direct_actions"] = []
    extended["bundle_digest"] = support._bundle_digest(
        extended_name,
        extended["required_actions"],
    )
    bundles.append(extended)
    _refresh_assessment_summary(assessment)
    support._refresh_assessment_digests(assessment)
    support._write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)
    expected_bundles: dict[str, str] = {}

    def validate(receipt: object, **expected: object) -> object:
        assert isinstance(receipt, dict)
        version = receipt["version"]
        bundle = expected["expected_bundle"]
        assert isinstance(version, str)
        assert isinstance(bundle, str)
        expected_bundles[version] = bundle
        return SimpleNamespace(
            schema_version=1,
            ds_version=version,
            bundle=bundle,
            wheel_filename=_WHEEL_FILENAME,
            wheel_sha256=_WHEEL_SHA256,
            receipt_digest="sha256:" + version.replace(".", "0").ljust(64, "0"),
            required_actions=("doctor",),
            authoring_mode="opaque",
        )

    validator_loads = _install_prepared_validator(
        corpus,
        monkeypatch,
        source_root=source_root,
        validate=validate,
    )

    summary = corpus.check_conformance_bundle_evidence_corpus(
        evidence_root,
        source_root=source_root,
    )

    baseline_bundles = _current_highest_ready_bundles()
    expected_after_extension = {
        version: (
            extended_name
            if baseline_bundles[version] == _FULL_BUNDLE
            else baseline_bundles[version]
        )
        for version in TARGET_DS_VERSIONS
    }
    assert expected_bundles == expected_after_extension
    assert summary.bundle_counts == dict(Counter(expected_after_extension.values()))
    assert validator_loads == [source_root]


def test_checker_rejects_incomparable_highest_ready_bundles_after_truth_change(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    source_root, assessment, support = _candidate_assessment(tmp_path)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    full = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == _FULL_BUNDLE
    )
    full["extends"] = []
    full["direct_actions"] = deepcopy(full["required_actions"])
    bundles.sort(key=lambda bundle: bundle["name"])
    support._refresh_assessment_digests(assessment)
    support._write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)
    first_shared_ready_version = next(
        version
        for version in TARGET_DS_VERSIONS
        if _bundle_coordinate_status(assessment, _LEGACY_BUNDLE, version) == "ready"
        and _bundle_coordinate_status(assessment, _FULL_BUNDLE, version) == "ready"
    )
    escaped_version = first_shared_ready_version.replace(".", r"\.")

    with pytest.raises(
        ValueError,
        match=(
            rf"DS {escaped_version} has multiple "
            r"incomparable highest-ready conformance "
            r"bundles: full_core/v1, legacy_core/v1"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(
            evidence_root,
            source_root=source_root,
        )


def test_checker_rejects_a_version_without_any_ready_bundle_in_source_truth(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    source_root, assessment, support = _candidate_assessment(tmp_path)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    full = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == _FULL_BUNDLE
    )
    full["extends"] = []
    absent_action = "alert-plugin.create"
    required_actions = full["required_actions"]
    assert isinstance(required_actions, list)
    required_actions.append(absent_action)
    required_actions.sort()
    full["direct_actions"] = deepcopy(required_actions)
    full["bundle_digest"] = support._bundle_digest(
        _FULL_BUNDLE,
        required_actions,
    )
    _refresh_bundle_coordinates_from_profiles(full, required_actions=required_actions)
    assessment["bundles"] = [full]
    _refresh_assessment_summary(assessment)
    support._refresh_assessment_digests(assessment)
    support._write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)

    with pytest.raises(
        ValueError,
        match=r"DS 1\.3\.9 has no ready conformance bundle",
    ):
        corpus.check_conformance_bundle_evidence_corpus(
            evidence_root,
            source_root=source_root,
        )


def test_checker_fails_closed_on_tampered_assessment_readiness(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    source_root, assessment, support = _candidate_assessment(tmp_path)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    full = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == _FULL_BUNDLE
    )
    coordinates = full["versions"]
    assert isinstance(coordinates, list)
    coordinate = next(
        row
        for row in coordinates
        if isinstance(row, dict) and row.get("version") == "2.0.9"
    )
    coordinate["status"] = "blocked"
    full["ready_coordinate_count"] = 12
    full["blocked_coordinate_count"] = 3
    _refresh_assessment_summary(assessment)
    support._refresh_assessment_digests(assessment)
    support._write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)

    with pytest.raises(
        ValueError,
        match=(
            r"current conformance assessment is invalid: .*readiness and "
            r"blockers are inconsistent"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(
            evidence_root,
            source_root=source_root,
        )


def test_checker_rejects_nonliteral_generated_profile_without_executing_it(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)
    source_root = tmp_path / "candidate-source"
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    profile_path.parent.mkdir(parents=True)
    marker = tmp_path / "generated-profile-executed"
    profile_path.write_text(
        "_PROFILE_JSON = "
        f"__import__('pathlib').Path({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"_PROFILE_JSON must be a literal assignment",
    ):
        corpus.check_conformance_bundle_evidence_corpus(
            evidence_root,
            source_root=source_root,
        )

    assert not marker.exists()


def test_checker_rejects_source_root_target_versions_outside_runtime_bundle_manifest(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    support = importlib.import_module("tests.tools.test_conformance_bundle_evidence")
    evidence_root = tmp_path / "conformance-bundles"
    _write_stub_corpus(evidence_root)
    source_root = support._copy_current_source_truth(tmp_path)
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    profile_json = json.dumps(
        {
            "schema_version": 1,
            "target_versions": list(TARGET_DS_VERSIONS[:-1]),
        }
    )
    profile_path.write_text(
        f"_PROFILE_JSON = {profile_json!r}\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=(
            r"generated target versions differ from the canonical "
            r"runtime bundle manifest"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(
            evidence_root,
            source_root=source_root,
        )


def test_importing_corpus_checker_does_not_execute_generated_profile_module(
    tmp_path: Path,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    candidate_source = tmp_path / "candidate" / "src"
    generated = candidate_source / "dsctl" / "generated"
    generated.mkdir(parents=True)
    (candidate_source / "dsctl" / "__init__.py").write_text("", encoding="utf-8")
    (generated / "__init__.py").write_text("", encoding="utf-8")
    marker = tmp_path / "generated-profile-executed"
    (generated / "version_profiles.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('executed')\n"
        f"TARGET_DS_VERSIONS = {TARGET_DS_VERSIONS!r}\n",
        encoding="utf-8",
    )
    python_path = os.pathsep.join(
        (
            str(candidate_source),
            str(project_root / "tools"),
            str(project_root / "src"),
        )
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = python_path

    result = subprocess.run(
        [sys.executable, "-c", "import live_gate.conformance_bundle_corpus"],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert not marker.exists()


def test_cli_rejects_a_symlink_evidence_root_without_resolving_its_identity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_cli_module()
    real_root = tmp_path / "real-conformance-bundles"
    _write_real_corpus(real_root)
    evidence_root = tmp_path / "conformance-bundles"
    evidence_root.symlink_to(real_root, target_is_directory=True)

    returncode = checker.main(["--evidence-root", str(evidence_root)])

    assert returncode == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        f"conformance-bundle evidence root must not be a symlink: {evidence_root}"
        in captured.err
    )


@pytest.mark.parametrize("replacement_kind", ["symlink", "different-inode", "move-out"])
def test_checker_rejects_corpus_root_replaced_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    moved_root = tmp_path / "moved-conformance-bundles"
    real_open = os.open
    replaced = False

    def replace_after_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal replaced
        if dir_fd is None and os.fsdecode(path) == str(evidence_root) and not replaced:
            descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
            replaced = True
            evidence_root.rename(moved_root)
            if replacement_kind == "symlink":
                evidence_root.symlink_to(moved_root, target_is_directory=True)
            elif replacement_kind == "different-inode":
                shutil.copytree(moved_root, evidence_root)
            return descriptor
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", replace_after_open)

    with pytest.raises(
        ValueError,
        match=r"conformance-bundle evidence root changed during validation",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert replaced


@pytest.mark.parametrize("target", ["root", "version"])
def test_checker_rejects_unsafe_directory_modes(
    tmp_path: Path,
    target: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    unsafe = evidence_root if target == "root" else evidence_root / "3.2.2"
    unsafe.chmod(0o777)

    with pytest.raises(
        ValueError,
        match=rf"has unsafe mode 0o777: {unsafe}",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_requires_corpus_directories_owned_by_effective_user(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    actual_user = os.geteuid()
    monkeypatch.setattr(os, "geteuid", lambda: actual_user + 1)

    with pytest.raises(
        ValueError,
        match=rf"must be owned by effective user {actual_user + 1}: {evidence_root}",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_a_symlink_version_directory(tmp_path: Path) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    version_root = evidence_root / "3.2.2"
    real_version_root = tmp_path / "real-3.2.2"
    version_root.rename(real_version_root)
    version_root.symlink_to(real_version_root, target_is_directory=True)

    with pytest.raises(
        ValueError,
        match=(
            r"receipt directory for DS 3\.2\.2 must not be a symlink: "
            rf"{version_root}"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_version_directory_replaced_by_symlink_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    version_root = evidence_root / "3.2.2"
    moved_version_root = tmp_path / "moved-3.2.2"
    real_open = os.open
    replaced = False

    def replace_after_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal replaced
        decoded = os.fsdecode(path)
        is_anchored_version_open = dir_fd is not None and decoded == "3.2.2"
        is_path_receipt_open = dir_fd is None and Path(decoded).parent == version_root
        if is_anchored_version_open and not replaced:
            descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
            replaced = True
            version_root.rename(moved_version_root)
            version_root.symlink_to(moved_version_root, target_is_directory=True)
            return descriptor
        if is_path_receipt_open and not replaced:
            replaced = True
            version_root.rename(moved_version_root)
            version_root.symlink_to(moved_version_root, target_is_directory=True)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", replace_after_open)

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 receipt directory changed during validation",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert replaced


@pytest.mark.parametrize("replacement_kind", ["different-inode", "move-out"])
def test_checker_rejects_version_directory_replaced_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    version_root = evidence_root / "3.2.2"
    moved_version_root = tmp_path / "moved-3.2.2"
    real_open = os.open
    replaced = False

    def replace_after_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal replaced
        if dir_fd is not None and os.fsdecode(path) == "3.2.2" and not replaced:
            descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
            replaced = True
            version_root.rename(moved_version_root)
            if replacement_kind == "different-inode":
                shutil.copytree(moved_version_root, version_root)
            return descriptor
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", replace_after_open)

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 receipt directory changed during validation",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert replaced


def test_checker_rejects_a_symlink_receipt(tmp_path: Path) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    real_receipt = tmp_path / "real-3.2.2-receipt.json"
    receipt.rename(real_receipt)
    receipt.symlink_to(real_receipt)

    with pytest.raises(
        ValueError,
        match=rf"DS 3\.2\.2 receipt must be a non-symlink regular file: {receipt}",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_a_receipt_replaced_by_symlink_after_lstat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    replacement = tmp_path / "same-valid-receipt.json"
    shutil.copy2(receipt, replacement)
    real_open = os.open
    replaced = False

    def replace_before_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal replaced
        if _open_targets_receipt(path, dir_fd=dir_fd, receipt=receipt) and not replaced:
            replaced = True
            receipt.unlink()
            receipt.symlink_to(replacement)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", replace_before_open)

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 receipt changed or could not be opened securely",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert replaced


def test_checker_rejects_a_receipt_replaced_by_another_inode_after_lstat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    replacement = tmp_path / "same-valid-receipt.json"
    shutil.copy2(receipt, replacement)
    original_inode = receipt.stat().st_ino
    replacement_inode = replacement.stat().st_ino
    assert replacement_inode != original_inode
    real_open = os.open
    replaced = False

    def replace_before_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal replaced
        if _open_targets_receipt(path, dir_fd=dir_fd, receipt=receipt) and not replaced:
            replaced = True
            # Keep the replacement inode alive before removing the original;
            # unlink followed by copy can reuse the original inode on Linux.
            replacement.replace(receipt)
            assert receipt.stat().st_ino == replacement_inode
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", replace_before_open)

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 receipt changed or could not be opened securely",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert replaced


def test_checker_fails_closed_without_a_no_follow_open_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    monkeypatch.delattr(os, "O_NOFOLLOW")

    with pytest.raises(
        ValueError,
        match=r"conformance-bundle evidence root .* cannot be securely opened",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_closes_every_opened_corpus_descriptor_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    receipt.write_text("{}\n", encoding="utf-8")
    real_open = os.open
    real_close = os.close
    opened: list[int] = []
    closed: list[int] = []

    def track_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        opened.append(descriptor)
        return descriptor

    def track_close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(os, "open", track_open)
    monkeypatch.setattr(os, "close", track_close)

    with pytest.raises(ValueError, match=r"invalid conformance-bundle receipt"):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert opened
    assert Counter(opened) == Counter(closed)


def test_checker_rejects_a_world_writable_executable_receipt(tmp_path: Path) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    receipt.chmod(0o777)

    with pytest.raises(
        ValueError,
        match=(
            rf"DS 3\.2\.2 receipt has unsafe mode 0o777: {receipt}; "
            r"execute and group/world-write bits are forbidden"
        ),
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


@pytest.mark.parametrize("mode", [0o700, 0o620, 0o602])
def test_checker_rejects_each_forbidden_receipt_permission_class(
    tmp_path: Path,
    mode: int,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "3.2.2")
    receipt.chmod(mode)

    with pytest.raises(ValueError, match=rf"receipt has unsafe mode {mode:#o}"):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


@pytest.mark.parametrize("mode", [0o600, 0o644])
def test_checker_accepts_non_executable_non_shared_writable_receipt_modes(
    tmp_path: Path,
    mode: int,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    _receipt_path(evidence_root, "3.2.2").chmod(mode)

    summary = corpus.check_conformance_bundle_evidence_corpus(evidence_root)

    assert len(summary.receipts) == len(TARGET_DS_VERSIONS)


def test_checker_rejects_a_duplicate_top_level_json_key(tmp_path: Path) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "1.3.9")
    source = receipt.read_text(encoding="utf-8")
    receipt.write_text(
        source.replace(
            '"schema_version": 1,',
            '"schema_version": 1,\n  "schema_version": 1,',
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"duplicate JSON object key 'schema_version'",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_a_duplicate_nested_json_key(tmp_path: Path) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "1.3.9")
    source = receipt.read_text(encoding="utf-8")
    receipt.write_text(
        source.replace(
            '"artifact": "installed-wheel-console-script",',
            (
                '"artifact": "installed-wheel-console-script",\n'
                '    "artifact": "installed-wheel-console-script",'
            ),
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"duplicate JSON object key 'artifact'",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def test_checker_rejects_a_nan_json_constant_at_the_parser_boundary(
    tmp_path: Path,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "1.3.9")
    _replace_remote_mutations_with_json_constant(receipt, constant="NaN")

    with pytest.raises(
        ValueError,
        match=r"non-standard JSON constant 'NaN' is forbidden",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


@pytest.mark.parametrize("constant", ["Infinity", "-Infinity"])
def test_checker_rejects_infinite_json_constants_at_the_parser_boundary(
    tmp_path: Path,
    constant: str,
) -> None:
    corpus = _load_corpus_module()
    evidence_root = tmp_path / "conformance-bundles"
    _write_real_corpus(evidence_root)
    receipt = _receipt_path(evidence_root, "1.3.9")
    _replace_remote_mutations_with_json_constant(receipt, constant=constant)

    with pytest.raises(
        ValueError,
        match=rf"non-standard JSON constant '{constant}' is forbidden",
    ):
        corpus.check_conformance_bundle_evidence_corpus(evidence_root)


def _load_corpus_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.conformance_bundle_corpus")


def _load_cli_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("check_conformance_bundle_evidence")


def _install_prepared_validator(
    corpus: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    *,
    source_root: Path,
    validate: Callable[..., object],
) -> list[Path]:
    prepared = corpus.ConformanceBundleEvidenceValidator.load(
        source_root=source_root,
    )
    loads: list[Path] = []

    def load(
        _validator_type: type[object],
        *,
        source_root: Path,
    ) -> object:
        loads.append(source_root)
        return SimpleNamespace(
            assessment=prepared.assessment,
            validate=validate,
        )

    monkeypatch.setattr(
        corpus.ConformanceBundleEvidenceValidator,
        "load",
        classmethod(load),
    )
    return loads


def _expected_bundle(version: str) -> str:
    return _current_highest_ready_bundles()[version]


def _expected_bundle_counts() -> dict[str, int]:
    return dict(Counter(_current_highest_ready_bundles().values()))


def _current_highest_ready_bundles() -> dict[str, str]:
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = generated.CONFORMANCE_BUNDLE_DATA
    assert isinstance(assessment, dict)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    ancestors = {
        bundle["name"]: frozenset(bundle["extends"])
        for bundle in bundles
        if isinstance(bundle, dict)
    }

    expected: dict[str, str] = {}
    for version in TARGET_DS_VERSIONS:
        ready = {
            bundle["name"]
            for bundle in bundles
            if isinstance(bundle, dict)
            and _bundle_coordinate_status(assessment, bundle["name"], version)
            == "ready"
        }
        maxima = sorted(
            candidate
            for candidate in ready
            if not any(
                candidate in ancestors[other] for other in ready if other != candidate
            )
        )
        assert len(maxima) == 1
        expected[version] = maxima[0]
    return expected


def _bundle_coordinate_status(
    assessment: dict[str, object],
    bundle_name: str,
    version: str,
) -> str:
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    bundle = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == bundle_name
    )
    coordinates = bundle["versions"]
    assert isinstance(coordinates, list)
    coordinate = next(
        row
        for row in coordinates
        if isinstance(row, dict) and row.get("version") == version
    )
    status = coordinate["status"]
    assert isinstance(status, str)
    return status


def _refresh_bundle_coordinates_from_profiles(
    bundle: dict[str, object],
    *,
    required_actions: list[str],
) -> None:
    generated = importlib.import_module("dsctl.generated.version_profiles")
    profiles = generated.PROFILE_DATA["profiles"]
    assert isinstance(profiles, dict)
    coordinates = bundle["versions"]
    assert isinstance(coordinates, list)
    ready_count = 0
    for coordinate in coordinates:
        assert isinstance(coordinate, dict)
        version = coordinate["version"]
        assert isinstance(version, str)
        profile = profiles[version]
        assert isinstance(profile, dict)
        actions = profile["actions"]
        assert isinstance(actions, dict)
        blockers = []
        for action in required_actions:
            capability = actions[action]
            assert isinstance(capability, dict)
            if capability["availability"] == "supported":
                continue
            blockers.append(
                {
                    field: capability[field]
                    for field in (
                        "availability",
                        "constraint",
                        "execution_mode",
                        "reason",
                        "verification",
                    )
                }
                | {"action": action}
            )
        coordinate["blockers"] = blockers
        coordinate["status"] = "ready" if not blockers else "blocked"
        ready_count += not blockers
    bundle["status"] = "ready" if ready_count == len(coordinates) else "blocked"
    bundle["ready_coordinate_count"] = ready_count
    bundle["blocked_coordinate_count"] = len(coordinates) - ready_count


def _write_stub_corpus(root: Path) -> None:
    for version in TARGET_DS_VERSIONS:
        path = root / version / f"{_RECEIPT_DATE}-{_WHEEL_SHA256[7:19]}.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "version": version,
                    "recorded_at": "2026-08-10T12:00:00+00:00",
                }
            )
            + "\n",
            encoding="utf-8",
        )


def _write_real_corpus(root: Path) -> None:
    for version in TARGET_DS_VERSIONS:
        receipt = _new_receipt(
            bundle_name=_expected_bundle(version),
            ds_version=version,
        )
        path = root / version / f"{_RECEIPT_DATE}-{_WHEEL_SHA256[7:19]}.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")


def _receipt_digest(root: Path, version: str) -> str:
    receipt = _read_receipt(_receipt_path(root, version))
    digest = receipt["receipt_digest"]
    assert isinstance(digest, str)
    return digest


def _new_receipt(*, bundle_name: str, ds_version: str) -> dict[str, object]:
    support = importlib.import_module("tests.tools.test_conformance_bundle_evidence")
    receipt_factory = support._receipt
    receipt = receipt_factory(bundle_name=bundle_name, ds_version=ds_version)
    assert isinstance(receipt, dict)
    return deepcopy(receipt)


def _receipt_path(root: Path, version: str) -> Path:
    return next((root / version).iterdir())


def _read_receipt(path: Path) -> dict[str, object]:
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(receipt, dict)
    return receipt


def _write_receipt(path: Path, receipt: dict[str, object]) -> None:
    path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")


def _replace_remote_mutations_with_json_constant(path: Path, *, constant: str) -> None:
    receipt = _read_receipt(path)
    effects = receipt["effects"]
    assert isinstance(effects, dict)
    remote_mutations = effects["remote_mutations"]
    assert isinstance(remote_mutations, int)
    source = path.read_text(encoding="utf-8")
    needle = f'"remote_mutations": {remote_mutations}'
    assert source.count(needle) == 1
    path.write_text(
        source.replace(needle, f'"remote_mutations": {constant}', 1),
        encoding="utf-8",
    )


def _refresh_receipt_digest(receipt: dict[str, object]) -> None:
    evidence = importlib.import_module("live_gate.conformance_bundle_evidence")
    receipt["receipt_digest"] = evidence.canonical_conformance_bundle_receipt_digest(
        receipt
    )


def _refresh_assessment_summary(assessment: dict[str, object]) -> None:
    bundles = assessment["bundles"]
    summary = assessment["summary"]
    versions = assessment["target_versions"]
    assert isinstance(bundles, list)
    assert isinstance(summary, dict)
    assert isinstance(versions, list)
    ready_count = 0
    for bundle in bundles:
        assert isinstance(bundle, dict)
        coordinates = bundle["versions"]
        assert isinstance(coordinates, list)
        ready_count += sum(
            isinstance(coordinate, dict) and coordinate.get("status") == "ready"
            for coordinate in coordinates
        )
    coordinate_count = len(bundles) * len(versions)
    summary.update(
        {
            "bundle_count": len(bundles),
            "coordinate_count": coordinate_count,
            "ready_coordinate_count": ready_count,
            "blocked_coordinate_count": coordinate_count - ready_count,
        }
    )


def _candidate_assessment(
    tmp_path: Path,
) -> tuple[Path, dict[str, object], ModuleType]:
    support = importlib.import_module("tests.tools.test_conformance_bundle_evidence")
    source_root = support._copy_current_source_truth(tmp_path)
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = deepcopy(generated.CONFORMANCE_BUNDLE_DATA)
    assert isinstance(assessment, dict)
    return source_root, assessment, support


def _open_targets_receipt(
    path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    *,
    dir_fd: int | None,
    receipt: Path,
) -> bool:
    decoded = os.fsdecode(path)
    if dir_fd is None:
        return decoded == str(receipt)
    if decoded != receipt.name:
        return False
    opened_parent = os.fstat(dir_fd)
    expected_parent = receipt.parent.stat()
    return (
        opened_parent.st_dev == expected_parent.st_dev
        and opened_parent.st_ino == expected_parent.st_ino
    )
