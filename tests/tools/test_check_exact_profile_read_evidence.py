from __future__ import annotations

import importlib
import json
import re
import shutil
import sys
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tests.live.exact_read_gate import (
    EXACT_PROFILE_READ_ACTIONS,
    EXACT_PROFILE_READ_RECIPES,
    canonical_read_bundle_digest,
)
from tests.tools.exact_profile_read_support import semantic_evidence_payload
from tests.tools.generated_profile_support import set_generated_operation_action

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS, VERSION_PROFILES

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType


_CLI_VERSION = "0.4.0"
_WHEEL_FILENAME = "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
_WHEEL_DIGEST = "sha256:" + "a" * 64
_RECEIPT_DATE = "2026-08-06"


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("check_exact_profile_read_evidence")


def test_static_profile_reader_matches_every_exact_runtime_profile() -> None:
    _load_module()
    reader = importlib.import_module("live_gate.exact_profile_read_corpus")
    artifacts = reader.load_tracked_artifacts(Path(__file__).resolve().parents[2])

    expected = deepcopy(VERSION_PROFILES)
    for profile in expected.values():
        profile["build_decisions"] = {
            operation: {
                field: decision[field]
                for field in (
                    "semantic_operation",
                    "stable_action",
                    "build_status",
                    "fingerprints",
                )
            }
            for operation, decision in profile["build_decisions"].items()
        }
    assert artifacts.profiles == expected


@pytest.mark.parametrize(
    "corruption", ["unknown_record", "wrong_operation", "fingerprint"]
)
def test_static_profile_reader_rejects_invalid_named_build_bindings(
    tmp_path: Path,
    corruption: str,
) -> None:
    checker = _load_module()
    source_root = tmp_path / "source"
    _copy_promotion_sources(source_root)
    path = source_root / "src/dsctl/generated/version_profiles.py"
    source = path.read_text(encoding="utf-8")
    payload = json.loads(
        source.split("_PROFILE_JSON = r'''\n", 1)[1].split("\n'''", 1)[0]
    )
    bindings = payload["profile_bindings"]["3.4.2"]["build_decisions"]
    record = payload["build_records"][bindings["project.get"]]
    if corruption == "unknown_record":
        bindings["project.get"] = "missing-record"
        message = "references unknown record"
    elif corruption == "wrong_operation":
        record["semantic_operation"] = "project.delete"
        message = "different semantic operation"
    else:
        fingerprints = payload["build_fields"]["fingerprints"][record["fingerprints"]]
        fingerprints["source"] = "sha256:invalid"
        message = "must be a SHA-256 fingerprint"
    path.write_text(f"_PROFILE_JSON = {json.dumps(payload)!r}\n", encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        checker.check_exact_profile_read_evidence_corpus(
            tmp_path / "not-needed", source_root=source_root
        )


class _CountedRecordPool(dict[str, object]):
    iterations = 0

    def __iter__(self) -> Iterator[str]:
        self.iterations += 1
        return super().__iter__()


def test_static_profile_reader_scans_each_shared_pool_once() -> None:
    _load_module()
    reader = importlib.import_module("live_gate.exact_profile_read_corpus")
    path = (
        Path(__file__).resolve().parents[2] / "src/dsctl/generated/version_profiles.py"
    )
    payload = reader._load_profile_data(path)
    pools = {
        name: _CountedRecordPool(records)
        for name, records in payload["build_fields"].items()
    }
    payload["build_fields"] = pools

    reader._materialize_profiles(payload, versions=TARGET_DS_VERSIONS, path=path)

    assert len(pools) == 5
    assert [pool.iterations for pool in pools.values()] == [1] * 5


@pytest.mark.parametrize(
    "field",
    [
        "source_operations",
        "type_closure",
        "selector_semantics",
        "evidence_sources",
        "fingerprints",
    ],
)
@pytest.mark.parametrize("corruption", ["pool", "missing_reference", "reference_type"])
def test_static_profile_reader_retains_validation_of_every_shared_field(
    field: str,
    corruption: str,
) -> None:
    _load_module()
    reader = importlib.import_module("live_gate.exact_profile_read_corpus")
    path = (
        Path(__file__).resolve().parents[2] / "src/dsctl/generated/version_profiles.py"
    )
    payload = reader._load_profile_data(path)
    binding = payload["profile_bindings"]["3.4.2"]["build_decisions"]["project.get"]
    record = payload["build_records"][binding]
    error: type[TypeError] | type[ValueError]
    if corruption == "pool":
        payload["build_fields"][field] = []
        error = TypeError
        message = "must be an object"
    elif corruption == "missing_reference":
        record[field] = "missing-record"
        error = ValueError
        message = "references unknown record"
    else:
        record[field] = 42
        error = TypeError
        message = "must be non-empty text"

    with pytest.raises(error, match=message):
        reader._materialize_profiles(payload, versions=TARGET_DS_VERSIONS, path=path)


def test_static_profile_reader_isolates_outputs_and_rereads_changed_source(
    tmp_path: Path,
) -> None:
    _load_module()
    reader = importlib.import_module("live_gate.exact_profile_read_corpus")
    source_root = tmp_path / "source"
    _copy_promotion_sources(source_root)
    first = reader.load_tracked_artifacts(source_root)
    original = deepcopy(first.profiles)
    fingerprints = [
        decision["fingerprints"]
        for profile in first.profiles.values()
        for decision in profile["build_decisions"].values()
    ]
    actions = [
        action
        for profile in first.profiles.values()
        for action in profile["actions"].values()
    ]
    assert len({id(value) for value in fingerprints}) == len(fingerprints)
    assert len({id(value) for value in actions}) == len(actions)
    fingerprints[0]["source"] = "mutated output"
    actions[0]["verification"] = "mutated output"
    assert reader.load_tracked_artifacts(source_root).profiles == original

    path = source_root / "src/dsctl/generated/version_profiles.py"
    payload = reader._load_profile_data(path)
    binding = payload["profile_bindings"]["3.4.2"]["build_decisions"]["project.get"]
    record = payload["build_records"][binding]
    payload["build_fields"]["fingerprints"][record["fingerprints"]]["source"] = (
        "invalid"
    )
    path.write_text(f"_PROFILE_JSON = {json.dumps(payload)!r}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a SHA-256 fingerprint"):
        reader.load_tracked_artifacts(source_root)


@pytest.fixture
def promotion_source(tmp_path: Path) -> Path:
    """A temporary source tree with all four reads marked as live_smoke."""
    source_root = tmp_path / "promotion-source"
    _copy_promotion_sources(source_root)
    return source_root


def test_checker_rejects_unverified_profile_despite_synthetic_receipts(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    profile_path = promotion_source / "src/dsctl/generated/version_profiles.py"
    source = profile_path.read_text(encoding="utf-8")
    payload = json.loads(
        source.split("_PROFILE_JSON = r'''\n", 1)[1].split("\n'''", 1)[0]
    )
    bindings = payload["profile_bindings"]["2.0.1"]["actions"]
    capability = deepcopy(payload["capability_records"][bindings["project.get"]])
    capability["verification"] = "contract_tested"
    payload["capability_records"]["synthetic-unverified-project-get"] = capability
    bindings["project.get"] = "synthetic-unverified-project-get"
    profile_path.write_text(
        f"_PROFILE_JSON = {json.dumps(payload)!r}\n", encoding="utf-8"
    )
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)

    with pytest.raises(ValueError, match=r"2\.0\.1:project\.get must be live_smoke"):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root, source_root=promotion_source
        )


def test_checker_accepts_one_current_same_wheel_receipt_per_exact_version(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)

    summary = checker.check_exact_profile_read_evidence_corpus(
        evidence_root,
        expected_wheel_filename=_WHEEL_FILENAME,
        expected_wheel_sha256=_WHEEL_DIGEST,
        source_root=promotion_source,
    )

    assert summary.versions == TARGET_DS_VERSIONS
    assert summary.cli_version == _CLI_VERSION
    assert summary.wheel_filename == _WHEEL_FILENAME
    assert summary.wheel_sha256 == _WHEEL_DIGEST
    assert len(summary.receipts) == len(TARGET_DS_VERSIONS)


@pytest.mark.parametrize(
    ("binding", "expected", "message"),
    [
        (
            "expected_wheel_filename",
            "dolphinscheduler_cli-0.4.0-rebuilt.whl",
            "wheel filename differs from expected value",
        ),
        (
            "expected_wheel_sha256",
            "sha256:" + "a" * 12 + "b" * 52,
            "wheel SHA-256 differs from expected value",
        ),
    ],
)
def test_checker_rejects_complete_corpus_from_another_release_wheel(
    tmp_path: Path,
    promotion_source: Path,
    binding: str,
    expected: str,
    message: str,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)

    with pytest.raises(ValueError, match=message):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
            **{binding: expected},
        )


def test_current_compiled_owners_do_not_replace_verbatim_legacy_manifest(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(path)
    contract = receipt["contract"]
    assert isinstance(contract, dict)
    operations = contract["semantic_operations"]
    assert isinstance(operations, list)
    assert "workflow.get" not in operations
    operations.append("workflow.get")
    _write_receipt(path, receipt)

    with pytest.raises(
        ValueError,
        match=(
            "contract differs from the current tracked generated artifact at: "
            "semantic_operations"
        ),
    ):
        _load_module().check_exact_profile_read_evidence_corpus(
            evidence_root, source_root=promotion_source
        )


def test_checker_rejects_schema_one_receipt_as_historical_only(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    receipt["schema_version"] = 1
    _write_receipt(receipt_path, receipt)

    with pytest.raises(ValueError, match="schema 2 is required for promotion"):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_treats_historical_manifest_schema_as_stale_current_binding(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    contract = receipt["contract"]
    assert isinstance(contract, dict)
    contract["bundle_manifest_schema_version"] = 1
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=(
            "differs from the current tracked generated artifact at: "
            "bundle_manifest_schema_version"
        ),
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_rejects_legacy_r4_receipts_without_action_verification_binding(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    for version in TARGET_DS_VERSIONS:
        receipt_path = _receipt_path(evidence_root, version)
        receipt = _read_receipt(receipt_path)
        read_bundle = receipt["read_bundle"]
        assert isinstance(read_bundle, dict)
        read_bundle.pop("action_verifications")
        _refresh_read_bundle_digest(read_bundle)
        _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=r"read_bundle\.action_verifications is required for promotion evidence",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


@pytest.mark.parametrize("verification", ["contract_tested", "live_full"])
def test_checker_rejects_tampered_or_contract_tested_promotion_mapping(
    tmp_path: Path,
    promotion_source: Path,
    verification: str,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    read_bundle = receipt["read_bundle"]
    assert isinstance(read_bundle, dict)
    verifications = read_bundle["action_verifications"]
    assert isinstance(verifications, dict)
    verifications["project.get"] = verification
    _refresh_read_bundle_digest(read_bundle)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=r"read action verifications differs.+project\.get",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_reports_missing_and_unexpected_version_directories(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    missing = evidence_root / "2.0.9"
    missing.rename(tmp_path / "removed-2.0.9")
    (evidence_root / "9.9.9").mkdir()

    with pytest.raises(
        ValueError,
        match=(
            r"missing version directories: 2\.0\.9; "
            r"unexpected root entries: 9\.9\.9"
        ),
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_requires_exactly_one_receipt_in_each_version_directory(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    duplicate = evidence_root / "3.2.2" / "2026-08-07-aaaaaaaaaaaa.json"
    duplicate.write_text("{}\n", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 requires exactly one current receipt; found 2",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


@pytest.mark.parametrize(
    ("keys", "label"),
    [
        (("dolphinscheduler", "release"), "dolphinscheduler.release"),
        (("profile", "ds"), "profile.ds"),
        (("profile", "selected_ds_version"), "profile.selected_ds_version"),
        (("profile", "contract_version"), "profile.contract_version"),
        (("contract", "ds_version"), "contract.ds_version"),
        (("contract", "source_tag"), "contract.source_tag"),
    ],
)
def test_checker_binds_all_six_receipt_version_fields_to_the_directory(
    tmp_path: Path,
    promotion_source: Path,
    keys: tuple[str, str],
    label: str,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    section = receipt[keys[0]]
    assert isinstance(section, dict)
    section[keys[1]] = "9.9.9"
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=rf"{re.escape(label)} must match directory version '3\.2\.2'",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("read_scope", "read bundle must bind the fixed four read actions"),
        ("remote_mutation", "read-only evidence must declare zero remote mutations"),
        ("fixture_mutation", "read-only evidence must declare fixture_mutated=false"),
        ("secrets", "passing read evidence must declare secrets_recorded=false"),
    ],
)
def test_checker_delegates_fixed_scope_and_safety_to_current_validator(
    tmp_path: Path,
    promotion_source: Path,
    case: str,
    message: str,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    if case == "read_scope":
        read_bundle = receipt["read_bundle"]
        assert isinstance(read_bundle, dict)
        actions = read_bundle["actions"]
        assert isinstance(actions, list)
        actions.pop()
    elif case == "remote_mutation":
        effects = receipt["effects"]
        assert isinstance(effects, dict)
        effects["remote_mutations"] = 1
    elif case == "fixture_mutation":
        effects = receipt["effects"]
        assert isinstance(effects, dict)
        effects["fixture_mutated"] = True
    else:
        receipt["secrets_recorded"] = True
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=rf"3\.2\.2/.+invalid exact-profile read receipt: {message}",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_binds_receipt_filename_to_full_runner_wheel_digest(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt_path.rename(receipt_path.with_name("2026-08-06-bbbbbbbbbbbb.json"))

    with pytest.raises(
        ValueError,
        match="filename digest suffix must be 'aaaaaaaaaaaa'",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_rejects_a_different_wheel_filename_inside_the_corpus(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.4.2")
    receipt = _read_receipt(receipt_path)
    runner = receipt["runner"]
    assert isinstance(runner, dict)
    runner["wheel_filename"] = "rebuilt-dolphinscheduler-cli.whl"
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=r"3\.4\.2/.+runner identity differs from the corpus wheel",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_compares_the_full_wheel_digest_not_only_filename_suffix(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.4.2")
    receipt = _read_receipt(receipt_path)
    runner = receipt["runner"]
    assert isinstance(runner, dict)
    runner["wheel_sha256"] = "sha256:" + "a" * 12 + "b" * 52
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=r"3\.4\.2/.+runner identity differs from the corpus wheel",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_binds_runner_cli_version_to_tracked_package_version(
    tmp_path: Path,
    promotion_source: Path,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    runner = receipt["runner"]
    assert isinstance(runner, dict)
    runner["cli_version"] = "9.9.9"
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=r"runner cli_version must be '0\.4\.0'",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


@pytest.mark.parametrize(
    ("stale_binding", "message"),
    [
        ("contract", r"contract differs.+rendered_contract_digest"),
        ("profile", r"profile differs.+fingerprints"),
    ],
)
def test_checker_rejects_stale_tracked_contract_and_fingerprints(
    tmp_path: Path,
    promotion_source: Path,
    stale_binding: str,
    message: str,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    if stale_binding == "contract":
        contract = receipt["contract"]
        assert isinstance(contract, dict)
        contract["rendered_contract_digest"] = "sha256:" + "b" * 64
    elif stale_binding == "profile":
        profile = receipt["profile"]
        assert isinstance(profile, dict)
        fingerprints = profile["fingerprints"]
        assert isinstance(fingerprints, dict)
        fingerprints["source"] = "sha256:" + "b" * 64
    _write_receipt(receipt_path, receipt)

    with pytest.raises(ValueError, match=message):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


@pytest.mark.parametrize(
    ("recipe_index", "action"),
    list(enumerate(action for action, _operation in EXACT_PROFILE_READ_RECIPES)),
)
def test_checker_binds_each_of_the_four_recipe_fingerprints_to_current_profile(
    tmp_path: Path,
    promotion_source: Path,
    recipe_index: int,
    action: str,
) -> None:
    checker = _load_module()
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)
    receipt_path = _receipt_path(evidence_root, "3.2.2")
    receipt = _read_receipt(receipt_path)
    read_bundle = receipt["read_bundle"]
    assert isinstance(read_bundle, dict)
    recipes = read_bundle["recipes"]
    assert isinstance(recipes, list)
    recipe = recipes[recipe_index]
    assert isinstance(recipe, dict)
    fingerprints = recipe["fingerprints"]
    assert isinstance(fingerprints, dict)
    fingerprints["source"] = "sha256:" + "b" * 64
    _refresh_read_bundle_digest(read_bundle)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        ValueError,
        match=rf"read recipe {re.escape(action)} differs.+fingerprints",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=promotion_source,
        )


def test_checker_rejects_generated_decision_bound_to_another_action(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = tmp_path / "source"
    _copy_promotion_sources(source_root)
    set_generated_operation_action(
        source_root,
        operation="project.page",
        action="workflow.list",
    )
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)

    with pytest.raises(
        ValueError,
        match=r"tracked profile 1\.3\.9:project\.page selects action "
        r"'workflow\.list', expected 'project\.list'",
    ):
        checker.check_exact_profile_read_evidence_corpus(
            evidence_root,
            source_root=source_root,
        )


def test_cli_accepts_an_explicit_synthetic_corpus_root(
    tmp_path: Path,
    promotion_source: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    source_root = promotion_source
    evidence_root = tmp_path / "exact-read"
    _write_current_corpus(evidence_root)

    returncode = checker.main(
        [
            "--evidence-root",
            str(evidence_root),
            "--source-root",
            str(source_root),
            "--expected-wheel-filename",
            _WHEEL_FILENAME,
            "--expected-wheel-sha256",
            _WHEEL_DIGEST,
        ]
    )

    assert returncode == 0
    assert (
        f"exact-profile read evidence check passed: {len(TARGET_DS_VERSIONS)} versions"
    ) in (capsys.readouterr().out)


def test_cli_default_tracked_root_is_fail_closed_until_corpus_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    source_root = Path(__file__).resolve().parents[2]
    missing = tmp_path / "not-yet-tracked-exact-read"
    monkeypatch.setattr(checker, "DEFAULT_EVIDENCE_ROOT", missing)

    returncode = checker.main(["--source-root", str(source_root)])

    assert returncode == 1
    error = capsys.readouterr().err
    assert f"exact-profile read evidence root does not exist: {missing}" in error
    assert "track one current receipt for every exact version" in error


def test_source_root_profile_loader_does_not_execute_nonliteral_assignments(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = tmp_path / "source"
    _copy_promotion_sources(source_root)
    sentinel = tmp_path / "profile-code-executed"
    profile_path = source_root / "src/dsctl/generated/version_profiles.py"
    source = profile_path.read_text(encoding="utf-8")
    start = source.index("_PROFILE_JSON = r'''\n")
    end = source.index("\n'''", start) + len("\n'''")
    malicious = (
        "_PROFILE_JSON = __import__('pathlib').Path("
        f"{str(sentinel)!r}).write_text('executed')"
    )
    profile_path.write_text(source[:start] + malicious + source[end:], encoding="utf-8")

    with pytest.raises((TypeError, ValueError)):
        checker.check_exact_profile_read_evidence_corpus(
            tmp_path / "not-needed",
            source_root=source_root,
        )

    assert not sentinel.exists()


def test_source_root_manifest_loader_does_not_execute_nonliteral_assignments(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = tmp_path / "source"
    _copy_promotion_sources(source_root)
    sentinel = tmp_path / "manifest-code-executed"
    manifest_path = source_root / "src/dsctl/generated/versions/ds_1_3_9/_manifest.py"
    source = manifest_path.read_text(encoding="utf-8")
    malicious = (
        "DS_VERSION = __import__('pathlib').Path("
        f"{str(sentinel)!r}).write_text('executed')"
    )
    source = re.sub(
        r"^DS_VERSION = .+$", malicious, source, count=1, flags=re.MULTILINE
    )
    manifest_path.write_text(source, encoding="utf-8")

    with pytest.raises(ValueError, match="DS_VERSION must be a literal assignment"):
        checker.check_exact_profile_read_evidence_corpus(
            tmp_path / "not-needed",
            source_root=source_root,
        )

    assert not sentinel.exists()


def test_source_root_rejects_historical_bundle_manifest_schema(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = tmp_path / "source"
    _copy_promotion_sources(source_root)
    manifest_path = source_root / "src/dsctl/generated/versions/ds_1_3_9/_manifest.py"
    source = manifest_path.read_text(encoding="utf-8")
    assignment = "BUNDLE_MANIFEST_SCHEMA_VERSION = 2"
    assert source.count(assignment) == 1
    manifest_path.write_text(
        source.replace(assignment, "BUNDLE_MANIFEST_SCHEMA_VERSION = 1"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="generated bundle manifest schema"):
        checker.check_exact_profile_read_evidence_corpus(
            tmp_path / "not-needed",
            source_root=source_root,
        )


def _write_current_corpus(root: Path) -> None:
    for version in TARGET_DS_VERSIONS:
        receipt = _current_receipt(version)
        destination = root / version / f"{_RECEIPT_DATE}-{_WHEEL_DIGEST[7:19]}.json"
        destination.parent.mkdir(parents=True)
        destination.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def _copy_promotion_sources(destination: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    profile_source = project_root / "src/dsctl/generated/version_profiles.py"
    profile_destination = destination / "src/dsctl/generated/version_profiles.py"
    profile_destination.parent.mkdir(parents=True)
    shutil.copy2(project_root / "pyproject.toml", destination / "pyproject.toml")
    source = profile_source.read_text(encoding="utf-8")
    prefix, payload_source = source.split("_PROFILE_JSON = r'''\n", 1)
    payload_source, suffix = payload_source.split("\n'''", 1)
    payload = json.loads(payload_source)
    for version in TARGET_DS_VERSIONS:
        bindings = payload["profile_bindings"][version]["actions"]
        for action in EXACT_PROFILE_READ_ACTIONS:
            key = f"synthetic-live:{version}:{action}"
            capability = deepcopy(payload["capability_records"][bindings[action]])
            capability["verification"] = "live_smoke"
            payload["capability_records"][key] = capability
            bindings[action] = key
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    profile_destination.write_text(
        prefix + "_PROFILE_JSON = r'''\n" + rendered + "\n'''" + suffix,
        encoding="utf-8",
    )
    for package in ("wire_programs", "wire_runtime"):
        shutil.copytree(
            project_root / "src/dsctl/generated" / package,
            destination / "src/dsctl/generated" / package,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    for version in TARGET_DS_VERSIONS:
        slug = version.replace(".", "_")
        relative = Path(f"src/dsctl/generated/versions/ds_{slug}/_manifest.py")
        target = destination / relative
        target.parent.mkdir(parents=True)
        shutil.copy2(project_root / relative, target)


def _receipt_path(root: Path, version: str) -> Path:
    return next((root / version).iterdir())


def _read_receipt(path: Path) -> dict[str, object]:
    value: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_receipt(path: Path, receipt: dict[str, object]) -> None:
    path.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _refresh_read_bundle_digest(read_bundle: dict[str, object]) -> None:
    digest_payload = {
        key: read_bundle[key]
        for key in ("actions", "action_verifications", "recipes")
        if key in read_bundle
    }
    read_bundle["digest"] = canonical_read_bundle_digest(digest_payload)


def _current_receipt(version: str) -> dict[str, object]:
    receipt = semantic_evidence_payload(version)
    profile = deepcopy(VERSION_PROFILES[version])
    receipt_profile = receipt["profile"]
    assert isinstance(receipt_profile, dict)
    receipt_profile.update(
        {
            "family": profile["family"],
            "support_level": profile["support_level"],
            "tested": profile["tested"],
            "fingerprints": deepcopy(profile["fingerprints"]),
        }
    )

    manifest = importlib.import_module(
        f"dsctl.generated.versions.ds_{version.replace('.', '_')}._manifest"
    )
    receipt["contract"] = {
        "bundle_manifest_schema_version": manifest.BUNDLE_MANIFEST_SCHEMA_VERSION,
        "ds_version": manifest.DS_VERSION,
        "selection": manifest.SELECTION,
        "semantic_operations": list(manifest.SEMANTIC_OPERATIONS),
        "source_tag": manifest.SOURCE_TAG,
        "source_commit": manifest.SOURCE_COMMIT,
        "source_tree": manifest.SOURCE_TREE,
        "source_contract_digest": manifest.SOURCE_CONTRACT_DIGEST,
        "rendered_contract_digest": manifest.RENDERED_CONTRACT_DIGEST,
        "operation_count": manifest.OPERATION_COUNT,
    }

    recipes = []
    build_decisions = profile["build_decisions"]
    assert isinstance(build_decisions, dict)
    for action, operation in EXACT_PROFILE_READ_RECIPES:
        decision = build_decisions[operation]
        assert isinstance(decision, dict)
        recipes.append(
            {
                "action": action,
                "semantic_operation": decision["semantic_operation"],
                "build_status": decision["build_status"],
                "fingerprints": deepcopy(decision["fingerprints"]),
            }
        )
    read_bundle = receipt["read_bundle"]
    assert isinstance(read_bundle, dict)
    read_bundle["recipes"] = recipes
    read_bundle["action_verifications"] = dict.fromkeys(
        EXACT_PROFILE_READ_ACTIONS, "live_smoke"
    )
    _refresh_read_bundle_digest(read_bundle)
    return receipt
