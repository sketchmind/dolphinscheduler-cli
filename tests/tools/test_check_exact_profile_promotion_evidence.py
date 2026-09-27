from __future__ import annotations

import hashlib
import importlib
import json
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tests.tools.generated_profile_support import set_generated_operation_action

if TYPE_CHECKING:
    from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]
_PROMOTION_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "workflow.describe",
    "workflow.digest",
    "workflow.export",
    "workflow.get",
    "workflow.list",
    "task.list",
    "task.get",
    "task.update",
)
_BASELINE_READ_ACTIONS = (
    "project.get",
    "project.list",
    "workflow.get",
    "workflow.list",
)


def _load_module() -> ModuleType:
    tools_dir = ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.exact_profile_promotion_evidence")


def test_checker_accepts_the_unclaimed_four_read_baseline(tmp_path: Path) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _BASELINE_READ_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    summary = checker.check_exact_profile_promotion_evidence(
        evidence_dir, source_root=source_root, ds_version="3.4.2"
    )

    assert summary.state == "unclaimed"
    assert summary.promoted_actions == (
        "project.get",
        "project.list",
        "workflow.get",
        "workflow.list",
    )
    assert summary.receipt is None


def test_checker_rejects_pre_campaign_allowance_before_full_metadata(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _BASELINE_READ_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    with pytest.raises(ValueError, match=r"complete 15-action metadata"):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir,
            source_root=source_root,
            allow_missing_current_receipt=True,
            ds_version="3.4.2",
        )


def test_checker_rejects_a_partial_promotion_claim(tmp_path: Path) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, (*_BASELINE_READ_ACTIONS, "doctor"))
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    with pytest.raises(
        ValueError,
        match=r"partial exact 3\.4\.2 promotion claim.+doctor",
    ):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )


def test_checker_requires_a_current_receipt_for_the_full_claim(tmp_path: Path) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    with pytest.raises(
        ValueError,
        match=(
            r"full exact 3\.4\.2 promotion claim requires one current "
            r"schema-7 receipt"
        ),
    ):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )


def test_checker_allows_an_explicit_pre_campaign_full_claim(tmp_path: Path) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    summary = checker.check_exact_profile_promotion_evidence(
        evidence_dir,
        source_root=source_root,
        allow_missing_current_receipt=True,
        ds_version="3.4.2",
    )

    assert summary.state == "pre_campaign_only"
    assert summary.promoted_actions == _PROMOTION_ACTIONS
    assert summary.receipt is None


def test_checker_accepts_one_current_schema_seven_receipt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = source_root / "docs/development/live-evidence/external-shell/3.4.2"
    evidence_dir.mkdir(parents=True)
    receipt_path = _write_schema_seven_receipt(evidence_dir, source_root=source_root)

    summary = checker.check_exact_profile_promotion_evidence(
        evidence_dir, source_root=source_root, ds_version="3.4.2"
    )

    assert summary.state == "promoted"
    assert summary.receipt == receipt_path
    assert summary.wheel_filename == "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    assert summary.wheel_sha256 == "sha256:" + "a" * 64

    result = checker.run_exact_profile_promotion_evidence_cli(
        ["--version", "3.4.2", "--source-root", str(source_root)]
    )
    assert result == 0
    assert "promotion evidence check passed" in capsys.readouterr().out


def test_checker_rejects_pre_campaign_allowance_when_current_receipt_exists(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    _write_schema_seven_receipt(evidence_dir, source_root=source_root)

    with pytest.raises(ValueError, match=r"PRE-CAMPAIGN ONLY"):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir,
            source_root=source_root,
            allow_missing_current_receipt=True,
            ds_version="3.4.2",
        )


def test_cli_labels_the_only_missing_receipt_allowance_as_pre_campaign(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    result = checker.run_exact_profile_promotion_evidence_cli(
        [
            "--version",
            "3.4.2",
            "--evidence-dir",
            str(evidence_dir),
            "--source-root",
            str(source_root),
            "--allow-missing-current-receipt",
        ]
    )

    assert result == 0
    assert "PRE-CAMPAIGN ONLY" in capsys.readouterr().out


@pytest.mark.parametrize("schema_version", [3, 4, 5, 6])
def test_checker_keeps_legacy_receipts_audit_only(
    tmp_path: Path,
    schema_version: int,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    payload = json.loads(
        (
            ROOT
            / "docs"
            / "development"
            / "live-evidence"
            / "external-shell"
            / "3.4.2"
            / "2026-08-04-13b81eedd389.json"
        ).read_text(encoding="utf-8")
    )
    payload["schema_version"] = schema_version
    (evidence_dir / f"schema-{schema_version}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"requires one current schema-7 receipt"):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )
    summary = checker.check_exact_profile_promotion_evidence(
        evidence_dir,
        source_root=source_root,
        allow_missing_current_receipt=True,
        ds_version="3.4.2",
    )
    assert summary.state == "pre_campaign_only"


def test_checker_requires_one_unique_current_receipt(tmp_path: Path) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    receipt = _write_schema_seven_receipt(evidence_dir, source_root=source_root)
    shutil.copy2(receipt, evidence_dir / "2026-08-11-aaaaaaaaaaaa.json")

    with pytest.raises(
        ValueError,
        match=r"exactly one current schema-7 receipt; found 2",
    ):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )


def test_checker_binds_receipt_filename_to_the_full_wheel_digest(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    receipt = _write_schema_seven_receipt(evidence_dir, source_root=source_root)
    receipt.rename(evidence_dir / "2026-08-10-bbbbbbbbbbbb.json")

    with pytest.raises(
        ValueError, match=r"filename digest suffix must be 'aaaaaaaaaaaa'"
    ):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("profile", r"profile differs.+fingerprints"),
        ("verification", r"gate action verification doctor must equal 'live_smoke'"),
        ("recipe", r"gate_bundle differs.+recipes"),
        ("manifest", r"contract differs.+rendered_contract_digest"),
    ],
)
def test_checker_binds_all_current_generated_claims(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    receipt_path = _write_schema_seven_receipt(evidence_dir, source_root=source_root)
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    if case == "profile":
        payload["profile"]["fingerprints"]["source"] = "sha256:" + "b" * 64
    elif case == "verification":
        payload["gate_bundle"]["action_verifications"]["doctor"] = "live_full"
        _refresh_gate_bundle_digest(payload)
    elif case == "recipe":
        payload["gate_bundle"]["recipes"][0]["fingerprints"]["source"] = (
            "sha256:" + "b" * 64
        )
        _refresh_gate_bundle_digest(payload)
    else:
        payload["contract"]["rendered_contract_digest"] = "sha256:" + "b" * 64
    receipt_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )


def test_checker_binds_recipe_action_to_generated_stable_action(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    _write_schema_seven_receipt(evidence_dir, source_root=source_root)
    set_generated_operation_action(
        source_root,
        operation="identity.current",
        action="capabilities",
    )

    with pytest.raises(ValueError, match=r"identity\.current.+stable action"):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, ds_version="3.4.2"
        )


@pytest.mark.parametrize(
    ("expected", "message"),
    [
        (
            {"expected_wheel_filename": "other.whl"},
            "wheel filename differs from the expected candidate",
        ),
        (
            {"expected_wheel_sha256": "sha256:" + "b" * 64},
            "wheel SHA-256 differs from the expected candidate",
        ),
    ],
)
def test_checker_binds_an_explicit_candidate_wheel_identity(
    tmp_path: Path,
    expected: dict[str, str],
    message: str,
) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    _set_live_actions(source_root, _PROMOTION_ACTIONS)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    _write_schema_seven_receipt(evidence_dir, source_root=source_root)

    with pytest.raises(ValueError, match=message):
        checker.check_exact_profile_promotion_evidence(
            evidence_dir, source_root=source_root, **expected, ds_version="3.4.2"
        )


def test_checker_never_executes_candidate_generated_source(tmp_path: Path) -> None:
    checker = _load_module()
    source_root = _copy_source_root(tmp_path)
    sentinel = tmp_path / "executed"
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    profile_path.write_text(
        "_PROFILE_JSON = __import__('pathlib').Path("
        + repr(str(sentinel))
        + ").write_text('executed')\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"_PROFILE_JSON must be a literal assignment"):
        checker.check_exact_profile_promotion_evidence(
            tmp_path / "evidence", source_root=source_root, ds_version="3.4.2"
        )
    assert not sentinel.exists()


def _copy_source_root(tmp_path: Path) -> Path:
    source_root = tmp_path / "source"
    generated_root = ROOT / "src" / "dsctl" / "generated"
    generated_paths = (
        generated_root / "version_profiles.py",
        *sorted((generated_root / "versions").glob("ds_*/_manifest.py")),
        *sorted((generated_root / "wire_programs").rglob("*.py")),
        *sorted((generated_root / "wire_runtime").rglob("*.py")),
    )
    for source_path in generated_paths:
        target_path = source_root / source_path.relative_to(ROOT)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
    shutil.copy2(ROOT / "pyproject.toml", source_root / "pyproject.toml")
    return source_root


def _set_live_actions(source_root: Path, actions: tuple[str, ...]) -> None:
    path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    source = path.read_text(encoding="utf-8")
    prefix = "_PROFILE_JSON = r'''\n"
    before, payload_and_suffix = source.split(prefix, maxsplit=1)
    payload_source, suffix = payload_and_suffix.split(
        "\n'''\n_SHARED_DATA",
        maxsplit=1,
    )
    payload = json.loads(payload_source)
    payload["capability_records"]["test_contract_policy"] = {
        "availability": "supported",
        "execution_mode": "generated_adapter",
        "verification": "contract_tested",
    }
    payload["capability_records"]["test_live_policy"] = {
        "availability": "supported",
        "execution_mode": "generated_adapter",
        "verification": "live_smoke",
    }
    bindings = payload["profile_bindings"]["3.4.2"]["actions"]
    for action in _PROMOTION_ACTIONS:
        bindings[action] = "test_contract_policy"
    for action in actions:
        bindings[action] = "test_live_policy"
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(
        before + prefix + rendered + "\n'''\n_SHARED_DATA" + suffix,
        encoding="utf-8",
    )


def _write_schema_seven_receipt(
    evidence_dir: Path,
    *,
    source_root: Path,
) -> Path:
    corpus = importlib.import_module("live_gate.exact_profile_read_corpus")
    artifacts = corpus.load_tracked_artifacts(source_root)
    profile = artifacts.profiles["3.4.2"]
    assert isinstance(profile, dict)
    actions = profile["actions"]
    decisions = profile["build_decisions"]
    assert isinstance(actions, dict)
    assert isinstance(decisions, dict)
    recipes = []
    action_verifications = {}
    for action, operation in (
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
    ):
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
        "actions": list(_PROMOTION_ACTIONS),
        "action_verifications": action_verifications,
        "recipes": recipes,
    }
    payload = json.loads(
        (
            ROOT
            / "docs"
            / "development"
            / "live-evidence"
            / "external-shell"
            / "3.4.2"
            / "2026-08-04-13b81eedd389.json"
        ).read_text(encoding="utf-8")
    )
    payload["schema_version"] = 7
    payload["fixture"]["provisioner"] = "dsmatrix-exact-read-state-projection/v2"
    payload["runner"] = {
        "artifact": "installed-wheel-console-script",
        "cli_version": artifacts.cli_version,
        "wheel_filename": "dolphinscheduler_cli-0.4.0-py3-none-any.whl",
        "wheel_sha256": "sha256:" + "a" * 64,
    }
    payload["profile"] = {
        "ds": profile["server_version"],
        "selected_ds_version": profile["server_version"],
        "contract_version": profile["contract_version"],
        "family": profile["family"],
        "support_level": profile["support_level"],
        "tested": profile["tested"],
        "fingerprints": profile["fingerprints"],
    }
    payload["contract"] = artifacts.contracts["3.4.2"]
    payload["gate_bundle"] = {
        **gate_bundle,
        "digest": _canonical_digest(gate_bundle),
    }
    receipt_path = evidence_dir / "2026-08-10-aaaaaaaaaaaa.json"
    receipt_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt_path


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
