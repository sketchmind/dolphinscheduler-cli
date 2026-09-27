"""Promotion evidence checks selected by an explicitly reviewed exact policy."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from live_gate.evidence_values import required_mapping as _required_mapping
from live_gate.evidence_values import required_text as _required_text
from live_gate.exact_profile_policy import (
    EXACT_PROFILE_GATE_POLICIES,
    ExactProfileGatePolicy,
    exact_profile_gate_policy,
)
from live_gate.exact_profile_read_corpus import (
    ROOT,
    TrackedArtifacts,
    load_tracked_artifacts,
    validate_receipt_filename,
)
from live_gate.runtime_ownership import load_current_runtime_ownership

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class ExactProfilePromotionEvidenceSummary:
    """Current source claim and its optional promotion receipt."""

    state: str
    promoted_actions: tuple[str, ...]
    receipt: Path | None
    cli_version: str
    wheel_filename: str | None
    wheel_sha256: str | None


def check_exact_profile_promotion_evidence(
    evidence_dir: Path,
    *,
    ds_version: str,
    source_root: Path = ROOT,
    allow_missing_current_receipt: bool = False,
    expected_wheel_filename: str | None = None,
    expected_wheel_sha256: str | None = None,
) -> ExactProfilePromotionEvidenceSummary:
    """Check a reviewed exact profile claim against its current receipt policy."""
    policy = exact_profile_gate_policy(ds_version)
    artifacts = load_tracked_artifacts(source_root)
    profile = _required_mapping(
        artifacts.profiles.get(policy.ds_version),
        label=f"tracked profile {policy.ds_version}",
    )
    actions = _required_mapping(
        profile.get("actions"),
        label=f"tracked profile {policy.ds_version} actions",
    )
    promoted = tuple(
        action
        for action in policy.actions
        if _required_mapping(
            actions.get(action),
            label=f"tracked profile {policy.ds_version}:{action}",
        ).get("verification")
        == "live_smoke"
    )
    if promoted == policy.unclaimed_read_actions:
        if allow_missing_current_receipt:
            message = (
                "allow_missing_current_receipt requires complete 15-action metadata"
            )
            raise ValueError(message)
        current_receipts = _current_receipts(evidence_dir, policy=policy)
        if current_receipts:
            message = (
                f"current schema-{policy.current_schema_version} receipt exists "
                "but source metadata has no full "
                f"exact {policy.ds_version} promotion claim"
            )
            raise ValueError(message)
        return ExactProfilePromotionEvidenceSummary(
            state="unclaimed",
            promoted_actions=promoted,
            receipt=None,
            cli_version=artifacts.cli_version,
            wheel_filename=None,
            wheel_sha256=None,
        )
    if promoted == policy.actions:
        current_receipts = _current_receipts(evidence_dir, policy=policy)
        if current_receipts and allow_missing_current_receipt:
            message = (
                "allow_missing_current_receipt is PRE-CAMPAIGN ONLY and cannot "
                "be used when a current schema-"
                f"{policy.current_schema_version} receipt exists"
            )
            raise ValueError(message)
        if not current_receipts and not allow_missing_current_receipt:
            message = (
                f"full exact {policy.ds_version} promotion claim requires "
                "one current schema-"
                f"{policy.current_schema_version} receipt"
            )
            raise ValueError(message)
        if not current_receipts:
            return ExactProfilePromotionEvidenceSummary(
                state="pre_campaign_only",
                promoted_actions=promoted,
                receipt=None,
                cli_version=artifacts.cli_version,
                wheel_filename=None,
                wheel_sha256=None,
            )
        if len(current_receipts) != 1:
            names = ", ".join(path.name for path, _receipt in current_receipts)
            message = (
                f"exact {policy.ds_version} promotion requires "
                "exactly one current schema-"
                f"{policy.current_schema_version} "
                f"receipt; found {len(current_receipts)} ({names})"
            )
            raise ValueError(message)
        receipt_path, receipt = current_receipts[0]
        runtime_operations = load_current_runtime_ownership(
            source_root, contracts=artifacts.contracts
        )
        legacy_operations = frozenset(
            cast(
                "list[str]",
                artifacts.contracts[policy.ds_version]["semantic_operations"],
            )
        )
        runner = _validate_current_receipt(
            receipt,
            policy=policy,
            path=receipt_path,
            artifacts=artifacts,
            profile=profile,
            compiled_operations=runtime_operations[policy.ds_version]
            - legacy_operations,
            expected_wheel_filename=expected_wheel_filename,
            expected_wheel_sha256=expected_wheel_sha256,
        )
        return ExactProfilePromotionEvidenceSummary(
            state="promoted",
            promoted_actions=promoted,
            receipt=receipt_path,
            cli_version=artifacts.cli_version,
            wheel_filename=runner[0],
            wheel_sha256=runner[1],
        )
    message = (
        f"partial exact {policy.ds_version} promotion claim; live_smoke actions: "
        + ", ".join(promoted)
    )
    raise ValueError(message)


def _current_receipts(
    evidence_dir: Path,
    *,
    policy: ExactProfileGatePolicy,
) -> list[tuple[Path, dict[str, object]]]:
    if not evidence_dir.exists():
        return []
    if not evidence_dir.is_dir():
        message = (
            f"exact {policy.ds_version} evidence path is not a directory: "
            f"{evidence_dir}"
        )
        raise NotADirectoryError(message)
    current: list[tuple[Path, dict[str, object]]] = []
    for path in sorted(evidence_dir.glob("*.json"), key=lambda item: item.name):
        if not path.is_file():
            message = (
                f"exact {policy.ds_version} evidence entry is not a regular file: "
                f"{path}"
            )
            raise ValueError(message)
        receipt = _load_receipt(path)
        schema_version = receipt.get("schema_version")
        if schema_version == policy.current_schema_version:
            current.append((path, receipt))
        elif schema_version not in (
            policy.supported_schema_versions - {policy.current_schema_version}
        ):
            message = f"{path}: unsupported exact {policy.ds_version} receipt schema"
            raise ValueError(message)
    return current


def _load_receipt(path: Path) -> dict[str, object]:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        message = f"{path}: receipt is not valid JSON: {error.msg}"
        raise ValueError(message) from error
    return _required_mapping(payload, label=f"{path}: receipt")


def _require_current_mapping(
    actual: Mapping[str, object],
    expected: Mapping[str, object],
    *,
    label: str,
) -> None:
    if actual == expected:
        return
    differing = sorted(
        key
        for key in set(actual) | set(expected)
        if actual.get(key) != expected.get(key)
    )
    message = (
        f"{label} differs from the current tracked generated artifact at: "
        + ", ".join(differing)
        + "; rerun the installed-wheel gate with the current canonical wheel"
    )
    raise ValueError(message)


def _validate_current_receipt(
    receipt: Mapping[str, object],
    *,
    policy: ExactProfileGatePolicy,
    path: Path,
    artifacts: TrackedArtifacts,
    profile: Mapping[str, object],
    compiled_operations: frozenset[str],
    expected_wheel_filename: str | None,
    expected_wheel_sha256: str | None,
) -> tuple[str, str]:
    cli_version = _required_text(
        artifacts.cli_version,
        label="tracked package version",
    )
    try:
        policy.validate_receipt(
            receipt,
            expected_cli_version=cli_version,
            compiled_semantic_operations=compiled_operations,
        )
    except (TypeError, ValueError) as error:
        message = (
            f"{path}: invalid current schema-{policy.current_schema_version} "
            f"exact {policy.ds_version} receipt: {error}"
        )
        raise ValueError(message) from error

    contract = _required_mapping(receipt.get("contract"), label=f"{path}: contract")
    contracts = _required_mapping(
        artifacts.contracts,
        label="tracked contracts",
    )
    tracked_contract = _required_mapping(
        contracts.get(policy.ds_version),
        label=f"tracked contract {policy.ds_version}",
    )
    _require_current_mapping(contract, tracked_contract, label=f"{path}: contract")
    _validate_profile_binding(receipt, profile=profile, path=path)
    _validate_recipe_binding(receipt, policy=policy, profile=profile, path=path)
    _validate_source_closure(profile, policy=policy, contract=tracked_contract)

    runner = _required_mapping(receipt.get("runner"), label=f"{path}: runner")
    wheel_filename = _required_text(
        runner.get("wheel_filename"),
        label=f"{path}: runner wheel_filename",
    )
    wheel_sha256 = _required_text(
        runner.get("wheel_sha256"),
        label=f"{path}: runner wheel_sha256",
    )
    validate_receipt_filename(path, wheel_sha256=wheel_sha256)
    if (
        expected_wheel_filename is not None
        and wheel_filename != expected_wheel_filename
    ):
        message = (
            f"{path}: runner wheel filename differs from the expected candidate; "
            f"expected {expected_wheel_filename!r}, got {wheel_filename!r}"
        )
        raise ValueError(message)
    if expected_wheel_sha256 is not None and wheel_sha256 != expected_wheel_sha256:
        message = (
            f"{path}: runner wheel SHA-256 differs from the expected candidate; "
            f"expected {expected_wheel_sha256!r}, got {wheel_sha256!r}"
        )
        raise ValueError(message)
    return wheel_filename, wheel_sha256


def _validate_profile_binding(
    receipt: Mapping[str, object],
    *,
    profile: Mapping[str, object],
    path: Path,
) -> None:
    actual = _required_mapping(receipt.get("profile"), label=f"{path}: profile")
    expected = {
        "ds": profile.get("server_version"),
        "selected_ds_version": profile.get("server_version"),
        "contract_version": profile.get("contract_version"),
        "family": profile.get("family"),
        "support_level": profile.get("support_level"),
        "tested": profile.get("tested"),
        "fingerprints": profile.get("fingerprints"),
    }
    _require_current_mapping(
        {key: actual.get(key) for key in expected},
        expected,
        label=f"{path}: profile",
    )


def _validate_recipe_binding(
    receipt: Mapping[str, object],
    *,
    policy: ExactProfileGatePolicy,
    profile: Mapping[str, object],
    path: Path,
) -> None:
    gate_bundle = _required_mapping(
        receipt.get("gate_bundle"),
        label=f"{path}: gate_bundle",
    )
    actions = _required_mapping(
        profile.get("actions"),
        label=f"tracked profile {policy.ds_version} actions",
    )
    decisions = _required_mapping(
        profile.get("build_decisions"),
        label=f"tracked profile {policy.ds_version} decisions",
    )
    expected_verifications: dict[str, object] = {}
    expected_recipes: list[dict[str, object]] = []
    for action, operation in policy.recipes:
        capability = _required_mapping(
            actions.get(action),
            label=f"tracked profile {policy.ds_version}:{action}",
        )
        expected_verifications[action] = capability.get("verification")
        decision = _required_mapping(
            decisions.get(operation),
            label=f"tracked profile {policy.ds_version} decision {operation}",
        )
        stable_action = _required_text(
            decision.get("stable_action"),
            label=(
                f"tracked profile {policy.ds_version} "
                f"decision {operation} stable action"
            ),
        )
        if stable_action != action:
            message = (
                f"tracked profile {policy.ds_version} "
                f"decision {operation} stable action "
                f"must equal {action!r}; got {stable_action!r}"
            )
            raise ValueError(message)
        expected_recipes.append(
            {
                "action": action,
                "semantic_operation": decision.get("semantic_operation"),
                "build_status": decision.get("build_status"),
                "fingerprints": decision.get("fingerprints"),
            }
        )
    actual = {
        "actions": gate_bundle.get("actions"),
        "action_verifications": gate_bundle.get("action_verifications"),
        "recipes": gate_bundle.get("recipes"),
    }
    expected = {
        "actions": list(policy.actions),
        "action_verifications": expected_verifications,
        "recipes": expected_recipes,
    }
    _require_current_mapping(actual, expected, label=f"{path}: gate_bundle")


def _validate_source_closure(
    profile: Mapping[str, object],
    *,
    policy: ExactProfileGatePolicy,
    contract: Mapping[str, object],
) -> None:
    profile_source = _required_mapping(
        profile.get("source"),
        label=f"tracked profile {policy.ds_version} source",
    )
    expected_source = {
        "tag": contract.get("source_tag"),
        "commit": contract.get("source_commit"),
        "tree": contract.get("source_tree"),
    }
    _require_current_mapping(
        profile_source,
        expected_source,
        label=f"tracked {policy.ds_version} profile/manifest source closure",
    )


def run_exact_profile_promotion_evidence_cli(argv: list[str] | None = None) -> int:
    """Run promotion checks for one explicitly selected, reviewed exact policy."""
    parser = argparse.ArgumentParser(
        description="Validate exact-profile promotion evidence."
    )
    parser.add_argument(
        "--version", required=True, choices=tuple(EXACT_PROFILE_GATE_POLICIES)
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        help="directory containing this exact profile's live receipts",
    )
    parser.add_argument(
        "--allow-missing-current-receipt",
        action="store_true",
        help="allow a full metadata claim only before running its live campaign",
    )
    parser.add_argument("--expected-wheel-filename")
    parser.add_argument("--expected-wheel-sha256")
    parser.add_argument(
        "--source-root", type=Path, default=ROOT, help=argparse.SUPPRESS
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    policy = exact_profile_gate_policy(args.version)
    source_root = args.source_root.expanduser().resolve()
    evidence_dir = args.evidence_dir or policy.evidence_directory(source_root)
    try:
        summary = check_exact_profile_promotion_evidence(
            evidence_dir.expanduser().resolve(),
            ds_version=policy.ds_version,
            source_root=source_root,
            allow_missing_current_receipt=args.allow_missing_current_receipt,
            expected_wheel_filename=args.expected_wheel_filename,
            expected_wheel_sha256=args.expected_wheel_sha256,
        )
    except (OSError, TypeError, ValueError) as error:
        print(
            f"exact {policy.ds_version} promotion evidence check failed: {error}",
            file=sys.stderr,
        )
        return 1
    prefix = f"exact {policy.ds_version} promotion evidence check passed: "
    if summary.state == "pre_campaign_only":
        print(
            prefix
            + "PRE-CAMPAIGN ONLY; "
            + f"current schema-{policy.current_schema_version} "
            "receipt is still required"
        )
    elif summary.state == "unclaimed":
        print(prefix + "no current promotion claim")
    else:
        print(prefix + f"wheel={summary.wheel_filename}; digest={summary.wheel_sha256}")
    return 0
