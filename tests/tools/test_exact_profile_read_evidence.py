from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from tests.live.exact_read_gate import (
    EXACT_PROFILE_READ_ACTIONS,
    EXACT_PROFILE_READ_RECIPES,
    canonical_read_bundle_digest,
    validate_exact_profile_read_evidence_payload,
)
from tests.tools.exact_profile_read_support import (
    evidence_payload,
    semantic_evidence_payload,
)

# These schema-one receipts predate the 21 intermediate profile admissions.
_HISTORICAL_RECEIPT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)


def test_tracked_schema_one_corpora_remain_auditable() -> None:
    project_root = Path(__file__).resolve().parents[2]
    evidence_root = project_root / "docs/development/live-evidence/history/exact-read"
    assert {path.name for path in evidence_root.iterdir() if path.is_dir()} == set(
        _HISTORICAL_RECEIPT_VERSIONS
    )
    validated_versions = []
    for version in _HISTORICAL_RECEIPT_VERSIONS:
        receipts = tuple((evidence_root / version).glob("*.json"))
        assert len(receipts) == 1
        payload = json.loads(receipts[0].read_text(encoding="utf-8"))
        assert payload["schema_version"] == 1
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version=version,
        )
        validated_versions.append(version)

    assert tuple(validated_versions) == _HISTORICAL_RECEIPT_VERSIONS


def test_schema_one_accepts_legacy_receipt_without_action_verification_mapping() -> (
    None
):
    payload = evidence_payload()

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="3.2.2",
    )


def test_schema_one_accepts_digest_bound_action_verifications() -> None:
    payload = evidence_payload()
    _bind_action_verifications(payload)

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="3.2.2",
    )


def test_schema_one_keeps_historical_341_full_receipt_auditable() -> None:
    payload = evidence_payload(ds_version="3.4.1")
    profile = payload["profile"]
    contract = payload["contract"]
    assert isinstance(profile, dict)
    assert isinstance(contract, dict)
    profile["full_adapter"] = True
    contract["selection"] = "full"
    contract["semantic_operations"] = []
    contract["operation_count"] = 298

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="3.4.1",
    )


def test_schema_two_accepts_semantic_profile_and_runtime_slice_for_341() -> None:
    payload = semantic_evidence_payload(ds_version="3.4.1")

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="3.4.1",
    )


@pytest.mark.parametrize("missing", [None, *EXACT_PROFILE_READ_ACTIONS])
def test_zero_native_slice_requires_every_verified_current_read_owner(
    missing: str | None,
) -> None:
    payload = semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract.update(semantic_operations=[], operation_count=0)
    owners = frozenset(
        operation
        for action, operation in EXACT_PROFILE_READ_RECIPES
        if action != missing
    )

    if missing is None:
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
            compiled_semantic_operations=owners,
        )
    else:
        with pytest.raises(ValueError, match="not present in the runtime-slice"):
            validate_exact_profile_read_evidence_payload(
                payload,
                expected_cli_version="0.4.0",
                expected_ds_version="3.2.2",
                compiled_semantic_operations=owners,
            )


@pytest.mark.parametrize("selection", ["runtime-slice", "full"])
def test_empty_current_roots_do_not_skip_owner_validation_without_context(
    selection: str,
) -> None:
    payload = semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract.update(
        selection=selection,
        semantic_operations=[],
        operation_count=0 if selection == "runtime-slice" else 298,
    )

    with pytest.raises(ValueError, match="not present in the runtime-slice"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


@pytest.mark.parametrize("schema_version", [1, 2])
@pytest.mark.parametrize("count", [-1, True])
def test_read_receipt_rejects_invalid_native_count(
    schema_version: int, count: object
) -> None:
    payload = evidence_payload() if schema_version == 1 else semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["operation_count"] = count

    with pytest.raises((TypeError, ValueError), match="operation_count"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


@pytest.mark.parametrize("count", [0, 4])
def test_historical_slice_cannot_borrow_current_compiled_owners(count: int) -> None:
    payload = evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract.update(semantic_operations=[], operation_count=count)

    with pytest.raises(ValueError, match=r"operation_count|must include semantic"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
            compiled_semantic_operations=frozenset(
                operation for _action, operation in EXACT_PROFILE_READ_RECIPES
            ),
        )


@pytest.mark.parametrize("schema_version", [1, 2])
def test_trusted_compiled_owner_context_only_augments_current_receipts(
    schema_version: int,
) -> None:
    payload = evidence_payload() if schema_version == 1 else semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    operations = contract["semantic_operations"]
    assert isinstance(operations, list)
    operations.remove("project.get")

    with pytest.raises(ValueError, match="not present in the runtime-slice manifest"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )

    # The pure validator receives context, not authority to invent it. Candidate
    # loaders separately prove that a root really moved out of the legacy slice.
    if schema_version == 2:
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
            compiled_semantic_operations=frozenset({"project.get"}),
        )
    else:
        with pytest.raises(
            ValueError, match="not present in the runtime-slice manifest"
        ):
            validate_exact_profile_read_evidence_payload(
                payload,
                expected_cli_version="0.4.0",
                expected_ds_version="3.2.2",
                compiled_semantic_operations=frozenset({"project.get"}),
            )


def test_receipts_accept_historical_and_current_bundle_manifest_schemas() -> None:
    historical = evidence_payload()
    current = semantic_evidence_payload()

    validate_exact_profile_read_evidence_payload(
        historical,
        expected_cli_version="0.4.0",
        expected_ds_version="3.2.2",
    )
    validate_exact_profile_read_evidence_payload(
        current,
        expected_cli_version="0.4.0",
        expected_ds_version="3.2.2",
    )


def test_receipt_rejects_unknown_bundle_manifest_schema() -> None:
    payload = semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["bundle_manifest_schema_version"] = 3

    with pytest.raises(ValueError, match="bundle manifest schema must be 1 or 2"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_schema_two_requires_digest_bound_action_verifications() -> None:
    payload = semantic_evidence_payload()
    read_bundle = payload["read_bundle"]
    assert isinstance(read_bundle, dict)
    read_bundle.pop("action_verifications")

    with pytest.raises(ValueError, match="action_verifications"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_schema_two_rejects_legacy_implementation_identity_fields() -> None:
    payload = semantic_evidence_payload()
    profile = payload["profile"]
    assert isinstance(profile, dict)
    profile["read_adapter"] = "GeneratedReadAdapter"

    with pytest.raises(ValueError, match="profile keys differ"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_schema_two_rejects_unknown_contract_selection() -> None:
    payload = semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["selection"] = "shared-family"

    with pytest.raises(ValueError, match="selection must be full or runtime-slice"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_schema_two_rejects_nonempty_full_contract_roots() -> None:
    payload = semantic_evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["selection"] = "full"

    with pytest.raises(ValueError, match="full contract manifest must use an empty"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_rejects_unknown_schema() -> None:
    payload = semantic_evidence_payload()
    payload["schema_version"] = 3

    with pytest.raises(ValueError, match="schema_version must be 1 or 2"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_schema_one_rejects_incomplete_action_verification_mapping() -> None:
    payload = evidence_payload()
    _bind_action_verifications(payload)
    bundle = payload["read_bundle"]
    assert isinstance(bundle, dict)
    verifications = bundle["action_verifications"]
    assert isinstance(verifications, dict)
    verifications.pop("workflow.get")
    _refresh_bundle_digest(bundle)

    with pytest.raises(ValueError, match="read action verifications keys differ"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_schema_one_rejects_unknown_action_verification_level() -> None:
    payload = evidence_payload()
    _bind_action_verifications(payload)
    bundle = payload["read_bundle"]
    assert isinstance(bundle, dict)
    verifications = bundle["action_verifications"]
    assert isinstance(verifications, dict)
    verifications["project.get"] = "release_claim"
    _refresh_bundle_digest(bundle)

    with pytest.raises(ValueError, match="recognized verification level"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_rejects_claimed_remote_mutation() -> None:
    payload = deepcopy(evidence_payload())
    effects = payload["effects"]
    assert isinstance(effects, dict)
    effects["remote_mutations"] = 1

    with pytest.raises(
        ValueError,
        match="read-only evidence must declare zero remote mutations",
    ):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


@pytest.mark.parametrize(
    "provisioner",
    [
        "Bearer super-secret-token",
        "api_token=super-secret-token",
        "DS_API_TOKEN=super-secret-token",
        "AWS_SECRET_ACCESS_KEY=super-secret-token",
        "db_password=super-secret-token",
        "-----BEGIN PRIVATE KEY-----",
    ],
)
def test_exact_profile_read_evidence_rejects_sensitive_provenance_text(
    provisioner: str,
) -> None:
    payload = deepcopy(evidence_payload())
    fixture = payload["fixture"]
    assert isinstance(fixture, dict)
    fixture["provisioner"] = provisioner

    with pytest.raises(ValueError, match="evidence contains sensitive text"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_rejects_wrong_identity_epoch() -> None:
    payload = deepcopy(evidence_payload())
    fixture = payload["fixture"]
    assert isinstance(fixture, dict)
    fixture["project_identity_kind"] = "id"

    with pytest.raises(
        ValueError,
        match=r"DS 3\.2\.2 read fixtures must use code",
    ):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_rejects_unknown_release_state() -> None:
    payload = deepcopy(evidence_payload())
    fixture = payload["fixture"]
    assert isinstance(fixture, dict)
    fixture["workflow_release_state"] = "BROKEN"

    with pytest.raises(ValueError, match="release state must be ONLINE or OFFLINE"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_rejects_swapped_recipe_mapping() -> None:
    payload = deepcopy(evidence_payload())
    bundle = payload["read_bundle"]
    assert isinstance(bundle, dict)
    recipes = bundle["recipes"]
    assert isinstance(recipes, list)
    first = recipes[0]
    second = recipes[1]
    assert isinstance(first, dict)
    assert isinstance(second, dict)
    first["semantic_operation"], second["semantic_operation"] = (
        second["semantic_operation"],
        first["semantic_operation"],
    )
    digest_payload = {"actions": bundle["actions"], "recipes": recipes}
    bundle["digest"] = canonical_read_bundle_digest(digest_payload)

    with pytest.raises(ValueError, match="exact semantic operation"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_rejects_missing_schedule_assertion() -> None:
    payload = deepcopy(evidence_payload())
    trace = payload["operation_trace"]
    assert isinstance(trace, list)
    workflow_get = trace[10]
    assert isinstance(workflow_get, dict)
    assertions = workflow_get["assertions"]
    assert isinstance(assertions, list)
    assertions.remove("schedule-hydrated")

    with pytest.raises(ValueError, match="trace assertions"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_accepts_legacy_project_adapter() -> None:
    payload = evidence_payload(ds_version="1.3.9")

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="1.3.9",
    )


def test_exact_profile_read_evidence_accepts_distinct_root_and_closure_counts() -> None:
    payload = evidence_payload()
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["operation_count"] = 3

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="3.2.2",
    )


def test_exact_profile_read_evidence_rejects_empty_runtime_slice_roots() -> None:
    payload = deepcopy(evidence_payload())
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["semantic_operations"] = []

    with pytest.raises(
        ValueError,
        match="runtime-slice contract manifest must include semantic operation roots",
    ):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="3.2.2",
        )


def test_exact_profile_read_evidence_accepts_matrix_local_image_id() -> None:
    payload = evidence_payload(ds_version="2.0.0")
    cluster = payload["dolphinscheduler"]
    assert isinstance(cluster, dict)
    cluster["image_ref"] = "dsmatrix-local/dolphinscheduler:2.0.0"

    validate_exact_profile_read_evidence_payload(
        payload,
        expected_cli_version="0.4.0",
        expected_ds_version="2.0.0",
    )


def test_exact_profile_read_evidence_rejects_short_identity_in_trace() -> None:
    payload = evidence_payload(ds_version="1.3.9")
    trace = payload["operation_trace"]
    assert isinstance(trace, list)
    project_get = trace[7]
    assert isinstance(project_get, dict)
    project_get["assertions"] = ["native-project-identity-matched", "fixture-id-2"]

    with pytest.raises(ValueError, match="trace assertions"):
        validate_exact_profile_read_evidence_payload(
            payload,
            expected_cli_version="0.4.0",
            expected_ds_version="1.3.9",
        )


def _bind_action_verifications(payload: dict[str, object]) -> None:
    bundle = payload["read_bundle"]
    assert isinstance(bundle, dict)
    bundle["action_verifications"] = dict.fromkeys(
        EXACT_PROFILE_READ_ACTIONS,
        "live_smoke",
    )
    _refresh_bundle_digest(bundle)


def _refresh_bundle_digest(bundle: dict[str, object]) -> None:
    bundle["digest"] = canonical_read_bundle_digest(
        {
            key: bundle[key]
            for key in ("actions", "action_verifications", "recipes")
            if key in bundle
        }
    )
