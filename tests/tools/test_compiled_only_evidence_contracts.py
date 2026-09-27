"""Empty legacy slices still require independently verified executable roots."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tests.live import conformance_bundle_gate, exact_profile_gate
from tests.tools.test_conformance_bundle_evidence import _receipt
from tests.tools.test_exact_profile_live_gate import _test_gate_recipes

import exact_342_evidence
from live_gate.conformance_bundle_evidence import (
    ConformanceBundleEvidenceValidator,
)
from live_gate.conformance_evidence.bindings import _current_generated_contract
from live_gate.exact_profile_policy import exact_profile_gate_policy

if TYPE_CHECKING:
    from collections.abc import Mapping

_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def validator() -> ConformanceBundleEvidenceValidator:
    return ConformanceBundleEvidenceValidator.load(source_root=_ROOT)


@pytest.fixture
def contract(validator: ConformanceBundleEvidenceValidator) -> dict[str, object]:
    return dict(validator._current.contracts["3.4.2"])


def test_all_current_slices_are_empty_but_runtime_ownership_is_not(
    validator: ConformanceBundleEvidenceValidator,
) -> None:
    current = validator._current
    assert len(current.contracts) == 37
    assert len(frozenset().union(*current.runtime_operations.values())) == 157
    assert {
        version
        for version, operations in current.runtime_operations.items()
        if "user.identity" in operations
    } == {"3.4.3"}
    for version, contract in current.contracts.items():
        assert contract["selection"] == "runtime-slice"
        assert contract["semantic_operations"] == []
        assert contract["operation_count"] == 0
        assert {
            "project.get",
            "workflow.get",
            "resource.page",
            "resource.download",
        } <= current.runtime_operations[version]


@pytest.mark.parametrize("missing", [None, "project.page", "all"])
def test_conformance_receipt_always_requires_the_real_runtime_union(
    validator: ConformanceBundleEvidenceValidator,
    missing: str | None,
) -> None:
    runtime = dict(validator._current.runtime_operations)
    if missing == "all":
        runtime["3.2.2"] = frozenset()
    elif missing is not None:
        runtime["3.2.2"] -= {missing}
    candidate = replace(
        validator, _current=replace(validator._current, runtime_operations=runtime)
    )
    receipt = _receipt(ds_version="3.2.2")
    if missing is None:
        assert candidate.validate(receipt).ds_version == "3.2.2"
    else:
        with pytest.raises(ValueError, match="recipes are absent"):
            candidate.validate(receipt)


@pytest.mark.parametrize("count", [0, -1, True])
def test_exact_342_wheel_manifest_accepts_only_nonnegative_integer_counts(
    contract: dict[str, object], count: object
) -> None:
    assignments = {
        source: contract[target]
        for source, target in exact_profile_gate._MANIFEST_FIELDS.items()
    }
    assignments["SEMANTIC_OPERATIONS"] = ()
    assignments["OPERATION_COUNT"] = count
    if type(count) is int and count == 0:
        assert (
            exact_profile_gate._validated_exact_profile_manifest(
                assignments, policy=exact_profile_gate_policy("3.4.2")
            )
            == contract
        )
    else:
        with pytest.raises((TypeError, ValueError), match="operation count"):
            exact_profile_gate._validated_exact_profile_manifest(
                assignments, policy=exact_profile_gate_policy("3.4.2")
            )


@pytest.mark.parametrize("count", [0, -1, True])
def test_installed_conformance_contract_accepts_only_nonnegative_integer_counts(
    contract: dict[str, object], count: object
) -> None:
    profile = {
        "source": {
            "tag": contract["source_tag"],
            "commit": contract["source_commit"],
            "tree": contract["source_tree"],
        }
    }
    candidate = {**contract, "operation_count": count}
    if type(count) is int and count == 0:
        assert (
            conformance_bundle_gate._validated_installed_contract(
                candidate, ds_version="3.4.2", profile=profile
            )
            == contract
        )
    else:
        with pytest.raises(ValueError, match="operation_count"):
            conformance_bundle_gate._validated_installed_contract(
                candidate, ds_version="3.4.2", profile=profile
            )


@pytest.mark.parametrize("count", [0, -1, True])
def test_generated_conformance_contract_keeps_count_and_selection_guards(
    contract: dict[str, object], count: object
) -> None:
    contracts: Mapping[str, Mapping[str, object]] = {
        "3.4.2": {**contract, "operation_count": count}
    }
    if type(count) is int and count == 0:
        bound = _current_generated_contract("3.4.2", contracts=contracts)
        assert bound.semantic_operations == ()
        assert bound.operation_count == 0
        contracts = {"3.4.2": {**contract, "selection": "full"}}
        with pytest.raises(ValueError, match="operation_count must be positive"):
            _current_generated_contract("3.4.2", contracts=contracts)
    else:
        with pytest.raises((TypeError, ValueError), match="operation_count"):
            _current_generated_contract("3.4.2", contracts=contracts)


@pytest.mark.parametrize("missing", [None, "task.get", "all"])
def test_schema_7_empty_contract_requires_verified_compiled_semantics(
    contract: dict[str, object],
    validator: ConformanceBundleEvidenceValidator,
    missing: str | None,
) -> None:
    compiled = validator._current.runtime_operations["3.4.2"]
    if missing == "all":
        compiled = frozenset()
    elif missing is not None:
        compiled -= {missing}
    if missing is None:
        assert (
            exact_342_evidence._validate_contract(
                contract, schema_version=7, compiled_operations=compiled
            )
            == []
        )
    else:
        with pytest.raises(ValueError, match="exact task-definition operations"):
            exact_342_evidence._validate_contract(
                contract, schema_version=7, compiled_operations=compiled
            )


@pytest.mark.parametrize("count", [-1, True])
def test_schema_7_empty_contract_rejects_invalid_counts(
    contract: dict[str, object],
    validator: ConformanceBundleEvidenceValidator,
    count: object,
) -> None:
    contract["operation_count"] = count
    with pytest.raises((TypeError, ValueError), match="operation_count"):
        exact_342_evidence._validate_contract(
            contract,
            schema_version=7,
            compiled_operations=validator._current.runtime_operations["3.4.2"],
        )


@pytest.mark.parametrize("schema", [3, 4, 5, 6])
@pytest.mark.parametrize("empty_roots", [False, True])
def test_historical_342_contracts_keep_nonempty_roots_and_positive_counts(
    contract: dict[str, object], schema: int, *, empty_roots: bool
) -> None:
    if schema < 6:
        contract.pop("bundle_manifest_schema_version")
    contract["semantic_operations"] = [] if empty_roots else ["task.get"]
    contract["operation_count"] = 1 if empty_roots else 0
    with pytest.raises(
        (TypeError, ValueError),
        match="non-empty string list" if empty_roots else "must be positive",
    ):
        exact_342_evidence._validate_contract(
            contract, schema_version=schema, compiled_operations=frozenset()
        )


@pytest.mark.parametrize("missing", [None, "project.page", "all"])
def test_installed_342_gate_still_requires_every_runtime_recipe(
    contract: dict[str, object],
    validator: ConformanceBundleEvidenceValidator,
    missing: str | None,
) -> None:
    runtime = validator._current.runtime_operations["3.4.2"]
    if missing == "all":
        runtime = frozenset()
    elif missing is not None:
        runtime -= {missing}
    recipes = list(_test_gate_recipes())
    if missing is None:
        assert exact_profile_gate._load_gate_recipes(
            recipes,
            manifest=contract,
            runtime_operations=runtime,
            policy=exact_profile_gate_policy("3.4.2"),
        ) == tuple(recipes)
    else:
        with pytest.raises(AssertionError, match="absent from the exact runtime"):
            exact_profile_gate._load_gate_recipes(
                recipes,
                manifest=contract,
                runtime_operations=runtime,
                policy=exact_profile_gate_policy("3.4.2"),
            )
