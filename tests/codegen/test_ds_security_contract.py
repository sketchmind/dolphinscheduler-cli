from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.ir import ContractSnapshot

REPO_ROOT = Path(__file__).resolve().parents[2]


def _security_contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.security_contract")


def _operation_ids(snapshot: ContractSnapshot) -> set[str]:
    return {operation.operation_id for operation in snapshot.operations}


def _model_keys(snapshot: ContractSnapshot) -> set[str]:
    return {model.import_path for model in snapshot.models}


def test_security_contract_covers_every_exact_target_without_inference() -> None:
    contract = _security_contract()

    assert tuple(contract.SECURITY_CONTRACTS) == contract.TARGET_SECURITY_VERSIONS
    for version in contract.TARGET_SECURITY_VERSIONS:
        assert contract.security_contract(version).version == version

    with pytest.raises(ValueError, match="no reviewed security-domain contract"):
        contract.security_contract("3.4.4")


@pytest.mark.source_contract
def test_every_declared_security_source_and_model_exists_in_its_snapshot(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _security_contract()

    for version in contract.TARGET_SECURITY_VERSIONS:
        snapshot = exact_contract_corpus.snapshot(version)
        operation_ids = _operation_ids(snapshot)
        model_keys = _model_keys(snapshot)
        sources = contract.semantic_operation_sources(version)
        roots = contract.semantic_operation_type_roots(version)

        assert roots.keys() == sources.keys()
        for action, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, action)
            assert set(roots[action]) <= model_keys, (version, action)


def test_security_recipes_preserve_reviewed_absence_and_route_boundaries() -> None:
    contract = _security_contract()

    legacy = contract.semantic_operation_sources("1.3.9")
    two_zero = contract.semantic_operation_sources("2.0.0")
    two_zero_nine = contract.semantic_operation_sources("2.0.9")
    three_zero = contract.semantic_operation_sources("3.0.0")
    latest = contract.semantic_operation_sources("3.4.2")

    assert "user.revoke.project" in legacy
    assert legacy["user.revoke.project"][-1] == "UsersController.grantProject"
    assert "user.revoke.project" not in two_zero
    assert two_zero_nine["user.revoke.project"][-1] == ("UsersController.revokeProject")
    assert "user.grant.namespace" not in two_zero_nine
    assert three_zero["user.grant.namespace"][-1] == ("UsersController.grantNamespace")
    assert (
        "DataSourceController.getAuthorizedDatasourceList"
        in latest["user.grant.datasource"]
    )


def test_token_recipes_use_main_routes_and_capture_legacy_required_token() -> None:
    contract = _security_contract()

    assert contract.security_contract("1.3.9").access_token.token_required is True
    assert contract.security_contract("2.0.0").access_token.token_required is True
    assert contract.security_contract("2.0.9").access_token.token_required is False

    for version in contract.TARGET_SECURITY_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        token_sources = {
            source
            for action, action_sources in sources.items()
            if action.startswith("access-token.")
            for source in action_sources
        }
        assert token_sources
        assert all(
            source.startswith(("AccessTokenController.", "UsersController."))
            for source in token_sources
        )
        assert all("AccessTokenV2Controller" not in source for source in token_sources)


def test_user_field_facets_are_exact_version_decisions() -> None:
    contract = _security_contract()

    one_three = contract.security_contract("1.3.9").user
    two_zero = contract.security_contract("2.0.0").user
    three_zero = contract.security_contract("3.0.0").user

    assert (one_three.has_state, one_three.has_time_zone) == (False, False)
    assert (two_zero.has_state, two_zero.has_time_zone) == (True, False)
    assert (three_zero.has_state, three_zero.has_time_zone) == (True, True)
    assert contract.action_support("1.3.9")["user.create"] == "limited"
    assert contract.action_support("2.0.0")["user.update"] == "limited"
    assert contract.action_support("3.0.0")["user.update"] == "supported"


def test_343_distinguishes_full_user_detail_from_identity_only_selection() -> None:
    contract = _security_contract()
    sources = contract.semantic_operation_sources("3.4.3")
    roots = contract.semantic_operation_type_roots("3.4.3")
    assert contract.security_contract("3.4.3").user.simple_user_list is True
    assert contract.security_contract("3.4.2").user.simple_user_list is False
    assert "UsersController.listAll" not in sources["user.get"]
    assert "UsersController.getUserInfo" in sources["user.get"]
    assert "UsersController.queryUserList" in sources["user.get"]
    for action in (
        "access-token.create",
        "access-token.update",
        "access-token.generate",
    ):
        assert "UsersController.getUserInfo" in sources[action]
        assert "UsersController.listAll" in sources[action]
        assert "UsersController.listUser" not in sources[action]
        assert "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO" in roots[action]
    for action in ("user.grant.datasource", "user.revoke.datasource"):
        assert (
            "org.apache.dolphinscheduler.api.vo.DataSourceSimpleInfoVO" in roots[action]
        )
        assert "org.apache.dolphinscheduler.dao.entity.DataSource" not in roots[action]
