from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.ir import ContractSnapshot
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract


def _load(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def _compile(bundles: tuple[Any, ...], *definitions: Any) -> Any:
    return _load("ds_codegen.compiled_domains").compile_domains(
        bundles,
        tuple(definitions),
    )


@pytest.fixture(scope="module")
def compiled_domain_set(exact_runtime_bundles: tuple[Any, ...]) -> Any:
    alert_groups = _load("ds_codegen.compiled_alert_groups")
    clusters = _load("ds_codegen.compiled_clusters")
    environments = _load("ds_codegen.compiled_environments")
    worker_groups = _load("ds_codegen.compiled_worker_groups")
    return _compile(
        exact_runtime_bundles,
        clusters.CLUSTER_COMPILED_DOMAIN,
        environments.ENVIRONMENT_COMPILED_DOMAIN,
        worker_groups.WORKER_GROUP_COMPILED_DOMAIN,
        alert_groups.ALERT_GROUP_COMPILED_DOMAIN,
    )


def test_compiled_domains_own_every_coordinate_then_strip_the_union_once(
    exact_runtime_bundles: tuple[Any, ...],
    compiled_domain_set: Any,
) -> None:
    alert_groups = _load("ds_codegen.compiled_alert_groups")
    clusters = _load("ds_codegen.compiled_clusters")
    environments = _load("ds_codegen.compiled_environments")
    worker_groups = _load("ds_codegen.compiled_worker_groups")
    source_matrix = _load("ds_codegen.compatibility_impact")
    definitions = (
        clusters.CLUSTER_COMPILED_DOMAIN,
        environments.ENVIRONMENT_COMPILED_DOMAIN,
        worker_groups.WORKER_GROUP_COMPILED_DOMAIN,
        alert_groups.ALERT_GROUP_COMPILED_DOMAIN,
    )
    expected_requests = {
        "cluster": {"page", "code", "create", "update"},
        "environment": {"page", "code", "create", "update"},
        "worker_group": {
            "page",
            "id",
            "save_basic",
            "save_described",
            "save_described_other",
        },
        "alert_group": {
            "page",
            "page_legacy",
            "id",
            "create",
            "create_legacy",
            "update",
            "update_legacy",
        },
    }

    standalone = {
        definition.name: _compile(exact_runtime_bundles, definition).plan(
            definition.name
        )
        for definition in definitions
    }

    for definition in definitions:
        plan = compiled_domain_set.plan(definition.name)
        assert plan == standalone[definition.name]
        assert tuple(profile.version for profile in plan.profiles) == (
            source_matrix.REVIEWED_DS_VERSIONS
        )
        assert {
            profile.version
            for profile in plan.profiles
            if profile.status == "upstream_absent"
        } == set(definition.absent_versions)
        assert all(
            not profile.programs
            for profile in plan.profiles
            if profile.status == "upstream_absent"
        )
        assert {request.schema for request in plan.requests} == expected_requests[
            definition.name
        ]

    removed_semantics = set().union(
        *(definition.semantic_operations for definition in definitions)
    )
    for bundle in compiled_domain_set.legacy_bundles:
        operations = {
            operation.operation_id for operation in bundle.snapshot.operations
        }
        response_types = {
            model.import_path
            for model in (*bundle.snapshot.dtos, *bundle.snapshot.models)
        }
        assert not any(
            operation.startswith(
                (
                    "ClusterController.",
                    "EnvironmentController.",
                    "WorkerGroupController.",
                    "AlertGroupController.",
                )
            )
            for operation in operations
        ), bundle.spec.version
        assert not (
            response_types
            & {
                "org.apache.dolphinscheduler.api.dto.ClusterDto",
                "org.apache.dolphinscheduler.dao.entity.Cluster",
                "org.apache.dolphinscheduler.api.dto.EnvironmentDto",
                "org.apache.dolphinscheduler.dao.entity.Environment",
                "org.apache.dolphinscheduler.dao.entity.WorkerGroup",
                "org.apache.dolphinscheduler.dao.entity.WorkerGroupPageDetail",
                "org.apache.dolphinscheduler.dao.entity.AlertGroup",
                "org.apache.dolphinscheduler.dao.vo.AlertGroupVo",
            }
        ), bundle.spec.version
        exact_enums = {item.import_path for item in bundle.snapshot.enums}
        worker_group_source = (
            "org.apache.dolphinscheduler.common.enums.WorkerGroupSource"
        )
        if bundle.spec.version in {
            "3.3.1",
            "3.3.2",
            "3.4.0",
            "3.4.1",
            "3.4.2",
            "3.4.3",
        }:
            assert worker_group_source in exact_enums
        else:
            assert worker_group_source not in exact_enums
        alert_type = "org.apache.dolphinscheduler.common.enums.AlertType"
        if bundle.spec.version == "1.3.9":
            assert alert_type in exact_enums
        else:
            assert alert_type not in exact_enums
        assert not (set(bundle.metadata.semantic_operations) & removed_semantics), (
            bundle.spec.version
        )


def test_cluster_compiler_preserves_all_response_and_envelope_epochs(
    compiled_domain_set: Any,
) -> None:
    plan = compiled_domain_set.plan("cluster")
    by_version = {profile.version: profile for profile in plan.profiles}
    expected = {
        "3.1.0": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.1": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.2": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.9": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.3": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.4": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.5": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.6": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.7": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.1.8": ("page_process_strict", "get_process", "update_void", "delete_void"),
        "3.2.0": ("page_process", "get_process", "update_void", "delete_void"),
        "3.2.1": ("page_process", "get_process", "update_entity", "delete_bool"),
        "3.2.2": ("page_process", "get_process", "update_entity", "delete_bool"),
        "3.3.1": ("page_workflow", "get_workflow", "update_entity", "delete_bool"),
        "3.3.2": ("page_workflow", "get_workflow", "update_entity", "delete_bool"),
        "3.4.0": ("page_workflow", "get_workflow", "update_entity", "delete_bool"),
        "3.4.1": ("page_workflow", "get_workflow", "update_entity", "delete_bool"),
        "3.4.2": ("page_workflow", "get_workflow", "update_entity", "delete_bool"),
        "3.4.3": ("page_workflow", None, "update_entity", "delete_bool"),
    }
    _assert_epochs(by_version, expected)


def test_compiled_profiles_materialize_exact_domain_recipes(
    compiled_domain_set: Any,
) -> None:
    process_void = "process_definitions_void_mutations"
    process_entity = "process_definitions_entity_mutations"
    workflow_entity = "workflow_definitions_entity_mutations"
    legacy_groups = "legacy_alert_type"
    plugin_void = "plugin_instances_void_mutations"
    plugin_create = "plugin_instances_entity_create"
    plugin_entities = "plugin_instances_entity_mutations"
    expected = {
        "1.3.9": (None, None, "basic", legacy_groups),
        "2.0.0": (None, "void_update", "basic", plugin_void),
        "2.0.1": (None, "void_update", "basic", plugin_void),
        "2.0.9": (None, "void_update", "basic", plugin_void),
        "2.0.2": (None, "void_update", "basic", plugin_void),
        "2.0.3": (None, "void_update", "basic", plugin_void),
        "2.0.4": (None, "void_update", "basic", plugin_void),
        "2.0.5": (None, "void_update", "basic", plugin_void),
        "2.0.6": (None, "void_update", "basic", plugin_void),
        "2.0.7": (None, "void_update", "basic", plugin_void),
        "2.0.8": (None, "void_update", "basic", plugin_void),
        "3.0.0": (None, "void_update", "basic", plugin_create),
        "3.0.1": (None, "void_update", "basic", plugin_create),
        "3.0.6": (None, "void_update", "basic", plugin_create),
        "3.0.2": (None, "void_update", "basic", plugin_create),
        "3.0.3": (None, "void_update", "basic", plugin_create),
        "3.0.4": (None, "void_update", "basic", plugin_create),
        "3.0.5": (None, "void_update", "basic", plugin_create),
        "3.1.0": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.1": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.2": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.9": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.3": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.4": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.5": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.6": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.7": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.1.8": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.2.0": (
            process_void,
            "void_update",
            "described_legacy_not_found",
            plugin_create,
        ),
        "3.2.1": (
            process_entity,
            "entity_update",
            "described_legacy_not_found",
            plugin_entities,
        ),
        "3.2.2": (
            process_entity,
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
        "3.3.1": (
            workflow_entity,
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
        "3.3.2": (
            workflow_entity,
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
        "3.4.0": (
            workflow_entity,
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
        "3.4.1": (
            workflow_entity,
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
        "3.4.2": (
            workflow_entity,
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
        "3.4.3": (
            "workflow_definitions_paged_readback",
            "entity_update",
            "described_modern_not_found",
            plugin_entities,
        ),
    }
    assert set(expected) == {
        profile.version for profile in compiled_domain_set.plan("cluster").profiles
    }
    domains = ("cluster", "environment", "worker_group", "alert_group")
    for version, recipes in expected.items():
        profiles = tuple(
            compiled_domain_set.plan(name).profile(version) for name in domains
        )
        assert tuple(profile.recipe_id for profile in profiles) == recipes
        assert tuple(profile.record()["recipe_id"] for profile in profiles) == recipes


def test_alert_group_compiler_rejects_crossed_recipe_epochs() -> None:
    definition = _load("ds_codegen.compiled_alert_groups").ALERT_GROUP_COMPILED_DOMAIN
    with pytest.raises(ValueError, match="recipe is unsupported"):
        definition.recipe_policy(
            {
                "page": "page_entity_nullable_id_strict",
                "direct_get": "direct_get_entity_required_id",
                "create": "create_entity_required_id",
                "update": "update_void",
                "delete": "delete_void",
            }
        )


def test_343_private_user_identity_has_one_owner_without_public_action_growth(
    compiled_all_domains: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    runtime_contract = _load("ds_codegen.runtime_contract")
    user_definition = _load("ds_codegen.compiled_users").USER_COMPILED_DOMAIN
    source_matrix = _load("ds_codegen.compatibility_impact")
    actions = _load("dsctl.cli_surface").stable_leaf_actions()
    auxiliary = runtime_contract.runtime_auxiliary_operation_bindings("3.4.3")

    assert len(actions) == 181
    assert set(auxiliary) == {"user.identity", "project-preference.read"}
    assert set(auxiliary).isdisjoint(actions)
    assert "user.identity" not in runtime_contract.runtime_operation_bindings("3.4.3")
    assert "user.identity" in user_definition.semantic_operations
    assert user_definition.semantic_absent_versions["user.identity"] == (
        frozenset(source_matrix.REVIEWED_DS_VERSIONS) - {"3.4.3"}
    )
    identity_sources = set(auxiliary["user.identity"].source_operations)
    assert identity_sources == {
        "UsersController.getUserInfo",
        "UsersController.listAll",
        "UsersController.queryUserList",
    }
    user_profile = compiled_all_domains.plan("user").profile("3.4.3")
    user_programs = dict(user_profile.programs)
    assert len(user_programs) == 17
    assert identity_sources == {
        user_programs[name].source_operation for name in ("current", "all", "page")
    }
    original = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.3"
    )
    remaining = next(
        bundle
        for bundle in compiled_all_domains.legacy_bundles
        if bundle.spec.version == "3.4.3"
    )
    assert "user.identity" in original.metadata.semantic_operations
    assert "user.identity" not in remaining.metadata.semantic_operations
    assert remaining.snapshot.operation_count == 0
    assert not identity_sources.intersection(
        operation.operation_id for operation in remaining.snapshot.operations
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [("documentation", "Unconsumed controller documentation."), ("path", "/verify-v2")],
)
def test_unconsumed_source_drift_changes_exact_audit_identity_only(
    exact_contract_corpus: ExactContractCorpus,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    compiled_domain_set: CompiledDomainSet,
    field: str,
    value: str,
) -> None:
    definition = _load("ds_codegen.compiled_clusters").CLUSTER_COMPILED_DOMAIN
    source = exact_contract_corpus.snapshot("3.4.1")
    unconsumed = "ClusterController.verifyCluster"
    assert any(operation.operation_id == unconsumed for operation in source.operations)
    operations = []
    for operation in source.operations:
        changed_operation = operation
        if operation.operation_id == unconsumed:
            if field == "documentation":
                changed_operation = replace(operation, documentation=value)
            else:
                changed_operation = replace(operation, path=value)
        operations.append(changed_operation)
    changed_source = replace(source, operations=operations)
    changed_bundles = tuple(
        _with_source_snapshot(bundle, changed_source)
        if bundle.spec.version == "3.4.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    before = compiled_domain_set.plan("cluster")
    after = _compile(changed_bundles, definition).plan("cluster")

    assert after.codecs == before.codecs
    assert after.requests == before.requests
    assert after.responses == before.responses
    for old, new in zip(before.profiles, after.profiles, strict=True):
        assert new.programs == old.programs
        if old.version == "3.4.1":
            assert new.source_contract_digest != old.source_contract_digest
            assert new.record()["source"] != old.record()["source"]
            assert new.record()["profile_digest"] != old.record()["profile_digest"]
        else:
            assert new == old


def test_consumed_request_shape_changes_only_its_executable_identity(
    exact_contract_corpus: ExactContractCorpus,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    compiled_domain_set: CompiledDomainSet,
) -> None:
    definition = _load("ds_codegen.compiled_clusters").CLUSTER_COMPILED_DOMAIN
    changed_bundles = []
    for bundle in exact_runtime_bundles:
        if bundle.spec.version in definition.absent_versions:
            changed_bundles.append(bundle)
            continue
        source = exact_contract_corpus.snapshot(bundle.spec.version)
        changed_source = replace(
            source,
            operations=[
                replace(
                    operation,
                    parameters=[
                        replace(parameter, default_value="reviewed-default")
                        if parameter.name == "searchVal"
                        else parameter
                        for parameter in operation.parameters
                    ],
                )
                if operation.operation_id == "ClusterController.queryClusterListPaging"
                else operation
                for operation in source.operations
            ],
        )
        changed_bundles.append(_with_source_snapshot(bundle, changed_source))
    before = compiled_domain_set.plan("cluster")
    after = _compile(tuple(changed_bundles), definition).plan("cluster")

    changed_programs = []
    for old, new in zip(before.profiles, after.profiles, strict=True):
        if old.status == "upstream_absent":
            assert new == old
            continue
        assert new.source_contract_digest != old.source_contract_digest
        assert new.record()["profile_digest"] != old.record()["profile_digest"]
        assert new.recipe_id == old.recipe_id
        for primitive, old_program in old.programs:
            new_program = dict(new.programs)[primitive]
            if primitive == "page":
                changed_programs.append((old.version, primitive))
                assert (
                    new_program.request_schema_digest
                    != old_program.request_schema_digest
                )
                assert new_program.codec_digest != old_program.codec_digest
                assert new_program.program_digest != old_program.program_digest
                assert new_program.response_digest == old_program.response_digest
                assert (
                    new_program.response_schema_digest
                    == old_program.response_schema_digest
                )
            else:
                assert new_program == old_program
    assert len(changed_programs) == 19


def test_recipe_changes_exact_profile_without_changing_exchange_programs(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    compiled_domain_set: CompiledDomainSet,
) -> None:
    definition = _load("ds_codegen.compiled_clusters").CLUSTER_COMPILED_DOMAIN

    def alternate_recipe(codecs: Mapping[str, str]) -> str:
        return f"{definition.recipe_policy(codecs)}_alternate"

    before = compiled_domain_set.plan("cluster")
    after = _compile(
        exact_runtime_bundles,
        replace(definition, recipe_policy=alternate_recipe),
    ).plan("cluster")

    assert after.codecs == before.codecs
    for old, new in zip(before.profiles, after.profiles, strict=True):
        assert new.programs == old.programs
        assert new.record()["source"] == old.record()["source"]
        if old.status == "upstream_absent":
            assert new == old
        else:
            assert new.recipe_id != old.recipe_id
            assert new.record()["profile_digest"] != old.record()["profile_digest"]


def _with_source_snapshot(
    bundle: RuntimeBundle,
    source: ContractSnapshot,
) -> RuntimeBundle:
    """Model a full-source edit while retaining its independent audit digest."""
    contract_digest = _load("ds_codegen.contract_inputs").contract_snapshot_digest
    snapshot = _load("ds_codegen.runtime_contract").configured_runtime_contract_slice(
        source
    )
    return replace(
        bundle,
        snapshot=snapshot,
        metadata=replace(
            bundle.metadata,
            source_contract_digest=contract_digest(source),
            rendered_contract_digest=contract_digest(snapshot),
            operation_count=snapshot.operation_count,
            enum_count=snapshot.enum_count,
            dto_count=snapshot.dto_count,
            model_count=snapshot.model_count,
        ),
    )


def test_environment_compiler_preserves_all_response_and_envelope_epochs(
    compiled_domain_set: Any,
) -> None:
    plan = compiled_domain_set.plan("environment")
    by_version = {profile.version: profile for profile in plan.profiles}
    expected = {
        "2.0.0": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.1": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.9": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.2": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.3": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.4": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.5": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.6": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.7": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "2.0.8": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.0": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.1": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.6": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.2": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.3": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.4": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.0.5": ("page_legacy_strict", "get_legacy", "update_void", "delete_void"),
        "3.1.0": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.1": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.2": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.9": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.3": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.4": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.5": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.6": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.7": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.1.8": ("page_list_strict", "get_modern", "update_void", "delete_void"),
        "3.2.0": ("page_list", "get_modern", "update_void", "delete_void"),
        "3.2.1": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.2.2": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.3.1": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.3.2": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.4.0": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.4.1": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.4.2": ("page_list", "get_modern", "update_entity", "delete_void"),
        "3.4.3": ("page_list", "get_modern", "update_entity", "delete_void"),
    }
    _assert_epochs(by_version, expected)
    for version in expected:
        create_source = dict(by_version[version].programs)["create"].source_operation
        expected_method = (
            "EnvironmentController.createProject"
            if version
            in {
                "2.0.0",
                "2.0.1",
                "2.0.2",
                "2.0.3",
                "2.0.4",
                "2.0.5",
                "2.0.6",
                "2.0.7",
                "2.0.8",
                "2.0.9",
                "3.0.0",
                "3.0.1",
                "3.0.2",
                "3.0.3",
                "3.0.4",
                "3.0.5",
                "3.0.6",
            }
            else "EnvironmentController.createEnvironment"
        )
        assert create_source == expected_method


def test_worker_group_compiler_preserves_all_wire_and_response_epochs(
    compiled_domain_set: Any,
) -> None:
    plan = compiled_domain_set.plan("worker_group")
    by_version = {profile.version: profile for profile in plan.profiles}
    expected = {
        "1.3.9": ("page_basic_legacy", "save_legacy_void", "delete_form_void"),
        "2.0.0": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.1": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.9": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.2": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.3": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.4": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.5": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.6": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.7": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "2.0.8": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.0": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.1": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.6": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.2": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.3": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.4": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.0.5": ("page_basic_strict", "save_basic_void", "delete_path_void"),
        "3.1.0": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.1": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.2": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.9": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.3": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.4": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.5": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.6": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.7": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.1.8": (
            "page_described_strict",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.2.0": (
            "page_described",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.2.1": (
            "page_described",
            "save_described_other_void",
            "delete_path_void",
        ),
        "3.2.2": (
            "page_described",
            "save_described_other_entity",
            "delete_path_void",
        ),
        "3.3.1": ("page_detail", "save_described_entity", "delete_path_void"),
        "3.3.2": ("page_detail", "save_described_entity", "delete_path_void"),
        "3.4.0": ("page_detail", "save_described_entity", "delete_path_void"),
        "3.4.1": ("page_detail", "save_described_entity", "delete_path_void"),
        "3.4.2": ("page_detail", "save_described_entity", "delete_path_void"),
        "3.4.3": ("page_detail", "save_described_entity", "delete_path_void"),
    }
    assert set(expected) == set(by_version)
    for version, epoch in expected.items():
        programs = dict(by_version[version].programs)
        assert set(programs) == {"page", "save", "delete"}
        assert (
            tuple(programs[name].codec for name in ("page", "save", "delete")) == epoch
        )
        assert programs["page"].result_envelope == "optional"
        assert all(
            programs[name].result_envelope == "required" for name in ("save", "delete")
        )
    assert {
        program.source_operation
        for profile in plan.profiles
        for _primitive, program in profile.programs
    } == {
        "WorkerGroupController.queryAllWorkerGroupsPaging",
        "WorkerGroupController.saveWorkerGroup",
        "WorkerGroupController.deleteById",
        "WorkerGroupController.deleteWorkerGroupById",
    }
    codecs = dict(plan.codecs)
    assert {
        (
            record["method"],
            record["path"],
            record["channel"],
            record["params"],
            record["path_encoding"],
        )
        for record in codecs.values()
    } == {
        ("GET", "worker-group/list-paging", "query", "page", None),
        ("GET", "worker-groups", "query", "page", None),
        ("POST", "worker-group/save", "form", "save_basic", None),
        ("POST", "worker-groups", "form", "save_basic", None),
        (
            "POST",
            "worker-groups",
            "form",
            "save_described_other",
            None,
        ),
        ("POST", "worker-groups", "form", "save_described", None),
        ("POST", "worker-group/delete-by-id", "form", "id", None),
        (
            "DELETE",
            "worker-groups/{id}",
            "path",
            "id",
            "percent-encoded-utf8-segment-v1",
        ),
    }


def test_alert_group_compiler_preserves_absence_and_all_wire_epochs(
    compiled_domain_set: Any,
) -> None:
    plan = compiled_domain_set.plan("alert_group")
    by_version = {profile.version: profile for profile in plan.profiles}
    expected = {
        "1.3.9": (
            "page_legacy_alert_type",
            None,
            "create_legacy_void",
            "update_legacy_void",
            "delete_legacy_void",
        ),
        "2.0.0": (
            "page_vo_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.1": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.9": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.2": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.3": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.4": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.5": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.6": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.7": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.8": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "3.0.0": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.1": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.6": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.2": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.3": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.4": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.5": (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.1.0": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.1": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.2": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.9": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.3": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.4": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.5": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.6": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.7": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.8": (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.2.0": (
            "page_entity_nullable_id",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
    }
    modern = (
        "page_entity_nullable_id",
        "direct_get_entity_nullable_id",
        "create_entity_nullable_id",
        "update_entity_nullable_id",
        "delete_bool",
    )
    expected.update(
        dict.fromkeys(
            (
                "3.2.1",
                "3.2.2",
                "3.3.1",
                "3.3.2",
                "3.4.0",
                "3.4.1",
                "3.4.2",
                "3.4.3",
            ),
            modern,
        )
    )
    assert set(expected) == set(by_version)
    for version, epoch in expected.items():
        programs = dict(by_version[version].programs)
        assert set(programs) == (
            {"page", "create", "update", "delete"}
            if version == "1.3.9"
            else {"page", "direct_get", "create", "update", "delete"}
        )
        assert (
            tuple(
                programs[name].codec if name in programs else None
                for name in ("page", "direct_get", "create", "update", "delete")
            )
            == epoch
        )
        assert programs["page"].result_envelope == "optional"
        if "direct_get" in programs:
            assert programs["direct_get"].result_envelope == "optional"
        assert all(
            programs[name].result_envelope == "required"
            for name in ("create", "update", "delete")
        )
    assert {
        program.source_operation
        for profile in plan.profiles
        for _primitive, program in profile.programs
    } == {
        "AlertGroupController.listPaging",
        "AlertGroupController.queryAlertGroupById",
        "AlertGroupController.createAlertgroup",
        "AlertGroupController.createAlertGroup",
        "AlertGroupController.updateAlertgroup",
        "AlertGroupController.updateAlertGroupById",
        "AlertGroupController.delAlertgroupById",
        "AlertGroupController.deleteAlertGroupById",
    }
    request_by_schema = {request.schema: request for request in plan.requests}
    assert request_by_schema["create_legacy"].module_name is not None
    assert request_by_schema["update_legacy"].module_name is not None
    update = dict(plan.codecs)["update_entity_nullable_id"]
    assert (update["method"], update["channel"], update["path"]) == (
        "PUT",
        "path_form",
        "alert-groups/{id}",
    )
    assert update["path_fields"] == ["id"]
    assert update["fields"] == [
        {"name": "id", "binding": "path_variable"},
        {"name": "groupName", "binding": "request_param"},
        {"name": "description", "binding": "request_param"},
        {"name": "alertInstanceIds", "binding": "request_param"},
    ]


def _assert_epochs(
    by_version: dict[str, Any],
    expected: Mapping[str, tuple[str, str | None, str, str]],
) -> None:
    assert set(expected) == {
        version
        for version, profile in by_version.items()
        if profile.status == "supported"
    }
    for version, epoch in expected.items():
        programs = dict(by_version[version].programs)
        expected_primitives = {"page", "create", "update", "delete"}
        if epoch[1] is not None:
            expected_primitives.add("get")
        assert set(programs) == expected_primitives
        assert (
            programs["page"].codec,
            programs["get"].codec if "get" in programs else None,
            programs["update"].codec,
            programs["delete"].codec,
        ) == epoch
        assert programs["create"].codec == "create_int"
        assert programs["page"].result_envelope == "optional"
        if "get" in programs:
            assert programs["get"].result_envelope == "optional"
        assert all(
            programs[name].result_envelope == "required"
            for name in ("create", "update", "delete")
        )


@pytest.mark.parametrize(
    (
        "domain_module",
        "definition_name",
        "version",
        "import_path",
        "field_name",
        "match",
    ),
    [
        (
            "ds_codegen.compiled_clusters",
            "CLUSTER_COMPILED_DOMAIN",
            "3.4.1",
            "org.apache.dolphinscheduler.api.dto.ClusterDto",
            "code",
            r"response closure (?:page_workflow|get_workflow) differs",
        ),
        (
            "ds_codegen.compiled_clusters",
            "CLUSTER_COMPILED_DOMAIN",
            "3.4.1",
            "org.apache.dolphinscheduler.api.utils.PageInfo",
            "total",
            r"response closure page_workflow differs",
        ),
        (
            "ds_codegen.compiled_environments",
            "ENVIRONMENT_COMPILED_DOMAIN",
            "3.4.1",
            "org.apache.dolphinscheduler.api.dto.EnvironmentDto",
            "code",
            r"response closure (?:page_list|get_modern) differs",
        ),
        (
            "ds_codegen.compiled_environments",
            "ENVIRONMENT_COMPILED_DOMAIN",
            "3.4.1",
            "org.apache.dolphinscheduler.dao.entity.Environment",
            "config",
            r"response closure update_entity differs",
        ),
        (
            "ds_codegen.compiled_worker_groups",
            "WORKER_GROUP_COMPILED_DOMAIN",
            "3.4.1",
            "org.apache.dolphinscheduler.dao.entity.WorkerGroupPageDetail",
            "source",
            r"response closure page_detail differs",
        ),
        (
            "ds_codegen.compiled_alert_groups",
            "ALERT_GROUP_COMPILED_DOMAIN",
            "3.4.1",
            "org.apache.dolphinscheduler.dao.entity.AlertGroup",
            "id",
            r"alert_group entity response epoch changed",
        ),
    ],
)
def test_compiled_response_closure_drift_fails_compilation(
    exact_runtime_bundles: tuple[Any, ...],
    domain_module: str,
    definition_name: str,
    version: str,
    import_path: str,
    field_name: str,
    match: str,
) -> None:
    definition = getattr(_load(domain_module), definition_name)
    mutated = _mutate_response_model_field(
        exact_runtime_bundles,
        version=version,
        import_path=import_path,
        field_name=field_name,
        java_type="String" if field_name != "config" else "Long",
    )

    with pytest.raises(ValueError, match=match):
        _compile(mutated, definition)


def test_worker_group_response_enum_closure_drift_fails_compilation(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    mutated = _mutate_response_enum_value(
        exact_runtime_bundles,
        version="3.4.1",
        import_path="org.apache.dolphinscheduler.common.enums.WorkerGroupSource",
        value_name="CONFIG",
        arguments=["CONFIG", "9", "changed"],
    )

    with pytest.raises(ValueError, match=r"response closure page_detail differs"):
        _compile(mutated, definition)


@pytest.mark.parametrize(
    ("domain_module", "definition_name", "operation_id", "changes"),
    [
        pytest.param(
            "ds_codegen.compiled_clusters",
            "CLUSTER_COMPILED_DOMAIN",
            "ClusterController.createCluster",
            {"java_type": "Long"},
            id="cluster-java-type",
        ),
        pytest.param(
            "ds_codegen.compiled_environments",
            "ENVIRONMENT_COMPILED_DOMAIN",
            "EnvironmentController.createEnvironment",
            {"required": True},
            id="environment-required",
        ),
        pytest.param(
            "ds_codegen.compiled_worker_groups",
            "WORKER_GROUP_COMPILED_DOMAIN",
            "WorkerGroupController.saveWorkerGroup",
            {"default_value": "fallback"},
            id="worker-group-default",
        ),
        pytest.param(
            "ds_codegen.compiled_alert_groups",
            "ALERT_GROUP_COMPILED_DOMAIN",
            "AlertGroupController.createAlertGroup",
            {"java_type": "Long"},
            id="alert-group-java-type",
        ),
    ],
)
def test_compiled_request_executable_schema_drift_fails_compilation(
    exact_runtime_bundles: tuple[Any, ...],
    domain_module: str,
    definition_name: str,
    operation_id: str,
    changes: dict[str, Any],
) -> None:
    definition = getattr(_load(domain_module), definition_name)
    mutated = _mutate_request_parameter(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id=operation_id,
        parameter_name="description",
        changes=changes,
    )

    expected_schema = "save_described" if "WorkerGroup" in operation_id else "create"
    with pytest.raises(ValueError, match=rf"request schema {expected_schema} differs"):
        _compile(mutated, definition)


def test_environment_shared_request_schema_detects_get_delete_drift(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_environments").ENVIRONMENT_COMPILED_DOMAIN
    mutated = _mutate_request_parameter(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id="EnvironmentController.deleteEnvironment",
        parameter_name="environmentCode",
        changes={"java_type": "String"},
    )

    with pytest.raises(ValueError, match=r"request schema code differs"):
        _compile(mutated, definition)


def test_environment_source_transport_drift_fails_compilation(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_environments").ENVIRONMENT_COMPILED_DOMAIN
    mutated = _mutate_operation(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id="EnvironmentController.createEnvironment",
        path="environment/create-v2",
    )

    with pytest.raises(ValueError, match=r"environment create request shape has 0"):
        _compile(mutated, definition)


@pytest.mark.parametrize("suffix", ["?leak=true", "#fragment"])
def test_compiled_path_rejects_query_or_fragment_syntax(
    exact_runtime_bundles: tuple[Any, ...],
    suffix: str,
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    mutated = _mutate_operation(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id="WorkerGroupController.deleteWorkerGroupById",
        path=f"worker-groups/{{id}}{suffix}",
    )

    with pytest.raises(ValueError, match=r"query or fragment syntax"):
        _compile(mutated, definition)


def test_compiled_path_requires_exactly_one_integer_variable(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    mutated = _mutate_request_parameter(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id="WorkerGroupController.deleteWorkerGroupById",
        parameter_name="id",
        changes={"java_type": "String"},
    )

    with pytest.raises(ValueError, match=r"reviewed integer-segment shape"):
        _compile(mutated, definition)


@pytest.mark.parametrize("required_fields", [None, frozenset({"name", "addrList"})])
def test_compiled_request_epoch_selection_rejects_multiple_matches(
    exact_runtime_bundles: tuple[Any, ...],
    required_fields: frozenset[str] | None,
) -> None:
    domains = _load("ds_codegen.compiled_domains")
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    duplicate = domains.CompiledRequestEpoch(
        method="POST",
        path="worker-groups",
        channel="form",
        request_schema="ambiguous_save",
        request_model="AmbiguousWorkerGroupSaveParams",
        request_fields=("id", "name", "addrList"),
        required_fields=required_fields,
    )
    drifted = replace(
        definition,
        primitives=tuple(
            replace(primitive, requests=(*primitive.requests, duplicate))
            if primitive.name == "save"
            else primitive
            for primitive in definition.primitives
        ),
    )

    with pytest.raises(ValueError, match=r"save request shape has 2 matching epochs"):
        _compile(exact_runtime_bundles, drifted)


@pytest.mark.parametrize(
    ("required_fields", "message"),
    [
        (frozenset({"name"}), "request shape has 0 matching epochs"),
        (frozenset({"name", "addrList", "unreviewed"}), "request epoch is invalid"),
    ],
)
def test_compiled_request_requiredness_must_match_the_reviewed_fields(
    exact_runtime_bundles: tuple[Any, ...],
    required_fields: frozenset[str],
    message: str,
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    drifted = replace(
        definition,
        primitives=tuple(
            replace(
                primitive,
                requests=tuple(
                    replace(request, required_fields=required_fields)
                    for request in primitive.requests
                ),
            )
            if primitive.name == "save"
            else primitive
            for primitive in definition.primitives
        ),
    )

    with pytest.raises(ValueError, match=message):
        _compile(exact_runtime_bundles, drifted)


def test_compiled_request_rejects_mixed_parameter_channels(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    mutated = _append_operation_parameter(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id="WorkerGroupController.deleteWorkerGroupById",
        source_parameter="id",
        name="shadow",
        wire_name="shadow",
        binding="request_param",
    )

    with pytest.raises(ValueError, match=r"delete request transport is unsupported"):
        _compile(mutated, definition)


def test_alert_group_direct_get_absence_is_explicit_in_definition(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_alert_groups").ALERT_GROUP_COMPILED_DOMAIN
    drifted = replace(
        definition,
        primitives=tuple(
            replace(primitive, absent_versions=frozenset())
            if primitive.name == "direct_get"
            else primitive
            for primitive in definition.primitives
        ),
    )

    with pytest.raises(ValueError, match=r"operation mapping is ambiguous"):
        _compile(exact_runtime_bundles, drifted)


def test_alert_group_unexpected_direct_source_fails_compilation(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_alert_groups").ALERT_GROUP_COMPILED_DOMAIN
    source = next(
        operation
        for bundle in exact_runtime_bundles
        if bundle.spec.version == "2.0.0"
        for operation in bundle.snapshot.operations
        if operation.operation_id == "AlertGroupController.queryAlertGroupById"
    )
    mutated = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[*bundle.snapshot.operations, source],
            ),
        )
        if bundle.spec.version == "1.3.9"
        else bundle
        for bundle in exact_runtime_bundles
    )

    with pytest.raises(ValueError, match=r"unowned classified operations"):
        _compile(mutated, definition)


def test_alert_group_request_enum_drift_fails_compilation(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_alert_groups").ALERT_GROUP_COMPILED_DOMAIN
    mutated = _mutate_response_enum_value(
        exact_runtime_bundles,
        version="1.3.9",
        import_path="org.apache.dolphinscheduler.common.enums.AlertType",
        value_name="EMAIL",
        arguments=["9", "changed"],
    )

    with pytest.raises(ValueError, match=r"AlertType enum changed"):
        _compile(mutated, definition)


def test_compiled_path_requires_exact_placeholder_inventory(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    mutated = _mutate_operation(
        exact_runtime_bundles,
        version="3.4.1",
        operation_id="WorkerGroupController.deleteWorkerGroupById",
        path="worker-groups/{workerId}",
    )

    with pytest.raises(ValueError, match="implicit path arguments are unsupported"):
        _compile(mutated, definition)


def test_compiled_definition_path_rejects_query_syntax(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_worker_groups").WORKER_GROUP_COMPILED_DOMAIN
    delete = next(item for item in definition.primitives if item.name == "delete")
    path_epoch = next(item for item in delete.requests if item.channel == "path")
    drifted = replace(
        definition,
        primitives=tuple(
            replace(
                primitive,
                requests=tuple(
                    replace(request, path=f"{request.path}?force=1")
                    if request == path_epoch
                    else request
                    for request in primitive.requests
                ),
            )
            if primitive.name == "delete"
            else primitive
            for primitive in definition.primitives
        ),
    )

    with pytest.raises(ValueError, match=r"query or fragment syntax"):
        _compile(exact_runtime_bundles, drifted)


def test_owned_source_operation_must_be_classified(
    exact_runtime_bundles: tuple[Any, ...],
) -> None:
    definition = _load("ds_codegen.compiled_environments").ENVIRONMENT_COMPILED_DOMAIN
    classify = definition.classify_operation
    drifted = replace(
        definition,
        classify_operation=lambda operation: (
            None
            if operation.method_name == "queryEnvironmentByCode"
            else classify(operation)
        ),
    )

    with pytest.raises(
        ValueError,
        match=r"owned source operation .*queryEnvironmentByCode.* is unclassified",
    ):
        _compile(exact_runtime_bundles, drifted)


def test_supported_profile_requires_every_semantic_binding(
    exact_runtime_bundles: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    domains = _load("ds_codegen.compiled_domains")
    definition = _load("ds_codegen.compiled_environments").ENVIRONMENT_COMPILED_DOMAIN
    runtime_bindings = domains.runtime_operation_bindings

    def missing_environment_get(version: str) -> dict[str, Any]:
        bindings = dict(runtime_bindings(version))
        if version == "3.4.1":
            bindings.pop("environment.get")
        return bindings

    monkeypatch.setattr(
        domains,
        "runtime_operation_bindings",
        missing_environment_get,
    )

    with pytest.raises(
        ValueError,
        match=r"environment DS 3\.4\.1 semantic binding inventory is incomplete",
    ):
        _compile(exact_runtime_bundles, definition)


def _mutate_response_model_field(
    bundles: tuple[Any, ...],
    *,
    version: str,
    import_path: str,
    field_name: str,
    **changes: Any,
) -> tuple[Any, ...]:
    updated = []
    for bundle in bundles:
        if bundle.spec.version != version:
            updated.append(bundle)
            continue
        models = [
            replace(
                model,
                fields=[
                    replace(field, **changes) if field.name == field_name else field
                    for field in model.fields
                ],
            )
            if model.import_path == import_path
            else model
            for model in bundle.snapshot.models
        ]
        updated.append(
            replace(bundle, snapshot=replace(bundle.snapshot, models=models))
        )
    return tuple(updated)


def _mutate_request_parameter(
    bundles: tuple[Any, ...],
    *,
    version: str,
    operation_id: str,
    parameter_name: str,
    changes: dict[str, Any],
) -> tuple[Any, ...]:
    updated = []
    for bundle in bundles:
        if bundle.spec.version != version:
            updated.append(bundle)
            continue
        operations = [
            replace(
                operation,
                parameters=[
                    replace(parameter, **changes)
                    if parameter.name == parameter_name
                    else parameter
                    for parameter in operation.parameters
                ],
            )
            if operation.operation_id == operation_id
            else operation
            for operation in bundle.snapshot.operations
        ]
        updated.append(
            replace(bundle, snapshot=replace(bundle.snapshot, operations=operations))
        )
    return tuple(updated)


def _mutate_response_enum_value(
    bundles: tuple[Any, ...],
    *,
    version: str,
    import_path: str,
    value_name: str,
    arguments: list[str],
) -> tuple[Any, ...]:
    updated = []
    for bundle in bundles:
        if bundle.spec.version != version:
            updated.append(bundle)
            continue
        enums = [
            replace(
                enum,
                values=[
                    replace(value, arguments=arguments)
                    if value.name == value_name
                    else value
                    for value in enum.values
                ],
            )
            if enum.import_path == import_path
            else enum
            for enum in bundle.snapshot.enums
        ]
        updated.append(replace(bundle, snapshot=replace(bundle.snapshot, enums=enums)))
    return tuple(updated)


def _mutate_operation(
    bundles: tuple[Any, ...],
    *,
    version: str,
    operation_id: str,
    **changes: Any,
) -> tuple[Any, ...]:
    updated = []
    for bundle in bundles:
        if bundle.spec.version != version:
            updated.append(bundle)
            continue
        operations = [
            replace(operation, **changes)
            if operation.operation_id == operation_id
            else operation
            for operation in bundle.snapshot.operations
        ]
        updated.append(
            replace(bundle, snapshot=replace(bundle.snapshot, operations=operations))
        )
    return tuple(updated)


def _append_operation_parameter(
    bundles: tuple[Any, ...],
    *,
    version: str,
    operation_id: str,
    source_parameter: str,
    **changes: Any,
) -> tuple[Any, ...]:
    updated = []
    for bundle in bundles:
        if bundle.spec.version != version:
            updated.append(bundle)
            continue
        operations = []
        for operation in bundle.snapshot.operations:
            if operation.operation_id != operation_id:
                operations.append(operation)
                continue
            parameter = next(
                item for item in operation.parameters if item.name == source_parameter
            )
            operations.append(
                replace(
                    operation,
                    parameters=[*operation.parameters, replace(parameter, **changes)],
                )
            )
        updated.append(
            replace(bundle, snapshot=replace(bundle.snapshot, operations=operations))
        )
    return tuple(updated)
