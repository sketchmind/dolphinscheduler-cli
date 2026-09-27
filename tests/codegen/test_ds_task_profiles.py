from __future__ import annotations

import copy
import hashlib
import importlib
import json
import runpy
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus


_EXPECTED_PROFILE_VERSIONS = (
    "1.3.9",
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
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

# These review-owned digests are intentionally outside the generator. They make
# coordinated fact/review drift and evidence-plane deletion require an explicit
# test attestation update without copying the 11,000-line ledgers into Python.
_TASK_PROFILE_FACTS_ATTESTATION_DIGEST = (
    "sha256:e65a41d7f1e60a2f13bcf13e5dfa2ad1b502be4f08448cffebfdf28c026c1f7a"
)
_TASK_PROFILE_REVIEWS_ATTESTATION_DIGEST = (
    "sha256:d20a905eb861a608618d16dfaa139c30e815306eb92f0a9858d56ae37a45d8e1"
)


def _module() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.task_profiles")


@pytest.fixture(scope="module")
def task_profile_documents() -> tuple[dict[str, Any], dict[str, Any]]:
    task_profiles = _module()
    return (
        task_profiles.load_task_profile_document(
            task_profiles.DEFAULT_TASK_PROFILE_FACTS
        ),
        task_profiles.load_task_profile_document(
            task_profiles.DEFAULT_TASK_PROFILE_REVIEWS
        ),
    )


@pytest.fixture(scope="module")
def compiled_task_profiles(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> dict[str, Any]:
    facts, reviews = task_profile_documents
    data: dict[str, Any] = _module().compile_task_profile_data(facts, reviews)
    return data


def test_tracked_task_profile_inputs_match_independent_attestations(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    facts, reviews = task_profile_documents

    assert _attestation_digest(facts) == _TASK_PROFILE_FACTS_ATTESTATION_DIGEST
    assert _attestation_digest(reviews) == _TASK_PROFILE_REVIEWS_ATTESTATION_DIGEST


def test_task_facts_can_exceed_the_reviewed_selection(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    facts, original_reviews = task_profile_documents
    reviews = copy.deepcopy(original_reviews)
    reviews["profiles"] = {"3.4.1": reviews["profiles"]["3.4.1"]}

    compiled = _module().compile_task_profile_data(facts, reviews)

    assert compiled["target_versions"] == ["3.4.1"]
    assert tuple(compiled["profiles"]) == ("3.4.1",)
    assert len(facts["profiles"]) == 37


def test_mechanical_task_inventory_can_select_its_own_exact_sources(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    facts, _ = task_profile_documents
    inventory = _inventory_from_facts(facts)
    inventory["targets"] = [
        target for target in inventory["targets"] if target["label"] == "3.4.1"
    ]

    projected = _module().project_task_profile_facts(inventory)

    assert projected["target_versions"] == ["3.4.1"]
    assert tuple(projected["profiles"]) == ("3.4.1",)


def test_tracked_facts_compile_every_exact_task_profile(
    compiled_task_profiles: dict[str, Any],
) -> None:
    data = compiled_task_profiles

    assert tuple(data["target_versions"]) == _EXPECTED_PROFILE_VERSIONS
    assert tuple(data["profiles"]) == _EXPECTED_PROFILE_VERSIONS
    assert len(data["profiles"]["1.3.9"]["task_types"]) == 13
    assert len(data["profiles"]["3.2.2"]["task_types"]) == 39
    assert len(data["profiles"]["3.4.2"]["task_types"]) == 37
    assert data["profiles"]["3.4.2"]["task_types"]["EMR_SERVERLESS"] == {
        "parameter_model_import": (
            "org.apache.dolphinscheduler.plugin.task.emrserverless."
            "EmrServerlessParameters"
        ),
        "semantic_fingerprint": (
            "sha256:ae9ec4cfe99775fa76653b464da46c5bdf48e73228ea350303ac5572ff878b95"
        ),
        "registration_kind": "spi_factory",
    }


def test_343_admission_retains_exact_holes_and_reviews_datax_source_change(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    facts, reviews = task_profile_documents
    latest = facts["profiles"]["3.4.3"]["task_types"]
    previous = facts["profiles"]["3.4.2"]["task_types"]
    assert set(latest) == set(previous)
    assert {
        task_type
        for task_type in latest
        if latest[task_type]["semantic_fingerprint"]
        != previous[task_type]["semantic_fingerprint"]
    } == {"DATAX"}
    assert latest["DATAX"]["semantic_fingerprint"] == (
        "sha256:1f0fbb5bc5c1b646732ad1ead35647d2175102b3a48b53a24d0fb43739706f64"
    )
    admitted = reviews["profiles"]["3.4.3"]
    assert len(admitted["typed_authoring"]) == 35
    assert "FLINK_STREAM" not in admitted["typed_authoring"]
    assert set(admitted["typed_authoring_exclusions"]) == {"LINKIS"}
    for task_type, review in admitted["typed_authoring"].items():
        assert (
            review["cli_model"]
            == reviews["profiles"]["3.4.2"]["typed_authoring"][task_type]["cli_model"]
        )
    datax_review = admitted["typed_authoring"]["DATAX"]
    dependent_review = admitted["typed_authoring"]["DEPENDENT"]
    assert any(
        path.endswith("DataxParametersTest.java")
        for path in datax_review["evidence_paths"]["regression_tests"]
    )
    assert any(
        path.endswith("DependentItemTest.java")
        for path in dependent_review["evidence_paths"]["regression_tests"]
    )


def test_typed_authoring_matrix_is_exact_and_complete(
    compiled_task_profiles: dict[str, Any],
) -> None:
    data = compiled_task_profiles

    modern_types = {
        "CONDITIONS",
        "DEPENDENT",
        "DINKY",
        "DVC",
        "EMR",
        "HIVECLI",
        "HTTP",
        "JUPYTER",
        "MLFLOW",
        "OPENMLDB",
        "PROCEDURE",
        "PYTHON",
        "REMOTESHELL",
        "SAGEMAKER",
        "SHELL",
        "SQL",
        "SQOOP",
        "SUB_WORKFLOW",
        "SWITCH",
        "ZEPPELIN",
    }
    legacy_logic_types = {
        "CONDITIONS",
        "DEPENDENT",
        "HTTP",
        "PROCEDURE",
        "PYTHON",
        "SHELL",
        "SQL",
        "SQOOP",
        "SUB_WORKFLOW",
        "SWITCH",
    }
    pigeon_legacy_types = legacy_logic_types | {"PIGEON"}
    emr_pigeon_legacy_types = pigeon_legacy_types | {"EMR", "ZEPPELIN"}
    spark_emr_pigeon_legacy_types = emr_pigeon_legacy_types | {"SPARK"}
    flink_spark_emr_pigeon_legacy_types = spark_emr_pigeon_legacy_types | {"FLINK"}
    hivecli_emr_pigeon_legacy_types = emr_pigeon_legacy_types | {
        "DINKY",
        "DVC",
        "HIVECLI",
        "JUPYTER",
        "MLFLOW",
        "OPENMLDB",
    }
    spark_hivecli_emr_pigeon_legacy_types = hivecli_emr_pigeon_legacy_types | {
        "SAGEMAKER",
        "SPARK",
    }
    flink_spark_hivecli_emr_pigeon_legacy_types = (
        spark_hivecli_emr_pigeon_legacy_types | {"FLINK"}
    )
    pigeon_modern_types = modern_types | {"PIGEON"}
    spark_pigeon_modern_types = pigeon_modern_types | {"SPARK"}
    flink_spark_pigeon_modern_types = spark_pigeon_modern_types | {"FLINK"}
    spark_modern_types = modern_types | {"SPARK"}
    flink_spark_modern_types = spark_modern_types | {"FLINK"}
    serverless_modern_types = spark_modern_types | {"EMR_SERVERLESS"}
    flink_serverless_modern_types = serverless_modern_types | {"FLINK"}
    data_factory_types = {"DATA_FACTORY"}
    datax_types = {"DATAX"}
    dms_types = {"DMS"}
    datasync_types = {"DATASYNC"}
    aliyun_serverless_spark_types = {"ALIYUN_SERVERLESS_SPARK"}
    blocking_types = {"BLOCKING"}
    chunjun_types = {"CHUNJUN"}
    grpc_types = {"GRPC"}
    java_types = {"JAVA"}
    k8s_types = {"K8S"}
    mr_types = {"MR"}
    dynamic_types = {"DYNAMIC"}
    waterdrop_types = {"WATERDROP"}
    data_quality_types = {"DATA_QUALITY"}
    kubeflow_types = {"KUBEFLOW"}
    pytorch_types = {"PYTORCH"}
    seatunnel_types = {"SEATUNNEL"}
    expected_memberships = {
        "1.3.9": {
            "CONDITIONS",
            "DATAX",
            "DEPENDENT",
            "HTTP",
            "MR",
            "PROCEDURE",
            "PYTHON",
            "SHELL",
            "SQL",
            "SQOOP",
            "SUB_WORKFLOW",
        },
        "2.0.0": pigeon_legacy_types | datax_types | mr_types,
        "2.0.9": pigeon_legacy_types | datax_types | mr_types | waterdrop_types,
        "3.0.0": flink_spark_emr_pigeon_legacy_types
        | blocking_types
        | data_quality_types
        | datax_types
        | mr_types
        | seatunnel_types,
        "3.0.6": flink_spark_emr_pigeon_legacy_types
        | blocking_types
        | data_quality_types
        | datax_types
        | mr_types
        | seatunnel_types,
        "3.1.0": spark_hivecli_emr_pigeon_legacy_types
        | blocking_types
        | chunjun_types
        | data_quality_types
        | mr_types
        | pytorch_types
        | seatunnel_types,
        "3.1.9": flink_spark_hivecli_emr_pigeon_legacy_types
        | blocking_types
        | chunjun_types
        | data_quality_types
        | datax_types
        | k8s_types
        | mr_types
        | pytorch_types
        | {"FLINK_STREAM"}
        | seatunnel_types,
        "3.2.0": flink_spark_pigeon_modern_types
        | blocking_types
        | chunjun_types
        | data_factory_types
        | data_quality_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | mr_types
        | pytorch_types
        | {"FLINK_STREAM"}
        | seatunnel_types,
        "3.2.1": flink_spark_pigeon_modern_types
        | blocking_types
        | chunjun_types
        | data_factory_types
        | data_quality_types
        | datasync_types
        | datax_types
        | dms_types
        | k8s_types
        | kubeflow_types
        | mr_types
        | pytorch_types
        | {"FLINK_STREAM"}
        | seatunnel_types,
        "3.2.2": flink_spark_pigeon_modern_types
        | blocking_types
        | chunjun_types
        | data_factory_types
        | data_quality_types
        | datasync_types
        | datax_types
        | dms_types
        | dynamic_types
        | java_types
        | k8s_types
        | kubeflow_types
        | mr_types
        | pytorch_types
        | {"FLINK_STREAM"}
        | seatunnel_types,
        "3.3.1": flink_spark_modern_types
        | chunjun_types
        | data_factory_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | aliyun_serverless_spark_types
        | mr_types
        | pytorch_types
        | seatunnel_types,
        "3.3.2": flink_spark_modern_types
        | chunjun_types
        | data_factory_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | aliyun_serverless_spark_types
        | mr_types
        | pytorch_types
        | seatunnel_types,
        "3.4.0": flink_spark_modern_types
        | chunjun_types
        | data_factory_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | aliyun_serverless_spark_types
        | grpc_types
        | mr_types
        | seatunnel_types,
        "3.4.1": flink_spark_modern_types
        | chunjun_types
        | data_factory_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | aliyun_serverless_spark_types
        | grpc_types
        | mr_types
        | seatunnel_types,
        "3.4.2": flink_serverless_modern_types
        | chunjun_types
        | data_factory_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | aliyun_serverless_spark_types
        | grpc_types
        | mr_types
        | seatunnel_types,
        "3.4.3": flink_serverless_modern_types
        | chunjun_types
        | data_factory_types
        | datasync_types
        | datax_types
        | dms_types
        | java_types
        | k8s_types
        | kubeflow_types
        | aliyun_serverless_spark_types
        | grpc_types
        | mr_types
        | seatunnel_types,
    }

    for version in (
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
    ):
        expected_memberships[version] = expected_memberships["2.0.9"]
    for version in ("3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5"):
        expected_memberships[version] = expected_memberships["3.0.6"]
    for version, holes in {
        "3.1.1": {"FLINK", "FLINK_STREAM", "K8S", "SAGEMAKER"},
        "3.1.2": {"FLINK_STREAM", "K8S", "OPENMLDB", "SAGEMAKER"},
        "3.1.3": {"FLINK_STREAM", "K8S"},
        "3.1.4": {"FLINK_STREAM"},
        "3.1.5": set(),
        "3.1.6": set(),
        "3.1.7": set(),
        "3.1.8": set(),
    }.items():
        expected_memberships[version] = expected_memberships["3.1.9"] - holes

    actual_memberships = {
        version: set(profile["typed_authoring_reviews"])
        for version, profile in data["profiles"].items()
    }
    assert actual_memberships == expected_memberships
    assert [
        len(actual_memberships[version]) for version in _EXPECTED_PROFILE_VERSIONS
    ] == [
        11,
        13,
        14,
        14,
        14,
        14,
        14,
        14,
        14,
        14,
        14,
        20,
        20,
        20,
        20,
        20,
        20,
        20,
        27,
        27,
        27,
        29,
        30,
        31,
        31,
        31,
        31,
        31,
        37,
        36,
        38,
        34,
        34,
        34,
        34,
        35,
        35,
    ]
    assert sum(len(memberships) for memberships in actual_memberships.values()) == 902
    assert len(set().union(*actual_memberships.values())) == 42

    actual_exclusions = {
        version: set(profile.get("typed_authoring_exclusions", {}))
        for version, profile in data["profiles"].items()
        if profile.get("typed_authoring_exclusions")
    }
    assert actual_exclusions == {
        "2.0.0": {"WATERDROP"},
        "3.1.0": {"DATAX", "K8S"},
        "3.1.1": {"FLINK", "FLINK_STREAM", "K8S", "SAGEMAKER"},
        "3.1.2": {"FLINK_STREAM", "K8S", "OPENMLDB", "SAGEMAKER"},
        "3.1.3": {"FLINK_STREAM", "K8S"},
        "3.1.4": {"FLINK_STREAM"},
        "3.2.0": {"DYNAMIC", "LINKIS"},
        "3.2.1": {"DYNAMIC", "JAVA", "LINKIS"},
        "3.2.2": {"LINKIS"},
        "3.3.1": {"LINKIS"},
        "3.3.2": {"LINKIS"},
        "3.4.0": {"LINKIS"},
        "3.4.1": {"LINKIS"},
        "3.4.2": {"LINKIS"},
        "3.4.3": {"LINKIS"},
    }

    decided_source_types = {
        review.get("source_task_type", task_type)
        for profile in data["profiles"].values()
        for task_type, review in profile["typed_authoring_reviews"].items()
    }
    decided_source_types.update(
        task_type
        for profile in data["profiles"].values()
        for task_type in profile.get("typed_authoring_exclusions", {})
    )
    fully_unreviewed_by_type = {
        task_type: sum(
            task_type in profile["task_types"] for profile in data["profiles"].values()
        )
        for task_type in sorted(
            set().union(
                *(set(profile["task_types"]) for profile in data["profiles"].values())
            )
            - decided_source_types
        )
    }
    assert fully_unreviewed_by_type == {}
    assert sum(fully_unreviewed_by_type.values()) == 0

    # Presence alone remains insufficient: opaque native plugins outside the
    # current canonical model set do not become typed merely by discovery.
    assert "DATAX" in data["profiles"]["3.1.0"]["task_types"]
    assert "DATAX" not in data["profiles"]["3.1.0"]["typed_authoring_reviews"]
    assert "DATAX" in data["profiles"]["3.1.0"]["typed_authoring_exclusions"]

    stable_facts = data["profiles"]["3.4.1"]["task_types"]
    for profile in data["profiles"].values():
        for task_type, review in profile["typed_authoring_reviews"].items():
            if review["review"] != "3.4.1-fp-match":
                continue
            source_task_type = review.get("source_task_type", task_type)
            assert (
                profile["task_types"][source_task_type]["semantic_fingerprint"]
                == stable_facts[task_type]["semantic_fingerprint"]
            )


def test_legacy_sub_process_review_projects_to_canonical_sub_workflow(
    compiled_task_profiles: dict[str, Any],
) -> None:
    data = compiled_task_profiles

    for version in (
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
    ):
        profile = data["profiles"][version]
        assert "SUB_PROCESS" in profile["task_types"]
        assert "SUB_WORKFLOW" not in profile["task_types"]
        assert "SUB_PROCESS" not in profile["typed_authoring_reviews"]
        review = profile["typed_authoring_reviews"]["SUB_WORKFLOW"]
        assert review["source_task_type"] == "SUB_PROCESS"
        assert review["cli_model"] == "SubWorkflowTaskParamsSpec"
        assert (
            review["semantic_fingerprint"]
            == (profile["task_types"]["SUB_PROCESS"]["semantic_fingerprint"])
        )


def test_rendered_profiles_preserve_named_facts_and_source_task_aliases(
    tmp_path: Path,
    compiled_task_profiles: dict[str, Any],
) -> None:
    output = tmp_path / "task_profiles.py"
    output.write_text(
        _module().render_task_profile_data(compiled_task_profiles), encoding="utf-8"
    )
    loaded = runpy.run_path(str(output))["TASK_PROFILES"]
    assert loaded == compiled_task_profiles["profiles"]
    procedure = loaded["1.3.9"]["typed_authoring_reviews"]["PROCEDURE"]
    assert "source_task_type" not in procedure
    child_workflow = loaded["2.0.0"]["typed_authoring_reviews"]["SUB_WORKFLOW"]
    assert child_workflow["source_task_type"] == "SUB_PROCESS"
    assert child_workflow["cli_model"] == "SubWorkflowTaskParamsSpec"
    dvc = loaded["3.4.2"]["task_types"]["DVC"]
    assert set(dvc) == {
        "parameter_model_import",
        "semantic_fingerprint",
        "registration_kind",
    }
    assert dvc["registration_kind"] == "spi_factory"
    loaded["3.4.1"]["task_types"]["DVC"]["registration_kind"] = "changed"
    assert dvc["registration_kind"] == "spi_factory"


@pytest.mark.parametrize(
    ("version", "task_type", "evidence_plane"),
    [
        ("3.1.0", "K8S", "watcher_runtime_hole"),
        ("3.1.0", "DATAX", "parameter_preparation"),
        ("3.2.1", "JAVA", "broken_executor"),
        ("3.2.0", "DYNAMIC", "broken_child_tenant_flow"),
        ("3.2.1", "DYNAMIC", "broken_child_tenant_and_start_params"),
    ],
)
def test_rendered_exclusions_preserve_reviewed_reason_without_evidence_prose(
    tmp_path: Path,
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
    compiled_task_profiles: dict[str, Any],
    version: str,
    task_type: str,
    evidence_plane: str,
) -> None:
    _, reviews = task_profile_documents
    source = _module().render_task_profile_data(compiled_task_profiles)
    output = tmp_path / "task_profiles.py"
    output.write_text(source, encoding="utf-8")
    profiles = runpy.run_path(str(output))["TASK_PROFILES"]
    review = reviews["profiles"][version]["typed_authoring_exclusions"][task_type]
    exclusion = profiles[version]["typed_authoring_exclusions"][task_type]
    assert exclusion["reason"] == review["reason"]
    assert (
        exclusion["semantic_fingerprint"]
        == (profiles[version]["task_types"][task_type]["semantic_fingerprint"])
    )
    assert review["evidence_paths"][evidence_plane][0] not in source
    excluded_versions = {
        "2.0.0",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
    assert {
        version
        for version, profile in profiles.items()
        if "typed_authoring_exclusions" in profile
    } == excluded_versions


def test_task_profile_renderer_rejects_unowned_future_fields(
    compiled_task_profiles: dict[str, Any],
) -> None:
    task_profiles = _module()
    data = copy.deepcopy(compiled_task_profiles)
    data["profiles"]["3.4.2"]["task_types"]["DVC"]["future_fact"] = True

    with pytest.raises(ValueError, match="unsupported task-fact fields: future_fact"):
        task_profiles.render_task_profile_data(data)


def test_review_cli_task_type_alias_must_be_unique_and_canonical(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, baseline_reviews = task_profile_documents
    reviews = copy.deepcopy(baseline_reviews)
    aliased = copy.deepcopy(reviews["profiles"]["2.0.0"]["typed_authoring"])
    aliased["SHELL"]["cli_task_type"] = "SUB_WORKFLOW"
    reviews["profiles"]["2.0.0"]["typed_authoring"] = aliased

    with pytest.raises(
        ValueError,
        match=r"2\.0\.0 repeats CLI task type SUB_WORKFLOW",
    ):
        task_profiles.compile_task_profile_data(facts, reviews)

    reviews = copy.deepcopy(baseline_reviews)
    reviews["profiles"]["2.0.0"]["typed_authoring"]["SUB_PROCESS"]["cli_task_type"] = (
        "sub_workflow"
    )
    with pytest.raises(ValueError, match=r"cli_task_type must be canonical"):
        task_profiles.compile_task_profile_data(facts, reviews)


def test_reviewed_source_coordinate_removal_fails_closed(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    baseline_facts, reviews = task_profile_documents
    facts = copy.deepcopy(baseline_facts)
    facts["profiles"]["3.4.2"]["task_types"].pop("GRPC")

    with pytest.raises(
        ValueError,
        match=r"3\.4\.2\.GRPC has no exact upstream task fact",
    ):
        task_profiles.compile_task_profile_data(facts, reviews)


def test_review_fingerprint_drift_fails_closed(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, baseline_reviews = task_profile_documents
    reviews = copy.deepcopy(baseline_reviews)
    reviews["profiles"]["3.4.2"]["typed_authoring"]["SQL"]["semantic_fingerprint"] = (
        "sha256:" + "0" * 64
    )

    with pytest.raises(ValueError, match=r"3\.4\.2\.SQL fingerprint is stale"):
        task_profiles.compile_task_profile_data(
            facts,
            reviews,
        )


def test_typed_review_still_requires_generator_only_evidence(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    facts, reviews = copy.deepcopy(task_profile_documents)
    reviews["profiles"]["3.4.2"]["typed_authoring"]["SQL"].pop("evidence")

    with pytest.raises(
        ValueError,
        match=r"3\.4\.2\.SQL\.evidence must be a non-empty string",
    ):
        _module().compile_task_profile_data(facts, reviews)


@pytest.mark.parametrize(
    ("case", "match"),
    [
        ("missing-fact", r"3\.1\.0\.MISSING has no exact upstream task fact"),
        ("noncanonical", r"3\.1\.0 task type must be canonical"),
        ("stale-fingerprint", r"3\.1\.0\.K8S fingerprint is stale"),
        ("blank-reason", r"3\.1\.0\.K8S\.reason must be a non-empty string"),
        ("blank-evidence", r"3\.1\.0\.K8S\.evidence must be a non-empty string"),
        (
            "missing-evidence-paths",
            r"3\.1\.0\.K8S requires structured evidence paths",
        ),
        (
            "positive-overlap",
            r"3\.1\.9\.K8S overlaps a positive typed-authoring review",
        ),
    ],
)
def test_typed_authoring_exclusions_fail_closed_without_exact_negative_evidence(
    case: str,
    match: str,
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = copy.deepcopy(task_profile_documents)
    exclusions = reviews["profiles"]["3.1.0"]["typed_authoring_exclusions"]
    exclusion = exclusions["K8S"]
    if case == "missing-fact":
        exclusions["MISSING"] = exclusions.pop("K8S")
    elif case == "noncanonical":
        exclusions["k8s"] = exclusions.pop("K8S")
    elif case == "stale-fingerprint":
        exclusion["semantic_fingerprint"] = "sha256:" + "0" * 64
    elif case == "blank-reason":
        exclusion["reason"] = " "
    elif case == "blank-evidence":
        exclusion["evidence"] = ""
    elif case == "missing-evidence-paths":
        exclusion.pop("evidence_paths")
    elif case == "positive-overlap":
        reviews["profiles"]["3.1.9"]["typed_authoring_exclusions"] = {
            "K8S": copy.deepcopy(exclusion)
        }
    else:  # pragma: no cover - the parametrization is closed above
        raise AssertionError(case)

    with pytest.raises(ValueError, match=match):
        task_profiles.compile_task_profile_data(facts, reviews)


def test_typed_authoring_exclusion_paths_use_exact_source_validation(
    tmp_path: Path,
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = copy.deepcopy(task_profile_documents)
    exclusion = reviews["profiles"]["3.1.0"]["typed_authoring_exclusions"]["K8S"]
    reviews["profiles"]["3.1.0"]["typed_authoring"] = {}
    reviews["profiles"]["3.1.0"]["typed_authoring_exclusions"] = {"K8S": exclusion}

    with pytest.raises(ValueError, match=r"3\.1\.0\.K8S evidence path does not exist"):
        task_profiles.compile_task_profile_data(
            facts,
            reviews,
            source_roots={"3.1.0": tmp_path},
        )

    exclusion["evidence_paths"] = {"watcher_runtime_hole": ["../K8sTaskExecutor.java"]}
    with pytest.raises(
        ValueError,
        match=r"evidence path must be normalized and repository-relative",
    ):
        task_profiles.compile_task_profile_data(facts, reviews)


def test_review_source_tree_drift_fails_closed(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = copy.deepcopy(task_profile_documents)
    reviews["profiles"]["3.4.2"]["source_tree"] = "0" * 40

    with pytest.raises(ValueError, match=r"3\.4\.2 source tree is stale"):
        task_profiles.compile_task_profile_data(facts, reviews)


@pytest.mark.parametrize(
    ("case", "match"),
    [
        ("container", r"requires structured evidence paths"),
        ("plane", r"requires structured evidence paths"),
        ("path", r"evidence plane 'server_parameter_model' requires source paths"),
    ],
)
def test_typed_review_evidence_removal_fails_closed(
    case: str,
    match: str,
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = copy.deepcopy(task_profile_documents)
    review = reviews["profiles"]["2.0.0"]["typed_authoring"]["SHELL"]
    if case == "container":
        review.pop("evidence_paths")
    elif case == "plane":
        review["evidence_paths"].pop("server_parameter_model")
    elif case == "path":
        review["evidence_paths"]["server_parameter_model"].clear()
    else:  # pragma: no cover - the parametrization is closed above
        raise AssertionError(case)

    with pytest.raises(ValueError, match=match):
        task_profiles.compile_task_profile_data(facts, reviews)


def test_evidence_paths_must_exist_when_exact_source_root_is_supplied(
    tmp_path: Path,
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = copy.deepcopy(task_profile_documents)
    reviews["profiles"]["3.4.2"]["typed_authoring"] = {
        "DVC": reviews["profiles"]["3.4.2"]["typed_authoring"]["DVC"]
    }

    with pytest.raises(
        ValueError,
        match=r"3\.4\.2\.DVC evidence path does not exist",
    ):
        task_profiles.compile_task_profile_data(
            facts,
            reviews,
            source_roots={"3.4.2": tmp_path},
        )


def test_evidence_paths_must_be_normalized_and_repository_relative(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = copy.deepcopy(task_profile_documents)
    reviews["profiles"]["3.4.2"]["typed_authoring"]["SQL"]["evidence_paths"] = {
        "server_parameter_model": ["../SqlParameters.java"],
    }

    with pytest.raises(
        ValueError,
        match=r"evidence path must be normalized and repository-relative",
    ):
        task_profiles.compile_task_profile_data(facts, reviews)


@pytest.mark.source_contract
def test_exact_local_sources_satisfy_all_review_evidence_paths(
    exact_contract_corpus: ExactContractCorpus,
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, reviews = task_profile_documents
    source_roots = {
        version: exact_contract_corpus.source_root(version)
        for version in exact_contract_corpus.versions
    }

    data = task_profiles.compile_task_profile_data(
        facts,
        reviews,
        source_roots=source_roots,
    )

    assert tuple(data["profiles"]) == _EXPECTED_PROFILE_VERSIONS


def test_review_source_tree_and_evidence_paths_are_generator_only(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
    compiled_task_profiles: dict[str, Any],
) -> None:
    task_profiles = _module()
    _, reviews = task_profile_documents
    data = compiled_task_profiles
    rendered = task_profiles.render_task_profile_data(data)
    sql_evidence_path = reviews["profiles"]["3.4.2"]["typed_authoring"]["SQL"][
        "evidence_paths"
    ]["server_parameter_model"][0]

    assert "source_tree" not in data["profiles"]["3.4.2"]
    assert (
        "evidence_paths"
        not in data["profiles"]["3.4.2"]["typed_authoring_reviews"]["SQL"]
    )
    assert sql_evidence_path not in rendered


def test_runtime_decisions_keep_exact_review_links_without_evidence_prose(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
    compiled_task_profiles: dict[str, Any],
) -> None:
    task_profiles = _module()
    _, reviews = task_profile_documents
    for version, profile in compiled_task_profiles["profiles"].items():
        source_profile = reviews["profiles"][version]
        for task_type, review in profile["typed_authoring_reviews"].items():
            source_task_type = review.get("source_task_type", task_type)
            source_review = source_profile["typed_authoring"][source_task_type]
            expected_review = {
                field: source_review[field]
                for field in ("review", "cli_model", "semantic_fingerprint")
            }
            if source_task_type != task_type:
                expected_review["source_task_type"] = source_task_type
            assert review == expected_review
        for task_type, exclusion in profile.get(
            "typed_authoring_exclusions", {}
        ).items():
            source_exclusion = source_profile["typed_authoring_exclusions"][task_type]
            assert exclusion == {
                field: source_exclusion[field]
                for field in ("semantic_fingerprint", "reason")
            }

    facts, revised_reviews = copy.deepcopy(task_profile_documents)
    for profile in revised_reviews["profiles"].values():
        for decision in (
            *profile["typed_authoring"].values(),
            *profile.get("typed_authoring_exclusions", {}).values(),
        ):
            decision["evidence"] = "Revised evidence prose retained only in the ledger."
    revised = task_profiles.compile_task_profile_data(facts, revised_reviews)
    assert revised == compiled_task_profiles
    assert task_profiles.render_task_profile_data(revised) == (
        task_profiles.render_task_profile_data(compiled_task_profiles)
    )


def test_transitional_control_task_inventory_fails_closed_when_incomplete(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    facts, _ = task_profile_documents
    inventory = _inventory_from_facts(facts)
    target = next(item for item in inventory["targets"] if item["label"] == "3.2.2")
    target["task_types"] = [
        item for item in target["task_types"] if item["key"] != "DYNAMIC"
    ]
    target["counts"]["task_types"] -= 1

    with pytest.raises(
        ValueError,
        match=r"3\.2\.2 control task inventory is incomplete: DYNAMIC",
    ):
        task_profiles.project_task_profile_facts(inventory)


def test_generated_task_profiles_are_fresh_readable_and_importable(
    compiled_task_profiles: dict[str, Any],
) -> None:
    task_profiles = _module()
    expected = compiled_task_profiles
    rendered = task_profiles.render_task_profile_data(expected)
    marker = "_TASK_PROFILE_JSON = r'''\n"
    payload = rendered.split(marker, maxsplit=1)[1].split("\n'''", maxsplit=1)[0]
    root = Path(__file__).resolve().parents[2]
    generated_path = root / task_profiles.GENERATED_TASK_PROFILE_PATH

    assert json.loads(payload) == expected
    assert '\n  "profiles": {\n    "1.3.9": {' in payload
    assert '"semantic_fingerprint": "sha256:' in payload
    assert '"parameter_model_import":' in payload
    assert "base64" not in rendered
    assert "_POOL" not in rendered
    assert generated_path.read_text(encoding="utf-8") == rendered
    loaded = runpy.run_path(str(generated_path))
    assert loaded["TASK_PROFILE_DATA"] == expected
    assert loaded["TARGET_DS_VERSIONS"] == _EXPECTED_PROFILE_VERSIONS


def test_exact_inventory_projection_is_fresh_when_local_inventory_exists(
    task_profile_documents: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    task_profiles = _module()
    root = Path(__file__).resolve().parents[2]
    inventory_path = root / "build/ds_task_plugins/all-version-inventory.json"
    if not inventory_path.is_file():
        pytest.skip("exact task-plugin inventory is an optional generated workspace")

    projected = task_profiles.project_task_profile_facts(
        task_profiles.load_task_profile_document(inventory_path)
    )
    tracked, _ = task_profile_documents

    assert projected == tracked


def _inventory_from_facts(facts: dict[str, Any]) -> dict[str, Any]:
    targets: list[dict[str, Any]] = []
    for version in facts["target_versions"]:
        profile = facts["profiles"][version]
        task_types = [
            {"key": task_type, **copy.deepcopy(fact)}
            for task_type, fact in profile["task_types"].items()
        ]
        targets.append(
            {
                "label": version,
                "ds_version": version,
                "source_complete": True,
                "contract_fingerprint": profile["contract_fingerprint"],
                "provenance": copy.deepcopy(profile["provenance"]),
                "counts": {
                    "task_types": len(task_types),
                    "models": 0,
                    "enums": 0,
                },
                "task_types": task_types,
            }
        )
    return {
        "schema_version": 1,
        "kind": "dolphinscheduler-task-plugin-contract-inventory",
        "complete": True,
        "source_complete": True,
        "targets": targets,
        "diagnostics": [],
    }


def _attestation_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
