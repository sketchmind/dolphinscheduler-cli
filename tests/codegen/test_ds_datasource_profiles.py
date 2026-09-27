from __future__ import annotations

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

INTERMEDIATE_VERSIONS = (
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
)


def _load_module() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.datasource_profiles")


def _modern_snapshot() -> Any:
    ir = importlib.import_module("ds_codegen.ir")
    return ir.ContractSnapshot(
        ds_version="3.4.2",
        operation_count=0,
        enum_count=1,
        dto_count=0,
        model_count=1,
        operations=[],
        enums=[
            ir.EnumSpec(
                name="DbType",
                import_path="org.apache.dolphinscheduler.spi.enums.DbType",
                documentation=None,
                fields=[],
                json_value_field=None,
                values=[
                    ir.EnumValueSpec(name=name, arguments=[], documentation=None)
                    for name in ("MYSQL", "SSH", "K8S")
                ],
            )
        ],
        dtos=[],
        models=[
            ir.ModelSpec(
                name="BaseDataSourceParamDTO",
                import_path=(
                    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
                    "BaseDataSourceParamDTO"
                ),
                kind="other_class",
                documentation=None,
                extends=None,
                fields=[
                    _field(ir, "name", "String"),
                    _field(ir, "password", "String"),
                    _field(ir, "type", "DbType"),
                ],
            )
        ],
    )


def _legacy_snapshot() -> Any:
    ir = importlib.import_module("ds_codegen.ir")
    return ir.ContractSnapshot(
        ds_version="1.3.9",
        operation_count=1,
        enum_count=1,
        dto_count=0,
        model_count=0,
        operations=[
            ir.OperationSpec(
                operation_id="DataSourceController.updateDataSource",
                controller="org.example.DataSourceController",
                method_name="updateDataSource",
                api_group="data_source",
                http_method="POST",
                path="datasources/update",
                summary=None,
                description=None,
                documentation=None,
                parameter_docs={},
                returns_doc=None,
                consumes=[],
                return_type="Result",
                inferred_return_type=None,
                logical_return_type="Void",
                response_projection="direct",
                parameters=[
                    _parameter(ir, "sessionUser", "request_attribute"),
                    _parameter(ir, "id", "request_param"),
                    _parameter(ir, "name", "request_param"),
                    _parameter(ir, "password", "request_param"),
                    _parameter(ir, "type", "request_param"),
                ],
            )
        ],
        enums=[
            ir.EnumSpec(
                name="DbType",
                import_path="org.apache.dolphinscheduler.common.enums.DbType",
                documentation=None,
                fields=[],
                json_value_field=None,
                values=[
                    ir.EnumValueSpec(
                        name="MYSQL",
                        arguments=[],
                        documentation=None,
                    )
                ],
            )
        ],
        dtos=[],
        models=[],
    )


def _field(ir: Any, name: str, java_type: str) -> Any:
    return ir.DtoFieldSpec(
        name=name,
        java_type=java_type,
        wire_name=name,
        required=None,
        default_value=None,
        nullable=True,
        default_factory=None,
        description=None,
        example=None,
        allowable_values=None,
        documentation=None,
    )


def _parameter(ir: Any, name: str, binding: str) -> Any:
    return ir.ParameterSpec(
        name=name,
        java_type="String",
        binding=binding,
        wire_name=name,
        required=None,
        default_value=None,
        hidden=False,
        description=None,
        example=None,
        allowable_values=None,
        schema_type=None,
    )


def test_compiler_combines_exact_closure_with_reviewed_plugin_facets() -> None:
    profiles = _load_module()

    profile = profiles.compile_datasource_profile(
        _modern_snapshot(),
        plugin_fields_by_type={
            "SSH": ("privateKey",),
            "K8S": ("kubeConfig", "namespace"),
        },
    )

    assert profile.type_names == ("MYSQL", "SSH", "K8S")
    assert profile.type_aliases == {
        "MYSQL": "MYSQL",
        "SSH": "SSH",
        "K8S": "K8S",
    }
    assert profile.base_field_names == ("name", "password", "type")
    assert profile.plugin_fields_by_type == {
        "SSH": ("privateKey",),
        "K8S": ("kubeConfig", "namespace"),
    }
    assert profile.sensitive_field_names == (
        "password",
        "privateKey",
        "kubeConfig",
    )


def test_legacy_base_fields_come_from_exact_update_form() -> None:
    profiles = _load_module()

    profile = profiles.compile_datasource_profile(
        _legacy_snapshot(),
        plugin_fields_by_type={},
    )

    assert profile.base_field_names == ("id", "name", "password", "type")
    assert profile.sensitive_field_names == ("password",)


def test_compiler_fails_closed_when_reviews_reference_an_absent_db_type() -> None:
    profiles = _load_module()

    with pytest.raises(ValueError, match="unknown DbType"):
        profiles.compile_datasource_profile(
            _modern_snapshot(),
            plugin_fields_by_type={"ORACLE": ("connectType",)},
        )


def test_tracked_reviews_cover_all_exact_profiles_and_ssh_rename() -> None:
    profiles = _load_module()

    reviews = profiles.load_datasource_profile_reviews()

    assert tuple(reviews) == (
        "1.3.9",
        "2.0.0",
        *INTERMEDIATE_VERSIONS[:8],
        "2.0.9",
        "3.0.0",
        *INTERMEDIATE_VERSIONS[8:13],
        "3.0.6",
        "3.1.0",
        *INTERMEDIATE_VERSIONS[13:],
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
    assert reviews["3.3.2"]["SSH"] == ("publicKey",)
    assert reviews["3.4.0"]["SSH"] == ("privateKey",)
    assert reviews["3.4.3"]["SSH"] == ("privateKey",)


def test_renderer_materializes_importable_canonical_profile_data(
    tmp_path: Path,
) -> None:
    profiles = _load_module()
    profile = profiles.compile_datasource_profile(
        _modern_snapshot(),
        plugin_fields_by_type={
            "SSH": ("privateKey",),
            "K8S": ("kubeConfig", "namespace"),
        },
    )

    output = profiles.write_datasource_profiles(tmp_path, (profile,))
    generated = runpy.run_path(str(output))

    assert generated["DATASOURCE_PROFILE_SCHEMA_VERSION"] == 1
    assert generated["TARGET_DATASOURCE_VERSIONS"] == ("3.4.2",)
    assert generated["DATASOURCE_PROFILES"]["3.4.2"] == {
        "type_names": ["MYSQL", "SSH", "K8S"],
        "type_aliases": {"MYSQL": "MYSQL", "SSH": "SSH", "K8S": "K8S"},
        "base_field_names": ["name", "password", "type"],
        "plugin_fields_by_type": {
            "SSH": ["privateKey"],
            "K8S": ["kubeConfig", "namespace"],
        },
        "sensitive_field_names": ["password", "privateKey", "kubeConfig"],
    }
    assert "do not edit" in output.read_text(encoding="utf-8")


@pytest.mark.source_contract
@pytest.mark.parametrize("version", INTERMEDIATE_VERSIONS)
def test_intermediate_datasource_review_matches_exact_source_and_profile(
    exact_contract_corpus: ExactContractCorpus, version: str
) -> None:
    profiles = _load_module()
    document = json.loads(profiles.DEFAULT_DATASOURCE_PROFILE_REVIEWS.read_text())
    evidence = document["evidence"]["intermediate_releases"]
    reviewed_source = evidence["sources"][version]
    source_root = exact_contract_corpus.source_root(version)
    files = sorted(
        path
        for path in (source_root / evidence["dto_root"]).rglob("*ParamDTO.java")
        if "datasource" in path.name.lower()
    )
    file_digests = [
        [
            path.relative_to(source_root).as_posix(),
            hashlib.sha256(path.read_bytes()).hexdigest(),
        ]
        for path in files
    ]
    digest = hashlib.sha256(
        json.dumps(file_digests, separators=(",", ":")).encode()
    ).hexdigest()
    assert len(files) == reviewed_source["dto_file_count"]
    assert "sha256:" + digest == reviewed_source["dto_files_digest"]
    assert reviewed_source["tag"] == version
    ledger = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "tools/ds_codegen/version_profile_decisions.json"
        ).read_text()
    )
    assert {key: reviewed_source[key] for key in ("tag", "commit", "tree")} == ledger[
        "sources"
    ][version]

    snapshot = exact_contract_corpus.snapshot(version)
    reviews = profiles.load_datasource_profile_reviews()
    profile = profiles.compile_datasource_profile(
        snapshot, plugin_fields_by_type=reviews[version]
    )
    assert profile.version == version
    assert profile.plugin_fields_by_type["HIVE"] == (
        "principal",
        "javaSecurityKrb5Conf",
        "loginUserKeytabUsername",
        "loginUserKeytabPath",
    )
    assert (
        profile.plugin_fields_by_type["SPARK"] == profile.plugin_fields_by_type["HIVE"]
    )
    assert profile.plugin_fields_by_type["ORACLE"] == ("connectType",)
    if version in INTERMEDIATE_VERSIONS[13:]:
        assert document["versions"][version] == "athena"
        assert profile.plugin_fields_by_type["ATHENA"] == ("awsRegion",)
    else:
        assert document["versions"][version] == "hdfs-oracle"
        assert "ATHENA" not in profile.plugin_fields_by_type
    model_imports = {model.import_path for model in snapshot.models} | {
        dto.import_path for dto in snapshot.dtos
    }
    assert (
        "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
        "BaseDataSourceParamDTO"
    ) in model_imports
