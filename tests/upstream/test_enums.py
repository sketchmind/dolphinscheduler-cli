import pytest

from dsctl.errors import ConfigError
from dsctl.upstream import (
    datasource_base_payload_fields,
    datasource_payload_contract,
    datasource_payload_field_names,
    datasource_sensitive_payload_fields,
    datasource_type_names,
    get_enum_spec,
    normalize_datasource_type,
    supported_enum_names,
)


def test_supported_enum_names_are_sorted_and_stable() -> None:
    names = supported_enum_names("3.4.1")

    assert names == tuple(sorted(names))
    assert "priority" in names
    assert "resource-type" in names
    assert "workflow-execution-status" in names


@pytest.mark.parametrize(
    ("version", "expected_count"),
    [
        ("1.3.9", 19),
        ("2.0.0", 23),
        ("2.0.9", 23),
        ("3.0.0", 28),
        ("3.0.6", 28),
        ("3.1.0", 30),
        ("3.1.9", 30),
        ("3.2.0", 32),
        ("3.2.1", 33),
        ("3.2.2", 32),
        ("3.3.1", 28),
        ("3.3.2", 28),
        ("3.4.0", 28),
        ("3.4.1", 28),
        ("3.4.2", 28),
    ],
)
def test_every_profile_exposes_its_complete_exact_enum_catalog(
    version: str,
    expected_count: int,
) -> None:
    assert len(supported_enum_names(version)) == expected_count


def test_enum_evolution_is_not_projected_from_the_stable_profile() -> None:
    legacy_data_type = get_enum_spec("1.3.9", "data-type")
    current_data_type = get_enum_spec("3.4.2", "data-type")

    assert legacy_data_type is not None
    assert current_data_type is not None
    assert [member.name for member in legacy_data_type.members] == [
        "VARCHAR",
        "INTEGER",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "BOOLEAN",
    ]
    assert [member.name for member in current_data_type.members][-2:] == [
        "LIST",
        "FILE",
    ]


def test_nested_public_task_enum_tracks_upstream_versions() -> None:
    assert get_enum_spec("3.1.9", "dependent-failure-policy") is None

    spec = get_enum_spec("3.2.0", "dependent-failure-policy")

    assert spec is not None
    assert spec.module == "plugin.task_api.parameters.dependent_parameters"
    assert spec.class_name == "DependentParametersDependentFailurePolicyEnum"
    assert [member.name for member in spec.members] == [
        "DEPENDENT_FAILURE_FAILURE",
        "DEPENDENT_FAILURE_WAITING",
    ]
    assert get_enum_spec("3.2.2", "dependent-failure-policy") == spec
    assert get_enum_spec("3.3.1", "dependent-failure-policy") is None


def test_get_enum_spec_resolves_aliases_and_member_attributes() -> None:
    spec = get_enum_spec("3.4.1", "DbType")

    assert spec is not None
    assert spec.name == "db-type"
    assert spec.module == "spi.enums.db_type"
    assert spec.class_name == "DbType"
    assert spec.value_type == "string"
    assert spec.members[0].name == "MYSQL"
    assert spec.members[0].attributes == {
        "code": 0,
        "descp": "mysql",
        "name_field": "mysql",
    }


def test_worker_group_source_remains_in_the_public_exact_enum_catalog() -> None:
    assert "worker-group-source" not in supported_enum_names("3.2.2")
    assert "worker-group-source" in supported_enum_names("3.3.1")

    spec = get_enum_spec("3.4.2", "WorkerGroupSource")

    assert spec is not None
    assert spec.name == "worker-group-source"
    assert spec.module == "common.enums.worker_group_source"
    assert spec.class_name == "WorkerGroupSource"
    assert [(member.name, member.value) for member in spec.members] == [
        ("CONFIG", "CONFIG"),
        ("UI", "UI"),
    ]
    assert [member.attributes for member in spec.members] == [
        {"code": 1, "desc": "config"},
        {"code": 2, "desc": "ui"},
    ]


def test_alert_type_remains_in_the_public_exact_enum_catalog() -> None:
    assert "alert-type" in supported_enum_names("1.3.9")
    assert "alert-type" not in supported_enum_names("2.0.0")

    spec = get_enum_spec("1.3.9", "AlertType")

    assert spec is not None
    assert spec.module == "common.enums.alert_type"
    assert [(member.name, member.value) for member in spec.members] == [
        ("EMAIL", "EMAIL"),
        ("SMS", "SMS"),
    ]
    assert [member.attributes for member in spec.members] == [
        {"code": 0, "descp": "email"},
        {"code": 1, "descp": "SMS"},
    ]


def test_datasource_contract_exposes_db_type_and_payload_fields() -> None:
    assert datasource_type_names("3.4.1")[:3] == (
        "MYSQL",
        "POSTGRESQL",
        "HIVE",
    )
    assert normalize_datasource_type("3.4.1", "mysql") == "MYSQL"
    assert (
        normalize_datasource_type("3.4.1", "aliyun-serverless-spark")
        == "ALIYUN_SERVERLESS_SPARK"
    )
    assert (
        normalize_datasource_type("3.4.1", "aliyun serverless spark")
        == "ALIYUN_SERVERLESS_SPARK"
    )

    fields = datasource_base_payload_fields("3.4.1")
    assert [field.name for field in fields] == [
        "id",
        "name",
        "note",
        "host",
        "port",
        "database",
        "userName",
        "password",
        "other",
        "type",
    ]
    type_field = fields[-1]
    assert type_field.name == "type"
    assert type_field.cli_required is True
    assert "MYSQL" in type_field.choices


def test_datasource_contract_rejects_unreviewed_versions() -> None:
    with pytest.raises(ConfigError, match="Unsupported datasource contract version"):
        datasource_payload_contract("9.9.9")


@pytest.mark.parametrize(
    ("version", "present", "absent"),
    [
        ("1.3.9", "H2", "PRESTO"),
        ("2.0.0", "PRESTO", "REDSHIFT"),
        ("3.0.0", "REDSHIFT", "ATHENA"),
        ("3.1.0", "ATHENA", "TRINO"),
        ("3.2.0", "SSH", "SAGEMAKER"),
        ("3.2.1", "SAGEMAKER", "K8S"),
        ("3.3.1", "K8S", "UNKNOWN"),
        ("3.4.2", "DOLPHINDB", "UNKNOWN"),
        ("3.4.3", "DOLPHINDB", "UNKNOWN"),
    ],
)
def test_datasource_type_catalog_tracks_exact_introduction_boundaries(
    version: str,
    present: str,
    absent: str,
) -> None:
    types = datasource_type_names(version)

    assert present in types
    assert absent not in types


def test_datasource_plugin_fields_track_ssh_wire_rename() -> None:
    assert "publicKey" in datasource_payload_field_names("3.3.2", "SSH")
    assert "privateKey" not in datasource_payload_field_names("3.3.2", "SSH")
    assert "privateKey" in datasource_payload_field_names("3.4.0", "SSH")
    assert "publicKey" not in datasource_payload_field_names("3.4.0", "SSH")


def test_datasource_sensitive_fields_cover_every_plugin_secret() -> None:
    assert datasource_sensitive_payload_fields("1.3.9") == ("password",)
    assert set(datasource_sensitive_payload_fields("3.3.2")) == {
        "password",
        "publicKey",
        "accessKeySecret",
        "kubeConfig",
    }
    assert set(datasource_sensitive_payload_fields("3.4.2")) == {
        "password",
        "privateKey",
        "accessKeySecret",
        "kubeConfig",
    }
