from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

from dsctl.errors import UserInputError
from dsctl.upstream import (
    DATASOURCE_CONTRACT_VERSION,
    datasource_base_payload_fields,
    datasource_type_names,
    normalize_datasource_type,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.yaml_io import JsonObject, JsonValue


DATASOURCE_TYPE_ENUM = "db-type"
DEFAULT_DATASOURCE_TEMPLATE_TYPE = "MYSQL"
DATASOURCE_TEMPLATE_DISCOVERY_COMMAND = "dsctl template datasource"
DATASOURCE_TEMPLATE_COMMAND = "dsctl template datasource --type MYSQL"
DATASOURCE_TEMPLATE_COMMAND_PATTERN = "dsctl template datasource --type TYPE"
DATASOURCE_TEMPLATE_TARGET_COMMANDS = (
    "dsctl datasource create --file FILE",
    "dsctl datasource update DATASOURCE --file FILE",
)
DATASOURCE_TYPE_DISCOVERY_COMMAND = f"dsctl enum list {DATASOURCE_TYPE_ENUM}"
DATASOURCE_PAYLOAD_REVIEW_SUGGESTION = (
    "Review the DS-native JSON payload, or run `dsctl template datasource` "
    "to choose a type and `dsctl template datasource --type TYPE` to generate "
    "a skeleton."
)


class DataSourcePayloadTemplateData(TypedDict):
    """One concrete datasource payload template."""

    type: str
    target_commands: list[str]
    source_option: str
    payload: JsonObject
    json: str
    fields: list[JsonObject]
    rules: list[str]


class DataSourcePayloadTemplateIndexData(TypedDict):
    """Compact datasource payload-template discovery."""

    default_type: str
    template_command: str
    template_command_pattern: str
    target_commands: list[str]
    type_enum: str
    type_discovery_command: str
    supported_types: list[str]


@dataclass(frozen=True)
class _DataSourceTypeExtraField:
    name: str
    value_type: str
    description: str
    example: JsonValue
    choices: tuple[str, ...] = ()

    def to_data(self) -> JsonObject:
        data: JsonObject = {
            "name": self.name,
            "value_type": self.value_type,
            "description": self.description,
            "example": self.example,
        }
        if self.choices:
            data["choices"] = list(self.choices)
        return data


def supported_datasource_template_types() -> tuple[str, ...]:
    """Return datasource types supported by local payload templates."""
    return datasource_type_names(DATASOURCE_CONTRACT_VERSION)


def normalize_datasource_payload_type(datasource_type: str) -> str | None:
    """Normalize one user-provided datasource type against generated DbType."""
    return normalize_datasource_type(DATASOURCE_CONTRACT_VERSION, datasource_type)


def require_datasource_payload_type(datasource_type: str) -> str:
    """Normalize one datasource type or raise a stable user-input error."""
    normalized = normalize_datasource_payload_type(datasource_type)
    if normalized is not None:
        return normalized
    supported = list(supported_datasource_template_types())
    message = f"Unsupported datasource type {datasource_type!r}"
    raise UserInputError(
        message,
        details={
            "type": datasource_type,
            "supported_types": supported,
        },
        suggestion=(
            "Run `dsctl template datasource` to choose a supported datasource "
            "type, then `dsctl template datasource --type TYPE`."
        ),
    )


def datasource_payload_command_data() -> JsonObject:
    """Return compact datasource payload metadata for command schema."""
    return {
        "format": "json",
        "source_option": "--file",
        "target_commands": list(DATASOURCE_TEMPLATE_TARGET_COMMANDS),
        "ds_model": "BaseDataSourceParamDTO",
        "upstream_request_shape": "DataSourceController request body String jsonStr",
        "template_command": DATASOURCE_TEMPLATE_COMMAND,
        "template_command_pattern": DATASOURCE_TEMPLATE_COMMAND_PATTERN,
        "template_discovery_command": DATASOURCE_TEMPLATE_DISCOVERY_COMMAND,
        "template_json_path": "data.json",
        "template_payload_path": "data.payload",
        "type_enum": DATASOURCE_TYPE_ENUM,
        "type_discovery_command": DATASOURCE_TYPE_DISCOVERY_COMMAND,
        "rules": datasource_payload_rules(),
    }


def datasource_template_index_data() -> DataSourcePayloadTemplateIndexData:
    """Return compact datasource template discovery metadata."""
    return DataSourcePayloadTemplateIndexData(
        default_type=DEFAULT_DATASOURCE_TEMPLATE_TYPE,
        template_command=DATASOURCE_TEMPLATE_COMMAND,
        template_command_pattern=DATASOURCE_TEMPLATE_COMMAND_PATTERN,
        target_commands=list(DATASOURCE_TEMPLATE_TARGET_COMMANDS),
        type_enum=DATASOURCE_TYPE_ENUM,
        type_discovery_command=DATASOURCE_TYPE_DISCOVERY_COMMAND,
        supported_types=list(supported_datasource_template_types()),
    )


def datasource_template_data(datasource_type: str) -> DataSourcePayloadTemplateData:
    """Return one DS-native datasource JSON payload template."""
    normalized_type = require_datasource_payload_type(datasource_type)
    payload = _datasource_payload_template(normalized_type)
    return DataSourcePayloadTemplateData(
        type=normalized_type,
        target_commands=list(DATASOURCE_TEMPLATE_TARGET_COMMANDS),
        source_option="--file",
        payload=payload,
        json=json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        fields=_payload_fields_for_type(normalized_type),
        rules=datasource_payload_rules(),
    )


def datasource_payload_rules() -> list[str]:
    """Return stable datasource payload authoring rules."""
    return [
        "Create payloads must not include id; DS assigns it.",
        "Update payloads may omit id or set it to the selected datasource id.",
        "Create payloads must include the real password when the type uses one.",
        (
            "Update payloads may use the masked password ****** to preserve "
            "the stored password."
        ),
        "Use DS-native field names exactly, including userName and type.",
        "Use `dsctl datasource test DATASOURCE` after create or update.",
    ]


def _base_payload_fields_data() -> list[JsonObject]:
    return [
        field.to_data()
        for field in datasource_base_payload_fields(DATASOURCE_CONTRACT_VERSION)
    ]


def _payload_fields_for_type(datasource_type: str) -> list[JsonObject]:
    fields = _base_payload_fields_data()
    for field in fields:
        if field.get("name") == "type":
            field.pop("choices", None)
    fields.extend(
        field.to_data() for field in _EXTRA_FIELDS_BY_TYPE.get(datasource_type, ())
    )
    return fields


def _datasource_payload_template(datasource_type: str) -> JsonObject:
    profile = _TEMPLATE_PROFILES.get(datasource_type)
    if profile is not None:
        return dict(profile)

    payload: JsonObject = {
        "type": datasource_type,
        "name": f"{datasource_type.lower()}_example",
        "note": "",
        "host": "db.example.com",
        "port": _DEFAULT_PORT_BY_TYPE.get(datasource_type, 0),
        "database": "default",
        "userName": "user",
        "password": "change-me",
        "other": _default_other_params(datasource_type),
    }
    payload.update(_extra_payload_defaults(datasource_type))
    return payload


def _default_other_params(datasource_type: str) -> dict[str, str]:
    if datasource_type == "MYSQL":
        return {"serverTimezone": "UTC"}
    return {}


def _extra_payload_defaults(datasource_type: str) -> JsonObject:
    return {
        field.name: field.example
        for field in _EXTRA_FIELDS_BY_TYPE.get(datasource_type, ())
    }


def _profile_payload(
    datasource_type: str,
    payload: Mapping[str, JsonValue],
) -> JsonObject:
    base: JsonObject = {
        "type": datasource_type,
        "name": f"{datasource_type.lower()}_example",
        "note": "",
    }
    base.update(payload)
    return base


_DEFAULT_PORT_BY_TYPE: dict[str, int] = {
    "MYSQL": 3306,
    "POSTGRESQL": 5432,
    "HIVE": 10000,
    "SPARK": 10000,
    "CLICKHOUSE": 8123,
    "ORACLE": 1521,
    "SQLSERVER": 1433,
    "DB2": 50000,
    "PRESTO": 8080,
    "H2": 9092,
    "REDSHIFT": 5439,
    "TRINO": 8080,
    "STARROCKS": 9030,
    "AZURESQL": 1433,
    "DAMENG": 5236,
    "OCEANBASE": 2881,
    "SSH": 22,
    "KYUUBI": 10009,
    "DATABEND": 8000,
    "SNOWFLAKE": 443,
    "VERTICA": 5433,
    "HANA": 30015,
    "DORIS": 9030,
    "DOLPHINDB": 8848,
}
_HDFS_EXTRA_FIELDS = (
    _DataSourceTypeExtraField(
        "principal",
        "string",
        "Kerberos principal for HDFS-style datasource plugins.",
        "",
    ),
    _DataSourceTypeExtraField(
        "javaSecurityKrb5Conf",
        "string",
        "Path to krb5.conf for Kerberos-enabled HDFS-style datasource plugins.",
        "",
    ),
    _DataSourceTypeExtraField(
        "loginUserKeytabUsername",
        "string",
        "Keytab login user for Kerberos-enabled HDFS-style datasource plugins.",
        "",
    ),
    _DataSourceTypeExtraField(
        "loginUserKeytabPath",
        "string",
        "Keytab path for Kerberos-enabled HDFS-style datasource plugins.",
        "",
    ),
)
_EXTRA_FIELDS_BY_TYPE: dict[str, tuple[_DataSourceTypeExtraField, ...]] = {
    "ALIYUN_SERVERLESS_SPARK": (
        _DataSourceTypeExtraField(
            "accessKeyId",
            "string",
            "Aliyun access key id.",
            "change-me",
        ),
        _DataSourceTypeExtraField(
            "accessKeySecret",
            "string",
            "Aliyun access key secret.",
            "change-me",
        ),
        _DataSourceTypeExtraField(
            "regionId",
            "string",
            "Aliyun region id.",
            "cn-hangzhou",
        ),
        _DataSourceTypeExtraField(
            "endpoint",
            "string",
            "Optional Aliyun endpoint override.",
            "",
        ),
    ),
    "ATHENA": (
        _DataSourceTypeExtraField(
            "awsRegion",
            "string",
            "AWS region used by the Athena datasource plugin.",
            "us-east-1",
        ),
    ),
    "AZURESQL": (
        _DataSourceTypeExtraField(
            "mode",
            "enum",
            "Azure SQL authentication mode.",
            "SqlPassword",
            choices=(
                "SqlPassword",
                "ActiveDirectoryPassword",
                "ActiveDirectoryMSI",
                "ActiveDirectoryServicePrincipal",
                "accessToken",
            ),
        ),
        _DataSourceTypeExtraField(
            "MSIClientId",
            "string",
            "Azure managed-identity client id for ActiveDirectoryMSI mode.",
            "",
        ),
        _DataSourceTypeExtraField(
            "endpoint",
            "string",
            "Access-token endpoint for accessToken mode.",
            "",
        ),
    ),
    "HIVE": _HDFS_EXTRA_FIELDS,
    "K8S": (
        _DataSourceTypeExtraField(
            "kubeConfig",
            "string",
            "Kubernetes kubeconfig content.",
            "change-me",
        ),
        _DataSourceTypeExtraField(
            "namespace",
            "string",
            "Kubernetes namespace.",
            "default",
        ),
    ),
    "OCEANBASE": (
        _DataSourceTypeExtraField(
            "compatibleMode",
            "string",
            "OceanBase compatibility mode.",
            "mysql",
        ),
    ),
    "ORACLE": (
        _DataSourceTypeExtraField(
            "connectType",
            "enum",
            "Oracle connection type.",
            "ORACLE_SERVICE_NAME",
            choices=("ORACLE_SERVICE_NAME", "ORACLE_SID"),
        ),
    ),
    "REDSHIFT": (
        _DataSourceTypeExtraField(
            "mode",
            "enum",
            "Redshift authentication mode.",
            "password",
            choices=("password", "IAM-accessKey"),
        ),
        _DataSourceTypeExtraField(
            "dbUser",
            "string",
            "Redshift IAM database user for IAM-accessKey mode.",
            "",
        ),
    ),
    "SAGEMAKER": (
        _DataSourceTypeExtraField(
            "awsRegion",
            "string",
            "AWS region used by the SageMaker datasource plugin.",
            "us-east-1",
        ),
    ),
    "SPARK": _HDFS_EXTRA_FIELDS,
    "SSH": (
        _DataSourceTypeExtraField(
            "privateKey",
            "string",
            "Optional SSH private key content.",
            "",
        ),
    ),
    "ZEPPELIN": (
        _DataSourceTypeExtraField(
            "restEndpoint",
            "string",
            "Zeppelin REST endpoint.",
            "https://zeppelin.example.com",
        ),
    ),
}
_TEMPLATE_PROFILES: dict[str, JsonObject] = {
    "ALIYUN_SERVERLESS_SPARK": _profile_payload(
        "ALIYUN_SERVERLESS_SPARK",
        _extra_payload_defaults("ALIYUN_SERVERLESS_SPARK"),
    ),
    "ATHENA": _profile_payload(
        "ATHENA",
        {
            "database": "default",
            "userName": "access-key-id",
            "password": "secret-access-key",
            **_extra_payload_defaults("ATHENA"),
        },
    ),
    "K8S": _profile_payload(
        "K8S",
        _extra_payload_defaults("K8S"),
    ),
    "SAGEMAKER": _profile_payload(
        "SAGEMAKER",
        {
            "userName": "access-key-id",
            "password": "secret-access-key",
            **_extra_payload_defaults("SAGEMAKER"),
        },
    ),
    "SSH": _profile_payload(
        "SSH",
        {
            "host": "ssh.example.com",
            "port": _DEFAULT_PORT_BY_TYPE["SSH"],
            "userName": "user",
            "password": "change-me",
            **_extra_payload_defaults("SSH"),
        },
    ),
    "ZEPPELIN": _profile_payload(
        "ZEPPELIN",
        {
            "userName": "user",
            "password": "change-me",
            **_extra_payload_defaults("ZEPPELIN"),
        },
    ),
}


__all__ = [
    "DATASOURCE_PAYLOAD_REVIEW_SUGGESTION",
    "DATASOURCE_TEMPLATE_COMMAND",
    "DATASOURCE_TEMPLATE_COMMAND_PATTERN",
    "DATASOURCE_TEMPLATE_DISCOVERY_COMMAND",
    "DATASOURCE_TEMPLATE_TARGET_COMMANDS",
    "DATASOURCE_TYPE_DISCOVERY_COMMAND",
    "DATASOURCE_TYPE_ENUM",
    "DEFAULT_DATASOURCE_TEMPLATE_TYPE",
    "datasource_payload_command_data",
    "datasource_payload_rules",
    "datasource_template_data",
    "datasource_template_index_data",
    "normalize_datasource_payload_type",
    "require_datasource_payload_type",
    "supported_datasource_template_types",
]
