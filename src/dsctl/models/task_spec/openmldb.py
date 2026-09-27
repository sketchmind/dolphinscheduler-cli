from __future__ import annotations

import re
from typing import Literal

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

_OPENMLDB_IPV4_OCTET_PATTERN = r"(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])"
_OPENMLDB_IPV4_HOST_PATTERN = (
    rf"(?:{_OPENMLDB_IPV4_OCTET_PATTERN}\.){{3}}"
    rf"{_OPENMLDB_IPV4_OCTET_PATTERN}"
)
_OPENMLDB_DNS_LABEL_PATTERN = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_OPENMLDB_DNS_HOST_PATTERN = (
    rf"(?=[A-Za-z0-9.-]{{1,253}}:)"
    rf"(?![0-9.]+:){_OPENMLDB_DNS_LABEL_PATTERN}"
    rf"(?:\.{_OPENMLDB_DNS_LABEL_PATTERN})*"
)
_OPENMLDB_ZK_PORT_PATTERN = (
    r"(?:[1-9]|[1-9][0-9]{1,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|"
    r"65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])"
)
_OPENMLDB_ZK_ENDPOINT_PATTERN = (
    rf"(?:{_OPENMLDB_IPV4_HOST_PATTERN}|{_OPENMLDB_DNS_HOST_PATTERN})"
    rf":{_OPENMLDB_ZK_PORT_PATTERN}"
)
OPENMLDB_ZK_JSON_SCHEMA_PATTERN = (
    rf"^{_OPENMLDB_ZK_ENDPOINT_PATTERN}"
    rf"(?:,{_OPENMLDB_ZK_ENDPOINT_PATTERN})*(?![\s\S])"
)
_OPENMLDB_ZNODE_COMPONENT_PATTERN = r"[A-Za-z0-9_-][A-Za-z0-9._-]*"
OPENMLDB_ZK_PATH_JSON_SCHEMA_PATTERN = (
    rf"^/(?:{_OPENMLDB_ZNODE_COMPONENT_PATTERN}"
    rf"(?:/{_OPENMLDB_ZNODE_COMPONENT_PATTERN})*)?(?![\s\S])"
)
_OPENMLDB_SQL_BLANK_CHARACTERS = (
    r"\x09-\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
_OPENMLDB_SQL_BLANK_PATTERN = re.compile(rf"^[{_OPENMLDB_SQL_BLANK_CHARACTERS}]*$")
OPENMLDB_SQL_JSON_SCHEMA_PATTERN = (
    rf"^(?![{_OPENMLDB_SQL_BLANK_CHARACTERS}]*(?![\s\S]))"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[^\x00-\x08\x0b-\x1f\x22\x3b\x5c\x7f-\x9f]*(?![\s\S])"
)
_OPENMLDB_ZK_PATTERN = re.compile(OPENMLDB_ZK_JSON_SCHEMA_PATTERN)
_OPENMLDB_ZK_PATH_PATTERN = re.compile(OPENMLDB_ZK_PATH_JSON_SCHEMA_PATTERN)


def validate_openmldb_zk(value: str) -> str:
    """Accept one unambiguous DNS/IPv4 ZooKeeper host:port ensemble."""
    if not _OPENMLDB_ZK_PATTERN.fullmatch(value):
        message = (
            "zk must be a comma-separated DNS/IPv4 host:port ensemble with "
            "ports from 1 through 65535"
        )
        raise ValueError(message)
    return value


def validate_openmldb_zk_path(value: str) -> str:
    """Keep one absolute conservative ZooKeeper znode path Python-safe."""
    if not _OPENMLDB_ZK_PATH_PATTERN.fullmatch(value):
        message = "zkPath must be one absolute conservative ZooKeeper znode path"
        raise ValueError(message)
    return value


def validate_openmldb_sql(value: str) -> str:
    """Keep one literal statement safe inside upstream's Python string source."""
    if _OPENMLDB_SQL_BLANK_PATTERN.fullmatch(value):
        message = "sql must not be blank"
        raise ValueError(message)
    if "${" in value or "$[" in value:
        message = "sql must not contain DolphinScheduler placeholders"
        raise ValueError(message)
    if ";" in value or '"' in value or "\\" in value:
        message = (
            "sql must be one statement without semicolons, double quotes, "
            "or backslashes"
        )
        raise ValueError(message)
    if any(
        (ord(character) < 0x20 and character not in "\t\n")
        or 0x7F <= ord(character) <= 0x9F
        for character in value
    ):
        message = "sql contains an unsupported control character"
        raise ValueError(message)
    return value


class OpenmldbLiteralSingleStatementTaskParamsSpec(TaskParamsSpec):
    """Python-source-safe literal OpenMLDB statement on the identity wire."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    zk: str = Field(
        min_length=1,
        json_schema_extra={"pattern": OPENMLDB_ZK_JSON_SCHEMA_PATTERN},
    )
    zk_path: str = Field(
        alias="zkPath",
        min_length=1,
        json_schema_extra={"pattern": OPENMLDB_ZK_PATH_JSON_SCHEMA_PATTERN},
    )
    execute_mode: Literal["offline", "online"] = Field(alias="executeMode")
    sql: str = Field(
        min_length=1,
        json_schema_extra={"pattern": OPENMLDB_SQL_JSON_SCHEMA_PATTERN},
    )

    @field_validator("zk")
    @classmethod
    def validate_zk(cls, value: str) -> str:
        """Reject ambiguous endpoints and values unsafe in the generated URI."""
        return validate_openmldb_zk(value)

    @field_validator("zk_path")
    @classmethod
    def validate_zk_path(cls, value: str) -> str:
        """Reject traversal and Python-source delimiters from the znode path."""
        return validate_openmldb_zk_path(value)

    @field_validator("sql")
    @classmethod
    def validate_sql(cls, value: str) -> str:
        """Reject splitting and Python-source injection without rewriting SQL."""
        return validate_openmldb_sql(value)
