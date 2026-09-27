from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from dsctl.models.common import (
    YamlSpecModel,
    YamlValue,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

GRPC_CONNECT_TIMEOUT_MS_MAX = 9_223_372_036_854_775_807
GRPC_PROTO_FIELD_NUMBER_MAX = 536_870_911
GRPC_PROTO_RESERVED_FIELD_NUMBER_MIN = 19_000
GRPC_PROTO_RESERVED_FIELD_NUMBER_MAX = 19_999
GRPC_PROTO_RESERVED_IDENTIFIERS = frozenset(
    {
        "bool",
        "bytes",
        "double",
        "edition",
        "enum",
        "extend",
        "extensions",
        "false",
        "fixed32",
        "fixed64",
        "float",
        "group",
        "import",
        "inf",
        "int32",
        "int64",
        "map",
        "max",
        "message",
        "nan",
        "oneof",
        "option",
        "optional",
        "package",
        "public",
        "repeated",
        "required",
        "reserved",
        "returns",
        "rpc",
        "service",
        "sfixed32",
        "sfixed64",
        "sint32",
        "sint64",
        "stream",
        "string",
        "syntax",
        "to",
        "true",
        "uint32",
        "uint64",
        "weak",
    }
)
_GRPC_PROTO_RESERVED_ALTERNATION = "|".join(
    re.escape(identifier) for identifier in sorted(GRPC_PROTO_RESERVED_IDENTIFIERS)
)
GRPC_PROTO_IDENTIFIER_JSON_SCHEMA_PATTERN = (
    rf"^(?!(?:{_GRPC_PROTO_RESERVED_ALTERNATION})$)"
    r"[A-Za-z_][A-Za-z0-9_]*$"
)
_GRPC_PROTO_IDENTIFIER_PATTERN = re.compile(GRPC_PROTO_IDENTIFIER_JSON_SCHEMA_PATTERN)
GRPC_SERVICE_IDENTIFIER_JSON_SCHEMA_PATTERN = (
    rf"^(?!(?:Request|Response)$)(?!(?:{_GRPC_PROTO_RESERVED_ALTERNATION})$)"
    r"[A-Za-z_][A-Za-z0-9_]*$"
)
_GRPC_SECRET_FIELD_NAMES = frozenset(
    {
        "apikey",
        "authtoken",
        "clientsecret",
        "credential",
        "password",
        "passwd",
        "privatekey",
        "secret",
        "token",
    }
)


def _grpc_ascii_case_insensitive_literal(value: str) -> str:
    """Render one ASCII word as an ECMAScript/Python-compatible regex literal."""
    return "".join(
        f"[{character.lower()}{character.upper()}]"
        if character.isascii() and character.isalpha()
        else re.escape(character)
        for character in value
    )


_GRPC_SECRET_FIELD_ALTERNATION = "|".join(
    _grpc_ascii_case_insensitive_literal(identifier)
    for identifier in sorted(_GRPC_SECRET_FIELD_NAMES)
)
GRPC_RECORD_FIELD_IDENTIFIER_JSON_SCHEMA_PATTERN = (
    rf"^(?!(?:{_GRPC_PROTO_RESERVED_ALTERNATION})$)"
    rf"(?!(?:{_GRPC_SECRET_FIELD_ALTERNATION})$)"
    r"[A-Za-z_][A-Za-z0-9_]*$"
)
_GRPC_RECORD_FIELD_IDENTIFIER_PATTERN = re.compile(
    GRPC_RECORD_FIELD_IDENTIFIER_JSON_SCHEMA_PATTERN
)
_GRPC_IPV4_OCTET_PATTERN = r"(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])"
_GRPC_IPV4_HOST_PATTERN = (
    rf"(?:{_GRPC_IPV4_OCTET_PATTERN}\.){{3}}{_GRPC_IPV4_OCTET_PATTERN}"
)
_GRPC_DNS_LABEL_PATTERN = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_GRPC_DNS_HOST_PATTERN = (
    rf"(?=[A-Za-z0-9.-]{{1,253}}:)"
    rf"(?![0-9.]+:){_GRPC_DNS_LABEL_PATTERN}"
    rf"(?:\.{_GRPC_DNS_LABEL_PATTERN})*"
)
_GRPC_PORT_PATTERN = (
    r"(?:[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|"
    r"65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])"
)
GRPC_TARGET_JSON_SCHEMA_PATTERN = (
    r"^(?![\s\S]*(?:\$\{|\$\[))"
    rf"(?:{_GRPC_IPV4_HOST_PATTERN}|{_GRPC_DNS_HOST_PATTERN})"
    rf":{_GRPC_PORT_PATTERN}$"
)
_GRPC_TARGET_PATTERN = re.compile(GRPC_TARGET_JSON_SCHEMA_PATTERN)
GRPC_LITERAL_STRING_JSON_SCHEMA_PATTERN = (
    r"^(?![\s\S]*(?:\$\{|\$\[))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*$"
)
_GRPC_LITERAL_STRING_PATTERN = re.compile(GRPC_LITERAL_STRING_JSON_SCHEMA_PATTERN)
_GRPC_MESSAGE_TYPE_ERROR = "grpc_message_type"
_GRPC_MESSAGE_KEY_ERROR = "grpc_message_key"
_GRPC_LOGGED_VALUE_DESCRIPTION = "INFO-logged; not secret storage."
GrpcLiteralString = Annotated[
    str,
    Field(
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": GRPC_LITERAL_STRING_JSON_SCHEMA_PATTERN},
    ),
]


def validate_grpc_proto_identifier(value: str, *, field: str) -> str:
    """Accept one package-free proto3 identifier safe for generated source."""
    if not _GRPC_PROTO_IDENTIFIER_PATTERN.fullmatch(value):
        message = (
            f"{field} must be one non-reserved protobuf identifier matching "
            "[A-Za-z_][A-Za-z0-9_]*"
        )
        raise ValueError(message)
    return value


def _validate_grpc_literal_string(value: str, *, field: str) -> str:
    """Reject values the reviewed GRPC executor could substitute or log unsafely."""
    if not _GRPC_LITERAL_STRING_PATTERN.fullmatch(value):
        message = (
            f"{field} must be a literal string without DS placeholders, controls, "
            "or surrogates"
        )
        raise ValueError(message)
    return value


def validate_grpc_record_field_identifier(value: str) -> str:
    """Accept a proto identifier that is also safe for INFO-logged messages."""
    if not _GRPC_RECORD_FIELD_IDENTIFIER_PATTERN.fullmatch(value):
        message = (
            "record field name must be a non-reserved protobuf identifier and is "
            "not secret storage because upstream logs authored values at INFO"
        )
        raise ValueError(message)
    return value


def validate_grpc_target(value: str) -> str:
    """Accept one literal DNS-or-IPv4 host and bounded TCP port."""
    if not _GRPC_TARGET_PATTERN.fullmatch(value):
        message = (
            "url must be one literal DNS-or-IPv4 host:port target with a port "
            "between 1 and 65535"
        )
        raise ValueError(message)
    return value


class GrpcStringFieldSpec(YamlSpecModel):
    """Flat protobuf string field."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    name: str = Field(
        min_length=1,
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": GRPC_PROTO_IDENTIFIER_JSON_SCHEMA_PATTERN},
    )
    number: int = Field(
        ge=1,
        le=GRPC_PROTO_FIELD_NUMBER_MAX,
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "not": {
                "minimum": GRPC_PROTO_RESERVED_FIELD_NUMBER_MIN,
                "maximum": GRPC_PROTO_RESERVED_FIELD_NUMBER_MAX,
            }
        },
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Keep generated descriptor and proto3 names valid and unambiguous."""
        return validate_grpc_proto_identifier(value, field="record field name")

    @field_validator("number")
    @classmethod
    def validate_number(cls, value: int) -> int:
        """Reject protobuf's implementation-reserved field-number range."""
        if (
            GRPC_PROTO_RESERVED_FIELD_NUMBER_MIN
            <= value
            <= GRPC_PROTO_RESERVED_FIELD_NUMBER_MAX
        ):
            message = "record field number uses protobuf's reserved 19000-19999 range"
            raise ValueError(message)
        return value


class GrpcRequestStringFieldSpec(GrpcStringFieldSpec):
    """Flat request string field."""

    name: str = Field(
        min_length=1,
        description=(
            "Case-insensitive exact denylist: apiKey, authToken, "
            "clientSecret, credential, password, passwd, privateKey, secret, "
            "token. INFO-logged; not secret storage."
        ),
        json_schema_extra={"pattern": GRPC_RECORD_FIELD_IDENTIFIER_JSON_SCHEMA_PATTERN},
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Reject exact secret-storage names only on the request record."""
        return validate_grpc_record_field_identifier(value)


def _grpc_proto_json_field_name(value: str) -> str:
    """Apply protobuf's underscore-to-camel JSON field-name transformation."""
    result: list[str] = []
    uppercase_next = False
    for character in value:
        if character == "_":
            uppercase_next = True
            continue
        result.append(character.upper() if uppercase_next else character)
        uppercase_next = False
    return "".join(result)


class GrpcLiteralUnaryStringRecordTaskParamsSpec(TaskParamsSpec):
    """Literal unary GRPC string-record call."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        strict=True,
        json_schema_extra={
            "x-dsctl-runtime-validations": [
                (
                    "requestFields and responseFields each require unique raw "
                    "names, ASCII-case-insensitive protobuf JSON camel-case "
                    "names, and field numbers"
                ),
                "message keys must exactly match requestFields names",
            ]
        },
    )

    url: str = Field(
        min_length=1,
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": GRPC_TARGET_JSON_SCHEMA_PATTERN},
    )
    channel_credential_type: Literal["INSECURE", "TLS_DEFAULT"] = Field(
        alias="channelCredentialType",
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
    )
    service_name: str = Field(
        alias="serviceName",
        min_length=1,
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": GRPC_SERVICE_IDENTIFIER_JSON_SCHEMA_PATTERN},
    )
    method_name: str = Field(
        alias="methodName",
        min_length=1,
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": GRPC_PROTO_IDENTIFIER_JSON_SCHEMA_PATTERN},
    )
    request_fields: list[GrpcRequestStringFieldSpec] = Field(
        alias="requestFields",
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"uniqueItems": True},
    )
    response_fields: list[GrpcStringFieldSpec] = Field(
        alias="responseFields",
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"uniqueItems": True},
    )
    message: dict[str, GrpcLiteralString] = Field(
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "propertyNames": {
                "pattern": GRPC_RECORD_FIELD_IDENTIFIER_JSON_SCHEMA_PATTERN
            }
        },
    )
    grpc_connect_timeout_ms: int = Field(
        alias="grpcConnectTimeoutMs",
        gt=0,
        le=GRPC_CONNECT_TIMEOUT_MS_MAX,
        description=_GRPC_LOGGED_VALUE_DESCRIPTION,
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        """Keep the transport target literal and credential-free."""
        return validate_grpc_target(value)

    @field_validator("service_name", "method_name")
    @classmethod
    def validate_identifier(cls, value: str, info: ValidationInfo) -> str:
        """Keep generated service and RPC source valid proto3."""
        field = "serviceName" if info.field_name == "service_name" else "methodName"
        return validate_grpc_proto_identifier(value, field=field)

    @field_validator("message", mode="before")
    @classmethod
    def validate_message_literals(cls, value: YamlValue) -> YamlValue:
        """Require one literal string map before Pydantic could coerce values."""
        if not isinstance(value, Mapping):
            message = "message must be one object of literal string values"
            raise PydanticCustomError(_GRPC_MESSAGE_TYPE_ERROR, message)
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                message = "message keys and values must all be strings"
                raise PydanticCustomError(_GRPC_MESSAGE_TYPE_ERROR, message)
            if not _GRPC_RECORD_FIELD_IDENTIFIER_PATTERN.fullmatch(key):
                message = (
                    "message property names must be safe record identifiers; "
                    "authored values are not secret storage"
                )
                raise PydanticCustomError(_GRPC_MESSAGE_KEY_ERROR, message)
            _validate_grpc_literal_string(item, field="message")
        return value

    @model_validator(mode="after")
    def validate_record_contract(self) -> GrpcLiteralUnaryStringRecordTaskParamsSpec:
        """Close names, numbers, and request payload around one exact descriptor."""
        if self.service_name in {"Request", "Response"}:
            message = "serviceName must not collide with Request or Response"
            raise ValueError(message)
        for alias, fields in (
            ("requestFields", self.request_fields),
            ("responseFields", self.response_fields),
        ):
            names = [field.name for field in fields]
            numbers = [field.number for field in fields]
            if len(names) != len(set(names)):
                message = f"{alias} names must be unique"
                raise ValueError(message)
            json_names = [_grpc_proto_json_field_name(name).lower() for name in names]
            if len(json_names) != len(set(json_names)):
                message = f"{alias} protobuf JSON names must be unique"
                raise ValueError(message)
            if len(numbers) != len(set(numbers)):
                message = f"{alias} numbers must be unique"
                raise ValueError(message)
        secret_names = sorted(
            field.name
            for field in self.request_fields
            if field.name.lower() in _GRPC_SECRET_FIELD_NAMES
        )
        if secret_names:
            message = (
                "requestFields are logged at INFO and are not secret storage; "
                f"secret-like names are rejected: {', '.join(secret_names)}"
            )
            raise ValueError(message)
        expected_message_keys = {field.name for field in self.request_fields}
        if set(self.message) != expected_message_keys:
            message = "message keys must exactly match requestFields names"
            raise ValueError(message)
        return self
