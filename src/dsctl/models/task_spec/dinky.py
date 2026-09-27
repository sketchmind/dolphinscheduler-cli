from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

_DINKY_IPV4_OCTET_PATTERN = r"(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])"
_DINKY_IPV4_HOST_PATTERN = (
    rf"(?:{_DINKY_IPV4_OCTET_PATTERN}\.){{3}}{_DINKY_IPV4_OCTET_PATTERN}"
)
_DINKY_IPV6_HOST_PATTERN = (
    r"(?:"
    r"(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}"
    r"|(?:[0-9A-Fa-f]{1,4}:){1,7}:"
    r"|(?:[0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}"
    r"|(?:[0-9A-Fa-f]{1,4}:){1,5}(?::[0-9A-Fa-f]{1,4}){1,2}"
    r"|(?:[0-9A-Fa-f]{1,4}:){1,4}(?::[0-9A-Fa-f]{1,4}){1,3}"
    r"|(?:[0-9A-Fa-f]{1,4}:){1,3}(?::[0-9A-Fa-f]{1,4}){1,4}"
    r"|(?:[0-9A-Fa-f]{1,4}:){1,2}(?::[0-9A-Fa-f]{1,4}){1,5}"
    r"|[0-9A-Fa-f]{1,4}:(?:(?::[0-9A-Fa-f]{1,4}){1,6})"
    r"|:(?:(?::[0-9A-Fa-f]{1,4}){1,7}|:)"
    r")"
)
_DINKY_DNS_LABEL_PATTERN = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_DINKY_DNS_HOST_PATTERN = (
    rf"(?=[A-Za-z0-9.-]{{1,253}}(?:[:/]|$))"
    rf"(?![0-9.]+(?=[:/]|$)){_DINKY_DNS_LABEL_PATTERN}"
    rf"(?:\.{_DINKY_DNS_LABEL_PATTERN})*"
)
DINKY_ADDRESS_JSON_SCHEMA_PATTERN = (
    r"^https?://(?!.*(?:\$\{|\$\[))"
    rf"(?:{_DINKY_IPV4_HOST_PATTERN}|{_DINKY_DNS_HOST_PATTERN}|"
    rf"\[{_DINKY_IPV6_HOST_PATTERN}\])"
    r"(?::(?:[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|"
    r"65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?"
    r"(?:/[^\s?#\x00-\x1f\x7f]*)?$"
)
DINKY_TASK_ID_JSON_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:~-]*$"
_DINKY_ADDRESS_PATTERN = re.compile(DINKY_ADDRESS_JSON_SCHEMA_PATTERN)
_DINKY_TASK_ID_PATTERN = re.compile(DINKY_TASK_ID_JSON_SCHEMA_PATTERN)


def validate_dinky_address(value: str) -> str:
    """Validate one literal worker-reachable Dinky HTTP(S) base URL."""
    if value != value.strip() or not _DINKY_ADDRESS_PATTERN.fullmatch(value):
        message = "address must be one literal absolute lowercase HTTP(S) base URL"
        raise ValueError(message)
    try:
        parsed = urlsplit(value)
        parsed_port = parsed.port
    except ValueError as exc:
        message = "address must contain one valid host and TCP port"
        raise ValueError(message) from exc
    hostname = parsed.hostname
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (parsed_port is not None and not 1 <= parsed_port <= 65_535)
        or not _is_valid_dinky_hostname(hostname)
    ):
        message = (
            "address must be an absolute lowercase HTTP(S) base URL with a valid "
            "host and port, without credentials, query, or fragment"
        )
        raise ValueError(message)
    return value


def _is_valid_dinky_hostname(hostname: str) -> bool:
    """Accept one conservative ASCII DNS name, IPv4 address, or IPv6 address."""
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        if re.fullmatch(r"[0-9.]+", hostname) or len(hostname) > 253:
            return False
        labels = hostname.split(".")
        return all(
            1 <= len(label) <= 63
            and label[0].isalnum()
            and label[-1].isalnum()
            and all(
                character.isascii() and (character.isalnum() or character == "-")
                for character in label
            )
            for label in labels
        )
    return True


def validate_dinky_task_id(value: str) -> str:
    """Validate one conservative literal Dinky job identifier token."""
    if not _DINKY_TASK_ID_PATTERN.fullmatch(value):
        message = "taskId must be one non-empty literal identifier token"
        raise ValueError(message)
    return value


class DinkyJobTriggerTaskParamsSpec(TaskParamsSpec):
    """Safe typed subset for triggering one existing Dinky job."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    address: str = Field(
        min_length=1,
        json_schema_extra={"pattern": DINKY_ADDRESS_JSON_SCHEMA_PATTERN},
    )
    task_id: str = Field(
        alias="taskId",
        min_length=1,
        json_schema_extra={"pattern": DINKY_TASK_ID_JSON_SCHEMA_PATTERN},
    )
    online: bool = Field(default=False, strict=True)

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the DS-native online default explicit on every exact wire."""
        return ("online",)

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        """Reject unresolved, credentialed, or malformed remote targets."""
        return validate_dinky_address(value)

    @field_validator("task_id")
    @classmethod
    def validate_task_id(cls, value: str) -> str:
        """Reject placeholders and delimiter syntax that Dinky will not resolve."""
        return validate_dinky_task_id(value)
