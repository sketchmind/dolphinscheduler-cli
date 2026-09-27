from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

GrpcWireEpoch = Literal["literal-unary-string-record-call"]


@dataclass(frozen=True, slots=True)
class GrpcAuthoringSurface:
    """Reviewed unary GRPC execution, transport, and recovery semantics."""

    available: bool
    wire_epoch: GrpcWireEpoch | None
    parameter_substitution: bool
    authored_values_logged: bool
    result_output_supported: bool
    cancel_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_may_duplicate: bool
    channel_shutdown: bool
    event_loop_group_shutdown: bool
    tls_uses_default_trust: bool
    tls_hostname_verification: bool
    custom_ca_supported: bool
    mutual_tls_supported: bool
    request_authentication_supported: bool


_GRPC_ABSENT = GrpcAuthoringSurface(
    available=False,
    wire_epoch=None,
    parameter_substitution=False,
    authored_values_logged=False,
    result_output_supported=False,
    cancel_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_may_duplicate=False,
    channel_shutdown=False,
    event_loop_group_shutdown=False,
    tls_uses_default_trust=False,
    tls_hostname_verification=False,
    custom_ca_supported=False,
    mutual_tls_supported=False,
    request_authentication_supported=False,
)
_GRPC_LITERAL_UNARY_STRING_RECORD_CALL = GrpcAuthoringSurface(
    available=True,
    wire_epoch="literal-unary-string-record-call",
    parameter_substitution=False,
    authored_values_logged=True,
    result_output_supported=False,
    cancel_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_may_duplicate=True,
    channel_shutdown=False,
    event_loop_group_shutdown=False,
    tls_uses_default_trust=True,
    tls_hostname_verification=True,
    custom_ca_supported=False,
    mutual_tls_supported=False,
    request_authentication_supported=False,
)


def _grpc_surface(version: str) -> GrpcAuthoringSurface:
    if version in {
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
    }:
        return _GRPC_ABSENT
    if version in {"3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _GRPC_LITERAL_UNARY_STRING_RECORD_CALL
    message = f"No exact GRPC authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
