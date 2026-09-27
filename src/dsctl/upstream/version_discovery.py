"""Read source-backed version hints before choosing an exact runtime profile."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

import httpx

from dsctl.auth import build_auth_headers
from dsctl.generated import version_discovery as discovery_facts
from dsctl.generated.version_discovery import PROBES, ProbeContract
from dsctl.upstream.api_contract_discovery import match_documents

if TYPE_CHECKING:
    from dsctl.config import ConnectionSettings
    from dsctl.support.json_types import JsonValue

_DISCOVERY_TIMEOUT_SECONDS = 45.0
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class DiscoveredVersion:
    """The database-recorded product version and the probe that reported it."""

    version: str
    source: str


@dataclass(frozen=True)
class DiscoveredTarget:
    """An exact metadata claim or non-authoritative document candidates."""

    version: str | None
    source: str
    candidate_versions: tuple[str, ...] = ()
    reported_version: str | None = None
    probes: tuple[dict[str, str | int], ...] = ()
    evidence_summary: str = ""
    compatible_operations: tuple[str, ...] = ()


class VersionDiscoveryError(Exception):
    """Safe diagnostics for the service layer to translate into a CLI error."""

    def __init__(
        self, reason: str, *, probes: tuple[dict[str, str | int], ...] = ()
    ) -> None:
        """Retain only structured, credential-free probe diagnostics."""
        super().__init__(f"Version discovery failed ({reason})")
        self.reason = reason
        self.probes = probes


def discovery_contract_digest() -> str:
    """Identify the generated discovery rules used by cached observations."""
    return discovery_facts.DISCOVERY_CONTRACT_DIGEST


def discover_version(connection: ConnectionSettings) -> DiscoveredVersion:
    """Try reviewed GET probes without assuming a DS runtime version.

    Only absent routes and the reviewed legacy route collision permit fallback.
    Authentication and transport failures remain visible; redirects never
    forward the configured credentials.
    """
    base = _connection_url(connection.api_url)
    probes: list[dict[str, str | int]] = []
    deadline = time.monotonic() + _DISCOVERY_TIMEOUT_SECONDS
    request_timeout = connection.api_timeout_seconds
    with httpx.Client(
        headers=build_auth_headers(connection),
        timeout=request_timeout,
        follow_redirects=False,
    ) as client:
        for contract in PROBES:
            version = _probe(client, base, contract, probes, deadline, request_timeout)
            if version is not None:
                return DiscoveredVersion(version=version, source=contract.source)
    reason = "unavailable"
    raise VersionDiscoveryError(reason, probes=tuple(probes))


def discover_target(connection: ConnectionSettings) -> DiscoveredTarget:
    """Collect exact metadata first, then reviewed API-document evidence.

    Document candidates never identify an exact release or exclude custom builds.
    Every probe is a generated, same-origin GET; a shared document is read once.
    """
    base = _connection_url(connection.api_url)
    probes: list[dict[str, str | int]] = []
    payloads: dict[str, JsonValue] = {}
    deadline = time.monotonic() + _DISCOVERY_TIMEOUT_SECONDS
    request_timeout = connection.api_timeout_seconds
    reported: str | None = None
    with httpx.Client(
        headers=build_auth_headers(connection),
        timeout=request_timeout,
        follow_redirects=False,
    ) as client:
        for contract in PROBES:
            payload = _probe_payload(
                client, base, contract, probes, deadline, request_timeout
            )
            payloads[contract.path] = payload
            if payload is None:
                continue
            if _legacy_route_miss(payload, contract):
                probes.append(
                    {"source": contract.source, "reason": "legacy_route_collision"}
                )
                continue
            metadata_version = _metadata_value(payload, contract, probes)
            if (
                reported is not None
                and metadata_version is not None
                and reported != metadata_version
            ):
                probes.append({"source": "metadata", "reason": "conflicting_versions"})
                return DiscoveredTarget(
                    version=None,
                    source="unresolved",
                    reported_version=reported,
                    probes=tuple(probes),
                    evidence_summary="Observed version metadata is contradictory.",
                )
            reported = metadata_version or reported
            if reported in contract.exact_versions:
                probes.append({"source": contract.source, "reason": "exact_metadata"})
                return DiscoveredTarget(
                    version=reported,
                    source=contract.source,
                    reported_version=reported,
                    probes=tuple(probes),
                    evidence_summary="Reviewed product-version metadata.",
                )
            if reported is not None and reported != "3.3.0":
                return DiscoveredTarget(
                    version=None,
                    source=contract.source,
                    reported_version=reported,
                    probes=tuple(probes),
                    evidence_summary=(
                        "Reported version has no reviewed exact metadata mapping."
                    ),
                )
        for document in discovery_facts.DOCUMENT_PROBES:
            if document.path in payloads:
                continue
            contract = ProbeContract(
                document.source, document.path, (), (), None, (), ()
            )
            payloads[document.path] = _probe_payload(
                client, base, contract, probes, deadline, request_timeout
            )
    return _document_target(payloads, probes, reported)


def _metadata_value(
    payload: JsonValue,
    contract: ProbeContract,
    probes: list[dict[str, str | int]],
) -> str | None:
    if not isinstance(payload, Mapping):
        _fail("invalid_response", contract, probes)
    if contract.success_code is not None:
        code = payload.get("code")
        if type(code) is not int or code != contract.success_code:
            _fail("invalid_response", contract, probes)
    for field in contract.version_fields:
        if not isinstance(payload, Mapping) or field not in payload:
            probes.append({"source": contract.source, "reason": "version_absent"})
            return None
        payload = payload[field]
    if (
        isinstance(payload, str)
        and len(payload) <= 32
        and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", payload)
    ):
        probes.append(
            {
                "source": contract.source,
                "reason": "metadata_observed"
                if payload in contract.exact_versions
                else "unsupported_version",
                "reported_version": payload,
            }
        )
        return payload
    if payload is not None and not (
        contract.source == "openapi" and payload in ("V1", "V2")
    ):
        _fail("invalid_response", contract, probes)
    probes.append({"source": contract.source, "reason": "version_absent"})
    return None


def _document_target(
    payloads: dict[str, JsonValue],
    probes: list[dict[str, str | int]],
    reported: str | None,
) -> DiscoveredTarget:
    match = match_documents(payloads)
    probes.extend(match.probes)
    return DiscoveredTarget(
        version=None,
        source="contract" if match.candidate_versions else "unresolved",
        candidate_versions=match.candidate_versions,
        reported_version=reported,
        probes=tuple(probes),
        evidence_summary=match.evidence_summary,
        compatible_operations=match.compatible_operations,
    )


def _connection_url(api_url: str) -> httpx.URL:
    reason = "invalid_connection"
    try:
        base = httpx.URL(api_url)
    except httpx.InvalidURL:
        raise VersionDiscoveryError(reason) from None
    if (
        base.scheme not in {"http", "https"}
        or not base.host
        or base.userinfo
        or base.query
        or base.fragment
    ):
        raise VersionDiscoveryError(reason)
    return base


def _probe(
    client: httpx.Client,
    base: httpx.URL,
    contract: ProbeContract,
    probes: list[dict[str, str | int]],
    deadline: float,
    request_timeout: float,
) -> str | None:
    payload = _probe_payload(client, base, contract, probes, deadline, request_timeout)
    if payload is None:
        return None
    if _legacy_route_miss(payload, contract):
        probes.append({"source": contract.source, "reason": "legacy_route_collision"})
        return None
    return _version_value(payload, contract, probes)


def _probe_payload(
    client: httpx.Client,
    base: httpx.URL,
    contract: ProbeContract,
    probes: list[dict[str, str | int]],
    deadline: float,
    request_timeout: float,
) -> JsonValue:
    url = f"{str(base).rstrip('/')}/{contract.path}"
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        _fail("transport_failed", contract, probes)
    timeout = httpx.Timeout(min(request_timeout, remaining))
    try:
        with client.stream("GET", url, timeout=timeout) as response:
            status = response.status_code
            if status in {404, 405}:
                probes.append(
                    {
                        "source": contract.source,
                        "reason": "unavailable",
                        "http_status": status,
                    }
                )
                return None
            if status in {401, 403}:
                _fail("authentication_failed", contract, probes, status)
            if 300 <= status < 400:
                _fail("redirect_refused", contract, probes, status)
            if status != 200:
                _fail("http_error", contract, probes, status)
            body = _response_body(response, contract, probes, deadline)
    except httpx.RequestError:
        _fail("transport_failed", contract, probes)
    try:
        payload: JsonValue = json.loads(body)
    except (ValueError, UnicodeError, RecursionError):
        _fail("invalid_response", contract, probes, status)
    if payload is None:
        _fail("invalid_response", contract, probes, status)
    return payload


def _response_body(
    response: httpx.Response,
    contract: ProbeContract,
    probes: list[dict[str, str | int]],
    deadline: float,
) -> bytes:
    body = bytearray()
    for chunk in response.iter_bytes():
        if time.monotonic() > deadline:
            _fail("transport_failed", contract, probes)
        if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
            _fail("response_too_large", contract, probes, response.status_code)
        body.extend(chunk)
    return bytes(body)


def _legacy_route_miss(payload: JsonValue, contract: ProbeContract) -> bool:
    if not isinstance(payload, Mapping):
        return False
    code = payload.get("code")
    return (
        type(code) is int
        and code in contract.route_miss_codes
        and payload.get("data") is None
    )


def _version_value(
    payload: JsonValue,
    contract: ProbeContract,
    probes: list[dict[str, str | int]],
) -> str:
    if contract.success_code is not None:
        if not isinstance(payload, Mapping):
            _fail("invalid_response", contract, probes)
        code = payload.get("code")
        if type(code) is not int or code != contract.success_code:
            _fail("invalid_response", contract, probes)
    for field in contract.version_fields:
        if not isinstance(payload, Mapping) or field not in payload:
            _fail("invalid_response", contract, probes)
        payload = payload[field]
    if not isinstance(payload, str) or re.fullmatch(r"\d+\.\d+\.\d+", payload) is None:
        _fail("invalid_response", contract, probes)
    if payload not in contract.exact_versions:
        _fail("unsupported_version", contract, probes)
    return payload


def _fail(
    reason: str,
    contract: ProbeContract,
    probes: list[dict[str, str | int]],
    status: int | None = None,
) -> NoReturn:
    result: dict[str, str | int] = {"source": contract.source, "reason": reason}
    if status is not None:
        result["http_status"] = status
    raise VersionDiscoveryError(reason, probes=(*probes, result)) from None
