"""Compare bounded API documents with generated public-controller evidence.

This is a candidate filter, not a release detector. Full route coverage
constrains reviewed candidates; documented scalar request contracts constrain
compatible operations. Custom builds and unmodelled DTO details remain possible.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.generated import version_discovery as facts

if TYPE_CHECKING:
    from dsctl.generated.version_discovery import (
        ApiOperationContract,
        ApiParameterContract,
    )
    from dsctl.support.json_types import JsonValue

_METHODS = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)
_MAX_OPERATIONS = 4096
_MAX_PARAMETERS = 512


@dataclass(frozen=True)
class DocumentParameter:
    """Only meaningful request fields, stripped of user-controlled prose."""

    name: str
    location: str
    schema_type: str | None
    required: bool
    enum_values: tuple[str, ...]
    default_value: str | None
    item_schema_type: str | None


@dataclass(frozen=True)
class DocumentOperation:
    """One declared HTTP operation and its documented request parameters."""

    method: str
    path: str
    parameters: tuple[DocumentParameter, ...]
    parameters_complete: bool = True


@dataclass(frozen=True)
class ContractMatch:
    """Safe summaries of full documents and surviving reviewed candidates."""

    candidate_versions: tuple[str, ...]
    probes: tuple[dict[str, str | int], ...]
    evidence_summary: str
    compatible_operations: tuple[str, ...] = ()


class InvalidDocumentError(ValueError):
    """A malformed or unsupported document cannot provide contract evidence."""


def match_documents(payloads: Mapping[str, JsonValue]) -> ContractMatch:
    """Match complete route sets and source-reviewed request parameter facts."""
    documents: dict[str, tuple[DocumentOperation, ...]] = {}
    probes: list[dict[str, str | int]] = []
    for probe in facts.DOCUMENT_PROBES:
        payload = payloads.get(probe.path)
        if payload is None:
            continue
        try:
            operations = parse_document(payload)
        except InvalidDocumentError:
            probes.append(
                {
                    "source": probe.source,
                    "path": probe.path,
                    "reason": "invalid_document",
                }
            )
            continue
        documents[probe.path] = operations
        probes.append(
            {
                "source": probe.source,
                "path": probe.path,
                "reason": "document_observed",
                "operation_count": len(operations),
            }
        )
    return _match_profiles(documents, probes)


def _match_profiles(
    documents: Mapping[str, tuple[DocumentOperation, ...]],
    probes: list[dict[str, str | int]],
) -> ContractMatch:
    candidates: list[str] = []
    compatible: set[str] | None = None
    mismatched: set[str] = set()
    for version, profile in facts.CONTRACT_PROFILES.items():
        matches = _profile_matches(
            profile.document_paths, profile.operations, documents
        )
        if matches is None:
            continue
        candidates.append(version)
        accepted, rejected = matches
        compatible = accepted if compatible is None else compatible & accepted
        mismatched.update(rejected)
    probes.extend(
        {
            "source": "contract",
            "reason": "operation_contract_mismatch",
            "operation_id": operation_id,
        }
        for operation_id in sorted(mismatched)[:24]
    )
    probes.append(
        {
            "source": "contract",
            "reason": "candidates" if candidates else "no_contract_match",
            "candidate_count": len(candidates),
            "compatible_operation_count": len(compatible or ()),
            "operation_mismatch_count": len(mismatched),
        }
    )
    return ContractMatch(
        tuple(candidates),
        tuple(probes),
        "Complete reviewed public route sets constrain structural candidates. "
        "Only operations with matching documented request facts across every candidate "
        "are compatible; mismatches and unverified nested DTOs are excluded. "
        "Documentation cannot establish an exact release or exclude custom builds.",
        tuple(sorted(compatible or ())),
    )


def _profile_matches(
    paths: tuple[str, ...],
    operation_ids: tuple[str, ...],
    documents: Mapping[str, tuple[DocumentOperation, ...]],
) -> tuple[set[str], set[str]] | None:
    if not paths or any(path not in documents for path in paths):
        return None
    expected = {
        (operation.method.upper(), operation.path.strip("/")): operation
        for key in operation_ids
        for operation in (facts.OPERATION_CONTRACTS[key],)
    }
    observed = _scoped_document_operations(paths, documents, expected)
    if not expected or len(expected) != len(operation_ids) or observed is None:
        return None
    accepted: set[str] = set()
    rejected: set[str] = set()
    for key, actual in observed.items():
        operation = expected[key]
        target = accepted if _operation_matches(operation, actual) else rejected
        target.add(operation.operation_id)
    return accepted, rejected


def _scoped_document_operations(
    paths: tuple[str, ...],
    documents: Mapping[str, tuple[DocumentOperation, ...]],
    expected: Mapping[tuple[str, str], ApiOperationContract],
) -> dict[tuple[str, str], DocumentOperation] | None:
    groups = {probe.path: probe.document_group for probe in facts.DOCUMENT_PROBES}
    observed: dict[tuple[str, str], DocumentOperation] = {}
    for path in paths:
        group = groups[path]
        scoped_expected = {
            key
            for key, operation in expected.items()
            if group is None or operation.document_group == group
        }
        current = {
            (operation.method, operation.path): operation
            for operation in documents[path]
        }
        if current.keys() != scoped_expected or current.keys() & observed.keys():
            return None
        observed.update(current)
    return observed if observed.keys() == expected.keys() else None


def _operation_matches(
    expected: ApiOperationContract, actual: DocumentOperation
) -> bool:
    if not actual.parameters_complete:
        return False
    ignored = set(expected.ignored_document_parameters)
    remaining = {
        (p.location, p.name): p for p in actual.parameters if p.name not in ignored
    }
    for parameter in expected.parameters:
        if parameter.location == "model" or parameter.schema_type is None:
            return False
        if parameter.schema_type == "object" or (
            parameter.schema_type == "array"
            and parameter.item_schema_type
            not in {"string", "integer", "number", "boolean"}
        ):
            return False
        key = (parameter.location, parameter.name)
        if parameter.location == "body":
            bodies = [key for key in remaining if key[0] == "body"]
            if len(bodies) != 1:
                return False
            key = bodies[0]
        observed = remaining.pop(key, None)
        if observed is None or not _parameter_matches(parameter, observed):
            return False
    return not remaining


def _parameter_matches(
    expected: ApiParameterContract, actual: DocumentParameter
) -> bool:
    actual_type = actual.schema_type
    if actual_type == "ref" and actual.enum_values and expected.enum_values:
        actual_type = "string"
    if (
        expected.document_schema_types
        and actual_type not in expected.document_schema_types
    ):
        return False
    if actual.required not in expected.document_required:
        return False
    if set(actual.enum_values) != set(expected.enum_values):
        return False
    if (
        actual.default_value is not None
        and actual.default_value != expected.default_value
    ):
        return False
    return not (
        expected.item_schema_type is not None
        and actual.item_schema_type != expected.item_schema_type
    )


def parse_document(payload: JsonValue) -> tuple[DocumentOperation, ...]:
    """Read Swagger 2 or OpenAPI 3 without fetching any reference URLs."""
    document = _object(payload)
    if document.get("swagger") != "2.0" and not (
        isinstance(document.get("openapi"), str)
        and str(document["openapi"]).startswith("3.")
    ):
        raise InvalidDocumentError
    operations: list[DocumentOperation] = []
    for path, path_value in _object(document.get("paths")).items():
        if not path.startswith("/") or "?" in path or "#" in path:
            raise InvalidDocumentError
        item = _resolve(path_value, document)
        for method, value in item.items():
            if method not in _METHODS:
                continue
            operations.append(
                _parse_operation(method, path, item, _object(value), document)
            )
            if len(operations) > _MAX_OPERATIONS:
                raise InvalidDocumentError
    if not operations:
        raise InvalidDocumentError
    return tuple(operations)


def _parse_operation(
    method: str,
    path: str,
    item: Mapping[str, JsonValue],
    operation: Mapping[str, JsonValue],
    document: Mapping[str, JsonValue],
) -> DocumentOperation:
    try:
        shared = _parameters(item.get("parameters", []), document)
        parameters = _merged_parameters(
            shared, _parameters(operation.get("parameters", []), document)
        )
        if "requestBody" in operation:
            parameters += (_request_body(operation["requestBody"], document),)
    except InvalidDocumentError:
        return DocumentOperation(method.upper(), path.strip("/"), (), False)
    return DocumentOperation(method.upper(), path.strip("/"), parameters)


def _object(value: JsonValue) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise InvalidDocumentError
    return value


def _resolve(
    value: JsonValue, document: Mapping[str, JsonValue]
) -> Mapping[str, JsonValue]:
    result = _object(value)
    visited: set[str] = set()
    while "$ref" in result:
        reference = result["$ref"]
        if (
            not isinstance(reference, str)
            or not reference.startswith("#/")
            or reference in visited
        ):
            raise InvalidDocumentError
        visited.add(reference)
        if len(visited) > 32:
            raise InvalidDocumentError
        target: JsonValue = dict(document)
        for part in reference[2:].split("/"):
            target = _object(target).get(part.replace("~1", "/").replace("~0", "~"))
        result = _object(target)
    return result


def _parameters(
    value: JsonValue, document: Mapping[str, JsonValue]
) -> tuple[DocumentParameter, ...]:
    if not isinstance(value, list) or len(value) > _MAX_PARAMETERS:
        raise InvalidDocumentError
    result = tuple(_parameter(_resolve(item, document), document) for item in value)
    bound = tuple(p for p in result if p.location != "annotation")
    if len({(p.location, p.name) for p in bound}) != len(bound):
        raise InvalidDocumentError
    return result


def _parameter(
    value: Mapping[str, JsonValue], document: Mapping[str, JsonValue]
) -> DocumentParameter:
    name = value.get("name")
    location = value.get("in", "annotation")
    if not isinstance(name, str) or not isinstance(location, str):
        raise InvalidDocumentError
    if location == "annotation":
        # Springfox emits source-proven bogus ApiImplicitParam entries without
        # a binding location, sometimes referencing absent primitive models.
        # Preserve their names so only reviewed ignored names can be accepted.
        return DocumentParameter(name, location, None, False, (), None, None)
    schema = _resolve(value.get("schema", dict(value)), document)
    required = value.get("required", False)
    if not isinstance(required, bool):
        raise InvalidDocumentError
    enum = schema.get("enum", [])
    if not isinstance(enum, list) or any(
        not isinstance(item, (str, int, float, bool)) for item in enum
    ):
        raise InvalidDocumentError
    schema_type = schema.get("type")
    item_type = _object(schema.get("items", {})).get("type")
    if schema_type is not None and not isinstance(schema_type, str):
        raise InvalidDocumentError
    if item_type is not None and not isinstance(item_type, str):
        raise InvalidDocumentError
    return DocumentParameter(
        name,
        "request" if location in {"query", "formData"} else location,
        schema_type,
        required,
        tuple(sorted(str(item) for item in enum)),
        _scalar_default(schema.get("default")),
        item_type,
    )


def _scalar_default(value: JsonValue) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (str, int, float)):
        return str(value)
    raise InvalidDocumentError


def _merged_parameters(
    shared: tuple[DocumentParameter, ...], local: tuple[DocumentParameter, ...]
) -> tuple[DocumentParameter, ...]:
    merged = {(p.location, p.name): p for p in shared}
    merged.update({(p.location, p.name): p for p in local})
    return tuple(merged.values())


def _request_body(
    value: JsonValue, document: Mapping[str, JsonValue]
) -> DocumentParameter:
    body = _resolve(value, document)
    content = _object(body.get("content"))
    schemas = [
        _resolve(_object(media).get("schema"), document) for media in content.values()
    ]
    if not schemas or any(schema != schemas[0] for schema in schemas[1:]):
        raise InvalidDocumentError
    return _parameter(
        {
            "name": "body",
            "in": "body",
            "required": body.get("required", False),
            "schema": dict(schemas[0]),
        },
        document,
    )
