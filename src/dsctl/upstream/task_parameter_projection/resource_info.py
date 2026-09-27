from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.resources import ResourceAdapter
from dsctl.upstream.task_parameter_projection.shared import _projection_error

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
        TaskResourceRefIndex,
    )


def task_file_uses_id(version: str) -> bool:
    """Select ResourceInfo identity from the exact compiled resource recipe."""
    return ResourceAdapter.for_version(version).task_file_uses_id


def encode_resource_info(
    name: str,
    *,
    version: str,
    task_type: str,
    field: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Encode only a verified FILE identity, never manufacture legacy name wire."""
    if resource_refs is None or name not in resource_refs.verified_full_names:
        raise _resource_error(version, task_type, field, "encode")
    if task_file_uses_id(version):
        resource_id = resource_refs.id_by_full_name.get(name)
        if resource_id is None:
            raise _resource_error(version, task_type, field, "encode")
        return {"id": resource_id}
    wire_name = resource_refs.wire_full_name_by_full_name.get(name)
    if wire_name is None or not wire_name.endswith(f"/resources{name}"):
        raise _resource_error(version, task_type, field, "encode")
    return {"resourceName": wire_name}


def decode_resource_info(
    value: JsonValue,
    *,
    version: str,
    task_type: str,
    field: str,
    resource_refs: TaskResourceRefIndex | None,
) -> str:
    """Decode one exact FILE fixed point; unverified native state stays opaque."""
    key = "id" if task_file_uses_id(version) else "resourceName"
    if not isinstance(value, dict) or set(value) != {key} or resource_refs is None:
        raise _resource_error(version, task_type, field, "decode")
    identity = value[key]
    if key == "id":
        if isinstance(identity, bool) or not isinstance(identity, int) or identity <= 0:
            raise _resource_error(version, task_type, field, "decode")
        name = resource_refs.full_name_by_id.get(identity)
        if name is None:
            raise _resource_error(version, task_type, field, "decode")
        return name
    matches = [
        name
        for name, wire_name in resource_refs.wire_full_name_by_full_name.items()
        if wire_name == identity and name in resource_refs.verified_full_names
    ]
    if len(matches) != 1:
        raise _resource_error(version, task_type, field, "decode")
    name = matches[0]
    if (
        encode_resource_info(
            name,
            version=version,
            task_type=task_type,
            field=field,
            resource_refs=resource_refs,
        )
        != value
    ):
        raise _resource_error(version, task_type, field, "decode")
    return name


def resource_name_candidate(value: JsonValue) -> tuple[str, str] | None:
    """Offer a conservative storage-path candidate for later real FILE verification."""
    if not isinstance(value, dict) or set(value) != {"resourceName"}:
        return None
    wire_name = value["resourceName"]
    if not isinstance(wire_name, str) or not wire_name.startswith("/"):
        return None
    base, marker, tail = wire_name.partition("/resources/")
    if not base or not marker or not tail:
        return None
    return "/" + tail, wire_name


def _resource_error(
    version: str, task_type: str, field: str, direction: ProjectionDirection
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        task_type=task_type,
        field=field,
        direction=direction,
        reason="resource-file-identity-not-verified",
        message=f"{task_type} {field} requires one verified exact FILE identity",
    )
