from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING, cast

from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _decode_task_ref,
    _encode_task_ref,
    _list_field,
    _object_field,
    _projection_error,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionDirection,
    ProjectionSource,
    TaskGraphContext,
    TaskParameterProjectionError,
    TaskRefIndex,
)
from dsctl.upstream.task_references import (
    PREDICATE_TASK,
    task_references,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue


def _decode_opaque_blocking_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
    graph_context: TaskGraphContext | None,
) -> DecodedTaskParameters:
    """Canonicalize only the exact closed BLOCKING wire."""
    _require_blocking_available(version=version, direction="decode")
    try:
        projected = _decode_blocking(payload, version=version, refs=refs)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("BLOCKING", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    predecessor_codes = _blocking_canonical_predecessor_codes(projected, refs=refs)
    if graph_context is None or not graph_context.preserves_predecessors(
        predecessor_codes
    ):
        return DecodedTaskParameters(
            ProjectedTask("BLOCKING", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("BLOCKING", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


_BLOCKING_OWNED_FIELDS = frozenset(
    {"blockingOpportunity", "alertWhenBlocking", "dependence"}
)


def _encode_blocking(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
) -> JsonObject:
    """Project one canonical BLOCKING gate onto its exact REST taskParams wire."""
    return _project_blocking(
        payload,
        version=version,
        direction="encode",
        refs=refs,
    )


def _decode_blocking(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
) -> JsonObject:
    """Decode one exact BLOCKING REST taskParams wire to canonical task names."""
    return _project_blocking(
        payload,
        version=version,
        direction="decode",
        refs=refs,
    )


def _blocking_canonical_predecessor_codes(
    payload: JsonObject,
    *,
    refs: TaskRefIndex,
) -> frozenset[int]:
    """Recover native predecessor identities from a validated canonical gate."""
    return frozenset(
        refs.code_by_name[cast("str", reference.value)]
        for reference in task_references("BLOCKING", payload)
    )


def _project_blocking(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
) -> JsonObject:
    _require_blocking_available(version=version, direction=direction)
    _reject_blocking_unowned_fields(
        payload,
        allowed=_BLOCKING_OWNED_FIELDS,
        version=version,
        direction=direction,
        field="task_params",
    )
    opportunity = payload.get("blockingOpportunity")
    if opportunity not in {"BlockingOnSuccess", "BlockingOnFailed"}:
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field="task_params.blockingOpportunity",
            reason="unsupported-blocking-opportunity",
            message=(
                "BLOCKING blockingOpportunity must be BlockingOnSuccess or "
                "BlockingOnFailed"
            ),
        )
    alert = payload.get("alertWhenBlocking", False)
    if not isinstance(alert, bool):
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field="task_params.alertWhenBlocking",
            reason="invalid-boolean",
            message="BLOCKING alertWhenBlocking must be one strict boolean",
        )
    dependence = _blocking_dependence(
        payload,
        version=version,
        direction=direction,
        refs=refs,
    )
    return {
        "blockingOpportunity": opportunity,
        "alertWhenBlocking": alert,
        "dependence": dependence,
    }


def _blocking_dependence(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
) -> JsonObject:
    dependence = _object_field(
        payload,
        "dependence",
        version=version,
        direction=direction,
        task_type="BLOCKING",
        field="task_params.dependence",
    )
    _reject_blocking_unowned_fields(
        dependence,
        allowed=frozenset({"relation", "dependTaskList"}),
        version=version,
        direction=direction,
        field="task_params.dependence",
    )
    relation = dependence.get("relation")
    if relation not in {"AND", "OR"}:
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field="task_params.dependence.relation",
            reason="unsupported-dependency-relation",
            message="BLOCKING dependence.relation must be AND or OR",
        )
    groups = _list_field(
        dependence,
        "dependTaskList",
        version=version,
        direction=direction,
        task_type="BLOCKING",
        field="task_params.dependence.dependTaskList",
    )
    if not groups:
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field="task_params.dependence.dependTaskList",
            reason="empty-dependency-groups",
            message="BLOCKING dependTaskList must not be empty",
        )
    projected_groups: list[JsonValue] = []
    for group_index, raw_group in enumerate(groups):
        group_field = f"task_params.dependence.dependTaskList[{group_index}]"
        if not isinstance(raw_group, Mapping):
            raise _blocking_projection_error(
                version=version,
                direction=direction,
                field=group_field,
                reason="invalid-object-shape",
                message=f"BLOCKING requires {group_field} to be a JSON object",
            )
        group = deepcopy(dict(raw_group))
        _reject_blocking_unowned_fields(
            group,
            allowed=frozenset({"relation", "dependItemList"}),
            version=version,
            direction=direction,
            field=group_field,
        )
        group_relation = group.get("relation")
        if group_relation not in {"AND", "OR"}:
            raise _blocking_projection_error(
                version=version,
                direction=direction,
                field=f"{group_field}.relation",
                reason="unsupported-dependency-relation",
                message=f"BLOCKING {group_field}.relation must be AND or OR",
            )
        item_list_field = f"{group_field}.dependItemList"
        items = _list_field(
            group,
            "dependItemList",
            version=version,
            direction=direction,
            task_type="BLOCKING",
            field=item_list_field,
        )
        if not items:
            raise _blocking_projection_error(
                version=version,
                direction=direction,
                field=item_list_field,
                reason="empty-dependency-items",
                message="BLOCKING dependItemList must not be empty",
            )
        projected_items = [
            _blocking_predicate(
                raw_item,
                version=version,
                direction=direction,
                refs=refs,
                field=f"{item_list_field}[{item_index}]",
            )
            for item_index, raw_item in enumerate(items)
        ]
        projected_groups.append(
            {
                "relation": group_relation,
                "dependItemList": projected_items,
            }
        )
    return {
        "relation": relation,
        "dependTaskList": projected_groups,
    }


def _blocking_predicate(
    raw_item: JsonValue,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
    field: str,
) -> JsonObject:
    if not isinstance(raw_item, Mapping):
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason="invalid-object-shape",
            message=f"BLOCKING requires {field} to be a JSON object",
        )
    item = deepcopy(dict(raw_item))
    ref_key = PREDICATE_TASK.key(native=direction == "decode")
    _reject_blocking_unowned_fields(
        item,
        allowed=frozenset({ref_key, "status"}),
        version=version,
        direction=direction,
        field=field,
    )
    status = item.get("status")
    if status not in {"SUCCESS", "FAILURE"}:
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field=f"{field}.status",
            reason="unsupported-condition-status",
            message=f"BLOCKING supports only SUCCESS or FAILURE in {field}.status",
        )
    if ref_key not in item:
        raise _blocking_projection_error(
            version=version,
            direction=direction,
            field=f"{field}.{ref_key}",
            reason="missing-task-reference",
            message=f"BLOCKING requires {field}.{ref_key}",
        )
    if direction == "encode":
        return {
            PREDICATE_TASK.key(native=True): _encode_task_ref(
                item[ref_key],
                version=version,
                task_type="BLOCKING",
                field=f"{field}.{ref_key}",
                refs=refs,
            ),
            "status": status,
        }
    return {
        PREDICATE_TASK.key(): _decode_task_ref(
            item[ref_key],
            version=version,
            task_type="BLOCKING",
            field=f"{field}.{ref_key}",
            refs=refs,
        ),
        "status": status,
    }


def _reject_blocking_unowned_fields(
    payload: Mapping[str, JsonValue],
    *,
    allowed: frozenset[str],
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> None:
    unexpected = sorted(set(payload) - allowed)
    if not unexpected:
        return
    names = ", ".join(unexpected)
    raise _blocking_projection_error(
        version=version,
        direction=direction,
        field=f"{field}.{unexpected[0]}",
        reason="outside-reviewed-same-workflow-state-gate-subset",
        message=f"BLOCKING projection does not own fields: {names}",
    )


def _require_blocking_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).blocking.available:
        return
    raise _blocking_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"BLOCKING does not exist in DolphinScheduler {version}",
    )


def _blocking_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type="BLOCKING",
        field=field,
        reason=reason,
        message=message,
    )
