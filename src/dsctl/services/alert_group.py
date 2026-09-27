from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import ALERT_GROUP_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._page_result import paged_command_result
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.alert_groups import (
    ALERT_GROUP_DOMAIN,
    AlertGroupDomain,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.resolver import ResolvedAlertGroupData
from dsctl.upstream.resolver import alert_group as resolve_alert_group
from dsctl.upstream.serialization import (
    optional_text,
    serialize_alert_group,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocol import AlertGroupRecord


ALERT_GROUP_NOT_EXIST = 10011
ALERT_GROUP_EXIST = 10012
CREATE_ALERT_GROUP_ERROR = 10027
LIST_PAGING_ALERT_GROUP_ERROR = 10029
UPDATE_ALERT_GROUP_ERROR = 10030
DELETE_ALERT_GROUP_ERROR = 10031
USER_NO_OPERATION_PERM = 30001
DESCRIPTION_TOO_LONG_ERROR = 1400004
NOT_ALLOW_TO_DELETE_DEFAULT_ALARM_GROUP = 130030

AlertGroupRuntime: TypeAlias = BoundDomainServiceRuntime[AlertGroupDomain]

ALERT_GROUP_UPDATE_FIELDS_SUGGESTION = (
    "Pass at least one update flag such as --name, --description, "
    "--clear-description, --instance-id, --clear-instance-ids, or --group-type."
)
ALERT_GROUP_UPDATE_DIFFERENT_VALUES_SUGGESTION = (
    "Pass a different --name, --description, --instance-id, or --group-type "
    "value, or use --clear-description/--clear-instance-ids to remove stored values."
)


class DeleteAlertGroupData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    alertGroup: ResolvedAlertGroupData


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue
InstanceIdsUpdate = list[int] | _UnsetValue
GroupTypeUpdate = str | _UnsetValue


def list_alert_groups_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List alert groups with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_bound_domain_service_runtime(
        env_file,
        ALERT_GROUP_DOMAIN,
        _list_alert_groups_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_alert_group_result(
    alert_group: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one alert-group payload."""
    return run_with_bound_domain_service_runtime(
        env_file,
        ALERT_GROUP_DOMAIN,
        _get_alert_group_result,
        alert_group=alert_group,
    )


def create_alert_group_result(
    *,
    name: str,
    description: str | None = None,
    instance_ids: Sequence[int] | None = None,
    group_type: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one alert group from validated CLI input."""
    normalized_name = require_non_empty_text(name, label="alert group name")
    normalized_description = optional_text(description)
    normalized_instance_ids = _normalize_instance_ids(instance_ids or [])
    normalized_group_type = _normalize_group_type(group_type)

    return run_with_bound_domain_service_runtime(
        env_file,
        ALERT_GROUP_DOMAIN,
        _create_alert_group_result,
        name=normalized_name,
        description=normalized_description,
        alert_instance_ids=_encode_alert_instance_ids(normalized_instance_ids),
        instance_ids_explicit=instance_ids is not None,
        group_type=normalized_group_type,
    )


def update_alert_group_result(
    alert_group: str,
    *,
    name: str | None = None,
    description: DescriptionUpdate = UNSET,
    instance_ids: InstanceIdsUpdate = UNSET,
    group_type: GroupTypeUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update one alert group while preserving omitted fields."""
    if (
        name is None
        and description is UNSET
        and instance_ids is UNSET
        and group_type is UNSET
    ):
        message = "Alert-group update requires at least one field change"
        raise UserInputError(message, suggestion=ALERT_GROUP_UPDATE_FIELDS_SUGGESTION)

    normalized_name = (
        require_non_empty_text(name, label="alert group name")
        if name is not None
        else None
    )
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )
    normalized_instance_ids = (
        _normalize_instance_ids(instance_ids)
        if not isinstance(instance_ids, _UnsetValue)
        else UNSET
    )
    normalized_group_type = (
        _normalize_group_type_update(group_type)
        if not isinstance(group_type, _UnsetValue)
        else UNSET
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        ALERT_GROUP_DOMAIN,
        _update_alert_group_result,
        alert_group=alert_group,
        name=normalized_name,
        description=normalized_description,
        instance_ids=normalized_instance_ids,
        group_type=normalized_group_type,
    )


def delete_alert_group_result(
    alert_group: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one alert group after explicit confirmation."""
    require_delete_force(force=force, resource_label="Alert-group")

    return run_with_bound_domain_service_runtime(
        env_file,
        ALERT_GROUP_DOMAIN,
        _delete_alert_group_result,
        alert_group=alert_group,
    )


def _list_alert_groups_result(
    runtime: AlertGroupRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.domain.alert_groups
    return paged_command_result(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_alert_group,
        resource=ALERT_GROUP_RESOURCE,
        resolved={"search": search},
        translate_error=lambda error: _translate_alert_group_api_error(
            error,
            operation="list",
            group_name=search,
        ),
    )


def _get_alert_group_result(
    runtime: AlertGroupRuntime,
    *,
    alert_group: str,
) -> CommandResult:
    adapter = runtime.domain.alert_groups
    resolved_alert_group = resolve_alert_group(alert_group, adapter=adapter)
    fetched_alert_group = adapter.get(alert_group_id=resolved_alert_group.id)
    return CommandResult(
        data=require_json_object(
            serialize_alert_group(fetched_alert_group),
            label="alert-group data",
        ),
        resolved={
            "alertGroup": require_json_object(
                resolved_alert_group.to_data(),
                label="resolved alert-group",
            )
        },
    )


def _create_alert_group_result(
    runtime: AlertGroupRuntime,
    *,
    name: str,
    description: str | None,
    alert_instance_ids: str,
    instance_ids_explicit: bool,
    group_type: str | None,
) -> CommandResult:
    adapter = runtime.domain.alert_groups
    wire_instance_ids, wire_group_type = _association_create_values(
        association=adapter.association,
        alert_instance_ids=alert_instance_ids,
        instance_ids_explicit=instance_ids_explicit,
        group_type=group_type,
        ds_version=runtime.profile.ds_version,
    )
    try:
        created_alert_group = adapter.create(
            group_name=name,
            description=description,
            alert_instance_ids=wire_instance_ids,
            group_type=wire_group_type,
        )
    except ApiResultError as error:
        raise _translate_alert_group_api_error(
            error,
            operation="create",
            group_name=name,
        ) from error

    resolved_alert_group = _resolved_alert_group_data_from_record(created_alert_group)
    return CommandResult(
        data=require_json_object(
            serialize_alert_group(created_alert_group),
            label="alert-group data",
        ),
        resolved={
            "alertGroup": require_json_object(
                resolved_alert_group,
                label="resolved alert-group",
            )
        },
    )


def _update_alert_group_result(
    runtime: AlertGroupRuntime,
    *,
    alert_group: str,
    name: str | None,
    description: DescriptionUpdate,
    instance_ids: InstanceIdsUpdate,
    group_type: GroupTypeUpdate,
) -> CommandResult:
    adapter = runtime.domain.alert_groups
    _validate_update_association_input(
        association=adapter.association,
        instance_ids=instance_ids,
        group_type=group_type,
        ds_version=runtime.profile.ds_version,
    )
    resolved_alert_group = resolve_alert_group(alert_group, adapter=adapter)
    current_alert_group = adapter.get(alert_group_id=resolved_alert_group.id)

    next_name = name or _required_group_name(current_alert_group)
    next_description = (
        current_alert_group.description
        if isinstance(description, _UnsetValue)
        else description
    )
    next_instance_ids = (
        current_alert_group.alertInstanceIds or ""
        if isinstance(instance_ids, _UnsetValue)
        else _encode_alert_instance_ids(instance_ids)
    )
    next_group_type = (
        _record_group_type(current_alert_group)
        if isinstance(group_type, _UnsetValue)
        else group_type
    )
    wire_instance_ids = (
        None if adapter.association == "legacy-alert-type" else next_instance_ids
    )
    wire_group_type = (
        _require_current_group_type(next_group_type)
        if adapter.association == "legacy-alert-type"
        else None
    )
    if (
        next_name == _required_group_name(current_alert_group)
        and next_description == current_alert_group.description
        and (
            next_group_type == _record_group_type(current_alert_group)
            if adapter.association == "legacy-alert-type"
            else next_instance_ids == (current_alert_group.alertInstanceIds or "")
        )
    ):
        message = "Alert-group update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=ALERT_GROUP_UPDATE_DIFFERENT_VALUES_SUGGESTION,
        )

    try:
        updated_alert_group = adapter.update(
            alert_group_id=resolved_alert_group.id,
            group_name=next_name,
            description=next_description,
            alert_instance_ids=wire_instance_ids,
            group_type=wire_group_type,
        )
    except ApiResultError as error:
        raise _translate_alert_group_api_error(
            error,
            operation="update",
            alert_group_id=resolved_alert_group.id,
            group_name=next_name,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_alert_group(updated_alert_group),
            label="alert-group data",
        ),
        resolved={
            "alertGroup": require_json_object(
                resolved_alert_group.to_data(),
                label="resolved alert-group",
            )
        },
    )


def _delete_alert_group_result(
    runtime: AlertGroupRuntime,
    *,
    alert_group: str,
) -> CommandResult:
    adapter = runtime.domain.alert_groups
    resolved_alert_group = resolve_alert_group(alert_group, adapter=adapter)
    try:
        deleted = adapter.delete(alert_group_id=resolved_alert_group.id)
    except ApiResultError as error:
        raise _translate_alert_group_api_error(
            error,
            operation="delete",
            alert_group_id=resolved_alert_group.id,
            group_name=resolved_alert_group.group_name,
        ) from error

    data: DeleteAlertGroupData = {
        "deleted": deleted,
        "alertGroup": resolved_alert_group.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="alert-group delete data"),
        resolved={
            "alertGroup": require_json_object(
                resolved_alert_group.to_data(),
                label="resolved alert-group",
            )
        },
    )


def _resolved_alert_group_data_from_record(
    alert_group: AlertGroupRecord,
) -> ResolvedAlertGroupData:
    alert_group_id = alert_group.id
    group_name = alert_group.groupName
    if alert_group_id is None or group_name is None or not group_name.strip():
        message = "Alert-group payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_GROUP_RESOURCE},
        )
    return {
        "id": alert_group_id,
        "groupName": group_name,
        "description": alert_group.description,
    }


def _required_group_name(alert_group: AlertGroupRecord) -> str:
    group_name = alert_group.groupName
    if group_name is None or not group_name.strip():
        message = "Alert-group payload was missing required field 'groupName'"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_GROUP_RESOURCE},
        )
    return group_name


def _normalize_group_type(value: str | None) -> str | None:
    normalized = optional_text(value)
    if normalized is None:
        return None
    normalized = normalized.upper()
    if normalized in {"EMAIL", "SMS"}:
        return normalized
    message = "Alert-group type must be EMAIL or SMS"
    raise UserInputError(
        message,
        details={"group_type": normalized, "choices": ["EMAIL", "SMS"]},
        suggestion="Pass --group-type EMAIL or --group-type SMS.",
    )


def _normalize_group_type_update(value: str) -> str:
    normalized = _normalize_group_type(value)
    if normalized is not None:
        return normalized
    message = "Alert-group type cannot be empty"
    raise UserInputError(
        message,
        suggestion="Pass --group-type EMAIL or --group-type SMS.",
    )


def _record_group_type(alert_group: AlertGroupRecord) -> str | None:
    value = getattr(alert_group, "groupType", None)
    return value if isinstance(value, str) else None


def _association_create_values(
    *,
    association: str,
    alert_instance_ids: str,
    instance_ids_explicit: bool,
    group_type: str | None,
    ds_version: str,
) -> tuple[str | None, str | None]:
    if association == "legacy-alert-type":
        if instance_ids_explicit:
            raise _association_input_error(
                ds_version=ds_version,
                message=(
                    f"DolphinScheduler {ds_version} does not support --instance-id "
                    "for alert groups"
                ),
                suggestion="Omit --instance-id and pass --group-type EMAIL or SMS.",
            )
        if group_type is None:
            raise _association_input_error(
                ds_version=ds_version,
                message=(
                    f"DolphinScheduler {ds_version} alert-group create requires "
                    "--group-type"
                ),
                suggestion="Pass --group-type EMAIL or --group-type SMS.",
            )
        return None, group_type
    if group_type is not None:
        raise _association_input_error(
            ds_version=ds_version,
            message=(
                f"DolphinScheduler {ds_version} does not support --group-type "
                "for alert groups"
            ),
            suggestion="Omit --group-type and use --instance-id when needed.",
        )
    return alert_instance_ids, None


def _validate_update_association_input(
    *,
    association: str,
    instance_ids: InstanceIdsUpdate,
    group_type: GroupTypeUpdate,
    ds_version: str,
) -> None:
    if association == "legacy-alert-type" and not isinstance(
        instance_ids,
        _UnsetValue,
    ):
        raise _association_input_error(
            ds_version=ds_version,
            message=(
                f"DolphinScheduler {ds_version} does not support --instance-id "
                "or --clear-instance-ids for alert groups"
            ),
            suggestion=(
                "Omit plugin-instance flags; use --group-type EMAIL or SMS to "
                "change the legacy association."
            ),
        )
    if association != "legacy-alert-type" and not isinstance(
        group_type,
        _UnsetValue,
    ):
        raise _association_input_error(
            ds_version=ds_version,
            message=(
                f"DolphinScheduler {ds_version} does not support --group-type "
                "for alert groups"
            ),
            suggestion="Omit --group-type and use --instance-id when needed.",
        )


def _require_current_group_type(value: str | None) -> str:
    normalized = _normalize_group_type(value)
    if normalized is not None:
        return normalized
    message = "Legacy alert-group payload was missing required field 'groupType'"
    raise ApiTransportError(
        message,
        details={"resource": ALERT_GROUP_RESOURCE, "field": "groupType"},
    )


def _association_input_error(
    *,
    ds_version: str,
    message: str,
    suggestion: str,
) -> UserInputError:
    return UserInputError(
        message,
        details={
            "resource": ALERT_GROUP_RESOURCE,
            "selected_version": ds_version,
            "reason": "upstream_field_absent",
        },
        suggestion=suggestion,
    )


def _normalize_instance_ids(values: Sequence[int]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values:
        require_positive_int(value, label="instance_id")
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _encode_alert_instance_ids(values: Sequence[int]) -> str:
    return ",".join(str(value) for value in values)


def _translate_alert_group_api_error(
    error: ApiResultError,
    *,
    operation: str,
    alert_group_id: int | None = None,
    group_name: str | None = None,
) -> Exception:
    details: dict[str, int | str] = {"operation": operation}
    if alert_group_id is not None:
        details["id"] = alert_group_id
    if group_name is not None:
        details["groupName"] = group_name

    if error.result_code == ALERT_GROUP_NOT_EXIST:
        identifier = alert_group_id if alert_group_id is not None else group_name
        message = f"Alert group {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == ALERT_GROUP_EXIST:
        message = "Alert-group create/update conflicted with an existing group name"
        return ConflictError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Alert-group {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code == NOT_ALLOW_TO_DELETE_DEFAULT_ALARM_GROUP:
        message = "The default alert group cannot be deleted"
        return InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Choose a non-default alert group; DolphinScheduler does not "
                "allow deleting the default group."
            ),
        )
    if error.result_code == DESCRIPTION_TOO_LONG_ERROR:
        message = "Alert-group description was rejected by the upstream API"
        return UserInputError(
            message,
            details=details,
            suggestion="Shorten --description and retry the same alert-group command.",
        )
    if error.result_code in {
        CREATE_ALERT_GROUP_ERROR,
        LIST_PAGING_ALERT_GROUP_ERROR,
        UPDATE_ALERT_GROUP_ERROR,
        DELETE_ALERT_GROUP_ERROR,
    }:
        # These are generic controller/service fallback failures, not stable
        # alert-group domain states such as conflict, not-found, or permission.
        return error
    return error
