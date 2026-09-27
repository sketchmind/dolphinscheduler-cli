from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import DATASOURCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
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
from dsctl.services.datasource_payload import (
    DATASOURCE_PAYLOAD_REVIEW_SUGGESTION,
    require_datasource_payload_type,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.datasource_contracts import (
    datasource_payload_field_names,
    datasource_sensitive_payload_fields,
)
from dsctl.upstream.datasources import (
    DATASOURCE_DOMAIN,
    DataSourceDomain,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.resolver import ResolvedDataSource, ResolvedDataSourceData
from dsctl.upstream.resolver import datasource as resolve_datasource
from dsctl.upstream.serialization import (
    enum_value,
    serialize_datasource,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.output import JsonObject, JsonValue
    from dsctl.upstream.protocol import DataSourceOperations, DataSourceRecord


MASKED_PASSWORD = "*" * 6
MASKED_SECRET = MASKED_PASSWORD

DATASOURCE_EXISTS = 10015
CREATE_DATASOURCE_ERROR = 10033
UPDATE_DATASOURCE_ERROR = 10034
CONNECT_DATASOURCE_FAILURE = 10036
CONNECTION_TEST_FAILURE = 10037
DELETE_DATASOURCE_FAILURE = 10038
VERIFY_DATASOURCE_NAME_FAILURE = 10039
RESOURCE_NOT_EXIST = 20004
USER_NO_OPERATION_PERM = 30001
DESCRIPTION_TOO_LONG_ERROR = 1400004


class DeleteDataSourceData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    datasource: ResolvedDataSourceData


class ConnectionTestData(TypedDict):
    """CLI datasource connection-test payload."""

    connected: bool
    datasource: ResolvedDataSourceData


class DataSourceWarningDetail(TypedDict):
    """Structured warning emitted for datasource payload normalization."""

    code: str
    message: str
    field: str
    reason: str
    preserved_existing: bool


def list_datasources_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List datasources with explicit paging or auto-exhaust support."""
    normalized_search = _optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_bound_domain_service_runtime(
        env_file,
        DATASOURCE_DOMAIN,
        _list_datasources_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_datasource_result(
    datasource: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one datasource detail payload."""
    return run_with_bound_domain_service_runtime(
        env_file,
        DATASOURCE_DOMAIN,
        _get_datasource_result,
        datasource=datasource,
    )


def create_datasource_result(
    *,
    file: Path,
    env_file: str | None = None,
) -> CommandResult:
    """Create one datasource from one DS-native JSON payload file."""
    payload = _load_datasource_payload_or_error(file)
    _require_datasource_name(payload, operation="create")
    _require_datasource_type_text(payload, operation="create")
    if "id" in payload:
        message = "Datasource create payload must not include id"
        raise UserInputError(
            message,
            details={"file": str(file)},
            suggestion="Remove `id` from the create payload; DS assigns it.",
        )
    return run_with_bound_domain_service_runtime(
        env_file,
        DATASOURCE_DOMAIN,
        _create_datasource_result,
        payload=payload,
        source_file=file,
    )


def update_datasource_result(
    datasource: str,
    *,
    file: Path,
    env_file: str | None = None,
) -> CommandResult:
    """Update one datasource from one DS-native JSON payload file."""
    payload = _load_datasource_payload_or_error(file)
    _require_datasource_name(payload, operation="update")
    _require_datasource_type_text(payload, operation="update")

    return run_with_bound_domain_service_runtime(
        env_file,
        DATASOURCE_DOMAIN,
        _update_datasource_result,
        datasource=datasource,
        payload=payload,
        source_file=file,
    )


def delete_datasource_result(
    datasource: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one datasource after explicit confirmation."""
    require_delete_force(force=force, resource_label="Datasource")

    return run_with_bound_domain_service_runtime(
        env_file,
        DATASOURCE_DOMAIN,
        _delete_datasource_result,
        datasource=datasource,
    )


def connection_test_datasource_result(
    datasource: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Run one datasource connection test."""
    return run_with_bound_domain_service_runtime(
        env_file,
        DATASOURCE_DOMAIN,
        _test_datasource_result,
        datasource=datasource,
    )


def _list_datasources_result(
    runtime: BoundDomainServiceRuntime[DataSourceDomain],
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.domain.datasources
    return paged_command_result(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_datasource,
        resource=DATASOURCE_RESOURCE,
        resolved={"search": search},
        translate_error=lambda error: _translate_datasource_api_error(
            error,
            operation="list",
            name=search,
        ),
    )


def _get_datasource_result(
    runtime: BoundDomainServiceRuntime[DataSourceDomain],
    *,
    datasource: str,
) -> CommandResult:
    adapter = runtime.domain.datasources
    resolved_datasource = _resolve_datasource_or_error(
        adapter,
        datasource,
        operation="get",
    )
    fetched_datasource = _redacted_datasource_detail(
        _get_datasource_detail_or_error(
            adapter,
            datasource_id=resolved_datasource.id,
            operation="get",
            name=resolved_datasource.name,
        ),
        ds_version=runtime.profile.ds_version,
    )
    return CommandResult(
        data=require_json_object(
            fetched_datasource,
            label="datasource data",
        ),
        resolved={
            "datasource": require_json_object(
                resolved_datasource.to_data(),
                label="resolved datasource",
            )
        },
    )


def _create_datasource_result(
    runtime: BoundDomainServiceRuntime[DataSourceDomain],
    *,
    payload: JsonObject,
    source_file: Path,
) -> CommandResult:
    adapter = runtime.domain.datasources
    normalized_payload = _validated_version_payload(
        payload,
        ds_version=runtime.profile.ds_version,
        operation="create",
        source_file=source_file,
    )
    _reject_masked_create_secrets(
        normalized_payload,
        ds_version=runtime.profile.ds_version,
        source_file=source_file,
    )
    payload_json = _payload_json(normalized_payload)
    try:
        created_datasource = adapter.create(payload_json=payload_json)
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation="create",
            name=_payload_name(normalized_payload),
            file=str(source_file),
        ) from error

    resolved_datasource = _resolved_datasource_data(created_datasource)
    fetched_datasource = _redacted_datasource_detail(
        _get_datasource_detail_or_error(
            adapter,
            datasource_id=resolved_datasource["id"],
            operation="create_readback",
            name=resolved_datasource["name"],
            file=str(source_file),
        ),
        ds_version=runtime.profile.ds_version,
    )
    return CommandResult(
        data=require_json_object(
            fetched_datasource,
            label="datasource data",
        ),
        resolved={
            "datasource": require_json_object(
                resolved_datasource,
                label="resolved datasource",
            )
        },
    )


def _update_datasource_result(
    runtime: BoundDomainServiceRuntime[DataSourceDomain],
    *,
    datasource: str,
    payload: JsonObject,
    source_file: Path,
) -> CommandResult:
    adapter = runtime.domain.datasources
    resolved_datasource = _resolve_datasource_or_error(
        adapter,
        datasource,
        operation="update",
        file=str(source_file),
    )
    validated_payload = _validated_version_payload(
        payload,
        ds_version=runtime.profile.ds_version,
        operation="update",
        source_file=source_file,
    )
    current_detail = _get_datasource_detail_or_error(
        adapter,
        datasource_id=resolved_datasource.id,
        operation="update",
        name=resolved_datasource.name,
        file=str(source_file),
    )
    _require_unchanged_datasource_type(
        validated_payload,
        current_detail=current_detail,
        ds_version=runtime.profile.ds_version,
        source_file=source_file,
    )
    normalized_payload, warnings, warning_details = _normalized_update_payload(
        validated_payload,
        datasource_id=resolved_datasource.id,
        current_detail=current_detail,
        sensitive_fields=datasource_sensitive_payload_fields(
            runtime.profile.ds_version
        ),
        blank_password_preserves_existing=(adapter.blank_password_preserves_existing),
        source_file=source_file,
    )
    payload_json = _payload_json(normalized_payload)
    try:
        adapter.update(
            datasource_id=resolved_datasource.id,
            payload_json=payload_json,
        )
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation="update",
            datasource_id=resolved_datasource.id,
            name=resolved_datasource.name,
            file=str(source_file),
        ) from error

    fetched_datasource = _redacted_datasource_detail(
        _get_datasource_detail_or_error(
            adapter,
            datasource_id=resolved_datasource.id,
            operation="update_readback",
            name=resolved_datasource.name,
            file=str(source_file),
        ),
        ds_version=runtime.profile.ds_version,
    )
    return CommandResult(
        data=require_json_object(
            fetched_datasource,
            label="datasource data",
        ),
        resolved={
            "datasource": require_json_object(
                resolved_datasource.to_data(),
                label="resolved datasource",
            )
        },
        warnings=warnings,
        warning_details=warning_details,
    )


def _delete_datasource_result(
    runtime: BoundDomainServiceRuntime[DataSourceDomain],
    *,
    datasource: str,
) -> CommandResult:
    adapter = runtime.domain.datasources
    resolved_datasource = _resolve_datasource_or_error(
        adapter,
        datasource,
        operation="delete",
    )
    try:
        deleted = adapter.delete(datasource_id=resolved_datasource.id)
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation="delete",
            datasource_id=resolved_datasource.id,
            name=resolved_datasource.name,
        ) from error

    return CommandResult(
        data=require_json_object(
            DeleteDataSourceData(
                deleted=deleted,
                datasource=resolved_datasource.to_data(),
            ),
            label="datasource delete data",
        ),
        resolved={
            "datasource": require_json_object(
                resolved_datasource.to_data(),
                label="resolved datasource",
            )
        },
    )


def _test_datasource_result(
    runtime: BoundDomainServiceRuntime[DataSourceDomain],
    *,
    datasource: str,
) -> CommandResult:
    adapter = runtime.domain.datasources
    resolved_datasource = _resolve_datasource_or_error(
        adapter,
        datasource,
        operation="connection_test",
    )
    try:
        connected = adapter.connection_test(datasource_id=resolved_datasource.id)
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation="connection_test",
            datasource_id=resolved_datasource.id,
            name=resolved_datasource.name,
        ) from error

    return CommandResult(
        data=require_json_object(
            ConnectionTestData(
                connected=connected,
                datasource=resolved_datasource.to_data(),
            ),
            label="datasource test data",
        ),
        resolved={
            "datasource": require_json_object(
                resolved_datasource.to_data(),
                label="resolved datasource",
            )
        },
    )


def _resolve_datasource_or_error(
    adapter: DataSourceOperations,
    datasource: str,
    *,
    operation: str,
    file: str | None = None,
) -> ResolvedDataSource:
    try:
        return resolve_datasource(datasource, adapter=adapter)
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation=operation,
            name=datasource,
            file=file,
        ) from error


def _get_datasource_detail_or_error(
    adapter: DataSourceOperations,
    *,
    datasource_id: int,
    operation: str,
    name: str | None,
    file: str | None = None,
) -> JsonObject:
    try:
        return adapter.get(datasource_id=datasource_id)
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation=operation,
            datasource_id=datasource_id,
            name=name,
            file=file,
        ) from error


def _load_datasource_payload_or_error(path: Path) -> JsonObject:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        message = f"Could not read datasource payload file: {exc}"
        raise UserInputError(
            message,
            details={"file": str(path)},
            suggestion=(
                "Check that the file exists and is readable, then retry with "
                "`--file FILE`."
            ),
        ) from exc
    except json.JSONDecodeError as exc:
        message = f"Datasource payload file is not valid JSON: {exc.msg}"
        raise UserInputError(
            message,
            details={"file": str(path)},
            suggestion=(
                "Fix the JSON syntax, or run `dsctl template datasource "
                "--ds-version VERSION` to choose a type and add `--type TYPE` "
                "to generate a skeleton for the target cluster version."
            ),
        ) from exc
    return dict(require_json_object(parsed, label="datasource payload"))


def _normalized_update_payload(
    payload: Mapping[str, JsonValue],
    *,
    datasource_id: int,
    current_detail: Mapping[str, JsonValue],
    sensitive_fields: tuple[str, ...],
    blank_password_preserves_existing: bool,
    source_file: Path,
) -> tuple[JsonObject, list[str], list[DataSourceWarningDetail]]:
    normalized_payload = dict(payload)
    _validate_update_payload_id(
        normalized_payload,
        datasource_id=datasource_id,
        source_file=source_file,
    )
    warnings, warning_details = _merge_preserved_secrets(
        normalized_payload,
        current_detail=current_detail,
        sensitive_fields=sensitive_fields,
        blank_password_preserves_existing=blank_password_preserves_existing,
        source_file=source_file,
    )
    return normalized_payload, warnings, warning_details


def _validate_update_payload_id(
    payload: Mapping[str, JsonValue],
    *,
    datasource_id: int,
    source_file: Path,
) -> None:
    payload_id = payload.get("id")
    if payload_id is not None:
        if not isinstance(payload_id, int):
            message = "Datasource update payload id must be an integer"
            raise UserInputError(
                message,
                details={"file": str(source_file)},
                suggestion=(
                    "Set `id` to an integer matching the selected datasource, "
                    "or remove it from the payload."
                ),
            )
        if payload_id != datasource_id:
            message = "Datasource update payload id did not match the selected id"
            raise UserInputError(
                message,
                details={
                    "file": str(source_file),
                    "payload_id": payload_id,
                    "selected_id": datasource_id,
                },
                suggestion=(
                    "Update the payload `id` to match the selected datasource, or "
                    "remove `id` and let the CLI target control the update."
                ),
            )


def _merge_preserved_secrets(
    normalized_payload: JsonObject,
    *,
    current_detail: Mapping[str, JsonValue],
    sensitive_fields: tuple[str, ...],
    blank_password_preserves_existing: bool,
    source_file: Path,
) -> tuple[list[str], list[DataSourceWarningDetail]]:
    warnings: list[str] = []
    warning_details: list[DataSourceWarningDetail] = []
    for field in sensitive_fields:
        supplied = field in normalized_payload
        incoming = normalized_payload.get(field)
        preserve = not supplied or incoming == MASKED_SECRET
        if not preserve:
            continue
        existing = current_detail.get(field)
        if existing is None:
            if supplied:
                message = (
                    f"Datasource update cannot preserve unavailable field {field!r}"
                )
                raise UserInputError(
                    message,
                    details={"file": str(source_file), "field": field},
                    suggestion=(
                        f"Replace the masked {field} placeholder with the real "
                        "value, then retry."
                    ),
                )
            continue
        if existing == MASKED_SECRET:
            if field != "password" or not blank_password_preserves_existing:
                message = f"Datasource update cannot safely recover masked {field!r}"
                raise UserInputError(
                    message,
                    details={"file": str(source_file), "field": field},
                    suggestion=(
                        f"Set {field!r} to its real value in the update payload, "
                        "then retry."
                    ),
                )
            # The exact adapter proves whether blank means "preserve".
            normalized_payload[field] = ""
        else:
            normalized_payload[field] = existing
        if not supplied:
            continue
        message = (
            f"datasource update: masked {field} placeholder detected; "
            f"preserving the existing {field}"
        )
        warnings.append(message)
        warning_details.append(
            DataSourceWarningDetail(
                code=(
                    f"datasource_update_preserved_existing_{_snake_case_field(field)}"
                ),
                message=message,
                field=field,
                reason="masked_placeholder",
                preserved_existing=True,
            )
        )
    return warnings, warning_details


def _snake_case_field(field: str) -> str:
    return "".join(
        f"_{character.lower()}" if character.isupper() else character
        for character in field
    )


def _require_datasource_name(
    payload: Mapping[str, JsonValue],
    *,
    operation: str,
) -> str:
    name = payload.get("name")
    if not isinstance(name, str):
        message = f"Datasource {operation} payload requires string field 'name'"
        raise UserInputError(
            message,
            suggestion=DATASOURCE_PAYLOAD_REVIEW_SUGGESTION,
        )
    return require_non_empty_text(name, label="datasource name")


def _require_datasource_type_text(
    payload: Mapping[str, JsonValue],
    *,
    operation: str,
) -> str:
    datasource_type = payload.get("type")
    if not isinstance(datasource_type, str):
        message = f"Datasource {operation} payload requires string field 'type'"
        raise UserInputError(
            message,
            suggestion=DATASOURCE_PAYLOAD_REVIEW_SUGGESTION,
        )
    return require_non_empty_text(datasource_type, label="datasource type")


def _validated_version_payload(
    payload: Mapping[str, JsonValue],
    *,
    ds_version: str,
    operation: str,
    source_file: Path,
) -> JsonObject:
    normalized_payload = dict(payload)
    datasource_type = _require_datasource_type_text(
        payload,
        operation=operation,
    )
    normalized_type = require_datasource_payload_type(
        datasource_type,
        version=ds_version,
    )
    normalized_payload["type"] = normalized_type
    allowed_fields = set(datasource_payload_field_names(ds_version, normalized_type))
    unexpected_fields = sorted(set(normalized_payload) - allowed_fields)
    if unexpected_fields:
        message = (
            f"Datasource {operation} payload contains fields not accepted by "
            f"DolphinScheduler {ds_version}"
        )
        raise UserInputError(
            message,
            details={
                "file": str(source_file),
                "ds_version": ds_version,
                "type": normalized_type,
                "unexpected_fields": unexpected_fields,
            },
            suggestion=(
                "Remove the unexpected fields or generate an exact-version "
                f"template with `dsctl template datasource --ds-version "
                f"{ds_version} --type {normalized_type}`."
            ),
        )
    return normalized_payload


def _reject_masked_create_secrets(
    payload: Mapping[str, JsonValue],
    *,
    ds_version: str,
    source_file: Path,
) -> None:
    masked_fields = sorted(
        field
        for field in datasource_sensitive_payload_fields(ds_version)
        if payload.get(field) == MASKED_SECRET
    )
    if not masked_fields:
        return
    message = "Datasource create payload must include real secret values"
    raise UserInputError(
        message,
        details={"file": str(source_file), "masked_fields": masked_fields},
        suggestion=(
            "Replace every masked secret placeholder with its real value before "
            "creating the datasource."
        ),
    )


def _require_unchanged_datasource_type(
    payload: Mapping[str, JsonValue],
    *,
    current_detail: Mapping[str, JsonValue],
    ds_version: str,
    source_file: Path,
) -> None:
    requested_type = payload.get("type")
    current_type = current_detail.get("type")
    if requested_type == current_type:
        return
    message = "Datasource update cannot change the datasource type"
    raise UserInputError(
        message,
        details={
            "file": str(source_file),
            "ds_version": ds_version,
            "requested_type": requested_type,
            "current_type": current_type,
        },
        suggestion=(
            "Keep the existing datasource type, or create a new datasource of "
            "the desired type."
        ),
    )


def _redacted_datasource_detail(
    detail: Mapping[str, JsonValue],
    *,
    ds_version: str,
) -> JsonObject:
    sensitive_fields = set(datasource_sensitive_payload_fields(ds_version))
    return {
        key: _redacted_datasource_value(
            value,
            field=key,
            sensitive_fields=sensitive_fields,
        )
        for key, value in detail.items()
    }


def _redacted_datasource_value(
    value: JsonValue,
    *,
    field: str,
    sensitive_fields: set[str],
) -> JsonValue:
    if field in sensitive_fields and value is not None:
        return MASKED_SECRET
    if isinstance(value, Mapping):
        return {
            str(key): _redacted_datasource_value(
                nested,
                field=str(key),
                sensitive_fields=sensitive_fields,
            )
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [
            _redacted_datasource_value(
                nested,
                field=field,
                sensitive_fields=sensitive_fields,
            )
            for nested in value
        ]
    return value


def _payload_json(payload: Mapping[str, JsonValue]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _payload_name(payload: Mapping[str, JsonValue]) -> str | None:
    name = payload.get("name")
    return name if isinstance(name, str) else None


def _resolved_datasource_data(datasource: DataSourceRecord) -> ResolvedDataSourceData:
    datasource_id = datasource.id
    datasource_name = datasource.name
    if datasource_id is None or datasource_name is None:
        message = "Datasource payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": DATASOURCE_RESOURCE},
        )
    return {
        "id": datasource_id,
        "name": datasource_name,
        "note": datasource.note,
        "type": enum_value(datasource.type),
    }


def _translate_datasource_api_error(
    error: ApiResultError,
    *,
    operation: str,
    datasource_id: int | None = None,
    name: str | None = None,
    file: str | None = None,
) -> Exception:
    result_code = error.result_code
    details: dict[str, int | str] = {"operation": operation}
    if datasource_id is not None:
        details["id"] = datasource_id
    if name is not None:
        details["name"] = name
    if file is not None:
        details["file"] = file

    if result_code == RESOURCE_NOT_EXIST:
        identifier = datasource_id if datasource_id is not None else name
        message = f"Datasource {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if result_code == DATASOURCE_EXISTS:
        return ConflictError(error.message, details=details)
    if result_code == USER_NO_OPERATION_PERM:
        return PermissionDeniedError(error.message, details=details)
    if result_code == DESCRIPTION_TOO_LONG_ERROR:
        return UserInputError(
            error.message,
            details=details,
            suggestion="Shorten the datasource description, then retry.",
        )
    if result_code in {
        CREATE_DATASOURCE_ERROR,
        UPDATE_DATASOURCE_ERROR,
        CONNECT_DATASOURCE_FAILURE,
        CONNECTION_TEST_FAILURE,
        DELETE_DATASOURCE_FAILURE,
        VERIFY_DATASOURCE_NAME_FAILURE,
    }:
        return UserInputError(
            error.message,
            details=details,
            suggestion=DATASOURCE_PAYLOAD_REVIEW_SUGGESTION,
        )
    return error


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None
