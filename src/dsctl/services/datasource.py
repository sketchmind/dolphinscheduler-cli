from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypeAlias, TypedDict

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
from dsctl.services._serialization import (
    DataSourceData,
    serialize_datasource,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.datasource_payload import (
    DATASOURCE_PAYLOAD_REVIEW_SUGGESTION,
    require_datasource_payload_type,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedDataSourceData
from dsctl.services.resolver import datasource as resolve_datasource
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from dsctl.upstream.protocol import DataSourceRecord


MASKED_PASSWORD = "*" * 6

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

DataSourcePageData: TypeAlias = PageData[DataSourceData]


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

    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    payload["type"] = _require_datasource_type(payload, operation="create")
    if "id" in payload:
        message = "Datasource create payload must not include id"
        raise UserInputError(
            message,
            details={"file": str(file)},
            suggestion="Remove `id` from the create payload; DS assigns it.",
        )
    if payload.get("password") == MASKED_PASSWORD:
        message = "Datasource create payload must include the real password"
        raise UserInputError(
            message,
            details={"file": str(file)},
            suggestion=(
                "Replace the masked password placeholder with the real password "
                "before creating the datasource."
            ),
        )

    return run_with_service_runtime(
        env_file,
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
    payload["type"] = _require_datasource_type(payload, operation="update")

    return run_with_service_runtime(
        env_file,
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

    return run_with_service_runtime(
        env_file,
        _delete_datasource_result,
        datasource=datasource,
    )


def connection_test_datasource_result(
    datasource: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Run one datasource connection test."""
    return run_with_service_runtime(
        env_file,
        _test_datasource_result,
        datasource=datasource,
    )


def _list_datasources_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.datasources
    data: DataSourcePageData = requested_page_data(
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
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_datasource_api_error(
            error,
            operation="list",
            name=search,
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="datasource list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_datasource_result(
    runtime: ServiceRuntime,
    *,
    datasource: str,
) -> CommandResult:
    adapter = runtime.upstream.datasources
    resolved_datasource = resolve_datasource(
        datasource,
        adapter=adapter,
    )
    fetched_datasource = adapter.get(datasource_id=resolved_datasource.id)
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
    runtime: ServiceRuntime,
    *,
    payload: dict[str, object],
    source_file: Path,
) -> CommandResult:
    adapter = runtime.upstream.datasources
    payload_json = _payload_json(payload)
    try:
        created_datasource = adapter.create(payload_json=payload_json)
    except ApiResultError as error:
        raise _translate_datasource_api_error(
            error,
            operation="create",
            name=_payload_name(payload),
            file=str(source_file),
        ) from error

    resolved_datasource = _resolved_datasource_data(created_datasource)
    fetched_datasource = adapter.get(datasource_id=resolved_datasource["id"])
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
    runtime: ServiceRuntime,
    *,
    datasource: str,
    payload: dict[str, object],
    source_file: Path,
) -> CommandResult:
    adapter = runtime.upstream.datasources
    resolved_datasource = resolve_datasource(datasource, adapter=adapter)
    normalized_payload, warnings, warning_details = _normalized_update_payload(
        payload,
        datasource_id=resolved_datasource.id,
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

    fetched_datasource = adapter.get(datasource_id=resolved_datasource.id)
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
    runtime: ServiceRuntime,
    *,
    datasource: str,
) -> CommandResult:
    adapter = runtime.upstream.datasources
    resolved_datasource = resolve_datasource(datasource, adapter=adapter)
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
    runtime: ServiceRuntime,
    *,
    datasource: str,
) -> CommandResult:
    adapter = runtime.upstream.datasources
    resolved_datasource = resolve_datasource(datasource, adapter=adapter)
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


def _load_datasource_payload_or_error(path: Path) -> dict[str, object]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        message = f"Could not read datasource payload file: {exc}"
        raise UserInputError(
            message,
            details={"file": str(path)},
            suggestion=(
                "Check that the file exists and is readable, then retry with "
                "`--file PATH`."
            ),
        ) from exc
    except json.JSONDecodeError as exc:
        message = f"Datasource payload file is not valid JSON: {exc.msg}"
        raise UserInputError(
            message,
            details={"file": str(path)},
            suggestion=(
                "Fix the JSON syntax, or run `dsctl template datasource` to choose "
                "a type and `dsctl template datasource --type TYPE` to regenerate "
                "a payload skeleton."
            ),
        ) from exc
    return dict(require_json_object(parsed, label="datasource payload"))


def _normalized_update_payload(
    payload: Mapping[str, object],
    *,
    datasource_id: int,
    source_file: Path,
) -> tuple[dict[str, object], list[str], list[DataSourceWarningDetail]]:
    normalized_payload = dict(payload)
    payload_id = normalized_payload.get("id")
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

    warnings: list[str] = []
    warning_details: list[DataSourceWarningDetail] = []
    if normalized_payload.get("password") == MASKED_PASSWORD:
        normalized_payload["password"] = ""
        message = (
            "datasource update: masked password placeholder detected; "
            "preserving the existing password"
        )
        warnings.append(message)
        warning_details.append(
            DataSourceWarningDetail(
                code="datasource_update_preserved_existing_password",
                message=message,
                field="password",
                reason="masked_placeholder",
                preserved_existing=True,
            )
        )
    return normalized_payload, warnings, warning_details


def _require_datasource_name(
    payload: Mapping[str, object],
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


def _require_datasource_type(
    payload: Mapping[str, object],
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
    normalized_type = require_non_empty_text(datasource_type, label="datasource type")
    return require_datasource_payload_type(normalized_type)


def _payload_json(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _payload_name(payload: Mapping[str, object]) -> str | None:
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
        "type": None if datasource.type is None else datasource.type.value,
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
