from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import RESOURCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import ResourceItemData, serialize_resource_item
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_non_negative_int,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import ResourceOperations


CREATE_RESOURCE_ERROR = 10054
QUERY_RESOURCES_LIST_ERROR = 10056
QUERY_RESOURCES_LIST_PAGING = 10057
DELETE_RESOURCE_ERROR = 10058
VIEW_RESOURCE_FILE_ON_LINE_ERROR = 10060
CREATE_RESOURCE_FILE_ON_LINE_ERROR = 10061
RESOURCE_FILE_IS_EMPTY = 10062
DOWNLOAD_RESOURCE_FILE_ERROR = 10064
RESOURCE_NOT_EXIST = 20004
RESOURCE_EXIST = 20005
RESOURCE_FILE_EXIST = 20011
RESOURCE_FILE_NOT_EXIST = 20012
PARENT_RESOURCE_NOT_EXIST = 20015
RESOURCE_NOT_EXIST_OR_NO_PERMISSION = 20016
USER_NO_OPERATION_PERM = 30001

ResourcePageData: TypeAlias = PageData[ResourceItemData]


class ResourceContentData(TypedDict):
    """CLI text view payload for one resource file."""

    content: str | None


class ResolvedResourceData(TypedDict):
    """Stable CLI metadata for one resource target."""

    fullName: str
    name: str
    directory: str | None
    isDirectory: bool | None


class DeleteResourceData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    resource: ResolvedResourceData


class DownloadResourceData(TypedDict):
    """CLI download confirmation payload."""

    fullName: str
    saved_to: str
    size: int
    content_type: str | None


def list_resources_result(
    *,
    env_file: str | None = None,
    directory: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List resources inside one DS directory."""
    normalized_search = _optional_text(search)
    normalized_directory = _optional_text(directory)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_resources_result,
        directory=normalized_directory,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def view_resource_result(
    resource: str,
    *,
    skip_line_num: int = 0,
    limit: int = 100,
    env_file: str | None = None,
) -> CommandResult:
    """View one text content window for one resource file."""
    normalized_resource = _resource_path(resource)
    require_non_negative_int(skip_line_num, label="skip_line_num")
    require_positive_int(limit, label="limit")

    return run_with_service_runtime(
        env_file,
        _view_resource_result,
        resource=normalized_resource,
        skip_line_num=skip_line_num,
        limit=limit,
    )


def upload_resource_result(
    *,
    file: Path,
    directory: str | None = None,
    name: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Upload one local file into one DS directory."""
    normalized_directory = _optional_text(directory)
    normalized_name = (
        _resource_leaf_name_or_error(name, label="resource name")
        if name is not None
        else _resource_leaf_name_or_error(file.name, label="resource name")
    )

    return run_with_service_runtime(
        env_file,
        _upload_resource_result,
        file=file,
        directory=normalized_directory,
        name=normalized_name,
    )


def create_resource_result(
    *,
    name: str,
    content: str,
    directory: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one text resource from inline content."""
    normalized_directory = _optional_text(directory)
    normalized_name = _resource_leaf_name_or_error(name, label="resource name")
    if content == "":
        message = "resource content must not be empty"
        raise UserInputError(
            message,
            suggestion=(
                "Pass non-empty inline content, or use `resource upload` if the "
                "content already lives in a local file."
            ),
        )
    file_name, suffix = _split_resource_name(normalized_name)

    return run_with_service_runtime(
        env_file,
        _create_resource_result,
        name=normalized_name,
        file_name=file_name,
        suffix=suffix,
        content=content,
        directory=normalized_directory,
    )


def mkdir_resource_result(
    *,
    name: str,
    directory: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one directory inside one DS resource directory."""
    normalized_directory = _optional_text(directory)
    normalized_name = _resource_leaf_name_or_error(name, label="directory name")

    return run_with_service_runtime(
        env_file,
        _mkdir_resource_result,
        name=normalized_name,
        directory=normalized_directory,
    )


def download_resource_result(
    resource: str,
    *,
    output: Path | None = None,
    overwrite: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Download one remote resource to one local file path."""
    normalized_resource = _resource_path(resource)

    return run_with_service_runtime(
        env_file,
        _download_resource_result,
        resource=normalized_resource,
        output=output,
        overwrite=overwrite,
    )


def delete_resource_result(
    resource: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one resource after explicit confirmation."""
    is_directory = _resource_delete_kind(resource)
    normalized_resource = _resource_path(resource)
    require_delete_force(force=force, resource_label="Resource")

    return run_with_service_runtime(
        env_file,
        _delete_resource_result,
        resource=normalized_resource,
        is_directory=is_directory,
    )


def _list_resources_result(
    runtime: ServiceRuntime,
    *,
    directory: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.resources
    resolved_directory = _effective_directory(adapter, directory)
    data: ResourcePageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            directory=resolved_directory,
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_resource_item,
        resource=RESOURCE_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_resource_api_error(
            error,
            operation="list",
            directory=resolved_directory,
        ),
    )
    return CommandResult(
        data=require_json_object(data, label="resource list data"),
        resolved={
            "directory": resolved_directory,
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _view_resource_result(
    runtime: ServiceRuntime,
    *,
    resource: str,
    skip_line_num: int,
    limit: int,
) -> CommandResult:
    adapter = runtime.upstream.resources
    try:
        payload = adapter.view(
            full_name=resource,
            skip_line_num=skip_line_num,
            limit=limit,
        )
    except ApiResultError as error:
        raise _translate_resource_api_error(
            error,
            operation="view",
            full_name=resource,
        ) from error
    data: ResourceContentData = {"content": payload.content}
    return CommandResult(
        data=require_json_object(data, label="resource view data"),
        resolved={
            "resource": require_json_object(
                _resolved_resource_data(resource, is_directory=False),
                label="resolved resource",
            ),
            "skip_line_num": skip_line_num,
            "limit": limit,
        },
    )


def _upload_resource_result(
    runtime: ServiceRuntime,
    *,
    file: Path,
    directory: str | None,
    name: str,
) -> CommandResult:
    adapter = runtime.upstream.resources
    upload_size = 0
    resolved_directory = _effective_directory(adapter, directory)
    try:
        upload_size = file.stat().st_size
        with file.open("rb") as upload_file:
            adapter.upload(
                current_dir=resolved_directory,
                name=name,
                file=upload_file,
            )
    except OSError as exc:
        message = f"Could not read upload file: {exc}"
        raise UserInputError(
            message,
            details={"file": str(file)},
            suggestion=(
                "Check that the local file exists and is readable, then retry "
                "the upload."
            ),
        ) from exc
    except ApiResultError as error:
        raise _translate_resource_api_error(
            error,
            operation="upload",
            directory=resolved_directory,
            name=name,
        ) from error

    created = _resource_item_data_from_target(
        full_name=_join_resource_path(resolved_directory, name),
        is_directory=False,
        size=upload_size,
    )
    return CommandResult(
        data=require_json_object(created, label="resource upload data"),
        resolved={
            "resource": require_json_object(
                _resolved_resource_data(created["fullName"], is_directory=False),
                label="resolved resource",
            ),
            "source_file": str(file),
        },
    )


def _create_resource_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    file_name: str,
    suffix: str,
    content: str,
    directory: str | None,
) -> CommandResult:
    adapter = runtime.upstream.resources
    resolved_directory = _effective_directory(adapter, directory)
    try:
        adapter.create_from_content(
            current_dir=resolved_directory,
            file_name=file_name,
            suffix=suffix,
            content=content,
        )
    except ApiResultError as error:
        raise _translate_resource_api_error(
            error,
            operation="create",
            directory=resolved_directory,
            name=name,
        ) from error

    created = _resource_item_data_from_target(
        full_name=_join_resource_path(resolved_directory, name),
        is_directory=False,
        size=len(content.encode("utf-8")),
    )
    return CommandResult(
        data=require_json_object(created, label="resource create data"),
        resolved={
            "resource": require_json_object(
                _resolved_resource_data(created["fullName"], is_directory=False),
                label="resolved resource",
            )
        },
    )


def _mkdir_resource_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    directory: str | None,
) -> CommandResult:
    adapter = runtime.upstream.resources
    resolved_directory = _effective_directory(adapter, directory)
    try:
        adapter.create_directory(
            current_dir=resolved_directory,
            name=name,
        )
    except ApiResultError as error:
        raise _translate_resource_api_error(
            error,
            operation="mkdir",
            directory=resolved_directory,
            name=name,
        ) from error

    created = _resource_item_data_from_target(
        full_name=_join_resource_path(resolved_directory, name),
        is_directory=True,
    )
    return CommandResult(
        data=require_json_object(created, label="resource mkdir data"),
        resolved={
            "resource": require_json_object(
                _resolved_resource_data(created["fullName"], is_directory=True),
                label="resolved resource",
            )
        },
    )


def _download_resource_result(
    runtime: ServiceRuntime,
    *,
    resource: str,
    output: Path | None,
    overwrite: bool,
) -> CommandResult:
    adapter = runtime.upstream.resources
    try:
        payload = adapter.download(full_name=resource)
    except ApiResultError as error:
        raise _translate_resource_api_error(
            error,
            operation="download",
            full_name=resource,
        ) from error

    target_path = _download_target_path(resource, output)
    if target_path.exists() and not overwrite:
        message = "Download output path already exists; pass --overwrite to replace it"
        raise UserInputError(
            message,
            details={"output": str(target_path)},
            suggestion="Retry with --overwrite, or choose a different --output path.",
        )
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(payload.content)
    except OSError as exc:
        message = f"Could not write downloaded resource: {exc}"
        raise UserInputError(
            message,
            details={"output": str(target_path)},
            suggestion="Choose a writable --output path, then retry the download.",
        ) from exc

    data: DownloadResourceData = {
        "fullName": resource,
        "saved_to": str(target_path),
        "size": len(payload.content),
        "content_type": payload.content_type,
    }
    return CommandResult(
        data=require_json_object(data, label="resource download data"),
        resolved={
            "resource": require_json_object(
                _resolved_resource_data(resource, is_directory=False),
                label="resolved resource",
            )
        },
    )


def _delete_resource_result(
    runtime: ServiceRuntime,
    *,
    resource: str,
    is_directory: bool | None,
) -> CommandResult:
    adapter = runtime.upstream.resources
    try:
        deleted = adapter.delete(full_name=resource)
    except ApiResultError as error:
        raise _translate_resource_api_error(
            error,
            operation="delete",
            full_name=resource,
        ) from error

    resource_data = _resolved_resource_data(resource, is_directory=is_directory)
    data: DeleteResourceData = {"deleted": deleted, "resource": resource_data}
    return CommandResult(
        data=require_json_object(data, label="resource delete data"),
        resolved={
            "resource": require_json_object(
                resource_data,
                label="resolved resource",
            )
        },
    )


def _effective_directory(adapter: ResourceOperations, directory: str | None) -> str:
    if directory is None:
        return _resource_path(adapter.base_dir())
    return _resource_path(directory)


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _resource_path(value: str) -> str:
    normalized = require_non_empty_text(value, label="resource path")
    if normalized == "/":
        return normalized
    return normalized.rstrip("/")


def _resource_leaf_name_or_error(value: str, *, label: str) -> str:
    normalized = require_non_empty_text(value, label=label)
    if "/" in normalized:
        message = f"{label} must be one leaf name, not a path"
        raise UserInputError(
            message,
            details={"value": normalized},
            suggestion=(
                "Pass only the leaf name here, and use `--directory` to choose "
                "the parent resource directory."
            ),
        )
    return normalized


def _split_resource_name(name: str) -> tuple[str, str]:
    file_name, dot, suffix = name.rpartition(".")
    if dot == "":
        message = "resource name must include one file extension"
        raise UserInputError(
            message,
            details={"name": name},
            suggestion="Rename the resource to include an extension such as `.sql`.",
        )
    if suffix == "":
        message = "resource name must include one non-empty file extension"
        raise UserInputError(
            message,
            details={"name": name},
            suggestion="Use a resource name with a non-empty extension such as `.py`.",
        )
    return file_name, suffix


def _join_resource_path(directory: str, name: str) -> str:
    if directory == "/":
        return f"/{name}"
    return f"{directory.rstrip('/')}/{name}"


def _resource_name(full_name: str) -> str:
    normalized = full_name.rstrip("/")
    name = normalized.rsplit("/", 1)[-1]
    if not name:
        message = "resource path must identify one file or directory name"
        raise UserInputError(
            message,
            details={"fullName": full_name},
            suggestion=(
                "Pass a concrete resource path such as `/tenant/resources/demo.sql`, "
                "not the root directory."
            ),
        )
    return name


def _resource_directory(full_name: str) -> str | None:
    normalized = full_name.rstrip("/")
    if "/" not in normalized:
        return None
    directory = normalized.rsplit("/", 1)[0]
    return directory or "/"


def _resource_delete_kind(value: str) -> bool | None:
    normalized = require_non_empty_text(value, label="resource path")
    if normalized == "/":
        return True
    return True if normalized.endswith("/") else None


def _resolved_resource_data(
    full_name: str | None,
    *,
    is_directory: bool | None,
) -> ResolvedResourceData:
    if not isinstance(full_name, str):
        message = "resource full name must be present"
        raise UserInputError(
            message,
            suggestion="Retry the resource command with a concrete full resource path.",
        )
    normalized_full_name = _resource_path(full_name)
    return {
        "fullName": normalized_full_name,
        "name": _resource_name(normalized_full_name),
        "directory": _resource_directory(normalized_full_name),
        "isDirectory": is_directory,
    }


def _resource_item_data_from_target(
    *,
    full_name: str,
    is_directory: bool,
    size: int = 0,
) -> ResourceItemData:
    name = _resource_name(full_name)
    return {
        "alias": name,
        "userName": None,
        "fileName": name,
        "fullName": full_name,
        "isDirectory": is_directory,
        "type": "FILE",
        "size": size,
        "createTime": None,
        "updateTime": None,
    }


def _download_target_path(resource: str, output: Path | None) -> Path:
    leaf_name = _resource_name(resource)
    if output is None:
        return (Path.cwd() / leaf_name).resolve()

    output_path = output.expanduser()
    if output_path.exists() and output_path.is_dir():
        return (output_path / leaf_name).resolve()
    return output_path.resolve()


def _translate_resource_api_error(
    error: ApiResultError,
    *,
    operation: str,
    full_name: str | None = None,
    directory: str | None = None,
    name: str | None = None,
) -> Exception:
    details: dict[str, str] = {"operation": operation}
    if full_name is not None:
        details["fullName"] = full_name
    if directory is not None:
        details["directory"] = directory
    if name is not None:
        details["name"] = name

    if error.result_code in {RESOURCE_EXIST, RESOURCE_FILE_EXIST}:
        target_name = name or full_name or "resource"
        message = f"Resource {target_name!r} already exists"
        return ConflictError(message, details=details)
    if error.result_code in {
        RESOURCE_NOT_EXIST,
        RESOURCE_FILE_NOT_EXIST,
        PARENT_RESOURCE_NOT_EXIST,
    }:
        target_name = full_name or directory or name or "resource"
        message = f"Resource {target_name!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == RESOURCE_NOT_EXIST_OR_NO_PERMISSION:
        target_name = full_name or directory or name or "resource"
        message = f"Resource {target_name!r} was not found or is not accessible"
        return NotFoundError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = "Resource operation requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code == RESOURCE_FILE_IS_EMPTY:
        message = "Resource content must not be empty"
        return UserInputError(
            message,
            details=details,
            suggestion=(
                "Provide non-empty file content before retrying the resource "
                "create or upload operation."
            ),
        )
    return error
