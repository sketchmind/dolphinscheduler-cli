from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import ACCESS_TOKEN_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._resolver_kernel import collect_resolution_page_items
from dsctl.services._serialization import (
    AccessTokenData,
    optional_text,
    require_resource_int,
    require_resource_text,
    serialize_access_token,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import (
    DEFAULT_RESOLUTION_PAGE_SIZE,
    MAX_RESOLUTION_PAGES,
)
from dsctl.services.resolver import user as resolve_user
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import AccessTokenRecord


USER_NO_OPERATION_PERM = 30001
CREATE_ACCESS_TOKEN_ERROR = 70010
GENERATE_TOKEN_ERROR = 70011
UPDATE_ACCESS_TOKEN_ERROR = 70013
ACCESS_TOKEN_NOT_EXIST = 70015

AccessTokenPageData: TypeAlias = PageData[AccessTokenData]


class AccessTokenSelectionData(TypedDict):
    """Resolved access-token selector emitted in JSON envelopes."""

    id: int
    userId: int
    userName: str | None


class DeleteAccessTokenData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    accessToken: AccessTokenSelectionData


class GeneratedAccessTokenData(TypedDict):
    """CLI token-generation payload."""

    token: str
    userId: int
    expireTime: str


def list_access_tokens_result(
    *,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List access tokens with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    return run_with_service_runtime(
        env_file,
        _list_access_tokens_result,
        search=normalized_search,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
    )


def get_access_token_result(
    access_token_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Get one access token by numeric id."""
    normalized_access_token_id = require_positive_int(
        access_token_id,
        label="access_token_id",
    )
    return run_with_service_runtime(
        env_file,
        _get_access_token_result,
        access_token_id=normalized_access_token_id,
    )


def create_access_token_result(
    *,
    user: str,
    expire_time: str,
    token: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one access token for one resolved user."""
    normalized_user = require_non_empty_text(user, label="user")
    normalized_expire_time = require_non_empty_text(
        expire_time,
        label="expire time",
    )
    normalized_token = (
        None if token is None else require_non_empty_text(token, label="token")
    )
    return run_with_service_runtime(
        env_file,
        _create_access_token_result,
        user=normalized_user,
        expire_time=normalized_expire_time,
        token=normalized_token,
    )


def update_access_token_result(
    access_token_id: int,
    *,
    user: str | None = None,
    expire_time: str | None = None,
    token: str | None = None,
    regenerate_token: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Update one access token while preserving omitted fields."""
    if user is None and expire_time is None and token is None and not regenerate_token:
        message = "Access-token update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --user, --expire-time, "
                "--token, or --regenerate-token."
            ),
        )
    if token is not None and regenerate_token:
        message = "Access-token update cannot use --token with --regenerate-token"
        raise UserInputError(
            message,
            suggestion=(
                "Choose exactly one token source: pass --token explicitly, or use "
                "--regenerate-token to mint a new token."
            ),
        )

    normalized_access_token_id = require_positive_int(
        access_token_id,
        label="access_token_id",
    )
    normalized_user = (
        None if user is None else require_non_empty_text(user, label="user")
    )
    normalized_expire_time = (
        None
        if expire_time is None
        else require_non_empty_text(expire_time, label="expire time")
    )
    normalized_token = (
        None if token is None else require_non_empty_text(token, label="token")
    )
    return run_with_service_runtime(
        env_file,
        _update_access_token_result,
        access_token_id=normalized_access_token_id,
        user=normalized_user,
        expire_time=normalized_expire_time,
        token=normalized_token,
        regenerate_token=regenerate_token,
    )


def delete_access_token_result(
    access_token_id: int,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one access token after explicit confirmation."""
    require_delete_force(force=force, resource_label="Access-token")
    normalized_access_token_id = require_positive_int(
        access_token_id,
        label="access_token_id",
    )
    return run_with_service_runtime(
        env_file,
        _delete_access_token_result,
        access_token_id=normalized_access_token_id,
    )


def generate_access_token_result(
    *,
    user: str,
    expire_time: str,
    env_file: str | None = None,
) -> CommandResult:
    """Generate one token string without persisting it."""
    normalized_user = require_non_empty_text(user, label="user")
    normalized_expire_time = require_non_empty_text(
        expire_time,
        label="expire time",
    )
    return run_with_service_runtime(
        env_file,
        _generate_access_token_result,
        user=normalized_user,
        expire_time=normalized_expire_time,
    )


def _list_access_tokens_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.access_tokens
    data: AccessTokenPageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_access_token,
        resource=ACCESS_TOKEN_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_access_token_api_error(
            error,
            operation="list",
            token_id=None,
            user_id=None,
        ),
    )
    return CommandResult(
        data=require_json_object(data, label="access-token list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_access_token_result(
    runtime: ServiceRuntime,
    *,
    access_token_id: int,
) -> CommandResult:
    access_token = _get_access_token_record(runtime, access_token_id=access_token_id)
    return CommandResult(
        data=require_json_object(
            serialize_access_token(access_token),
            label="access-token data",
        ),
        resolved={
            "accessToken": require_json_object(
                _access_token_selection_data(access_token),
                label="resolved access token",
            )
        },
    )


def _create_access_token_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    expire_time: str,
    token: str | None,
) -> CommandResult:
    resolved_user = resolve_user(user, adapter=runtime.upstream.users)
    try:
        access_token = runtime.upstream.access_tokens.create(
            user_id=resolved_user.id,
            expire_time=expire_time,
            token=token,
        )
    except ApiResultError as error:
        raise _translate_access_token_api_error(
            error,
            operation="create",
            token_id=None,
            user_id=resolved_user.id,
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_access_token(access_token),
            label="access-token data",
        ),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            )
        },
    )


def _update_access_token_result(
    runtime: ServiceRuntime,
    *,
    access_token_id: int,
    user: str | None,
    expire_time: str | None,
    token: str | None,
    regenerate_token: bool,
) -> CommandResult:
    current_access_token = _get_access_token_record(
        runtime,
        access_token_id=access_token_id,
    )
    current_user_id = require_resource_int(
        current_access_token.userId,
        resource=ACCESS_TOKEN_RESOURCE,
        field_name="access_token.userId",
    )
    resolved_user = resolve_user(
        user if user is not None else str(current_user_id),
        adapter=runtime.upstream.users,
    )
    updated_expire_time = (
        expire_time
        if expire_time is not None
        else require_resource_text(
            current_access_token.expireTime,
            resource=ACCESS_TOKEN_RESOURCE,
            field_name="access_token.expireTime",
        )
    )
    updated_token = token
    if updated_token is None and not regenerate_token:
        updated_token = require_resource_text(
            current_access_token.token,
            resource=ACCESS_TOKEN_RESOURCE,
            field_name="access_token.token",
        )
    try:
        access_token = runtime.upstream.access_tokens.update(
            token_id=access_token_id,
            user_id=resolved_user.id,
            expire_time=updated_expire_time,
            token=updated_token,
        )
    except ApiResultError as error:
        raise _translate_access_token_api_error(
            error,
            operation="update",
            token_id=access_token_id,
            user_id=resolved_user.id,
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_access_token(access_token),
            label="access-token data",
        ),
        resolved={
            "accessToken": require_json_object(
                _access_token_selection_data(current_access_token),
                label="resolved access token",
            ),
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            ),
        },
    )


def _delete_access_token_result(
    runtime: ServiceRuntime,
    *,
    access_token_id: int,
) -> CommandResult:
    access_token = _get_access_token_record(runtime, access_token_id=access_token_id)
    try:
        runtime.upstream.access_tokens.delete(token_id=access_token_id)
    except ApiResultError as error:
        raise _translate_access_token_api_error(
            error,
            operation="delete",
            token_id=access_token_id,
            user_id=None,
        ) from error
    return CommandResult(
        data=require_json_object(
            DeleteAccessTokenData(
                deleted=True,
                accessToken=_access_token_selection_data(access_token),
            ),
            label="access-token delete data",
        ),
        resolved={
            "accessToken": require_json_object(
                _access_token_selection_data(access_token),
                label="resolved access token",
            )
        },
    )


def _generate_access_token_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    expire_time: str,
) -> CommandResult:
    resolved_user = resolve_user(user, adapter=runtime.upstream.users)
    try:
        token = runtime.upstream.access_tokens.generate(
            user_id=resolved_user.id,
            expire_time=expire_time,
        )
    except ApiResultError as error:
        raise _translate_access_token_api_error(
            error,
            operation="generate",
            token_id=None,
            user_id=resolved_user.id,
        ) from error
    return CommandResult(
        data=require_json_object(
            GeneratedAccessTokenData(
                token=token,
                userId=resolved_user.id,
                expireTime=expire_time,
            ),
            label="generated access-token data",
        ),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            )
        },
    )


def _get_access_token_record(
    runtime: ServiceRuntime,
    *,
    access_token_id: int,
) -> AccessTokenRecord:
    access_tokens = collect_resolution_page_items(
        fetch_page=lambda page_no, page_size: runtime.upstream.access_tokens.list(
            page_no=page_no,
            page_size=page_size,
            search=None,
        ),
        page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
        max_pages=MAX_RESOLUTION_PAGES,
        safety_message=(
            f"Access-token scan for id {access_token_id} exceeded the resolver "
            "safety limit"
        ),
        safety_details={
            "resource": ACCESS_TOKEN_RESOURCE,
            "id": access_token_id,
        },
    )
    for access_token in access_tokens:
        if access_token.id == access_token_id:
            return access_token
    message = f"Access-token id {access_token_id} was not found"
    raise NotFoundError(
        message,
        details={"resource": ACCESS_TOKEN_RESOURCE, "id": access_token_id},
    )


def _access_token_selection_data(
    access_token: AccessTokenRecord,
) -> AccessTokenSelectionData:
    return AccessTokenSelectionData(
        id=require_resource_int(
            access_token.id,
            resource=ACCESS_TOKEN_RESOURCE,
            field_name="access_token.id",
        ),
        userId=require_resource_int(
            access_token.userId,
            resource=ACCESS_TOKEN_RESOURCE,
            field_name="access_token.userId",
        ),
        userName=access_token.userName,
    )


def _translate_access_token_api_error(
    error: ApiResultError,
    *,
    operation: str,
    token_id: int | None,
    user_id: int | None,
) -> Exception:
    details: dict[str, object] = {
        "resource": ACCESS_TOKEN_RESOURCE,
        "operation": operation,
    }
    if token_id is not None:
        details["id"] = token_id
    if user_id is not None:
        details["user_id"] = user_id
    if error.result_code == USER_NO_OPERATION_PERM:
        return PermissionDeniedError(
            f"Access-token {operation} requires additional permissions",
            details=details,
        )
    if error.result_code == ACCESS_TOKEN_NOT_EXIST and token_id is not None:
        return NotFoundError(
            f"Access-token id {token_id} was not found",
            details={"resource": ACCESS_TOKEN_RESOURCE, "id": token_id},
        )
    if error.result_code in (
        CREATE_ACCESS_TOKEN_ERROR,
        GENERATE_TOKEN_ERROR,
        UPDATE_ACCESS_TOKEN_ERROR,
    ):
        return ConflictError(
            "Access-token request was rejected by the upstream API",
            details=details,
        )
    return error
