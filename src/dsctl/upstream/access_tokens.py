from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Protocol, cast

from dsctl.errors import (
    ApiTransportError,
    NotFoundError,
    UserInputError,
)
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.resolver import user as resolve_user
from dsctl.upstream.response_projection import (
    non_empty_text,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    require_boolean,
    require_none,
    response_field,
    sequence_field,
)
from dsctl.upstream.users import (
    UserIdentity,
    UserIdentityLookup,
    bind_user_identity_lookup,
    bind_user_lookup,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        AccessTokenPageRecord,
        AccessTokenRecord,
        UserReadOperations,
    )
    from dsctl.upstream.resolver import ResolvedUser
    from dsctl.upstream.wire import CompiledWireProfile


_RESOURCE = "access-token"
_EntityResult = Literal["none", "entity"]
_DeleteResult = Literal["none", "boolean"]
_ExpireTimeReadback = Literal["native_date", "request_text"]
_AccessTokenPrimitive = Literal["list", "create", "update", "delete", "generate"]
_ACCESS_TOKEN_PROGRAMS = CompiledDomainPrograms[_AccessTokenPrimitive](
    name="access_token",
    schema_constant="COMPILED_ACCESS_TOKEN_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "list": READ_RETRY_OPTIONAL,
        "create": MUTATION_ONCE_REQUIRED,
        "update": MUTATION_ONCE_REQUIRED,
        "delete": MUTATION_ONCE_REQUIRED,
        "generate": MUTATION_ONCE_REQUIRED,
    },
)


@dataclass(frozen=True)
class AccessTokenSnapshot:
    """Version-neutral access-token projection consumed by stable services."""

    id: int
    userId: int  # noqa: N815
    token: str | None
    expireTime: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    userName: str | None  # noqa: N815


@dataclass(frozen=True)
class AccessTokenSelection:
    """Resolved access token plus the payload returned to the caller."""

    record: AccessTokenRecord


@dataclass(frozen=True)
class AccessTokenMutation:
    """Access-token mutation payload plus its resolved target user."""

    record: AccessTokenRecord
    user: ResolvedUser | UserIdentity
    previous: AccessTokenRecord | None = None


@dataclass(frozen=True)
class AccessTokenDeletion:
    """Verified access-token deletion outcome."""

    record: AccessTokenRecord
    deleted: bool


@dataclass(frozen=True)
class GeneratedToken:
    """Generated, non-persisted token and its resolved owning user."""

    token: str
    user: ResolvedUser | UserIdentity
    expire_time: str


@dataclass(frozen=True)
class _AccessTokenRecipe:
    create_result: _EntityResult
    update_result: _EntityResult
    delete_result: _DeleteResult
    token_required: bool
    expire_time_readback: _ExpireTimeReadback


_RECIPE_REQUIRED_TOKEN = _AccessTokenRecipe(
    create_result="none",
    update_result="none",
    delete_result="none",
    token_required=True,
    expire_time_readback="native_date",
)
_RECIPE_ENTITY_VOID = _AccessTokenRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="none",
    token_required=False,
    expire_time_readback="request_text",
)
_RECIPE_ENTITY_BOOLEAN = _AccessTokenRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    token_required=False,
    expire_time_readback="request_text",
)

_ACCESS_TOKEN_RECIPES = {
    "legacy_139": _RECIPE_REQUIRED_TOKEN,
    "legacy_200": _RECIPE_REQUIRED_TOKEN,
    "entity_void": _RECIPE_ENTITY_VOID,
    "entity_bool": _RECIPE_ENTITY_BOOLEAN,
}

# DS 1.3.9 and every 2.0.x profile expose bare java.util.Date fields while
# configuring only a Jackson timezone. DS 3.0.0 introduces the explicit
# yyyy-MM-dd HH:mm:ss response date format that can round-trip request text.
_NATIVE_DATE_PROFILE_VERSIONS = frozenset(
    {
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
    }
)


class _AccessTokenOperations(Protocol):
    """Token persistence port consumed by the caller-oriented domain."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AccessTokenPageRecord:
        """Return one token page."""

    def get(self, *, token_id: int) -> AccessTokenRecord:
        """Return one token by id."""

    def create(
        self,
        *,
        user_id: int,
        expire_time: str,
        token: str | None,
    ) -> AccessTokenRecord:
        """Create and read back one token."""

    def update(
        self,
        *,
        token_id: int,
        user_id: int,
        expire_time: str,
        token: str | None,
        regenerate_token: bool,
        previous_token: str | None,
    ) -> AccessTokenRecord:
        """Update and read back one token."""

    def delete(self, *, token_id: int) -> bool:
        """Delete and verify one token."""

    def generate(self, *, user_id: int, expire_time: str) -> str:
        """Generate a non-persisted token."""


@dataclass(frozen=True)
class AccessTokenDomain:
    """Caller-oriented token lifecycle with user resolution and preservation."""

    ds_version: str
    tokens: _AccessTokenOperations
    users: UserReadOperations
    identity_users: UserIdentityLookup | None = None
    requires_explicit_expire_time: bool = False

    def _resolve_user(self, selector: str) -> ResolvedUser | UserIdentity:
        if self.identity_users is not None:
            return self.identity_users.resolve(selector)
        return resolve_user(selector, adapter=self.users)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AccessTokenPageRecord:
        """Return one exact projected access-token page."""
        return self.tokens.list(page_no=page_no, page_size=page_size, search=search)

    def get(self, access_token_id: int) -> AccessTokenSelection:
        """Resolve one access token by its native numeric id."""
        return AccessTokenSelection(record=self.tokens.get(token_id=access_token_id))

    def create(
        self,
        *,
        user: str,
        expire_time: str,
        token: str | None,
    ) -> AccessTokenMutation:
        """Resolve a user, create a token, and return verified readback."""
        resolved = self._resolve_user(user)
        return AccessTokenMutation(
            record=self.tokens.create(
                user_id=resolved.id,
                expire_time=expire_time,
                token=token,
            ),
            user=resolved,
        )

    def update(
        self,
        access_token_id: int,
        *,
        user: str | None,
        expire_time: str | None,
        token: str | None,
        regenerate_token: bool,
    ) -> AccessTokenMutation:
        """Preserve omitted token fields and return verified readback."""
        current = self.tokens.get(token_id=access_token_id)
        if expire_time is None and self.requires_explicit_expire_time:
            message = (
                "Access-token update on this DolphinScheduler version requires "
                "an explicit expire time"
            )
            raise UserInputError(
                message,
                suggestion=(
                    "Pass --expire-time in DolphinScheduler's native server-local "
                    "`YYYY-MM-DD HH:MM:SS` format."
                ),
            )
        current_user_id = _required_record_int(current.userId, field="userId")
        resolved = self._resolve_user(
            user if user is not None else str(current_user_id),
        )
        next_expire_time = (
            expire_time
            if expire_time is not None
            else _required_record_text(current.expireTime, field="expireTime")
        )
        next_token = token
        if next_token is None and not regenerate_token:
            next_token = _required_record_text(current.token, field="token")
        if (
            not regenerate_token
            and resolved.id == current_user_id
            and next_expire_time == current.expireTime
            and next_token == current.token
        ):
            message = "Access-token update requires at least one field change"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass a different --user, --expire-time, or --token value, "
                    "or use --regenerate-token."
                ),
            )
        updated = self.tokens.update(
            token_id=access_token_id,
            user_id=resolved.id,
            expire_time=next_expire_time,
            token=next_token,
            regenerate_token=regenerate_token,
            previous_token=current.token,
        )
        return AccessTokenMutation(record=updated, user=resolved, previous=current)

    def delete(self, access_token_id: int) -> AccessTokenDeletion:
        """Delete one resolved token and verify that it is absent."""
        current = self.tokens.get(token_id=access_token_id)
        return AccessTokenDeletion(
            record=current,
            deleted=self.tokens.delete(token_id=access_token_id),
        )

    def generate(self, *, user: str, expire_time: str) -> GeneratedToken:
        """Generate one non-persisted token for a resolved user."""
        resolved = self._resolve_user(user)
        return GeneratedToken(
            token=self.tokens.generate(
                user_id=resolved.id,
                expire_time=expire_time,
            ),
            user=resolved,
            expire_time=expire_time,
        )


class AccessTokenAdapter:
    """Compiled access-token adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed lifecycle recipe."""
        self._profile = _ACCESS_TOKEN_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> AccessTokenAdapter:
        """Return the adapter for one explicitly reviewed version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> AccessTokenDomain:
        """Bind the complete token lifecycle and its user-resolution port."""
        programs = _ACCESS_TOKEN_PROGRAMS.bind(
            self._profile, profile, http_client=http_client
        )
        return AccessTokenDomain(
            ds_version=self.ds_version,
            tokens=_CompiledAccessTokenOperations(programs, self._recipe),
            users=bind_user_lookup(profile, http_client=http_client),
            identity_users=bind_user_identity_lookup(profile, http_client=http_client),
            requires_explicit_expire_time=(
                self._recipe.expire_time_readback == "native_date"
            ),
        )


ACCESS_TOKEN_DOMAIN = BoundDomain[AccessTokenDomain](
    name=_RESOURCE,
    adapter_for_version=AccessTokenAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledAccessTokenOperations:
    programs: BoundCompiledPrograms[_AccessTokenPrimitive]
    recipe: _AccessTokenRecipe

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AccessTokenPageRecord:
        page = self.programs.call(
            "list", {"pageNo": page_no, "pageSize": page_size, "searchVal": search}
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        )
        projected = [
            _access_token_snapshot(item, ds_version=self.ds_version) for item in items
        ]
        return cast(
            "AccessTokenPageRecord",
            project_page(
                page,
                projected,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_RESOURCE,
            ),
        )

    def list_all(self) -> Sequence[AccessTokenRecord]:
        return cast(
            "Sequence[AccessTokenRecord]",
            collect_pages(self.list, resource=_RESOURCE),
        )

    def get(self, *, token_id: int) -> AccessTokenRecord:
        for item in self.list_all():
            if item.id == token_id:
                return item
        message = f"Access-token id {token_id} was not found"
        raise NotFoundError(
            message,
            details={"resource": _RESOURCE, "id": token_id},
        )

    def create(
        self,
        *,
        user_id: int,
        expire_time: str,
        token: str | None,
    ) -> AccessTokenRecord:
        resolved_token = token
        if self.recipe.token_required and resolved_token is None:
            resolved_token = self.generate(user_id=user_id, expire_time=expire_time)
        before_ids = {item.id for item in self.list_all()}
        result = mutation_call(
            lambda: self.programs.call(
                "create",
                {
                    "userId": user_id,
                    "expireTime": expire_time,
                    "token": resolved_token,
                },
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="create",
        )

        def verify() -> AccessTokenRecord:
            _require_entity_result(
                result,
                expected=self.recipe.create_result,
                operation="create",
                ds_version=self.ds_version,
            )
            returned_id = _optional_returned_id(result, ds_version=self.ds_version)
            candidates = [
                item
                for item in self.list_all()
                if item.id not in before_ids
                and item.userId == user_id
                and _matches_expire_time(
                    item.expireTime,
                    requested=expire_time,
                    mode=self.recipe.expire_time_readback,
                )
                and (resolved_token is None or item.token == resolved_token)
                and (returned_id is None or item.id == returned_id)
            ]
            if len(candidates) != 1:
                message = (
                    "Access-token create readback did not return one exact new token"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "user_id": user_id,
                        "expire_time": expire_time,
                        "match_count": len(candidates),
                    },
                )
            return candidates[0]

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="create",
        )

    def update(
        self,
        *,
        token_id: int,
        user_id: int,
        expire_time: str,
        token: str | None,
        regenerate_token: bool,
        previous_token: str | None,
    ) -> AccessTokenRecord:
        resolved_token = token
        if regenerate_token and self.recipe.token_required:
            resolved_token = self.generate(user_id=user_id, expire_time=expire_time)
        result = mutation_call(
            lambda: self.programs.call(
                "update",
                {
                    "id": token_id,
                    "userId": user_id,
                    "expireTime": expire_time,
                    "token": resolved_token,
                },
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="update",
        )

        def verify() -> AccessTokenRecord:
            _require_entity_result(
                result,
                expected=self.recipe.update_result,
                operation="update",
                ds_version=self.ds_version,
            )
            updated = self.get(token_id=token_id)
            if updated.userId != user_id or not _matches_expire_time(
                updated.expireTime,
                requested=expire_time,
                mode=self.recipe.expire_time_readback,
            ):
                message = "Access-token update readback did not match requested fields"
                raise ApiTransportError(message, details={"id": token_id})
            if resolved_token is not None and updated.token != resolved_token:
                message = "Access-token update readback did not match requested token"
                raise ApiTransportError(message, details={"id": token_id})
            if regenerate_token and (
                not updated.token or updated.token == previous_token
            ):
                message = "Access-token regeneration did not produce a fresh token"
                raise ApiTransportError(message, details={"id": token_id})
            return updated

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="update",
        )

    def delete(self, *, token_id: int) -> bool:
        result = mutation_call(
            lambda: self.programs.call("delete", {"id": token_id}),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="delete",
        )

        def verify() -> bool:
            if self.recipe.delete_result == "none":
                require_none(
                    result,
                    ds_version=self.ds_version,
                    resource=_RESOURCE,
                    field="deleteResult",
                )
            else:
                # DS 3.2.1+ deletes first and then returns Result.success(false).
                # The scalar proves the exact response shape, while the fresh list
                # read below proves deletion; its truth value is not a deletion flag.
                require_boolean(
                    result,
                    ds_version=self.ds_version,
                    resource=_RESOURCE,
                    field="deleteResult",
                )
            if any(item.id == token_id for item in self.list_all()):
                message = "Access-token delete readback still returned the deleted id"
                raise ApiTransportError(message, details={"id": token_id})
            return True

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="delete",
        )

    def generate(self, *, user_id: int, expire_time: str) -> str:
        # This non-persisting POST still uses a once-only program: never retry it.
        result = self.programs.call(
            "generate", {"userId": user_id, "expireTime": expire_time}
        )
        return non_empty_text(
            result,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            field="token",
        )


def _recipe_for_profile(profile: CompiledWireProfile) -> _AccessTokenRecipe:
    try:
        recipe = _ACCESS_TOKEN_RECIPES[profile.recipe_id or ""]
    except KeyError as exc:
        message = f"Compiled access-token recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc
    if profile.ds_version in _NATIVE_DATE_PROFILE_VERSIONS:
        return replace(recipe, expire_time_readback="native_date")
    return recipe


def access_token_update_requires_expire_time(ds_version: str) -> bool:
    """Return whether exact update cannot safely preserve an omitted expiry."""
    profile = _ACCESS_TOKEN_PROGRAMS.profile(ds_version)
    return _recipe_for_profile(profile).expire_time_readback == "native_date"


def _access_token_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> AccessTokenSnapshot:
    return AccessTokenSnapshot(
        id=positive_int(
            response_field(item, "id", ds_version=ds_version, resource=_RESOURCE),
            ds_version=ds_version,
            resource=_RESOURCE,
            field="id",
        ),
        userId=positive_int(
            response_field(item, "userId", ds_version=ds_version, resource=_RESOURCE),
            ds_version=ds_version,
            resource=_RESOURCE,
            field="userId",
        ),
        token=optional_text_field(
            item, "token", ds_version=ds_version, resource=_RESOURCE
        ),
        expireTime=optional_text_field(
            item, "expireTime", ds_version=ds_version, resource=_RESOURCE
        ),
        createTime=optional_text_field(
            item, "createTime", ds_version=ds_version, resource=_RESOURCE
        ),
        updateTime=optional_text_field(
            item, "updateTime", ds_version=ds_version, resource=_RESOURCE
        ),
        userName=optional_text_field(
            item, "userName", ds_version=ds_version, resource=_RESOURCE
        ),
    )


def _optional_returned_id(
    result: OpaqueGeneratedValue, *, ds_version: str
) -> int | None:
    if result is None:
        return None
    return positive_int(
        response_field(result, "id", ds_version=ds_version, resource=_RESOURCE),
        ds_version=ds_version,
        resource=_RESOURCE,
        field="id",
    )


def _matches_expire_time(
    observed: str | None,
    *,
    requested: str,
    mode: _ExpireTimeReadback,
) -> bool:
    if observed is None or observed == "":
        return False
    if mode == "request_text":
        return observed == requested
    # Legacy controllers parse a naive server-local request into java.util.Date;
    # their JSON list response serializes the same instant with an explicit offset.
    # Exact id/user/token fields identify the same create or update. Validate the
    # native date structure without inventing a server timezone to compare unlike
    # representations of the same instant.
    if len(observed) < 19 or observed[10] not in {" ", "T"}:
        return False
    try:
        datetime.fromisoformat(observed)
    except ValueError:
        return False
    return True


def _require_entity_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _EntityResult,
    operation: str,
    ds_version: str,
) -> None:
    if expected == "none":
        require_none(
            result,
            ds_version=ds_version,
            resource=_RESOURCE,
            field=f"{operation}Result",
        )
        return
    if result is None:
        raise projection_error(
            ds_version=ds_version,
            resource=_RESOURCE,
            field=f"{operation}Result",
            reason="entity-returning operation returned null",
        )


def _required_record_int(value: OpaqueGeneratedValue, *, field: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    message = f"Access-token payload was missing required field {field!r}"
    raise ApiTransportError(
        message,
        details={"resource": _RESOURCE, "field": field},
    )


def _required_record_text(value: OpaqueGeneratedValue, *, field: str) -> str:
    if isinstance(value, str) and value:
        return value
    message = f"Access-token payload was missing required field {field!r}"
    raise ApiTransportError(
        message,
        details={"resource": _RESOURCE, "field": field},
    )


__all__ = [
    "ACCESS_TOKEN_DOMAIN",
    "AccessTokenAdapter",
    "AccessTokenDeletion",
    "AccessTokenDomain",
    "AccessTokenMutation",
    "AccessTokenSelection",
    "AccessTokenSnapshot",
    "GeneratedToken",
    "access_token_update_requires_expire_time",
]
