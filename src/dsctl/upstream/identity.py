from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from dsctl.errors import ApiTransportError
from dsctl.upstream.users import _USER_PROGRAMS, _recipe_for_profile

if TYPE_CHECKING:
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.protocol import (
        CurrentUserOperations,
        CurrentUserRecord,
        StringEnumValue,
    )
    from dsctl.upstream.users import _UserPrimitive


@dataclass(frozen=True)
class CurrentUserSnapshot:
    """Version-neutral authenticated-user fields consumed above the DS seam."""

    userName: str | None  # noqa: N815
    userType: StringEnumValue | None  # noqa: N815
    tenantCode: str | None  # noqa: N815
    queueName: str | None  # noqa: N815
    queue: str | None
    timeZone: str | None  # noqa: N815


class IdentityAdapter:
    """Project authenticated identity through the exact compiled user wire."""

    def __init__(self, ds_version: str) -> None:
        """Select the reviewed user profile without creating clients or doing I/O."""
        self._profile = _USER_PROGRAMS.profile(ds_version)
        self._has_time_zone = _recipe_for_profile(self._profile).has_time_zone
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> IdentityAdapter:
        """Return the identity adapter for one explicitly reviewed version."""
        return cls(ds_version)

    def bind_identity(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> CurrentUserOperations:
        """Bind the authenticated-user operation for the exact profile."""
        return cast(
            "CurrentUserOperations",
            _CompiledIdentityOperations(
                _USER_PROGRAMS.bind(self._profile, profile, http_client=http_client),
                self._has_time_zone,
            ),
        )


@dataclass(frozen=True)
class _CompiledIdentityOperations:
    programs: BoundCompiledPrograms[_UserPrimitive]
    has_time_zone: bool

    def current(self) -> CurrentUserRecord:
        return _project_current_user(
            self.programs.call("current", {}),
            ds_version=self.programs.ds_version,
            has_time_zone=self.has_time_zone,
        )


def _project_current_user(
    payload: OpaqueGeneratedValue,
    *,
    ds_version: str,
    has_time_zone: bool,
) -> CurrentUserSnapshot:
    """Project one generated User while making legacy absence explicit."""
    return CurrentUserSnapshot(
        userName=_optional_text(payload, "userName", ds_version=ds_version),
        userType=_optional_enum(payload, "userType", ds_version=ds_version),
        tenantCode=_optional_text(payload, "tenantCode", ds_version=ds_version),
        queueName=_optional_text(payload, "queueName", ds_version=ds_version),
        queue=_optional_text(payload, "queue", ds_version=ds_version),
        timeZone=(
            _optional_text(payload, "timeZone", ds_version=ds_version)
            if has_time_zone
            else None
        ),
    )


def _optional_text(
    payload: OpaqueGeneratedValue, field: str, *, ds_version: str
) -> str | None:
    value = _response_field(payload, field, ds_version=ds_version)
    if value is None or isinstance(value, str):
        return value
    raise _projection_error(
        ds_version=ds_version,
        field=field,
        reason="current-user field is not text or null",
    )


def _optional_enum(
    payload: OpaqueGeneratedValue,
    field: str,
    *,
    ds_version: str,
) -> StringEnumValue | None:
    value = _response_field(payload, field, ds_version=ds_version)
    if value is None:
        return None
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, str):
        return cast("StringEnumValue", value)
    raise _projection_error(
        ds_version=ds_version,
        field=field,
        reason="current-user enum field has no string value",
    )


def _response_field(
    payload: OpaqueGeneratedValue, field: str, *, ds_version: str
) -> OpaqueGeneratedValue:
    try:
        return getattr(payload, field)
    except AttributeError as exc:
        raise _projection_error(
            ds_version=ds_version,
            field=field,
            reason="generated current-user payload is missing a canonical field",
        ) from exc


def _projection_error(
    *,
    ds_version: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    return ApiTransportError(
        "DolphinScheduler response cannot be projected to the current-user contract.",
        details={
            "ds_version": ds_version,
            "resource": "current_user",
            "field": field,
            "reason": reason,
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=(
            "Verify DS_VERSION matches the server and retry after checking API health."
        ),
    )


__all__ = ["CurrentUserSnapshot", "IdentityAdapter"]
