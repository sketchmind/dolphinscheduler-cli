from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.errors import ConflictError, UserInputError
from dsctl.output import require_json_object
from dsctl.services._serialization import optional_text
from dsctl.services.selection import SelectedValue

if TYPE_CHECKING:
    from dsctl.services.runtime import ServiceRuntime
    from dsctl.support.yaml_io import JsonObject


@dataclass(frozen=True)
class ProjectPreferenceDefaults:
    """Normalized project-level defaults loaded from DS project preference."""

    task_priority: str | None = None
    warning_type: str | None = None
    worker_group: str | None = None
    tenant_code: str | None = None
    environment_code: int | None = None
    warning_group_id: int | None = None


def select_worker_group(
    explicit_worker_group: str | None,
    *,
    project_preference: ProjectPreferenceDefaults | None = None,
) -> SelectedValue:
    """Resolve one worker-group input plus the source that supplied it."""
    normalized_flag = optional_text(explicit_worker_group)
    if normalized_flag is not None:
        return SelectedValue(value=normalized_flag, source="flag")

    if project_preference is not None and project_preference.worker_group is not None:
        return SelectedValue(
            value=project_preference.worker_group,
            source="project_preference",
        )

    return SelectedValue(value="default", source="default")


def select_tenant_code(
    explicit_tenant_code: str | None,
    *,
    runtime: ServiceRuntime,
    project_preference: ProjectPreferenceDefaults | None = None,
) -> SelectedValue:
    """Resolve one tenant-code input plus the source that supplied it."""
    normalized_flag = optional_text(explicit_tenant_code)
    if normalized_flag is not None:
        return SelectedValue(value=normalized_flag, source="flag")

    if project_preference is not None and project_preference.tenant_code is not None:
        return SelectedValue(
            value=project_preference.tenant_code,
            source="project_preference",
        )

    normalized_current_user = runtime.current_user_defaults.tenant_code
    if normalized_current_user is not None:
        return SelectedValue(value=normalized_current_user, source="current_user")

    return SelectedValue(value="default", source="default")


def selected_tenant_code(
    explicit_tenant_code: str | None,
    *,
    runtime: ServiceRuntime,
    project_preference: ProjectPreferenceDefaults | None = None,
) -> str:
    """Resolve one tenant-code input against local and remote defaults."""
    return select_tenant_code(
        explicit_tenant_code,
        runtime=runtime,
        project_preference=project_preference,
    ).value


def load_project_preference_defaults(
    runtime: ServiceRuntime,
    *,
    project_code: int,
) -> ProjectPreferenceDefaults | None:
    """Return enabled project-preference defaults for one resolved project."""
    project_preference = runtime.upstream.project_preferences.get(
        project_code=project_code
    )
    if project_preference is None or project_preference.state != 1:
        return None

    preferences_text = optional_text(project_preference.preferences)
    if preferences_text is None:
        return None

    try:
        decoded_preferences = json.loads(preferences_text)
    except json.JSONDecodeError as error:
        message = "Stored project preference must be valid JSON"
        raise ConflictError(
            message,
            details={
                "projectCode": project_code,
                "field": "preferences",
            },
            suggestion=(
                "Fix the remote value with `dsctl project-preference update` "
                "before retrying."
            ),
        ) from error

    try:
        preferences = require_json_object(
            decoded_preferences,
            label="stored project preference",
        )
    except UserInputError as error:
        message = "Stored project preference must be one JSON object"
        raise ConflictError(
            message,
            details={
                "projectCode": project_code,
                "field": "preferences",
            },
            suggestion=(
                "Fix the remote value with `dsctl project-preference update` "
                "before retrying."
            ),
        ) from error
    return ProjectPreferenceDefaults(
        task_priority=_optional_preference_text(
            preferences,
            field_name="taskPriority",
            project_code=project_code,
        ),
        warning_type=_optional_preference_text(
            preferences,
            field_name="warningType",
            project_code=project_code,
        ),
        worker_group=_optional_preference_text(
            preferences,
            field_name="workerGroup",
            project_code=project_code,
        ),
        tenant_code=_optional_preference_text(
            preferences,
            field_name="tenant",
            project_code=project_code,
        ),
        environment_code=_optional_preference_int(
            preferences,
            field_name="environmentCode",
            project_code=project_code,
        ),
        warning_group_id=_optional_alert_group_id(
            preferences,
            project_code=project_code,
        ),
    )


def _optional_preference_text(
    preferences: JsonObject,
    *,
    field_name: str,
    project_code: int,
) -> str | None:
    value = preferences.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        message = f"Stored project preference field {field_name} must be a string"
        raise ConflictError(
            message,
            details={
                "projectCode": project_code,
                "field": field_name,
                "actual_type": type(value).__name__,
            },
            suggestion=(
                "Fix the remote value with `dsctl project-preference update` "
                "before retrying."
            ),
        )
    return optional_text(value)


def _optional_preference_int(
    preferences: JsonObject,
    *,
    field_name: str,
    project_code: int,
) -> int | None:
    value = preferences.get(field_name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        message = f"Stored project preference field {field_name} must be an integer"
        raise ConflictError(
            message,
            details={
                "projectCode": project_code,
                "field": field_name,
                "actual_type": type(value).__name__,
            },
            suggestion=(
                "Fix the remote value with `dsctl project-preference update` "
                "before retrying."
            ),
        )
    if value < 0:
        message = f"Stored project preference field {field_name} must be non-negative"
        raise ConflictError(
            message,
            details={
                "projectCode": project_code,
                "field": field_name,
                "value": value,
            },
            suggestion=(
                "Fix the remote value with `dsctl project-preference update` "
                "before retrying."
            ),
        )
    return value


def _optional_alert_group_id(
    preferences: JsonObject,
    *,
    project_code: int,
) -> int | None:
    alert_groups = _optional_preference_int(
        preferences,
        field_name="alertGroups",
        project_code=project_code,
    )
    alert_group = _optional_preference_int(
        preferences,
        field_name="alertGroup",
        project_code=project_code,
    )
    if (
        alert_groups is not None
        and alert_group is not None
        and alert_groups != alert_group
    ):
        message = "Stored project preference alert-group fields disagree"
        raise ConflictError(
            message,
            details={
                "projectCode": project_code,
                "alertGroups": alert_groups,
                "alertGroup": alert_group,
            },
            suggestion=(
                "Keep only one alert-group field or make both values match, then retry."
            ),
        )
    return alert_groups if alert_groups is not None else alert_group
