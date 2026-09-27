from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.errors import ConfigError, UnsupportedFeatureError
from dsctl.models.workflow_spec import WorkflowAuthoringContext
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringCatalog,
    TaskAuthoringIntent,
    default_task_authoring_catalog,
    get_task_authoring_catalog,
)
from dsctl.services.version_resolution import resolve_version
from dsctl.upstream.schedules import schedule_contract_features

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.common import YamlObject


@dataclass(frozen=True, slots=True)
class _CatalogTaskParamsNormalizer:
    """Bind one exact catalog and intent without process-global state."""

    catalog: TaskAuthoringCatalog
    intent: TaskAuthoringIntent

    def authorize_task_type(self, task_type: str) -> None:
        """Require one task under the profile's typed-or-opaque create policy."""
        self.catalog.require_authoring_task_type(
            task_type,
            intent=self.catalog.effective_authoring_intent(
                task_type,
                requested=self.intent,
            ),
        )

    def validate_task_identity(self, task_type: str, task_name: str) -> None:
        """Apply identity rules only to exact typed create/edit intent."""
        if self.intent not in {
            TaskAuthoringIntent.TYPED_CREATE,
            TaskAuthoringIntent.TYPED_EDIT,
        }:
            return
        if not self.catalog.supports_typed_authoring(task_type):
            return
        self.catalog.validate_task_identity(task_type, task_name)

    def __call__(self, task_type: str, task_params: YamlObject) -> YamlObject:
        effective_intent = self.catalog.effective_authoring_intent(
            task_type,
            requested=self.intent,
            task_params=task_params,
        )
        if effective_intent in {
            TaskAuthoringIntent.TYPED_CREATE,
            TaskAuthoringIntent.TYPED_EDIT,
        }:
            self.catalog.validate_authored_task_params(task_type, task_params)
        return self.catalog.normalize_task_params(
            task_type,
            task_params,
            intent=effective_intent,
        )


def _selected_task_authoring_catalog(
    catalog: TaskAuthoringCatalog | None,
) -> TaskAuthoringCatalog:
    """Resolve the stable catalog default at one explicit call boundary."""
    return default_task_authoring_catalog() if catalog is None else catalog


def workflow_authoring_context(
    *,
    catalog: TaskAuthoringCatalog | None,
    intent: TaskAuthoringIntent,
) -> WorkflowAuthoringContext:
    """Build an immutable workflow parse context for one selected profile."""
    selected = _selected_task_authoring_catalog(catalog)
    normalizer = _CatalogTaskParamsNormalizer(selected, intent)
    return WorkflowAuthoringContext(
        authorize_task_type=normalizer.authorize_task_type,
        validate_task_identity=normalizer.validate_task_identity,
        normalize_task_params=normalizer,
        validate_global_params=selected.validate_global_params,
        schedule_timezone_supported=schedule_contract_features(
            selected.profile_version
        ).timezone,
        schedule_missed_fire_policy_choices=schedule_contract_features(
            selected.profile_version
        ).missed_fire_policy_choices,
    )


def workflow_authoring_catalog_for_version(
    ds_version: str,
) -> TaskAuthoringCatalog:
    """Return the exact materialized catalog or fail closed."""
    try:
        return get_task_authoring_catalog(ds_version)
    except ValueError as exc:
        message = (
            f"Task authoring is not materialized for DolphinScheduler {ds_version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": ds_version,
                "feature": "task_authoring_catalog",
                "reason": str(exc),
            },
            suggestion=(
                "Run `dsctl capabilities` for the selected profile and use only "
                "authoring actions backed by an exact task catalog."
            ),
        ) from exc


def load_selected_task_authoring_catalog(
    env_file: str | Path | None,
) -> TaskAuthoringCatalog:
    """Load the selected version and return its exact authoring catalog."""
    return workflow_authoring_catalog_for_version(
        resolve_version(env_file, mode="local").version
    )


def require_task_authoring_catalog_profile(
    catalog: TaskAuthoringCatalog,
    *,
    ds_version: str,
) -> None:
    """Reject a runtime whose profile differs from the parsed authoring profile."""
    if catalog.profile_version == ds_version:
        return
    message = "The DolphinScheduler profile changed during workflow authoring."
    raise ConfigError(
        message,
        details={
            "authoring_version": catalog.profile_version,
            "runtime_version": ds_version,
        },
        suggestion=(
            "Keep DS_VERSION and --env-file unchanged for the command duration, "
            "then retry."
        ),
    )


__all__ = [
    "load_selected_task_authoring_catalog",
    "require_task_authoring_catalog_profile",
    "workflow_authoring_catalog_for_version",
    "workflow_authoring_context",
]
