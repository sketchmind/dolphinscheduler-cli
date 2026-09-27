from __future__ import annotations

from typing import NotRequired, TypedDict, cast

from dsctl.generated.task_profiles import TARGET_DS_VERSIONS, TASK_PROFILES


class TaskProfileFact(TypedDict):
    """Exact upstream model identity and registration evidence for one task."""

    parameter_model_import: str
    semantic_fingerprint: str
    registration_kind: str


class TaskProfileReview(TypedDict):
    """Reviewed typed authoring claim, independently bound to its source fact."""

    semantic_fingerprint: str
    review: str
    cli_model: str
    source_task_type: NotRequired[str]


class TaskProfileExclusion(TypedDict):
    """Exact negative authoring decision and its source fingerprint."""

    semantic_fingerprint: str
    reason: str


class TaskAuthoringProfile(TypedDict):
    """Named source facts and reviewed claims consumed by authoring services."""

    task_types: dict[str, TaskProfileFact]
    typed_authoring_reviews: dict[str, TaskProfileReview]
    typed_authoring_exclusions: NotRequired[dict[str, TaskProfileExclusion]]


def task_profile_versions() -> tuple[str, ...]:
    """Return exact releases with materialized task authoring evidence."""
    return cast("tuple[str, ...]", TARGET_DS_VERSIONS)


def task_authoring_profile(version: str) -> TaskAuthoringProfile:
    """Expose one generated exact profile through the typed upstream boundary."""
    return cast("TaskAuthoringProfile", TASK_PROFILES[version])


__all__ = [
    "TaskAuthoringProfile",
    "TaskProfileExclusion",
    "TaskProfileFact",
    "TaskProfileReview",
    "task_authoring_profile",
    "task_profile_versions",
]
