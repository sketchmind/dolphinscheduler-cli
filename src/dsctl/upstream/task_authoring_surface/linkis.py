from __future__ import annotations

from dataclasses import dataclass, replace

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS


@dataclass(frozen=True, slots=True)
class LinkisAuthoringSurface:
    """Linkis CLI result transport, tracking, and recovery facts."""

    registered: bool
    typed_available: bool
    exclusion_reason: str | None
    command_result_populated: bool
    status_check_count: int
    status_polled_to_terminal: bool
    task_id_persisted_after_submit: bool
    cancel_after_submit_failure: bool
    parameter_substitution: bool
    task_params_logged: bool
    command_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_can_duplicate: bool


_LINKIS_ABSENT = LinkisAuthoringSurface(
    registered=False,
    typed_available=False,
    exclusion_reason="upstream-absent",
    command_result_populated=False,
    status_check_count=0,
    status_polled_to_terminal=False,
    task_id_persisted_after_submit=False,
    cancel_after_submit_failure=False,
    parameter_substitution=False,
    task_params_logged=False,
    command_logged=False,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_can_duplicate=False,
)
_LINKIS_RUNTIME_HOLE = replace(
    _LINKIS_ABSENT,
    registered=True,
    exclusion_reason=("linkis-command-result-never-populated-and-status-not-polled"),
    status_check_count=1,
    parameter_substitution=True,
    task_params_logged=True,
    command_logged=True,
    retry_can_duplicate=True,
)


def _linkis_surface(version: str) -> LinkisAuthoringSurface:
    if version in {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }:
        return _LINKIS_RUNTIME_HOLE
    if version in TARGET_DS_VERSIONS:
        return _LINKIS_ABSENT
    message = f"No exact LINKIS authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
