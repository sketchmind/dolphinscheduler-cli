from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DinkyVariableForwarding = Literal[
    "none",
    "workflow_globals_and_local_params",
    "workflow_globals_and_local_placeholders",
    "all_prepared_params",
]


@dataclass(frozen=True, slots=True)
class DinkyAuthoringSurface:
    """Dinky API negotiation, parameter forwarding, and recovery hazards."""

    available: bool
    variable_forwarding: DinkyVariableForwarding | None
    version_negotiation: bool
    task_params_logged: bool
    variables_logged: bool
    authenticated_request: bool
    explicit_http_timeout: bool
    failover_supported: bool
    retry_may_resubmit: bool


_DINKY_ABSENT = DinkyAuthoringSurface(
    available=False,
    variable_forwarding=None,
    version_negotiation=False,
    task_params_logged=False,
    variables_logged=False,
    authenticated_request=False,
    explicit_http_timeout=False,
    failover_supported=False,
    retry_may_resubmit=False,
)
_DINKY_LEGACY_API = DinkyAuthoringSurface(
    available=True,
    variable_forwarding="none",
    version_negotiation=False,
    task_params_logged=True,
    variables_logged=False,
    authenticated_request=False,
    explicit_http_timeout=False,
    failover_supported=False,
    retry_may_resubmit=True,
)
_DINKY_GLOBAL_AND_LOCAL_PARAMS = DinkyAuthoringSurface(
    available=True,
    variable_forwarding="workflow_globals_and_local_params",
    version_negotiation=True,
    task_params_logged=True,
    variables_logged=False,
    authenticated_request=False,
    explicit_http_timeout=False,
    failover_supported=False,
    retry_may_resubmit=True,
)
_DINKY_LOCAL_PLACEHOLDERS = DinkyAuthoringSurface(
    available=True,
    variable_forwarding="workflow_globals_and_local_placeholders",
    version_negotiation=True,
    task_params_logged=True,
    variables_logged=False,
    authenticated_request=False,
    explicit_http_timeout=False,
    failover_supported=False,
    retry_may_resubmit=True,
)
_DINKY_ALL_PREPARED_PARAMS_LOGGED = DinkyAuthoringSurface(
    available=True,
    variable_forwarding="all_prepared_params",
    version_negotiation=True,
    task_params_logged=True,
    variables_logged=True,
    authenticated_request=False,
    explicit_http_timeout=False,
    failover_supported=False,
    retry_may_resubmit=True,
)


def _dinky_surface(version: str) -> DinkyAuthoringSurface:
    if version in {
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
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }:
        return _DINKY_ABSENT
    if version in {
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
    }:
        return _DINKY_LEGACY_API
    if version in {"3.2.1", "3.2.2"}:
        return _DINKY_GLOBAL_AND_LOCAL_PARAMS
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1"}:
        return _DINKY_LOCAL_PLACEHOLDERS
    if version in {"3.4.2", "3.4.3"}:
        return _DINKY_ALL_PREPARED_PARAMS_LOGGED
    message = f"No exact DINKY authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
