from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

DynamicWireEpoch = Literal["legacy-single-dimension-comma-fanout"]


@dataclass(frozen=True, slots=True)
class DynamicAuthoringSurface:
    """Dynamic child-workflow fan-out and exact 3.2.x runtime defects."""

    registered: bool
    typed_available: bool
    exclusion_reason: str | None
    wire_epoch: DynamicWireEpoch | None
    child_tenant_forwarded: bool
    child_start_param_guarded: bool
    pending_children_cancelled: bool
    parameter_substitution: bool
    parameter_groups_logged: bool
    output_logged: bool
    result_output_supported: bool
    persisted_child_relations: bool
    workflow_recovery_reuses_child_instances: bool
    ordinary_task_retry_resets_failed_children: bool
    failover_recovery_supported: bool
    cancellation_best_effort: bool
    pause_cascades: bool
    schedule_context_forwarded: bool
    partial_materialization_can_duplicate: bool


_DYNAMIC_ABSENT = DynamicAuthoringSurface(
    registered=False,
    typed_available=False,
    exclusion_reason="upstream-absent",
    wire_epoch=None,
    child_tenant_forwarded=False,
    child_start_param_guarded=False,
    pending_children_cancelled=False,
    parameter_substitution=False,
    parameter_groups_logged=False,
    output_logged=False,
    result_output_supported=False,
    persisted_child_relations=False,
    workflow_recovery_reuses_child_instances=False,
    ordinary_task_retry_resets_failed_children=False,
    failover_recovery_supported=False,
    cancellation_best_effort=False,
    pause_cascades=False,
    schedule_context_forwarded=False,
    partial_materialization_can_duplicate=False,
)
_DYNAMIC_320_RUNTIME_HOLE = replace(
    _DYNAMIC_ABSENT,
    registered=True,
    exclusion_reason="dynamic-child-tenant-not-forwarded",
    wire_epoch="legacy-single-dimension-comma-fanout",
    child_start_param_guarded=True,
    parameter_substitution=True,
    parameter_groups_logged=True,
    output_logged=True,
    result_output_supported=True,
    persisted_child_relations=True,
    workflow_recovery_reuses_child_instances=True,
    failover_recovery_supported=True,
    cancellation_best_effort=True,
    partial_materialization_can_duplicate=True,
)
_DYNAMIC_321_RUNTIME_HOLE = replace(
    _DYNAMIC_320_RUNTIME_HOLE,
    exclusion_reason=("dynamic-child-tenant-not-forwarded-and-start-param-unguarded"),
    child_start_param_guarded=False,
)
_DYNAMIC_322_LITERAL_FANOUT = replace(
    _DYNAMIC_320_RUNTIME_HOLE,
    typed_available=True,
    exclusion_reason=None,
    child_tenant_forwarded=True,
    child_start_param_guarded=True,
    pending_children_cancelled=True,
)


def _dynamic_surface(version: str) -> DynamicAuthoringSurface:
    if version == "3.2.0":
        return _DYNAMIC_320_RUNTIME_HOLE
    if version == "3.2.1":
        return _DYNAMIC_321_RUNTIME_HOLE
    if version == "3.2.2":
        return _DYNAMIC_322_LITERAL_FANOUT
    if version in TARGET_DS_VERSIONS:
        return _DYNAMIC_ABSENT
    message = f"No exact DYNAMIC authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
