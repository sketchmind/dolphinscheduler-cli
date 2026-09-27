from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from dsctl.cli_surface import (
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.output import (
    require_json_object,
)
from dsctl.services._runtime_defaults import (
    ProjectPreferenceDefaults,
    load_project_preference_defaults_from_operations,
    select_worker_group,
)
from dsctl.services._validation import (
    optional_ds_datetime,
    require_non_negative_int,
    validate_ds_datetime_range,
)
from dsctl.services.selection import (
    SelectedValue,
)
from dsctl.upstream.definition_models import (
    NativeCode,
)
from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.serialization import (
    optional_text,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.services.workflow._selection import (
        _ResolvedWorkflowTarget,
    )
    from dsctl.support.yaml_io import JsonObject, JsonValue
    from dsctl.upstream.workflows import (
        WorkflowOperations,
    )

from dsctl.services.workflow._types import (
    _WORKFLOW_BACKFILL_COMPLEMENT_DEPENDENT_MODES,
    _WORKFLOW_BACKFILL_EXECUTION_ORDERS,
    _WORKFLOW_BACKFILL_RUN_MODES,
    _WORKFLOW_RUN_FAILURE_STRATEGIES,
    _WORKFLOW_RUN_PRIORITIES,
    _WORKFLOW_RUN_WARNING_TYPES,
    WorkflowBackfillTimeMode,
    WorkflowServiceRuntime,
)


def _load_workflow_project_preference_defaults(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
) -> ProjectPreferenceDefaults | None:
    if not isinstance(target.project.native, NativeCode):
        return None
    return load_project_preference_defaults_from_operations(
        runtime.domain.workflows.project_preferences,
        project_code=target.project.native.value,
    )


@dataclass(frozen=True)
class _SelectedOptionalInt:
    """One optional integer runtime input plus the source that supplied it."""

    value: int | None
    source: str


@dataclass(frozen=True)
class _WorkflowRunSettings:
    """Normalized DS start-workflow-instance settings."""

    worker_group: SelectedValue
    tenant: SelectedValue
    failure_strategy: SelectedValue
    warning_type: SelectedValue
    workflow_instance_priority: SelectedValue
    warning_group_id: _SelectedOptionalInt
    environment_code: _SelectedOptionalInt
    start_params: str | None
    start_param_names: list[str]
    execution_dry_run: bool


@dataclass(frozen=True)
class _WorkflowBackfillSettings:
    """Normalized DS complement-data settings."""

    schedule_time: str
    schedule_time_mode: WorkflowBackfillTimeMode
    run_mode: SelectedValue
    expected_parallelism_number: int | None
    complement_dependent_mode: SelectedValue
    all_level_dependent: bool
    execution_order: SelectedValue


def _workflow_run_settings(
    *,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    start_params: tuple[str | None, list[str]],
    project_preference: ProjectPreferenceDefaults | None,
    execution_dry_run: bool,
) -> _WorkflowRunSettings:
    serialized_start_params, start_param_names = start_params
    return _WorkflowRunSettings(
        worker_group=select_worker_group(
            worker_group,
            project_preference=project_preference,
        ),
        tenant=_select_workflow_run_tenant_code(
            tenant,
            project_preference=project_preference,
        ),
        failure_strategy=_select_workflow_run_enum(
            failure_strategy,
            project_preference_value=None,
            default="CONTINUE",
            choices=_WORKFLOW_RUN_FAILURE_STRATEGIES,
            label="failure strategy",
            option_name="failure-strategy",
            preference_field=None,
        ),
        warning_type=_select_workflow_run_enum(
            warning_type,
            project_preference_value=(
                None if project_preference is None else project_preference.warning_type
            ),
            default="NONE",
            choices=_WORKFLOW_RUN_WARNING_TYPES,
            label="warning type",
            option_name="warning-type",
            preference_field="warningType",
        ),
        workflow_instance_priority=_select_workflow_run_enum(
            priority,
            project_preference_value=(
                None if project_preference is None else project_preference.task_priority
            ),
            default="MEDIUM",
            choices=_WORKFLOW_RUN_PRIORITIES,
            label="workflow instance priority",
            option_name="priority",
            preference_field="taskPriority",
        ),
        warning_group_id=_select_workflow_run_optional_int(
            warning_group_id,
            project_preference_value=(
                None
                if project_preference is None
                else project_preference.warning_group_id
            ),
            label="warning-group-id",
        ),
        environment_code=_select_workflow_run_optional_int(
            environment_code,
            project_preference_value=(
                None
                if project_preference is None
                else project_preference.environment_code
            ),
            label="environment-code",
        ),
        start_params=serialized_start_params,
        start_param_names=start_param_names,
        execution_dry_run=execution_dry_run,
    )


def _select_workflow_run_tenant_code(
    explicit_tenant_code: str | None,
    *,
    project_preference: ProjectPreferenceDefaults | None,
) -> SelectedValue:
    normalized_flag = optional_text(explicit_tenant_code)
    if normalized_flag is not None:
        return SelectedValue(value=normalized_flag, source="flag")

    if project_preference is not None and project_preference.tenant_code is not None:
        return SelectedValue(
            value=project_preference.tenant_code,
            source="project_preference",
        )

    return SelectedValue(value="default", source="default")


def _select_workflow_run_enum(
    explicit_value: str | None,
    *,
    project_preference_value: str | None,
    default: str,
    choices: tuple[str, ...],
    label: str,
    option_name: str,
    preference_field: str | None,
) -> SelectedValue:
    normalized_flag = optional_text(explicit_value)
    if normalized_flag is not None:
        return SelectedValue(
            value=_normalized_workflow_run_enum(
                normalized_flag,
                choices=choices,
                label=label,
                suggestion=f"Pass --{option_name} as one of: {', '.join(choices)}.",
            ),
            source="flag",
        )

    if project_preference_value is not None:
        return SelectedValue(
            value=_normalized_workflow_run_enum(
                project_preference_value,
                choices=choices,
                label=label,
                suggestion=(
                    "Fix the remote value with `dsctl project-preference update` "
                    "before retrying."
                ),
                preference_field=preference_field,
            ),
            source="project_preference",
        )

    return SelectedValue(value=default, source="default")


def _normalized_workflow_run_enum(
    value: str,
    *,
    choices: tuple[str, ...],
    label: str,
    suggestion: str,
    preference_field: str | None = None,
    aliases: dict[str, str] | None = None,
) -> str:
    normalized = value.strip().upper().replace("-", "_")
    if aliases is not None:
        normalized = aliases.get(normalized, normalized)
    if normalized in choices:
        return normalized
    message = f"Workflow run {label} must be one of: {', '.join(choices)}"
    if preference_field is not None:
        raise ConflictError(
            message,
            details={"field": preference_field, "value": value},
            suggestion=suggestion,
        )
    raise UserInputError(
        message,
        details={label.replace(" ", "_"): value},
        suggestion=suggestion,
    )


def _select_workflow_run_optional_int(
    explicit_value: int | None,
    *,
    project_preference_value: int | None,
    label: str,
) -> _SelectedOptionalInt:
    if explicit_value is not None:
        return _SelectedOptionalInt(
            value=require_non_negative_int(explicit_value, label=label),
            source="flag",
        )
    if project_preference_value is not None:
        return _SelectedOptionalInt(
            value=project_preference_value,
            source="project_preference",
        )
    return _SelectedOptionalInt(value=None, source="default")


def _workflow_backfill_settings(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
    run_mode: str | None,
    expected_parallelism_number: int | None,
    complement_dependent_mode: str | None,
    all_level_dependent: bool,
    execution_order: str | None,
    execution_schedule_time_shape: Literal["comma-range", "json"] = "json",
    ds_version: str | None = None,
) -> _WorkflowBackfillSettings:
    return _WorkflowBackfillSettings(
        schedule_time=_workflow_backfill_schedule_time(
            start=start,
            end=end,
            dates=dates,
            execution_schedule_time_shape=execution_schedule_time_shape,
            ds_version=ds_version,
        ),
        schedule_time_mode=_workflow_backfill_time_mode(
            start=start,
            end=end,
            dates=dates,
        ),
        run_mode=_select_workflow_backfill_enum(
            run_mode,
            default="RUN_MODE_SERIAL",
            choices=_WORKFLOW_BACKFILL_RUN_MODES,
            aliases={
                "SERIAL": "RUN_MODE_SERIAL",
                "PARALLEL": "RUN_MODE_PARALLEL",
            },
            label="run mode",
            option_name="run-mode",
        ),
        expected_parallelism_number=(
            None
            if expected_parallelism_number is None
            else require_non_negative_int(
                expected_parallelism_number,
                label="expected-parallelism-number",
            )
        ),
        complement_dependent_mode=_select_workflow_backfill_enum(
            complement_dependent_mode,
            default="OFF_MODE",
            choices=_WORKFLOW_BACKFILL_COMPLEMENT_DEPENDENT_MODES,
            aliases={
                "OFF": "OFF_MODE",
                "ALL": "ALL_DEPENDENT",
            },
            label="complement dependent mode",
            option_name="complement-dependent-mode",
        ),
        all_level_dependent=all_level_dependent,
        execution_order=_select_workflow_backfill_enum(
            execution_order,
            default="DESC_ORDER",
            choices=_WORKFLOW_BACKFILL_EXECUTION_ORDERS,
            aliases={
                "DESC": "DESC_ORDER",
                "ASC": "ASC_ORDER",
            },
            label="execution order",
            option_name="execution-order",
        ),
    )


def _validate_workflow_backfill_inputs(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
    run_mode: str | None,
    expected_parallelism_number: int | None,
    complement_dependent_mode: str | None,
    all_level_dependent: bool,
    execution_order: str | None,
    params: list[str],
) -> None:
    """Validate profile-independent backfill inputs before opening a runtime."""
    _workflow_backfill_settings(
        start=start,
        end=end,
        dates=dates,
        run_mode=run_mode,
        expected_parallelism_number=expected_parallelism_number,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
    )
    _workflow_run_start_params(params)


def _select_workflow_backfill_enum(
    explicit_value: str | None,
    *,
    default: str,
    choices: tuple[str, ...],
    aliases: dict[str, str],
    label: str,
    option_name: str,
) -> SelectedValue:
    normalized_flag = optional_text(explicit_value)
    if normalized_flag is not None:
        return SelectedValue(
            value=_normalized_workflow_run_enum(
                normalized_flag,
                choices=choices,
                aliases=aliases,
                label=label,
                suggestion=f"Pass --{option_name} as one of: {', '.join(aliases)}.",
            ),
            source="flag",
        )
    return SelectedValue(value=default, source="default")


def _workflow_backfill_schedule_time(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
    execution_schedule_time_shape: Literal["comma-range", "json"] = "json",
    ds_version: str | None = None,
) -> str:
    normalized_dates = _workflow_backfill_dates(dates)
    normalized_start = optional_ds_datetime(start, label="start")
    normalized_end = optional_ds_datetime(end, label="end")
    if normalized_dates:
        if execution_schedule_time_shape == "comma-range":
            message = (
                "Explicit --date values are not representable by the "
                "selected DolphinScheduler complement-data executor."
            )
            raise UnsupportedFeatureError(
                message,
                details={
                    "resource": WORKFLOW_RESOURCE,
                    "action": "workflow.backfill",
                    "parameter": "complementScheduleDateList",
                    "ds_version": ds_version,
                    "reason": "upstream_capability_absent",
                },
                suggestion="Pass --start and --end instead of repeated --date.",
            )
        if normalized_start is not None or normalized_end is not None:
            message = "Workflow backfill accepts either --date or --start/--end"
            raise UserInputError(
                message,
                suggestion=(
                    "Use repeated --date values, or pass both --start and --end."
                ),
            )
        payload = {"complementScheduleDateList": ",".join(normalized_dates)}
    else:
        if normalized_start is None or normalized_end is None:
            message = "Workflow backfill requires --start and --end, or --date"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass both --start 'yyyy-MM-dd HH:mm:ss' and --end "
                    "'yyyy-MM-dd HH:mm:ss', or repeat --date for explicit "
                    "complement schedule dates."
                ),
            )
        validate_ds_datetime_range(normalized_start, normalized_end)
        if execution_schedule_time_shape == "comma-range":
            return f"{normalized_start},{normalized_end}"
        payload = {
            "complementStartDate": normalized_start,
            "complementEndDate": normalized_end,
        }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _workflow_backfill_time_mode(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
) -> WorkflowBackfillTimeMode:
    del start, end
    return "dates" if _workflow_backfill_dates(dates) else "range"


def _workflow_backfill_dates(dates: list[str]) -> list[str]:
    normalized_dates = []
    for date in dates:
        normalized = optional_ds_datetime(date, label="date")
        if normalized is not None:
            normalized_dates.append(normalized)
    return normalized_dates


def _workflow_run_start_params(params: list[str]) -> tuple[str | None, list[str]]:
    if not params:
        return None, []
    payload: dict[str, str] = {}
    for item in params:
        key, separator, raw_value = item.partition("=")
        normalized_key = key.strip()
        if not separator or not normalized_key:
            message = f"Invalid --param value {item!r}; expected KEY=VALUE"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass runtime parameters as `--param name=value`; repeat the "
                    "option for multiple workflow start parameters."
                ),
            )
        if normalized_key in payload:
            message = (
                f"Workflow start parameter {normalized_key!r} was specified more "
                "than once"
            )
            raise UserInputError(
                message,
                suggestion="Pass each workflow start parameter name only once.",
            )
        payload[normalized_key] = raw_value
    return (
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        list(payload),
    )


def _workflow_run_start_params_for_profile(
    operations: WorkflowOperations,
    *,
    params: list[str],
    action: str,
) -> tuple[str | None, list[str]]:
    start_params, names = _workflow_run_start_params(params)
    return operations.normalize_start_params(start_params, action=action), names


def _validate_workflow_start_param_names_for_profile(
    operations: WorkflowOperations,
    *,
    target: _ResolvedWorkflowTarget,
    start_param_names: list[str],
    action: str,
) -> None:
    semantics = get_parameter_semantics(operations.ds_version).startup
    if not start_param_names or semantics.key_scope != "declared-workflow-globals":
        return
    workflow_view = target.scope.view
    declared_names = _workflow_global_parameter_names(
        global_param_map=workflow_view.global_param_map,
        serialized=workflow_view.global_params,
    )
    undeclared_names = sorted(set(start_param_names).difference(declared_names))
    if not undeclared_names:
        return
    message = (
        f"DolphinScheduler {operations.ds_version} silently ignores start parameters "
        "that are not declared as workflow globals."
    )
    raise UnsupportedFeatureError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "action": action,
            "parameter": "startParams",
            "ds_version": operations.ds_version,
            "undeclared_keys": undeclared_names,
            "declared_keys": sorted(declared_names),
            "reason": "upstream_silently_ignores_undeclared_keys",
        },
        suggestion=(
            "Declare each key in workflow.global_params before starting this "
            "DS 2.0.0 workflow, or omit the undeclared --param values."
        ),
    )


def _workflow_global_parameter_names(
    *,
    global_param_map: Mapping[str, str | None] | None,
    serialized: str | None,
) -> set[str]:
    names = set(global_param_map or {})
    if serialized is None:
        return names
    try:
        raw_params: JsonValue = json.loads(serialized)
    except json.JSONDecodeError as exc:
        message = "Workflow globalParams was not valid JSON"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE, "field": "globalParams"},
        ) from exc
    if not isinstance(raw_params, list):
        message = "Workflow globalParams was not a list"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE, "field": "globalParams"},
        )
    for raw_param in raw_params:
        if not isinstance(raw_param, dict):
            continue
        prop = raw_param.get("prop")
        if isinstance(prop, str) and prop:
            names.add(prop)
    return names


def _selected_optional_int_data(selected: _SelectedOptionalInt) -> JsonObject:
    return require_json_object(
        {
            "value": selected.value,
            "source": selected.source,
        },
        label="selected optional integer",
    )
