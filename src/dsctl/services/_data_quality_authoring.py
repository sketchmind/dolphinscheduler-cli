from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

from dsctl.errors import ApiTransportError, InvalidStateError, UserInputError
from dsctl.upstream.data_quality import DataQualityAuthoringInspectionError
from dsctl.upstream.task_parameter_projection import ProjectionSource

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.upstream.protocol import DataQualityAuthoringInspector


class _DataQualityInspectionDetails(TypedDict):
    action: str
    ds_version: str
    field: str
    reason: str
    task: str
    task_type: str


def preflight_data_quality_authoring(
    inspector: DataQualityAuthoringInspector | None,
    *,
    spec: WorkflowSpec,
    projection_sources: Mapping[str, ProjectionSource],
    ds_version: str,
    action: str,
) -> None:
    """Attest mutable live prerequisites for each newly authored DQ task."""
    for task in spec.tasks:
        if (
            task.type.upper() != "DATA_QUALITY"
            or projection_sources.get(task.name) is not ProjectionSource.TYPED_AUTHORING
        ):
            continue
        if inspector is None:
            message = "DATA_QUALITY live authoring preflight is unavailable."
            raise ApiTransportError(
                message,
                details=_inspection_details(
                    task=task,
                    action=action,
                    ds_version=ds_version,
                    field="inspector",
                    reason="the selected exact profile did not bind an inspector",
                ),
                suggestion=(
                    "Verify the selected DolphinScheduler version and retry. "
                    "Do not create or edit this typed DATA_QUALITY task without "
                    "the live preflight."
                ),
            )
        datasource_id, database = _canonical_datasource(task)
        try:
            inspector.inspect(
                datasource_id=datasource_id,
                database=database,
            )
        except DataQualityAuthoringInspectionError as error:
            _raise_inspection_error(
                error,
                task=task,
                action=action,
                ds_version=ds_version,
                datasource_id=datasource_id,
            )


def _canonical_datasource(task: WorkflowTaskSpec) -> tuple[int, str | None]:
    params = task.task_params
    if not isinstance(params, Mapping):
        message = "Compiled DATA_QUALITY authoring parameters are unavailable."
        raise ApiTransportError(
            message,
            details={"task": task.name, "task_type": "DATA_QUALITY"},
        )
    datasource_id = params.get("datasource")
    if not isinstance(datasource_id, int) or isinstance(datasource_id, bool):
        message = "Compiled DATA_QUALITY datasource identity is invalid."
        raise ApiTransportError(
            message,
            details={"task": task.name, "task_type": "DATA_QUALITY"},
        )
    database_value = params.get("database")
    database = database_value if isinstance(database_value, str) else None
    return datasource_id, database


def _raise_inspection_error(
    error: DataQualityAuthoringInspectionError,
    *,
    task: WorkflowTaskSpec,
    action: str,
    ds_version: str,
    datasource_id: int,
) -> None:
    details = _inspection_details(
        task=task,
        action=action,
        ds_version=ds_version,
        field=error.field,
        reason=error.reason,
    )
    if error.field == "remote":
        message = "DATA_QUALITY live authoring preflight could not inspect the server."
        raise ApiTransportError(
            message,
            details=details,
            suggestion=(
                "Restore DolphinScheduler API access, then retry the same command. "
                "The workflow was not mutated."
            ),
        ) from error
    if error.field.startswith("datasource"):
        message = (
            f"DATA_QUALITY live authoring preflight rejected datasource "
            f"{datasource_id} for task '{task.name}'."
        )
        raise UserInputError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl datasource list`, then `dsctl datasource get "
                f"{datasource_id}`. Choose one permission-visible MYSQL datasource "
                "whose database matches the workflow task."
            ),
        ) from error
    message = f"DATA_QUALITY live authoring preflight rejected task '{task.name}'."
    raise InvalidStateError(
        message,
        details=details,
        suggestion=(
            "Restore stock DATA_QUALITY rule id 10 and its enabled FixValue form "
            "semantics, then retry. The workflow was not mutated."
        ),
    ) from error


def _inspection_details(
    *,
    task: WorkflowTaskSpec,
    action: str,
    ds_version: str,
    field: str,
    reason: str,
) -> _DataQualityInspectionDetails:
    return {
        "action": action,
        "ds_version": ds_version,
        "field": field,
        "reason": reason,
        "task": task.name,
        "task_type": "DATA_QUALITY",
    }


__all__ = ["preflight_data_quality_authoring"]
