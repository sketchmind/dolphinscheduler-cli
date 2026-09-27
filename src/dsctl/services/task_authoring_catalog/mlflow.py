from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

MLFLOW_MODEL_SERVE_FACET = "MLFLOW/model_serve"


def _mlflow_model_serve_runtime_guidance() -> str:
    return (
        "Route to a POSIX worker with a compatible mlflow CLI, tracking and "
        "artifact-store access, worker-managed credentials, and an available "
        "listening port. Upstream starts mlflow models serve in the foreground "
        "on all interfaces; cancellation only controls the local shell process, "
        "and no MLflow-specific reconnect or worker-failover resume exists."
    )


def _mlflow_model_serve_fields() -> tuple[TaskAuthoringField, ...]:
    guidance = _mlflow_model_serve_runtime_guidance()
    return (
        model_field(
            "task_params.mlflowTaskType",
            "enum",
            choices=("MLflow Models",),
            compile_path="taskDefinitionJson[].taskParams.mlflowTaskType",
            description=f"Exact native model-deployment discriminator. {guidance}",
        ),
        model_field(
            "task_params.deployType",
            "enum",
            choices=("MLFLOW",),
            compile_path="taskDefinitionJson[].taskParams.deployType",
            description=(
                "Direct foreground MLflow serving only; Docker deployment and "
                "MLflow Projects remain opaque-preserve-only."
            ),
        ),
        model_field(
            "task_params.mlflowTrackingUri",
            compile_path="taskDefinitionJson[].taskParams.mlflowTrackingUri",
            description=(
                "Shell-safe absolute HTTP(S) tracking endpoint without inline "
                "credentials, query, fragment, or DS placeholder syntax."
            ),
        ),
        model_field(
            "task_params.deployModelKey",
            compile_path="taskDefinitionJson[].taskParams.deployModelKey",
            description=(
                "Portable shell-safe MLflow model URI: models:/ requires exactly "
                "name/version-or-stage, while runs:/ requires run-id/artifact and "
                "may include additional artifact subpaths."
            ),
        ),
        model_field(
            "task_params.deployPort",
            compile_path="taskDefinitionJson[].taskParams.deployPort",
            description=(
                "Exact decimal wire string from 1 through 65535; the worker must "
                "make this foreground service port available."
            ),
        ),
    )


def _mlflow_model_serve_template_body(body: str) -> str:
    comments = (
        f"# Runtime prerequisite: {_mlflow_model_serve_runtime_guidance()}\n"
        "# Security boundary: keep credentials in worker configuration; typed "
        "values are restricted because upstream inserts them into a shell command.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _is_reviewed_mlflow_opaque_mode(task_params: Mapping[str, YamlValue]) -> bool:
    """Recognize exact native modes outside the typed model-serve facet."""
    task_mode = task_params.get("mlflowTaskType")
    if task_mode == "MLflow Projects":
        return task_params.get("mlflowJobType") in {
            "BasicAlgorithm",
            "AutoML",
            "CustomProject",
        }
    return task_mode == "MLflow Models" and task_params.get("deployType") == "DOCKER"


_MLFLOW_MODEL_SERVE_TEMPLATES = (
    TaskAuthoringTemplate(
        name="minimal",
        summary="Serve one MLflow model in the worker foreground.",
        payload_modes=("task_params",),
        yaml=_mlflow_model_serve_template_body(
            """# Task template for MLflow model serving
name: serve-registered-model
type: MLFLOW
description: Serve one registered MLflow model
task_params:
  mlflowTaskType: MLflow Models
  deployType: MLFLOW
  mlflowTrackingUri: https://mlflow.example.com/api
  deployModelKey: models:/fraud-detector/1
  deployPort: "7000"
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
)


_MLFLOW_MODEL_SERVE_CONTRACT = TaskAuthoringFacetContract(
    facet_id=MLFLOW_MODEL_SERVE_FACET,
    family="mlflow-model-serve-v1",
    review="mlflow-model-serve-shell-safe-subset",
    params_model=_family_model("MLFLOW"),
    fields=_mlflow_model_serve_fields(),
    state_rules=(),
    templates=_MLFLOW_MODEL_SERVE_TEMPLATES,
    opaque_authoring_selector=_is_reviewed_mlflow_opaque_mode,
)


def _mlflow_model_serve_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=_MLFLOW_MODEL_SERVE_CONTRACT,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="MLFLOW",
        category="MachineLearning",
        kind="typed",
        default_facet=MLFLOW_MODEL_SERVE_FACET,
        facets={MLFLOW_MODEL_SERVE_FACET: membership},
    )
