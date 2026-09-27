"""Dispatch exact family validation after canonical model normalization."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.services.task_authoring_catalog.datax import (
    validate_semantics as validate_datax_semantics,
)
from dsctl.services.task_authoring_catalog.dependent import (
    validate_semantics as validate_dependent_semantics,
)
from dsctl.services.task_authoring_catalog.emr import (
    validate_semantics as validate_emr_semantics,
)
from dsctl.services.task_authoring_catalog.emr_serverless import (
    validate_semantics as validate_emr_serverless_semantics,
)
from dsctl.services.task_authoring_catalog.k8s import (
    validate_semantics as validate_k8s_semantics,
)
from dsctl.services.task_authoring_catalog.sagemaker import (
    validate_semantics as validate_sagemaker_semantics,
)
from dsctl.services.task_authoring_catalog.zeppelin import (
    validate_semantics as validate_zeppelin_semantics,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.upstream.task_authoring_surface import TaskAuthoringSurface


def validate_task_semantics(
    task_type: str,
    task_params: YamlObject,
    *,
    version: str,
    surface: TaskAuthoringSurface,
) -> None:
    """Apply only the additional exact semantics owned by a reviewed family."""
    if task_type == "DATAX":
        validate_datax_semantics(task_params, surface=surface.datax)
    elif task_type == "DEPENDENT":
        validate_dependent_semantics(
            task_params, version=version, surface=surface.dependent
        )
    elif task_type == "EMR":
        validate_emr_semantics(task_params, version=version, surface=surface.emr)
    elif task_type == "EMR_SERVERLESS":
        validate_emr_serverless_semantics(task_params)
    elif task_type == "SAGEMAKER":
        validate_sagemaker_semantics(
            task_params, version=version, surface=surface.sagemaker
        )
    elif task_type == "ZEPPELIN":
        validate_zeppelin_semantics(
            task_params, version=version, surface=surface.zeppelin
        )
    elif task_type == "K8S":
        validate_k8s_semantics(task_params, version=version, surface=surface.k8s)
