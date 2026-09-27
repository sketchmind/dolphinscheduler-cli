from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from pydantic import (
    ValidationError,
)

from dsctl.models.common import (
    ModelValidationError,
    YamlObject,
    model_validation_issues,
    prefixed_model_validation_issues,
)
from dsctl.models.task_spec.aliyun_serverless_spark import (
    AliyunServerlessSparkLiteralJarTaskParamsSpec,
)
from dsctl.models.task_spec.blocking import (
    BlockingTaskParamsSpec,
)
from dsctl.models.task_spec.chunjun import (
    ChunJunLiteralLocalJsonJobTaskParamsSpec,
)
from dsctl.models.task_spec.conditions import (
    ConditionsTaskParamsSpec,
)
from dsctl.models.task_spec.data_factory import (
    DataFactoryPipelineTriggerTaskParamsSpec,
)
from dsctl.models.task_spec.data_quality import (
    DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec,
)
from dsctl.models.task_spec.datasync import (
    DatasyncTaskParamsSpec,
)
from dsctl.models.task_spec.datax import (
    DataxLiteralCustomJsonJobTaskParamsSpec,
)
from dsctl.models.task_spec.dependent import (
    DependentTaskParamsSpec,
)
from dsctl.models.task_spec.dinky import (
    DinkyJobTriggerTaskParamsSpec,
)
from dsctl.models.task_spec.dms import (
    DmsResumeExistingFullLoadTaskParamsSpec,
)
from dsctl.models.task_spec.dvc import (
    DvcTaskParamsSpec,
)
from dsctl.models.task_spec.dynamic import (
    DynamicLiteralSingleDimensionFanoutTaskParamsSpec,
)
from dsctl.models.task_spec.emr import (
    EmrTaskParamsSpec,
)
from dsctl.models.task_spec.emr_serverless import (
    EmrServerlessTaskParamsSpec,
)
from dsctl.models.task_spec.flink import (
    FlinkInlineLocalSqlTaskParamsSpec,
)
from dsctl.models.task_spec.grpc import (
    GrpcLiteralUnaryStringRecordTaskParamsSpec,
)
from dsctl.models.task_spec.hivecli import (
    HiveCliTaskParamsSpec,
)
from dsctl.models.task_spec.http import (
    HttpTaskParamsSpec,
)
from dsctl.models.task_spec.java import (
    JavaLiteralFatJarTaskParamsSpec,
)
from dsctl.models.task_spec.jupyter import (
    JupyterNotebookTaskParamsSpec,
)
from dsctl.models.task_spec.k8s import (
    K8sLiteralContainerJobTaskParamsSpec,
)
from dsctl.models.task_spec.kubeflow import (
    KubeflowTfjobManifestTaskParamsSpec,
)
from dsctl.models.task_spec.mlflow import (
    MlflowModelServeTaskParamsSpec,
)
from dsctl.models.task_spec.mr import (
    MrLiteralJavaJarTaskParamsSpec,
)
from dsctl.models.task_spec.openmldb import (
    OpenmldbLiteralSingleStatementTaskParamsSpec,
)
from dsctl.models.task_spec.pigeon import (
    PigeonTaskParamsSpec,
)
from dsctl.models.task_spec.procedure import (
    ProcedureTaskParamsSpec,
)
from dsctl.models.task_spec.pytorch import (
    PytorchLiteralResourceScriptTaskParamsSpec,
)
from dsctl.models.task_spec.remoteshell import (
    RemoteShellTaskParamsSpec,
)
from dsctl.models.task_spec.sagemaker import (
    SagemakerStartPipelineExecutionTaskParamsSpec,
)
from dsctl.models.task_spec.script import (
    ScriptTaskParamsSpec,
)
from dsctl.models.task_spec.seatunnel import (
    SeatunnelLiteralLocalConfigTaskParamsSpec,
)
from dsctl.models.task_spec.spark import (
    SparkInlineLocalSqlTaskParamsSpec,
)
from dsctl.models.task_spec.sql import (
    SqlTaskParamsSpec,
)
from dsctl.models.task_spec.sqoop import (
    SqoopLiteralCommandTaskParamsSpec,
)
from dsctl.models.task_spec.sub_workflow import (
    SubWorkflowTaskParamsSpec,
)
from dsctl.models.task_spec.switch import (
    SwitchTaskParamsSpec,
)
from dsctl.models.task_spec.waterdrop import (
    WaterdropLiteralLocalConfigTaskParamsSpec,
)
from dsctl.models.task_spec.zeppelin import (
    ZeppelinParagraphTaskParamsSpec,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.task_spec.base import TaskParamsSpec


@dataclass(frozen=True, slots=True)
class TaskFamilyModel:
    """Canonical family identity and model availability, never exact eligibility.

    Stable-default and implicit-normalization flags preserve the historical model
    lookup API. Source-reviewed facet membership alone authorizes typed operations.
    """

    task_type: str
    params_model: type[TaskParamsSpec]
    stable_default: bool = True
    implicit_normalization: bool = True
    aliases: tuple[str, ...] = ()


_TASK_FAMILY_MODELS: Mapping[str, TaskFamilyModel] = MappingProxyType(
    {
        family.task_type: family
        for family in (
            TaskFamilyModel(
                "ALIYUN_SERVERLESS_SPARK", AliyunServerlessSparkLiteralJarTaskParamsSpec
            ),
            TaskFamilyModel("BLOCKING", BlockingTaskParamsSpec, stable_default=False),
            TaskFamilyModel("CHUNJUN", ChunJunLiteralLocalJsonJobTaskParamsSpec),
            TaskFamilyModel("CONDITIONS", ConditionsTaskParamsSpec),
            TaskFamilyModel("DATASYNC", DatasyncTaskParamsSpec),
            TaskFamilyModel("DATAX", DataxLiteralCustomJsonJobTaskParamsSpec),
            TaskFamilyModel("DATA_FACTORY", DataFactoryPipelineTriggerTaskParamsSpec),
            TaskFamilyModel(
                "DATA_QUALITY",
                DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec,
                stable_default=False,
            ),
            TaskFamilyModel("DEPENDENT", DependentTaskParamsSpec),
            TaskFamilyModel("DINKY", DinkyJobTriggerTaskParamsSpec),
            TaskFamilyModel("DMS", DmsResumeExistingFullLoadTaskParamsSpec),
            TaskFamilyModel("DVC", DvcTaskParamsSpec),
            TaskFamilyModel(
                "DYNAMIC",
                DynamicLiteralSingleDimensionFanoutTaskParamsSpec,
                stable_default=False,
            ),
            TaskFamilyModel("EMR", EmrTaskParamsSpec),
            TaskFamilyModel(
                "EMR_SERVERLESS", EmrServerlessTaskParamsSpec, stable_default=False
            ),
            TaskFamilyModel("FLINK", FlinkInlineLocalSqlTaskParamsSpec),
            TaskFamilyModel(
                "FLINK_STREAM",
                FlinkInlineLocalSqlTaskParamsSpec,
                stable_default=False,
                implicit_normalization=False,
            ),
            TaskFamilyModel("GRPC", GrpcLiteralUnaryStringRecordTaskParamsSpec),
            TaskFamilyModel("HIVECLI", HiveCliTaskParamsSpec),
            TaskFamilyModel("HTTP", HttpTaskParamsSpec),
            TaskFamilyModel("JAVA", JavaLiteralFatJarTaskParamsSpec),
            TaskFamilyModel("JUPYTER", JupyterNotebookTaskParamsSpec),
            TaskFamilyModel("K8S", K8sLiteralContainerJobTaskParamsSpec),
            TaskFamilyModel("KUBEFLOW", KubeflowTfjobManifestTaskParamsSpec),
            TaskFamilyModel("MLFLOW", MlflowModelServeTaskParamsSpec),
            TaskFamilyModel("MR", MrLiteralJavaJarTaskParamsSpec),
            TaskFamilyModel("OPENMLDB", OpenmldbLiteralSingleStatementTaskParamsSpec),
            TaskFamilyModel("PIGEON", PigeonTaskParamsSpec, stable_default=False),
            TaskFamilyModel("PROCEDURE", ProcedureTaskParamsSpec),
            TaskFamilyModel("PYTHON", ScriptTaskParamsSpec),
            TaskFamilyModel(
                "PYTORCH",
                PytorchLiteralResourceScriptTaskParamsSpec,
                stable_default=False,
            ),
            TaskFamilyModel(
                "REMOTESHELL", RemoteShellTaskParamsSpec, aliases=("REMOTE_SHELL",)
            ),
            TaskFamilyModel("SAGEMAKER", SagemakerStartPipelineExecutionTaskParamsSpec),
            TaskFamilyModel("SEATUNNEL", SeatunnelLiteralLocalConfigTaskParamsSpec),
            TaskFamilyModel("SHELL", ScriptTaskParamsSpec),
            TaskFamilyModel("SPARK", SparkInlineLocalSqlTaskParamsSpec),
            TaskFamilyModel("SQL", SqlTaskParamsSpec),
            TaskFamilyModel("SQOOP", SqoopLiteralCommandTaskParamsSpec),
            TaskFamilyModel("SUB_WORKFLOW", SubWorkflowTaskParamsSpec),
            TaskFamilyModel("SWITCH", SwitchTaskParamsSpec),
            TaskFamilyModel(
                "WATERDROP",
                WaterdropLiteralLocalConfigTaskParamsSpec,
                stable_default=False,
            ),
            TaskFamilyModel("ZEPPELIN", ZeppelinParagraphTaskParamsSpec),
        )
    }
)
_TASK_TYPE_ALIASES = {
    alias: family.task_type
    for family in _TASK_FAMILY_MODELS.values()
    for alias in family.aliases
}


def task_family_model(task_type: str) -> TaskFamilyModel | None:
    """Return a known family identity without claiming exact typed eligibility."""
    return _TASK_FAMILY_MODELS.get(canonical_task_type(task_type))


def task_family_models() -> tuple[TaskFamilyModel, ...]:
    """Return all registered model families in canonical identity order."""
    return tuple(_TASK_FAMILY_MODELS.values())


def canonical_task_type(task_type: str) -> str:
    """Normalize one task type name to the DS-native canonical value."""
    normalized = task_type.strip().upper()
    return _TASK_TYPE_ALIASES.get(normalized, normalized)


def supported_typed_task_types() -> tuple[str, ...]:
    """Return typed task types on the stable default profile."""
    return tuple(
        name for name, family in _TASK_FAMILY_MODELS.items() if family.stable_default
    )


def task_params_model_for_type(task_type: str) -> type[TaskParamsSpec] | None:
    """Return the typed task_params model for one task type, when available."""
    family = task_family_model(task_type)
    return (
        family.params_model
        if family is not None and family.implicit_normalization
        else None
    )


def normalize_typed_task_params(
    model: type[TaskParamsSpec],
    task_params: YamlObject,
) -> YamlObject:
    """Normalize an authored payload while rejecting unowned top-level fields."""
    validate_by_name = model.model_config.get("validate_by_name") is not False
    validate_by_alias = model.model_config.get("validate_by_alias") is not False
    accepted_fields: set[str] = set()
    for field_name, field in model.model_fields.items():
        if validate_by_name or field.alias is None:
            accepted_fields.add(field_name)
        if validate_by_alias and isinstance(field.alias, str):
            accepted_fields.add(field.alias)
    unknown_fields = sorted(set(task_params).difference(accepted_fields))
    if unknown_fields:
        fields = ", ".join(unknown_fields)
        message = (
            f"task_params contains unsupported fields for typed authoring: {fields}"
        )
        raise ValueError(message)
    return _normalize_task_params_model(model, task_params)


def normalize_task_params(task_type: str, task_params: YamlObject) -> YamlObject:
    """Validate one known task_params block and return a normalized plain object."""
    model = task_params_model_for_type(task_type)
    if model is None:
        return task_params
    return _normalize_task_params_model(model, task_params)


def _normalize_task_params_model(
    model: type[TaskParamsSpec],
    task_params: YamlObject,
) -> YamlObject:
    """Validate task params through one selected typed model."""
    try:
        return model.model_validate(task_params).to_payload()
    except ValidationError as exc:
        raise ModelValidationError(
            prefixed_model_validation_issues(
                model_validation_issues(exc),
                prefix="task_params",
            )
        ) from exc
