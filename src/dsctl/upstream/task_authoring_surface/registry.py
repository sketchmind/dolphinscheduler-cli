from __future__ import annotations

from dataclasses import dataclass
from functools import cache

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.registry import normalize_version
from dsctl.upstream.task_authoring_surface.aliyun_serverless_spark import (
    AliyunServerlessSparkAuthoringSurface,
    _aliyun_serverless_spark_surface,
)
from dsctl.upstream.task_authoring_surface.blocking import (
    BlockingAuthoringSurface,
    _blocking_surface,
)
from dsctl.upstream.task_authoring_surface.chunjun import (
    ChunJunAuthoringSurface,
    _chunjun_surface,
)
from dsctl.upstream.task_authoring_surface.data_factory import (
    DataFactoryAuthoringSurface,
    _data_factory_surface,
)
from dsctl.upstream.task_authoring_surface.data_quality import (
    DataQualityAuthoringSurface,
    _data_quality_surface,
)
from dsctl.upstream.task_authoring_surface.datasync import (
    DatasyncAuthoringSurface,
    _datasync_surface,
)
from dsctl.upstream.task_authoring_surface.datax import (
    DataxAuthoringSurface,
    _datax_surface,
)
from dsctl.upstream.task_authoring_surface.dependent import (
    _DEPENDENT_139,
    _DEPENDENT_BASIC_BASE,
    _DEPENDENT_BASIC_EXTENDED,
    _DEPENDENT_FAILURE_CONTROL,
    _DEPENDENT_PARAMETER_PASSING,
    DependentAuthoringSurface,
)
from dsctl.upstream.task_authoring_surface.dinky import (
    DinkyAuthoringSurface,
    _dinky_surface,
)
from dsctl.upstream.task_authoring_surface.dms import (
    DmsAuthoringSurface,
    _dms_surface,
)
from dsctl.upstream.task_authoring_surface.dynamic import (
    DynamicAuthoringSurface,
    _dynamic_surface,
)
from dsctl.upstream.task_authoring_surface.emr import (
    EmrAuthoringSurface,
    _emr_surface,
)
from dsctl.upstream.task_authoring_surface.flink import (
    FlinkInlineSqlAuthoringSurface,
    FlinkStreamInlineSqlAuthoringSurface,
    _flink_inline_sql_surface,
    _flink_stream_inline_sql_surface,
)
from dsctl.upstream.task_authoring_surface.grpc import (
    GrpcAuthoringSurface,
    _grpc_surface,
)
from dsctl.upstream.task_authoring_surface.hivecli import (
    HiveCliAuthoringSurface,
    _hive_cli_surface,
)
from dsctl.upstream.task_authoring_surface.http import (
    _HTTP_LEGACY_WITH_BODY,
    _HTTP_MODERN,
    _HTTP_WITHOUT_BODY,
    HttpAuthoringSurface,
)
from dsctl.upstream.task_authoring_surface.java import (
    JavaAuthoringSurface,
    _java_surface,
)
from dsctl.upstream.task_authoring_surface.jupyter import (
    JupyterAuthoringSurface,
    _jupyter_surface,
)
from dsctl.upstream.task_authoring_surface.k8s import (
    K8sAuthoringSurface,
    _k8s_surface,
)
from dsctl.upstream.task_authoring_surface.kubeflow import (
    KubeflowAuthoringSurface,
    _kubeflow_surface,
)
from dsctl.upstream.task_authoring_surface.linkis import (
    LinkisAuthoringSurface,
    _linkis_surface,
)
from dsctl.upstream.task_authoring_surface.mr import (
    MrAuthoringSurface,
    _mr_surface,
)
from dsctl.upstream.task_authoring_surface.openmldb import (
    OpenmldbAuthoringSurface,
    _openmldb_surface,
)
from dsctl.upstream.task_authoring_surface.procedure import (
    ProcedureAuthoringSurface,
    _procedure_surface,
)
from dsctl.upstream.task_authoring_surface.pytorch import (
    PytorchAuthoringSurface,
    _pytorch_surface,
)
from dsctl.upstream.task_authoring_surface.sagemaker import (
    SagemakerAuthoringSurface,
    _sagemaker_surface,
)
from dsctl.upstream.task_authoring_surface.seatunnel import (
    SeatunnelAuthoringSurface,
    _seatunnel_surface,
)
from dsctl.upstream.task_authoring_surface.spark import (
    SparkInlineSqlAuthoringSurface,
    _spark_inline_sql_surface,
)
from dsctl.upstream.task_authoring_surface.sqoop import (
    SqoopAuthoringSurface,
    _sqoop_surface,
)
from dsctl.upstream.task_authoring_surface.sub_workflow import (
    NestedWorkflowAuthoringSurface,
    _nested_workflow_surface,
)
from dsctl.upstream.task_authoring_surface.task_node import (
    _LEGACY_PROCESS_TASK_NODE,
    _TASK_DEFINITION_NODE,
    TaskNodeAuthoringSurface,
)
from dsctl.upstream.task_authoring_surface.waterdrop import (
    WaterdropAuthoringSurface,
    _waterdrop_surface,
)
from dsctl.upstream.task_authoring_surface.zeppelin import (
    ZeppelinAuthoringSurface,
    _zeppelin_surface,
)


@dataclass(frozen=True, slots=True)
class TaskAuthoringSurface:
    """Reviewed exact-version discovery surface shared by schema and templates."""

    version: str
    http: HttpAuthoringSurface
    dependent: DependentAuthoringSurface
    blocking: BlockingAuthoringSurface
    aliyun_serverless_spark: AliyunServerlessSparkAuthoringSurface
    java: JavaAuthoringSurface
    pytorch: PytorchAuthoringSurface
    mr: MrAuthoringSurface
    sqoop: SqoopAuthoringSurface
    waterdrop: WaterdropAuthoringSurface
    seatunnel: SeatunnelAuthoringSurface
    dynamic: DynamicAuthoringSurface
    linkis: LinkisAuthoringSurface
    data_factory: DataFactoryAuthoringSurface
    data_quality: DataQualityAuthoringSurface
    datax: DataxAuthoringSurface
    chunjun: ChunJunAuthoringSurface
    datasync: DatasyncAuthoringSurface
    sagemaker: SagemakerAuthoringSurface
    dinky: DinkyAuthoringSurface
    dms: DmsAuthoringSurface
    grpc: GrpcAuthoringSurface
    k8s: K8sAuthoringSurface
    kubeflow: KubeflowAuthoringSurface
    emr: EmrAuthoringSurface
    hive_cli: HiveCliAuthoringSurface
    flink_inline_sql: FlinkInlineSqlAuthoringSurface
    flink_stream_inline_sql: FlinkStreamInlineSqlAuthoringSurface
    spark_inline_sql: SparkInlineSqlAuthoringSurface
    openmldb: OpenmldbAuthoringSurface
    jupyter: JupyterAuthoringSurface
    zeppelin: ZeppelinAuthoringSurface
    procedure: ProcedureAuthoringSurface
    nested_workflow: NestedWorkflowAuthoringSurface
    task_node: TaskNodeAuthoringSurface


def _surface(
    version: str,
    *,
    http: HttpAuthoringSurface,
    dependent: DependentAuthoringSurface,
    task_node: TaskNodeAuthoringSurface = _TASK_DEFINITION_NODE,
) -> TaskAuthoringSurface:
    return TaskAuthoringSurface(
        version=version,
        http=http,
        dependent=dependent,
        blocking=_blocking_surface(version),
        aliyun_serverless_spark=_aliyun_serverless_spark_surface(version),
        java=_java_surface(version),
        pytorch=_pytorch_surface(version),
        mr=_mr_surface(version),
        sqoop=_sqoop_surface(version),
        waterdrop=_waterdrop_surface(version),
        seatunnel=_seatunnel_surface(version),
        dynamic=_dynamic_surface(version),
        linkis=_linkis_surface(version),
        data_factory=_data_factory_surface(version),
        data_quality=_data_quality_surface(version),
        datax=_datax_surface(version),
        chunjun=_chunjun_surface(version),
        datasync=_datasync_surface(version),
        sagemaker=_sagemaker_surface(version),
        dinky=_dinky_surface(version),
        dms=_dms_surface(version),
        grpc=_grpc_surface(version),
        k8s=_k8s_surface(version),
        kubeflow=_kubeflow_surface(version),
        emr=_emr_surface(version),
        hive_cli=_hive_cli_surface(version),
        flink_inline_sql=_flink_inline_sql_surface(version),
        flink_stream_inline_sql=_flink_stream_inline_sql_surface(version),
        spark_inline_sql=_spark_inline_sql_surface(version),
        openmldb=_openmldb_surface(version),
        jupyter=_jupyter_surface(version),
        zeppelin=_zeppelin_surface(version),
        procedure=_procedure_surface(version),
        nested_workflow=_nested_workflow_surface(version),
        task_node=task_node,
    )


_SURFACE_INPUTS: dict[
    str,
    tuple[HttpAuthoringSurface, DependentAuthoringSurface, TaskNodeAuthoringSurface],
] = {
    "1.3.9": (_HTTP_WITHOUT_BODY, _DEPENDENT_139, _LEGACY_PROCESS_TASK_NODE),
    "2.0.0": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.1": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.2": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.3": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.4": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.5": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.6": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.7": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.8": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "2.0.9": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "3.0.0": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "3.0.1": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "3.0.2": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.0.3": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.0.4": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.0.5": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.0.6": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.0": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_BASE, _TASK_DEFINITION_NODE),
    "3.1.1": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.2": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.3": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.4": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.5": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.6": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.7": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.8": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.1.9": (_HTTP_WITHOUT_BODY, _DEPENDENT_BASIC_EXTENDED, _TASK_DEFINITION_NODE),
    "3.2.0": (_HTTP_WITHOUT_BODY, _DEPENDENT_FAILURE_CONTROL, _TASK_DEFINITION_NODE),
    "3.2.1": (
        _HTTP_LEGACY_WITH_BODY,
        _DEPENDENT_PARAMETER_PASSING,
        _TASK_DEFINITION_NODE,
    ),
    "3.2.2": (
        _HTTP_LEGACY_WITH_BODY,
        _DEPENDENT_PARAMETER_PASSING,
        _TASK_DEFINITION_NODE,
    ),
    "3.3.1": (_HTTP_MODERN, _DEPENDENT_PARAMETER_PASSING, _TASK_DEFINITION_NODE),
    "3.3.2": (_HTTP_MODERN, _DEPENDENT_PARAMETER_PASSING, _TASK_DEFINITION_NODE),
    "3.4.0": (_HTTP_MODERN, _DEPENDENT_PARAMETER_PASSING, _TASK_DEFINITION_NODE),
    "3.4.1": (_HTTP_MODERN, _DEPENDENT_PARAMETER_PASSING, _TASK_DEFINITION_NODE),
    "3.4.2": (_HTTP_MODERN, _DEPENDENT_PARAMETER_PASSING, _TASK_DEFINITION_NODE),
    "3.4.3": (_HTTP_MODERN, _DEPENDENT_PARAMETER_PASSING, _TASK_DEFINITION_NODE),
}

if tuple(_SURFACE_INPUTS) != TARGET_DS_VERSIONS:
    message = "Task authoring surfaces must cover every exact DS profile in order"
    raise RuntimeError(message)


@cache
def _surface_for_exact_version(version: str) -> TaskAuthoringSurface:
    http, dependent, task_node = _SURFACE_INPUTS[version]
    return _surface(
        version,
        http=http,
        dependent=dependent,
        task_node=task_node,
    )


def get_task_authoring_surface(version: str) -> TaskAuthoringSurface:
    """Return reviewed schema/template semantics for one exact DS release."""
    normalized = normalize_version(version)
    if normalized not in _SURFACE_INPUTS:
        supported = ", ".join(TARGET_DS_VERSIONS)
        message = (
            f"No exact task authoring surface for DolphinScheduler {version}. "
            f"Supported versions: {supported}"
        )
        raise ValueError(message)
    return _surface_for_exact_version(normalized)
