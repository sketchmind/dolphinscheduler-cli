from __future__ import annotations

from functools import partial
from types import MappingProxyType
from typing import TYPE_CHECKING

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.task_parameter_projection.aliyun_serverless_spark import (
    _decode_aliyun_serverless_spark,
    _encode_aliyun_serverless_spark,
    _require_aliyun_serverless_spark_available,
)
from dsctl.upstream.task_parameter_projection.blocking import (
    _decode_blocking,
    _decode_opaque_blocking_with_provenance,
    _encode_blocking,
    _require_blocking_available,
)
from dsctl.upstream.task_parameter_projection.chunjun import (
    _decode_chunjun,
    _decode_opaque_chunjun_with_provenance,
    _encode_chunjun,
    _require_chunjun_available,
)
from dsctl.upstream.task_parameter_projection.conditions import (
    _decode_conditions,
    _encode_conditions,
)
from dsctl.upstream.task_parameter_projection.data_factory import (
    _decode_data_factory,
    _encode_data_factory,
    _require_data_factory_available,
)
from dsctl.upstream.task_parameter_projection.data_quality import (
    _decode_data_quality,
    _decode_opaque_data_quality_with_provenance,
    _encode_data_quality,
    _guard_data_quality_present,
)
from dsctl.upstream.task_parameter_projection.datasync import (
    _decode_datasync,
    _decode_opaque_datasync_with_provenance,
    _encode_datasync,
    _require_datasync_available,
)
from dsctl.upstream.task_parameter_projection.datax import (
    _decode_datax,
    _decode_opaque_datax_with_provenance,
    _encode_datax,
    _require_datax_plugin_present,
)
from dsctl.upstream.task_parameter_projection.dependent import (
    _decode_dependent,
    _encode_dependent,
)
from dsctl.upstream.task_parameter_projection.dinky import (
    _decode_dinky,
    _encode_dinky,
    _project_opaque_dinky,
    _require_dinky_available,
)
from dsctl.upstream.task_parameter_projection.dms import (
    _decode_dms,
    _encode_dms,
    _require_dms_available,
)
from dsctl.upstream.task_parameter_projection.dynamic import (
    _decode_dynamic_literal_single_dimension_fanout,
    _decode_opaque_dynamic_with_provenance,
    _encode_dynamic_literal_single_dimension_fanout,
)
from dsctl.upstream.task_parameter_projection.emr import (
    _decode_emr,
    _encode_emr,
)
from dsctl.upstream.task_parameter_projection.flink import (
    _decode_flink_inline_local_sql,
    _decode_flink_stream_inline_local_sql,
    _decode_opaque_flink_with_provenance,
    _encode_flink_inline_local_sql,
    _encode_flink_stream_inline_local_sql,
)
from dsctl.upstream.task_parameter_projection.grpc import (
    _decode_grpc,
    _encode_grpc,
    _require_grpc_available,
)
from dsctl.upstream.task_parameter_projection.http import (
    _decode_http,
    _encode_http,
)
from dsctl.upstream.task_parameter_projection.java import (
    _decode_java_literal_fat_jar,
    _decode_opaque_java_with_provenance,
    _encode_java_literal_fat_jar,
    _require_java_plugin_present,
)
from dsctl.upstream.task_parameter_projection.jupyter import (
    _decode_jupyter,
    _encode_jupyter,
    _looks_canonical_jupyter,
)
from dsctl.upstream.task_parameter_projection.k8s import (
    _decode_k8s,
    _decode_opaque_k8s_with_provenance,
    _encode_k8s,
    _require_k8s_plugin_present,
)
from dsctl.upstream.task_parameter_projection.kubeflow import (
    _decode_kubeflow,
    _encode_kubeflow,
    _require_kubeflow_available,
)
from dsctl.upstream.task_parameter_projection.mr import (
    _decode_mr_literal_java_jar_job,
    _decode_opaque_mr_with_provenance,
    _encode_mr_literal_java_jar_job,
    _require_mr_plugin_present,
)
from dsctl.upstream.task_parameter_projection.openmldb import (
    _decode_openmldb,
    _encode_openmldb,
    _require_openmldb_plugin_present,
)
from dsctl.upstream.task_parameter_projection.procedure import (
    _decode_procedure,
    _encode_procedure,
    _try_project_opaque_procedure,
)
from dsctl.upstream.task_parameter_projection.pytorch import (
    _decode_opaque_pytorch_with_provenance,
    _decode_pytorch_literal_resource_script,
    _encode_pytorch_literal_resource_script,
    _require_pytorch_plugin_present,
)
from dsctl.upstream.task_parameter_projection.recognition import (
    _looks_canonical,
)
from dsctl.upstream.task_parameter_projection.sagemaker import (
    _decode_opaque_sagemaker_with_provenance,
    _decode_sagemaker,
    _encode_sagemaker,
    _require_sagemaker_when_selected,
)
from dsctl.upstream.task_parameter_projection.script import (
    _decode_legacy_python,
    _decode_legacy_shell,
    _decode_opaque_script_with_provenance,
    _encode_legacy_python,
    _encode_legacy_shell,
)
from dsctl.upstream.task_parameter_projection.seatunnel import (
    _decode_opaque_seatunnel_with_provenance,
    _decode_seatunnel_literal_local_config,
    _encode_seatunnel_literal_local_config,
    _guard_seatunnel_available,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _LEGACY_VERSIONS,
    _NESTED_WORKFLOW_TYPES,
    _copy_json_object,
    _decode_canonical_native,
    _exact_version,
    _opaque_projection_identity,
    _reject_139_code_projection,
)
from dsctl.upstream.task_parameter_projection.spark import (
    _decode_opaque_spark_with_provenance,
    _decode_spark_inline_local_sql,
    _encode_spark_inline_local_sql,
)
from dsctl.upstream.task_parameter_projection.sql import (
    _decode_opaque_sql_with_provenance,
    _decode_sql,
    _encode_sql,
)
from dsctl.upstream.task_parameter_projection.sqoop import (
    _decode_opaque_sqoop_with_provenance,
    _decode_sqoop_literal_command,
    _encode_sqoop_literal_command,
    _require_sqoop_plugin_present,
)
from dsctl.upstream.task_parameter_projection.sub_workflow import (
    _decode_sub_workflow,
    _encode_sub_workflow,
)
from dsctl.upstream.task_parameter_projection.switch import (
    _decode_switch,
    _encode_switch,
    _try_project_opaque_modern_switch,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionSource,
    TaskGraphContext,
    TaskParameterProjectionError,
    TaskRefIndex,
    TaskResourceRefIndex,
    TaskWorkflowRefIndex,
    _CanonicalNative,
    _LeafProjection,
    _LocalReferenceProjection,
    _ResourceProjection,
    _TaskProjection,
    _WorkflowProjection,
)
from dsctl.upstream.task_parameter_projection.waterdrop import (
    _decode_opaque_waterdrop_with_provenance,
    _decode_waterdrop_literal_local_config_job,
    _encode_waterdrop_literal_local_config_job,
)
from dsctl.upstream.task_parameter_projection.zeppelin import (
    _decode_zeppelin,
    _encode_zeppelin,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject

if _LEGACY_VERSIONS | {
    "1.3.9",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
} != set(TARGET_DS_VERSIONS):
    message = "Task parameter projection epochs must cover every exact DS profile"
    raise RuntimeError(message)


def encode_task_parameters(
    *,
    version: str,
    task_type: str,
    task_params: JsonObject,
    refs: TaskRefIndex,
    source: ProjectionSource,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
) -> ProjectedTask:
    """Project canonical typed params onto one exact native task wire."""
    normalized_version = _exact_version(version)
    normalized_type = task_type.strip().upper()
    payload = _copy_json_object(task_params, label="canonical task_params")
    if source is ProjectionSource.OPAQUE_PRESERVE:
        _require_sagemaker_when_selected(
            task_type=normalized_type,
            version=normalized_version,
            direction="encode",
        )
        if normalized_type == "DINKY":
            return _project_opaque_dinky(
                payload,
                version=normalized_version,
                direction="encode",
            )
        guarded_identity = _guarded_opaque_identity_leaf(
            task_type=normalized_type,
            payload=payload,
            version=normalized_version,
        )
        if guarded_identity is not None:
            return guarded_identity
        if normalized_type in {
            "ALIYUN_SERVERLESS_SPARK",
            "DATA_FACTORY",
            "FLINK",
            "FLINK_STREAM",
            "OPENMLDB",
            "SAGEMAKER",
            "SPARK",
        }:
            return ProjectedTask(normalized_type, payload)
        if _opaque_projection_identity(
            version=normalized_version,
            task_type=normalized_type,
        ):
            return ProjectedTask(normalized_type, payload)
        opaque_result = _opaque_encode_result(
            payload,
            version=normalized_version,
            task_type=normalized_type,
            refs=refs,
        )
        if opaque_result is not None:
            return opaque_result
    return _encode_typed_task_parameters(
        version=normalized_version,
        task_type=normalized_type,
        payload=payload,
        refs=refs,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )


def _guarded_opaque_identity_leaf(
    *,
    task_type: str,
    payload: JsonObject,
    version: str,
) -> ProjectedTask | None:
    """Guard native preservation using the same family's projection binding."""
    binding = _TASK_PROJECTIONS.get(task_type)
    if binding is None or binding.preserve_guard is None:
        return None
    binding.preserve_guard(version=version, direction="encode")
    return ProjectedTask(task_type, payload)


def _encode_typed_task_parameters(
    *,
    version: str,
    task_type: str,
    payload: JsonObject,
    refs: TaskRefIndex,
    resource_refs: TaskResourceRefIndex | None,
    workflow_refs: TaskWorkflowRefIndex | None,
) -> ProjectedTask:
    """Dispatch an explicit typed binding after opaque policy is resolved."""
    binding = _TASK_PROJECTIONS.get(task_type)
    if binding is None and task_type not in _NESTED_WORKFLOW_TYPES:
        return ProjectedTask(task_type, payload)
    _require_sagemaker_when_selected(
        task_type=task_type, version=version, direction="encode"
    )
    _reject_139_code_projection(
        version=version, direction="encode", task_type=task_type
    )
    if binding is None:
        task_type, payload = _encode_sub_workflow(
            payload, version=version, task_type=task_type
        )
    elif isinstance(binding, _LeafProjection):
        payload = binding.encode(payload, version=version)
    elif isinstance(binding, _ResourceProjection):
        payload = binding.encode(payload, version=version, resource_refs=resource_refs)
    elif isinstance(binding, _WorkflowProjection):
        payload = binding.encode(payload, version=version, workflow_refs=workflow_refs)
    else:
        payload = binding.encode(payload, version=version, refs=refs)
    return ProjectedTask(task_type, payload)


def _opaque_encode_result(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
    refs: TaskRefIndex,
) -> ProjectedTask | None:
    """Return a complete opaque result or defer canonical input to typed projection."""
    try:
        projected_procedure = _try_project_opaque_procedure(
            payload,
            task_type=task_type,
        )
        if projected_procedure is not None:
            return ProjectedTask(task_type, projected_procedure)
        projected_switch = _try_project_opaque_modern_switch(
            payload,
            version=version,
            direction="encode",
            task_type=task_type,
            refs=refs,
        )
    except TaskParameterProjectionError:
        projected_switch = None
    if projected_switch is not None:
        return ProjectedTask(task_type, projected_switch)
    looks_canonical = (
        _looks_canonical_jupyter(version=version, payload=payload)
        if task_type == "JUPYTER"
        else _looks_canonical(
            version=version,
            task_type=task_type,
            payload=payload,
            refs=refs,
        )
    )
    if version == "1.3.9" or not looks_canonical:
        return ProjectedTask(task_type, payload)
    return None


def decode_task_parameters(
    *,
    version: str,
    task_type: str,
    task_params: JsonObject,
    refs: TaskRefIndex,
    source: ProjectionSource,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
    graph_context: TaskGraphContext | None = None,
) -> ProjectedTask:
    """Project one exact native task wire back to canonical typed params."""
    return decode_task_parameters_with_provenance(
        version=version,
        task_type=task_type,
        task_params=task_params,
        refs=refs,
        source=source,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
        graph_context=graph_context,
    ).task


def decode_task_parameters_with_provenance(
    *,
    version: str,
    task_type: str,
    task_params: JsonObject,
    refs: TaskRefIndex,
    source: ProjectionSource,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
    graph_context: TaskGraphContext | None = None,
) -> DecodedTaskParameters:
    """Decode params and declare how the returned shape must be re-encoded."""
    normalized_version = _exact_version(version)
    normalized_type = task_type.strip().upper()
    payload = _copy_json_object(task_params, label="native task_params")
    if source is ProjectionSource.OPAQUE_PRESERVE:
        leaf_decoding = _decode_opaque_leaf_with_provenance(
            task_type=normalized_type,
            payload=payload,
            version=normalized_version,
            refs=refs,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
            graph_context=graph_context,
        )
        if leaf_decoding is not None:
            return leaf_decoding
        if _opaque_projection_identity(
            version=normalized_version,
            task_type=normalized_type,
        ):
            return DecodedTaskParameters(
                ProjectedTask(normalized_type, payload),
                ProjectionSource.OPAQUE_PRESERVE,
            )
        try:
            projected_procedure = _try_project_opaque_procedure(
                payload,
                task_type=normalized_type,
            )
            if projected_procedure is not None:
                return DecodedTaskParameters(
                    ProjectedTask(normalized_type, projected_procedure),
                    ProjectionSource.OPAQUE_PRESERVE,
                )
            projected_switch = _try_project_opaque_modern_switch(
                payload,
                version=normalized_version,
                direction="decode",
                task_type=normalized_type,
                refs=refs,
            )
            if projected_switch is not None:
                return DecodedTaskParameters(
                    ProjectedTask(normalized_type, projected_switch),
                    ProjectionSource.OPAQUE_PRESERVE,
                )
            return decode_task_parameters_with_provenance(
                version=normalized_version,
                task_type=normalized_type,
                task_params=payload,
                refs=refs,
                source=ProjectionSource.TYPED_AUTHORING,
                resource_refs=resource_refs,
                workflow_refs=workflow_refs,
                graph_context=graph_context,
            )
        except TaskParameterProjectionError:
            return DecodedTaskParameters(
                ProjectedTask(normalized_type, payload),
                ProjectionSource.OPAQUE_PRESERVE,
            )
    return DecodedTaskParameters(
        _decode_typed_task_parameters(
            version=normalized_version,
            task_type=normalized_type,
            payload=payload,
            refs=refs,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        ),
        ProjectionSource.TYPED_AUTHORING,
    )


def _decode_opaque_leaf_with_provenance(
    *,
    task_type: str,
    payload: JsonObject,
    version: str,
    refs: TaskRefIndex,
    resource_refs: TaskResourceRefIndex | None,
    workflow_refs: TaskWorkflowRefIndex | None,
    graph_context: TaskGraphContext | None,
) -> DecodedTaskParameters | None:
    """Decode only the native policy explicitly bound to this task family."""
    binding = _TASK_PROJECTIONS.get(task_type)
    if binding is None or binding.native_decode is None:
        return None
    if isinstance(binding, _ResourceProjection):
        return binding.native_decode(
            payload, version=version, resource_refs=resource_refs
        )
    if isinstance(binding, _WorkflowProjection):
        return binding.native_decode(
            payload, version=version, workflow_refs=workflow_refs
        )
    if isinstance(binding, _LocalReferenceProjection):
        return binding.native_decode(
            payload, version=version, refs=refs, graph_context=graph_context
        )
    if isinstance(binding.native_decode, _CanonicalNative):
        binding.native_decode.guard(version=version, direction="decode")
        return _decode_canonical_native(
            payload, version=version, task_type=task_type, decoder=binding.decode
        )
    return binding.native_decode(payload, version=version)


def _decode_typed_task_parameters(
    *,
    version: str,
    task_type: str,
    payload: JsonObject,
    refs: TaskRefIndex,
    resource_refs: TaskResourceRefIndex | None,
    workflow_refs: TaskWorkflowRefIndex | None,
) -> ProjectedTask:
    """Dispatch an explicit typed binding after opaque policy is resolved."""
    binding = _TASK_PROJECTIONS.get(task_type)
    if binding is None and task_type not in _NESTED_WORKFLOW_TYPES:
        return ProjectedTask(task_type, payload)
    _require_sagemaker_when_selected(
        task_type=task_type, version=version, direction="decode"
    )
    _reject_139_code_projection(
        version=version, direction="decode", task_type=task_type
    )
    if binding is None:
        task_type, payload = _decode_sub_workflow(
            payload, version=version, task_type=task_type
        )
    elif isinstance(binding, _LeafProjection):
        payload = binding.decode(payload, version=version)
    elif isinstance(binding, _ResourceProjection):
        payload = binding.decode(payload, version=version, resource_refs=resource_refs)
    elif isinstance(binding, _WorkflowProjection):
        payload = binding.decode(payload, version=version, workflow_refs=workflow_refs)
    else:
        payload = binding.decode(payload, version=version, refs=refs)
    return ProjectedTask(task_type, payload)


# A binding pairs the two directions and the native preservation availability
# guard. Eligibility still comes from the selected source-reviewed profile.
_TASK_PROJECTIONS: Mapping[str, _TaskProjection] = MappingProxyType(
    {
        "ALIYUN_SERVERLESS_SPARK": _LeafProjection(
            _encode_aliyun_serverless_spark,
            _decode_aliyun_serverless_spark,
            native_decode=_CanonicalNative(_require_aliyun_serverless_spark_available),
        ),
        "BLOCKING": _LocalReferenceProjection(
            _encode_blocking,
            _decode_blocking,
            preserve_guard=_require_blocking_available,
            native_decode=_decode_opaque_blocking_with_provenance,
        ),
        "CHUNJUN": _LeafProjection(
            _encode_chunjun,
            _decode_chunjun,
            preserve_guard=_require_chunjun_available,
            native_decode=_decode_opaque_chunjun_with_provenance,
        ),
        "CONDITIONS": _LocalReferenceProjection(_encode_conditions, _decode_conditions),
        "DATASYNC": _LeafProjection(
            _encode_datasync,
            _decode_datasync,
            preserve_guard=_require_datasync_available,
            native_decode=_decode_opaque_datasync_with_provenance,
        ),
        "DATAX": _LeafProjection(
            _encode_datax,
            _decode_datax,
            preserve_guard=_require_datax_plugin_present,
            native_decode=_decode_opaque_datax_with_provenance,
        ),
        "DATA_FACTORY": _LeafProjection(
            _encode_data_factory,
            _decode_data_factory,
            native_decode=_CanonicalNative(_require_data_factory_available),
        ),
        "DATA_QUALITY": _LeafProjection(
            _encode_data_quality,
            _decode_data_quality,
            preserve_guard=_guard_data_quality_present,
            native_decode=_decode_opaque_data_quality_with_provenance,
        ),
        "DEPENDENT": _LeafProjection(_encode_dependent, _decode_dependent),
        "DINKY": _LeafProjection(
            _encode_dinky,
            _decode_dinky,
            native_decode=_CanonicalNative(_require_dinky_available),
        ),
        "DMS": _LeafProjection(
            _encode_dms,
            _decode_dms,
            preserve_guard=_require_dms_available,
            native_decode=_CanonicalNative(_require_dms_available),
        ),
        "DYNAMIC": _WorkflowProjection(
            _encode_dynamic_literal_single_dimension_fanout,
            _decode_dynamic_literal_single_dimension_fanout,
            native_decode=_decode_opaque_dynamic_with_provenance,
        ),
        "EMR": _LeafProjection(_encode_emr, _decode_emr),
        "FLINK": _LeafProjection(
            _encode_flink_inline_local_sql,
            _decode_flink_inline_local_sql,
            native_decode=partial(
                _decode_opaque_flink_with_provenance, task_type="FLINK"
            ),
        ),
        "FLINK_STREAM": _LeafProjection(
            _encode_flink_stream_inline_local_sql,
            _decode_flink_stream_inline_local_sql,
            native_decode=partial(
                _decode_opaque_flink_with_provenance, task_type="FLINK_STREAM"
            ),
        ),
        "GRPC": _LeafProjection(
            _encode_grpc,
            _decode_grpc,
            preserve_guard=_require_grpc_available,
            native_decode=_CanonicalNative(_require_grpc_available),
        ),
        "HTTP": _LeafProjection(_encode_http, _decode_http),
        "JAVA": _ResourceProjection(
            _encode_java_literal_fat_jar,
            _decode_java_literal_fat_jar,
            preserve_guard=_require_java_plugin_present,
            native_decode=_decode_opaque_java_with_provenance,
        ),
        "JUPYTER": _LeafProjection(_encode_jupyter, _decode_jupyter),
        "K8S": _LeafProjection(
            _encode_k8s,
            _decode_k8s,
            preserve_guard=_require_k8s_plugin_present,
            native_decode=_decode_opaque_k8s_with_provenance,
        ),
        "KUBEFLOW": _LeafProjection(
            _encode_kubeflow,
            _decode_kubeflow,
            preserve_guard=_require_kubeflow_available,
            native_decode=_CanonicalNative(_require_kubeflow_available),
        ),
        "MR": _ResourceProjection(
            _encode_mr_literal_java_jar_job,
            _decode_mr_literal_java_jar_job,
            preserve_guard=_require_mr_plugin_present,
            native_decode=_decode_opaque_mr_with_provenance,
        ),
        "OPENMLDB": _LeafProjection(
            _encode_openmldb,
            _decode_openmldb,
            native_decode=_CanonicalNative(_require_openmldb_plugin_present),
        ),
        "PROCEDURE": _LeafProjection(_encode_procedure, _decode_procedure),
        "PYTHON": _ResourceProjection(
            _encode_legacy_python,
            _decode_legacy_python,
            native_decode=partial(
                _decode_opaque_script_with_provenance, task_type="PYTHON"
            ),
        ),
        "PYTORCH": _ResourceProjection(
            _encode_pytorch_literal_resource_script,
            _decode_pytorch_literal_resource_script,
            preserve_guard=_require_pytorch_plugin_present,
            native_decode=_decode_opaque_pytorch_with_provenance,
        ),
        "SAGEMAKER": _LeafProjection(
            _encode_sagemaker,
            _decode_sagemaker,
            native_decode=_decode_opaque_sagemaker_with_provenance,
        ),
        "SEATUNNEL": _LeafProjection(
            _encode_seatunnel_literal_local_config,
            _decode_seatunnel_literal_local_config,
            preserve_guard=_guard_seatunnel_available,
            native_decode=_decode_opaque_seatunnel_with_provenance,
        ),
        "SHELL": _ResourceProjection(
            _encode_legacy_shell,
            _decode_legacy_shell,
            native_decode=partial(
                _decode_opaque_script_with_provenance, task_type="SHELL"
            ),
        ),
        "SPARK": _LeafProjection(
            _encode_spark_inline_local_sql,
            _decode_spark_inline_local_sql,
            native_decode=_decode_opaque_spark_with_provenance,
        ),
        "SQL": _LeafProjection(
            _encode_sql, _decode_sql, native_decode=_decode_opaque_sql_with_provenance
        ),
        "SQOOP": _LeafProjection(
            _encode_sqoop_literal_command,
            _decode_sqoop_literal_command,
            preserve_guard=_require_sqoop_plugin_present,
            native_decode=_decode_opaque_sqoop_with_provenance,
        ),
        "SWITCH": _LocalReferenceProjection(_encode_switch, _decode_switch),
        "WATERDROP": _ResourceProjection(
            _encode_waterdrop_literal_local_config_job,
            _decode_waterdrop_literal_local_config_job,
            native_decode=_decode_opaque_waterdrop_with_provenance,
        ),
        "ZEPPELIN": _LeafProjection(_encode_zeppelin, _decode_zeppelin),
    }
)
